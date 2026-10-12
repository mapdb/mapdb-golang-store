package btree

import (
	"encoding/binary"
	"iter"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/format"
	"github.com/mapdb/mapdb5-go/internal/handle"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// BufferTreeMap is a buffer tree (Bε-tree / fractal-tree family) over a
// store.StoreDelta — the WRITE-OPTIMIZED companion to BTreeMap (Java
// org.mapdb.btree.BufferTreeMap, spec 03 §BufferTreeMap).
//
// The base tree is a B-link tree (same node header, key group and split
// discipline as BTreeMap, minus the leaf fence) whose every node record
// additionally carries a BUFFER of pending operations in its spare record
// capacity, framed by format.BufferedPageFormat:
//
//	int32 baseLen
//	base: fp header (format.WriteBaseFp), packInt(keysLen<<4|flags),
//	      [packLong(link) unless RIGHT], key group,
//	      child recids (dir, packed longs) | value group (leaf)
//	op*:  reverse-readable delta frames (OpPut / OpDelete), oldest first
//
// Writes are BLIND MESSAGES: PutOnly/RemoveOnly append one op into the
// ROOT's spare capacity via StoreDelta.Append — no node rewrite, no read.
// When a node's buffer is full (Append refused) its ops flush one level
// down in per-child batches; leaves consolidate ops into the base image
// (LWW, tombstones drop out) and split k-way when overfull. Buffer budget is
// level-dependent headroom provisioned by UpdateWithHeadroom on every
// rewrite: BufferBytes at directory nodes, LeafHeadroom at leaves.
//
// Invariants (Java): within one record later ops override earlier; across
// levels a node's ops override its whole subtree (ops only move DOWN). A
// point read scans each node's op tail newest-first top-down and the first
// match answers; only then is the leaf base searched. Ops are appended to
// children BEFORE the parent is rewritten without them (a racing reader
// sees a moving op once or twice — LWW-idempotent — never zero times).
// Splits write right siblings first. No node merge on remove.
//
// Concurrency: THREAD-SAFE but writes DO NOT SCALE — a single map-global
// writer mutex serializes every mutation (Put/Remove, the CAS methods and
// poll all run inside it). Readers take no map lock: each node read is one
// store.Read under the store's own per-record locking, which is the
// happens-before edge (Java's lock-free readers; Go v1 store reads are
// locked, GD6). Iteration is a read-only DFS merge, weakly consistent.
// These guarantees hold only over a thread-safe store, as for BTreeMap.
//
// Not ported (PORTING-GAPS): modification listeners, external values, the
// O(1) counter (Java BufferTreeMap has none of them either); LSN stamping
// (Java STAMP_LSN == false).
type BufferTreeMap[K, V any] struct {
	inner *bufferTreeInner[K, V]
}

// DefaultLeafHeadroom is Java's default leaf append headroom (chosen by
// sweep: halves the live footprint vs flat BufferBytes everywhere).
const DefaultLeafHeadroom = 128

// LeafHeadroomNone requests ZERO leaf headroom (Java leafHeadroom == 0):
// leaf appends are refused and ops take the deliver/consolidate path. The
// Config zero value (0) selects DefaultLeafHeadroom instead.
const LeafHeadroomNone = -1

// bufferTreeBaseFpBitsPerKey is the leaf negative-lookup Bloom budget
// (Java BASE_FP_BITS_PER_KEY = 8: ~1 byte/key, ~2% false positives).
const bufferTreeBaseFpBitsPerKey = 8

// BufferTreeConfig configures a BufferTreeMap. MaxNodeSize, BufferBytes and
// LeafHeadroom are open-time policy, NOT persisted (Java parity): pass the
// same values to OpenBufferTreeMap to keep the allocation policy.
type BufferTreeConfig[K, V any] struct {
	KeyFormat   ser.GroupFormat[K]
	ValueFormat ser.GroupFormat[V]
	MaxNodeSize int // 4..=1048576
	BufferBytes int // directory append headroom, >= 32
	// LeafHeadroom is the leaf append headroom in bytes; 0 selects
	// DefaultLeafHeadroom, LeafHeadroomNone selects zero headroom.
	LeafHeadroom int
}

type bufferTreeInner[K, V any] struct {
	st             store.StoreDelta
	kf             ser.GroupFormat[K]
	vf             ser.GroupFormat[V]
	codec          *format.BufferedPageFormat[K, V]
	nodeSer        ser.AnySerializer
	maxNodeSize    int
	bufferBytes    int
	leafHeadroom   int
	rootRecidRecid uint64
	canonicalKey   bool
	// writeMu is the single map-global writer lock (Java writeLock).
	writeMu       sync.Mutex
	rootCacheable bool
	cachedRoot    atomic.Uint64
	// structVer is a seqlock over splits: it turns ODD before the first
	// split of a structural section rewrites any node in place (the one
	// change that shrinks a node's key coverage) and EVEN again only once
	// the section has published every separator / the new root. Iterators
	// never descend while it is odd and re-descend from a fresh root when
	// it changes across their node loads, so neither a stale parent/root
	// snapshot nor an unpublished child split hides a split-off right part.
	structVer atomic.Uint64
	// poisoned is set when a structural update (flush/split/root-grow)
	// panicked part-way: every later op panics KindPoisoned (spec 03
	// lock-leak / poison discipline).
	poisoned atomic.Bool
	lease    *store.LeaseGuard
	closed   atomic.Bool
	ops      handle.Tracker
}

// btOp is one buffered message: a PUT, or a tombstone when del.
type btOp[K, V any] struct {
	key   K
	value V
	del   bool
}

// bnode is the immutable buffer-tree node: BTreeMap-shaped base plus the
// decoded op tail (append order == oldest first).
type bnode[K, V any] struct {
	flags    int32
	link     uint64
	keys     ser.Group
	values   ser.Group // leaf
	children []uint64  // dir
	ops      []btOp[K, V]
}

func (n *bnode[K, V]) isDir() bool   { return n.flags&flagDir != 0 }
func (n *bnode[K, V]) isRight() bool { return n.flags&flagRight != 0 }

// ===================== node serializer =====================

type bnodeSerializer[K, V any] struct {
	in *bufferTreeInner[K, V]
}

func (s bnodeSerializer[K, V]) SizeHint() int {
	in := s.in
	ke := in.kf.Element().FixedSize()
	ve := in.vf.Element().FixedSize()
	keyBytes, valBytes := 32, 32
	if ke > 0 {
		keyBytes = ke
	}
	if ve > 0 {
		valBytes = max(ve, 9)
	}
	fpBytes := 0
	if in.codec.BaseFingerprintEnabled() {
		fpBytes = in.maxNodeSize + 4
	}
	return 24 + (in.maxNodeSize+1)*(keyBytes+valBytes) + in.bufferBytes + fpBytes
}

func (s bnodeSerializer[K, V]) Serialize(w *bin.Writer, n bnode[K, V]) {
	in := s.in
	lenSlot := w.Pos
	w.WriteInt(0) // baseLen backpatched below
	baseStart := w.Pos
	// Base fingerprint FIRST in the base region (leaves only — a dir read
	// routes to a child, "absent" is meaningless there); rebuilt on every
	// whole-image rewrite so it always matches the base keys.
	if n.isDir() {
		in.codec.WriteBaseFp(w, nil)
	} else {
		in.codec.WriteBaseFp(w, n.keys)
	}
	keysLen := in.kf.Size(n.keys)
	w.PackInt(int32(keysLen<<4) | n.flags)
	if !n.isRight() {
		w.PackLong(int64(n.link))
	}
	in.kf.SerializeGroup(w, n.keys)
	if n.isDir() {
		for _, c := range n.children {
			w.PackLong(int64(c))
		}
	} else {
		in.vf.SerializeGroup(w, n.values)
	}
	binary.BigEndian.PutUint32(w.Buf[lenSlot:], uint32(w.Pos-baseStart))
	for _, op := range n.ops {
		in.writeOp(w, op)
	}
}

func (s bnodeSerializer[K, V]) Deserialize(r *bin.Reader, size int) bnode[K, V] {
	in := s.in
	start := r.Pos()
	if size < 4 {
		panic(mapdb5.NewCorruption("buffer-tree record too short for node header"))
	}
	baseLen := int(r.ReadInt())
	if baseLen < 0 || baseLen > size-4 {
		panic(mapdb5.NewCorruption("torn baseLen"))
	}
	var hdr format.BaseHeader
	in.codec.ParseBaseHeader(r, start+4, baseLen, &hdr)
	r.Seek(hdr.NodeBase)
	h := r.UnpackInt()
	flags := h & 0xF
	keysLen := int(uint32(h) >> 4)
	if keysLen > baseLen {
		panic(mapdb5.NewCorruption("torn keysLen"))
	}
	n := bnode[K, V]{flags: flags}
	if flags&flagRight == 0 {
		l := r.UnpackLong()
		if l == 0 {
			panic(mapdb5.NewCorruption("non-rightmost node with zero link"))
		}
		n.link = uint64(l)
	}
	n.keys = in.kf.DeserializeGroup(r, keysLen)
	in.checkSorted(n.keys)
	if flags&flagDir != 0 {
		childCount := keysLen
		if flags&flagRight != 0 {
			childCount++
		}
		if childCount == 0 {
			panic(mapdb5.NewCorruption("directory node with no children"))
		}
		n.children = make([]uint64, childCount)
		for i := range n.children {
			c := r.UnpackLong()
			if c == 0 {
				panic(mapdb5.NewCorruption("directory child recid is zero"))
			}
			n.children[i] = uint64(c)
		}
	} else {
		n.values = in.vf.DeserializeGroup(r, keysLen)
	}
	// The node base must fill the base region exactly: a forged baseLen
	// would otherwise shift the op-tail boundary undetected.
	if r.Pos() != start+4+baseLen {
		panic(mapdb5.NewCorruption("buffer-tree base length mismatch"))
	}
	in.codec.ForEachEntryOldestFirst(r, start+4+baseLen, start+size, func(op int, k K, v V, _ int64) {
		n.ops = append(n.ops, btOp[K, V]{key: k, value: v, del: op == format.OpDelete})
	})
	r.Seek(start + size)
	return n
}

func (s bnodeSerializer[K, V]) FixedSize() int                { return -1 }
func (s bnodeSerializer[K, V]) Compare(a, b bnode[K, V]) int  { panic(misuseBNode()) }
func (s bnodeSerializer[K, V]) Equals(a, b bnode[K, V]) bool  { panic(misuseBNode()) }
func (s bnodeSerializer[K, V]) NaturalOrder() bool            { return false }
func (s bnodeSerializer[K, V]) EqualsBySerializedBytes() bool { return true }

func misuseBNode() error {
	return mapdb5.NewMisuse("buffer-tree nodes are never compared")
}

func (in *bufferTreeInner[K, V]) checkSorted(keys ser.Group) {
	n := in.kf.Size(keys)
	if n < 2 {
		return
	}
	prev := in.kf.Get(keys, 0)
	for i := 1; i < n; i++ {
		cur := in.kf.Get(keys, i)
		if in.kf.Compare(prev, cur) >= 0 {
			panic(mapdb5.NewCorruption("buffer-tree node keys not strictly ascending"))
		}
		prev = cur
	}
}

// writeOp frames one op (LSN unassigned — Java STAMP_LSN == false).
func (in *bufferTreeInner[K, V]) writeOp(w *bin.Writer, op btOp[K, V]) {
	kind := format.OpPut
	if op.del {
		kind = format.OpDelete
	}
	in.codec.EncodeInto(w, kind, op.key, op.value, format.LSNUnassigned)
}

func (in *bufferTreeInner[K, V]) encodeOps(ops []btOp[K, V]) []byte {
	w := bin.GetWriter()
	defer bin.PutWriter(w)
	for _, op := range ops {
		in.writeOp(w, op)
	}
	return w.CopyBytes()
}

// ===================== construction & lifecycle =====================

func (cfg BufferTreeConfig[K, V]) validate() (leafHeadroom int, err error) {
	if cfg.KeyFormat == nil || cfg.ValueFormat == nil {
		return 0, mapdb5.New(mapdb5.KindWrongConfiguration, "nil group format")
	}
	if cfg.MaxNodeSize < MinMaxNodeSize || cfg.MaxNodeSize > MaxMaxNodeSize {
		return 0, mapdb5.New(mapdb5.KindWrongConfiguration, "maxNodeSize must be in 4..=1048576")
	}
	if cfg.BufferBytes < 32 {
		return 0, mapdb5.New(mapdb5.KindWrongConfiguration, "bufferBytes must be >= 32")
	}
	switch {
	case cfg.LeafHeadroom == 0:
		return DefaultLeafHeadroom, nil
	case cfg.LeafHeadroom == LeafHeadroomNone:
		return 0, nil
	case cfg.LeafHeadroom < 0:
		return 0, mapdb5.New(mapdb5.KindWrongConfiguration, "leafHeadroom must be >= 0 (or LeafHeadroomNone)")
	}
	return cfg.LeafHeadroom, nil
}

// newBufferTreeInner builds unpublished state (no lease, rootRecidRecid 0)
// — the Java "tmp" instance used to write the initial nodes.
func newBufferTreeInner[K, V any](s store.StoreDelta, cfg BufferTreeConfig[K, V], leafHeadroom int) *bufferTreeInner[K, V] {
	in := &bufferTreeInner[K, V]{
		st:            s,
		kf:            cfg.KeyFormat,
		vf:            cfg.ValueFormat,
		codec:         format.New(cfg.KeyFormat, cfg.ValueFormat, true, true, bufferTreeBaseFpBitsPerKey),
		maxNodeSize:   cfg.MaxNodeSize,
		bufferBytes:   cfg.BufferBytes,
		leafHeadroom:  leafHeadroom,
		canonicalKey:  cfg.KeyFormat.Element().EqualsBySerializedBytes(),
		rootCacheable: !s.IsTx(),
	}
	in.nodeSer = ser.Erase[bnode[K, V]](bnodeSerializer[K, V]{in: in})
	return in
}

// NewBufferTreeMap creates a fresh empty map over s. Panics
// *mapdb5.DBError{KindWrongConfiguration} on an invalid config.
func NewBufferTreeMap[K, V any](s store.StoreDelta, cfg BufferTreeConfig[K, V]) *BufferTreeMap[K, V] {
	m, err := createBufferTree(s, cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func createBufferTree[K, V any](s store.StoreDelta, cfg BufferTreeConfig[K, V]) (m *BufferTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	lh, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	tmp := newBufferTreeInner(s, cfg, lh)
	root := tmp.newNode(bnode[K, V]{flags: flagLeft | flagRight, keys: cfg.KeyFormat.Empty(), values: cfg.ValueFormat.Empty()})
	rrr := store.Put(s, int64(root), ser.Long)
	return openBufferTree(s, rrr, cfg, lh)
}

// OpenBufferTreeMap reopens the map whose root-pointer recid is
// rootRecidRecid (BufferTreeMap.RootRecidRecid). Errors (GD5): invalid
// config, a damaged root pointer, a conflicting live handle.
func OpenBufferTreeMap[K, V any](s store.StoreDelta, rootRecidRecid store.Recid, cfg BufferTreeConfig[K, V]) (m *BufferTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	lh, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	return openBufferTree(s, rootRecidRecid, cfg, lh)
}

func openBufferTree[K, V any](s store.StoreDelta, rrr uint64, cfg BufferTreeConfig[K, V], leafHeadroom int) (*BufferTreeMap[K, V], error) {
	if rrr == 0 {
		return nil, mapdb5.NewCorruption("root_recid_recid must be a valid recid")
	}
	lease, err := store.AcquireLease(s, rrr, store.LeaseReadWrite)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			lease.Release()
		}
	}()
	in := newBufferTreeInner(s, cfg, leafHeadroom)
	in.rootRecidRecid = rrr
	in.lease = lease
	m := &BufferTreeMap[K, V]{inner: in}
	// Validate the root pointer and root node up front: a damaged header
	// is an open-time error, not a first-op panic.
	root := m.loadRootRecid()
	if n := m.load(root); n.flags&(flagLeft|flagRight) != flagLeft|flagRight {
		return nil, mapdb5.NewCorruption("buffer-tree root is not a LEFT|RIGHT node")
	}
	committed = true
	return m, nil
}

// CreateBufferTreeFromSorted bulk-builds a map from STRICTLY ascending
// entries with the default pump fill. Every node is written once with its
// level's headroom and an EMPTY op tail (fully consolidated). Returns
// pump.ErrNotSorted on misordered/duplicate input. Single-threaded; the
// caller commits.
func CreateBufferTreeFromSorted[K, V any](s store.StoreDelta, cfg BufferTreeConfig[K, V], entries iter.Seq2[K, V]) (*BufferTreeMap[K, V], error) {
	return CreateBufferTreeFromSortedFill(s, cfg, DefaultPumpFill(cfg.MaxNodeSize), entries)
}

// CreateBufferTreeFromSortedFill is CreateBufferTreeFromSorted with an
// explicit node fill (<= MaxNodeSize).
func CreateBufferTreeFromSortedFill[K, V any](s store.StoreDelta, cfg BufferTreeConfig[K, V], nodeFill int, entries iter.Seq2[K, V]) (m *BufferTreeMap[K, V], err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				if de.Kind == mapdb5.KindNotSorted {
					err = errNotSorted
					return
				}
				err = de
				return
			}
			panic(r)
		}
	}()
	lh, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	tmp := newBufferTreeInner(s, cfg, lh)
	p := NewTreePump[K, V](s, bufferTreeSink[K, V]{in: tmp}, cfg.MaxNodeSize, nodeFill)
	for k, v := range entries {
		p.Put(k, v)
	}
	root := p.Finish()
	rrr := store.Put(s, int64(root), ser.Long)
	return openBufferTree(s, rrr, cfg, lh)
}

// bufferTreeSink is the TreePump NodeSink writing buffer-tree nodes with
// their level's headroom.
type bufferTreeSink[K, V any] struct{ in *bufferTreeInner[K, V] }

func (s bufferTreeSink[K, V]) CompareKeys(a, b K) int { return s.in.kf.Compare(a, b) }

func (s bufferTreeSink[K, V]) WriteLeaf(recid store.Recid, flags int32, link store.Recid, keys []K, values []V) {
	s.in.rewrite(recid, bnode[K, V]{flags: flags, link: link, keys: s.in.kf.FromSlice(keys), values: s.in.vf.FromSlice(values)})
}

func (s bufferTreeSink[K, V]) WriteDir(recid store.Recid, flags int32, link store.Recid, keys []K, children []store.Recid) {
	s.in.rewrite(recid, bnode[K, V]{flags: flags, link: link, keys: s.in.kf.FromSlice(keys), children: children})
}

// RootRecidRecid is the root-pointer recid; persist it to reopen the map.
func (m *BufferTreeMap[K, V]) RootRecidRecid() store.Recid { return m.inner.rootRecidRecid }

// MaxNodeSize is the per-node key ceiling.
func (m *BufferTreeMap[K, V]) MaxNodeSize() int { return m.inner.maxNodeSize }

// KeySerializer is the key element serializer.
func (m *BufferTreeMap[K, V]) KeySerializer() ser.Serializer[K] { return m.inner.kf.Element() }

// ValueSerializer is the value element serializer.
func (m *BufferTreeMap[K, V]) ValueSerializer() ser.Serializer[V] { return m.inner.vf.Element() }

// IsClosed reports whether the handle was closed or the store is closed.
func (m *BufferTreeMap[K, V]) IsClosed() bool {
	return m.inner.closed.Load() || m.inner.st.IsClosed()
}

// Close invalidates the handle (every later op panics KindClosed), drains
// in-flight ops, then releases the open lease. Does NOT close the store.
// Idempotent.
func (m *BufferTreeMap[K, V]) Close() error {
	m.CloseHandle()
	return nil
}

// CloseHandle is the DB-owned retirement path (see BTreeMap.CloseHandle):
// invalidate, DRAIN admitted ops, then release the lease. BufferTreeMap
// has no callbacks, so a Close re-entering from inside an admitted op (a
// store/serializer hook) is misuse: releasing the lease while the outer
// write continues would admit a second live writer. It panics instead.
func (m *BufferTreeMap[K, V]) CloseHandle() {
	if m.inner.ops.Holds() {
		panic(mapdb5.NewMisuse("buffer-tree Close called from inside one of its own operations"))
	}
	if m.inner.closed.CompareAndSwap(false, true) {
		m.inner.ops.Wait()
		if l := m.inner.lease; l != nil {
			l.Release()
		}
	}
}

// opGuard admits one operation to the lifetime drain (admit, then
// re-check closed — see BTreeMap.opGuard).
//
// Internal store funnels (load/rootRecid) deliberately do NOT re-check
// closed: an admitted op must run to completion — aborting a writer
// between a child split and its parent's separator insert would persist a
// tree that loses keys from traversal. CloseHandle drains admitted ops
// before releasing the lease, which is what makes that safe.
func (m *BufferTreeMap[K, V]) opGuard() func() {
	m.checkOpen()
	release := m.inner.ops.Admit()
	if m.inner.closed.Load() {
		release()
		panicBufferTreeClosed()
	}
	return release
}

// checkOpen fails fast on a closed or poisoned handle (op entry and every
// resumed iterator yield).
func (m *BufferTreeMap[K, V]) checkOpen() {
	if m.inner.closed.Load() {
		panicBufferTreeClosed()
	}
	if m.inner.poisoned.Load() {
		panic(mapdb5.New(mapdb5.KindPoisoned,
			"buffer-tree poisoned by a failed structural update; reopen the store"))
	}
}

// structural runs a flush/split/root-grow section; any panic escaping it
// poisons the map (the tree may be part-way through a multi-record
// structural change) and is re-raised.
func (m *BufferTreeMap[K, V]) structural(f func()) {
	ok := false
	defer func() {
		if !ok {
			// structVer may stay odd: iterators then wait until checkOpen
			// reports the poison.
			m.inner.poisoned.Store(true)
		}
	}()
	f()
	ok = true
	if v := m.inner.structVer.Load(); v&1 == 1 {
		m.inner.structVer.Store(v + 1) // splits fully published
	}
}

func panicBufferTreeClosed() {
	panic(mapdb5.New(mapdb5.KindClosed, "buffer-tree map handle was closed"))
}

// ===================== store funnels =====================

func (m *BufferTreeMap[K, V]) loadRootRecid() uint64 {
	v, ok := store.Get(m.inner.st, m.inner.rootRecidRecid, ser.Long)
	if !ok || v <= 0 {
		panic(mapdb5.NewCorruption("buffer-tree root pointer missing or invalid"))
	}
	return uint64(v)
}

// rootRecid returns the cached root, reloading on tx stores where a
// rollback can rewind the pointer. The cache is INITIALIZED only by CAS
// from 0: a reader that fetched the pointer before a concurrent root-grow
// must never overwrite the newer root growRoot published (writers trust the
// cache, and appending into a superseded root — now only the leftmost
// child — would put ops outside its routing band).
func (m *BufferTreeMap[K, V]) rootRecid() uint64 {
	if r := m.inner.cachedRoot.Load(); r != 0 {
		return r
	}
	r := m.loadRootRecid()
	if m.inner.rootCacheable && !m.inner.cachedRoot.CompareAndSwap(0, r) {
		return m.inner.cachedRoot.Load()
	}
	return r
}

func (m *BufferTreeMap[K, V]) load(recid uint64) bnode[K, V] {
	v := m.inner.st.Get(recidOrCorrupt(recid), m.inner.nodeSer)
	if v == nil {
		panic(mapdb5.NewCorruption("buffer-tree node record is null"))
	}
	n, ok := v.(bnode[K, V])
	if !ok {
		panic(mapdb5.NewCorruption("buffer-tree record is not a node"))
	}
	return n
}

func (in *bufferTreeInner[K, V]) headroomFor(flags int32) int {
	if flags&flagDir != 0 {
		return in.bufferBytes
	}
	return in.leafHeadroom
}

// newNode = preallocate + first write with the level's headroom.
func (in *bufferTreeInner[K, V]) newNode(n bnode[K, V]) uint64 {
	recid := in.st.Preallocate()
	in.rewrite(recid, n)
	return recid
}

func (in *bufferTreeInner[K, V]) rewrite(recid uint64, n bnode[K, V]) {
	in.st.UpdateWithHeadroom(recid, n, in.nodeSer, in.headroomFor(n.flags))
}

// ===================== write path (single writer, blind messages) =====================

// PutOnly is the blind upsert — the PRIMARY hot path: one append into the
// root buffer, no old-value read.
func (m *BufferTreeMap[K, V]) PutOnly(key K, value V) {
	release := m.opGuard()
	defer release()
	m.inner.writeMu.Lock()
	defer m.inner.writeMu.Unlock()
	m.appendMessage(btOp[K, V]{key: key, value: value})
}

// RemoveOnly is the blind delete: appends a tombstone without reading.
func (m *BufferTreeMap[K, V]) RemoveOnly(key K) {
	release := m.opGuard()
	defer release()
	m.inner.writeMu.Lock()
	defer m.inner.writeMu.Unlock()
	m.appendMessage(btOp[K, V]{key: key, del: true})
}

// locked runs f inside one admitted writer critical section.
func (m *BufferTreeMap[K, V]) locked(f func()) {
	release := m.opGuard()
	defer release()
	m.inner.writeMu.Lock()
	defer m.inner.writeMu.Unlock()
	f()
}

// Put is the Map-contract upsert returning the previous value; the lookup
// and the append share one writer critical section.
func (m *BufferTreeMap[K, V]) Put(key K, value V) (old V, had bool) {
	m.locked(func() {
		old, had = m.doGet(key)
		m.appendMessage(btOp[K, V]{key: key, value: value})
	})
	return
}

// Remove deletes key returning the previous value. An absent key appends
// nothing.
func (m *BufferTreeMap[K, V]) Remove(key K) (old V, had bool) {
	m.locked(func() {
		old, had = m.doGet(key)
		if had {
			m.appendMessage(btOp[K, V]{key: key, del: true})
		}
	})
	return
}

// PutIfAbsent inserts only when key is absent; returns the existing value
// and true when present (no write).
func (m *BufferTreeMap[K, V]) PutIfAbsent(key K, value V) (old V, had bool) {
	m.locked(func() {
		old, had = m.doGet(key)
		if !had {
			m.appendMessage(btOp[K, V]{key: key, value: value})
		}
	})
	return
}

// RemoveIfEquals removes key only when mapped to a value equal to
// expected (value element Equals).
func (m *BufferTreeMap[K, V]) RemoveIfEquals(key K, expected V) (removed bool) {
	m.locked(func() {
		cur, had := m.doGet(key)
		if had && m.inner.vf.Element().Equals(cur, expected) {
			m.appendMessage(btOp[K, V]{key: key, del: true})
			removed = true
		}
	})
	return
}

// Replace overwrites key only when present; returns the previous value.
func (m *BufferTreeMap[K, V]) Replace(key K, value V) (old V, had bool) {
	m.locked(func() {
		old, had = m.doGet(key)
		if had {
			m.appendMessage(btOp[K, V]{key: key, value: value})
		}
	})
	return
}

// ReplaceIfEquals overwrites key only when mapped to a value equal to
// oldValue.
func (m *BufferTreeMap[K, V]) ReplaceIfEquals(key K, oldValue, newValue V) (replaced bool) {
	m.locked(func() {
		cur, had := m.doGet(key)
		if had && m.inner.vf.Element().Equals(cur, oldValue) {
			m.appendMessage(btOp[K, V]{key: key, value: newValue})
			replaced = true
		}
	})
	return
}

// appendMessage appends one op into the root buffer; on capacity refusal it
// flushes the root one level down (splits grow the tree) and retries, and
// an op larger than an empty buffer is pushed down directly. writeMu held.
func (m *BufferTreeMap[K, V]) appendMessage(op btOp[K, V]) {
	in := m.inner
	single := []btOp[K, V]{op}
	enc := in.encodeOps(single)
	root := m.rootRecid()
	if in.st.Append(root, enc) != store.AppendRefused {
		return
	}
	m.structural(func() {
		if parts := m.deliver(root, nil); parts != nil {
			m.growRoot(root, parts)
		}
		root = m.rootRecid()
		if in.st.Append(root, enc) != store.AppendRefused {
			return
		}
		if parts := m.deliver(root, single); parts != nil {
			m.growRoot(root, parts)
		}
	})
}

// splitPart is a new right sibling produced by a split; sep is the left
// neighbour's inclusive high bound.
type splitPart[K any] struct {
	sep   K
	recid uint64
}

func combineOps[K, V any](own, newer []btOp[K, V]) []btOp[K, V] {
	if len(own) == 0 {
		return newer
	}
	all := make([]btOp[K, V], 0, len(own)+len(newer))
	all = append(all, own...)
	return append(all, newer...)
}

// route is the child index for key (same inclusive-separator rule as get).
func (in *bufferTreeInner[K, V]) route(keys ser.Group, key K, childCount int) int {
	ci, _ := in.kf.Search(keys, key)
	if ci >= childCount {
		ci = childCount - 1
	}
	return ci
}

// partition splits ops per child by base-key routing (nil = no ops).
func (in *bufferTreeInner[K, V]) partition(dir *bnode[K, V], ops []btOp[K, V]) [][]btOp[K, V] {
	batches := make([][]btOp[K, V], len(dir.children))
	for _, op := range ops {
		ci := in.route(dir.keys, op.key, len(dir.children))
		batches[ci] = append(batches[ci], op)
	}
	return batches
}

// deliver hands `incoming` (newer than anything buffered here) to the node
// at recid, flushing its own buffer along the way. Leaf: consolidate. Dir:
// per-child batches appended (recursing on refusal), then this dir is
// rewritten with an empty buffer and any new separators — children BEFORE
// parent. Returns new right siblings, or nil when the node did not split.
func (m *BufferTreeMap[K, V]) deliver(recid uint64, incoming []btOp[K, V]) []splitPart[K] {
	in := m.inner
	n := m.load(recid)
	all := combineOps(n.ops, incoming)
	if !n.isDir() {
		keys, vals := in.applyOps(n.keys, n.values, all)
		return m.writeSplitParts(recid, n.flags, n.link, keys, vals, nil)
	}
	batches := in.partition(&n, all)
	newKeys, newChildren := n.keys, n.children
	baseChanged := false
	for ci, batch := range batches {
		if batch == nil {
			continue
		}
		if in.st.Append(n.children[ci], in.encodeOps(batch)) != store.AppendRefused {
			continue
		}
		for _, p := range m.deliver(n.children[ci], batch) {
			newKeys, newChildren = in.insertSep(newKeys, newChildren, p)
			baseChanged = true
		}
	}
	if !baseChanged && len(all) == 0 {
		return nil
	}
	return m.writeSplitParts(recid, n.flags, n.link, newKeys, nil, newChildren)
}

// insertSep folds one child split into a dir base.
func (in *bufferTreeInner[K, V]) insertSep(keys ser.Group, children []uint64, p splitPart[K]) (ser.Group, []uint64) {
	ip, found := in.kf.Search(keys, p.sep)
	if found {
		panic(mapdb5.NewCorruption("buffer-tree duplicate separator"))
	}
	return in.kf.Insert(keys, ip, p.sep), insertU64(children, ip+1, p.recid)
}

// writeSplitParts rewrites the node with an EMPTY buffer, splitting k-way
// when the key count exceeds maxNodeSize. Right parts are written first
// (referent before referrer); the leftmost part is rewritten in place last.
func (m *BufferTreeMap[K, V]) writeSplitParts(recid uint64, flags int32, link uint64, keys, values ser.Group, children []uint64) []splitPart[K] {
	in := m.inner
	total := in.kf.Size(keys)
	dir := flags&flagDir != 0
	if total <= in.maxNodeSize {
		in.rewrite(recid, bnode[K, V]{flags: flags, link: link, keys: keys, values: values, children: children})
		return nil
	}
	// Coverage of the node at recid is about to shrink: mark a split in
	// flight before any in-place rewrite (see structVer; writer-only under
	// writeMu, closed again by structural()).
	if v := in.structVer.Load(); v&1 == 0 {
		in.structVer.Store(v + 1)
	}
	right := flags&flagRight != 0
	p := max(2, (total+in.maxNodeSize-1)/in.maxNodeSize)
	if (total+p-1)/p >= in.maxNodeSize {
		p++
	}
	cut := make([]int, p+1) // part i covers keys [cut[i], cut[i+1])
	base, rem := total/p, total%p
	for i := 0; i < p; i++ {
		cut[i+1] = cut[i] + base
		if i < rem {
			cut[i+1]++
		}
	}
	partRecid := make([]uint64, p)
	nextLink := link
	for i := p - 1; i >= 1; i-- { // rights first
		a, b := cut[i], cut[i+1]
		f := int32(0)
		if dir {
			f |= flagDir
		}
		last := i == p-1 && right
		if last {
			f |= flagRight
		}
		part := bnode[K, V]{flags: f, keys: in.kf.CopyRange(keys, a, b)}
		if !last {
			part.link = nextLink
		}
		if dir {
			cb := b
			if last {
				cb = b + 1
			}
			part.children = append([]uint64(nil), children[a:cb]...)
		} else {
			part.values = in.vf.CopyRange(values, a, b)
		}
		partRecid[i] = in.newNode(part)
		nextLink = partRecid[i]
	}
	f0 := flags & flagLeft
	if dir {
		f0 |= flagDir
	}
	left := bnode[K, V]{flags: f0, link: nextLink, keys: in.kf.CopyRange(keys, 0, cut[1])}
	if dir {
		left.children = append([]uint64(nil), children[:cut[1]]...)
	} else {
		left.values = in.vf.CopyRange(values, 0, cut[1])
	}
	in.rewrite(recid, left)
	partRecid[0] = recid
	parts := make([]splitPart[K], p-1)
	for i := 0; i < p-1; i++ {
		// last key of part i: its inclusive high bound, stays left
		parts[i] = splitPart[K]{sep: in.kf.Get(keys, cut[i+1]-1), recid: partRecid[i+1]}
	}
	return parts
}

// growRoot builds new root level(s) over [oldRoot, parts...].
func (m *BufferTreeMap[K, V]) growRoot(oldRoot uint64, parts []splitPart[K]) {
	in := m.inner
	childs := []uint64{oldRoot}
	seps := make([]K, 0, len(parts))
	for _, sp := range parts {
		seps = append(seps, sp.sep)
		childs = append(childs, sp.recid)
	}
	for len(seps) > in.maxNodeSize {
		// one dir level; the rightmost chunk carries the extra child (RIGHT
		// dir shape); built right-to-left so links are known.
		chunk := max(2, (in.maxNodeSize+1)/2)
		var ranges [][2]int
		for a := 0; a < len(childs); a += chunk {
			ranges = append(ranges, [2]int{a, min(len(childs), a+chunk)})
		}
		recids := make([]uint64, len(ranges))
		var linkRight uint64
		for r := len(ranges) - 1; r >= 0; r-- {
			a, b := ranges[r][0], ranges[r][1]
			isRight := r == len(ranges)-1
			keyTo := b
			if isRight {
				keyTo = b - 1
			}
			fl := flagDir
			if r == 0 {
				fl |= flagLeft
			}
			nd := bnode[K, V]{keys: in.kf.FromSlice(append([]K(nil), seps[a:keyTo]...)), children: append([]uint64(nil), childs[a:b]...)}
			if isRight {
				fl |= flagRight
			} else {
				nd.link = linkRight
			}
			nd.flags = fl
			recids[r] = in.newNode(nd)
			linkRight = recids[r]
		}
		var nc []uint64
		var ns []K
		for r := range ranges {
			nc = append(nc, recids[r])
			if r < len(ranges)-1 {
				ns = append(ns, seps[ranges[r][1]-1])
			}
		}
		childs, seps = nc, ns
	}
	newRoot := in.newNode(bnode[K, V]{flags: flagDir | flagLeft | flagRight, keys: in.kf.FromSlice(seps), children: childs})
	store.Update(in.st, in.rootRecidRecid, int64(newRoot), ser.Long)
	if in.rootCacheable {
		in.cachedRoot.Store(newRoot)
	}
}

// applyOps LWW-applies ops (oldest first) onto a leaf base image; tombstones
// drop out. Net effect per key, then a two-pointer merge with the sorted
// base (O(N+B)). Method-local state only — also runs on reader goroutines.
func (in *bufferTreeInner[K, V]) applyOps(keys, values ser.Group, ops []btOp[K, V]) (ser.Group, ser.Group) {
	if len(ops) == 0 {
		return keys, values
	}
	net := in.netOps(ops)
	n := in.kf.Size(keys)
	outK := make([]K, 0, n+len(net))
	outV := make([]V, 0, n+len(net))
	i, oi := 0, 0
	for i < n || oi < len(net) {
		var c int
		switch {
		case i >= n:
			c = 1
		case oi >= len(net):
			c = -1
		default:
			c = in.kf.Compare(in.kf.Get(keys, i), net[oi].key)
		}
		if c < 0 {
			outK = append(outK, in.kf.Get(keys, i))
			outV = append(outV, in.vf.Get(values, i))
			i++
			continue
		}
		op := net[oi]
		oi++
		if c == 0 {
			i++
		}
		if !op.del {
			outK = append(outK, op.key)
			outV = append(outV, op.value)
		}
	}
	return in.kf.FromSlice(outK), in.vf.FromSlice(outV)
}

// netOps collapses oldest-first ops to the last op per key, ascending in
// the KEY FORMAT order (the one coherent tree order).
func (in *bufferTreeInner[K, V]) netOps(ops []btOp[K, V]) []btOp[K, V] {
	idx := make([]int, len(ops))
	for i := range idx {
		idx[i] = i
	}
	// stable sort by key keeps append order within equal keys; the last
	// of each run wins.
	sortStableBy(idx, func(a, b int) int { return in.kf.Compare(ops[a].key, ops[b].key) })
	net := make([]btOp[K, V], 0, len(ops))
	for j := 0; j < len(idx); j++ {
		if j+1 < len(idx) && in.kf.Compare(ops[idx[j]].key, ops[idx[j+1]].key) == 0 {
			continue
		}
		net = append(net, ops[idx[j]])
	}
	return net
}

// FlushAll pushes every buffer down to the leaves (Java test hook; a writer
// op). Correctness never depends on it.
func (m *BufferTreeMap[K, V]) FlushAll() {
	m.locked(func() { m.structural(m.flushAllLocked) })
}

func (m *BufferTreeMap[K, V]) flushAllLocked() {
	for {
		anyOps := false
		root := m.rootRecid()
		if parts := m.forceFlush(root, &anyOps); parts != nil {
			m.growRoot(root, parts)
			continue // a split interrupted the pass; run another
		}
		if !anyOps {
			return
		}
	}
}

// forceFlush is one flush pass over the subtree; returns this node's own
// splits (the caller re-passes — fresh splits may still hold ops).
func (m *BufferTreeMap[K, V]) forceFlush(recid uint64, anyOps *bool) []splitPart[K] {
	in := m.inner
	n := m.load(recid)
	if !n.isDir() {
		if len(n.ops) == 0 {
			return nil
		}
		*anyOps = true
		return m.deliver(recid, nil)
	}
	if len(n.ops) > 0 {
		*anyOps = true
		if own := m.deliver(recid, nil); own != nil {
			return own
		}
		n = m.load(recid)
	}
	newKeys, newChildren := n.keys, n.children
	changed := false
	for _, child := range n.children {
		for _, p := range m.forceFlush(child, anyOps) {
			newKeys, newChildren = in.insertSep(newKeys, newChildren, p)
			changed = true
		}
	}
	if changed {
		return m.writeSplitParts(recid, n.flags, n.link, newKeys, nil, newChildren)
	}
	return nil
}
