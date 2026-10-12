package htree

// HTreeMap — port of Java org.mapdb.htree.HTreeMap: a segmented hash tree
// over a Store4 store.
//
// Structure — two layers stacked:
//   - SEGMENTATION: the top concShift bits of the mixed 32-bit key hash pick
//     one of 1<<concShift segments; each segment is an independent hash tree
//     guarded by its own sync.RWMutex (Java ReentrantReadWriteLock).
//   - HASH DIRECTORY TREE (per segment, DirTree): an auto-expanding sparse
//     tree keyed by the remaining levels*dirShift hash bits. Tree values are
//     recids of BUCKET records: packInt(count) + flat (key, value) pairs of
//     entries whose keys share the full hash index.
//
// Invariant: concShift + levels*dirShift == 32 (the full 32-bit hash is
// partitioned; anything less silently drops hash bits — Java §0.4).
//
// Concurrency: reads/iteration take the segment read lock, mutations the
// write lock (both no-ops when !store.IsThreadSafe()). Go's RWMutex is
// non-reentrant where Java's ReentrantReadWriteLock is not: a sync listener
// re-entering a WRITE is caught by the defer-window tripwire before it can
// block, and a write owner's re-entrant READ is exempted via segOwners.
// Iteration drains one segment at a time under its read lock and yields
// from the snapshot after release — weakly consistent, callbacks may mutate.
//
// Persistence: create allocates one empty dir root per segment plus a
// header record {concShift, dirShift, levels, hashSeed, segmentRoots[]};
// persist HeaderRecid and OpenHTreeMap to reattach. Dir roots are only ever
// updated in place, so the header is immutable after create (Java §0.6).

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/internal/handle"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// HConfig configures NewHTreeMap / OpenHTreeMap / CreateHTreeMapFromSorted.
type HConfig[K, V any] struct {
	KeySer   ser.Serializer[K]
	ValueSer ser.Serializer[V]
	// Geometry: all three zero → Java defaults (4, 7, 4). Otherwise
	// concShift ∈ [0,12], dirShift ∈ [1,MaxDirShift], levels ≥ 1 and
	// concShift + levels*dirShift must equal 32.
	ConcShift int
	DirShift  int
	Levels    int
	// HashSeed is mixed into every placement hash and persisted in the
	// header. Ignored unless HashSeedSet (unset → a random seed at create;
	// open always reconstructs the seed from the header).
	HashSeed    int32
	HashSeedSet bool
	// Hasher maps (key, seed) → 32-bit placement hash. nil →
	// mdhash.SerHasher32 over KeySer (deterministic across reopen, NOT
	// Java-hashCode-compatible — pass e.g. mdhash.String32 for Java wire
	// placement). Must be supplied identically on every open.
	Hasher mdhash.Hasher[K]
}

// hleaf is one bucket record: parallel key/value slices (Java's flat
// Object[]{k0,v0,k1,v1,…}).
type hleaf[K, V any] struct {
	keys []K
	vals []V
}

// leafSerializer is the bucket wire codec: packInt(count) then (key,
// value) per entry, inline (Java HTreeMap.leafSerializer).
type leafSerializer[K, V any] struct {
	keySer   ser.Serializer[K]
	valueSer ser.Serializer[V]
}

func (s leafSerializer[K, V]) Serialize(w *bin.Writer, leaf hleaf[K, V]) {
	if len(leaf.keys) != len(leaf.vals) || len(leaf.keys) == 0 {
		panic(mapdb5.NewMisuse("htree leaf must hold >= 1 paired entries"))
	}
	w.PackInt(int32(len(leaf.keys)))
	for i := range leaf.keys {
		s.keySer.Serialize(w, leaf.keys[i])
		s.valueSer.Serialize(w, leaf.vals[i])
	}
}

func (s leafSerializer[K, V]) Deserialize(r *bin.Reader, size int) hleaf[K, V] {
	n := int(r.UnpackInt())
	// Each entry needs >= 2 serialized bytes: n > size is torn/corrupt.
	if n < 1 || (size >= 0 && n > size) {
		panic(mapdb5.NewCorruption("htree leaf entry count out of range"))
	}
	leaf := hleaf[K, V]{keys: make([]K, n), vals: make([]V, n)}
	for i := 0; i < n; i++ {
		leaf.keys[i] = s.keySer.Deserialize(r, -1)
		leaf.vals[i] = s.valueSer.Deserialize(r, -1)
	}
	return leaf
}

func (s leafSerializer[K, V]) FixedSize() int { return -1 }
func (s leafSerializer[K, V]) SizeHint() int  { return 34 }
func (s leafSerializer[K, V]) Compare(a, b hleaf[K, V]) int {
	panic(mapdb5.NewMisuse("htree leaf codec is record-internal, not orderable"))
}
func (s leafSerializer[K, V]) Equals(a, b hleaf[K, V]) bool {
	panic(mapdb5.NewMisuse("htree leaf codec is record-internal, not orderable"))
}
func (s leafSerializer[K, V]) NaturalOrder() bool            { return false }
func (s leafSerializer[K, V]) EqualsBySerializedBytes() bool { return true }

// hHeader is the immutable map metadata record, written once at create
// (Java HTreeMap.Header).
type hHeader struct {
	concShift, dirShift, levels int32
	hashSeed                    int32
	segmentRoots                []uint64
}

// headerSerializer is the header wire codec: packInt geometry triple, a
// RAW int32 hash seed (Java writeInt — may be negative, not packed), then
// one packLong per segment root.
type headerSerializer struct{}

var _ ser.Serializer[hHeader] = headerSerializer{}

func (headerSerializer) Serialize(w *bin.Writer, h hHeader) {
	w.PackInt(h.concShift)
	w.PackInt(h.dirShift)
	w.PackInt(h.levels)
	w.WriteInt(h.hashSeed)
	for _, root := range h.segmentRoots {
		w.PackLong(int64(root))
	}
}

func (headerSerializer) Deserialize(r *bin.Reader, size int) hHeader {
	concShift := r.UnpackInt()
	dirShift := r.UnpackInt()
	levels := r.UnpackInt()
	hashSeed := r.ReadInt()
	// Untrusted wire value: bound before 1<<concShift can drive a huge
	// allocation off corrupt bytes.
	if concShift < 0 || concShift > 30 {
		panic(mapdb5.NewCorruption("htree header concShift out of range"))
	}
	roots := make([]uint64, 1<<concShift)
	for i := range roots {
		v := r.UnpackLong()
		if v <= 0 {
			panic(mapdb5.NewCorruption("htree header segment root recid out of range"))
		}
		roots[i] = uint64(v)
	}
	return hHeader{concShift: concShift, dirShift: dirShift, levels: levels,
		hashSeed: hashSeed, segmentRoots: roots}
}

func (headerSerializer) FixedSize() int { return -1 }
func (headerSerializer) SizeHint() int  { return 64 }
func (headerSerializer) Compare(a, b hHeader) int {
	panic(mapdb5.NewMisuse("htree header codec is record-internal, not orderable"))
}
func (headerSerializer) Equals(a, b hHeader) bool {
	panic(mapdb5.NewMisuse("htree header codec is record-internal, not orderable"))
}
func (headerSerializer) NaturalOrder() bool            { return false }
func (headerSerializer) EqualsBySerializedBytes() bool { return true }

var htreeStateIDs atomic.Uint64

// dbErrorToErr converts a *mapdb5.DBError panic into the named error return
// (constructor boundary, GD5); other panics propagate.
func dbErrorToErr(err *error) {
	if r := recover(); r != nil {
		if de, ok := mapdb5.AsDBError(r); ok {
			*err = de
			return
		}
		panic(r)
	}
}

// hinner is the shared map state (one HTreeMap handle == one hinner).
type hinner[K, V any] struct {
	st           store.Store
	keySer       ser.Serializer[K]
	valueSer     ser.Serializer[V]
	concShift    int
	dirShift     int
	levels       int
	headerRecid  store.Recid
	segmentRoots []uint64
	// hashOf maps a key to its UNSIGNED placement hash in
	// [0, 1<<(concShift+levels*dirShift)): uint32(Hasher.Hash) for the
	// 32-bit map, top-48 of Hasher64 for the 48-bit variant — one
	// representation lets both variants share this inner state.
	hashOf   func(K) int64
	hashSeed int64 // 32-bit maps use the low 32 bits
	// seg is the shared per-segment lock+listener machinery (seglock.go).
	seg          segRuntime[K, V]
	segmentShift int // levels*dirShift: hash bits consumed by the dir tree
	indexMask    int64
	concMask     int
	leafSer      leafSerializer[K, V]
	// closed + ops implement the Go drain-on-retire contract (Java has no
	// close primitive; its maps hold no lease): CloseHandle flips closed,
	// drains ops admitted before it, then releases the open lease.
	closed atomic.Bool
	ops    handle.Tracker
	lease  *store.LeaseGuard
	// id uniquely names this shared state for self-bind detection.
	id uint64
}

// HTreeMap is a persistent segmented hash map over a store.Store. Its zero
// value is unusable; construct via NewHTreeMap/OpenHTreeMap/
// CreateHTreeMapFromSorted. Safe for concurrent use when the backing store
// IsThreadSafe; at most one live mutating handle per (store, headerRecid)
// (enforced by the open lease — Java §0.7).
type HTreeMap[K, V any] struct {
	inner *hinner[K, V]
}

// ===================== construction =====================

// checkConfig is Java's argument validation: WrongConfiguration on
// out-of-range geometry (create-time caller error).
func checkConfig(concShift, dirShift, levels int) error {
	// hard cap 12 (4096 segments): segments are lock granularity, more was
	// measured as a cache-scatter regression (Java checkConfig).
	switch {
	case concShift < 0 || concShift > 12:
		return mapdb5.New(mapdb5.KindWrongConfiguration,
			fmt.Sprintf("concShift must be in [0,12], got %d", concShift))
	case dirShift < 1 || dirShift > MaxDirShift:
		return mapdb5.New(mapdb5.KindWrongConfiguration,
			fmt.Sprintf("dirShift must be in [1,%d], got %d", MaxDirShift, dirShift))
	case levels < 1:
		return mapdb5.New(mapdb5.KindWrongConfiguration,
			fmt.Sprintf("levels must be >= 1, got %d", levels))
	case concShift+levels*dirShift != 32:
		// full 32-bit partition; anything less silently drops hash bits
		return mapdb5.New(mapdb5.KindWrongConfiguration,
			fmt.Sprintf("concShift + levels*dirShift must equal 32, got %d + %d*%d",
				concShift, levels, dirShift))
	}
	return nil
}

// resolveConfig applies the Java defaults (4,7,4) when all three geometry
// fields are zero, else validates.
func resolveConfig[K, V any](cfg HConfig[K, V]) (concShift, dirShift, levels int, err error) {
	if cfg.ConcShift == 0 && cfg.DirShift == 0 && cfg.Levels == 0 {
		return 4, 7, 4, nil
	}
	if err := checkConfig(cfg.ConcShift, cfg.DirShift, cfg.Levels); err != nil {
		return 0, 0, 0, err
	}
	return cfg.ConcShift, cfg.DirShift, cfg.Levels, nil
}

func resolveHasher[K, V any](cfg HConfig[K, V]) mdhash.Hasher[K] {
	if cfg.Hasher != nil {
		return cfg.Hasher
	}
	if cfg.KeySer == nil {
		panic(mapdb5.NewMisuse("htree: KeySer is required"))
	}
	return mdhash.SerHasher32(cfg.KeySer)
}

func newInner[K, V any](s store.Store, cfg HConfig[K, V], headerRecid store.Recid,
	h hHeader, lease *store.LeaseGuard) *hinner[K, V] {
	hasher := resolveHasher(cfg)
	// 32-bit variant: the placement hash is the unsigned 32-bit mix.
	hashOf := func(k K) int64 {
		return int64(uint32(hasher.Hash(k, h.hashSeed)))
	}
	return newInnerBase(s, cfg.KeySer, cfg.ValueSer, headerRecid,
		h.segmentRoots, int(h.concShift), int(h.dirShift), int(h.levels),
		hashOf, int64(h.hashSeed),
		leafSerializer[K, V]{keySer: cfg.KeySer, valueSer: cfg.ValueSer}, lease)
}

// newInnerBase is the variant-agnostic inner-state constructor (32 and
// 48-bit maps differ only in the hashOf closure, seed width and header
// codec).
func newInnerBase[K, V any](s store.Store, keySer ser.Serializer[K], valueSer ser.Serializer[V],
	headerRecid store.Recid, segmentRoots []uint64, concShift, dirShift, levels int,
	hashOf func(K) int64, hashSeed int64, leafSer leafSerializer[K, V],
	lease *store.LeaseGuard) *hinner[K, V] {
	nseg := 1 << concShift
	return &hinner[K, V]{
		st:           s,
		keySer:       keySer,
		valueSer:     valueSer,
		concShift:    concShift,
		dirShift:     dirShift,
		levels:       levels,
		headerRecid:  headerRecid,
		segmentRoots: segmentRoots,
		hashOf:       hashOf,
		hashSeed:     hashSeed,
		seg:          newSegRuntime[K, V](nseg, s.IsThreadSafe()),
		segmentShift: levels * dirShift,
		indexMask:    (int64(1) << (levels * dirShift)) - 1,
		concMask:     nseg - 1,
		leafSer:      leafSer,
		id:           htreeStateIDs.Add(1),
		lease:        lease,
	}
}

// NewHTreeMap creates a fresh empty segmented hash map over s and returns
// the live handle. Panics *mapdb5.DBError{KindWrongConfiguration} on bad
// geometry, *mapdb5.DBError{KindMisuse} on missing serializers.
func NewHTreeMap[K, V any](s store.Store, cfg HConfig[K, V]) *HTreeMap[K, V] {
	m, err := createHTreeMap(s, cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func createHTreeMap[K, V any](s store.Store, cfg HConfig[K, V]) (*HTreeMap[K, V], error) {
	concShift, dirShift, levels, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree: KeySer and ValueSer are required")
	}
	seed := cfg.HashSeed
	if !cfg.HashSeedSet {
		seed = int32(rand.Uint32())
	}
	roots := make([]uint64, 1<<concShift)
	for i := range roots {
		roots[i] = store.Put(s, DirEmpty(), DirSer)
	}
	hdr := hHeader{
		concShift: int32(concShift), dirShift: int32(dirShift), levels: int32(levels),
		hashSeed: seed, segmentRoots: roots,
	}
	headerRecid := store.Put(s, hdr, headerSerializer{})
	return openHTreeMap(s, headerRecid, cfg)
}

// OpenHTreeMap reopens the map whose header record is headerRecid
// (HTreeMap.HeaderRecid). Store corruption, a damaged header, and a
// conflicting live handle are returned as errors (GD5). The Hasher in cfg
// must match the one supplied at create.
func OpenHTreeMap[K, V any](s store.Store, headerRecid store.Recid, cfg HConfig[K, V]) (m *HTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	return openHTreeMap(s, headerRecid, cfg)
}

func openHTreeMap[K, V any](s store.Store, headerRecid store.Recid, cfg HConfig[K, V]) (*HTreeMap[K, V], error) {
	if headerRecid == 0 {
		return nil, mapdb5.NewCorruption("htree header recid must be a valid recid")
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree: KeySer and ValueSer are required")
	}
	lease, err := store.AcquireLease(s, uint64(headerRecid), store.LeaseReadWrite)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			lease.Release()
		}
	}()
	hdr, ok := store.Get(s, uint64(headerRecid), headerSerializer{})
	if !ok {
		return nil, mapdb5.NewCorruption("htree header record is null")
	}
	// Stored geometry is untrusted (C2): validate as corruption, never as a
	// caller configuration error.
	if err := checkConfig(int(hdr.concShift), int(hdr.dirShift), int(hdr.levels)); err != nil {
		return nil, mapdb5.NewCorruption("htree header geometry invalid: " + err.Error())
	}
	if len(hdr.segmentRoots) != 1<<hdr.concShift {
		return nil, mapdb5.NewCorruption("htree header segment root count mismatch")
	}
	committed = true
	return &HTreeMap[K, V]{inner: newInner(s, cfg, headerRecid, hdr, lease)}, nil
}

// HeaderRecid is the recid of the header record; persist it to reopen the
// map via OpenHTreeMap.
func (m *HTreeMap[K, V]) HeaderRecid() store.Recid { return m.inner.headerRecid }

// HashSeed is the persisted placement-hash seed (read-only; set at create).
func (m *HTreeMap[K, V]) HashSeed() int32 { return int32(m.inner.hashSeed) }

// Segments is the physical segment count (1 << concShift).
func (m *HTreeMap[K, V]) Segments() int { return len(m.inner.segmentRoots) }

// IsClosed reports whether this handle was retired or the backing store
// is closed.
func (m *HTreeMap[K, V]) IsClosed() bool {
	return m.inner.closed.Load() || m.inner.st.IsClosed()
}

// KeySerializer is the key element serializer.
func (m *HTreeMap[K, V]) KeySerializer() ser.Serializer[K] { return m.inner.keySer }

// ValueSerializer is the value element serializer.
func (m *HTreeMap[K, V]) ValueSerializer() ser.Serializer[V] { return m.inner.valueSer }

// StateID uniquely identifies this map's shared state.
func (m *HTreeMap[K, V]) StateID() uint64 { return m.inner.id }

// SharesStateWith reports whether o is a handle over the SAME shared state.
func (m *HTreeMap[K, V]) SharesStateWith(o *HTreeMap[K, V]) bool {
	return m.inner == o.inner
}

// Close releases the map's open lease and invalidates the handle: every
// later op panics KindClosed. It does NOT close the backing store.
// Idempotent.
func (m *HTreeMap[K, V]) Close() error {
	m.CloseHandle()
	return nil
}

// CloseHandle is the DB-owned retirement path: invalidate the handle,
// DRAIN the ops admitted before retirement, then release the open lease
// (same contract as BTreeMap.CloseHandle).
func (m *HTreeMap[K, V]) CloseHandle() {
	if m.inner.closed.CompareAndSwap(false, true) {
		m.inner.ops.DropSelf()
		m.inner.ops.Wait()
		if l := m.inner.lease; l != nil {
			l.Release()
		}
	}
}

// opGuard admits one public operation to the lifetime drain: admitted
// BEFORE checking closed so an op racing CloseHandle is either waited on
// or rejected — never running after the lease is released.
func (m *HTreeMap[K, V]) opGuard() func() {
	if m.inner.closed.Load() {
		panicHClosed()
	}
	release := m.inner.ops.Admit()
	if m.inner.closed.Load() {
		release()
		panicHClosed()
	}
	return release
}

func (m *HTreeMap[K, V]) checkOpen() {
	if m.inner.closed.Load() {
		panicHClosed()
	}
}

func panicHClosed() {
	panic(mapdb5.New(mapdb5.KindClosed,
		"htree map handle was invalidated by close, delete or rollback"))
}

// ===================== hashing =====================

// hash is the single placement hash as an unsigned int64: the Hasher owns
// all mixing (Java §0.3); the 48-bit variant's closure yields the top 48.
func (m *HTreeMap[K, V]) hash(key K) int64 {
	return m.inner.hashOf(key)
}

func (m *HTreeMap[K, V]) segment(h int64) int {
	return int(h>>uint(m.inner.segmentShift)) & m.inner.concMask
}

func (m *HTreeMap[K, V]) index(h int64) int64 {
	return h & m.inner.indexMask
}

// ===================== locks =====================

func (m *HTreeMap[K, V]) lockRead(seg int)   { m.inner.seg.lockRead(seg) }
func (m *HTreeMap[K, V]) unlockRead(seg int) { m.inner.seg.unlockRead(seg) }

func (m *HTreeMap[K, V]) lockWrite(seg int) *deferWindow[K, V] {
	return m.inner.seg.lockWrite(seg)
}
func (m *HTreeMap[K, V]) unlockWrite(seg int, win *deferWindow[K, V]) error {
	return m.inner.seg.unlockWrite(seg, win)
}
func (m *HTreeMap[K, V]) runWrite(seg int, f func(*deferWindow[K, V]) error) error {
	return m.inner.seg.runWrite(seg, f)
}

// ===================== get =====================

// getRaw is the locked leaf read (GD6: the Java lock-free optimistic leaf
// read is NOT ported — every read takes the segment RLock).
func (m *HTreeMap[K, V]) getRaw(key K) (V, bool) {
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	m.lockRead(seg)
	defer m.unlockRead(seg)
	leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
		m.inner.levels-1, idx)
	var zero V
	if leafRecid == 0 {
		return zero, false
	}
	leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
	if !ok {
		panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
	}
	for i := range leaf.keys {
		if m.inner.keySer.Equals(leaf.keys[i], key) {
			return leaf.vals[i], true
		}
	}
	return zero, false
}

// Get returns the value for key; a missing key consults the value loader
// (Java get → load → putIfAbsent).
func (m *HTreeMap[K, V]) Get(key K) (V, bool) {
	defer m.opGuard()()
	if v, ok := m.getRaw(key); ok {
		return v, true
	}
	loaded, ok := m.inner.seg.runtime.load(key)
	if !ok || m.inner.st.IsReadOnly() {
		return loaded, ok
	}
	// The loader ran lock-free and may have retired this handle (Close
	// drops our admission): re-check before committing the loaded value.
	m.checkOpen()
	existing, exists := m.putInternal(key, loaded, true)
	if !exists {
		return loaded, true
	}
	return existing, true
}

// ContainsKey reports whether key has an entry (no value-loader consult).
func (m *HTreeMap[K, V]) ContainsKey(key K) bool {
	defer m.opGuard()()
	_, ok := m.getRaw(key)
	return ok
}

// ===================== put =====================

// Put inserts or replaces; returns the previous value and whether it
// existed.
func (m *HTreeMap[K, V]) Put(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.putInternal(key, value, false)
}

// PutIfAbsent inserts only if absent; returns the existing value and true
// when one was present, or the zero value and false when this call
// inserted.
func (m *HTreeMap[K, V]) PutIfAbsent(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.putInternal(key, value, true)
}

// PutOnly is the blind put (JCache void-put semantics): same cost as Put
// on this inline-value map; exists for API symmetry with the external
// variant.
func (m *HTreeMap[K, V]) PutOnly(key K, value V) {
	m.Put(key, value)
}

func (m *HTreeMap[K, V]) putInternal(key K, value V, onlyIfAbsent bool) (V, bool) {
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var old V
	var found bool
	err := m.runWrite(seg, func(win *deferWindow[K, V]) error {
		leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx)
		if leafRecid == 0 {
			// fresh bucket: write leaf first, then link it (Java order)
			leafRecid = int64(store.Put(m.inner.st,
				hleaf[K, V]{keys: []K{key}, vals: []V{value}}, m.inner.leafSer))
			TreePut(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
				m.inner.levels-1, idx, leafRecid)
			return m.inner.seg.runtime.fire(win, key, nil, &value, false)
		}
		leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
		if !ok {
			panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
		}
		for i := range leaf.keys {
			if m.inner.keySer.Equals(leaf.keys[i], key) { // key present
				old = leaf.vals[i]
				found = true
				if onlyIfAbsent {
					return nil
				}
				leaf2 := leaf // copy-on-write: heap stores alias the slice
				leaf2.vals = append([]V(nil), leaf.vals...)
				leaf2.vals[i] = value
				store.Update(m.inner.st, uint64(leafRecid), leaf2, m.inner.leafSer)
				return m.inner.seg.runtime.fire(win, key, &old, &value, false)
			}
		}
		// full-index hash collision: grow the bucket by one entry
		leaf.keys = append(append([]K(nil), leaf.keys...), key)
		leaf.vals = append(append([]V(nil), leaf.vals...), value)
		store.Update(m.inner.st, uint64(leafRecid), leaf, m.inner.leafSer)
		return m.inner.seg.runtime.fire(win, key, nil, &value, false)
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// ===================== remove / replace =====================

// Remove deletes the key's entry; returns the removed value and whether
// one existed.
func (m *HTreeMap[K, V]) Remove(key K) (V, bool) {
	defer m.opGuard()()
	return m.removeInternal(key, nil)
}

// RemoveIfEquals removes only if the current value Equals expected
// (valueSer equality, Java remove(key,value)).
func (m *HTreeMap[K, V]) RemoveIfEquals(key K, value V) bool {
	defer m.opGuard()()
	_, removed := m.removeInternal(key, &value)
	return removed
}

// RemoveOnly deletes the key's entry, returning only WHETHER one existed
// (JCache boolean remove).
func (m *HTreeMap[K, V]) RemoveOnly(key K) bool {
	defer m.opGuard()()
	_, removed := m.removeInternal(key, nil)
	return removed
}

// removeInternal removes; expectedValue non-nil = remove only on
// valueSer.Equals match.
func (m *HTreeMap[K, V]) removeInternal(key K, expectedValue *V) (V, bool) {
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var old V
	var found bool
	err := m.runWrite(seg, func(win *deferWindow[K, V]) error {
		leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx)
		if leafRecid == 0 {
			return nil
		}
		leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
		if !ok {
			panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
		}
		for i := range leaf.keys {
			if !m.inner.keySer.Equals(leaf.keys[i], key) {
				continue
			}
			old = leaf.vals[i]
			found = true
			if expectedValue != nil && !m.inner.valueSer.Equals(old, *expectedValue) {
				found = false
				return nil
			}
			if len(leaf.keys) == 1 {
				// last entry: unlink the bucket from the dir tree
				// (collapsing empty dirs on the way out), THEN free the
				// bucket record (Java order).
				if !TreeRemove(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
					m.inner.levels-1, idx) {
					panic(mapdb5.NewCorruption("dir tree lost the bucket it just resolved"))
				}
				store.Delete(m.inner.st, uint64(leafRecid), m.inner.leafSer)
			} else {
				// shrink the bucket by one entry
				keys := make([]K, 0, len(leaf.keys)-1)
				vals := make([]V, 0, len(leaf.vals)-1)
				keys = append(keys, leaf.keys[:i]...)
				keys = append(keys, leaf.keys[i+1:]...)
				vals = append(vals, leaf.vals[:i]...)
				vals = append(vals, leaf.vals[i+1:]...)
				store.Update(m.inner.st, uint64(leafRecid), hleaf[K, V]{keys, vals}, m.inner.leafSer)
			}
			return m.inner.seg.runtime.fire(win, key, &old, nil, false)
		}
		return nil // full-index collision but no key match
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// Replace replaces only if present; returns the previous value.
func (m *HTreeMap[K, V]) Replace(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.replaceInternal(key, nil, value)
}

// ReplaceIfEquals replaces only if the current value Equals oldValue.
func (m *HTreeMap[K, V]) ReplaceIfEquals(key K, oldValue, newValue V) bool {
	defer m.opGuard()()
	_, replaced := m.replaceInternal(key, &oldValue, newValue)
	return replaced
}

// replaceInternal replaces; expectedValue non-nil = only on Equals match.
func (m *HTreeMap[K, V]) replaceInternal(key K, expectedValue *V, newValue V) (V, bool) {
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var old V
	var found bool
	err := m.runWrite(seg, func(win *deferWindow[K, V]) error {
		leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx)
		if leafRecid == 0 {
			return nil
		}
		leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
		if !ok {
			panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
		}
		for i := range leaf.keys {
			if !m.inner.keySer.Equals(leaf.keys[i], key) {
				continue
			}
			old = leaf.vals[i]
			found = true
			if expectedValue != nil && !m.inner.valueSer.Equals(old, *expectedValue) {
				found = false
				return nil
			}
			leaf2 := leaf
			leaf2.vals = append([]V(nil), leaf.vals...)
			leaf2.vals[i] = newValue
			store.Update(m.inner.st, uint64(leafRecid), leaf2, m.inner.leafSer)
			return m.inner.seg.runtime.fire(win, key, &old, &newValue, false)
		}
		return nil // key absent
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// ===================== clear =====================

// Clear removes all entries, emitting modification callbacks.
func (m *HTreeMap[K, V]) Clear() {
	m.clearInternal(true, false)
}

// ClearWithoutNotification clears without emitting modification callbacks
// (Java clearWithoutNotification).
func (m *HTreeMap[K, V]) ClearWithoutNotification() {
	m.clearInternal(false, false)
}

// ClearWithExpire clears and marks every removal callback as
// automatically triggered (Java clearWithExpire).
func (m *HTreeMap[K, V]) ClearWithExpire() {
	m.clearInternal(true, true)
}

// clearInternal — Java: a throwing LISTENER must not abort the loop and
// leave later segments populated (their segment's removal already
// committed): capture, keep clearing, rethrow after. A MUTATION/store
// failure must abort: the current segment is inconsistent, continuing
// would destructively erase healthy later segments.
func (m *HTreeMap[K, V]) clearInternal(notify, triggered bool) {
	defer m.opGuard()()
	var listenerFail error
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		// A listener firing for an earlier segment may have retired this
		// handle (Close drops our admission): the retired op must not keep
		// mutating under a reopened handle's independent lock domain.
		m.checkOpen()
		win := m.lockWrite(seg)
		// collect first, fire only AFTER the whole traversal commits:
		// treeClear detaches the root before visiting, so a throwing
		// listener inside the visitor would orphan unvisited subtrees.
		var removed []hevent[K, V]
		var traverseErr any
		func() {
			defer func() {
				if r := recover(); r != nil {
					traverseErr = r
				}
			}()
			TreeClear(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
				func(index, leafRecid int64) {
					if notify {
						leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
						if !ok {
							panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
						}
						for i := range leaf.keys {
							v := leaf.vals[i]
							removed = append(removed, hevent[K, V]{
								key: leaf.keys[i], old: &v, trig: triggered})
						}
					}
					store.Delete(m.inner.st, uint64(leafRecid), m.inner.leafSer)
				})
		}()
		if traverseErr != nil {
			// mutation/store failure: deliver this window's events, then
			// abort with the traversal panic primary (listenerFail
			// suppressed behind it — Java clearInternal).
			derr := m.unlockWrite(seg, win)
			if e, ok := traverseErr.(error); ok {
				if derr != nil {
					e = errors.Join(e, derr)
				}
				panic(errors.Join(e, listenerFail))
			}
			panic(traverseErr)
		}
		if notify {
			if err := m.inner.seg.runtime.fireRemovalBatch(win, removed, triggered, false); err != nil {
				listenerFail = suppressErr(listenerFail, err)
			}
		}
		if err := m.unlockWrite(seg, win); err != nil {
			listenerFail = suppressErr(listenerFail, err)
		}
	}
	if listenerFail != nil {
		panic(listenerFail)
	}
}

// ===================== size =====================

// IsEmpty reports whether the map has no entries — O(segments), reading
// only each root's bitmaps (Java isEmpty).
func (m *HTreeMap[K, V]) IsEmpty() bool {
	defer m.opGuard()()
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		empty := func() bool {
			m.lockRead(seg)
			defer m.unlockRead(seg) // released on panic paths too
			return TreeIsEmpty(m.inner.segmentRoots[seg], m.inner.st)
		}()
		if !empty {
			return false
		}
	}
	return true
}

// LenLong is the exact entry count: bucket sizes summed per segment under
// its read lock (Java sizeLong).
func (m *HTreeMap[K, V]) LenLong() int64 {
	defer m.opGuard()()
	var total int64
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		func() {
			m.lockRead(seg)
			defer m.unlockRead(seg) // released on panic paths too
			TreeFold(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
				func(index, leafRecid int64) {
					leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
					if !ok {
						panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
					}
					total += int64(len(leaf.keys))
				})
		}()
	}
	return total
}

// Len saturates at math.MaxInt; use LenLong for the exact count (Java size).
func (m *HTreeMap[K, V]) Len() int {
	n := m.LenLong()
	if n > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(n)
}

// ===================== iteration =====================

// drainSegment snapshots one segment's entries under its read lock
// (Java drainSegment).
func (m *HTreeMap[K, V]) drainSegment(seg int) hleaf[K, V] {
	var out hleaf[K, V]
	m.lockRead(seg)
	defer m.unlockRead(seg)
	TreeFold(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
		func(index, leafRecid int64) {
			leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
			if !ok {
				panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
			}
			out.keys = append(out.keys, leaf.keys...)
			out.vals = append(out.vals, leaf.vals...)
		})
	return out
}

// yieldSnapshot yields one drained segment; returns false when yield
// stopped. The retired-handle check between entries matches BTreeMap's
// iterator discipline — a stale handle must not keep serving after close.
func (m *HTreeMap[K, V]) yieldSnapshot(snap hleaf[K, V], yield func(K, V) bool) bool {
	for i := range snap.keys {
		if !yield(snap.keys[i], snap.vals[i]) {
			return false
		}
		m.checkOpen()
	}
	return true
}

// All is the unordered entry iterator: snapshot-per-segment — each
// segment is drained under its read lock as iteration reaches it, then
// yielded lock-free (weakly consistent across segments; the callback may
// mutate the map).
func (m *HTreeMap[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
			// Admission is scoped to the segment SNAPSHOT, not the whole
			// iteration: holding it across yields would deadlock Close on a
			// suspended pull iterator (the consumer can only resume — and
			// thus release — after Close returns).
			release := m.opGuard()
			snap := m.drainSegment(seg)
			release()
			if !m.yieldSnapshot(snap, yield) {
				return
			}
		}
	}
}

// Keys iterates keys (snapshot-per-segment).
func (m *HTreeMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.All()(func(k K, _ V) bool { return yield(k) })
	}
}

// Values iterates values (snapshot-per-segment).
func (m *HTreeMap[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.All()(func(_ K, v V) bool { return yield(v) })
	}
}

// ForEach runs the callback OUTSIDE segment locks (Java forEach); the
// callback may mutate the map.
func (m *HTreeMap[K, V]) ForEach(f func(K, V)) {
	m.All()(func(k K, v V) bool { f(k, v); return true })
}

// Segments2 returns k <= max(1, n) non-overlapping weakly consistent views
// whose concatenation covers All() — the parallel-scan contract (physical
// segments coalesced into at most n views; n <= 0 or an empty map returns
// exactly one possibly-empty view).
func (m *HTreeMap[K, V]) Segments2(n int) []iter.Seq2[K, V] {
	nseg := len(m.inner.segmentRoots)
	// The partition contract demands exactly one view for n <= 0 or an
	// empty map — check every root's bitmaps under read locks.
	if n < 1 || m.allRootsEmpty() {
		n = 1
	}
	k := min(n, nseg)
	per := (nseg + k - 1) / k
	views := make([]iter.Seq2[K, V], 0, k)
	for lo := 0; lo < nseg; lo += per {
		hi := min(lo+per, nseg)
		lo, hi := lo, hi
		views = append(views, func(yield func(K, V) bool) {
			for seg := lo; seg < hi; seg++ {
				release := m.opGuard() // snapshot-scoped, see All
				snap := m.drainSegment(seg)
				release()
				if !m.yieldSnapshot(snap, yield) {
					return
				}
			}
		})
	}
	return views
}

// allRootsEmpty reports whether every segment root holds no entries
// (read-locked bitmap checks; lock released on panic paths too).
func (m *HTreeMap[K, V]) allRootsEmpty() bool {
	defer m.opGuard()()
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		empty := func() bool {
			m.lockRead(seg)
			defer m.unlockRead(seg)
			return TreeIsEmpty(m.inner.segmentRoots[seg], m.inner.st)
		}()
		if !empty {
			return false
		}
	}
	return true
}

// ===================== listeners / loader =====================

// ModificationListenerAdd registers a listener (order-sensitive
// SynchronousMapModificationListener fires under the segment write lock;
// ordinary listeners after it).
func (m *HTreeMap[K, V]) ModificationListenerAdd(l mapdb5.MapModificationListener[K, V]) {
	defer m.opGuard()()
	m.inner.seg.runtime.add(l)
}

// ModificationListenerRemove unregisters a listener.
func (m *HTreeMap[K, V]) ModificationListenerRemove(l mapdb5.MapModificationListener[K, V]) {
	m.inner.seg.runtime.remove(l)
}

// ValueLoader installs the missing-key loader consulted by Get (Java
// valueLoader). nil clears it.
func (m *HTreeMap[K, V]) ValueLoader(f func(K) (V, bool)) {
	if f == nil {
		m.inner.seg.runtime.valueLoader.Store(nil)
		return
	}
	m.inner.seg.runtime.valueLoader.Store(&f)
}

// ===================== verify =====================

// Verify walks every segment's dir tree and checks structural
// consistency: each bucket resolves and is non-empty, and every key's
// placement hash routes to the segment and stored index that hold it.
func (m *HTreeMap[K, V]) Verify() {
	defer m.opGuard()()
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		seg := seg
		func() {
			m.lockRead(seg)
			defer m.unlockRead(seg) // released on corruption panic too
			TreeFold(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
				func(storedIndex, leafRecid int64) {
					leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
					if !ok {
						panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
					}
					if len(leaf.keys) == 0 {
						panic(mapdb5.NewCorruption("empty bucket record"))
					}
					for i := range leaf.keys {
						h := m.hash(leaf.keys[i])
						if m.segment(h) != seg {
							panic(mapdb5.NewCorruption(
								"bucket key hashes to a different segment"))
						}
						if m.index(h) != storedIndex {
							panic(mapdb5.NewCorruption(
								"bucket key hash index does not match its dir-tree index"))
						}
					}
				})
		}()
	}
}
