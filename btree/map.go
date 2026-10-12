package btree

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/internal/handle"
	"github.com/mapdb/mapdb5-go/internal/listen"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// Legal MaxNodeSize range (Java requires >= 4; the port caps the top so a
// create can never persist a value the reopen validator would reject).
const (
	MinMaxNodeSize = 4
	MaxMaxNodeSize = 1 << 20
)

// Config configures a BTreeMap (spec 03). KeyFormat and ValueFormat own the
// packed node groups; their element serializers handle singleton records
// (root pointer, counter, external values).
type Config[K, V any] struct {
	KeyFormat      ser.GroupFormat[K]
	ValueFormat    ser.GroupFormat[V]
	MaxNodeSize    int
	Counter        bool        // create: allocate an O(1) LenLong counter record
	CounterRecid   store.Recid // reopen: recid of the counter record (0 = none)
	ExternalValues bool        // leaves hold value recids, not value bytes
}

// btreeInner is the shared map state.
type btreeInner[K, V any] struct {
	st             store.Store
	kf             ser.GroupFormat[K]
	vf             ser.GroupFormat[V]
	maxNodeSize    int
	rootRecidRecid uint64
	valueInline    bool
	counterRecid   uint64
	// External-value read barrier: readers hold RLock across the
	// value-record store.Get; remove holds Lock across the whole delete, so
	// a lock-free reader can never observe a value recid that a concurrent
	// remove deleted and the store reused.
	externalLock sync.RWMutex
	// externalOwner is the goid currently holding the external WRITE
	// barrier through a sync-listener fire (external-value remove). Go's
	// RWMutex is non-reentrant, so a listener re-entering the map on the
	// same goroutine would block on externalLock before ever reaching the
	// node-lock tripwire — a deadlock with the barrier held. Every
	// externalLock acquisition checks this first and panics instead; the
	// panic unwinds through the listener into invokeListener's recovery.
	externalOwner atomic.Uint64
	rootCacheable bool
	locks         *nodeLockTable
	// Per-level left-edge recid, index 0 = leaf level, last = root (Java
	// volatile long[]). Immutable slice republished copy-on-write.
	leftEdges atomic.Pointer[[]uint64]
	// Cached root recid (0 = not loaded). A stale value is harmless — an
	// old root still covers the whole key space via right links.
	cachedRoot atomic.Uint64
	// Set if a split published a child but failed to complete upward
	// propagation (e.g. a store I/O error mid root-grow): every op fails
	// fast instead of parking forever on a never-created left-edge level.
	poisoned atomic.Bool
	// Last store.StructuralGeneration the leftEdges cache was consistent
	// with; a mismatch triggers a one-shot rebuild (tx rollback can shrink
	// the tree out-of-band).
	lastStructGen atomic.Uint64
	// Modification listeners (runtime-only; see listener.go): syncLs fires
	// under the covering leaf lock, deferredLs after release. Both are
	// immutable slices republished copy-on-write under listenersMu.
	listenersMu sync.Mutex
	syncLs      atomic.Pointer[[]mapdb5.MapModificationListener[K, V]]
	deferredLs  atomic.Pointer[[]mapdb5.MapModificationListener[K, V]]
	lease       *store.LeaseGuard
	// closed marks a handle the owning DB retired (delete, rollback or
	// close): every later op fails fast. Without it a stale handle keeps
	// writing records a delete cleared — or, after the lease was released,
	// runs in a second lock domain alongside a fresh open of the same tree
	// (Java invalidates runtime handles for the same reason; its maps have
	// no close primitive only because they hold no lease).
	closed atomic.Bool
	// ops is the operation-lifetime drain — Java Atomic's handleLock
	// generalized over the whole tree. CloseHandle flips closed and waits
	// for every op admitted before it, so a retired handle can never keep
	// writing a rewound tree or race a fresh open's second lock domain.
	ops handle.Tracker
	// id uniquely names this shared state for self-bind detection (Bind
	// rejects a secondary that IS the primary).
	id uint64
}

// btreeStateIDs allocates the per-inner-state identity used by StateID.
var btreeStateIDs atomic.Uint64

// BTreeMap is a persistent B-link tree map over a store.Store. Its zero
// value is unusable; construct via NewBTreeMap/OpenBTreeMap/CreateFromSorted.
// Safe for concurrent use when the backing store IsThreadSafe.
type BTreeMap[K, V any] struct {
	inner *btreeInner[K, V]
}

func (m *BTreeMap[K, V]) kf() ser.GroupFormat[K] { return m.inner.kf }
func (m *BTreeMap[K, V]) vf() ser.GroupFormat[V] { return m.inner.vf }

func (m *BTreeMap[K, V]) nodeSer() nodeSerializer[K, V] {
	return newNodeSerializer(m.inner.kf, m.inner.vf, m.inner.maxNodeSize, m.inner.valueInline)
}

// RootRecidRecid is the recid of the root-pointer record; persist it to
// reopen the map via OpenBTreeMap.
func (m *BTreeMap[K, V]) RootRecidRecid() store.Recid { return m.inner.rootRecidRecid }

// CounterRecid is the recid of the O(1) size-counter record, or 0 when the
// counter is disabled.
func (m *BTreeMap[K, V]) CounterRecid() store.Recid { return m.inner.counterRecid }

// ValueInline reports whether values are encoded inside leaf nodes (false =
// external value records).
func (m *BTreeMap[K, V]) ValueInline() bool { return m.inner.valueInline }

// MaxNodeSize is the per-node key ceiling.
func (m *BTreeMap[K, V]) MaxNodeSize() int { return m.inner.maxNodeSize }

// IsClosed reports whether the backing store is closed.
func (m *BTreeMap[K, V]) IsClosed() bool {
	return m.inner.closed.Load() || m.inner.st.IsClosed()
}

// KeySerializer is the key element serializer.
func (m *BTreeMap[K, V]) KeySerializer() ser.Serializer[K] { return m.kf().Element() }

// ValueSerializer is the value element serializer.
func (m *BTreeMap[K, V]) ValueSerializer() ser.Serializer[V] { return m.vf().Element() }

// Close releases the map's open lease and invalidates the handle: every
// later op panics KindClosed. Releasing the lease while leaving the handle
// usable would let a fresh open of the same tree run in a second lock
// domain alongside this one. It does NOT close the backing store.
// Idempotent.
func (m *BTreeMap[K, V]) Close() error {
	m.CloseHandle()
	return nil
}

// CloseHandle is the DB-owned retirement path (Java DB.closeRuntimeHandle):
// invalidate the handle, DRAIN the ops admitted before retirement, then
// release the open lease. dropSelf keeps retirement invoked from inside an
// op (a listener calling Close) from waiting on itself — the outer op then
// fails fast at its next funnel check.
func (m *BTreeMap[K, V]) CloseHandle() {
	if m.inner.closed.CompareAndSwap(false, true) {
		m.inner.ops.DropSelf()
		m.inner.ops.Wait()
		if l := m.inner.lease; l != nil {
			l.Release()
		}
	}
}

// opGuard admits one public operation to the lifetime drain (the shared
// handle.Tracker — a per-goroutine admission set behind a WaitGroup, so a
// sync listener re-entering the map shares the outer admission instead of
// deadlocking on it). Admitting
// BEFORE checking closed is the ordering that matters: an op admitted
// before CloseHandle flips closed is waited on; one admitted after is
// rejected by the second check — so no admitted op ever touches the store
// after the lease is released.
func (m *BTreeMap[K, V]) opGuard() func() {
	if m.inner.closed.Load() {
		panicClosed()
	}
	release := m.inner.ops.Admit()
	if m.inner.closed.Load() {
		release()
		panicClosed()
	}
	return release
}

func panicClosed() {
	panic(mapdb5.New(mapdb5.KindClosed,
		"btree map handle was invalidated by DB delete, rollback or close"))
}

// checkOpen fails fast on a DB-retired handle (Java checkOpen). Called at
// every op's store funnel: rootRecid/loadRootRecid/load cover all tree
// descents and iterator resumes, LenLong's direct counter read, and
// listener registration.
func (m *BTreeMap[K, V]) checkOpen() {
	if m.inner.closed.Load() {
		panicClosed()
	}
}

// StateID uniquely identifies this map's shared state — two handles over
// the same btreeInner report the same id (Bind's self-cycle detection).
func (m *BTreeMap[K, V]) StateID() uint64 { return m.inner.id }

// SharesStateWith reports whether o is a handle over the SAME shared state
// (Java's object-identity equivalent for bind self-cycle rejection).
func (m *BTreeMap[K, V]) SharesStateWith(o *BTreeMap[K, V]) bool {
	return m.inner == o.inner
}

// ===================== construction =====================

// NewBTreeMap creates a fresh empty tree over s. Panics
// *mapdb5.DBError{KindWrongConfiguration} on an out-of-range MaxNodeSize.
func NewBTreeMap[K, V any](s store.Store, cfg Config[K, V]) *BTreeMap[K, V] {
	m, err := createMode(s, cfg, !cfg.ExternalValues)
	if err != nil {
		panic(err)
	}
	return m
}

// OpenBTreeMap reopens the map whose root-pointer recid is headerRecid
// (BTreeMap.RootRecidRecid). Store corruption, an unknown/damaged header,
// and a conflicting live handle are returned as errors (GD5). Set
// Config.CounterRecid to reattach a size counter created with
// Config.Counter (read it back from BTreeMap.CounterRecid after create).
func OpenBTreeMap[K, V any](s store.Store, headerRecid store.Recid, cfg Config[K, V]) (m *BTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	return openModeErr(s, headerRecid, cfg, cfg.CounterRecid, !cfg.ExternalValues)
}

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

// createMode allocates the empty root leaf, the root-pointer record, and
// optionally the size counter, then opens.
func createMode[K, V any](s store.Store, cfg Config[K, V], valueInline bool) (*BTreeMap[K, V], error) {
	if cfg.MaxNodeSize < MinMaxNodeSize || cfg.MaxNodeSize > MaxMaxNodeSize {
		return nil, mapdb5.New(mapdb5.KindWrongConfiguration, "maxNodeSize must be in 4..=1048576")
	}
	if cfg.KeyFormat == nil || (cfg.ValueFormat == nil && valueInline) {
		return nil, mapdb5.New(mapdb5.KindWrongConfiguration, "nil group format")
	}
	ns := newNodeSerializer(cfg.KeyFormat, cfg.ValueFormat, cfg.MaxNodeSize, valueInline)
	empty := node{flags: flagLeft | flagRight, keys: cfg.KeyFormat.Empty()}
	if valueInline {
		empty.values = cfg.ValueFormat.Empty()
	} else {
		empty.recids = []int64{}
	}
	rootRecid := store.Put(s, empty, ns)
	rrr := store.Put(s, int64(rootRecid), ser.Long)
	var counterRecid uint64
	if cfg.Counter {
		counterRecid = store.Put(s, int64(0), ser.Long)
	}
	return openModeErr(s, rrr, cfg, counterRecid, valueInline)
}

// openModeErr builds the shared map state: validates the stored config,
// acquires the open lease, validates the counter record, and builds the
// left-edges spine.
func openModeErr[K, V any](s store.Store, rrr uint64, cfg Config[K, V], counterRecid uint64, valueInline bool) (*BTreeMap[K, V], error) {
	// maxNodeSize reaches this path from the stored catalog on reopen; a
	// hostile/corrupt out-of-range value must be a corruption error, never
	// a panic (C2).
	if cfg.MaxNodeSize < MinMaxNodeSize || cfg.MaxNodeSize > MaxMaxNodeSize {
		return nil, mapdb5.NewCorruption("stored maxNodeSize out of range 4..=1048576")
	}
	if rrr == 0 {
		return nil, mapdb5.NewCorruption("root_recid_recid must be a valid recid")
	}
	lease, err := store.AcquireLease(s, rrr, store.LeaseReadWrite)
	if err != nil {
		return nil, err
	}
	// On any failure or panic before the map is published the lease must go
	// back — otherwise a failed open leaves the root pointer record locked
	// and every subsequent open wrongly reports the map as in use.
	committed := false
	defer func() {
		if !committed {
			lease.Release()
		}
	}()
	inner := &btreeInner[K, V]{
		st:             s,
		kf:             cfg.KeyFormat,
		vf:             cfg.ValueFormat,
		maxNodeSize:    cfg.MaxNodeSize,
		rootRecidRecid: rrr,
		valueInline:    valueInline,
		counterRecid:   counterRecid,
		rootCacheable:  !s.IsTx(),
		locks:          newNodeLockTable(s.IsThreadSafe()),
		lease:          lease,
		id:             btreeStateIDs.Add(1),
	}
	inner.lastStructGen.Store(s.StructuralGeneration())
	m := &BTreeMap[K, V]{inner: inner}
	// Validate the O(1) counter record up front: it must exist and be
	// non-negative, so a bad counter recid is rejected at open — before the
	// first mutation commits against a phantom/negative counter.
	if counterRecid != 0 {
		v, ok := store.Get(s, counterRecid, ser.Long)
		if !ok {
			return nil, mapdb5.NewCorruption("btree size counter record missing")
		}
		if v < 0 {
			return nil, mapdb5.NewCorruption("btree size counter record is negative")
		}
	}
	edges := m.buildLeftEdges()
	es := edges
	inner.leftEdges.Store(&es)
	committed = true
	return m, nil
}

// buildLeftEdges walks the leftmost spine root→leaf; result index 0 = leaf.
func (m *BTreeMap[K, V]) buildLeftEdges() []uint64 {
	cyc := newCycleGuard(cycleDescentSoft)
	var spine []uint64 // root-first
	current := m.loadRootRecid()
	spine = append(spine, current)
	n := m.load(current)
	// A healthy tree's root node is ALWAYS both LEFT and RIGHT. The one way
	// the root pointer names a node that isn't is a root-grow that published
	// the child split but failed before updating the pointer — detect that
	// unrecoverable partial state at open instead of parking a later writer.
	if n.flags&(flagLeft|flagRight) != flagLeft|flagRight {
		panic(mapdb5.NewCorruption("btree root is not root-shaped (incomplete root-grow); store is damaged"))
	}
	for n.isDir() {
		cyc.visit(current)
		current = n.children[0] // deserialize guarantees a dir has >=1 child
		spine = append(spine, current)
		n = m.load(current)
	}
	// leaf-first
	for i, j := 0, len(spine)-1; i < j; i, j = i+1, j-1 {
		spine[i], spine[j] = spine[j], spine[i]
	}
	return spine
}

func (m *BTreeMap[K, V]) loadRootRecid() uint64 {
	m.checkOpen()
	r, ok := store.Get(m.inner.st, m.inner.rootRecidRecid, ser.Long)
	if !ok {
		panic(mapdb5.NewCorruption("btree root pointer is null"))
	}
	// A crafted root pointer of 0 / negative would corrupt the next load;
	// reject it here (recids are positive).
	if r <= 0 {
		panic(mapdb5.NewCorruption("btree root pointer is not a valid recid"))
	}
	return uint64(r)
}

// refreshLeftEdgesIfTx resyncs the leftEdges structural cache with the
// tx-visible tree when a rollback may have shrunk it. Gated on
// store.StructuralGeneration — a cheap load-and-compare on the common
// (no-rollback) path; no-op for non-tx stores.
func (m *BTreeMap[K, V]) refreshLeftEdgesIfTx() {
	if m.inner.rootCacheable {
		return // non-tx: append-only cache is authoritative
	}
	gen := m.inner.st.StructuralGeneration()
	if gen == m.inner.lastStructGen.Load() {
		return
	}
	m.checkPoison()
	edges := m.buildLeftEdges()
	m.inner.leftEdges.Store(&edges)
	m.inner.lastStructGen.Store(gen)
}

func (m *BTreeMap[K, V]) checkPoison() {
	if m.inner.poisoned.Load() {
		panic(mapdb5.New(mapdb5.KindPoisoned, "btree poisoned by a failed structural update; reopen the store (a root-grow failure surfaces as corruption at open)"))
	}
}

// isCurrentRoot authoritatively answers "is recid the current root?" by
// reading the root-pointer record FRESH (not the possibly-stale cache).
// Root growth is gated on this AND the node's LEFT|RIGHT flags: the flag
// test is the concurrency serialization; this identity test rejects a
// crafted descendant falsely flagged root-shaped.
func (m *BTreeMap[K, V]) isCurrentRoot(recid uint64) bool {
	return m.loadRootRecid() == recid
}

// rootRecid is where every op starts; poison + closed checks here fail all
// ops fast.
func (m *BTreeMap[K, V]) rootRecid() uint64 {
	m.checkOpen()
	m.checkPoison()
	if r := m.inner.cachedRoot.Load(); r != 0 {
		return r
	}
	r := m.loadRootRecid()
	if m.inner.rootCacheable {
		m.inner.cachedRoot.Store(r)
	}
	return r
}

func (m *BTreeMap[K, V]) load(recid uint64) node {
	m.checkOpen()
	n, ok := store.Get(m.inner.st, recidOrCorrupt(recid), m.nodeSer())
	if !ok {
		panic(mapdb5.NewCorruption("btree node record is null"))
	}
	return n
}

func (m *BTreeMap[K, V]) lockGuard(recid uint64) nodeGuard {
	m.inner.locks.lock(recid)
	return nodeGuard{table: m.inner.locks, recid: recid, held: true}
}

// ===================== read path (push-down) =====================

// getAction is the lock-free push-down lookup action (Java BTreeMap$get
// RecordRead). The store resolves each node under its own locks and
// dispatches exactly one method per level.
type getAction[K, V any] struct {
	store.RecordReadBase
	key         K
	kf          ser.GroupFormat[K]
	vf          ser.GroupFormat[V]
	valueInline bool
	value       V
	storedRecid int64
	found       bool
}

func (a *getAction[K, V]) OnBytes(r *bin.Reader, size int) int64 {
	a.found = false
	a.storedRecid = 0
	var zero V
	a.value = zero
	h := r.UnpackInt()
	flags := h & 0xF
	keysLen := int(uint32(h) >> 4)
	// every key occupies >= 1 serialized byte, so keysLen > size is corrupt
	if keysLen > size {
		panic(mapdb5.NewCorruption("node header keysLen exceeds record size"))
	}
	// OnBytes bypasses nodeSerializer.Deserialize, so it must enforce the
	// same recid invariants (nonzero link when !RIGHT; nonzero dir children).
	var link uint64
	if flags&flagRight == 0 {
		l := r.UnpackLong()
		if l == 0 {
			panic(mapdb5.NewCorruption("non-rightmost node with zero link"))
		}
		link = uint64(l)
	}
	var pos int
	var found bool
	if a.kf.SupportsBinary() {
		pos, found = a.kf.BytesSearch(r, keysLen, a.key)
	} else {
		g := a.kf.DeserializeGroup(r, keysLen)
		pos, found = a.kf.Search(g, a.key)
	}
	if flags&flagDir != 0 {
		childCount := keysLen
		if flags&flagRight != 0 {
			childCount++
		}
		if childCount == 0 {
			panic(mapdb5.NewCorruption("directory node with no children"))
		}
		if pos >= childCount {
			return int64(link) // beyond high bound: right sibling (nonzero)
		}
		r.UnpackLongSkip(pos)
		child := r.UnpackLong()
		if child == 0 {
			panic(mapdb5.NewCorruption("directory child recid is zero"))
		}
		return child
	}
	// leaf
	if found {
		a.found = true
		if a.valueInline {
			if a.vf.SupportsBinary() {
				a.value = a.vf.BytesGet(r, keysLen, pos)
			} else {
				g := a.vf.DeserializeGroup(r, keysLen)
				a.value = a.vf.Get(g, pos)
			}
		} else {
			// external leaf: value slots are 8-byte-BE recids (LongFormat)
			a.storedRecid = ser.LongFormat.BytesGet(r, keysLen, pos)
		}
		return 0
	}
	if pos >= keysLen && link != 0 {
		return int64(link)
	}
	return 0
}

func (a *getAction[K, V]) OnObject(obj any) int64 {
	a.found = false
	a.storedRecid = 0
	var zero V
	a.value = zero
	n, ok := obj.(node)
	if !ok {
		panic(mapdb5.NewCorruption("btree getAction: object is not a node"))
	}
	pos, found := a.kf.Search(n.keys, a.key)
	if n.isDir() {
		if pos >= len(n.children) {
			return int64(n.link)
		}
		return int64(n.children[pos])
	}
	if found {
		a.found = true
		if a.valueInline {
			a.value = a.vf.Get(n.values, pos)
		} else {
			a.storedRecid = n.recids[pos]
		}
		return 0
	}
	if pos >= a.kf.Size(n.keys) && n.link != 0 {
		return int64(n.link)
	}
	return 0
}

// OnNull: a dir child / leaf link / root recid resolving to a
// null/preallocated record is a structurally impossible tree, not a "key
// absent" answer (D5).
func (a *getAction[K, V]) OnNull() int64 {
	panic(mapdb5.NewCorruption("btree node record is null"))
}

// doGet drives the descent; the returned flag says whether the key is
// present (the value side may still need external expansion).
func (m *BTreeMap[K, V]) descendGet(action *getAction[K, V]) {
	release := m.opGuard()
	defer release()
	cyc := newCycleGuard(cycleDescentSoft)
	current := m.rootRecid()
	for current != 0 {
		cyc.visit(current)
		current = uint64(m.inner.st.Read(recidOrCorrupt(current), action))
	}
}

// Get returns the value for key.
func (m *BTreeMap[K, V]) Get(key K) (V, bool) {
	// Admission spans the WHOLE op including external-value expansion:
	// descendGet's own guard ends at the descent, and a post-descent
	// store.Get must still be inside the drain or a retiring Close could
	// release the lease and let a reopened handle reuse the value recid.
	release := m.opGuard()
	defer release()
	if !m.inner.valueInline {
		// Hold the reclamation read barrier across descent + expansion: a
		// concurrent remove (write lock) can never delete and let the store
		// reuse the recid we just read.
		m.checkExternalReentry()
		m.inner.externalLock.RLock()
		defer m.inner.externalLock.RUnlock()
	}
	action := getAction[K, V]{key: key, kf: m.kf(), vf: m.vf(), valueInline: m.inner.valueInline}
	m.descendGet(&action)
	if !action.found {
		var zero V
		return zero, false
	}
	if m.inner.valueInline {
		return action.value, true
	}
	// Expand the captured value recid (still under the read barrier). A
	// missing record is corruption, NOT "key absent" — the barrier keeps a
	// concurrent remove out.
	if action.storedRecid == 0 {
		panic(mapdb5.NewCorruption("external get: no value recid captured"))
	}
	v, ok := store.Get(m.inner.st, recidOrCorrupt(uint64(action.storedRecid)), m.vf().Element())
	if !ok {
		panic(mapdb5.NewCorruption("external value record missing"))
	}
	return v, true
}

// ContainsKey reports whether key is present.
func (m *BTreeMap[K, V]) ContainsKey(key K) bool {
	action := getAction[K, V]{key: key, kf: m.kf(), vf: m.vf(), valueInline: m.inner.valueInline}
	m.descendGet(&action)
	return action.found
}

// ===================== writer helpers =====================

// routeChild picks the child index within a dir node (the found position or
// the insertion point); -1 = key beyond this node's bound, follow link.
func (m *BTreeMap[K, V]) routeChild(dir *node, key *K) int {
	pos, _ := m.kf().Search(dir.keys, *key)
	if pos >= len(dir.children) {
		return -1
	}
	return pos
}

// beyondLeaf: true when key lies beyond this leaf's inclusive fence.
func (m *BTreeMap[K, V]) beyondLeaf(leaf *node, key *K) bool {
	if leaf.isRight() || leaf.fence == nil {
		return false
	}
	// search on the 1-element fence group: insertion point 1 == key > fence.
	pos, found := m.kf().Search(leaf.fence, *key)
	return !found && pos == 1
}

// beyondDir: true when key lies beyond this dir's last key (its bound).
func (m *BTreeMap[K, V]) beyondDir(dir *node, key *K) bool {
	pos, found := m.kf().Search(dir.keys, *key)
	return !found && pos == m.kf().Size(dir.keys)
}

// lockCovering locks recid, loads it, and moves right (hand-over-hand, one
// lock held) until the node covers key. Returns the covering node with its
// lock HELD by the caller; on a panic (corrupt load, I/O failure) the lock
// is released before the panic propagates.
func (m *BTreeMap[K, V]) lockCovering(recid uint64, key *K, dirLevel bool) (node, nodeGuard) {
	cyc := newCycleGuard(cycleDescentSoft)
	guard := m.lockGuard(recid)
	defer func() {
		if r := recover(); r != nil {
			guard.release()
			panic(r)
		}
	}()
	n := m.load(guard.recid)
	for {
		var beyond bool
		if dirLevel {
			beyond = !n.isRight() && m.beyondDir(&n, key)
		} else {
			beyond = m.beyondLeaf(&n, key)
		}
		if !beyond {
			break
		}
		next := n.link // nonzero: a non-right node has a validated link
		cyc.visit(next)
		guard.release() // one lock at a time: release current, then lock next
		guard = m.lockGuard(next)
		n = m.load(next)
	}
	return n, guard
}

// lockLeaf descends unlocked to the leaf routing key, then lockCovering to
// the real owner. The caller MUST release the returned guard. stack, when
// non-nil, is filled with the covered parent path for split propagation.
func (m *BTreeMap[K, V]) lockLeaf(key *K, stack *[]uint64) (node, nodeGuard) {
	cyc := newCycleGuard(cycleDescentSoft)
	current := m.rootRecid()
	n := m.load(current)
	for n.isDir() {
		cyc.visit(current)
		if ci := m.routeChild(&n, key); ci < 0 {
			current = n.link
		} else {
			if stack != nil {
				*stack = append(*stack, current)
			}
			current = n.children[ci]
		}
		n = m.load(current)
	}
	return m.lockCovering(current, key, false)
}

// leafValueAt reads the value at leaf position pos, expanding a value recid
// via the store in external mode. Callers on the read path hold
// externalLock.RLock.
func (m *BTreeMap[K, V]) leafValueAt(n *node, pos int) V {
	if m.inner.valueInline {
		return m.vf().Get(n.values, pos)
	}
	if n.isDir() {
		panic(mapdb5.NewCorruption("value access on a directory node"))
	}
	v, ok := store.Get(m.inner.st, recidOrCorrupt(uint64(n.recids[pos])), m.vf().Element())
	if !ok {
		panic(mapdb5.NewCorruption("external value record missing"))
	}
	return v
}

// leafVals is a freshly-built leaf value group in either representation,
// so split/insert paths handle inline values and external recids uniformly.
type leafVals struct {
	inline ser.Group
	recids []int64
}

// publishLeafSet publishes an in-place value SET at pos. Inline: rewrite
// the leaf node. External: update the value record; the node is unchanged.
func (m *BTreeMap[K, V]) publishLeafSet(current uint64, n *node, pos int, value V) {
	if m.inner.valueInline {
		if n.isDir() {
			panic(mapdb5.NewCorruption("value set on a directory node"))
		}
		updated := node{flags: n.flags, link: n.link, keys: n.keys,
			values: m.vf().Set(n.values, pos, value), fence: n.fence}
		m.storeUpdate(current, &updated)
		return
	}
	if n.isDir() {
		panic(mapdb5.NewCorruption("value set on a directory node"))
	}
	store.Update(m.inner.st, recidOrCorrupt(uint64(n.recids[pos])), value, m.vf().Element())
}

// insertLeafVals builds the new value group after inserting value at ip,
// allocating an external value record when needed.
func (m *BTreeMap[K, V]) insertLeafVals(n *node, ip int, value V) leafVals {
	if m.inner.valueInline {
		if n.isDir() {
			panic(mapdb5.NewCorruption("value insert on a directory node"))
		}
		return leafVals{inline: m.vf().Insert(n.values, ip, value)}
	}
	if n.isDir() {
		panic(mapdb5.NewCorruption("value insert on a directory node"))
	}
	recid := store.Put(m.inner.st, value, m.vf().Element())
	r := make([]int64, 0, len(n.recids)+1)
	r = append(r, n.recids[:ip]...)
	r = append(r, int64(recid))
	r = append(r, n.recids[ip:]...)
	return leafVals{recids: r}
}

func (m *BTreeMap[K, V]) storeUpdate(recid uint64, n *node) {
	store.Update(m.inner.st, recid, *n, m.nodeSer())
}

func (m *BTreeMap[K, V]) storePutNode(n *node) uint64 {
	return store.Put(m.inner.st, *n, m.nodeSer())
}

// splitVals copies a leafVals half for a split ([from,to)), preserving
// representation.
func (m *BTreeMap[K, V]) splitVals(v leafVals, from, to int) leafVals {
	if m.inner.valueInline {
		return leafVals{inline: m.vf().CopyRange(v.inline, from, to)}
	}
	r := make([]int64, to-from)
	copy(r, v.recids[from:to])
	return leafVals{recids: r}
}

// ===================== size counter =====================

// addToCounter applies delta to the shared counter record via a CAS retry
// loop (Java addToCounter). Called AFTER the structural mutation commits.
// No-op when disabled.
func (m *BTreeMap[K, V]) addToCounter(delta int64) {
	cr := m.inner.counterRecid
	if cr == 0 {
		return
	}
	for {
		cur, ok := store.Get(m.inner.st, cr, ser.Long)
		if !ok {
			panic(mapdb5.NewCorruption("btree size counter record missing"))
		}
		if store.CompareAndSwap(m.inner.st, cr, cur, cur+delta, ser.Long) {
			return
		}
	}
}

// counterOrPoison runs a post-commit counter update; a failure after the
// node committed would leave a silently-wrong O(1) count, so the handle is
// poisoned before the panic propagates.
func (m *BTreeMap[K, V]) counterOrPoison(delta int64) {
	if m.inner.counterRecid == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			m.inner.poisoned.Store(true)
			panic(r)
		}
	}()
	m.addToCounter(delta)
}

func (m *BTreeMap[K, V]) poison() { m.inner.poisoned.Store(true) }

// ===================== put / putIfAbsent =====================

// Put inserts or overwrites; returns the previous value.
func (m *BTreeMap[K, V]) Put(key K, value V) (V, bool) {
	return m.putInternal(key, value, false)
}

// PutIfAbsent inserts only when key is absent; returns the existing value.
func (m *BTreeMap[K, V]) PutIfAbsent(key K, value V) (V, bool) {
	return m.putInternal(key, value, true)
}

func (m *BTreeMap[K, V]) putInternal(key K, value V, onlyIfAbsent bool) (V, bool) {
	release := m.opGuard()
	defer release()
	// Tx stores only: resync the structural cache with the tx-visible tree
	// (a prior rollback may have shrunk it) before a split/grow can consult
	// or extend it. No-op for non-tx stores.
	m.refreshLeftEdgesIfTx()
	var stack []uint64
	n, guard := m.lockLeaf(&key, &stack)
	defer guard.release()
	current := guard.recid
	if n.isDir() {
		panic(mapdb5.NewCorruption("put reached a directory node"))
	}
	pos, found := m.kf().Search(n.keys, key)
	if found {
		old := m.leafValueAt(&n, pos)
		if onlyIfAbsent {
			return old, true // no mutation: no counter change, no listener
		}
		m.publishLeafSet(current, &n, pos, value)
		// Sync listeners fire UNDER the leaf lock (preserves same-key event
		// order); the lock releases before the deferred fire, and a sync
		// failure skips the deferred fire — Java/Rust ordering.
		syncErr := m.fireSync(key, &old, &value)
		guard.release()
		if syncErr != nil {
			panic(syncErr)
		}
		if err := m.fireDeferred(key, &old, &value); err != nil {
			panic(err)
		}
		return old, true
	}
	newKeys := m.kf().Insert(n.keys, pos, key)
	newVals := m.insertLeafVals(&n, pos, value)
	var zero V
	if m.kf().Size(newKeys) <= m.inner.maxNodeSize {
		updated := node{flags: n.flags, link: n.link, keys: newKeys, fence: n.fence}
		if m.inner.valueInline {
			updated.values = newVals.inline
		} else {
			updated.recids = newVals.recids
		}
		m.storeUpdate(current, &updated)
		// counter BEFORE the listeners: the mutation is committed, so a
		// failing listener must not desync the O(1) count from the tree.
		m.counterOrPoison(1)
		syncErr := m.fireSync(key, nil, &value)
		guard.release()
		if syncErr != nil {
			panic(syncErr)
		}
		if err := m.fireDeferred(key, nil, &value); err != nil {
			panic(err)
		}
		return zero, false
	}
	// overfull: split. The counter bump and sync listeners run under the
	// leaf lock inside; structural completion is mandatory even on
	// failure — see splitLeafAndPropagate. Deferred listeners fire only
	// after a successful split (a sync failure panics above, skipping
	// this — Java's fire-point semantics).
	m.splitLeafAndPropagate(&guard, &n, newKeys, newVals, &stack, key, &value)
	if err := m.fireDeferred(key, nil, &value); err != nil {
		panic(err)
	}
	return zero, false
}

// splitLeafAndPropagate splits the (locked) overfull leaf and propagates
// separators upward. The right sibling B is written FIRST (referent before
// referrer), the left half republished with link=q (from that instant the
// split is fully searchable through the link), and only THEN is the leaf
// lock released and propagation completed.
//
// SYNC-LISTENER FIRE POINT: after the left half republishes (the insert's
// commit point) and after the O(1) counter bump, but BEFORE this leaf's
// lock is released. Even when the inserted key landed in B, every locking
// path to B still goes through A's link until the separator reaches the
// parent, and lockCovering is hand-over-hand — so no competing same-key
// writer can publish first. THROW RECOVERY: a listener failure must NOT
// skip propagateSplit (skipping the ROOT split's propagation would leave a
// later B-split spinning forever on a level-1 left edge that was never
// created). The listener error is captured, the lock released, propagation
// COMPLETED, and only then is the listener error surfaced. A structural
// propagation error is primary (counter + listener failures attached as
// suppressed); the counter error is secondary (poisons the O(1) count);
// the listener error is last.
func (m *BTreeMap[K, V]) splitLeafAndPropagate(guard *nodeGuard, orig *node, keys ser.Group, vals leafVals, stack *[]uint64, key K, value *V) {
	recid := guard.recid
	// Root-growth gate — BOTH conditions, evaluated before any publication:
	//   (a) orig still carries LEFT|RIGHT (root shape) — the concurrency
	//       serialization: the first splitter republishes the root LEFT-only
	//       under its lock before releasing, so a second splitter sees the
	//       flipped flag and does NOT also grow a root.
	//   (b) the root pointer authoritatively names this recid — rejects a
	//       CRAFTED descendant falsely flagged LEFT|RIGHT.
	wasRoot := orig.flags&(flagLeft|flagRight) == flagLeft|flagRight && m.isCurrentRoot(recid)
	total := m.kf().Size(keys)
	h := total / 2
	b := node{
		flags: orig.flags &^ flagLeft, // B keeps RIGHT status of the original
		link:  orig.link,
		keys:  m.kf().CopyRange(keys, h, total),
		fence: orig.fence,
	}
	bVals := m.splitVals(vals, h, total)
	if m.inner.valueInline {
		b.values = bVals.inline
	} else {
		b.recids = bVals.recids
	}
	// On any panic before publication the caller's deferred guard.release
	// frees the leaf lock; the orphaned b is harmless garbage.
	q := m.storePutNode(&b)
	sep := m.kf().Get(keys, h-1)
	aFence := m.kf().Insert(m.kf().Empty(), 0, sep)
	aVals := m.splitVals(vals, 0, h)
	a := node{
		flags: orig.flags &^ flagRight,
		link:  q,
		keys:  m.kf().CopyRange(keys, 0, h),
		fence: aFence,
	}
	if m.inner.valueInline {
		a.values = aVals.inline
	} else {
		a.recids = aVals.recids
	}
	m.storeUpdate(recid, &a)
	// The split is now published (searchable via the link). The counter
	// bump and sync listeners run under the still-held leaf lock; BOTH are
	// SECONDARY to structural completion — skipping the ROOT split's
	// propagation would leave the root pointer naming a LEFT-only node
	// (unopenable: not root-shaped) and strand a later B-split spinning on
	// a level-1 left edge that was never created. Failures are captured,
	// the lock released, propagation completed, then surfaced:
	// structural (primary) > counter (poison) > listener.
	counterErr := capturePanic(func() { m.addToCounter(1) })
	listenerErr := m.fireSync(key, nil, value)
	guard.release()
	propErr := capturePanic(func() {
		m.propagateSplit(recid, q, sep, wasRoot, stack, 1)
	})
	if propErr != nil {
		m.poison()
		panic(listen.JoinSuppressed(propErr, counterErr, listenerErr))
	}
	if counterErr != nil {
		m.poison()
		panic(listen.JoinSuppressed(counterErr, listenerErr))
	}
	if listenerErr != nil {
		panic(listenerErr)
	}
}

// capturePanic runs f and returns its panic value (nil on success) — the Go
// port of Rust's `let res = fallible()` capture in the split path, where a
// secondary failure must not skip mandatory structural completion.
func capturePanic(f func()) (r any) {
	defer func() { r = recover() }()
	f()
	return nil
}

// propagateSplit inserts (sep → newChild right of oldChild) into the parent
// level; splits upward as needed. level counts from 0 = leaf. No lock held
// on entry; all locks acquired below are released on every path including
// panics (the deferred release tracks the latest guard).
func (m *BTreeMap[K, V]) propagateSplit(oldChild, newChild uint64, sep K, childWasRoot bool, stack *[]uint64, level int) {
	var guard nodeGuard
	defer func() { guard.release() }()
	for {
		if childWasRoot {
			// grow the tree by one level
			rootKeys := m.kf().Insert(m.kf().Empty(), 0, sep)
			newRoot := node{
				flags:    flagDir | flagLeft | flagRight,
				keys:     rootKeys,
				children: []uint64{oldChild, newChild},
			}
			// rguard auto-unlocks the root-pointer recid on any panic.
			rguard := m.lockGuard(m.inner.rootRecidRecid)
			defer rguard.release()
			newRootRecid := m.storePutNode(&newRoot)
			store.Update(m.inner.st, m.inner.rootRecidRecid, int64(newRootRecid), ser.Long)
			if m.inner.rootCacheable {
				m.inner.cachedRoot.Store(newRootRecid)
			}
			le := *m.inner.leftEdges.Load()
			// Root grow appends exactly one level, so the cache must
			// describe a tree of height `level`. A mismatch means the cache
			// drifted (tx rollback that shrank the tree, or a crafted
			// uneven-depth tree); fail hard instead of appending onto a
			// stale vector naming deleted/reused recids.
			if len(le) != level {
				m.inner.poisoned.Store(true)
				panic(mapdb5.NewCorruption("btree leftEdges/level mismatch (stale structural cache); reopen the store"))
			}
			grown := make([]uint64, 0, len(le)+1)
			grown = append(grown, le...)
			grown = append(grown, newRootRecid)
			m.inner.leftEdges.Store(&grown)
			return
		}
		var start uint64
		if len(*stack) == 0 {
			start = m.leftEdge(level)
		} else {
			start = (*stack)[len(*stack)-1]
			*stack = (*stack)[:len(*stack)-1]
		}
		var n node
		n, guard = m.lockCovering(start, &sep, true)
		current := guard.recid
		// Same dual gate as the leaf split: LEFT|RIGHT flag AND
		// authoritative root-pointer identity.
		currentIsRoot := n.flags&(flagLeft|flagRight) == flagLeft|flagRight && m.isCurrentRoot(current)
		pos, dup := m.kf().Search(n.keys, sep)
		// A separator is the max key of a freshly-split left half: strictly
		// inside the covering dir and distinct from every existing separator.
		// A crafted parent that already contains it is corruption, not a
		// duplicate insert.
		if dup {
			guard.release()
			panic(mapdb5.NewCorruption("duplicate parent separator"))
		}
		if !n.isDir() {
			guard.release()
			panic(mapdb5.NewCorruption("propagate reached a leaf node"))
		}
		newKeys := m.kf().Insert(n.keys, pos, sep)
		newChildren := insertU64(n.children, pos+1, newChild)
		keysLen := m.kf().Size(newKeys)
		if keysLen <= m.inner.maxNodeSize {
			updated := node{flags: n.flags, link: n.link, keys: newKeys, children: newChildren}
			m.storeUpdate(current, &updated)
			guard.release()
			return
		}
		// split dir node
		hh := keysLen / 2
		b := node{
			flags:    n.flags &^ flagLeft,
			link:     n.link,
			keys:     m.kf().CopyRange(newKeys, hh, keysLen),
			children: append([]uint64(nil), newChildren[hh:]...),
		}
		q := m.storePutNode(&b)
		parentSep := m.kf().Get(newKeys, hh-1)
		a := node{
			flags:    n.flags &^ flagRight,
			link:     q,
			keys:     m.kf().CopyRange(newKeys, 0, hh),
			children: append([]uint64(nil), newChildren[:hh]...),
		}
		m.storeUpdate(current, &a)
		childWasRoot = currentIsRoot
		guard.release()
		oldChild = current
		newChild = q
		sep = parentSep
		level++
	}
}

// leftEdge returns the left-edge recid of level; spins while a concurrent
// root split creating this level is between publishing the child and
// appending here. Bails out when the map was poisoned by a failed
// root-grow, so a level that will never be published cannot park a writer
// forever.
func (m *BTreeMap[K, V]) leftEdge(level int) uint64 {
	for {
		le := *m.inner.leftEdges.Load()
		if level < len(le) {
			return le[level]
		}
		m.checkPoison()
		time.Sleep(leftEdgeSpin)
	}
}

// ===================== remove / replace =====================

// Remove deletes key; returns the old value.
func (m *BTreeMap[K, V]) Remove(key K) (V, bool) {
	return m.removeInternal(key, nil)
}

// RemoveIfEquals deletes key only when its current value Equals expected.
func (m *BTreeMap[K, V]) RemoveIfEquals(key K, expected V) bool {
	_, ok := m.removeInternal(key, &expected)
	return ok
}

// removeInternal is the remove core; external maps take the write barrier
// across the whole remove so no lock-free reader can observe a value recid
// this remove deletes and the store then reuses. The node mutation,
// external value-record delete, counter adjustment and sync listeners all
// happen inside the barrier (under the leaf lock); the sync fire error is
// captured and surfaced only after the barrier released, and a sync
// failure skips the deferred fire — Java/Rust ordering.
// checkExternalReentry is the external-barrier reentrancy tripwire: a
// synchronous listener that calls back into this map while its remove
// holds the write barrier panics here (a listener-catchable error) rather
// than deadlocking on the non-reentrant externalLock. Compare with the
// node-lock tripwire — same goroutine ⇒ fail fast.
func (m *BTreeMap[K, V]) checkExternalReentry() {
	if id := m.inner.externalOwner.Load(); id != 0 && id == goid() {
		panic(mapdb5.NewCorruption("reentrant btree external barrier"))
	}
}

func (m *BTreeMap[K, V]) removeInternal(key K, expected *V) (V, bool) {
	release := m.opGuard()
	defer release()
	var old V
	var found bool
	var syncErr error
	if m.inner.valueInline {
		old, found, syncErr = m.removeBarrier(key, expected)
	} else {
		m.checkExternalReentry()
		func() {
			m.inner.externalLock.Lock()
			// Publish the barrier's owner BEFORE user code can run under
			// it: a sync listener firing inside removeBarrier re-entering
			// the map trips checkExternalReentry rather than deadlocking
			// here. Cleared before Unlock so no waiter inherits it.
			m.inner.externalOwner.Store(goid())
			defer func() {
				m.inner.externalOwner.Store(0)
				m.inner.externalLock.Unlock()
			}()
			old, found, syncErr = m.removeBarrier(key, expected)
		}()
	}
	if !found {
		var zero V
		return zero, false
	}
	if syncErr != nil {
		panic(syncErr)
	}
	if err := m.fireDeferred(key, &old, nil); err != nil {
		panic(err)
	}
	return old, true
}

func (m *BTreeMap[K, V]) removeBarrier(key K, expected *V) (V, bool, error) {
	n, guard := m.lockLeaf(&key, nil)
	defer guard.release()
	current := guard.recid
	if n.isDir() {
		panic(mapdb5.NewCorruption("remove reached a directory node"))
	}
	pos, found := m.kf().Search(n.keys, key)
	var zero V
	if !found {
		return zero, false, nil
	}
	old := m.leafValueAt(&n, pos)
	if expected != nil && !m.vf().Element().Equals(old, *expected) {
		return zero, false, nil
	}
	// no merging/rebalance (mapdb3 semantics): fence retained.
	newKeys := m.kf().Delete(n.keys, pos)
	if m.inner.valueInline {
		updated := node{flags: n.flags, link: n.link, keys: newKeys,
			values: m.vf().Delete(n.values, pos), fence: n.fence}
		m.storeUpdate(current, &updated)
	} else {
		recid := n.recids[pos]
		updated := node{flags: n.flags, link: n.link, keys: newKeys,
			recids: deleteI64(n.recids, pos), fence: n.fence}
		m.storeUpdate(current, &updated)
		// AFTER the node no longer references it (Java order): free the
		// value record. The write barrier keeps concurrent readers out.
		if m.inner.counterRecid != 0 {
			// A delete failure here skips the counter decrement below:
			// the entry is gone from the tree yet still counted — the
			// same "never trust a wrong O(1) count" invariant that makes
			// add_to_counter failures poison. Fail fast rather than
			// silently desyncing the persisted counter.
			delErr := capturePanic(func() {
				store.Delete(m.inner.st, recidOrCorrupt(uint64(recid)), m.vf().Element())
			})
			if delErr != nil {
				m.poison()
				panic(delErr)
			}
		} else {
			store.Delete(m.inner.st, recidOrCorrupt(uint64(recid)), m.vf().Element())
		}
	}
	m.counterOrPoison(-1)
	// Sync listeners fire under the still-held leaf lock (the deferred
	// release above drops it on return); the error is returned so the
	// caller can release the external barrier before surfacing it.
	return old, true, m.fireSync(key, &old, nil)
}

// Replace overwrites the value for key only when present; returns the old
// value.
func (m *BTreeMap[K, V]) Replace(key K, value V) (V, bool) {
	return m.replaceInternal(key, nil, value)
}

// ReplaceIfEquals overwrites only when the current value Equals oldValue.
func (m *BTreeMap[K, V]) ReplaceIfEquals(key K, oldValue, newValue V) bool {
	_, ok := m.replaceInternal(key, &oldValue, newValue)
	return ok
}

func (m *BTreeMap[K, V]) replaceInternal(key K, expected *V, newValue V) (V, bool) {
	release := m.opGuard()
	defer release()
	n, guard := m.lockLeaf(&key, nil)
	defer guard.release()
	current := guard.recid
	if n.isDir() {
		panic(mapdb5.NewCorruption("replace reached a directory node"))
	}
	pos, found := m.kf().Search(n.keys, key)
	var zero V
	if !found {
		return zero, false
	}
	old := m.leafValueAt(&n, pos)
	if expected != nil && !m.vf().Element().Equals(old, *expected) {
		return zero, false
	}
	// replace of an existing key: counter unchanged.
	m.publishLeafSet(current, &n, pos, newValue)
	syncErr := m.fireSync(key, &old, &newValue)
	guard.release()
	if syncErr != nil {
		panic(syncErr)
	}
	if err := m.fireDeferred(key, &old, &newValue); err != nil {
		panic(err)
	}
	return old, true
}

// ===================== counters / size / clear =====================

// Len is the entry count, saturated at MaxInt.
func (m *BTreeMap[K, V]) Len() int {
	n := m.LenLong()
	if n > math.MaxInt {
		return math.MaxInt
	}
	return int(n)
}

// LenLong is the authoritative entry count (Java sizeLong): O(1) when the
// counter is enabled, otherwise a key-only leaf-chain walk.
func (m *BTreeMap[K, V]) LenLong() int64 {
	release := m.opGuard()
	defer release()
	if m.inner.counterRecid != 0 {
		m.checkPoison()
		v, ok := store.Get(m.inner.st, m.inner.counterRecid, ser.Long)
		if !ok {
			panic(mapdb5.NewCorruption("btree size counter record missing"))
		}
		if v < 0 {
			// A negative persisted counter would cast to ~1.8e19 — a
			// wrong-config/corruption signal, not a real size.
			panic(mapdb5.NewCorruption("btree size counter record is negative"))
		}
		return v
	}
	return m.sizeLongRange(bound[K]{}, bound[K]{})
}

// bound is an optional inclusive/exclusive range endpoint in the map's
// ascending key order.
type bound[K any] struct {
	key  K
	set  bool
	incl bool
}

// sizeLongRange counts in-range entries walking KEYS only (external maps
// expand no value records here).
func (m *BTreeMap[K, V]) sizeLongRange(lo, hi bound[K]) int64 {
	var count int64
	m.walkKeys(lo, hi, func(k *K) bool {
		count++
		return true
	})
	return count
}

// IsEmpty reports whether the map has no entries (key-only early exit).
func (m *BTreeMap[K, V]) IsEmpty() bool {
	any := false
	m.walkKeys(bound[K]{}, bound[K]{}, func(k *K) bool {
		any = true
		return false
	})
	return !any
}

// Clear removes every entry (leaving empty leaves linked, mapdb3
// semantics).
func (m *BTreeMap[K, V]) Clear() {
	var keys []K
	m.walkKeys(bound[K]{}, bound[K]{}, func(k *K) bool {
		keys = append(keys, *k)
		return true
	})
	for _, k := range keys {
		m.Remove(k)
	}
}

// ===================== leaf-chain helpers =====================

// firstLeafForLowerBound returns the leaf that routes lo (leftmost leaf
// when unset), reached by the same unlocked routing as the writers.
func (m *BTreeMap[K, V]) firstLeafForLowerBound(lo *K) node {
	cyc := newCycleGuard(cycleDescentSoft)
	current := m.rootRecid()
	n := m.load(current)
	for n.isDir() {
		cyc.visit(current)
		var ci int
		if lo == nil {
			ci = 0
		} else {
			ci = m.routeChild(&n, lo)
		}
		if ci < 0 {
			current = n.link
		} else {
			current = n.children[ci]
		}
		n = m.load(current)
	}
	return n
}

// firstLeafRecidForLowerBound is the recid variant of the above.
func (m *BTreeMap[K, V]) firstLeafRecidForLowerBound(lo *K) uint64 {
	cyc := newCycleGuard(cycleDescentSoft)
	current := m.rootRecid()
	n := m.load(current)
	for n.isDir() {
		cyc.visit(current)
		var ci int
		if lo == nil {
			ci = 0
		} else {
			ci = m.routeChild(&n, lo)
		}
		if ci < 0 {
			current = n.link
		} else {
			current = n.children[ci]
		}
		n = m.load(current)
	}
	return current
}

// leafStartPos computes the first position in leaf satisfying the lower
// bound (mirrors the ascending iterator's start position).
func (m *BTreeMap[K, V]) leafStartPos(leaf *node, lo bound[K]) int {
	if !lo.set {
		return 0
	}
	pos, found := m.kf().Search(leaf.keys, lo.key)
	if found && lo.incl {
		return pos
	}
	if found {
		return pos + 1
	}
	return pos
}

// belowLo reports whether k is strictly below the lower bound.
func (m *BTreeMap[K, V]) belowLo(k K, lo bound[K]) bool {
	if !lo.set {
		return false
	}
	c := m.kf().Compare(k, lo.key)
	return c < 0 || (c == 0 && !lo.incl)
}

// aboveHi reports whether k is strictly above the upper bound.
func (m *BTreeMap[K, V]) aboveHi(k K, hi bound[K]) bool {
	if !hi.set {
		return false
	}
	c := m.kf().Compare(k, hi.key)
	return c > 0 || (c == 0 && !hi.incl)
}

// walkKeys walks in-range keys ascending WITHOUT expanding external value
// records — only key groups are read, so no external read barrier is
// needed. f returning false stops early. Weakly consistent.
func (m *BTreeMap[K, V]) walkKeys(lo, hi bound[K], f func(k *K) bool) {
	release := m.opGuard()
	defer release()
	cyc := newCycleGuard(cycleScanSoft)
	var loKey *K
	if lo.set {
		loKey = &lo.key
	}
	leaf := m.firstLeafForLowerBound(loKey)
	pos := m.leafStartPos(&leaf, lo)
	loPending := lo.set
	for {
		for pos >= m.kf().Size(leaf.keys) {
			link := leaf.link
			if link == 0 {
				return
			}
			cyc.visit(link)
			leaf = m.load(link)
			pos = 0
		}
		k := m.kf().Get(leaf.keys, pos)
		if loPending {
			if m.belowLo(k, lo) {
				pos++
				continue
			}
			loPending = false
		}
		if m.aboveHi(k, hi) {
			return
		}
		if !f(&k) {
			return
		}
		pos++
	}
}

// insertU64 returns a copy of arr with v inserted at pos (copy-on-write).
func insertU64(arr []uint64, pos int, v uint64) []uint64 {
	r := make([]uint64, 0, len(arr)+1)
	r = append(r, arr[:pos]...)
	r = append(r, v)
	return append(r, arr[pos:]...)
}

// deleteI64 returns a copy of arr without element pos.
func deleteI64(arr []int64, pos int) []int64 {
	r := make([]int64, 0, len(arr)-1)
	r = append(r, arr[:pos]...)
	return append(r, arr[pos+1:]...)
}

// ===================== Verify =====================

// Verify checks the tree's structural invariants: root shape, node decode,
// leaf-link chain completeness against a full tree descent, key ordering
// (enforced at decode), and (when enabled) the counter record against the
// actual entry count. Panics KindVerifyFailed on inconsistency.
func (m *BTreeMap[K, V]) Verify() {
	m.verify()
}
