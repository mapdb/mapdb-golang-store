package htree

// HTreeCache — port of Java org.mapdb.htree.HTreeCache: the HTreeMap
// sibling with per-entry EXPIRATION (TTL and/or max-size/store-size
// eviction). A self-contained collection (Java keeps it separate rather
// than feature flags in hot paths); DirTree, segRuntime, MapRuntime,
// QueueLong and the lifecycle machinery are shared unchanged.
//
// Structure on top of the segment/dir-tree layout:
//   - buckets store TRIPLES (key, value, queueNodeRecid) — values inline;
//   - each segment owns a FIFO QueueLong ordered oldest-first: every
//     entry has exactly one queue node carrying (expiryStamp, leafRecid);
//   - each segment owns an O(1) IN-MEMORY size counter (atomic.Int64,
//     bumped under the write lock) so LenLong/max-size never traverse
//     the tree nor touch the store. The counter is reconstructed at open
//     from the persisted queue length; the counter RECIDS are still
//     allocated (header format stability) but never read/written.
//
// Expiry config (fixed at create, persisted in the header):
//   - TTL (clock ticks, 0 = off): entries expire ttl after last WRITE,
//     or after last ACCESS when AccessOrder. Deadline is OPEN: live at
//     writeTime+ttl, expired once that instant has PASSED.
//   - AccessOrder: a Get hit bumps the entry to the queue head — with
//     MaxSize this makes eviction LRU instead of FIFO; Gets then take
//     the segment WRITE lock.
//   - MaxSize (0 = off): APPROXIMATE bound, enforced per segment as
//     MaxSize/segments (floor; mapdb3 semantics).
//   - StoreSize (bytes, 0 = off): APPROXIMATE byte budget on
//     store.CurrentSize() — enforced by whichever segment a mutating op
//     touches, evicting that segment's own oldest; a foreground op takes
//     at most fgStoreEvictCap victims. Requires a store reporting
//     CurrentSize() > 0 (heap reports 0 = silently disabled).
//   - MaxEvictPerOp (0 = unbounded): total victims per foreground sweep.
//
// Eviction is a SWEEP at the start of every mutating op on the touched
// segment (plus ExpireEvict for a global unbounded reclaim). Reads are
// STRICT: TTL-expired entries read as absent even before a sweep reclaims
// them; LenLong/IsEmpty are counter-based PHYSICAL occupancy.
//
// Optional runtime wiring (NOT persisted — re-supply on every open):
//   - Overflow map: receives CAPACITY-evicted entries and backs Get
//     misses (spillover backing store; TTL-EXPIRED victims are logically
//     dead and never handed off). Handoff/miss-load run with NO segment
//     lock held; overflow graphs must be acyclic.
//   - Eviction listener: invoked once per sweep-evicted entry with its
//     EvictionCause, no lock held; runs AFTER the overflow handoff.

import (
	"errors"
	"iter"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/internal/handle"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/queue"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// fgStoreEvictCap bounds the store-size victims one FOREGROUND sweep may
// take (Java FG_STORE_EVICT_CAP). ExpireEvict is unbounded.
const fgStoreEvictCap = 16

// EvictionCause is why a sweep removed an entry (Java EvictionReason).
type EvictionCause int

const (
	// EvictTTL: the entry's TTL lapsed (logically dead; never overflowed).
	EvictTTL EvictionCause = iota
	// EvictMaxSize: evicted to keep the segment within its max-size share.
	EvictMaxSize
	// EvictStoreSize: evicted to keep the backing store under its byte
	// budget.
	EvictStoreSize
)

// OverflowMap is the spillover backing store a cache can hand
// capacity-evicted entries to and consult on Get misses. Satisfied by
// *HTreeMap, *HTreeMapExternal, *HTreeMap48, *BTreeMap and *HTreeCache.
type OverflowMap[K, V any] interface {
	Get(K) (V, bool)
	Put(K, V) (V, bool)
}

// cleaf is one cache bucket record: parallel (key, value, nodeRecid)
// triples (Java's Object[]{k0, v0, nodeRecid0, …}).
type cleaf[K, V any] struct {
	keys    []K
	vals    []V
	nodeRec []uint64
}

// cleafSerializer is the cache bucket wire codec: packInt(count) then
// (key, value, packLong nodeRecid) per entry.
type cleafSerializer[K, V any] struct {
	keySer   ser.Serializer[K]
	valueSer ser.Serializer[V]
}

func (s cleafSerializer[K, V]) Serialize(w *bin.Writer, leaf cleaf[K, V]) {
	if len(leaf.keys) != len(leaf.vals) || len(leaf.keys) != len(leaf.nodeRec) || len(leaf.keys) == 0 {
		panic(mapdb5.NewMisuse("htree cache leaf must hold >= 1 paired triples"))
	}
	w.PackInt(int32(len(leaf.keys)))
	for i := range leaf.keys {
		s.keySer.Serialize(w, leaf.keys[i])
		s.valueSer.Serialize(w, leaf.vals[i])
		w.PackLong(int64(leaf.nodeRec[i]))
	}
}

func (s cleafSerializer[K, V]) Deserialize(r *bin.Reader, size int) cleaf[K, V] {
	n := int(r.UnpackInt())
	if n < 1 || (size >= 0 && n > size) {
		panic(mapdb5.NewCorruption("htree cache leaf entry count out of range"))
	}
	leaf := cleaf[K, V]{keys: make([]K, n), vals: make([]V, n), nodeRec: make([]uint64, n)}
	for i := 0; i < n; i++ {
		leaf.keys[i] = s.keySer.Deserialize(r, -1)
		leaf.vals[i] = s.valueSer.Deserialize(r, -1)
		v := r.UnpackLong()
		if v <= 0 {
			panic(mapdb5.NewCorruption("htree cache leaf node recid out of range"))
		}
		leaf.nodeRec[i] = uint64(v)
	}
	return leaf
}

func (s cleafSerializer[K, V]) FixedSize() int { return -1 }
func (s cleafSerializer[K, V]) SizeHint() int  { return 48 }
func (s cleafSerializer[K, V]) Compare(a, b cleaf[K, V]) int {
	panic(mapdb5.NewMisuse("htree cache leaf codec is record-internal, not orderable"))
}
func (s cleafSerializer[K, V]) Equals(a, b cleaf[K, V]) bool {
	panic(mapdb5.NewMisuse("htree cache leaf codec is record-internal, not orderable"))
}
func (s cleafSerializer[K, V]) NaturalOrder() bool            { return false }
func (s cleafSerializer[K, V]) EqualsBySerializedBytes() bool { return true }

// leafDelete returns the leaf with triple i removed.
func leafDelete[K, V any](leaf cleaf[K, V], i int) cleaf[K, V] {
	keys := make([]K, 0, len(leaf.keys)-1)
	vals := make([]V, 0, len(leaf.vals)-1)
	nodes := make([]uint64, 0, len(leaf.nodeRec)-1)
	keys = append(keys, leaf.keys[:i]...)
	keys = append(keys, leaf.keys[i+1:]...)
	vals = append(vals, leaf.vals[:i]...)
	vals = append(vals, leaf.vals[i+1:]...)
	nodes = append(nodes, leaf.nodeRec[:i]...)
	nodes = append(nodes, leaf.nodeRec[i+1:]...)
	return cleaf[K, V]{keys, vals, nodes}
}

// cHeader is the persisted cache metadata (Java Header.SER): geometry,
// expiry config, and all per-segment recids. Written once at create.
type cHeader struct {
	concShift, dirShift, levels int32
	hashSeed                    int32
	ttl, maxSize                int64
	storeSize, maxEvictPerOp    int64
	accessOrder                 bool
	segmentRoots                []uint64
	counterRecids               []uint64
	queueTails                  []uint64
	queueHeads                  []uint64
	queueHeadPrevs              []uint64
}

type cHeaderSer struct{}

var cHeaderSerializer = ser.NewAnyOf[cHeader](cHeaderSer{})

func (cHeaderSer) Serialize(w *bin.Writer, h cHeader) {
	w.PackInt(h.concShift)
	w.PackInt(h.dirShift)
	w.PackInt(h.levels)
	w.WriteInt(h.hashSeed)
	w.PackLong(h.ttl)
	w.PackLong(h.maxSize)
	w.PackLong(h.storeSize)
	w.PackLong(h.maxEvictPerOp)
	w.WriteU8(boolByte(h.accessOrder))
	for _, r := range h.segmentRoots {
		w.PackLong(int64(r))
	}
	for _, r := range h.counterRecids {
		w.PackLong(int64(r))
	}
	for _, r := range h.queueTails {
		w.PackLong(int64(r))
	}
	for _, r := range h.queueHeads {
		w.PackLong(int64(r))
	}
	for _, r := range h.queueHeadPrevs {
		w.PackLong(int64(r))
	}
}

func (cHeaderSer) Deserialize(r *bin.Reader, size int) cHeader {
	h := cHeader{
		concShift:     r.UnpackInt(),
		dirShift:      r.UnpackInt(),
		levels:        r.UnpackInt(),
		hashSeed:      r.ReadInt(),
		ttl:           r.UnpackLong(),
		maxSize:       r.UnpackLong(),
		storeSize:     r.UnpackLong(),
		maxEvictPerOp: r.UnpackLong(),
	}
	h.accessOrder = r.ReadU8() != 0
	if h.concShift < 0 || h.concShift > 12 || h.dirShift <= 0 ||
		h.dirShift > MaxDirShift || h.levels <= 0 {
		panic(mapdb5.NewCorruption("htree cache header geometry out of range"))
	}
	segments := 1 << h.concShift
	alloc := func() []uint64 {
		a := make([]uint64, segments)
		for i := range a {
			v := r.UnpackLong()
			if v <= 0 {
				panic(mapdb5.NewCorruption("htree cache header recid out of range"))
			}
			a[i] = uint64(v)
		}
		return a
	}
	h.segmentRoots = alloc()
	h.counterRecids = alloc()
	h.queueTails = alloc()
	h.queueHeads = alloc()
	h.queueHeadPrevs = alloc()
	return h
}

func (cHeaderSer) FixedSize() int                { return -1 }
func (cHeaderSer) SizeHint() int                 { return 512 }
func (cHeaderSer) Compare(a, b cHeader) int      { panic(misuseCHeader()) }
func (cHeaderSer) Equals(a, b cHeader) bool      { panic(misuseCHeader()) }
func (cHeaderSer) NaturalOrder() bool            { return false }
func (cHeaderSer) EqualsBySerializedBytes() bool { return true }

func misuseCHeader() error {
	return mapdb5.NewMisuse("htree cache header codec is record-internal, not orderable")
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// CConfig configures HTreeCache creation/opening.
type CConfig[K, V any] struct {
	KeySer   ser.Serializer[K]
	ValueSer ser.Serializer[V]
	// Geometry: all three zero → Java defaults (4, 7, 4); otherwise
	// concShift + levels*dirShift must equal 32 (concShift capped at 12).
	ConcShift int
	DirShift  int
	Levels    int
	// TTL in clock ticks (0 = off): entries expire TTL after last write,
	// or after last access when AccessOrder.
	TTL int64
	// AccessOrder bumps a Get hit to the queue head — eviction becomes
	// LRU instead of FIFO with MaxSize; Gets take the write lock.
	AccessOrder bool
	// MaxSize is the approximate global bound, enforced per segment as
	// MaxSize/segments (floor). 0 = off.
	MaxSize int64
	// StoreSize is the approximate byte budget on store.CurrentSize().
	// 0 = off. Requires a store reporting CurrentSize() > 0.
	StoreSize int64
	// MaxEvictPerOp bounds total victims per foreground sweep
	// (0 = unbounded). ExpireEvict is always unbounded.
	MaxEvictPerOp int64
	// HashSeed is the persisted 32-bit seed; ignored unless HashSeedSet.
	HashSeed    int32
	HashSeedSet bool
	// Hasher maps (key, seed) → placement hash. nil → mdhash.SerHasher32.
	Hasher mdhash.Hasher[K]
	// Clock supplies the current tick (default: wall-clock ms). Injectable
	// for deterministic expiry tests.
	Clock func() int64
}

// cvictim is one sweep casualty pending the post-unlock flush.
type cvictim[K, V any] struct {
	key    K
	value  V
	reason EvictionCause
}

type cinner[K, V any] struct {
	st            store.Store
	keySer        ser.Serializer[K]
	valueSer      ser.Serializer[V]
	concShift     int
	dirShift      int
	levels        int
	hashSeed      int64
	headerRecid   store.Recid
	segmentRoots  []uint64
	queues        []*queue.QueueLong
	counters      []atomic.Int64
	ttl           int64
	maxSize       int64
	storeSize     int64
	maxEvictPerOp int64
	accessOrder   bool
	clock         func() int64
	hashOf        func(K) int64
	seg           segRuntime[K, V]
	segmentShift  int
	indexMask     int64
	concMask      int
	leafSer       cleafSerializer[K, V]
	closed        atomic.Bool
	ops           handle.Tracker
	lease         *store.LeaseGuard
	id            uint64

	overflow    atomic.Pointer[ovBox[K, V]] // nil box or nil ov = no overflow
	evictListen atomic.Value                // stores func(K, V, EvictionCause)
	inOverflow  sync.Map                    // goid → struct{}: reentrancy guard
	inEvictCB   sync.Map                    // goid → struct{}: eviction-callback mutation guard
	// lastNow is the high-water clock tick: expiry reads a MONOTONE clock
	// so queue order stays deadline order even if Clock steps backward.
	lastNow atomic.Int64
}

// ovBox holds the nullable overflow interface so any implementation (or
// none) can be swapped in (atomic.Value rejects nil and type changes).
type ovBox[K, V any] struct{ ov OverflowMap[K, V] }

// HTreeCache is the expiring segmented hash map. The zero value is
// unusable; construct via NewHTreeCache/OpenHTreeCache.
type HTreeCache[K, V any] struct {
	inner *cinner[K, V]
}

// ===================== construction =====================

func checkCConfig[K, V any](s store.Store, cfg CConfig[K, V]) (concShift, dirShift, levels int, err error) {
	concShift, dirShift, levels = cfg.ConcShift, cfg.DirShift, cfg.Levels
	if concShift == 0 && dirShift == 0 && levels == 0 {
		concShift, dirShift, levels = 4, 7, 4 // Java create defaults
	}
	if concShift < 0 || concShift > 12 {
		return 0, 0, 0, mapdb5.NewMisuse("concShift must be in [0,12]")
	}
	if dirShift < 1 || dirShift > MaxDirShift || levels < 1 {
		return 0, 0, 0, mapdb5.NewMisuse("dirShift/levels out of range")
	}
	if concShift+levels*dirShift != 32 {
		return 0, 0, 0, mapdb5.NewMisuse("concShift + levels*dirShift must equal 32")
	}
	if cfg.TTL < 0 || cfg.MaxSize < 0 || cfg.StoreSize < 0 || cfg.MaxEvictPerOp < 0 {
		return 0, 0, 0, mapdb5.NewMisuse("expiry bounds must be >= 0")
	}
	if cfg.TTL == 0 && cfg.MaxSize == 0 && cfg.StoreSize == 0 {
		return 0, 0, 0, mapdb5.NewMisuse("cache needs ttl and/or maxSize and/or storeSize; for a plain map use HTreeMap")
	}
	if cfg.StoreSize > 0 && cfg.TTL == 0 && cfg.MaxSize == 0 && s.CurrentSize() == 0 {
		return 0, 0, 0, mapdb5.NewMisuse(
			"storeSize is the only eviction bound but this store reports CurrentSize()==0")
	}
	return concShift, dirShift, levels, nil
}

func newCInner[K, V any](s store.Store, cfg CConfig[K, V], headerRecid store.Recid,
	h cHeader, lease *store.LeaseGuard) *cinner[K, V] {
	hasher := resolveHasher(HConfig[K, V]{KeySer: cfg.KeySer, Hasher: cfg.Hasher})
	clock := cfg.Clock
	if clock == nil {
		clock = func() int64 { return time.Now().UnixMilli() }
	}
	nseg := len(h.segmentRoots)
	seed := h.hashSeed
	in := &cinner[K, V]{
		st:            s,
		keySer:        cfg.KeySer,
		valueSer:      cfg.ValueSer,
		concShift:     int(h.concShift),
		dirShift:      int(h.dirShift),
		levels:        int(h.levels),
		hashSeed:      int64(seed),
		headerRecid:   headerRecid,
		segmentRoots:  h.segmentRoots,
		ttl:           h.ttl,
		maxSize:       h.maxSize,
		storeSize:     h.storeSize,
		maxEvictPerOp: h.maxEvictPerOp,
		accessOrder:   h.accessOrder,
		clock:         clock,
		hashOf: func(k K) int64 {
			return int64(uint32(hasher.Hash(k, seed)))
		},
		seg:          newSegRuntime[K, V](nseg, s.IsThreadSafe()),
		segmentShift: int(h.levels) * int(h.dirShift),
		indexMask:    (int64(1) << (h.levels * h.dirShift)) - 1,
		concMask:     nseg - 1,
		leafSer:      cleafSerializer[K, V]{keySer: cfg.KeySer, valueSer: cfg.ValueSer},
		id:           htreeStateIDs.Add(1),
		lease:        lease,
	}
	in.queues = make([]*queue.QueueLong, nseg)
	in.counters = make([]atomic.Int64, nseg)
	for i := 0; i < nseg; i++ {
		q, err := queue.OpenExpireQueue(s, h.queueTails[i], h.queueHeads[i], h.queueHeadPrevs[i])
		if err != nil {
			panic(err)
		}
		in.queues[i] = q
		// reconstruct the in-memory counter from the persisted queue
		// (invariant: counter == queue length == physical entries)
		in.counters[i].Store(int64(q.Len()))
	}
	return in
}

// NewHTreeCache creates a fresh cache over s.
func NewHTreeCache[K, V any](s store.Store, cfg CConfig[K, V]) *HTreeCache[K, V] {
	m, err := createHTreeCache(s, cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func createHTreeCache[K, V any](s store.Store, cfg CConfig[K, V]) (*HTreeCache[K, V], error) {
	concShift, dirShift, levels, err := checkCConfig(s, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree cache: KeySer and ValueSer are required")
	}
	seed := cfg.HashSeed
	if !cfg.HashSeedSet {
		seed = int32(rand.Uint32())
	}
	nseg := 1 << concShift
	roots := make([]uint64, nseg)
	counters := make([]uint64, nseg)
	qTails := make([]uint64, nseg)
	qHeads := make([]uint64, nseg)
	qHeadPrevs := make([]uint64, nseg)
	for i := 0; i < nseg; i++ {
		roots[i] = store.Put(s, DirEmpty(), DirSer)
		counters[i] = store.Put(s, int64(0), ser.Long) // allocated, unused (format stability)
		q, err := queue.NewExpireQueue(s)
		if err != nil {
			return nil, err
		}
		qTails[i] = uint64(q.TailRecid())
		qHeads[i] = uint64(q.HeadRecid())
		qHeadPrevs[i] = uint64(q.HeadPrevRecid())
	}
	hdr := cHeader{
		concShift: int32(concShift), dirShift: int32(dirShift), levels: int32(levels),
		hashSeed: seed, ttl: cfg.TTL, accessOrder: cfg.AccessOrder,
		maxSize: cfg.MaxSize, storeSize: cfg.StoreSize, maxEvictPerOp: cfg.MaxEvictPerOp,
		segmentRoots: roots, counterRecids: counters,
		queueTails: qTails, queueHeads: qHeads, queueHeadPrevs: qHeadPrevs,
	}
	headerRecid := store.Put(s, hdr, cHeaderSerializer)
	return openHTreeCache(s, headerRecid, cfg)
}

// OpenHTreeCache reattaches a cache persisted under headerRecid; the
// serializers, hasher and clock must match create.
func OpenHTreeCache[K, V any](s store.Store, headerRecid store.Recid, cfg CConfig[K, V]) (m *HTreeCache[K, V], err error) {
	defer dbErrorToErr(&err)
	return openHTreeCache(s, headerRecid, cfg)
}

func openHTreeCache[K, V any](s store.Store, headerRecid store.Recid, cfg CConfig[K, V]) (*HTreeCache[K, V], error) {
	if headerRecid == 0 {
		return nil, mapdb5.NewCorruption("htree cache header recid must be a valid recid")
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree cache: KeySer and ValueSer are required")
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
	hdr, ok := store.Get(s, uint64(headerRecid), cHeaderSerializer)
	if !ok {
		return nil, mapdb5.NewCorruption("htree cache header record is null")
	}
	if int(hdr.concShift)+int(hdr.levels)*int(hdr.dirShift) != 32 {
		return nil, mapdb5.NewCorruption("htree cache header geometry invalid")
	}
	in := newCInner(s, cfg, headerRecid, hdr, lease) // may panic: lease still released
	committed = true
	return &HTreeCache[K, V]{inner: in}, nil
}

// ===================== accessors / lifecycle =====================

// HeaderRecid is the recid of the header record.
func (m *HTreeCache[K, V]) HeaderRecid() store.Recid { return m.inner.headerRecid }

// Segments is the physical segment count.
func (m *HTreeCache[K, V]) Segments() int { return len(m.inner.segmentRoots) }

// TTL returns the configured time-to-live in clock ticks (0 = off).
func (m *HTreeCache[K, V]) TTL() int64 { return m.inner.ttl }

// IsClosed reports whether this handle was retired or the store closed.
func (m *HTreeCache[K, V]) IsClosed() bool {
	return m.inner.closed.Load() || m.inner.st.IsClosed()
}

func (m *HTreeCache[K, V]) KeySerializer() ser.Serializer[K]   { return m.inner.keySer }
func (m *HTreeCache[K, V]) ValueSerializer() ser.Serializer[V] { return m.inner.valueSer }
func (m *HTreeCache[K, V]) StateID() uint64                    { return m.inner.id }
func (m *HTreeCache[K, V]) SharesStateWith(o *HTreeCache[K, V]) bool {
	return m.inner == o.inner
}

// Close releases the open lease and invalidates the handle. Idempotent.
func (m *HTreeCache[K, V]) Close() error {
	m.CloseHandle()
	return nil
}

// CloseHandle is the retirement path (HTreeMap.CloseHandle parity).
func (m *HTreeCache[K, V]) CloseHandle() {
	if m.inner.closed.CompareAndSwap(false, true) {
		m.inner.ops.DropSelf()
		m.inner.ops.Wait()
		if l := m.inner.lease; l != nil {
			l.Release()
		}
	}
}

func (m *HTreeCache[K, V]) opGuard() func() {
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

func (m *HTreeCache[K, V]) checkOpen() {
	if m.inner.closed.Load() {
		panicHClosed()
	}
}

// ===================== overflow / eviction listener =====================

// SetOverflow wires (or clears with nil) the spillover backing store —
// receives CAPACITY-evicted entries and backs Get misses. Handoff and
// miss-load run with no cache lock held. A cache cannot overflow into
// itself; overflow graphs must be acyclic. Runtime-only: re-supply after
// every open.
func (m *HTreeCache[K, V]) SetOverflow(ov OverflowMap[K, V]) {
	if c, ok := ov.(*HTreeCache[K, V]); ok && c.SharesStateWith(m) {
		panic(mapdb5.NewMisuse("a cache cannot overflow into itself"))
	}
	m.inner.overflow.Store(&ovBox[K, V]{ov: ov})
}

// SetEvictionListener wires (or clears with nil) the per-victim callback
// fired after sweeps with no lock held. Runtime-only.
func (m *HTreeCache[K, V]) SetEvictionListener(f func(key K, value V, cause EvictionCause)) {
	m.inner.evictListen.Store(f)
}

func (m *HTreeCache[K, V]) getOverflow() OverflowMap[K, V] {
	if b := m.inner.overflow.Load(); b != nil {
		return b.ov
	}
	return nil
}

func (m *HTreeCache[K, V]) getListener() func(K, V, EvictionCause) {
	if v := m.inner.evictListen.Load(); v != nil {
		if f, ok := v.(func(K, V, EvictionCause)); ok {
			return f
		}
	}
	return nil
}

func (m *HTreeCache[K, V]) enterOverflow() func() {
	id := handle.Goid()
	if id == 0 {
		return func() {}
	}
	m.inner.inOverflow.Store(id, struct{}{})
	return func() { m.inner.inOverflow.Delete(id) }
}

func (m *HTreeCache[K, V]) inOverflowNow() bool {
	id := handle.Goid()
	if id == 0 {
		return false
	}
	_, ok := m.inner.inOverflow.Load(id)
	return ok
}

// enterEvictCallback marks this goroutine as running the eviction
// listener; the returned func clears it (defer it).
func (m *HTreeCache[K, V]) enterEvictCallback() func() {
	id := handle.Goid()
	if id == 0 {
		return func() {}
	}
	m.inner.inEvictCB.Store(id, struct{}{})
	return func() { m.inner.inEvictCB.Delete(id) }
}

// checkNotInCallback rejects a mutation re-entered from this cache's
// eviction listener (spec 04: the callback must not mutate the map;
// boundedlru-style panic).
func (m *HTreeCache[K, V]) checkNotInCallback() {
	id := handle.Goid()
	if id == 0 {
		return
	}
	if _, ok := m.inner.inEvictCB.Load(id); ok {
		panic(mapdb5.NewMisuse("htree cache: eviction listener must not mutate the cache"))
	}
}

// ===================== hashing / locks =====================

func (m *HTreeCache[K, V]) hash(key K) int64 { return m.inner.hashOf(key) }

func (m *HTreeCache[K, V]) segment(h int64) int {
	return int(h>>uint(m.inner.segmentShift)) & m.inner.concMask
}

func (m *HTreeCache[K, V]) index(h int64) int64 { return h & m.inner.indexMask }

func (m *HTreeCache[K, V]) lockRead(seg int)   { m.inner.seg.lockRead(seg) }
func (m *HTreeCache[K, V]) unlockRead(seg int) { m.inner.seg.unlockRead(seg) }

// ===================== expiry plumbing =====================

// now is the MONOTONE clock tick: max(previous, Clock()). A backward step
// in Clock (wall-clock adjustment) cannot reorder queue deadlines, so the
// sweep's stop-at-first-live rule stays sound.
func (m *HTreeCache[K, V]) now() int64 { return m.advance(m.inner.clock()) }

// advance folds tick t into the high-water mark and returns the result.
func (m *HTreeCache[K, V]) advance(t int64) int64 {
	for {
		prev := m.inner.lastNow.Load()
		if t <= prev {
			return prev
		}
		if m.inner.lastNow.CompareAndSwap(prev, t) {
			return t
		}
	}
}

// stamp is the expiry instant for an entry written/accessed now; 0 = no
// TTL. A negative clock is rejected; now+ttl saturates at MaxInt64 rather
// than corrupting the queue's wire format (spec saturating-add).
func (m *HTreeCache[K, V]) stamp() int64 {
	if m.inner.ttl == 0 {
		return 0
	}
	now := m.inner.clock()
	if now < 0 {
		panic(mapdb5.NewMisuse("htree cache clock returned a negative tick"))
	}
	now = m.advance(now)
	s := now + m.inner.ttl
	if s < 0 {
		return math.MaxInt64
	}
	return s
}

// expired reports whether a queue node's stamp says "expired".
func (m *HTreeCache[K, V]) expired(timestamp int64) bool {
	return timestamp != 0 && timestamp < m.now()
}

// nodeExpired is the strict-read TTL check: one extra record read per
// hit, only when TTL is on.
func (m *HTreeCache[K, V]) nodeExpired(nodeRecid uint64) bool {
	if m.inner.ttl == 0 {
		return false
	}
	node, ok := store.Get(m.inner.st, nodeRecid, queue.LNodeSer)
	if !ok {
		panic(mapdb5.NewCorruption("queue node not found"))
	}
	return m.expired(int64(node.Timestamp))
}

func (m *HTreeCache[K, V]) counterAdd(seg int, delta int64) {
	m.inner.counters[seg].Add(delta)
}

// leafFor resolves idx's bucket under the segment lock (nil = absent).
func (m *HTreeCache[K, V]) leafFor(seg int, idx int64) (cleaf[K, V], bool) {
	leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
		m.inner.levels-1, idx)
	if leafRecid == 0 {
		return cleaf[K, V]{}, false
	}
	return m.loadLeaf(leafRecid), true
}

func (m *HTreeCache[K, V]) loadLeaf(leafRecid int64) cleaf[K, V] {
	leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.leafSer)
	if !ok {
		panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
	}
	return leaf
}

// ===================== sweep =====================

// newVictimList allocates the sweep-victim list when ANY sink is due:
// the listener needs every victim, overflow needs capacity victims when
// not already inside an overflow flush on this goroutine.
func (m *HTreeCache[K, V]) newVictimList() *[]cvictim[K, V] {
	needListener := m.getListener() != nil || m.inner.seg.runtime.hasListeners()
	needOverflow := m.getOverflow() != nil && !m.inOverflowNow()
	if !needListener && !needOverflow {
		return nil
	}
	v := make([]cvictim[K, V], 0, 4)
	return &v
}

// evictSegment sweeps one segment under its write lock: dequeue
// oldest-first while the segment is over its max-size share, OR the
// global store size is over budget (up to storeEvictCap), OR the node is
// TTL-expired — at most maxEvict victims total. Stops at the first
// non-purged node. Java evictSegment.
func (m *HTreeCache[K, V]) evictSegment(seg int, win *deferWindow[K, V],
	victims *[]cvictim[K, V], storeEvictCap, maxEvict int64) error {
	now := m.now()
	over := int64(0)
	if m.inner.maxSize != 0 {
		over = max(0, m.inner.counters[seg].Load()-m.inner.maxSize/int64(len(m.inner.segmentRoots)))
	}
	toTake := over
	storeLeft := int64(0)
	if m.inner.storeSize != 0 {
		storeLeft = storeEvictCap
	}
	taken := int64(0)
	m.inner.queues[seg].TakeUntil(func(nodeRecid store.Recid, node queue.LNode) bool {
		if taken >= maxEvict {
			return false
		}
		ttlExpired := node.Timestamp != 0 && int64(node.Timestamp) < now
		purge := false
		var reason EvictionCause
		if toTake > 0 {
			toTake--
			purge = true
			reason = EvictMaxSize
		}
		if !purge && storeLeft > 0 && m.inner.st.CurrentSize() > m.inner.storeSize {
			storeLeft--
			purge = true
			reason = EvictStoreSize
		}
		if !purge && ttlExpired {
			purge = true // TTL: dead
		}
		if ttlExpired {
			reason = EvictTTL // dead wins over any capacity reason
		}
		if purge {
			m.evictEntry(seg, int64(node.Value), nodeRecid, victims, reason)
			taken++
		}
		return purge
	})
	// SYNC listeners see victim removals UNDER the segment lock (per-key
	// ordering vs concurrent puts). Deferred events + overflow/listener
	// flush stay post-unlock (flushEvicted).
	if victims != nil && len(*victims) > 0 {
		events := make([]hevent[K, V], len(*victims))
		for i, v := range *victims {
			val := v.value
			events[i] = hevent[K, V]{key: v.key, old: &val, trig: true}
		}
		return m.inner.seg.runtime.fireRemovalBatch(win, events, true, true)
	}
	return nil
}

// evictEntry removes the entry whose queue node is nodeRecid from leaf
// leafRecid. The queue node itself is untouched — the takeUntil sweep
// dequeues it right after. The (key,value,reason) is captured BEFORE the
// leaf is touched for the post-unlock flush.
func (m *HTreeCache[K, V]) evictEntry(seg int, leafRecid int64, nodeRecid store.Recid,
	victims *[]cvictim[K, V], reason EvictionCause) {
	leaf := m.loadLeaf(leafRecid)
	for i := range leaf.keys {
		if leaf.nodeRec[i] != uint64(nodeRecid) {
			continue
		}
		key := leaf.keys[i]
		if victims != nil {
			*victims = append(*victims, cvictim[K, V]{key: key, value: leaf.vals[i], reason: reason})
		}
		h := m.hash(key)
		idx := m.index(h)
		if len(leaf.keys) == 1 {
			if !TreeRemove(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
				m.inner.levels-1, idx) {
				panic(mapdb5.NewCorruption("dir tree lost the bucket it just resolved"))
			}
			store.Delete(m.inner.st, uint64(leafRecid), m.inner.leafSer)
		} else {
			store.Update(m.inner.st, uint64(leafRecid), leafDelete(leaf, i), m.inner.leafSer)
		}
		m.counterAdd(seg, -1)
		return
	}
	panic(mapdb5.NewCorruption("queue node not found in its leaf"))
}

// flushEvicted hands victims to the overflow map and/or the eviction
// listener with NO lock held. Overflow first (capacity victims only —
// TTL-dead entries never leave the tombstone), then the listener (all
// victims), then deferred listener events.
func (m *HTreeCache[K, V]) flushEvicted(victims *[]cvictim[K, V]) error {
	if victims == nil || len(*victims) == 0 {
		return nil
	}
	el := m.getListener()
	ov := m.getOverflow()
	doOverflow := ov != nil && !m.inOverflowNow()
	var first error
	if doOverflow {
		// A throwing overflow.put propagates (the victim is already gone)
		// and skips the listener batch (Java flushEvicted ordering). The
		// guard is cleared on every exit, any panic payload included.
		func() {
			defer m.enterOverflow()()
			for _, v := range *victims {
				if v.reason != EvictTTL {
					ov.Put(v.key, v.value)
				}
			}
		}()
	}
	if el != nil {
		for _, v := range *victims {
			func() {
				defer m.enterEvictCallback()()
				defer func() {
					if r := recover(); r != nil {
						if e, ok := r.(error); ok {
							first = suppressErr(first, e)
							return
						}
						panic(r)
					}
				}()
				el(v.key, v.value, v.reason)
			}()
		}
	}
	// deferred listeners: the sync events already fired under the lock
	for _, v := range *victims {
		val := v.value
		if err := m.inner.seg.runtime.fireDeferred(nil, v.key, &val, nil, true); err != nil {
			first = suppressErr(first, err)
		}
	}
	return first
}

// fgMaxEvict is the per-op total-eviction cap for a FOREGROUND sweep.
func (m *HTreeCache[K, V]) fgMaxEvict() int64 {
	if m.inner.maxEvictPerOp == 0 {
		return math.MaxInt64
	}
	return m.inner.maxEvictPerOp
}

// ExpireEvict sweeps every segment now, UNBOUNDED — the escape hatch
// that reclaims every expired/over-budget entry regardless of
// MaxEvictPerOp (schedule it for background expiration; Java expireEvict).
func (m *HTreeCache[K, V]) ExpireEvict() {
	defer m.opGuard()()
	m.checkNotInCallback()
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		m.checkOpen() // a flushed listener may have retired this handle
		victims := m.newVictimList()
		err := m.runWriteFlush(seg, victims, func(win *deferWindow[K, V]) error {
			return m.evictSegment(seg, win, victims, math.MaxInt64, math.MaxInt64)
		})
		if err != nil {
			panic(err)
		}
	}
}

// runWriteFlush is runWrite plus the post-unlock victim flush: the body
// panic/error stays primary; unlock and flush failures are suppressed
// behind it (Java unlockWriteAndFlush). The flush ALWAYS runs.
func (m *HTreeCache[K, V]) runWriteFlush(seg int, victims *[]cvictim[K, V],
	f func(*deferWindow[K, V]) error) error {
	win := m.inner.seg.lockWrite(seg)
	var pval any
	var body error
	func() {
		defer func() {
			if r := recover(); r != nil {
				pval = r
			}
		}()
		body = f(win)
	}()
	derr := m.inner.seg.unlockWrite(seg, win)
	ferr := m.flushEvicted(victims)
	if pval != nil {
		if e, ok := pval.(error); ok {
			panic(errors.Join(e, suppressErr(derr, ferr)))
		}
		panic(pval)
	}
	return suppressErr(suppressErr(body, derr), ferr)
}

// ===================== get =====================

// Get returns the LIVE value for key (strict: TTL-expired reads as
// absent even before a sweep). With AccessOrder this is a WRITE: it
// bumps the hit to the queue head under the write lock, then sweeps — so
// a max-size sweep evicts some other (now oldest) entry, never the key
// just read. On a miss the overflow map and value loader are consulted
// (miss-load with putIfAbsent promotion).
func (m *HTreeCache[K, V]) Get(key K) (V, bool) {
	defer m.opGuard()()
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var hit V
	var found bool
	if m.inner.accessOrder && !m.inner.st.IsReadOnly() {
		m.checkNotInCallback() // an access-order Get is a write
		victims := m.newVictimList()
		err := m.runWriteFlush(seg, victims, func(win *deferWindow[K, V]) error {
			// lookup + bump FIRST, then sweep: the bumped hit is the newest
			// node, so the sweep evicts some OTHER (now oldest) entry.
			if leaf, ok := m.leafFor(seg, idx); ok {
				for i := range leaf.keys {
					if m.inner.keySer.Equals(leaf.keys[i], key) {
						if !m.nodeExpired(leaf.nodeRec[i]) {
							m.inner.queues[seg].Bump(leaf.nodeRec[i], uint64(m.stamp()))
							hit, found = leaf.vals[i], true
						}
						break
					}
				}
			}
			return m.evictSegment(seg, win, victims, fgStoreEvictCap, m.fgMaxEvict())
		})
		if err != nil {
			panic(err)
		}
	} else {
		func() {
			m.lockRead(seg)
			defer m.unlockRead(seg) // released before missLoad, on panic too
			if leaf, ok := m.leafFor(seg, idx); ok {
				for i := range leaf.keys {
					if m.inner.keySer.Equals(leaf.keys[i], key) {
						if !m.nodeExpired(leaf.nodeRec[i]) {
							hit, found = leaf.vals[i], true
						}
						break
					}
				}
			}
		}()
	}
	if found {
		return hit, true
	}
	return m.missLoad(key)
}

// missLoad consults the overflow map (then the value loader) on a Get
// miss, with NO lock held, and promotes a loaded value via putIfAbsent.
func (m *HTreeCache[K, V]) missLoad(key K) (V, bool) {
	var loaded V
	var ok bool
	if ov := m.getOverflow(); ov != nil && !m.inOverflowNow() {
		func() {
			defer m.enterOverflow()()
			loaded, ok = ov.Get(key)
		}()
	}
	if !ok {
		loaded, ok = m.inner.seg.runtime.load(key)
	}
	if !ok || m.inner.st.IsReadOnly() {
		return loaded, ok
	}
	m.checkOpen() // the loader ran lock-free and may have retired us
	// putIfAbsent promotion: a concurrent fresh put must win.
	m.putInternal(key, loaded, true)
	return loaded, true
}

// ContainsKey reports whether key has a LIVE entry; never bumps, never
// sweeps.
func (m *HTreeCache[K, V]) ContainsKey(key K) bool {
	defer m.opGuard()()
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	m.lockRead(seg)
	defer m.unlockRead(seg)
	if leaf, ok := m.leafFor(seg, idx); ok {
		for i := range leaf.keys {
			if m.inner.keySer.Equals(leaf.keys[i], key) {
				return !m.nodeExpired(leaf.nodeRec[i])
			}
		}
	}
	return false
}

// ===================== put =====================

// Put inserts or replaces; returns the previous LIVE value. A write
// always re-stamps the entry and bumps it to the queue head.
func (m *HTreeCache[K, V]) Put(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.putInternal(key, value, false)
}

// PutIfAbsent inserts only if absent (an expired entry counts as absent);
// returns the existing live value.
func (m *HTreeCache[K, V]) PutIfAbsent(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.putInternal(key, value, true)
}

// PutOnly is the blind put — insert/overwrite without returning the old
// value (same cost as Put; values are inline).
func (m *HTreeCache[K, V]) PutOnly(key K, value V) {
	defer m.opGuard()()
	m.putInternal(key, value, false)
}

func (m *HTreeCache[K, V]) putInternal(key K, value V, onlyIfAbsent bool) (V, bool) {
	m.checkNotInCallback()
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var old V
	var found bool
	victims := m.newVictimList()
	err := m.runWriteFlush(seg, victims, func(win *deferWindow[K, V]) error {
		if err := m.evictSegment(seg, win, victims, fgStoreEvictCap, m.fgMaxEvict()); err != nil {
			return err // sweep first; a failed sync removal aborts the write
		}
		m.checkOpen()         // a sync eviction listener may have retired this handle
		expireAt := m.stamp() // reject a bad clock before allocating
		leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx)
		if leafRecid == 0 {
			// fresh bucket: leafRecid and its queue node reference each
			// other — preallocate the leaf, enqueue, then fill (mapdb3 trick)
			leafRecid = int64(m.inner.st.Preallocate())
			nodeRecid := m.inner.queues[seg].Put(uint64(expireAt), uint64(leafRecid))
			store.Update(m.inner.st, uint64(leafRecid),
				cleaf[K, V]{keys: []K{key}, vals: []V{value}, nodeRec: []uint64{uint64(nodeRecid)}},
				m.inner.leafSer)
			TreePut(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
				m.inner.levels-1, idx, leafRecid)
			m.counterAdd(seg, +1)
			return m.inner.seg.runtime.fire(win, key, nil, &value, false)
		}
		leaf := m.loadLeaf(leafRecid)
		for i := range leaf.keys {
			if !m.inner.keySer.Equals(leaf.keys[i], key) {
				continue
			}
			// A throttled (or partial) sweep may have SKIPPED this key's
			// own expired node: a logically-absent match is rewritten in
			// place and reported as an INSERT, never revived as a hit.
			nodeRecid := leaf.nodeRec[i]
			live := !m.nodeExpired(nodeRecid)
			old = leaf.vals[i]
			if onlyIfAbsent && live {
				if m.inner.accessOrder {
					m.inner.queues[seg].Bump(nodeRecid, uint64(m.stamp()))
				}
				found = true
				return nil
			}
			m.inner.queues[seg].Bump(nodeRecid, uint64(expireAt)) // write re-stamps (revives)
			leaf2 := cleaf[K, V]{
				keys:    append([]K(nil), leaf.keys...),
				vals:    append([]V(nil), leaf.vals...),
				nodeRec: append([]uint64(nil), leaf.nodeRec...),
			}
			leaf2.vals[i] = value
			store.Update(m.inner.st, uint64(leafRecid), leaf2, m.inner.leafSer)
			found = live
			if live {
				return m.inner.seg.runtime.fire(win, key, &old, &value, false)
			}
			return m.inner.seg.runtime.fire(win, key, nil, &value, false)
		}
		// full-index hash collision: grow the bucket by one triple
		nodeRecid := m.inner.queues[seg].Put(uint64(m.stamp()), uint64(leafRecid))
		leaf.keys = append(append([]K(nil), leaf.keys...), key)
		leaf.vals = append(append([]V(nil), leaf.vals...), value)
		leaf.nodeRec = append(append([]uint64(nil), leaf.nodeRec...), uint64(nodeRecid))
		store.Update(m.inner.st, uint64(leafRecid), leaf, m.inner.leafSer)
		m.counterAdd(seg, +1)
		return m.inner.seg.runtime.fire(win, key, nil, &value, false)
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// ===================== remove / replace =====================

// Remove deletes the key's entry; returns the previous LIVE value (an
// expired entry is swept and treated as absent).
func (m *HTreeCache[K, V]) Remove(key K) (V, bool) {
	defer m.opGuard()()
	return m.removeInternal(key, nil)
}

// RemoveIfEquals removes only if the live value Equals expected.
func (m *HTreeCache[K, V]) RemoveIfEquals(key K, value V) bool {
	defer m.opGuard()()
	_, removed := m.removeInternal(key, &value)
	return removed
}

// RemoveOnly deletes the key's entry, returning only WHETHER a live one
// existed.
func (m *HTreeCache[K, V]) RemoveOnly(key K) bool {
	defer m.opGuard()()
	_, removed := m.removeInternal(key, nil)
	return removed
}

func (m *HTreeCache[K, V]) removeInternal(key K, expectedValue *V) (V, bool) {
	m.checkNotInCallback()
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var old V
	var removed bool
	victims := m.newVictimList()
	err := m.runWriteFlush(seg, victims, func(win *deferWindow[K, V]) error {
		if err := m.evictSegment(seg, win, victims, fgStoreEvictCap, m.fgMaxEvict()); err != nil {
			return err
		}
		m.checkOpen() // a sync eviction listener may have retired this handle
		leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx)
		if leafRecid == 0 {
			return nil
		}
		leaf := m.loadLeaf(leafRecid)
		for i := range leaf.keys {
			if !m.inner.keySer.Equals(leaf.keys[i], key) {
				continue
			}
			// A throttled sweep may have SKIPPED this key's expired node:
			// treat it as absent and leave it for a later sweep.
			if m.nodeExpired(leaf.nodeRec[i]) {
				return nil
			}
			old = leaf.vals[i]
			if expectedValue != nil && !m.inner.valueSer.Equals(old, *expectedValue) {
				return nil
			}
			m.inner.queues[seg].Remove(leaf.nodeRec[i], true)
			if len(leaf.keys) == 1 {
				if !TreeRemove(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
					m.inner.levels-1, idx) {
					panic(mapdb5.NewCorruption("dir tree lost the bucket it just resolved"))
				}
				store.Delete(m.inner.st, uint64(leafRecid), m.inner.leafSer)
			} else {
				store.Update(m.inner.st, uint64(leafRecid), leafDelete(leaf, i), m.inner.leafSer)
			}
			m.counterAdd(seg, -1)
			removed = true
			return m.inner.seg.runtime.fire(win, key, &old, nil, false)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return old, removed
}

// Replace replaces only if present and live; returns the previous value.
// A replace re-stamps and bumps like Put.
func (m *HTreeCache[K, V]) Replace(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.replaceInternal(key, nil, value)
}

// ReplaceIfEquals replaces only if the live value Equals oldValue.
func (m *HTreeCache[K, V]) ReplaceIfEquals(key K, oldValue, newValue V) bool {
	defer m.opGuard()()
	_, replaced := m.replaceInternal(key, &oldValue, newValue)
	return replaced
}

func (m *HTreeCache[K, V]) replaceInternal(key K, expectedValue *V, newValue V) (V, bool) {
	m.checkNotInCallback()
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var old V
	var found bool
	victims := m.newVictimList()
	err := m.runWriteFlush(seg, victims, func(win *deferWindow[K, V]) error {
		if err := m.evictSegment(seg, win, victims, fgStoreEvictCap, m.fgMaxEvict()); err != nil {
			return err
		}
		m.checkOpen() // a sync eviction listener may have retired this handle
		expireAt := m.stamp()
		leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx)
		if leafRecid == 0 {
			return nil
		}
		leaf := m.loadLeaf(leafRecid)
		for i := range leaf.keys {
			if !m.inner.keySer.Equals(leaf.keys[i], key) {
				continue
			}
			if m.nodeExpired(leaf.nodeRec[i]) {
				return nil // expired match is absent
			}
			old = leaf.vals[i]
			if expectedValue != nil && !m.inner.valueSer.Equals(old, *expectedValue) {
				return nil
			}
			m.inner.queues[seg].Bump(leaf.nodeRec[i], uint64(expireAt))
			leaf2 := cleaf[K, V]{
				keys:    append([]K(nil), leaf.keys...),
				vals:    append([]V(nil), leaf.vals...),
				nodeRec: append([]uint64(nil), leaf.nodeRec...),
			}
			leaf2.vals[i] = newValue
			store.Update(m.inner.st, uint64(leafRecid), leaf2, m.inner.leafSer)
			found = true
			return m.inner.seg.runtime.fire(win, key, &old, &newValue, false)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// ===================== clear =====================

// Clear removes all entries (and every queue node), zeroing counters.
func (m *HTreeCache[K, V]) Clear() {
	m.clearInternal(true, false)
}

// ClearWithoutNotification clears without modification callbacks.
func (m *HTreeCache[K, V]) ClearWithoutNotification() {
	m.clearInternal(false, false)
}

// ClearWithExpire clears marking every callback as triggered.
func (m *HTreeCache[K, V]) ClearWithExpire() {
	m.clearInternal(true, true)
}

func (m *HTreeCache[K, V]) clearInternal(notify, triggered bool) {
	defer m.opGuard()()
	m.checkNotInCallback()
	var listenerFail error
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		m.checkOpen()
		win := m.inner.seg.lockWrite(seg)
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
						leaf := m.loadLeaf(leafRecid)
						for i := range leaf.keys {
							v := leaf.vals[i]
							removed = append(removed, hevent[K, V]{
								key: leaf.keys[i], old: &v, trig: triggered})
						}
					}
					store.Delete(m.inner.st, uint64(leafRecid), m.inner.leafSer)
				})
			m.inner.queues[seg].Clear()
			m.inner.counters[seg].Store(0)
		}()
		if traverseErr != nil {
			derr := m.inner.seg.unlockWrite(seg, win)
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
		if err := m.inner.seg.unlockWrite(seg, win); err != nil {
			listenerFail = suppressErr(listenerFail, err)
		}
	}
	if listenerFail != nil {
		panic(listenerFail)
	}
}

// ===================== size / iteration =====================

// LenLong is the per-segment counter sum — PHYSICAL occupancy including
// un-swept expired entries (run ExpireEvict first for an exact live
// count).
func (m *HTreeCache[K, V]) LenLong() int64 {
	defer m.opGuard()()
	var total int64
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		total += m.inner.counters[seg].Load()
	}
	return total
}

// Len saturates at math.MaxInt.
func (m *HTreeCache[K, V]) Len() int {
	n := m.LenLong()
	if n > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(n)
}

// IsEmpty is counter-based physical occupancy (un-swept expired entries
// count).
func (m *HTreeCache[K, V]) IsEmpty() bool {
	defer m.opGuard()()
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		if m.inner.counters[seg].Load() != 0 {
			return false
		}
	}
	return true
}

// drainSegment snapshots one segment's LIVE entries under its read lock
// (TTL-expired skipped, strict reads).
func (m *HTreeCache[K, V]) drainSegment(seg int) hleaf[K, V] {
	var out hleaf[K, V]
	m.lockRead(seg)
	defer m.unlockRead(seg)
	TreeFold(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
		func(index, leafRecid int64) {
			leaf := m.loadLeaf(leafRecid)
			for i := range leaf.keys {
				if m.nodeExpired(leaf.nodeRec[i]) {
					continue
				}
				out.keys = append(out.keys, leaf.keys[i])
				out.vals = append(out.vals, leaf.vals[i])
			}
		})
	return out
}

// snapshotSegment drains one segment under a snapshot-scoped admission,
// released on every exit (never held across a yield: iter.Pull2 + Close).
func (m *HTreeCache[K, V]) snapshotSegment(seg int) hleaf[K, V] {
	defer m.opGuard()()
	return m.drainSegment(seg)
}

// All iterates LIVE entries, snapshot-per-segment (weakly consistent;
// callbacks may mutate the map). Admission is snapshot-scoped.
func (m *HTreeCache[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
			snap := m.snapshotSegment(seg)
			for i := range snap.keys {
				if !yield(snap.keys[i], snap.vals[i]) {
					return
				}
				m.checkOpen()
			}
		}
	}
}

// Keys iterates live keys.
func (m *HTreeCache[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.All()(func(k K, _ V) bool { return yield(k) })
	}
}

// Values iterates live values.
func (m *HTreeCache[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.All()(func(_ K, v V) bool { return yield(v) })
	}
}

// ForEach runs the callback outside segment locks.
func (m *HTreeCache[K, V]) ForEach(f func(K, V)) {
	m.All()(func(k K, v V) bool { f(k, v); return true })
}

// ===================== listeners / loader =====================

// ModificationListenerAdd registers a listener.
func (m *HTreeCache[K, V]) ModificationListenerAdd(l mapdb5.MapModificationListener[K, V]) {
	defer m.opGuard()()
	m.inner.seg.runtime.add(l)
}

// ModificationListenerRemove unregisters a listener.
func (m *HTreeCache[K, V]) ModificationListenerRemove(l mapdb5.MapModificationListener[K, V]) {
	m.inner.seg.runtime.remove(l)
}

// ValueLoader installs the missing-key loader consulted after the
// overflow map on a Get miss.
func (m *HTreeCache[K, V]) ValueLoader(f func(K) (V, bool)) {
	if f == nil {
		m.inner.seg.runtime.valueLoader.Store(nil)
		return
	}
	m.inner.seg.runtime.valueLoader.Store(&f)
}
