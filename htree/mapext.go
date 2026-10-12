package htree

// HTreeMapExternal — port of Java org.mapdb.htree.HTreeMapExternal: the
// HTreeMap variant with EXTERNAL values. The bucket stores (key,
// valueRecid) pairs; each value lives in its own store record. A
// deliberate, self-contained duplicate of the lean inline-value map
// (Java keeps them as separate classes rather than feature flags in hot
// paths); DirTree, segRuntime and the header codec are shared unchanged.
//
// The trade vs inline values:
//   - bucket scans deserialize KEYS ONLY — a collision bucket holding
//     megabyte values costs the same to scan as one holding bytes;
//   - overwrites/replaces rewrite ONLY the value record, never the bucket;
//   - every hit pays one extra record read (recid → value).
//
// Value records are owned by their entry: put allocates, overwrite updates
// in place, remove/clear delete. Value reads always happen under the
// segment read lock — a resolved recid must not outlive the lock or a
// concurrent remove could free it mid-read.

import (
	"errors"
	"iter"
	"math"
	"math/rand"
	"sync/atomic"

	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/internal/handle"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// eleaf is one external bucket record: parallel key/value-recid slices
// (Java's Object[]{k0, valueRecid0, k1, valueRecid1, …}).
type eleaf[K any] struct {
	keys    []K
	vrecids []uint64
}

// eleafSerializer is the external bucket wire codec: packInt(count) then
// (key, packLong(valueRecid)) per entry (Java leafSerializer(keySer)).
type eleafSerializer[K any] struct {
	keySer ser.Serializer[K]
}

var _ = ser.Serializer[eleaf[int64]](eleafSerializer[int64]{})

func (s eleafSerializer[K]) Serialize(w *bin.Writer, leaf eleaf[K]) {
	if len(leaf.keys) != len(leaf.vrecids) || len(leaf.keys) == 0 {
		panic(mapdb5.NewMisuse("htree external leaf must hold >= 1 paired entries"))
	}
	w.PackInt(int32(len(leaf.keys)))
	for i := range leaf.keys {
		s.keySer.Serialize(w, leaf.keys[i])
		w.PackLong(int64(leaf.vrecids[i]))
	}
}

func (s eleafSerializer[K]) Deserialize(r *bin.Reader, size int) eleaf[K] {
	n := int(r.UnpackInt())
	if n < 1 || (size >= 0 && n > size) {
		panic(mapdb5.NewCorruption("htree external leaf entry count out of range"))
	}
	leaf := eleaf[K]{keys: make([]K, n), vrecids: make([]uint64, n)}
	for i := 0; i < n; i++ {
		leaf.keys[i] = s.keySer.Deserialize(r, -1)
		v := r.UnpackLong()
		if v <= 0 {
			panic(mapdb5.NewCorruption("htree external leaf value recid out of range"))
		}
		leaf.vrecids[i] = uint64(v)
	}
	return leaf
}

func (s eleafSerializer[K]) FixedSize() int { return -1 }
func (s eleafSerializer[K]) SizeHint() int  { return 34 }
func (s eleafSerializer[K]) Compare(a, b eleaf[K]) int {
	panic(mapdb5.NewMisuse("htree external leaf codec is record-internal, not orderable"))
}
func (s eleafSerializer[K]) Equals(a, b eleaf[K]) bool {
	panic(mapdb5.NewMisuse("htree external leaf codec is record-internal, not orderable"))
}
func (s eleafSerializer[K]) NaturalOrder() bool            { return false }
func (s eleafSerializer[K]) EqualsBySerializedBytes() bool { return true }

type einner[K, V any] struct {
	st           store.Store
	keySer       ser.Serializer[K]
	valueSer     ser.Serializer[V]
	concShift    int
	dirShift     int
	levels       int
	hashSeed     int64
	headerRecid  store.Recid
	segmentRoots []uint64
	hashOf       func(K) int64
	seg          segRuntime[K, V]
	segmentShift int
	indexMask    int64
	concMask     int
	eleafSer     eleafSerializer[K]
	closed       atomic.Bool
	ops          handle.Tracker
	lease        *store.LeaseGuard
	id           uint64
}

// HTreeMapExternal is the external-value segmented hash map. Its zero
// value is unusable; construct via NewHTreeMapExternal/
// OpenHTreeMapExternal/CreateHTreeMapExternalFromSortedByHash.
type HTreeMapExternal[K, V any] struct {
	inner *einner[K, V]
}

// ===================== construction =====================

func newEInner[K, V any](s store.Store, cfg HConfig[K, V], headerRecid store.Recid,
	h hHeader, lease *store.LeaseGuard) *einner[K, V] {
	hasher := resolveHasher(cfg)
	nseg := 1 << h.concShift
	seed := h.hashSeed
	return &einner[K, V]{
		st:           s,
		keySer:       cfg.KeySer,
		valueSer:     cfg.ValueSer,
		concShift:    int(h.concShift),
		dirShift:     int(h.dirShift),
		levels:       int(h.levels),
		hashSeed:     int64(seed),
		headerRecid:  headerRecid,
		segmentRoots: h.segmentRoots,
		hashOf: func(k K) int64 {
			return int64(uint32(hasher.Hash(k, seed)))
		},
		seg:          newSegRuntime[K, V](nseg, s.IsThreadSafe()),
		segmentShift: int(h.levels) * int(h.dirShift),
		indexMask:    (int64(1) << (h.levels * h.dirShift)) - 1,
		concMask:     nseg - 1,
		eleafSer:     eleafSerializer[K]{keySer: cfg.KeySer},
		id:           htreeStateIDs.Add(1),
		lease:        lease,
	}
}

// NewHTreeMapExternal creates a fresh empty external-value map over s
// (same HConfig geometry/hashing contract as HTreeMap).
func NewHTreeMapExternal[K, V any](s store.Store, cfg HConfig[K, V]) *HTreeMapExternal[K, V] {
	m, err := createHTreeMapExternal(s, cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func createHTreeMapExternal[K, V any](s store.Store, cfg HConfig[K, V]) (*HTreeMapExternal[K, V], error) {
	concShift, dirShift, levels, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree external: KeySer and ValueSer are required")
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
	return openHTreeMapExternal(s, headerRecid, cfg)
}

// OpenHTreeMapExternal reopens the external-value map at headerRecid.
func OpenHTreeMapExternal[K, V any](s store.Store, headerRecid store.Recid, cfg HConfig[K, V]) (m *HTreeMapExternal[K, V], err error) {
	defer dbErrorToErr(&err)
	return openHTreeMapExternal(s, headerRecid, cfg)
}

func openHTreeMapExternal[K, V any](s store.Store, headerRecid store.Recid, cfg HConfig[K, V]) (*HTreeMapExternal[K, V], error) {
	if headerRecid == 0 {
		return nil, mapdb5.NewCorruption("htree external header recid must be a valid recid")
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree external: KeySer and ValueSer are required")
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
		return nil, mapdb5.NewCorruption("htree external header record is null")
	}
	if err := checkConfig(int(hdr.concShift), int(hdr.dirShift), int(hdr.levels)); err != nil {
		return nil, mapdb5.NewCorruption("htree external header geometry invalid: " + err.Error())
	}
	if len(hdr.segmentRoots) != 1<<hdr.concShift {
		return nil, mapdb5.NewCorruption("htree external header segment root count mismatch")
	}
	committed = true
	return &HTreeMapExternal[K, V]{inner: newEInner(s, cfg, headerRecid, hdr, lease)}, nil
}

// CreateHTreeMapExternalFromSortedByHash bulk-loads an external-value map
// from entries sorted by non-decreasing unsigned 32-bit placement hash.
// Values are written before the bucket records that reference them (Java).
func CreateHTreeMapExternalFromSortedByHash[K, V any](s store.Store, cfg HConfig[K, V],
	entries iter.Seq2[K, V]) (m *HTreeMapExternal[K, V], err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				if de.Kind == mapdb5.KindNotSorted {
					err = pump.ErrNotSorted
					return
				}
				err = de
				return
			}
			panic(r)
		}
	}()
	concShift, dirShift, levels, err2 := resolveConfig(cfg)
	if err2 != nil {
		return nil, err2
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree external: KeySer and ValueSer are required")
	}
	seed := cfg.HashSeed
	if !cfg.HashSeedSet {
		seed = int32(rand.Uint32())
	}
	hasher := resolveHasher(cfg)
	eleafSer := eleafSerializer[K]{keySer: cfg.KeySer}
	hashSource := func(key K) int64 {
		return int64(uint32(hasher.Hash(key, seed)))
	}
	headerRecid := htreeBuild(s, concShift, dirShift, levels, entries,
		hashSource, cfg.KeySer,
		func(keys []K, vals []V) uint64 {
			// values are written BEFORE the bucket that references them
			vrecids := make([]uint64, len(vals))
			for i, v := range vals {
				vrecids[i] = store.Put(s, v, cfg.ValueSer)
			}
			return store.Put(s, eleaf[K]{keys: keys, vrecids: vrecids}, eleafSer)
		},
		func(roots []uint64) uint64 {
			return store.Put(s, hHeader{
				concShift: int32(concShift), dirShift: int32(dirShift), levels: int32(levels),
				hashSeed: seed, segmentRoots: roots,
			}, headerSerializer{})
		})
	return openHTreeMapExternal(s, headerRecid, cfg)
}

// ===================== accessors / lifecycle =====================

// HeaderRecid is the recid of the header record.
func (m *HTreeMapExternal[K, V]) HeaderRecid() store.Recid { return m.inner.headerRecid }

// HashSeed is the persisted 32-bit placement-hash seed.
func (m *HTreeMapExternal[K, V]) HashSeed() int32 { return int32(m.inner.hashSeed) }

// Segments is the physical segment count.
func (m *HTreeMapExternal[K, V]) Segments() int { return len(m.inner.segmentRoots) }

// IsClosed reports whether this handle was retired or the store closed.
func (m *HTreeMapExternal[K, V]) IsClosed() bool {
	return m.inner.closed.Load() || m.inner.st.IsClosed()
}

func (m *HTreeMapExternal[K, V]) KeySerializer() ser.Serializer[K]     { return m.inner.keySer }
func (m *HTreeMapExternal[K, V]) ValueSerializer() ser.Serializer[V]   { return m.inner.valueSer }
func (m *HTreeMapExternal[K, V]) StateID() uint64                      { return m.inner.id }
func (m *HTreeMapExternal[K, V]) SharesStateWith(o *HTreeMapExternal[K, V]) bool {
	return m.inner == o.inner
}

// Close releases the open lease and invalidates the handle. Idempotent.
func (m *HTreeMapExternal[K, V]) Close() error {
	m.CloseHandle()
	return nil
}

// CloseHandle is the DB-owned retirement path (HTreeMap.CloseHandle).
func (m *HTreeMapExternal[K, V]) CloseHandle() {
	if m.inner.closed.CompareAndSwap(false, true) {
		m.inner.ops.DropSelf()
		m.inner.ops.Wait()
		if l := m.inner.lease; l != nil {
			l.Release()
		}
	}
}

func (m *HTreeMapExternal[K, V]) opGuard() func() {
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

func (m *HTreeMapExternal[K, V]) checkOpen() {
	if m.inner.closed.Load() {
		panicHClosed()
	}
}

// ===================== hashing / locks =====================

func (m *HTreeMapExternal[K, V]) hash(key K) int64 { return m.inner.hashOf(key) }

func (m *HTreeMapExternal[K, V]) segment(h int64) int {
	return int(h>>uint(m.inner.segmentShift)) & m.inner.concMask
}

func (m *HTreeMapExternal[K, V]) index(h int64) int64 { return h & m.inner.indexMask }

func (m *HTreeMapExternal[K, V]) lockRead(seg int)   { m.inner.seg.lockRead(seg) }
func (m *HTreeMapExternal[K, V]) unlockRead(seg int) { m.inner.seg.unlockRead(seg) }
func (m *HTreeMapExternal[K, V]) lockWrite(seg int) *deferWindow[K, V] {
	return m.inner.seg.lockWrite(seg)
}
func (m *HTreeMapExternal[K, V]) unlockWrite(seg int, win *deferWindow[K, V]) error {
	return m.inner.seg.unlockWrite(seg, win)
}
func (m *HTreeMapExternal[K, V]) runWrite(seg int, f func(*deferWindow[K, V]) error) error {
	return m.inner.seg.runWrite(seg, f)
}

// findValueRecid scans the bucket for key and returns its value recid
// (0 = absent; recid 0 is the store's null sentinel). Caller holds the
// segment lock.
func (m *HTreeMapExternal[K, V]) findValueRecid(leaf eleaf[K], key K) uint64 {
	for i := range leaf.keys {
		if m.inner.keySer.Equals(leaf.keys[i], key) {
			return leaf.vrecids[i]
		}
	}
	return 0
}

func (m *HTreeMapExternal[K, V]) loadLeaf(leafRecid int64) eleaf[K] {
	leaf, ok := store.Get(m.inner.st, uint64(leafRecid), m.inner.eleafSer)
	if !ok {
		panic(mapdb5.NewCorruption("dir tree points at a missing bucket record"))
	}
	return leaf
}

func (m *HTreeMapExternal[K, V]) loadValue(vrecid uint64) (V, bool) {
	return store.Get(m.inner.st, vrecid, m.inner.valueSer)
}

// ===================== get =====================

func (m *HTreeMapExternal[K, V]) getRaw(key K) (V, bool) {
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
	vrecid := m.findValueRecid(m.loadLeaf(leafRecid), key)
	if vrecid == 0 {
		return zero, false
	}
	// resolve INSIDE the read lock: the recid must not outlive it
	return m.loadValue(vrecid)
}

// Get returns the value for key; a missing key consults the value loader.
func (m *HTreeMapExternal[K, V]) Get(key K) (V, bool) {
	defer m.opGuard()()
	if v, ok := m.getRaw(key); ok {
		return v, true
	}
	loaded, ok := m.inner.seg.runtime.load(key)
	if !ok || m.inner.st.IsReadOnly() {
		return loaded, ok
	}
	// The loader ran lock-free and may have retired this handle: re-check
	// before committing the loaded value (see HTreeMap.Get).
	m.checkOpen()
	existing, exists := m.putInternal(key, loaded, true, false)
	if !exists {
		return loaded, true
	}
	return existing, true
}

// ContainsKey reports whether key has an entry; never reads a value
// record (Java containsKey).
func (m *HTreeMapExternal[K, V]) ContainsKey(key K) bool {
	defer m.opGuard()()
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	m.lockRead(seg)
	defer m.unlockRead(seg)
	leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
		m.inner.levels-1, idx)
	if leafRecid == 0 {
		return false
	}
	return m.findValueRecid(m.loadLeaf(leafRecid), key) != 0
}

// ===================== put =====================

// Put inserts or replaces; returns the previous value. Overwrite rewrites
// only the value record — the bucket is untouched.
func (m *HTreeMapExternal[K, V]) Put(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.putInternal(key, value, false, false)
}

// PutIfAbsent inserts only if absent; returns the existing value.
func (m *HTreeMapExternal[K, V]) PutIfAbsent(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.putInternal(key, value, true, false)
}

// PutOnly is the blind put: an overwrite does NOT read the old value
// record when no listeners are registered — the payoff of external
// values (Java putOnly).
func (m *HTreeMapExternal[K, V]) PutOnly(key K, value V) {
	defer m.opGuard()()
	m.putInternal(key, value, false, !m.inner.seg.runtime.hasListeners())
}

func (m *HTreeMapExternal[K, V]) putInternal(key K, value V, onlyIfAbsent, blind bool) (V, bool) {
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	var old V
	var found bool
	err := m.runWrite(seg, func(win *deferWindow[K, V]) error {
		leafRecid := TreeGet(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx)
		if leafRecid == 0 {
			// fresh bucket: value record first, then leaf, then link
			vrecid := store.Put(m.inner.st, value, m.inner.valueSer)
			leafRecid = int64(store.Put(m.inner.st,
				eleaf[K]{keys: []K{key}, vrecids: []uint64{vrecid}}, m.inner.eleafSer))
			TreePut(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
				m.inner.levels-1, idx, leafRecid)
			return m.inner.seg.runtime.fire(win, key, nil, &value, false)
		}
		leaf := m.loadLeaf(leafRecid)
		for i := range leaf.keys {
			if m.inner.keySer.Equals(leaf.keys[i], key) {
				vrecid := leaf.vrecids[i]
				if blind && !m.inner.seg.runtime.hasListeners() {
					// overwrite without reading the old value record
					store.Update(m.inner.st, vrecid, value, m.inner.valueSer)
					return nil
				}
				var ok bool
				old, ok = m.loadValue(vrecid)
				if !ok {
					panic(mapdb5.NewCorruption("bucket points at a missing value record"))
				}
				found = true
				if onlyIfAbsent {
					return nil
				}
				store.Update(m.inner.st, vrecid, value, m.inner.valueSer) // leaf untouched
				return m.inner.seg.runtime.fire(win, key, &old, &value, false)
			}
		}
		// full-index hash collision: grow the bucket by one entry
		vrecid := store.Put(m.inner.st, value, m.inner.valueSer)
		leaf.keys = append(append([]K(nil), leaf.keys...), key)
		leaf.vrecids = append(append([]uint64(nil), leaf.vrecids...), vrecid)
		store.Update(m.inner.st, uint64(leafRecid), leaf, m.inner.eleafSer)
		return m.inner.seg.runtime.fire(win, key, nil, &value, false)
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// ===================== remove / replace =====================

// Remove deletes the key's entry (and its value record); returns the
// previous value.
func (m *HTreeMapExternal[K, V]) Remove(key K) (V, bool) {
	defer m.opGuard()()
	return m.removeInternal(key, nil)
}

// RemoveIfEquals removes only if the current value Equals expected.
func (m *HTreeMapExternal[K, V]) RemoveIfEquals(key K, value V) bool {
	defer m.opGuard()()
	_, removed := m.removeInternal(key, &value)
	return removed
}

// RemoveOnly deletes the key's entry returning only WHETHER one existed —
// when no listeners are registered it does NOT read the old value record
// before freeing it (Java removeOnly fast path).
func (m *HTreeMapExternal[K, V]) RemoveOnly(key K) bool {
	defer m.opGuard()()
	if m.inner.seg.runtime.hasListeners() {
		_, removed := m.removeInternal(key, nil)
		return removed
	}
	h := m.hash(key)
	seg := m.segment(h)
	idx := m.index(h)
	removed := false
	err := m.runWrite(seg, func(win *deferWindow[K, V]) error {
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
			m.deleteEntryAt(seg, leaf, leafRecid, i, idx) // frees value record, no read
			removed = true
			return nil
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return removed
}

func (m *HTreeMapExternal[K, V]) removeInternal(key K, expectedValue *V) (V, bool) {
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
		leaf := m.loadLeaf(leafRecid)
		for i := range leaf.keys {
			if !m.inner.keySer.Equals(leaf.keys[i], key) {
				continue
			}
			vrecid := leaf.vrecids[i]
			var ok bool
			old, ok = m.loadValue(vrecid)
			if !ok {
				panic(mapdb5.NewCorruption("bucket points at a missing value record"))
			}
			found = true
			if expectedValue != nil && !m.inner.valueSer.Equals(old, *expectedValue) {
				found = false
				return nil
			}
			m.deleteEntryAt(seg, leaf, leafRecid, i, idx)
			return m.inner.seg.runtime.fire(win, key, &old, nil, false)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// deleteEntryAt frees entry i's value record and unlinks it (collapse the
// bucket, or shrink it by one pair). Caller holds the segment write lock.
func (m *HTreeMapExternal[K, V]) deleteEntryAt(seg int, leaf eleaf[K], leafRecid int64, i int, idx int64) {
	store.Delete(m.inner.st, leaf.vrecids[i], m.inner.valueSer) // entry owns its value record
	if len(leaf.keys) == 1 {
		if !TreeRemove(m.inner.dirShift, m.inner.segmentRoots[seg], m.inner.st,
			m.inner.levels-1, idx) {
			panic(mapdb5.NewCorruption("dir tree lost the bucket it just resolved"))
		}
		store.Delete(m.inner.st, uint64(leafRecid), m.inner.eleafSer)
		return
	}
	keys := make([]K, 0, len(leaf.keys)-1)
	vrecids := make([]uint64, 0, len(leaf.vrecids)-1)
	keys = append(keys, leaf.keys[:i]...)
	keys = append(keys, leaf.keys[i+1:]...)
	vrecids = append(vrecids, leaf.vrecids[:i]...)
	vrecids = append(vrecids, leaf.vrecids[i+1:]...)
	store.Update(m.inner.st, uint64(leafRecid), eleaf[K]{keys, vrecids}, m.inner.eleafSer)
}

// Replace replaces only if present; returns the previous value.
func (m *HTreeMapExternal[K, V]) Replace(key K, value V) (V, bool) {
	defer m.opGuard()()
	return m.replaceInternal(key, nil, value)
}

// ReplaceIfEquals replaces only if the current value Equals oldValue.
func (m *HTreeMapExternal[K, V]) ReplaceIfEquals(key K, oldValue, newValue V) bool {
	defer m.opGuard()()
	_, replaced := m.replaceInternal(key, &oldValue, newValue)
	return replaced
}

func (m *HTreeMapExternal[K, V]) replaceInternal(key K, expectedValue *V, newValue V) (V, bool) {
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
		vrecid := m.findValueRecid(m.loadLeaf(leafRecid), key)
		if vrecid == 0 {
			return nil
		}
		var ok bool
		old, ok = m.loadValue(vrecid)
		if !ok {
			panic(mapdb5.NewCorruption("bucket points at a missing value record"))
		}
		found = true
		if expectedValue != nil && !m.inner.valueSer.Equals(old, *expectedValue) {
			found = false
			return nil
		}
		store.Update(m.inner.st, vrecid, newValue, m.inner.valueSer) // leaf untouched
		return m.inner.seg.runtime.fire(win, key, &old, &newValue, false)
	})
	if err != nil {
		panic(err)
	}
	return old, found
}

// ===================== clear =====================

// Clear removes all entries, freeing every value record, bucket record
// and subdir record.
func (m *HTreeMapExternal[K, V]) Clear() {
	m.clearInternal(true, false)
}

// ClearWithoutNotification clears without modification callbacks.
func (m *HTreeMapExternal[K, V]) ClearWithoutNotification() {
	m.clearInternal(false, false)
}

// ClearWithExpire clears marking every callback as triggered.
func (m *HTreeMapExternal[K, V]) ClearWithExpire() {
	m.clearInternal(true, true)
}

// clearInternal — same listener-vs-mutation failure discipline as
// HTreeMap.clearInternal (listener failure must not strand later
// segments' allocated value records; a store failure aborts).
func (m *HTreeMapExternal[K, V]) clearInternal(notify, triggered bool) {
	defer m.opGuard()()
	var listenerFail error
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		// A listener firing for an earlier segment may have retired this
		// handle: the retired op must not keep mutating (see HTreeMap).
		m.checkOpen()
		win := m.lockWrite(seg)
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
					leaf := m.loadLeaf(leafRecid)
					for i := range leaf.keys {
						vrecid := leaf.vrecids[i]
						if notify {
							v, ok := m.loadValue(vrecid)
							if !ok {
								panic(mapdb5.NewCorruption("bucket points at a missing value record"))
							}
							removed = append(removed, hevent[K, V]{
								key: leaf.keys[i], old: &v, trig: triggered})
						}
						store.Delete(m.inner.st, vrecid, m.inner.valueSer)
					}
					store.Delete(m.inner.st, uint64(leafRecid), m.inner.eleafSer)
				})
		}()
		if traverseErr != nil {
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

// IsEmpty reports whether the map has no entries (O(segments) root
// bitmap reads).
func (m *HTreeMapExternal[K, V]) IsEmpty() bool {
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

// LenLong is the exact entry count (bucket sizes summed per segment).
func (m *HTreeMapExternal[K, V]) LenLong() int64 {
	defer m.opGuard()()
	var total int64
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		func() {
			m.lockRead(seg)
			defer m.unlockRead(seg) // released on panic paths too
			TreeFold(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
				func(index, leafRecid int64) {
					total += int64(len(m.loadLeaf(leafRecid).keys))
				})
		}()
	}
	return total
}

// Len saturates at math.MaxInt.
func (m *HTreeMapExternal[K, V]) Len() int {
	n := m.LenLong()
	if n > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(n)
}

// ===================== iteration =====================

// drainSegment snapshots one segment's entries under its read lock;
// values are resolved inside the lock so their recids are still live.
func (m *HTreeMapExternal[K, V]) drainSegment(seg int) hleaf[K, V] {
	var out hleaf[K, V]
	m.lockRead(seg)
	defer m.unlockRead(seg)
	TreeFold(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
		func(index, leafRecid int64) {
			leaf := m.loadLeaf(leafRecid)
			for i := range leaf.keys {
				v, ok := m.loadValue(leaf.vrecids[i])
				if !ok {
					panic(mapdb5.NewCorruption("bucket points at a missing value record"))
				}
				out.keys = append(out.keys, leaf.keys[i])
				out.vals = append(out.vals, v)
			}
		})
	return out
}

func (m *HTreeMapExternal[K, V]) yieldSnapshot(snap hleaf[K, V], yield func(K, V) bool) bool {
	for i := range snap.keys {
		if !yield(snap.keys[i], snap.vals[i]) {
			return false
		}
		m.checkOpen()
	}
	return true
}

// All is the unordered snapshot-per-segment entry iterator (weakly
// consistent; callbacks may mutate the map). Admission is scoped to the
// segment snapshot — holding it across yields would deadlock Close on a
// suspended pull iterator (see HTreeMap.All).
func (m *HTreeMapExternal[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
			release := m.opGuard()
			snap := m.drainSegment(seg)
			release()
			if !m.yieldSnapshot(snap, yield) {
				return
			}
		}
	}
}

// Keys iterates keys.
func (m *HTreeMapExternal[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.All()(func(k K, _ V) bool { return yield(k) })
	}
}

// Values iterates values.
func (m *HTreeMapExternal[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.All()(func(_ K, v V) bool { return yield(v) })
	}
}

// ForEach runs the callback outside segment locks.
func (m *HTreeMapExternal[K, V]) ForEach(f func(K, V)) {
	m.All()(func(k K, v V) bool { f(k, v); return true })
}

// Segments2 returns k <= max(1, n) non-overlapping weakly consistent
// views covering All() (same partition contract as HTreeMap.Segments2).
func (m *HTreeMapExternal[K, V]) Segments2(n int) []iter.Seq2[K, V] {
	nseg := len(m.inner.segmentRoots)
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

// allRootsEmpty — see HTreeMap.allRootsEmpty (same contract).
func (m *HTreeMapExternal[K, V]) allRootsEmpty() bool {
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

// ModificationListenerAdd registers a listener.
func (m *HTreeMapExternal[K, V]) ModificationListenerAdd(l mapdb5.MapModificationListener[K, V]) {
	defer m.opGuard()()
	m.inner.seg.runtime.add(l)
}

// ModificationListenerRemove unregisters a listener.
func (m *HTreeMapExternal[K, V]) ModificationListenerRemove(l mapdb5.MapModificationListener[K, V]) {
	m.inner.seg.runtime.remove(l)
}

// ValueLoader installs the missing-key loader consulted by Get.
func (m *HTreeMapExternal[K, V]) ValueLoader(f func(K) (V, bool)) {
	if f == nil {
		m.inner.seg.runtime.valueLoader.Store(nil)
		return
	}
	m.inner.seg.runtime.valueLoader.Store(&f)
}

// ===================== verify =====================

// Verify checks structural consistency: each bucket resolves, every
// key's placement hash routes to its segment and stored index, and every
// value recid resolves to a live record.
func (m *HTreeMapExternal[K, V]) Verify() {
	defer m.opGuard()()
	for seg := 0; seg < len(m.inner.segmentRoots); seg++ {
		seg := seg
		func() {
			m.lockRead(seg)
			defer m.unlockRead(seg) // released on corruption panic too
			TreeFold(m.inner.segmentRoots[seg], m.inner.st, m.inner.levels-1,
				func(storedIndex, leafRecid int64) {
					leaf := m.loadLeaf(leafRecid)
					if len(leaf.keys) == 0 {
						panic(mapdb5.NewCorruption("empty bucket record"))
					}
					for i := range leaf.keys {
						h := m.hash(leaf.keys[i])
						if m.segment(h) != seg || m.index(h) != storedIndex {
							panic(mapdb5.NewCorruption("bucket key placement mismatch"))
						}
						if _, ok := m.loadValue(leaf.vrecids[i]); !ok {
							panic(mapdb5.NewCorruption("bucket points at a missing value record"))
						}
					}
				})
		}()
	}
}
