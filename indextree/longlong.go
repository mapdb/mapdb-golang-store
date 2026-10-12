// Package indextree is the persistent positional/primitive collection
// family over the sparse bitmap-compressed DirTree shared with htree
// (Java org.mapdb.indextree).
//
// IndexTreeLongLongMap: primitive int64→int64 map. The DirTree maps
// key → valueRecid; each value is its own 8-byte record, so the full
// int64 value range (including 0 and negatives) is representable — the
// tree slot itself can only hold a non-zero recid. Key domain is
// [0, MaxKey] (one below MaxInt64 so the DirTree's key+1 terminal
// sentinel stays non-negative).
//
// IndexTreeList: positional list; a position maps through DirTree to the
// element record's recid directly (element recids ARE non-zero values,
// so no indirection record is needed). Positional insert/remove re-keys
// the tail of the index→recid mapping, keeping positions contiguous.
package indextree

import (
	"math"
	"sync"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/htree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// DirTree geometry shared by both collections: 9 levels × 7 bits = 63
// index bits — every non-negative int64 is addressable (Java
// IndexTreeLongLongMap.DIR_SHIFT/LEVELS).
const (
	DirShift = 7
	Levels   = 9
)

// MaxKey is the largest permitted key: one below math.MaxInt64 so the
// DirTree key+1 terminal sentinel stays non-negative (packLong domain).
const MaxKey = int64(1<<63 - 2)

func checkKey(key int64) {
	if key < 0 {
		panic(mapdb5.NewMisuse("negative key"))
	}
	if key > MaxKey {
		panic(mapdb5.NewMisuse("key exceeds MaxKey"))
	}
}

func keyInRange(key int64) bool { return key >= 0 && key <= MaxKey }

// llHeader is the persisted LongLongMap metadata (Java Header.SER:
// packInt dirShift, packInt levels, raw long defaultValue, packLong
// rootRecid, packLong counterRecid).
type llHeader struct {
	dirShift, levels        int32
	defaultValue            int64
	rootRecid, counterRecid uint64
}

type llHeaderSer struct{}

var llHeaderSerializer = ser.NewAnyOf[llHeader](llHeaderSer{})

func (llHeaderSer) Serialize(w *bin.Writer, h llHeader) {
	w.PackInt(h.dirShift)
	w.PackInt(h.levels)
	w.WriteLong(h.defaultValue)
	w.PackLong(int64(h.rootRecid))
	w.PackLong(int64(h.counterRecid))
}

func (llHeaderSer) Deserialize(r *bin.Reader, size int) llHeader {
	h := llHeader{
		dirShift:     r.UnpackInt(),
		levels:       r.UnpackInt(),
		defaultValue: r.ReadLong(),
		rootRecid:    uint64(r.UnpackLong()),
		counterRecid: uint64(r.UnpackLong()),
	}
	if h.dirShift <= 0 || h.dirShift > htree.MaxDirShift || h.levels <= 0 ||
		h.rootRecid == 0 || h.counterRecid == 0 {
		panic(mapdb5.NewCorruption("indextree header out of range"))
	}
	return h
}

func (llHeaderSer) FixedSize() int                { return -1 }
func (llHeaderSer) SizeHint() int                 { return 34 }
func (llHeaderSer) Compare(a, b llHeader) int     { panic(misuseInternal()) }
func (llHeaderSer) Equals(a, b llHeader) bool     { panic(misuseInternal()) }
func (llHeaderSer) NaturalOrder() bool            { return false }
func (llHeaderSer) EqualsBySerializedBytes() bool { return true }

func misuseInternal() error {
	return mapdb5.NewMisuse("indextree header codec is record-internal, not orderable")
}

// dbErrorToErr converts a *mapdb5.DBError panic into the named error
// return; other panics propagate (same pattern as htree/btree).
func dbErrorToErr(err *error) {
	if r := recover(); r != nil {
		if de, ok := mapdb5.AsDBError(r); ok {
			*err = de
			return
		}
		panic(r)
	}
}

// longValueRead decodes an 8-byte value record in place (Java READ_LONG).
type longValueRead struct{ store.RecordReadBase }

func (longValueRead) OnBytes(r *bin.Reader, size int) int64 { return r.ReadLong() }
func (longValueRead) OnObject(obj any) int64                { return obj.(int64) }
func (longValueRead) OnNull() int64 {
	panic(mapdb5.NewCorruption("indextree value record is null"))
}

var readLong store.RecordRead = longValueRead{}

// IndexTreeLongLongMap is a persistent primitive int64→int64 map over
// DirTree. A single RWMutex guards the whole map (no-op when the store
// is not thread-safe). The zero value is unusable; use New/Open.
type IndexTreeLongLongMap struct {
	st           store.Store
	headerRecid  store.Recid
	rootRecid    uint64
	counterRecid uint64
	defaultValue int64
	threadSafe   bool
	lock         sync.RWMutex
}

// NewIndexTreeLongLongMap creates a fresh map whose Get returns 0 for
// absent keys (mapdb3 default).
func NewIndexTreeLongLongMap(s store.Store) *IndexTreeLongLongMap {
	return NewIndexTreeLongLongMapWithDefault(s, 0)
}

// NewIndexTreeLongLongMapWithDefault creates a fresh map whose Get
// returns defaultValue for absent keys.
func NewIndexTreeLongLongMapWithDefault(s store.Store, defaultValue int64) *IndexTreeLongLongMap {
	rootRecid := store.Put(s, htree.DirEmpty(), htree.DirSer)
	counterRecid := store.Put(s, int64(0), ser.Long)
	hdr := llHeader{
		dirShift: DirShift, levels: Levels, defaultValue: defaultValue,
		rootRecid: rootRecid, counterRecid: counterRecid,
	}
	headerRecid := store.Put(s, hdr, llHeaderSerializer)
	return &IndexTreeLongLongMap{
		st: s, headerRecid: headerRecid, rootRecid: rootRecid,
		counterRecid: counterRecid, defaultValue: defaultValue,
		threadSafe: s.IsThreadSafe(),
	}
}

// OpenIndexTreeLongLongMap reattaches a map persisted under headerRecid.
// Corrupt or wrong-recid headers surface as errors, not panics.
func OpenIndexTreeLongLongMap(s store.Store, headerRecid store.Recid) (m *IndexTreeLongLongMap, err error) {
	defer dbErrorToErr(&err)
	if headerRecid == 0 {
		return nil, mapdb5.NewCorruption("indextree header recid must be a valid recid")
	}
	hdr, ok := store.Get(s, uint64(headerRecid), llHeaderSerializer)
	if !ok {
		return nil, mapdb5.NewCorruption("indextree header record is null")
	}
	if int(hdr.dirShift) != DirShift || int(hdr.levels) != Levels {
		return nil, mapdb5.NewCorruption("indextree header geometry mismatch")
	}
	return &IndexTreeLongLongMap{
		st: s, headerRecid: headerRecid, rootRecid: hdr.rootRecid,
		counterRecid: hdr.counterRecid, defaultValue: hdr.defaultValue,
		threadSafe: s.IsThreadSafe(),
	}, nil
}

// HeaderRecid is the recid of the header record; persist to reopen.
func (m *IndexTreeLongLongMap) HeaderRecid() store.Recid { return m.headerRecid }

// DefaultValue is what Get returns for an absent key.
func (m *IndexTreeLongLongMap) DefaultValue() int64 { return m.defaultValue }

func (m *IndexTreeLongLongMap) lockRead() {
	if m.threadSafe {
		m.lock.RLock()
	}
}
func (m *IndexTreeLongLongMap) unlockRead() {
	if m.threadSafe {
		m.lock.RUnlock()
	}
}
func (m *IndexTreeLongLongMap) lockWrite() {
	if m.threadSafe {
		m.lock.Lock()
	}
}
func (m *IndexTreeLongLongMap) unlockWrite() {
	if m.threadSafe {
		m.lock.Unlock()
	}
}

func (m *IndexTreeLongLongMap) treeGet(index int64) int64 {
	return htree.TreeGet(DirShift, m.rootRecid, m.st, Levels-1, index)
}

// ===================== reads =====================

// Get returns the mapped value, or DefaultValue if absent. Out-of-range
// keys panic Misuse (Java IllegalArgumentException).
func (m *IndexTreeLongLongMap) Get(key int64) int64 {
	checkKey(key)
	m.lockRead()
	defer m.unlockRead()
	vrecid := m.treeGet(key)
	if vrecid == 0 {
		return m.defaultValue
	}
	return m.st.Read(uint64(vrecid), readLong)
}

// GetOrDefault returns the mapped value or ifAbsent (independent of the
// configured default). Out-of-range keys are treated as absent.
func (m *IndexTreeLongLongMap) GetOrDefault(key, ifAbsent int64) int64 {
	if !keyInRange(key) {
		return ifAbsent
	}
	m.lockRead()
	defer m.unlockRead()
	vrecid := m.treeGet(key)
	if vrecid == 0 {
		return ifAbsent
	}
	return m.st.Read(uint64(vrecid), readLong)
}

// ContainsKey reports whether key is present; out-of-range keys are absent.
func (m *IndexTreeLongLongMap) ContainsKey(key int64) bool {
	if !keyInRange(key) {
		return false
	}
	m.lockRead()
	defer m.unlockRead()
	return m.treeGet(key) != 0
}

// Size is the entry count (O(1): reads the counter record).
func (m *IndexTreeLongLongMap) Size() int64 {
	m.lockRead()
	defer m.unlockRead()
	n, _ := store.Get(m.st, m.counterRecid, ser.Long)
	return n
}

// Len is Size for the collection interface (saturates at math.MaxInt).
func (m *IndexTreeLongLongMap) Len() int {
	n := m.Size()
	if n > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(n)
}

// IsEmpty reports whether the map has no entries (root bitmap check).
func (m *IndexTreeLongLongMap) IsEmpty() bool {
	m.lockRead()
	defer m.unlockRead()
	return htree.TreeIsEmpty(m.rootRecid, m.st)
}

// ForEach visits every (key, value) entry, unordered, under the read
// lock (Java forEach).
func (m *IndexTreeLongLongMap) ForEach(f func(key, value int64)) {
	for k, v := range m.All() {
		f(k, v)
	}
}

// All is the unordered entry iterator. Entries are snapshotted under the
// read lock and yielded after it is released, so the loop body may mutate
// the map and a suspended iter.Pull2 never blocks writers; weakly
// consistent with respect to concurrent mutation.
func (m *IndexTreeLongLongMap) All() func(func(int64, int64) bool) {
	return func(yield func(int64, int64) bool) {
		keys, vals := m.snapshot()
		for i := range keys {
			if !yield(keys[i], vals[i]) {
				return
			}
		}
	}
}

// snapshot collects every entry under the read lock.
func (m *IndexTreeLongLongMap) snapshot() (keys, vals []int64) {
	m.lockRead()
	defer m.unlockRead()
	htree.TreeFold(m.rootRecid, m.st, Levels-1,
		func(index, vrecid int64) {
			keys = append(keys, index)
			vals = append(vals, m.st.Read(uint64(vrecid), readLong))
		})
	return keys, vals
}

func (m *IndexTreeLongLongMap) incrementSize(delta int64) {
	n, _ := store.Get(m.st, m.counterRecid, ser.Long)
	store.Update(m.st, m.counterRecid, n+delta, ser.Long)
}

// ===================== writes =====================

// Put inserts or overwrites key→value.
func (m *IndexTreeLongLongMap) Put(key, value int64) {
	checkKey(key)
	m.lockWrite()
	defer m.unlockWrite()
	vrecid := m.treeGet(key)
	if vrecid == 0 {
		vrecid = int64(store.Put(m.st, value, ser.Long))
		htree.TreePut(DirShift, m.rootRecid, m.st, Levels-1, key, vrecid)
		m.incrementSize(1)
		return
	}
	store.Update(m.st, uint64(vrecid), value, ser.Long)
}

// PutIfAbsent inserts only if absent; returns the existing value, or
// newValue if inserted.
func (m *IndexTreeLongLongMap) PutIfAbsent(key, newValue int64) int64 {
	checkKey(key)
	m.lockWrite()
	defer m.unlockWrite()
	vrecid := m.treeGet(key)
	if vrecid != 0 {
		return m.st.Read(uint64(vrecid), readLong)
	}
	vrecid = int64(store.Put(m.st, newValue, ser.Long))
	htree.TreePut(DirShift, m.rootRecid, m.st, Levels-1, key, vrecid)
	m.incrementSize(1)
	return newValue
}

// AddTo is Put(key, Get(key)+delta); returns the new value.
func (m *IndexTreeLongLongMap) AddTo(key, delta int64) int64 {
	checkKey(key)
	m.lockWrite()
	defer m.unlockWrite()
	vrecid := m.treeGet(key)
	if vrecid == 0 {
		v := m.defaultValue + delta
		vrecid = int64(store.Put(m.st, v, ser.Long))
		htree.TreePut(DirShift, m.rootRecid, m.st, Levels-1, key, vrecid)
		m.incrementSize(1)
		return v
	}
	v := m.st.Read(uint64(vrecid), readLong) + delta
	store.Update(m.st, uint64(vrecid), v, ser.Long)
	return v
}

// Remove deletes key; returns true iff it was present (its value record
// is freed).
func (m *IndexTreeLongLongMap) Remove(key int64) bool {
	checkKey(key)
	m.lockWrite()
	defer m.unlockWrite()
	vrecid := m.treeGet(key)
	if vrecid == 0 {
		return false
	}
	if !htree.TreeRemove(DirShift, m.rootRecid, m.st, Levels-1, key) {
		panic(mapdb5.NewCorruption("dir tree lost the entry it just resolved"))
	}
	store.Delete(m.st, uint64(vrecid), ser.Long)
	m.incrementSize(-1)
	return true
}

// Clear removes every entry, freeing all value records; the root recid
// stays stable.
func (m *IndexTreeLongLongMap) Clear() {
	m.lockWrite()
	defer m.unlockWrite()
	htree.TreeClear(m.rootRecid, m.st, Levels-1,
		func(index, vrecid int64) {
			store.Delete(m.st, uint64(vrecid), ser.Long)
		})
	store.Update(m.st, m.counterRecid, int64(0), ser.Long)
}
