// Package sortedtable is the immutable, bulk-loaded sorted map (Java
// org.mapdb.sortedtable.SortedTableMap — the Store4 evolution of mapdb3's
// SortedTableMap).
//
// Layout: entries are packed into fixed-count pages; each page is ONE
// store record holding a key group + value group:
//
//	page = packInt(count) + keyFormat group + valueFormat group
//
// A single HEADER record is the page directory:
//
//	header = packInt(MAGIC) + packInt(VERSION) + packLong(size) +
//	         packInt(pageCount) + first-key group + pageCount×packLong(recid)
//
// Get routes through the in-memory directory (first keys) to the covering
// page, then pushes a RecordRead that binary-searches the page's
// serialized bytes in place — the page is never materialized when the
// formats support binary access. Iteration materializes one page at a
// time. All mutators panic KindReadOnly (Java UnsupportedOperationException).
package sortedtable

import (
	"iter"
	"math"

	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// DefaultEntriesPerPage is the builder's default page fill (Java
// DEFAULT_ENTRIES_PER_PAGE): small enough to keep a page's materialization
// cheap, large enough to amortize the per-page record + directory entry.
const DefaultEntriesPerPage = 128

// Header self-identification: cheap fail-fast on a wrong recid / wrong
// collection type at Open (Java MAGIC/VERSION).
const (
	stMagic   = 0x53544D31 // "STM1"
	stVersion = 1
)

// stPage is a page's in-memory image: a key group + a value group
// (opaque, owned by the formats).
type stPage struct {
	keys, values ser.Group
}

// pageSerializer is the page wire codec (Java PageSerializer).
type pageSerializer[K, V any] struct {
	keyFormat   ser.GroupFormat[K]
	valueFormat ser.GroupFormat[V]
}

func (s pageSerializer[K, V]) Serialize(w *bin.Writer, p stPage) {
	count := s.keyFormat.Size(p.keys)
	w.PackInt(int32(count))
	s.keyFormat.SerializeGroup(w, p.keys)
	s.valueFormat.SerializeGroup(w, p.values)
}

func (s pageSerializer[K, V]) Deserialize(r *bin.Reader, size int) stPage {
	count := int(r.UnpackInt())
	if count < 0 || (size >= 0 && count > size) {
		panic(mapdb5.NewCorruption("sortedtable page count out of range"))
	}
	return stPage{
		keys:   s.keyFormat.DeserializeGroup(r, count),
		values: s.valueFormat.DeserializeGroup(r, count),
	}
}

func (s pageSerializer[K, V]) FixedSize() int { return -1 }
func (s pageSerializer[K, V]) SizeHint() int  { return 1024 }
func (s pageSerializer[K, V]) Compare(a, b stPage) int {
	panic(mapdb5.NewMisuse("sortedtable page codec is record-internal, not orderable"))
}
func (s pageSerializer[K, V]) Equals(a, b stPage) bool {
	panic(mapdb5.NewMisuse("sortedtable page codec is record-internal, not orderable"))
}
func (s pageSerializer[K, V]) NaturalOrder() bool            { return false }
func (s pageSerializer[K, V]) EqualsBySerializedBytes() bool { return true }

// stHeader is the persisted directory record.
type stHeader struct {
	size       int64
	firstKeys  ser.Group
	pageRecids []uint64
}

type headerSerializer[K any] struct {
	keyFormat ser.GroupFormat[K]
}

func (s headerSerializer[K]) Serialize(w *bin.Writer, h stHeader) {
	w.PackInt(stMagic)
	w.PackInt(stVersion)
	w.PackLong(h.size)
	w.PackInt(int32(len(h.pageRecids)))
	s.keyFormat.SerializeGroup(w, h.firstKeys)
	for _, recid := range h.pageRecids {
		w.PackLong(int64(recid))
	}
}

func (s headerSerializer[K]) Deserialize(r *bin.Reader, size int) stHeader {
	magic := r.UnpackInt()
	if magic != stMagic {
		panic(mapdb5.NewCorruption("not a sortedtable header (bad magic)"))
	}
	if v := r.UnpackInt(); v != stVersion {
		panic(mapdb5.NewCorruption("unsupported sortedtable version"))
	}
	sz := r.UnpackLong()
	if sz < 0 {
		panic(mapdb5.NewCorruption("sortedtable size out of range"))
	}
	pageCount := int(r.UnpackInt())
	if pageCount < 0 || (size >= 0 && pageCount > size) {
		panic(mapdb5.NewCorruption("sortedtable page count out of range"))
	}
	h := stHeader{
		size:       sz,
		firstKeys:  s.keyFormat.DeserializeGroup(r, pageCount),
		pageRecids: make([]uint64, pageCount),
	}
	for i := range h.pageRecids {
		recid := r.UnpackLong()
		if recid <= 0 {
			panic(mapdb5.NewCorruption("sortedtable page recid out of range"))
		}
		h.pageRecids[i] = uint64(recid)
	}
	return h
}

func (s headerSerializer[K]) FixedSize() int { return -1 }
func (s headerSerializer[K]) SizeHint() int  { return 256 }
func (s headerSerializer[K]) Compare(a, b stHeader) int {
	panic(mapdb5.NewMisuse("sortedtable header codec is record-internal, not orderable"))
}
func (s headerSerializer[K]) Equals(a, b stHeader) bool {
	panic(mapdb5.NewMisuse("sortedtable header codec is record-internal, not orderable"))
}
func (s headerSerializer[K]) NaturalOrder() bool            { return false }
func (s headerSerializer[K]) EqualsBySerializedBytes() bool { return true }

// stBound is an inclusive/exclusive key bound; unset = open (btree's
// bound shape).
type stBound[K any] struct {
	key  K
	set  bool
	incl bool
}

// SortedTableMap is the immutable bulk-built sorted map. All state is
// final after Open; any number of readers share it with no locking. The
// zero value is unusable; build via NewSink/CreateFromSorted or reattach
// with Open.
type SortedTableMap[K, V any] struct {
	st          store.Store
	keyFormat   ser.GroupFormat[K]
	valueFormat ser.GroupFormat[V]
	headerRecid store.Recid
	pageSer     pageSerializer[K, V]

	size       int64
	pageCount  int
	firstKeys  ser.Group // key group of size pageCount (page[i]'s first key)
	pageRecids []uint64
}

// OpenSortedTableMap reattaches a table persisted under headerRecid; the
// same formats must be supplied. Corrupt or wrong-recid headers surface
// as errors, not panics.
func OpenSortedTableMap[K, V any](s store.Store, headerRecid store.Recid,
	keyFormat ser.GroupFormat[K], valueFormat ser.GroupFormat[V]) (m *SortedTableMap[K, V], err error) {
	defer dbErrorToErr(&err)
	if headerRecid == 0 {
		return nil, mapdb5.NewCorruption("sortedtable header recid must be a valid recid")
	}
	hdr, ok := store.Get(s, uint64(headerRecid), headerSerializer[K]{keyFormat})
	if !ok {
		return nil, mapdb5.NewCorruption("sortedtable header record is null")
	}
	return &SortedTableMap[K, V]{
		st: s, keyFormat: keyFormat, valueFormat: valueFormat,
		headerRecid: headerRecid, pageSer: pageSerializer[K, V]{keyFormat, valueFormat},
		size: hdr.size, pageCount: len(hdr.pageRecids),
		firstKeys: hdr.firstKeys, pageRecids: hdr.pageRecids,
	}, nil
}

// HeaderRecid is the recid of the header (directory) record; persist to
// reopen the table.
func (m *SortedTableMap[K, V]) HeaderRecid() store.Recid { return m.headerRecid }

// DeleteAllRecords deletes every page record and the header owned by
// this table (Java deleteAllRecords — for owning facades like DB.Delete;
// the handle is invalid after this call).
func (m *SortedTableMap[K, V]) DeleteAllRecords() {
	for _, recid := range m.pageRecids {
		store.Delete(m.st, recid, m.pageSer)
	}
	store.Delete(m.st, uint64(m.headerRecid), headerSerializer[K]{m.keyFormat})
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

// ===================== Sink (bulk builder) =====================

// Sink is the write-once bulk builder: feed STRICTLY ASCENDING
// (key,value) pairs via Put, then call Create exactly once (Java
// SortedTableMap.Sink). Every page record and the header are written
// exactly once through store.Put. Single-threaded.
type Sink[K, V any] struct {
	st             store.Store
	keyFormat      ser.GroupFormat[K]
	valueFormat    ser.GroupFormat[V]
	pageSer        pageSerializer[K, V]
	entriesPerPage int

	pageKeys  []K
	pageVals  []V
	firstKeys []K
	recids    []uint64
	prevKey   K
	hasPrev   bool
	size      int64
	done      bool
}

// NewSink starts a bulk build with DefaultEntriesPerPage.
func NewSink[K, V any](s store.Store, keyFormat ser.GroupFormat[K],
	valueFormat ser.GroupFormat[V]) *Sink[K, V] {
	return NewSinkN(s, keyFormat, valueFormat, DefaultEntriesPerPage)
}

// NewSinkN starts a bulk build packing entriesPerPage entries per page.
func NewSinkN[K, V any](s store.Store, keyFormat ser.GroupFormat[K],
	valueFormat ser.GroupFormat[V], entriesPerPage int) *Sink[K, V] {
	if entriesPerPage < 1 {
		panic(mapdb5.NewMisuse("entriesPerPage must be >= 1"))
	}
	return &Sink[K, V]{
		st: s, keyFormat: keyFormat, valueFormat: valueFormat,
		pageSer:        pageSerializer[K, V]{keyFormat, valueFormat},
		entriesPerPage: entriesPerPage,
	}
}

// Put feeds one entry; keys must be strictly ascending in element order.
// A misordered or duplicate key panics KindNotSorted (Java
// DBException.NotSorted).
func (sn *Sink[K, V]) Put(key K, value V) {
	if sn.done {
		panic(mapdb5.NewMisuse("sink already finished"))
	}
	if sn.hasPrev && sn.keyFormat.Element().Compare(sn.prevKey, key) >= 0 {
		panic(mapdb5.New(mapdb5.KindNotSorted, "bulk-load keys not strictly ascending"))
	}
	sn.prevKey = key
	sn.hasPrev = true
	sn.pageKeys = append(sn.pageKeys, key)
	sn.pageVals = append(sn.pageVals, value)
	sn.size++
	if len(sn.pageKeys) == sn.entriesPerPage {
		sn.flushPage()
	}
}

func (sn *Sink[K, V]) flushPage() {
	if len(sn.pageKeys) == 0 {
		return
	}
	recid := store.Put(sn.st, stPage{
		keys:   sn.keyFormat.FromSlice(sn.pageKeys),
		values: sn.valueFormat.FromSlice(sn.pageVals),
	}, sn.pageSer)
	sn.firstKeys = append(sn.firstKeys, sn.pageKeys[0])
	sn.recids = append(sn.recids, recid)
	sn.pageKeys = sn.pageKeys[:0]
	sn.pageVals = sn.pageVals[:0]
}

// Create finishes the build, writes the header, and opens the table.
// Callable exactly once (Java sink.create).
func (sn *Sink[K, V]) Create() *SortedTableMap[K, V] {
	if sn.done {
		panic(mapdb5.NewMisuse("sink already finished"))
	}
	sn.done = true
	sn.flushPage()
	headerRecid := store.Put(sn.st, stHeader{
		size:       sn.size,
		firstKeys:  sn.keyFormat.FromSlice(sn.firstKeys),
		pageRecids: sn.recids,
	}, headerSerializer[K]{sn.keyFormat})
	m, err := OpenSortedTableMap[K, V](sn.st, headerRecid, sn.keyFormat, sn.valueFormat)
	if err != nil {
		panic(err)
	}
	return m
}

// CreateFromSorted bulk-builds a table from a strictly-ascending
// iterator (default page size). A misordered input returns
// pump.ErrNotSorted (pump conventions).
func CreateFromSorted[K, V any](s store.Store, keyFormat ser.GroupFormat[K],
	valueFormat ser.GroupFormat[V], sorted iter.Seq2[K, V]) (m *SortedTableMap[K, V], err error) {
	return CreateFromSortedN(s, keyFormat, valueFormat, DefaultEntriesPerPage, sorted)
}

// CreateFromSortedN is CreateFromSorted with an explicit page size.
func CreateFromSortedN[K, V any](s store.Store, keyFormat ser.GroupFormat[K],
	valueFormat ser.GroupFormat[V], entriesPerPage int,
	sorted iter.Seq2[K, V]) (m *SortedTableMap[K, V], err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok && de.Kind == mapdb5.KindNotSorted {
				err = pump.ErrNotSorted
				return
			}
			panic(r)
		}
	}()
	sink := NewSinkN(s, keyFormat, valueFormat, entriesPerPage)
	for k, v := range sorted {
		sink.Put(k, v)
	}
	return sink.Create(), nil
}

// ===================== directory routing =====================

// pageForFloor is the index of the page whose first key is the greatest
// <= key, or -1 when key is below page 0's first key.
func (m *SortedTableMap[K, V]) pageForFloor(key K) int {
	idx, found := m.keyFormat.Search(m.firstKeys, key)
	if found {
		return idx
	}
	return idx - 1 // insertion point - 1 (Java: -p-2)
}

// stGetAction resolves one page lookup inside the store against
// serialized bytes (or the live page on a heap store) — never
// materializes the page when the formats support binary access.
type stGetAction[K, V any] struct {
	keyFormat   ser.GroupFormat[K]
	valueFormat ser.GroupFormat[V]
	key         K
	value       V
	found       bool
}

func (a *stGetAction[K, V]) OnBytes(r *bin.Reader, size int) int64 {
	count := int(r.UnpackInt())
	// every key occupies >= 1 serialized byte, so count > size is torn
	if count < 0 || count > size {
		panic(mapdb5.NewCorruption("page count exceeds record size"))
	}
	var pos int
	var found bool
	if a.keyFormat.SupportsBinary() {
		pos, found = a.keyFormat.BytesSearch(r, count, a.key)
	} else {
		g := a.keyFormat.DeserializeGroup(r, count)
		pos, found = a.keyFormat.Search(g, a.key)
	}
	if found {
		a.found = true
		if a.valueFormat.SupportsBinary() {
			a.value = a.valueFormat.BytesGet(r, count, pos)
		} else {
			g := a.valueFormat.DeserializeGroup(r, count)
			a.value = a.valueFormat.Get(g, pos)
		}
	}
	return 0
}

func (a *stGetAction[K, V]) OnObject(obj any) int64 {
	p := obj.(stPage)
	pos, found := a.keyFormat.Search(p.keys, a.key)
	if found {
		a.found = true
		a.value = a.valueFormat.Get(p.values, pos)
	}
	return 0
}

func (a *stGetAction[K, V]) OnNull() int64 {
	panic(mapdb5.NewCorruption("sortedtable page record is null"))
}

// ===================== get / containsKey =====================

// Get returns the value for key (push-down byte-side search).
func (m *SortedTableMap[K, V]) Get(key K) (V, bool) {
	var zero V
	if m.pageCount == 0 {
		return zero, false
	}
	pageIdx := m.pageForFloor(key)
	if pageIdx < 0 {
		return zero, false
	}
	action := &stGetAction[K, V]{keyFormat: m.keyFormat, valueFormat: m.valueFormat, key: key}
	m.st.Read(m.pageRecids[pageIdx], action)
	return action.value, action.found
}

// ContainsKey reports whether key is present.
func (m *SortedTableMap[K, V]) ContainsKey(key K) bool {
	_, found := m.Get(key)
	return found
}

// ===================== size =====================

// LenLong is the exact entry count.
func (m *SortedTableMap[K, V]) LenLong() int64 { return m.size }

// Len saturates at math.MaxInt.
func (m *SortedTableMap[K, V]) Len() int {
	if m.size > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(m.size)
}

// IsEmpty reports whether the table has no entries.
func (m *SortedTableMap[K, V]) IsEmpty() bool { return m.size == 0 }

// ===================== iteration (page-at-a-time) =====================

func (m *SortedTableMap[K, V]) loadPage(idx int) stPage {
	p, ok := store.Get(m.st, m.pageRecids[idx], m.pageSer)
	if !ok {
		panic(mapdb5.NewCorruption("sortedtable directory points at a missing page"))
	}
	return p
}

// iterRange ascends [lo, hi] honoring inclusivity, materializing one
// page at a time (O(entriesPerPage) memory).
func (m *SortedTableMap[K, V]) iterRange(lo, hi stBound[K]) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if m.pageCount == 0 {
			return
		}
		pageIdx := 0
		if lo.set {
			pageIdx = max(0, m.pageForFloor(lo.key))
		}
		startPage := pageIdx
		for ; pageIdx < m.pageCount; pageIdx++ {
			page := m.loadPage(pageIdx)
			count := m.keyFormat.Size(page.keys)
			pos := 0
			if lo.set && pageIdx == startPage {
				idx, found := m.keyFormat.Search(page.keys, lo.key)
				if found {
					pos = idx
					if !lo.incl {
						pos++
					}
				} else {
					pos = idx // insertion point = first key > lo
				}
			}
			for ; pos < count; pos++ {
				k := m.keyFormat.Get(page.keys, pos)
				if hi.set {
					c := m.keyFormat.Compare(k, hi.key)
					if c > 0 || (c == 0 && !hi.incl) {
						return
					}
				}
				if !yield(k, m.valueFormat.Get(page.values, pos)) {
					return
				}
			}
		}
	}
}

// iterRangeDesc descends [lo, hi] honoring inclusivity.
func (m *SortedTableMap[K, V]) iterRangeDesc(lo, hi stBound[K]) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if m.pageCount == 0 {
			return
		}
		pageIdx := m.pageCount - 1
		if hi.set {
			pageIdx = m.pageForFloor(hi.key)
			if pageIdx < 0 {
				return // hi below every key
			}
		}
		startPage := pageIdx
		for ; pageIdx >= 0; pageIdx-- {
			page := m.loadPage(pageIdx)
			pos := m.keyFormat.Size(page.keys) - 1
			if hi.set && pageIdx == startPage {
				idx, found := m.keyFormat.Search(page.keys, hi.key)
				if found {
					pos = idx
					if !hi.incl {
						pos--
					}
				} else {
					pos = idx - 1 // last key < hi
				}
			}
			for ; pos >= 0; pos-- {
				k := m.keyFormat.Get(page.keys, pos)
				if lo.set {
					c := m.keyFormat.Compare(k, lo.key)
					if c < 0 || (c == 0 && !lo.incl) {
						return
					}
				}
				if !yield(k, m.valueFormat.Get(page.values, pos)) {
					return
				}
			}
		}
	}
}

// ===================== iterators / views =====================

// All iterates every entry ascending.
func (m *SortedTableMap[K, V]) All() iter.Seq2[K, V] {
	return m.iterRange(stBound[K]{}, stBound[K]{})
}

// Keys iterates keys ascending.
func (m *SortedTableMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.All()(func(k K, _ V) bool { return yield(k) })
	}
}

// Values iterates values in key order.
func (m *SortedTableMap[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.All()(func(_ K, v V) bool { return yield(v) })
	}
}

// ForEach visits every entry ascending.
func (m *SortedTableMap[K, V]) ForEach(f func(K, V)) {
	m.All()(func(k K, v V) bool { f(k, v); return true })
}

// HeadMap yields entries with keys strictly less than toKey.
func (m *SortedTableMap[K, V]) HeadMap(toKey K) iter.Seq2[K, V] {
	return m.iterRange(stBound[K]{}, stBound[K]{key: toKey, set: true, incl: false})
}

// TailMap yields entries with keys >= fromKey.
func (m *SortedTableMap[K, V]) TailMap(fromKey K) iter.Seq2[K, V] {
	return m.iterRange(stBound[K]{key: fromKey, set: true, incl: true}, stBound[K]{})
}

// SubMap yields entries with keys in [fromKey, toKey).
func (m *SortedTableMap[K, V]) SubMap(fromKey, toKey K) iter.Seq2[K, V] {
	return m.iterRange(stBound[K]{key: fromKey, set: true, incl: true},
		stBound[K]{key: toKey, set: true, incl: false})
}

// DescendingMap yields every entry descending.
func (m *SortedTableMap[K, V]) DescendingMap() iter.Seq2[K, V] {
	return m.iterRangeDesc(stBound[K]{}, stBound[K]{})
}

// DescendingKeys yields every key descending.
func (m *SortedTableMap[K, V]) DescendingKeys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.DescendingMap()(func(k K, _ V) bool { return yield(k) })
	}
}

// ===================== navigation =====================

func (m *SortedTableMap[K, V]) firstOf(lo, hi stBound[K]) (K, V, bool) {
	var fk K
	var fv V
	found := false
	m.iterRange(lo, hi)(func(k K, v V) bool {
		fk, fv, found = k, v, true
		return false
	})
	return fk, fv, found
}

func (m *SortedTableMap[K, V]) lastOf(lo, hi stBound[K]) (K, V, bool) {
	var lk K
	var lv V
	found := false
	m.iterRangeDesc(lo, hi)(func(k K, v V) bool {
		lk, lv, found = k, v, true
		return false
	})
	return lk, lv, found
}

// Min returns the least entry.
func (m *SortedTableMap[K, V]) Min() (K, V, bool) {
	return m.firstOf(stBound[K]{}, stBound[K]{})
}

// Max returns the greatest entry.
func (m *SortedTableMap[K, V]) Max() (K, V, bool) {
	return m.lastOf(stBound[K]{}, stBound[K]{})
}

// FirstEntry is Min (NavigableMap parity).
func (m *SortedTableMap[K, V]) FirstEntry() (K, V, bool) { return m.Min() }

// LastEntry is Max.
func (m *SortedTableMap[K, V]) LastEntry() (K, V, bool) { return m.Max() }

// Floor returns the greatest entry with key <= key.
func (m *SortedTableMap[K, V]) Floor(key K) (K, V, bool) {
	return m.lastOf(stBound[K]{}, stBound[K]{key: key, set: true, incl: true})
}

// Ceiling returns the least entry with key >= key.
func (m *SortedTableMap[K, V]) Ceiling(key K) (K, V, bool) {
	return m.firstOf(stBound[K]{key: key, set: true, incl: true}, stBound[K]{})
}

// Lower returns the greatest entry with key < key.
func (m *SortedTableMap[K, V]) Lower(key K) (K, V, bool) {
	return m.lastOf(stBound[K]{}, stBound[K]{key: key, set: true, incl: false})
}

// Higher returns the least entry with key > key.
func (m *SortedTableMap[K, V]) Higher(key K) (K, V, bool) {
	return m.firstOf(stBound[K]{key: key, set: true, incl: false}, stBound[K]{})
}

// ===================== immutable surface =====================

func readOnlyMutation() {
	panic(mapdb5.New(mapdb5.KindReadOnly, "sortedtable is immutable"))
}

// Put panics KindReadOnly — the table is immutable.
func (m *SortedTableMap[K, V]) Put(key K, value V) (V, bool) {
	readOnlyMutation()
	panic("unreachable")
}

// PutIfAbsent panics KindReadOnly.
func (m *SortedTableMap[K, V]) PutIfAbsent(key K, value V) (V, bool) {
	readOnlyMutation()
	panic("unreachable")
}

// Remove panics KindReadOnly.
func (m *SortedTableMap[K, V]) Remove(key K) (V, bool) { readOnlyMutation(); panic("unreachable") }

// RemoveIfEquals panics KindReadOnly.
func (m *SortedTableMap[K, V]) RemoveIfEquals(key K, value V) bool {
	readOnlyMutation()
	panic("unreachable")
}

// Replace panics KindReadOnly.
func (m *SortedTableMap[K, V]) Replace(key K, value V) (V, bool) {
	readOnlyMutation()
	panic("unreachable")
}

// ReplaceIfEquals panics KindReadOnly.
func (m *SortedTableMap[K, V]) ReplaceIfEquals(key K, old, new V) bool {
	readOnlyMutation()
	panic("unreachable")
}

// Clear panics KindReadOnly.
func (m *SortedTableMap[K, V]) Clear() { readOnlyMutation() }

// PollFirstEntry panics KindReadOnly.
func (m *SortedTableMap[K, V]) PollFirstEntry() (K, V, bool) {
	readOnlyMutation()
	panic("unreachable")
}

// PollLastEntry panics KindReadOnly.
func (m *SortedTableMap[K, V]) PollLastEntry() (K, V, bool) {
	readOnlyMutation()
	panic("unreachable")
}
