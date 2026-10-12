package db

// IndexTreeList / IndexTreeLongLongMap / SortedTableMap makers (Java
// DB.IndexTreeListMaker / IndexTreeLongLongMapMaker / SortedTableMapMaker).
// Catalog rows mirror Java:
//
//	IndexTreeList:        headerRecid serializer
//	IndexTreeLongLongMap: headerRecid
//	SortedTableMap:       keySerializer valueSerializer headerRecid
//	                      entriesPerPage
//
// These collections have no close primitive (Java leaves them as is on
// rollback/close); DB.Delete clears them (lists, long maps) or deletes all
// their records (sorted table).

import (
	"iter"
	"strconv"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/indextree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/sortedtable"
)

// ============================ IndexTreeList ============================

// IndexTreeListMaker builds a persistent positional list.
type IndexTreeListMaker[T any] struct {
	db   *DB
	name string
	sr   ser.Serializer[T]
}

// IndexTreeList begins a typed list open/create (Java db.indexTreeList).
func IndexTreeList[T any](d *DB, name string, serializer ser.Serializer[T]) *IndexTreeListMaker[T] {
	return &IndexTreeListMaker[T]{db: d, name: name, sr: serializer}
}

func (m *IndexTreeListMaker[T]) requireSerializer() {
	if m.sr == nil {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "indexTreeList "+m.name+" requires a serializer"))
	}
}

func (m *IndexTreeListMaker[T]) cache(l *indextree.IndexTreeList[T]) *indextree.IndexTreeList[T] {
	cacheInsert(m.db, m.name, l, nil, l.Clear, true)
	return l
}

func (m *IndexTreeListMaker[T]) createLocked() *indextree.IndexTreeList[T] {
	m.db.checkCreatable(m.name)
	m.requireSerializer()
	l := indextree.NewIndexTreeList(m.db.st, m.sr)
	n := m.name
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "IndexTreeList"
		c[n+"#serializer"] = ser.DescriptorOrCustom(m.sr)
		c[n+"#headerRecid"] = itoa(l.HeaderRecid())
	})
	return m.cache(l)
}

func (m *IndexTreeListMaker[T]) openLocked() *indextree.IndexTreeList[T] {
	n := m.name
	cat := m.db.catalog
	m.db.typeCheck(cat, n, "IndexTreeList")
	m.requireSerializer()
	verifySer(requireField(cat, n+"#serializer"), m.sr)
	if l, ok := cacheLookup[*indextree.IndexTreeList[T]](m.db, n); ok {
		return l
	}
	l, err := indextree.OpenIndexTreeList(m.db.st, parseRecid(cat, n+"#headerRecid"), m.sr)
	if err != nil {
		panic(err)
	}
	return m.cache(l)
}

// Create builds the named list.
func (m *IndexTreeListMaker[T]) Create() (l *indextree.IndexTreeList[T], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked(), nil
}

// Open reopens the named list.
func (m *IndexTreeListMaker[T]) Open() (l *indextree.IndexTreeList[T], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked(), nil
}

// CreateOrOpen opens when present, else creates.
func (m *IndexTreeListMaker[T]) CreateOrOpen() (l *indextree.IndexTreeList[T], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked(), nil
	}
	return m.createLocked(), nil
}

// ======================== IndexTreeLongLongMap ========================

// IndexTreeLongLongMapMaker builds a persistent int64→int64 map. The
// default value (returned for absent keys) is persisted in the header.
type IndexTreeLongLongMapMaker struct {
	db           *DB
	name         string
	defaultValue int64
}

// IndexTreeLongLongMap begins an open/create (Java
// db.indexTreeLongLongMap); defaultValue applies at create only.
func (d *DB) IndexTreeLongLongMap(name string, defaultValue int64) *IndexTreeLongLongMapMaker {
	return &IndexTreeLongLongMapMaker{db: d, name: name, defaultValue: defaultValue}
}

func (m *IndexTreeLongLongMapMaker) cache(mp *indextree.IndexTreeLongLongMap) *indextree.IndexTreeLongLongMap {
	cacheInsert(m.db, m.name, mp, nil, mp.Clear, true)
	return mp
}

func (m *IndexTreeLongLongMapMaker) createLocked() *indextree.IndexTreeLongLongMap {
	m.db.checkCreatable(m.name)
	mp := indextree.NewIndexTreeLongLongMapWithDefault(m.db.st, m.defaultValue)
	n := m.name
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "IndexTreeLongLongMap"
		c[n+"#headerRecid"] = itoa(mp.HeaderRecid())
	})
	return m.cache(mp)
}

func (m *IndexTreeLongLongMapMaker) openLocked() *indextree.IndexTreeLongLongMap {
	n := m.name
	m.db.typeCheck(m.db.catalog, n, "IndexTreeLongLongMap")
	if mp, ok := cacheLookup[*indextree.IndexTreeLongLongMap](m.db, n); ok {
		return mp
	}
	mp, err := indextree.OpenIndexTreeLongLongMap(m.db.st, parseRecid(m.db.catalog, n+"#headerRecid"))
	if err != nil {
		panic(err)
	}
	return m.cache(mp)
}

// Create builds the named map.
func (m *IndexTreeLongLongMapMaker) Create() (mp *indextree.IndexTreeLongLongMap, err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked(), nil
}

// Open reopens the named map.
func (m *IndexTreeLongLongMapMaker) Open() (mp *indextree.IndexTreeLongLongMap, err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked(), nil
}

// CreateOrOpen opens when present, else creates.
func (m *IndexTreeLongLongMapMaker) CreateOrOpen() (mp *indextree.IndexTreeLongLongMap, err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked(), nil
	}
	return m.createLocked(), nil
}

// ============================ SortedTableMap ============================

// SortedTableMapMaker builds an immutable bulk-loaded sorted table.
// Create/CreateOrOpen build an EMPTY table; populate at build time with
// CreateFrom or CreateFromSink.
type SortedTableMapMaker[K, V any] struct {
	db             *DB
	name           string
	kf             ser.GroupFormat[K]
	vf             ser.GroupFormat[V]
	entriesPerPage int
}

// SortedTableMap begins a typed sorted-table open/create (Java
// db.sortedTableMap). Default page fill sortedtable.DefaultEntriesPerPage.
func SortedTableMap[K, V any](d *DB, name string, keyFormat ser.GroupFormat[K], valueFormat ser.GroupFormat[V]) *SortedTableMapMaker[K, V] {
	return &SortedTableMapMaker[K, V]{db: d, name: name, kf: keyFormat, vf: valueFormat,
		entriesPerPage: sortedtable.DefaultEntriesPerPage}
}

// PageSize sets the entries packed per page (>= 1).
func (m *SortedTableMapMaker[K, V]) PageSize(entriesPerPage int) *SortedTableMapMaker[K, V] {
	m.entriesPerPage = entriesPerPage
	return m
}

func (m *SortedTableMapMaker[K, V]) requireFormats() {
	if m.kf == nil || m.vf == nil {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "sortedTableMap "+m.name+" requires key and value formats"))
	}
}

// register publishes a built table; on a publish failure its records are
// deleted (never reachable from the catalog).
func (m *SortedTableMapMaker[K, V]) register(t *sortedtable.SortedTableMap[K, V]) *sortedtable.SortedTableMap[K, V] {
	adopted := false
	defer func() {
		if !adopted {
			func() { defer func() { _ = recover() }(); t.DeleteAllRecords() }()
		}
	}()
	n := m.name
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "SortedTableMap"
		c[n+"#keySerializer"] = ser.DescriptorOrCustom(m.kf)
		c[n+"#valueSerializer"] = ser.DescriptorOrCustom(m.vf)
		c[n+"#headerRecid"] = itoa(t.HeaderRecid())
		c[n+"#entriesPerPage"] = strconv.Itoa(m.entriesPerPage)
	})
	cacheInsert(m.db, n, t, nil, t.DeleteAllRecords, true)
	adopted = true
	return t
}

func (m *SortedTableMapMaker[K, V]) createLocked() *sortedtable.SortedTableMap[K, V] {
	m.db.checkCreatable(m.name)
	m.requireFormats()
	return m.register(sortedtable.NewSinkN(m.db.st, m.kf, m.vf, m.entriesPerPage).Create())
}

func (m *SortedTableMapMaker[K, V]) openLocked() *sortedtable.SortedTableMap[K, V] {
	n := m.name
	cat := m.db.catalog
	m.db.typeCheck(cat, n, "SortedTableMap")
	m.requireFormats()
	verifyGroup(requireField(cat, n+"#keySerializer"), m.kf)
	verifyGroup(requireField(cat, n+"#valueSerializer"), m.vf)
	if t, ok := cacheLookup[*sortedtable.SortedTableMap[K, V]](m.db, n); ok {
		return t
	}
	t, err := sortedtable.OpenSortedTableMap(m.db.st, parseRecid(cat, n+"#headerRecid"), m.kf, m.vf)
	if err != nil {
		panic(err)
	}
	cacheInsert(m.db, n, t, nil, t.DeleteAllRecords, true)
	return t
}

// Create builds an EMPTY named table.
func (m *SortedTableMapMaker[K, V]) Create() (t *sortedtable.SortedTableMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked(), nil
}

// Open reopens the named table.
func (m *SortedTableMapMaker[K, V]) Open() (t *sortedtable.SortedTableMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked(), nil
}

// CreateOrOpen opens when present, else creates an empty table.
func (m *SortedTableMapMaker[K, V]) CreateOrOpen() (t *sortedtable.SortedTableMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked(), nil
	}
	return m.createLocked(), nil
}

// CreateFrom bulk-builds the table from STRICTLY ascending entries and
// registers it; nothing is registered when the input is not sorted.
func (m *SortedTableMapMaker[K, V]) CreateFrom(entries iter.Seq2[K, V]) (t *sortedtable.SortedTableMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	m.db.checkCreatable(m.name)
	m.requireFormats()
	built, cerr := sortedtable.CreateFromSortedN(m.db.st, m.kf, m.vf, m.entriesPerPage, entries)
	if cerr != nil {
		return nil, notSortedToDBError(cerr)
	}
	return m.register(built), nil
}

// DBSink is the incremental bulk builder whose Create also registers the
// table in the catalog (Java SortedTableMapMaker.DbSink).
type DBSink[K, V any] struct {
	m    *SortedTableMapMaker[K, V]
	sink *sortedtable.Sink[K, V]
}

// CreateFromSink starts an incremental build: Put strictly ascending
// entries, then Create exactly once.
func (m *SortedTableMapMaker[K, V]) CreateFromSink() *DBSink[K, V] {
	m.requireFormats()
	return &DBSink[K, V]{m: m, sink: sortedtable.NewSinkN(m.db.st, m.kf, m.vf, m.entriesPerPage)}
}

// Put appends the next entry (keys strictly ascending; panics
// KindNotSorted otherwise).
func (s *DBSink[K, V]) Put(key K, value V) *DBSink[K, V] {
	s.sink.Put(key, value)
	return s
}

// Create finalizes the table and registers it (fails if the name exists).
func (s *DBSink[K, V]) Create() (t *sortedtable.SortedTableMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m := s.m
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	m.db.checkCreatable(m.name)
	return m.register(s.sink.Create()), nil
}
