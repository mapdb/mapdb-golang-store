package db

import (
	"iter"
	"strconv"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/btree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// ======================== BufferTreeMap maker ========================

// BufferTreeMapMaker builds the write-optimized buffered B-tree (Java
// DB.BufferTreeMapMaker). Catalog row (Java parity): keySerializer,
// valueSerializer, rootRecidRecid, maxNodeSize, bufferBytes, leafHeadroom —
// leafHeadroom in raw bytes (0 = no leaf headroom, as in Java).
type BufferTreeMapMaker[K, V any] struct {
	db           *DB
	name         string
	kf           ser.GroupFormat[K]
	vf           ser.GroupFormat[V]
	maxNodeSize  int
	bufferBytes  int
	leafHeadroom int // raw bytes, Java semantics
}

// BufferTreeMap begins a typed buffer-tree open/create (Java
// db.bufferTreeMap). Defaults: maxNodeSize 32, bufferBytes 4096,
// leafHeadroom btree.DefaultLeafHeadroom.
func BufferTreeMap[K, V any](d *DB, name string, keyFormat ser.GroupFormat[K], valueFormat ser.GroupFormat[V]) *BufferTreeMapMaker[K, V] {
	return &BufferTreeMapMaker[K, V]{db: d, name: name, kf: keyFormat, vf: valueFormat,
		maxNodeSize: 32, bufferBytes: 4096, leafHeadroom: btree.DefaultLeafHeadroom}
}

// MaxNodeSize sets the per-node key ceiling.
func (m *BufferTreeMapMaker[K, V]) MaxNodeSize(n int) *BufferTreeMapMaker[K, V] {
	m.maxNodeSize = n
	return m
}

// BufferBytes sets the directory-node append headroom (>= 32).
func (m *BufferTreeMapMaker[K, V]) BufferBytes(n int) *BufferTreeMapMaker[K, V] {
	m.bufferBytes = n
	return m
}

// LeafHeadroom sets the leaf append headroom in bytes (0 = none).
func (m *BufferTreeMapMaker[K, V]) LeafHeadroom(n int) *BufferTreeMapMaker[K, V] {
	m.leafHeadroom = n
	return m
}

// cfgFor maps raw Java parameters onto btree.BufferTreeConfig (whose zero
// LeafHeadroom means "default", so a literal 0 becomes LeafHeadroomNone).
func (m *BufferTreeMapMaker[K, V]) cfgFor(maxNodeSize, bufferBytes, leafHeadroom int) btree.BufferTreeConfig[K, V] {
	lh := leafHeadroom
	switch {
	case lh == 0:
		lh = btree.LeafHeadroomNone
	case lh < 0:
		lh = -2 // rejected by the btree config validator
	}
	return btree.BufferTreeConfig[K, V]{KeyFormat: m.kf, ValueFormat: m.vf,
		MaxNodeSize: maxNodeSize, BufferBytes: bufferBytes, LeafHeadroom: lh}
}

func (m *BufferTreeMapMaker[K, V]) deltaStore() store.StoreDelta {
	sd, ok := m.db.st.(store.StoreDelta)
	if !ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"bufferTreeMap requires a delta-capable store; heapDB() is not supported"))
	}
	return sd
}

func (m *BufferTreeMapMaker[K, V]) requireFormats() {
	if m.kf == nil || m.vf == nil {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"bufferTreeMap "+m.name+" requires key and value formats"))
	}
}

func (m *BufferTreeMapMaker[K, V]) buildMap() *btree.BufferTreeMap[K, V] {
	m.requireFormats()
	return btree.NewBufferTreeMap(m.deltaStore(), m.cfgFor(m.maxNodeSize, m.bufferBytes, m.leafHeadroom))
}

func (m *BufferTreeMapMaker[K, V]) openFromCatalog(cat NameCatalog) (*btree.BufferTreeMap[K, V], error) {
	n := m.name
	root := parseRecid(cat, n+"#rootRecidRecid")
	num := func(f string) int {
		s, ok := cat[n+"#"+f]
		if !ok {
			panic(mapdb5.NewCorruption("catalog missing " + n + "#" + f))
		}
		return int(parseDecimal(s, n+" "+f))
	}
	return btree.OpenBufferTreeMap(m.deltaStore(), root, m.cfgFor(num("maxNodeSize"), num("bufferBytes"), num("leafHeadroom")))
}

func (m *BufferTreeMapMaker[K, V]) writeCatalog(cat NameCatalog, mp *btree.BufferTreeMap[K, V]) {
	n := m.name
	cat[n+"#type"] = "BufferTreeMap"
	cat[n+"#keySerializer"] = ser.DescriptorOrCustom(m.kf)
	cat[n+"#valueSerializer"] = ser.DescriptorOrCustom(m.vf)
	cat[n+"#rootRecidRecid"] = itoa(mp.RootRecidRecid())
	cat[n+"#maxNodeSize"] = strconv.Itoa(m.maxNodeSize)
	cat[n+"#bufferBytes"] = strconv.Itoa(m.bufferBytes)
	cat[n+"#leafHeadroom"] = strconv.Itoa(m.leafHeadroom)
}

func (m *BufferTreeMapMaker[K, V]) verifyCatalog(cat NameCatalog) {
	m.db.typeCheck(cat, m.name, "BufferTreeMap")
	m.requireFormats()
	kd, ok := cat[m.name+"#keySerializer"]
	if !ok {
		panic(mapdb5.NewCorruption("catalog missing keySerializer"))
	}
	verifyGroup(kd, m.kf)
	vd, ok := cat[m.name+"#valueSerializer"]
	if !ok {
		panic(mapdb5.NewCorruption("catalog missing valueSerializer"))
	}
	verifyGroup(vd, m.vf)
	m.deltaStore()
}

// cacheAndReturn caches the handle: delete clears the entries while live,
// then retires (retireLast); node + root-pointer records leak, as for
// TreeMap (Java parity).
func (m *BufferTreeMapMaker[K, V]) cacheAndReturn(mp *btree.BufferTreeMap[K, V]) *btree.BufferTreeMap[K, V] {
	cacheInsert(m.db, m.name, mp, mp.CloseHandle, mp.Clear, true)
	return mp
}

// Create builds the named map; fails WrongConfiguration when the name is
// taken or the store is not delta-capable.
func (m *BufferTreeMapMaker[K, V]) Create() (mp *btree.BufferTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name))
	}
	return m.publish(m.buildMap()), nil
}

// publish writes the catalog row and caches the handle; an unpublished
// handle releases its lease if publishing fails.
func (m *BufferTreeMapMaker[K, V]) publish(built *btree.BufferTreeMap[K, V]) *btree.BufferTreeMap[K, V] {
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle()
		}
	}()
	m.db.publishCatalog(func(c NameCatalog) { m.writeCatalog(c, built) })
	mp := m.cacheAndReturn(built)
	adopted = true
	return mp
}

// Open reopens the named map, verifying the stored codec descriptors.
func (m *BufferTreeMapMaker[K, V]) Open() (mp *btree.BufferTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked()
}

func (m *BufferTreeMapMaker[K, V]) openLocked() (*btree.BufferTreeMap[K, V], error) {
	m.verifyCatalog(m.db.catalog)
	if h, ok := cacheLookup[*btree.BufferTreeMap[K, V]](m.db, m.name); ok {
		return h, nil
	}
	built, oerr := m.openFromCatalog(m.db.catalog)
	if oerr != nil {
		return nil, oerr
	}
	return m.cacheAndReturn(built), nil
}

// CreateOrOpen creates when absent, else opens — one decision under the
// catalog write lock.
func (m *BufferTreeMapMaker[K, V]) CreateOrOpen() (mp *btree.BufferTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked()
	}
	validateName(m.name)
	return m.publish(m.buildMap()), nil
}

// CreateFrom bulk-builds the map from STRICTLY ascending entries (Java
// createFrom, fill max(2, maxNodeSize*3/4)); registers nothing when the
// pump fails NotSorted.
func (m *BufferTreeMapMaker[K, V]) CreateFrom(entries iter.Seq2[K, V]) (mp *btree.BufferTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.requireFormats()
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name))
	}
	cfg := m.cfgFor(m.maxNodeSize, m.bufferBytes, m.leafHeadroom)
	var built *btree.BufferTreeMap[K, V]
	var cerr error
	m.db.withUserCode(func() {
		built, cerr = btree.CreateBufferTreeFromSortedFill(m.deltaStore(), cfg, max(2, m.maxNodeSize*3/4), entries)
	})
	if cerr != nil {
		return nil, notSortedToDBError(cerr)
	}
	return m.publish(built), nil
}
