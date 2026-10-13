// Typed collection makers (Java DB.TreeMapMaker/TreeSetMaker/atomic makers/
// QueueMaker). Each validates the stored #type, verifies the persisted
// codec descriptors against the supplied formats/serializers, serves the
// DB-owned instance cache (Java same-live-instance), and publishes the
// `name#...` catalog row only AFTER all collection records are built.
package db

import (
	"errors"
	"iter"
	"math"
	"strconv"

	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/btree"
	"github.com/mapdb/mapdb5-go/queue"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// typeCheck verifies name exists with the expected #type; WrongConfiguration
// on absence (no such name) or a different stored type.
func (d *DB) typeCheck(cat NameCatalog, name, want string) {
	t, ok := cat[name+"#type"]
	if !ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "no such name: "+name))
	}
	if t != want {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"name "+name+" is a "+t+", not a "+want))
	}
}

// ============================ TreeMap maker ============================

// TreeMapMaker builds a typed tree map (Java DB.TreeMapMaker).
type TreeMapMaker[K, V any] struct {
	db            *DB
	name          string
	kf            ser.GroupFormat[K]
	vf            ser.GroupFormat[V]
	maxNodeSize   int
	counter       bool
	valuesOutside bool
	listeners     []mapdb5.MapModificationListener[K, V]
}

// TreeMap begins a typed tree-map open/create (Java db.treeMap).
func TreeMap[K, V any](d *DB, name string, keyFormat ser.GroupFormat[K], valueFormat ser.GroupFormat[V]) *TreeMapMaker[K, V] {
	return &TreeMapMaker[K, V]{
		db: d, name: name, kf: keyFormat, vf: valueFormat, maxNodeSize: 32,
	}
}

// MaxNodeSize sets the per-node key ceiling (Java maxNodeSize).
func (m *TreeMapMaker[K, V]) MaxNodeSize(n int) *TreeMapMaker[K, V] {
	m.maxNodeSize = n
	return m
}

// CounterEnable installs the O(1) size counter (Java counterEnable).
func (m *TreeMapMaker[K, V]) CounterEnable() *TreeMapMaker[K, V] {
	m.counter = true
	return m
}

// ValuesOutsideNodesEnable stores values in separate records (Java
// valuesOutsideNodesEnable).
func (m *TreeMapMaker[K, V]) ValuesOutsideNodesEnable() *TreeMapMaker[K, V] {
	m.valuesOutside = true
	return m
}

// ModificationListener registers a runtime modification listener applied on
// every open of the cached handle; a listener implementing
// mapdb5.SynchronousMapModificationListener fires under the leaf lock
// (Java modificationListener).
func (m *TreeMapMaker[K, V]) ModificationListener(l mapdb5.MapModificationListener[K, V]) *TreeMapMaker[K, V] {
	m.listeners = append(m.listeners, l)
	return m
}

func (m *TreeMapMaker[K, V]) applyListeners(mp *btree.BTreeMap[K, V]) {
	for _, l := range m.listeners {
		mp.ModificationListenerAdd(l)
	}
}

func (m *TreeMapMaker[K, V]) cfg() btree.Config[K, V] {
	return btree.Config[K, V]{
		KeyFormat:      m.kf,
		ValueFormat:    m.vf,
		MaxNodeSize:    m.maxNodeSize,
		Counter:        m.counter,
		ExternalValues: m.valuesOutside,
	}
}

// buildMap constructs the collection's records (before catalog publish).
func (m *TreeMapMaker[K, V]) buildMap() *btree.BTreeMap[K, V] {
	return btree.NewBTreeMap(m.db.st, m.cfg())
}

// openFromCatalog reopens the map from stored parameters.
func (m *TreeMapMaker[K, V]) openFromCatalog(cat NameCatalog) (*btree.BTreeMap[K, V], error) {
	n := m.name
	root := parseRecid(cat, n+"#rootRecidRecid")
	mns, ok := cat[n+"#maxNodeSize"]
	if !ok {
		panic(mapdb5.NewCorruption("catalog missing " + n + "#maxNodeSize"))
	}
	counter := parseRecidDefault0(cat, n+"#counterRecid")
	var valueInline bool
	switch s, ok := cat[n+"#valueInline"]; {
	case !ok: // absent → true (legacy default)
		valueInline = true
	case s == "true":
		valueInline = true
	case s == "false":
		valueInline = false
	default:
		panic(mapdb5.NewCorruption("catalog " + n + "#valueInline is not a boolean"))
	}
	cfg := m.cfg()
	cfg.Counter = false // reopen attaches the stored counter recid instead
	cfg.CounterRecid = counter
	cfg.MaxNodeSize = int(parseDecimal(mns, n+" maxNodeSize"))
	cfg.ExternalValues = !valueInline
	return btree.OpenBTreeMap(m.db.st, root, cfg)
}

// writeCatalog publishes the `name#...` row after the map is built.
func (m *TreeMapMaker[K, V]) writeCatalog(cat NameCatalog, mp *btree.BTreeMap[K, V]) {
	n := m.name
	cat[n+"#type"] = "TreeMap"
	cat[n+"#keySerializer"] = ser.DescriptorOrCustom(m.kf)
	cat[n+"#valueSerializer"] = ser.DescriptorOrCustom(m.vf)
	cat[n+"#rootRecidRecid"] = itoa(mp.RootRecidRecid())
	cat[n+"#maxNodeSize"] = itoa(uint64(m.maxNodeSize))
	cat[n+"#counterRecid"] = itoa(mp.CounterRecid())
	if mp.ValueInline() {
		cat[n+"#valueInline"] = "true"
	} else {
		cat[n+"#valueInline"] = "false"
	}
}

func (m *TreeMapMaker[K, V]) verifyCatalog(cat NameCatalog) {
	m.db.typeCheck(cat, m.name, "TreeMap")
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
}

// cacheAndReturn caches the handle: retire invalidates it and releases the
// open lease; the delete teardown clears entry/node records while still
// live, THEN retires (retireLast — Clear needs an open handle). The
// structural root + counter records LEAK, per Java — never freed while a
// handle may still use them.
//
// Runtime wiring (listeners) is applied BEFORE caching, so a wiring panic
// never leaves a dead handle cached under the name; the uncached handle
// releases its lease.
func (m *TreeMapMaker[K, V]) cacheAndReturn(mp *btree.BTreeMap[K, V]) *btree.BTreeMap[K, V] {
	adopted := false
	defer func() {
		if !adopted {
			mp.CloseHandle()
		}
	}()
	m.applyListeners(mp)
	cacheInsert(m.db, m.name, mp, mp.CloseHandle, mp.Clear, true)
	adopted = true
	return mp
}

// publishAndCache wires a freshly built map, then publishes its catalog
// row and caches it. Wiring first is Java create2's order (configure, then
// catalogSave + instances.put): a failing listener publishes nothing.
// The caller's adopted-defer releases built on any panic.
func (m *TreeMapMaker[K, V]) publishAndCache(built *btree.BTreeMap[K, V]) *btree.BTreeMap[K, V] {
	m.applyListeners(built)
	m.db.publishCatalog(func(c NameCatalog) { m.writeCatalog(c, built) })
	cacheInsert(m.db, m.name, built, built.CloseHandle, built.Clear, true)
	return built
}

// Create builds the named map; fails WrongConfiguration when the name is
// taken (Java create).
func (m *TreeMapMaker[K, V]) Create() (mp *btree.BTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name))
	}
	built := m.buildMap()
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle() // never cached: release the unpublished lease
		}
	}()
	mp = m.publishAndCache(built)
	adopted = true
	return mp, nil
}

// Open reopens the named map; fails WrongConfiguration on absence/type
// mismatch and verifies the stored codec descriptors (Java open).
func (m *TreeMapMaker[K, V]) Open() (mp *btree.BTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	m.verifyCatalog(m.db.catalog)
	if h, ok := cacheLookup[*btree.BTreeMap[K, V]](m.db, m.name); ok {
		m.applyListeners(h)
		return h, nil
	}
	built, oerr := m.openFromCatalog(m.db.catalog)
	if oerr != nil {
		return nil, oerr
	}
	return m.cacheAndReturn(built), nil
}

// CreateOrOpen creates when absent, else opens (Java createOrOpen) — one
// decision under the catalog write lock.
func (m *TreeMapMaker[K, V]) CreateOrOpen() (mp *btree.BTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		m.verifyCatalog(m.db.catalog)
		if h, hit := cacheLookup[*btree.BTreeMap[K, V]](m.db, m.name); hit {
			m.applyListeners(h)
			return h, nil
		}
		built, oerr := m.openFromCatalog(m.db.catalog)
		if oerr != nil {
			return nil, oerr
		}
		return m.cacheAndReturn(built), nil
	}
	validateName(m.name)
	built := m.buildMap()
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle()
		}
	}()
	mp = m.publishAndCache(built)
	adopted = true
	return mp, nil
}

// CreateFrom bulk-builds the map from STRICTLY ascending entries (Java
// createFrom); rejects external-value maps (Java behavior) and registers
// nothing when the pump fails NotSorted.
func (m *TreeMapMaker[K, V]) CreateFrom(entries iter.Seq2[K, V]) (mp *btree.BTreeMap[K, V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	if m.valuesOutside {
		panic(mapdb5.New(mapdb5.KindUnsupported,
			"createFrom rejects external-value maps (Java behavior)"))
	}
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name))
	}
	var built *btree.BTreeMap[K, V]
	var cerr error
	m.db.withUserCode(func() { built, cerr = btree.CreateFromSorted(m.db.st, m.cfg(), entries) })
	if cerr != nil {
		return nil, notSortedToDBError(cerr)
	}
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle()
		}
	}()
	mp = m.publishAndCache(built)
	adopted = true
	return mp, nil
}

// TreeSetMaker builds a typed tree set (Java DB.TreeSetMaker).
type TreeSetMaker[K any] struct {
	db          *DB
	name        string
	kf          ser.GroupFormat[K]
	maxNodeSize int
	counter     bool
}

// TreeSet begins a typed tree-set open/create (Java db.treeSet).
func TreeSet[K any](d *DB, name string, keyFormat ser.GroupFormat[K]) *TreeSetMaker[K] {
	return &TreeSetMaker[K]{db: d, name: name, kf: keyFormat, maxNodeSize: 32}
}

// MaxNodeSize sets the per-node key ceiling.
func (m *TreeSetMaker[K]) MaxNodeSize(n int) *TreeSetMaker[K] {
	m.maxNodeSize = n
	return m
}

// CounterEnable installs the O(1) size counter.
func (m *TreeSetMaker[K]) CounterEnable() *TreeSetMaker[K] {
	m.counter = true
	return m
}

func (m *TreeSetMaker[K]) cfg() btree.Config[K, struct{}] {
	return btree.Config[K, struct{}]{
		KeyFormat:   m.kf,
		ValueFormat: noValueFormat{},
		MaxNodeSize: m.maxNodeSize,
		Counter:     m.counter,
	}
}

func (m *TreeSetMaker[K]) cacheSet(s *NavigableSet[K]) *NavigableSet[K] {
	cacheInsert(m.db, m.name, s, s.closeHandle, s.Clear, true)
	return s
}

func (m *TreeSetMaker[K]) writeCatalog(cat NameCatalog, mp *btree.BTreeMap[K, struct{}]) {
	n := m.name
	cat[n+"#type"] = "TreeSet"
	cat[n+"#serializer"] = ser.DescriptorOrCustom(m.kf)
	cat[n+"#rootRecidRecid"] = itoa(mp.RootRecidRecid())
	cat[n+"#maxNodeSize"] = itoa(uint64(m.maxNodeSize))
	cat[n+"#counterRecid"] = itoa(mp.CounterRecid())
}

func (m *TreeSetMaker[K]) verifyCatalog(cat NameCatalog) {
	m.db.typeCheck(cat, m.name, "TreeSet")
	sd, ok := cat[m.name+"#serializer"]
	if !ok {
		panic(mapdb5.NewCorruption("catalog missing serializer"))
	}
	verifyGroup(sd, m.kf)
}

func (m *TreeSetMaker[K]) openFromCatalog(cat NameCatalog) (*NavigableSet[K], error) {
	n := m.name
	root := parseRecid(cat, n+"#rootRecidRecid")
	mns, ok := cat[n+"#maxNodeSize"]
	if !ok {
		panic(mapdb5.NewCorruption("catalog missing " + n + "#maxNodeSize"))
	}
	counter := parseRecidDefault0(cat, n+"#counterRecid")
	cfg := m.cfg()
	cfg.Counter = false
	cfg.CounterRecid = counter
	cfg.MaxNodeSize = int(parseDecimal(mns, n+" maxNodeSize"))
	mp, err := btree.OpenBTreeMap(m.db.st, root, cfg)
	if err != nil {
		return nil, err
	}
	return navigableSetFromMap(mp), nil
}

// Create builds the named set; fails WrongConfiguration when the name is
// taken.
func (m *TreeSetMaker[K]) Create() (s *NavigableSet[K], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name))
	}
	mp := btree.NewBTreeMap(m.db.st, m.cfg())
	adopted := false
	defer func() {
		if !adopted {
			mp.CloseHandle()
		}
	}()
	m.db.publishCatalog(func(c NameCatalog) { m.writeCatalog(c, mp) })
	ns := m.cacheSet(navigableSetFromMap(mp))
	adopted = true
	return ns, nil
}

// Open reopens the named set.
func (m *TreeSetMaker[K]) Open() (s *NavigableSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	m.verifyCatalog(m.db.catalog)
	if h, ok := cacheLookup[*NavigableSet[K]](m.db, m.name); ok {
		return h, nil
	}
	built, oerr := m.openFromCatalog(m.db.catalog)
	if oerr != nil {
		return nil, oerr
	}
	return m.cacheSet(built), nil
}

// CreateOrOpen creates when absent, else opens.
func (m *TreeSetMaker[K]) CreateOrOpen() (s *NavigableSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		m.verifyCatalog(m.db.catalog)
		if h, hit := cacheLookup[*NavigableSet[K]](m.db, m.name); hit {
			return h, nil
		}
		built, oerr := m.openFromCatalog(m.db.catalog)
		if oerr != nil {
			return nil, oerr
		}
		return m.cacheSet(built), nil
	}
	validateName(m.name)
	mp := btree.NewBTreeMap(m.db.st, m.cfg())
	adopted := false
	defer func() {
		if !adopted {
			mp.CloseHandle()
		}
	}()
	m.db.publishCatalog(func(c NameCatalog) { m.writeCatalog(c, mp) })
	ns := m.cacheSet(navigableSetFromMap(mp))
	adopted = true
	return ns, nil
}

// CreateFrom bulk-builds the set from STRICTLY ascending elements (Java
// createFrom).
func (m *TreeSetMaker[K]) CreateFrom(elements iter.Seq[K]) (s *NavigableSet[K], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name))
	}
	entries := func(yield func(K, struct{}) bool) {
		for e := range elements {
			if !yield(e, struct{}{}) {
				return
			}
		}
	}
	var mp *btree.BTreeMap[K, struct{}]
	var cerr error
	m.db.withUserCode(func() { mp, cerr = btree.CreateFromSorted(m.db.st, m.cfg(), iter.Seq2[K, struct{}](entries)) })
	if cerr != nil {
		return nil, notSortedToDBError(cerr)
	}
	adopted := false
	defer func() {
		if !adopted {
			mp.CloseHandle()
		}
	}()
	m.db.publishCatalog(func(c NameCatalog) { m.writeCatalog(c, mp) })
	ns := m.cacheSet(navigableSetFromMap(mp))
	adopted = true
	return ns, nil
}

// ============================ atomic makers ============================

// numericAtomicMaker is the shared create/open/createOrOpen shape for the
// primitive atomics.
type numericAtomicMaker[V any, A any] struct {
	db       *DB
	name     string
	initial  V
	ser      ser.Serializer[V]
	typeName string
	// makeFn builds the handle plus its retire hook (atomic invalidation —
	// Java closeHandle, run before the record is freed or rewound).
	makeFn func(s store.Store, recid uint64) (A, func())
}

func (m *numericAtomicMaker[V, A]) Create() (a A, err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked()
}
func (m *numericAtomicMaker[V, A]) createLocked() (A, error) {
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		var zero A
		return zero, mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name)
	}
	recid := store.Put(m.db.st, m.initial, m.ser)
	n := m.name
	ty := m.typeName
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = ty
		c[n+"#recid"] = itoa(recid)
	})
	a, retire := m.makeFn(m.db.st, recid)
	cacheInsert(m.db, m.name, a, retire, nil, false)
	return a, nil
}
func (m *numericAtomicMaker[V, A]) Open() (a A, err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked()
}
func (m *numericAtomicMaker[V, A]) openLocked() (A, error) {
	m.db.typeCheck(m.db.catalog, m.name, m.typeName)
	if h, ok := cacheLookup[A](m.db, m.name); ok {
		return h, nil
	}
	recid := parseRecid(m.db.catalog, m.name+"#recid")
	if recid == 0 {
		var zero A
		return zero, mapdb5.NewCorruption("catalog " + m.name + "#recid must be non-zero")
	}
	a, retire := m.makeFn(m.db.st, recid)
	cacheInsert(m.db, m.name, a, retire, nil, false)
	return a, nil
}
func (m *numericAtomicMaker[V, A]) CreateOrOpen() (a A, err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked()
	}
	return m.createLocked()
}

// AtomicLongMaker builds an AtomicLong (Java DB.AtomicLongMaker).
type AtomicLongMaker = numericAtomicMaker[int64, *AtomicLong]

// AtomicLong begins an atomic-long open/create (Java db.atomicLong).
func (d *DB) AtomicLong(name string) *AtomicLongMaker {
	return d.AtomicLongInit(name, 0)
}

// AtomicLongInit begins an atomic-long with an explicit initial value.
func (d *DB) AtomicLongInit(name string, initial int64) *AtomicLongMaker {
	return &AtomicLongMaker{db: d, name: name, initial: initial, ser: ser.Long,
		typeName: "AtomicLong",
		makeFn: func(s store.Store, r uint64) (*AtomicLong, func()) {
			a := newAtomicLong(s, r)
			return a, a.closeHandle
		}}
}

// AtomicIntegerMaker builds an AtomicInteger.
type AtomicIntegerMaker = numericAtomicMaker[int32, *AtomicInteger]

// AtomicInteger begins an atomic-integer open/create (Java db.atomicInteger).
func (d *DB) AtomicInteger(name string) *AtomicIntegerMaker {
	return d.AtomicIntegerInit(name, 0)
}

// AtomicIntegerInit begins an atomic-integer with an initial value.
func (d *DB) AtomicIntegerInit(name string, initial int32) *AtomicIntegerMaker {
	return &AtomicIntegerMaker{db: d, name: name, initial: initial, ser: ser.Int32,
		typeName: "AtomicInteger",
		makeFn: func(s store.Store, r uint64) (*AtomicInteger, func()) {
			a := newAtomicInteger(s, r)
			return a, a.closeHandle
		}}
}

// AtomicBooleanMaker builds an AtomicBoolean.
type AtomicBooleanMaker = numericAtomicMaker[bool, *AtomicBoolean]

// AtomicBoolean begins an atomic-boolean open/create (Java db.atomicBoolean).
func (d *DB) AtomicBoolean(name string) *AtomicBooleanMaker {
	return d.AtomicBooleanInit(name, false)
}

// AtomicBooleanInit begins an atomic-boolean with an initial value.
func (d *DB) AtomicBooleanInit(name string, initial bool) *AtomicBooleanMaker {
	return &AtomicBooleanMaker{db: d, name: name, initial: initial, ser: ser.Boolean,
		typeName: "AtomicBoolean",
		makeFn: func(s store.Store, r uint64) (*AtomicBoolean, func()) {
			a := newAtomicBoolean(s, r)
			return a, a.closeHandle
		}}
}

// AtomicStringMaker builds a nullable AtomicString (Java DB.AtomicStringMaker).
type AtomicStringMaker struct {
	db      *DB
	name    string
	initial *string
}

// AtomicString begins an atomic-string open/create (Java db.atomicString);
// the default value is null.
func (d *DB) AtomicString(name string) *AtomicStringMaker {
	return &AtomicStringMaker{db: d, name: name}
}

// AtomicStringInit begins an atomic-string with an initial value.
func (d *DB) AtomicStringInit(name, initial string) *AtomicStringMaker {
	return &AtomicStringMaker{db: d, name: name, initial: &initial}
}

func (m *AtomicStringMaker) Create() (a *AtomicString, err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked()
}
func (m *AtomicStringMaker) createLocked() (*AtomicString, error) {
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return nil, mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name)
	}
	// Always write a (present) record whose content encodes null-ness, never
	// a store-level null record (Java STRING_NULLABLE).
	recid := store.Put(m.db.st, m.initial, nullableStringSer{})
	n := m.name
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "AtomicString"
		c[n+"#recid"] = itoa(recid)
	})
	a := newAtomicString(m.db.st, recid)
	cacheInsert(m.db, m.name, a, a.closeHandle, nil, false)
	return a, nil
}
func (m *AtomicStringMaker) Open() (a *AtomicString, err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked()
}
func (m *AtomicStringMaker) openLocked() (*AtomicString, error) {
	m.db.typeCheck(m.db.catalog, m.name, "AtomicString")
	if h, ok := cacheLookup[*AtomicString](m.db, m.name); ok {
		return h, nil
	}
	recid := parseRecid(m.db.catalog, m.name+"#recid")
	a := newAtomicString(m.db.st, recid)
	cacheInsert(m.db, m.name, a, a.closeHandle, nil, false)
	return a, nil
}
func (m *AtomicStringMaker) CreateOrOpen() (a *AtomicString, err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked()
	}
	return m.createLocked()
}

// AtomicVarMaker builds a nullable AtomicVar over an element serializer
// (Java DB.AtomicVarMaker).
type AtomicVarMaker[V any] struct {
	db      *DB
	name    string
	sr      ser.Serializer[V]
	initial *V
}

// AtomicVarOf begins an atomic-var open/create (Java db.atomicVar); initial
// may be nil for a null record. (Suffixed: the cell type is AtomicVar[V].)
func AtomicVarOf[V any](d *DB, name string, serializer ser.Serializer[V], initial *V) *AtomicVarMaker[V] {
	return &AtomicVarMaker[V]{db: d, name: name, sr: serializer, initial: initial}
}

func (m *AtomicVarMaker[V]) Create() (a *AtomicVar[V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked()
}
func (m *AtomicVarMaker[V]) createLocked() (*AtomicVar[V], error) {
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return nil, mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name)
	}
	var recid uint64
	if m.initial == nil {
		recid = m.db.st.Preallocate()
	} else {
		recid = store.Put(m.db.st, *m.initial, m.sr)
	}
	n := m.name
	desc := ser.DescriptorOrCustom(m.sr)
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "AtomicVar"
		c[n+"#recid"] = itoa(recid)
		c[n+"#serializer"] = desc
	})
	a := newAtomicVar(m.db.st, recid, m.sr)
	cacheInsert(m.db, m.name, a, a.closeHandle, nil, false)
	return a, nil
}
func (m *AtomicVarMaker[V]) Open() (a *AtomicVar[V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked()
}
func (m *AtomicVarMaker[V]) openLocked() (*AtomicVar[V], error) {
	n := m.name
	m.db.typeCheck(m.db.catalog, n, "AtomicVar")
	sd, ok := m.db.catalog[n+"#serializer"]
	if !ok {
		return nil, mapdb5.NewCorruption("catalog missing AtomicVar serializer")
	}
	verifySer(sd, m.sr)
	if h, hit := cacheLookup[*AtomicVar[V]](m.db, n); hit {
		return h, nil
	}
	recid := parseRecid(m.db.catalog, n+"#recid")
	a := newAtomicVar(m.db.st, recid, m.sr)
	cacheInsert(m.db, n, a, a.closeHandle, nil, false)
	return a, nil
}
func (m *AtomicVarMaker[V]) CreateOrOpen() (a *AtomicVar[V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked()
	}
	return m.createLocked()
}

// ============================ queue maker ============================

// QueueMaker builds a persistent blocking queue / stack / circular queue
// (Java DB.QueueMaker / StackMaker / CircularQueueMaker).
type QueueMaker[V any] struct {
	db          *DB
	name        string
	sr          ser.Serializer[V]
	mode        queue.Mode
	catalogType string
	capacity    uint64
}

// Queue begins a FIFO persistent blocking queue maker (Java db.queue).
func Queue[V any](d *DB, name string, serializer ser.Serializer[V]) *QueueMaker[V] {
	return &QueueMaker[V]{db: d, name: name, sr: serializer,
		mode: queue.ModeFIFO, catalogType: "Queue", capacity: math.MaxInt64}
}

// Stack begins a LIFO persistent blocking stack maker (Java db.stack).
func Stack[V any](d *DB, name string, serializer ser.Serializer[V]) *QueueMaker[V] {
	return &QueueMaker[V]{db: d, name: name, sr: serializer,
		mode: queue.ModeLIFO, catalogType: "Stack", capacity: math.MaxInt64}
}

// CircularQueue begins a circular (overwrite-on-full) blocking queue maker
// (Java db.circularQueue).
func CircularQueue[V any](d *DB, name string, serializer ser.Serializer[V], capacity uint64) *QueueMaker[V] {
	return &QueueMaker[V]{db: d, name: name, sr: serializer,
		mode: queue.ModeCircular, catalogType: "CircularQueue", capacity: capacity}
}

// cacheQueue caches the shared queue handle with a close hook that wakes
// blocked producers/consumers and a delete teardown freeing node + header
// records (safe — all clones share one globally-closable handle).
func (m *QueueMaker[V]) cacheQueue(q *queue.PBQ[V]) *queue.PBQ[V] {
	cacheInsert(m.db, m.name, q, q.CloseHandle, q.PurgeRecords, false)
	return q
}

func (m *QueueMaker[V]) Create() (q *queue.PBQ[V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked()
}
func (m *QueueMaker[V]) createLocked() (*queue.PBQ[V], error) {
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return nil, mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+m.name)
	}
	desc := ser.DescriptorOrCustom(m.sr)
	q, err := queue.CreateBlockingQueue(m.db.st, m.sr, m.mode, m.capacity)
	if err != nil {
		return nil, err
	}
	header := q.HeaderRecid()
	n := m.name
	ty := m.catalogType
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = ty
		c[n+"#headerRecid"] = itoa(header)
		c[n+"#serializer"] = desc
	})
	return m.cacheQueue(q), nil
}
func (m *QueueMaker[V]) Open() (q *queue.PBQ[V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked()
}
func (m *QueueMaker[V]) openLocked() (*queue.PBQ[V], error) {
	n := m.name
	m.db.typeCheck(m.db.catalog, n, m.catalogType)
	sd, ok := m.db.catalog[n+"#serializer"]
	if !ok {
		return nil, mapdb5.NewCorruption("catalog missing queue serializer")
	}
	verifySer(sd, m.sr)
	if h, hit := cacheLookup[*queue.PBQ[V]](m.db, n); hit {
		if h.Mode() != m.mode {
			return nil, mapdb5.NewCorruption(
				"queue '" + n + "' header mode does not match its catalog #type " + m.catalogType)
		}
		return h, nil
	}
	header := parseRecid(m.db.catalog, n+"#headerRecid")
	q, err := queue.OpenBlockingQueue(m.db.st, header, m.sr)
	if err != nil {
		return nil, err
	}
	// The header's stored mode must match the requested/catalog mode; a
	// mismatch is a corrupt catalog↔header pairing (Java QueueMaker.open2
	// compares queue.mode()).
	if q.Mode() != m.mode {
		return nil, mapdb5.NewCorruption(
			"queue '" + n + "' header mode does not match its catalog #type " + m.catalogType)
	}
	return m.cacheQueue(q), nil
}
func (m *QueueMaker[V]) CreateOrOpen() (q *queue.PBQ[V], err error) {
	defer dbErrorToErr(&err)
	validateName(m.name)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked()
	}
	return m.createLocked()
}

// itoa is Java Long.toString for catalog decimal fields.
func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

// notSortedToDBError maps the pump's plain ErrNotSorted sentinel into the
// module's DBError kind; any other error passes through.
func notSortedToDBError(err error) error {
	if errors.Is(err, pump.ErrNotSorted) {
		return mapdb5.New(mapdb5.KindNotSorted, err.Error())
	}
	return err
}
