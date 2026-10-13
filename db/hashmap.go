package db

// Hash collection makers (Java DB.HashMapMaker / HashSetMaker /
// HashMap48Maker / HashSet48Maker). Catalog rows mirror Java exactly:
//
//	HashMap:   keySerializer valueSerializer headerRecid hasher kind
//	           concShift dirShift levels counterRecid
//	HashSet:   serializer headerRecid hasher kind concShift dirShift
//	           levels counterRecid
//	HashMap48: keySerializer valueSerializer headerRecid hasher64
//	           concShift dirShift levels
//	HashSet48: serializer headerRecid hasher64 concShift dirShift levels
//
// kind is PLAIN (htree.HTreeMap), EXTERNAL (htree.HTreeMapExternal,
// ValueInline(false)) or CACHE (htree.HTreeCache, any expiry option).
// Expiry parameters live in the cache header and are fixed at create; on
// reopen only the runtime-only overflow / eviction listener are applied.
// Not ported: Java's expireExecutor / expireCompactThreshold background
// sweep — call HTreeCache.ExpireEvict from your own goroutine instead.

import (
	"iter"
	"strconv"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/htree"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// ConcurrentHashMap is the surface shared by every hash map the facade
// returns (Java ConcurrentMap): *htree.HTreeMap, *htree.HTreeMapExternal,
// *htree.HTreeCache, *htree.HTreeMap48, and *CountedHashMap. Type-assert
// to the concrete type for variant-specific methods (e.g. ExpireEvict).
type ConcurrentHashMap[K, V any] interface {
	Get(key K) (V, bool)
	Put(key K, value V) (V, bool)
	PutOnly(key K, value V)
	PutIfAbsent(key K, value V) (V, bool)
	Remove(key K) (V, bool)
	RemoveOnly(key K) bool
	RemoveIfEquals(key K, value V) bool
	Replace(key K, value V) (V, bool)
	ReplaceIfEquals(key K, oldValue, newValue V) bool
	ContainsKey(key K) bool
	Len() int
	LenLong() int64
	IsEmpty() bool
	Clear()
	All() iter.Seq2[K, V]
	Keys() iter.Seq[K]
	Values() iter.Seq[V]
	HeaderRecid() store.Recid
	IsClosed() bool
	ModificationListenerAdd(l mapdb5.MapModificationListener[K, V])
	ModificationListenerRemove(l mapdb5.MapModificationListener[K, V])
	ValueLoader(f func(K) (V, bool))
	CloseHandle()
}

var (
	_ ConcurrentHashMap[int64, int64] = (*htree.HTreeMap[int64, int64])(nil)
	_ ConcurrentHashMap[int64, int64] = (*htree.HTreeMapExternal[int64, int64])(nil)
	_ ConcurrentHashMap[int64, int64] = (*htree.HTreeCache[int64, int64])(nil)
	_ ConcurrentHashMap[int64, int64] = (*htree.HTreeMap48[int64, int64])(nil)
	_ ConcurrentHashMap[int64, int64] = (*CountedHashMap[int64, int64])(nil)
)

// CountedHashMap is a hash map with a persisted O(1) size counter (Java
// CounterConcurrentMap, `counterEnable()`): a modification listener keeps
// the counter record in step, and Len/LenLong/IsEmpty read it.
type CountedHashMap[K, V any] struct {
	ConcurrentHashMap[K, V]
	counter *AtomicLong
}

func withCounter[K, V any](st store.Store, m ConcurrentHashMap[K, V], counterRecid uint64) *CountedHashMap[K, V] {
	counter := newAtomicLong(st, counterRecid)
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[K, V](
		func(_ K, oldValue, newValue *V, _ bool) error {
			switch {
			case oldValue == nil && newValue != nil:
				counter.IncrementAndGet()
			case oldValue != nil && newValue == nil:
				counter.DecrementAndGet()
			}
			return nil
		}))
	return &CountedHashMap[K, V]{ConcurrentHashMap: m, counter: counter}
}

// LenLong reads the persisted counter.
func (c *CountedHashMap[K, V]) LenLong() int64 { return c.counter.Get() }

// Len saturates the counter at the int range.
func (c *CountedHashMap[K, V]) Len() int { return int(min(c.counter.Get(), int64(^uint(0)>>1))) }

// IsEmpty reads the persisted counter.
func (c *CountedHashMap[K, V]) IsEmpty() bool { return c.counter.Get() == 0 }

// CounterRecid is the counter record.
func (c *CountedHashMap[K, V]) CounterRecid() store.Recid { return c.counter.Recid() }

// Unwrap returns the underlying map (Java CounterConcurrentMap.delegate).
func (c *CountedHashMap[K, V]) Unwrap() ConcurrentHashMap[K, V] { return c.ConcurrentHashMap }

// unwrapCounted strips a counter wrapper.
func unwrapCounted[K, V any](m ConcurrentHashMap[K, V]) ConcurrentHashMap[K, V] {
	if c, ok := m.(*CountedHashMap[K, V]); ok {
		return c.ConcurrentHashMap
	}
	return m
}

// expiry is the cache option set shared by the HashMap and HashSet makers.
type expiry struct {
	createTTL, updateTTL, getTTL int64
	maxSize, storeSize           int64
	maxEvictPerOp                int64
}

func (e *expiry) any() bool {
	return e.createTTL > 0 || e.updateTTL > 0 || e.getTTL > 0 || e.maxSize > 0 || e.storeSize > 0
}

func requirePositiveTTL(ttl int64, what string) int64 {
	if ttl <= 0 {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			what+" requires a positive TTL (got "+strconv.FormatInt(ttl, 10)+")"))
	}
	return ttl
}

func requireNonNegative(v int64, what string) int64 {
	if v < 0 {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			what+" must be non-negative (got "+strconv.FormatInt(v, 10)+")"))
	}
	return v
}

// geometry is the (concShift, dirShift, levels) catalog triple.
type geometry struct{ concShift, dirShift, levels int }

func (g geometry) write(cat NameCatalog, n string) {
	cat[n+"#concShift"] = strconv.Itoa(g.concShift)
	cat[n+"#dirShift"] = strconv.Itoa(g.dirShift)
	cat[n+"#levels"] = strconv.Itoa(g.levels)
}

// kindOf reads the stored hash-map kind (absent → PLAIN, Java default).
func kindOf(cat NameCatalog, n string, allowed ...string) string {
	k, ok := cat[n+"#kind"]
	if !ok {
		return "PLAIN"
	}
	for _, a := range allowed {
		if k == a {
			return k
		}
	}
	panic(mapdb5.NewCorruption("unknown hash collection kind in catalog for '" + n + "': " + k))
}

func requireField(cat NameCatalog, key string) string {
	v, ok := cat[key]
	if !ok {
		panic(mapdb5.NewCorruption("catalog missing " + key))
	}
	return v
}

// checkCreatable is the shared create-path guard (Java make2 create
// branch). Caller holds the admin lock.
func (d *DB) checkCreatable(name string) {
	validateName(name)
	if _, ok := d.catalog[name+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "name already exists: "+name))
	}
}

// ============================ HashMap ============================

// HashMapMaker builds a hash map (Java DB.HashMapMaker). The concrete
// backing type depends on the options: *htree.HTreeMap (default),
// *htree.HTreeMapExternal (ValueInline(false)) or *htree.HTreeCache (any
// expiry option); CounterEnable wraps it in *CountedHashMap.
type HashMapMaker[K, V any] struct {
	db          *DB
	name        string
	ks          ser.Serializer[K]
	vs          ser.Serializer[V]
	hasher      mdhash.Hasher[K]
	geo         geometry
	exp         expiry
	overflow    htree.OverflowMap[K, V]
	evictListen func(K, V, htree.EvictionCause)
	clock       func() int64
	listeners   []mapdb5.MapModificationListener[K, V]
	loader      func(K) (V, bool)
	valueInline bool
	counter     bool
	seed        int32
	seedSet     bool
}

// HashMap begins a typed hash-map open/create (Java db.hashMap). Defaults
// per Java: layout (4, 7, 4), inline values, no expiry.
func HashMap[K, V any](d *DB, name string, keySer ser.Serializer[K], valueSer ser.Serializer[V]) *HashMapMaker[K, V] {
	return &HashMapMaker[K, V]{db: d, name: name, ks: keySer, vs: valueSer,
		geo: geometry{4, 7, 4}, valueInline: true}
}

// Hasher sets the placement hasher; it must be re-supplied identically on
// every open. nil keeps Java's default where Go can reproduce it (int32,
// int64 and string keys), else the serialized-bytes hasher.
func (m *HashMapMaker[K, V]) Hasher(h mdhash.Hasher[K]) *HashMapMaker[K, V] {
	m.hasher = h
	return m
}

// Layout sets the geometry; concShift + levels*dirShift must equal 32.
func (m *HashMapMaker[K, V]) Layout(concShift, dirShift, levels int) *HashMapMaker[K, V] {
	m.geo = geometry{concShift, dirShift, levels}
	return m
}

// ExpireAfterCreate expires entries ttlMillis after creation (write-TTL).
func (m *HashMapMaker[K, V]) ExpireAfterCreate(ttlMillis int64) *HashMapMaker[K, V] {
	m.exp.createTTL = requirePositiveTTL(ttlMillis, "expireAfterCreate")
	return m
}

// ExpireAfterUpdate expires entries ttlMillis after their last update; must
// equal ExpireAfterCreate when both are set.
func (m *HashMapMaker[K, V]) ExpireAfterUpdate(ttlMillis int64) *HashMapMaker[K, V] {
	m.exp.updateTTL = requirePositiveTTL(ttlMillis, "expireAfterUpdate")
	return m
}

// ExpireAfterGet expires entries ttlMillis after their last access (LRU
// order); exclusive with the write-TTL setters.
func (m *HashMapMaker[K, V]) ExpireAfterGet(ttlMillis int64) *HashMapMaker[K, V] {
	m.exp.getTTL = requirePositiveTTL(ttlMillis, "expireAfterGet")
	return m
}

// ExpireMaxSize evicts once the map holds more than maxSize entries.
func (m *HashMapMaker[K, V]) ExpireMaxSize(maxSize int64) *HashMapMaker[K, V] {
	m.exp.maxSize = requireNonNegative(maxSize, "expireMaxSize")
	return m
}

// ExpireStoreSize evicts once the store exceeds storeSizeBytes.
func (m *HashMapMaker[K, V]) ExpireStoreSize(storeSizeBytes int64) *HashMapMaker[K, V] {
	m.exp.storeSize = requireNonNegative(storeSizeBytes, "expireStoreSize")
	return m
}

// ExpireMaxEvictPerOp caps the victims one foreground operation sweeps
// (0 = unbounded).
func (m *HashMapMaker[K, V]) ExpireMaxEvictPerOp(n int64) *HashMapMaker[K, V] {
	m.exp.maxEvictPerOp = requireNonNegative(n, "expireMaxEvictPerOp")
	return m
}

// ExpireOverflow wires the spillover map of an expiring cache (runtime
// only — re-supply on every open).
func (m *HashMapMaker[K, V]) ExpireOverflow(ov htree.OverflowMap[K, V]) *HashMapMaker[K, V] {
	m.overflow = ov
	return m
}

// ExpireEvictionListener wires the eviction listener of an expiring cache
// (runtime only — re-supply on every open).
func (m *HashMapMaker[K, V]) ExpireEvictionListener(f func(K, V, htree.EvictionCause)) *HashMapMaker[K, V] {
	m.evictListen = f
	return m
}

// ExpireClock injects the cache clock in milliseconds (Go-only; runtime
// only, default wall clock).
func (m *HashMapMaker[K, V]) ExpireClock(f func() int64) *HashMapMaker[K, V] {
	m.clock = f
	return m
}

// ModificationListener registers a runtime modification listener.
func (m *HashMapMaker[K, V]) ModificationListener(l mapdb5.MapModificationListener[K, V]) *HashMapMaker[K, V] {
	if l == nil {
		panic(mapdb5.NewMisuse("listener is nil"))
	}
	m.listeners = append(m.listeners, l)
	return m
}

// ValueLoader installs the missing-key loader.
func (m *HashMapMaker[K, V]) ValueLoader(f func(K) (V, bool)) *HashMapMaker[K, V] {
	m.loader = f
	return m
}

// CounterEnable persists an O(1) size counter (not with expiry, as Java).
func (m *HashMapMaker[K, V]) CounterEnable() *HashMapMaker[K, V] {
	m.counter = true
	return m
}

// ValueInline(false) stores values in their own records (external map; no
// expiry).
func (m *HashMapMaker[K, V]) ValueInline(inline bool) *HashMapMaker[K, V] {
	m.valueInline = inline
	return m
}

// HashSeed sets an explicit create-time seed (persisted in the header).
func (m *HashMapMaker[K, V]) HashSeed(seed int32) *HashMapMaker[K, V] {
	m.seed, m.seedSet = seed, true
	return m
}

func (m *HashMapMaker[K, V]) requireSerializers() {
	if m.ks == nil || m.vs == nil {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"hashMap "+m.name+" requires key and value serializers"))
	}
}

func (m *HashMapMaker[K, V]) hconfig(h mdhash.Hasher[K]) htree.HConfig[K, V] {
	return htree.HConfig[K, V]{KeySer: m.ks, ValueSer: m.vs,
		ConcShift: m.geo.concShift, DirShift: m.geo.dirShift, Levels: m.geo.levels,
		HashSeed: m.seed, HashSeedSet: m.seedSet, Hasher: h}
}

func (m *HashMapMaker[K, V]) cconfig(h mdhash.Hasher[K], ttl int64, accessOrder bool) htree.CConfig[K, V] {
	return htree.CConfig[K, V]{KeySer: m.ks, ValueSer: m.vs,
		ConcShift: m.geo.concShift, DirShift: m.geo.dirShift, Levels: m.geo.levels,
		TTL: ttl, AccessOrder: accessOrder, MaxSize: m.exp.maxSize,
		StoreSize: m.exp.storeSize, MaxEvictPerOp: m.exp.maxEvictPerOp,
		HashSeed: m.seed, HashSeedSet: m.seedSet, Hasher: h, Clock: m.clock}
}

// writeTTL resolves Java's single write-TTL rule.
func (m *HashMapMaker[K, V]) writeTTL() int64 {
	c, u := m.exp.createTTL, m.exp.updateTTL
	if c > 0 && u > 0 && c != u {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashMap "+m.name+
			": mapdb uses a single write-TTL; expireAfterCreate and expireAfterUpdate must be equal"))
	}
	return max(c, u)
}

func (m *HashMapMaker[K, V]) createLocked() ConcurrentHashMap[K, V] {
	m.db.checkCreatable(m.name)
	m.requireSerializers()
	writeTTL := m.writeTTL()
	anyExpiry := m.exp.any()
	switch {
	case writeTTL > 0 && m.exp.getTTL > 0:
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashMap "+m.name+
			": mapdb HTreeCache supports a single TTL; cannot combine a write-TTL with an access-TTL (expireAfterGet)"))
	case !m.valueInline && anyExpiry:
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashMap "+m.name+
			": external-value maps (valueInline(false)) do not support expiry"))
	case (m.overflow != nil || m.evictListen != nil) && !anyExpiry:
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashMap "+m.name+
			": expireOverflow/expireEvictionListener require an expiring cache (set a TTL, expireMaxSize or expireStoreSize)"))
	case m.exp.maxEvictPerOp > 0 && !anyExpiry:
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashMap "+m.name+
			": expireMaxEvictPerOp requires a TTL, expireMaxSize or expireStoreSize"))
	}
	h, marker := createHasher32(m.hasher, m.ks)
	var built ConcurrentHashMap[K, V]
	var kind string
	switch {
	case anyExpiry:
		ttl, accessOrder := writeTTL, false
		if m.exp.getTTL > 0 {
			ttl, accessOrder = m.exp.getTTL, true
		}
		built, kind = htree.NewHTreeCache(m.db.st, m.cconfig(h, ttl, accessOrder)), "CACHE"
	case !m.valueInline:
		built, kind = htree.NewHTreeMapExternal(m.db.st, m.hconfig(h)), "EXTERNAL"
	default:
		built, kind = htree.NewHTreeMap(m.db.st, m.hconfig(h)), "PLAIN"
	}
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle() // never cached: release the unpublished lease
		}
	}()
	var counterRecid uint64
	if m.counter && !anyExpiry {
		counterRecid = store.Put(m.db.st, int64(0), ser.Long)
	}
	n := m.name
	header := built.HeaderRecid()
	if counterRecid != 0 {
		built = withCounter(m.db.st, built, counterRecid)
	}
	// Runtime wiring before publish (Java create2): a failing listener or
	// loader publishes nothing and the adopted-defer releases built.
	m.configure(built)
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "HashMap"
		c[n+"#keySerializer"] = ser.DescriptorOrCustom(m.ks)
		c[n+"#valueSerializer"] = ser.DescriptorOrCustom(m.vs)
		c[n+"#headerRecid"] = itoa(header)
		m.geo.write(c, n)
		c[n+"#hasher"] = marker
		c[n+"#kind"] = kind
		c[n+"#counterRecid"] = itoa(counterRecid)
	})
	cacheInsert(m.db, m.name, built, built.CloseHandle, built.Clear, true)
	adopted = true
	return built
}

func (m *HashMapMaker[K, V]) verifyCatalog(cat NameCatalog) (kind string, h mdhash.Hasher[K]) {
	n := m.name
	m.db.typeCheck(cat, n, "HashMap")
	m.requireSerializers()
	verifySer(requireField(cat, n+"#keySerializer"), m.ks)
	verifySer(requireField(cat, n+"#valueSerializer"), m.vs)
	h = openHasher32(requireField(cat, n+"#hasher"), m.hasher, m.ks)
	kind = kindOf(cat, n, "PLAIN", "CACHE", "EXTERNAL")
	if (m.overflow != nil || m.evictListen != nil) && kind != "CACHE" {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashMap "+n+
			": expireOverflow/expireEvictionListener require an expiring cache"))
	}
	return kind, h
}

func (m *HashMapMaker[K, V]) openLocked() ConcurrentHashMap[K, V] {
	kind, h := m.verifyCatalog(m.db.catalog)
	if c, ok := cacheLookup[ConcurrentHashMap[K, V]](m.db, m.name); ok {
		m.configure(c)
		return c
	}
	n := m.name
	header := parseRecid(m.db.catalog, n+"#headerRecid")
	var built ConcurrentHashMap[K, V]
	var err error
	switch kind {
	case "EXTERNAL":
		built, err = htree.OpenHTreeMapExternal(m.db.st, header, m.hconfig(h))
	case "CACHE":
		built, err = htree.OpenHTreeCache(m.db.st, header, m.cconfig(h, 0, false))
	default:
		built, err = htree.OpenHTreeMap(m.db.st, header, m.hconfig(h))
	}
	if err != nil {
		panic(err)
	}
	if counterRecid := parseRecidDefault0(m.db.catalog, n+"#counterRecid"); counterRecid != 0 {
		built = withCounter(m.db.st, built, counterRecid)
	}
	return m.cacheAndConfigure(built)
}

// cacheAndConfigure caches the handle (delete clears entries while live,
// then retires) and applies the runtime-only wiring.
//
// Wiring runs BEFORE caching, so a wiring panic never leaves a dead handle
// cached under the name; the uncached handle releases its lease.
func (m *HashMapMaker[K, V]) cacheAndConfigure(mp ConcurrentHashMap[K, V]) ConcurrentHashMap[K, V] {
	adopted := false
	defer func() {
		if !adopted {
			mp.CloseHandle()
		}
	}()
	m.configure(mp)
	cacheInsert(m.db, m.name, mp, mp.CloseHandle, mp.Clear, true)
	adopted = true
	return mp
}

// configure applies runtime-only wiring (Java configureRuntime), also on
// cache hits.
func (m *HashMapMaker[K, V]) configure(mp ConcurrentHashMap[K, V]) {
	inner := unwrapCounted(mp)
	if c, ok := inner.(*htree.HTreeCache[K, V]); ok {
		if m.overflow != nil {
			c.SetOverflow(m.overflow)
		}
		if m.evictListen != nil {
			c.SetEvictionListener(m.evictListen)
		}
	}
	if m.loader != nil {
		inner.ValueLoader(m.loader)
	}
	for _, l := range m.listeners {
		inner.ModificationListenerAdd(l)
	}
}

// Create builds the named map; fails WrongConfiguration when the name is
// taken.
func (m *HashMapMaker[K, V]) Create() (mp ConcurrentHashMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked(), nil
}

// Open reopens the named map, verifying the stored codecs and hasher.
func (m *HashMapMaker[K, V]) Open() (mp ConcurrentHashMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked(), nil
}

// CreateOrOpen opens when present, else creates — one decision under the
// admin lock.
func (m *HashMapMaker[K, V]) CreateOrOpen() (mp ConcurrentHashMap[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked(), nil
	}
	return m.createLocked(), nil
}

// ============================ HashSet ============================

// MapBackedSet is a persistent hash set over a hash map with zero-byte
// values (Java MapBackedSet).
type MapBackedSet[K any] struct {
	m ConcurrentHashMap[K, struct{}]
}

// BackingMap exposes the backing map.
func (s *MapBackedSet[K]) BackingMap() ConcurrentHashMap[K, struct{}] { return s.m }

// Add inserts e; true if it was absent.
func (s *MapBackedSet[K]) Add(e K) bool {
	_, existed := s.m.PutIfAbsent(e, struct{}{})
	return !existed
}

// Contains reports membership.
func (s *MapBackedSet[K]) Contains(e K) bool { return s.m.ContainsKey(e) }

// Remove deletes e; true if it was present.
func (s *MapBackedSet[K]) Remove(e K) bool { return s.m.RemoveOnly(e) }

// Len is the element count (saturating).
func (s *MapBackedSet[K]) Len() int { return s.m.Len() }

// LenLong is the element count.
func (s *MapBackedSet[K]) LenLong() int64 { return s.m.LenLong() }

// IsEmpty reports emptiness.
func (s *MapBackedSet[K]) IsEmpty() bool { return s.m.IsEmpty() }

// Clear removes every element.
func (s *MapBackedSet[K]) Clear() { s.m.Clear() }

// All iterates the elements (weakly consistent, unordered).
func (s *MapBackedSet[K]) All() iter.Seq[K] { return s.m.Keys() }

// IsClosed reports whether the handle was retired.
func (s *MapBackedSet[K]) IsClosed() bool { return s.m.IsClosed() }

func (s *MapBackedSet[K]) closeHandle() { s.m.CloseHandle() }

// HashSetMaker builds a hash set (Java DB.HashSetMaker): an HTreeMap, or an
// HTreeCache when an expiry option is set, with zero-byte values.
type HashSetMaker[K any] struct {
	db      *DB
	name    string
	sr      ser.Serializer[K]
	hasher  mdhash.Hasher[K]
	geo     geometry
	exp     expiry
	clock   func() int64
	counter bool
	seed    int32
	seedSet bool
}

// HashSet begins a typed hash-set open/create (Java db.hashSet).
func HashSet[K any](d *DB, name string, serializer ser.Serializer[K]) *HashSetMaker[K] {
	return &HashSetMaker[K]{db: d, name: name, sr: serializer, geo: geometry{4, 7, 4}}
}

// Hasher sets the placement hasher (see HashMapMaker.Hasher).
func (m *HashSetMaker[K]) Hasher(h mdhash.Hasher[K]) *HashSetMaker[K] { m.hasher = h; return m }

// Layout sets the geometry.
func (m *HashSetMaker[K]) Layout(concShift, dirShift, levels int) *HashSetMaker[K] {
	m.geo = geometry{concShift, dirShift, levels}
	return m
}

// ExpireAfterCreate expires elements ttlMillis after creation.
func (m *HashSetMaker[K]) ExpireAfterCreate(ttlMillis int64) *HashSetMaker[K] {
	m.exp.createTTL = requirePositiveTTL(ttlMillis, "expireAfterCreate")
	return m
}

// ExpireAfterGet expires elements ttlMillis after their last access.
func (m *HashSetMaker[K]) ExpireAfterGet(ttlMillis int64) *HashSetMaker[K] {
	m.exp.getTTL = requirePositiveTTL(ttlMillis, "expireAfterGet")
	return m
}

// ExpireMaxSize evicts once the set holds more than maxSize elements.
func (m *HashSetMaker[K]) ExpireMaxSize(maxSize int64) *HashSetMaker[K] {
	m.exp.maxSize = requireNonNegative(maxSize, "expireMaxSize")
	return m
}

// ExpireStoreSize evicts once the store exceeds storeSizeBytes.
func (m *HashSetMaker[K]) ExpireStoreSize(storeSizeBytes int64) *HashSetMaker[K] {
	m.exp.storeSize = requireNonNegative(storeSizeBytes, "expireStoreSize")
	return m
}

// ExpireMaxEvictPerOp caps the victims one foreground operation sweeps.
func (m *HashSetMaker[K]) ExpireMaxEvictPerOp(n int64) *HashSetMaker[K] {
	m.exp.maxEvictPerOp = requireNonNegative(n, "expireMaxEvictPerOp")
	return m
}

// ExpireClock injects the cache clock (Go-only, runtime only).
func (m *HashSetMaker[K]) ExpireClock(f func() int64) *HashSetMaker[K] { m.clock = f; return m }

// HashSeed sets an explicit create-time seed.
func (m *HashSetMaker[K]) HashSeed(seed int32) *HashSetMaker[K] {
	m.seed, m.seedSet = seed, true
	return m
}

// CounterEnable persists an O(1) size counter (not with expiry).
func (m *HashSetMaker[K]) CounterEnable() *HashSetMaker[K] { m.counter = true; return m }

func (m *HashSetMaker[K]) requireSerializer() {
	if m.sr == nil {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashSet "+m.name+" requires a serializer"))
	}
}

func (m *HashSetMaker[K]) hconfig(h mdhash.Hasher[K]) htree.HConfig[K, struct{}] {
	return htree.HConfig[K, struct{}]{KeySer: m.sr, ValueSer: noValueSer{},
		ConcShift: m.geo.concShift, DirShift: m.geo.dirShift, Levels: m.geo.levels,
		HashSeed: m.seed, HashSeedSet: m.seedSet, Hasher: h}
}

func (m *HashSetMaker[K]) cconfig(h mdhash.Hasher[K], ttl int64, accessOrder bool) htree.CConfig[K, struct{}] {
	return htree.CConfig[K, struct{}]{KeySer: m.sr, ValueSer: noValueSer{},
		ConcShift: m.geo.concShift, DirShift: m.geo.dirShift, Levels: m.geo.levels,
		TTL: ttl, AccessOrder: accessOrder, MaxSize: m.exp.maxSize,
		StoreSize: m.exp.storeSize, MaxEvictPerOp: m.exp.maxEvictPerOp,
		HashSeed: m.seed, HashSeedSet: m.seedSet, Hasher: h, Clock: m.clock}
}

func (m *HashSetMaker[K]) cacheSet(s *MapBackedSet[K]) *MapBackedSet[K] {
	cacheInsert(m.db, m.name, s, s.closeHandle, s.Clear, true)
	return s
}

func (m *HashSetMaker[K]) createLocked() *MapBackedSet[K] {
	m.db.checkCreatable(m.name)
	m.requireSerializer()
	anyExpiry := m.exp.any()
	if m.exp.createTTL > 0 && m.exp.getTTL > 0 {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashSet "+m.name+
			": mapdb HTreeCache supports a single TTL; cannot combine expireAfterCreate with expireAfterGet"))
	}
	if m.exp.maxEvictPerOp > 0 && !anyExpiry {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashSet "+m.name+
			": expireMaxEvictPerOp requires a TTL, expireMaxSize or expireStoreSize"))
	}
	h, marker := createHasher32(m.hasher, m.sr)
	var built ConcurrentHashMap[K, struct{}]
	kind := "PLAIN"
	if anyExpiry {
		ttl, accessOrder := m.exp.createTTL, false
		if m.exp.getTTL > 0 {
			ttl, accessOrder = m.exp.getTTL, true
		}
		built, kind = htree.NewHTreeCache(m.db.st, m.cconfig(h, ttl, accessOrder)), "CACHE"
	} else {
		built = htree.NewHTreeMap(m.db.st, m.hconfig(h))
	}
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle()
		}
	}()
	var counterRecid uint64
	if m.counter && !anyExpiry {
		counterRecid = store.Put(m.db.st, int64(0), ser.Long)
	}
	n := m.name
	header := built.HeaderRecid()
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "HashSet"
		c[n+"#serializer"] = ser.DescriptorOrCustom(m.sr)
		c[n+"#headerRecid"] = itoa(header)
		m.geo.write(c, n)
		c[n+"#hasher"] = marker
		c[n+"#kind"] = kind
		c[n+"#counterRecid"] = itoa(counterRecid)
	})
	if counterRecid != 0 {
		built = withCounter(m.db.st, built, counterRecid)
	}
	s := m.cacheSet(&MapBackedSet[K]{m: built})
	adopted = true
	return s
}

func (m *HashSetMaker[K]) openLocked() *MapBackedSet[K] {
	n := m.name
	cat := m.db.catalog
	m.db.typeCheck(cat, n, "HashSet")
	m.requireSerializer()
	verifySer(requireField(cat, n+"#serializer"), m.sr)
	h := openHasher32(requireField(cat, n+"#hasher"), m.hasher, m.sr)
	kind := kindOf(cat, n, "PLAIN", "CACHE")
	if s, ok := cacheLookup[*MapBackedSet[K]](m.db, n); ok {
		return s
	}
	header := parseRecid(cat, n+"#headerRecid")
	var built ConcurrentHashMap[K, struct{}]
	var err error
	if kind == "CACHE" {
		built, err = htree.OpenHTreeCache(m.db.st, header, m.cconfig(h, 0, false))
	} else {
		built, err = htree.OpenHTreeMap(m.db.st, header, m.hconfig(h))
	}
	if err != nil {
		panic(err)
	}
	if counterRecid := parseRecidDefault0(cat, n+"#counterRecid"); counterRecid != 0 {
		built = withCounter(m.db.st, built, counterRecid)
	}
	return m.cacheSet(&MapBackedSet[K]{m: built})
}

// Create builds the named set.
func (m *HashSetMaker[K]) Create() (s *MapBackedSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked(), nil
}

// Open reopens the named set.
func (m *HashSetMaker[K]) Open() (s *MapBackedSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked(), nil
}

// CreateOrOpen opens when present, else creates.
func (m *HashSetMaker[K]) CreateOrOpen() (s *MapBackedSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked(), nil
	}
	return m.createLocked(), nil
}

// ============================ HashMap48 ============================

// HashMap48Maker builds the wide-hash map (Java DB.HashMap48Maker).
// Defaults per Java: layout (6, 6, 7).
type HashMap48Maker[K, V any] struct {
	db        *DB
	name      string
	ks        ser.Serializer[K]
	vs        ser.Serializer[V]
	hasher    mdhash.Hasher64[K]
	geo       geometry
	seed      int64
	seedSet   bool
	listeners []mapdb5.MapModificationListener[K, V]
	loader    func(K) (V, bool)
}

// HashMap48 begins a typed 48-bit hash-map open/create (Java db.hashMap48).
func HashMap48[K, V any](d *DB, name string, keySer ser.Serializer[K], valueSer ser.Serializer[V]) *HashMap48Maker[K, V] {
	return &HashMap48Maker[K, V]{db: d, name: name, ks: keySer, vs: valueSer, geo: geometry{6, 6, 7}}
}

// Hasher sets the 64-bit placement hasher (mdhash.Long64/String64/
// ByteArray64 are recorded by name, as in Java).
func (m *HashMap48Maker[K, V]) Hasher(h mdhash.Hasher64[K]) *HashMap48Maker[K, V] {
	m.hasher = h
	return m
}

// Layout sets the geometry; concShift + levels*dirShift must equal 48.
func (m *HashMap48Maker[K, V]) Layout(concShift, dirShift, levels int) *HashMap48Maker[K, V] {
	m.geo = geometry{concShift, dirShift, levels}
	return m
}

// HashSeed sets an explicit create-time seed.
func (m *HashMap48Maker[K, V]) HashSeed(seed int64) *HashMap48Maker[K, V] {
	m.seed, m.seedSet = seed, true
	return m
}

// ModificationListener registers a runtime modification listener.
func (m *HashMap48Maker[K, V]) ModificationListener(l mapdb5.MapModificationListener[K, V]) *HashMap48Maker[K, V] {
	if l == nil {
		panic(mapdb5.NewMisuse("listener is nil"))
	}
	m.listeners = append(m.listeners, l)
	return m
}

// ValueLoader installs the missing-key loader.
func (m *HashMap48Maker[K, V]) ValueLoader(f func(K) (V, bool)) *HashMap48Maker[K, V] {
	m.loader = f
	return m
}

func (m *HashMap48Maker[K, V]) requireSerializers() {
	if m.ks == nil || m.vs == nil {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"hashMap48 "+m.name+" requires key and value serializers"))
	}
}

func (m *HashMap48Maker[K, V]) config(h mdhash.Hasher64[K]) htree.H48Config[K, V] {
	return htree.H48Config[K, V]{KeySer: m.ks, ValueSer: m.vs,
		ConcShift: m.geo.concShift, DirShift: m.geo.dirShift, Levels: m.geo.levels,
		HashSeed: m.seed, HashSeedSet: m.seedSet, Hasher: h}
}

func (m *HashMap48Maker[K, V]) configure(mp *htree.HTreeMap48[K, V]) {
	if m.loader != nil {
		mp.ValueLoader(m.loader)
	}
	for _, l := range m.listeners {
		mp.ModificationListenerAdd(l)
	}
}

func (m *HashMap48Maker[K, V]) publish(built *htree.HTreeMap48[K, V], marker string) *htree.HTreeMap48[K, V] {
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle()
		}
	}()
	n := m.name
	m.configure(built) // wiring before publish (Java create2)
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "HashMap48"
		c[n+"#keySerializer"] = ser.DescriptorOrCustom(m.ks)
		c[n+"#valueSerializer"] = ser.DescriptorOrCustom(m.vs)
		c[n+"#headerRecid"] = itoa(built.HeaderRecid())
		c[n+"#hasher64"] = marker
		m.geo.write(c, n)
	})
	cacheInsert(m.db, n, built, built.CloseHandle, built.Clear, true)
	adopted = true
	return built
}

func (m *HashMap48Maker[K, V]) createLocked() *htree.HTreeMap48[K, V] {
	m.db.checkCreatable(m.name)
	m.requireSerializers()
	h, marker := createHasher64(m.hasher, m.ks)
	return m.publish(htree.NewHTreeMap48(m.db.st, m.config(h)), marker)
}

func (m *HashMap48Maker[K, V]) openLocked() *htree.HTreeMap48[K, V] {
	n := m.name
	cat := m.db.catalog
	m.db.typeCheck(cat, n, "HashMap48")
	m.requireSerializers()
	verifySer(requireField(cat, n+"#keySerializer"), m.ks)
	verifySer(requireField(cat, n+"#valueSerializer"), m.vs)
	h := openHasher64(requireField(cat, n+"#hasher64"), m.hasher, m.ks)
	if mp, ok := cacheLookup[*htree.HTreeMap48[K, V]](m.db, n); ok {
		m.configure(mp)
		return mp
	}
	mp, err := htree.OpenHTreeMap48(m.db.st, parseRecid(cat, n+"#headerRecid"), m.config(h))
	if err != nil {
		panic(err)
	}
	adopted := false
	defer func() {
		if !adopted {
			mp.CloseHandle()
		}
	}()
	m.configure(mp)
	cacheInsert(m.db, n, mp, mp.CloseHandle, mp.Clear, true)
	adopted = true
	return mp
}

// Create builds the named map.
func (m *HashMap48Maker[K, V]) Create() (mp *htree.HTreeMap48[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked(), nil
}

// Open reopens the named map.
func (m *HashMap48Maker[K, V]) Open() (mp *htree.HTreeMap48[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked(), nil
}

// CreateOrOpen opens when present, else creates.
func (m *HashMap48Maker[K, V]) CreateOrOpen() (mp *htree.HTreeMap48[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked(), nil
	}
	return m.createLocked(), nil
}

// CreateFromSortedByHash bulk-builds from entries sorted by unsigned 64-bit
// placement hash; an explicit HashSeed is required so callers can compute
// the order (Java parity).
func (m *HashMap48Maker[K, V]) CreateFromSortedByHash(entries iter.Seq2[K, V]) (mp *htree.HTreeMap48[K, V], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	m.db.checkCreatable(m.name)
	m.requireSerializers()
	if !m.seedSet {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashMap48 bulk creation requires an explicit hashSeed"))
	}
	h, marker := createHasher64(m.hasher, m.ks)
	var built *htree.HTreeMap48[K, V]
	var cerr error
	m.db.withUserCode(func() { built, cerr = htree.CreateHTreeMap48FromSortedByHash(m.db.st, m.config(h), entries) })
	if cerr != nil {
		return nil, notSortedToDBError(cerr)
	}
	return m.publish(built, marker), nil
}

// ============================ HashSet48 ============================

// HashSet48Maker builds a set over HTreeMap48 (Java DB.HashSet48Maker).
type HashSet48Maker[K any] struct {
	db      *DB
	name    string
	sr      ser.Serializer[K]
	hasher  mdhash.Hasher64[K]
	geo     geometry
	seed    int64
	seedSet bool
}

// HashSet48 begins a typed 48-bit hash-set open/create (Java db.hashSet48).
func HashSet48[K any](d *DB, name string, serializer ser.Serializer[K]) *HashSet48Maker[K] {
	return &HashSet48Maker[K]{db: d, name: name, sr: serializer, geo: geometry{6, 6, 7}}
}

// Hasher sets the 64-bit placement hasher.
func (m *HashSet48Maker[K]) Hasher(h mdhash.Hasher64[K]) *HashSet48Maker[K] { m.hasher = h; return m }

// Layout sets the geometry.
func (m *HashSet48Maker[K]) Layout(concShift, dirShift, levels int) *HashSet48Maker[K] {
	m.geo = geometry{concShift, dirShift, levels}
	return m
}

// HashSeed sets an explicit create-time seed.
func (m *HashSet48Maker[K]) HashSeed(seed int64) *HashSet48Maker[K] {
	m.seed, m.seedSet = seed, true
	return m
}

func (m *HashSet48Maker[K]) requireSerializer() {
	if m.sr == nil {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashSet48 "+m.name+" requires a serializer"))
	}
}

func (m *HashSet48Maker[K]) config(h mdhash.Hasher64[K]) htree.H48Config[K, struct{}] {
	return htree.H48Config[K, struct{}]{KeySer: m.sr, ValueSer: noValueSer{},
		ConcShift: m.geo.concShift, DirShift: m.geo.dirShift, Levels: m.geo.levels,
		HashSeed: m.seed, HashSeedSet: m.seedSet, Hasher: h}
}

func (m *HashSet48Maker[K]) publish(built *htree.HTreeMap48[K, struct{}], marker string) *MapBackedSet[K] {
	adopted := false
	defer func() {
		if !adopted {
			built.CloseHandle()
		}
	}()
	n := m.name
	m.db.publishCatalog(func(c NameCatalog) {
		c[n+"#type"] = "HashSet48"
		c[n+"#serializer"] = ser.DescriptorOrCustom(m.sr)
		c[n+"#headerRecid"] = itoa(built.HeaderRecid())
		c[n+"#hasher64"] = marker
		m.geo.write(c, n)
	})
	s := &MapBackedSet[K]{m: built}
	cacheInsert(m.db, n, s, s.closeHandle, s.Clear, true)
	adopted = true
	return s
}

func (m *HashSet48Maker[K]) createLocked() *MapBackedSet[K] {
	m.db.checkCreatable(m.name)
	m.requireSerializer()
	h, marker := createHasher64(m.hasher, m.sr)
	return m.publish(htree.NewHTreeMap48(m.db.st, m.config(h)), marker)
}

func (m *HashSet48Maker[K]) openLocked() *MapBackedSet[K] {
	n := m.name
	cat := m.db.catalog
	m.db.typeCheck(cat, n, "HashSet48")
	m.requireSerializer()
	verifySer(requireField(cat, n+"#serializer"), m.sr)
	h := openHasher64(requireField(cat, n+"#hasher64"), m.hasher, m.sr)
	if s, ok := cacheLookup[*MapBackedSet[K]](m.db, n); ok {
		return s
	}
	mp, err := htree.OpenHTreeMap48(m.db.st, parseRecid(cat, n+"#headerRecid"), m.config(h))
	if err != nil {
		panic(err)
	}
	s := &MapBackedSet[K]{m: mp}
	cacheInsert(m.db, n, s, s.closeHandle, s.Clear, true)
	return s
}

// Create builds the named set.
func (m *HashSet48Maker[K]) Create() (s *MapBackedSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.createLocked(), nil
}

// Open reopens the named set.
func (m *HashSet48Maker[K]) Open() (s *MapBackedSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	return m.openLocked(), nil
}

// CreateOrOpen opens when present, else creates.
func (m *HashSet48Maker[K]) CreateOrOpen() (s *MapBackedSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	if _, ok := m.db.catalog[m.name+"#type"]; ok {
		return m.openLocked(), nil
	}
	return m.createLocked(), nil
}

// CreateFromSortedByHash bulk-builds from elements sorted by unsigned
// 64-bit placement hash; requires an explicit HashSeed.
func (m *HashSet48Maker[K]) CreateFromSortedByHash(elements iter.Seq[K]) (s *MapBackedSet[K], err error) {
	defer dbErrorToErr(&err)
	m.db.lockAdmin()
	defer m.db.adminMu.Unlock()
	m.db.checkCreatable(m.name)
	m.requireSerializer()
	if !m.seedSet {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "hashSet48 bulk creation requires an explicit hashSeed"))
	}
	h, marker := createHasher64(m.hasher, m.sr)
	entries := func(yield func(K, struct{}) bool) {
		for e := range elements {
			if !yield(e, struct{}{}) {
				return
			}
		}
	}
	var built *htree.HTreeMap48[K, struct{}]
	var cerr error
	m.db.withUserCode(func() {
		built, cerr = htree.CreateHTreeMap48FromSortedByHash(m.db.st, m.config(h), iter.Seq2[K, struct{}](entries))
	})
	if cerr != nil {
		return nil, notSortedToDBError(cerr)
	}
	return m.publish(built, marker), nil
}
