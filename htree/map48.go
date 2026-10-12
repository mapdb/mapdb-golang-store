package htree

// HTreeMap48 — port of Java org.mapdb.htree.HTreeMap48: the HTreeMap
// variant with a 48-BIT hash budget for maps whose index space outgrows
// the 32-bit partition (the 32-bit map saturates around 10^9 entries;
// buckets then grow linearly). It consumes the TOP 48 bits of a 64-bit
// Hasher64 hash, partitioned concShift + levels*dirShift == 48; DirTree
// runs on int64 indices already and is shared unchanged.
//
// ENTROPY (Java's warning, verbatim intent): maps that NEED the 48-bit
// space must supply a genuinely 64-bit Hasher64 — mdhash.Long64,
// String64, ByteArray64 or Mixing64. The SerHasher64 default is
// deterministic across reopen but NOT Java-compatible.
//
// The header is NOT interchangeable with HTreeMap's (long seed, 48-bit
// budget). Everything else — locking, weakly-consistent iteration,
// persistence — is HTreeMap's.

import (
	"iter"
	"math/rand"

	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// h48HashBits is the hash budget the map consumes (top slice of the
// 64-bit hash) — Java HTreeMap48.HASH_BITS.
const h48HashBits = 48

// H48Config configures the 48-bit map (HConfig's 64-bit counterpart).
type H48Config[K, V any] struct {
	KeySer   ser.Serializer[K]
	ValueSer ser.Serializer[V]
	// Geometry: all three zero → Java defaults (6, 6, 7); otherwise
	// concShift + levels*dirShift must equal 48.
	ConcShift int
	DirShift  int
	Levels    int
	// HashSeed is the persisted 64-bit seed; ignored unless HashSeedSet.
	HashSeed    int64
	HashSeedSet bool
	// Hasher maps (key, seed) → 64-bit placement hash (only the top 48
	// bits are consumed). nil → mdhash.SerHasher64 over KeySer.
	Hasher mdhash.Hasher64[K]
}

// hHeader48 is the 48-bit header record — same shape as the 32-bit
// header but with a raw int64 seed (Java writeLong).
type hHeader48 struct {
	concShift, dirShift, levels int32
	hashSeed                    int64
	segmentRoots                []uint64
}

type header48Serializer struct{}

var _ ser.Serializer[hHeader48] = header48Serializer{}

func (header48Serializer) Serialize(w *bin.Writer, h hHeader48) {
	w.PackInt(h.concShift)
	w.PackInt(h.dirShift)
	w.PackInt(h.levels)
	w.WriteLong(h.hashSeed)
	for _, root := range h.segmentRoots {
		w.PackLong(int64(root))
	}
}

func (header48Serializer) Deserialize(r *bin.Reader, size int) hHeader48 {
	concShift := r.UnpackInt()
	dirShift := r.UnpackInt()
	levels := r.UnpackInt()
	hashSeed := r.ReadLong()
	if concShift < 0 || concShift > 30 {
		panic(mapdb5.NewCorruption("htree48 header concShift out of range"))
	}
	roots := make([]uint64, 1<<concShift)
	for i := range roots {
		v := r.UnpackLong()
		if v <= 0 {
			panic(mapdb5.NewCorruption("htree48 header segment root recid out of range"))
		}
		roots[i] = uint64(v)
	}
	return hHeader48{concShift: concShift, dirShift: dirShift, levels: levels,
		hashSeed: hashSeed, segmentRoots: roots}
}

func (header48Serializer) FixedSize() int { return -1 }
func (header48Serializer) SizeHint() int  { return 68 }
func (header48Serializer) Compare(a, b hHeader48) int {
	panic(mapdb5.NewMisuse("htree48 header codec is record-internal, not orderable"))
}
func (header48Serializer) Equals(a, b hHeader48) bool {
	panic(mapdb5.NewMisuse("htree48 header codec is record-internal, not orderable"))
}
func (header48Serializer) NaturalOrder() bool            { return false }
func (header48Serializer) EqualsBySerializedBytes() bool { return true }

func checkConfig48(concShift, dirShift, levels int) error {
	// hard cap 12 (4096 segments) — see HTreeMap.checkConfig.
	switch {
	case concShift < 0 || concShift > 12:
		return mapdb5.New(mapdb5.KindWrongConfiguration, "concShift must be in [0,12]")
	case dirShift < 1 || dirShift > MaxDirShift:
		return mapdb5.New(mapdb5.KindWrongConfiguration, "dirShift out of range")
	case levels < 1:
		return mapdb5.New(mapdb5.KindWrongConfiguration, "levels must be >= 1")
	case concShift+levels*dirShift != h48HashBits:
		return mapdb5.New(mapdb5.KindWrongConfiguration,
			"concShift + levels*dirShift must equal 48")
	}
	return nil
}

func resolveConfig48[K, V any](cfg H48Config[K, V]) (concShift, dirShift, levels int, err error) {
	if cfg.ConcShift == 0 && cfg.DirShift == 0 && cfg.Levels == 0 {
		return 6, 6, 7, nil // Java defaults: 64 segments, 7 levels x 6 bits
	}
	if err := checkConfig48(cfg.ConcShift, cfg.DirShift, cfg.Levels); err != nil {
		return 0, 0, 0, err
	}
	return cfg.ConcShift, cfg.DirShift, cfg.Levels, nil
}

func resolveHasher48[K, V any](cfg H48Config[K, V]) mdhash.Hasher64[K] {
	if cfg.Hasher != nil {
		return cfg.Hasher
	}
	if cfg.KeySer == nil {
		panic(mapdb5.NewMisuse("htree48: KeySer is required"))
	}
	return mdhash.SerHasher64(cfg.KeySer)
}

// HTreeMap48 is the 48-bit-budget HTreeMap variant. Its zero value is
// unusable; construct via NewHTreeMap48/OpenHTreeMap48/
// CreateHTreeMap48FromSortedByHash. All op semantics are HTreeMap's.
type HTreeMap48[K, V any] struct {
	m *HTreeMap[K, V]
}

// NewHTreeMap48 creates a fresh empty 48-bit hash map over s.
func NewHTreeMap48[K, V any](s store.Store, cfg H48Config[K, V]) *HTreeMap48[K, V] {
	m, err := createHTreeMap48(s, cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func createHTreeMap48[K, V any](s store.Store, cfg H48Config[K, V]) (*HTreeMap48[K, V], error) {
	concShift, dirShift, levels, err := resolveConfig48(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree48: KeySer and ValueSer are required")
	}
	seed := cfg.HashSeed
	if !cfg.HashSeedSet {
		seed = int64(rand.Uint64())
	}
	roots := make([]uint64, 1<<concShift)
	for i := range roots {
		roots[i] = store.Put(s, DirEmpty(), DirSer)
	}
	hdr := hHeader48{
		concShift: int32(concShift), dirShift: int32(dirShift), levels: int32(levels),
		hashSeed: seed, segmentRoots: roots,
	}
	headerRecid := store.Put(s, hdr, header48Serializer{})
	return openHTreeMap48(s, headerRecid, cfg)
}

// OpenHTreeMap48 reopens the 48-bit map whose header record is
// headerRecid; the Hasher in cfg must match create's.
func OpenHTreeMap48[K, V any](s store.Store, headerRecid store.Recid, cfg H48Config[K, V]) (m *HTreeMap48[K, V], err error) {
	defer dbErrorToErr(&err)
	return openHTreeMap48(s, headerRecid, cfg)
}

func openHTreeMap48[K, V any](s store.Store, headerRecid store.Recid, cfg H48Config[K, V]) (*HTreeMap48[K, V], error) {
	if headerRecid == 0 {
		return nil, mapdb5.NewCorruption("htree48 header recid must be a valid recid")
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree48: KeySer and ValueSer are required")
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
	hdr, ok := store.Get(s, uint64(headerRecid), header48Serializer{})
	if !ok {
		return nil, mapdb5.NewCorruption("htree48 header record is null")
	}
	if err := checkConfig48(int(hdr.concShift), int(hdr.dirShift), int(hdr.levels)); err != nil {
		return nil, mapdb5.NewCorruption("htree48 header geometry invalid: " + err.Error())
	}
	if len(hdr.segmentRoots) != 1<<hdr.concShift {
		return nil, mapdb5.NewCorruption("htree48 header segment root count mismatch")
	}
	hasher := resolveHasher48(cfg)
	seed := hdr.hashSeed
	// top 48 bits of the 64-bit mix — the best-mixed slice, and segment
	// selection at the very top mirrors the 32-bit map's layout (Java).
	hashOf := func(k K) int64 {
		return int64(uint64(hasher.Hash64(k, seed)) >> (64 - h48HashBits))
	}
	committed = true
	return &HTreeMap48[K, V]{m: &HTreeMap[K, V]{inner: newInnerBase(
		s, cfg.KeySer, cfg.ValueSer, headerRecid, hdr.segmentRoots,
		int(hdr.concShift), int(hdr.dirShift), int(hdr.levels),
		hashOf, seed,
		leafSerializer[K, V]{keySer: cfg.KeySer, valueSer: cfg.ValueSer}, lease)}}, nil
}

// CreateHTreeMap48FromSortedByHash bulk-loads a 48-bit map from entries
// sorted by non-decreasing unsigned 64-bit placement hash — the top 48
// bits are consumed, so full-64 sorting is a valid finer order (Java).
func CreateHTreeMap48FromSortedByHash[K, V any](s store.Store, cfg H48Config[K, V],
	entries iter.Seq2[K, V]) (m *HTreeMap48[K, V], err error) {
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
	concShift, dirShift, levels, err2 := resolveConfig48(cfg)
	if err2 != nil {
		return nil, err2
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree48: KeySer and ValueSer are required")
	}
	seed := cfg.HashSeed
	if !cfg.HashSeedSet {
		seed = int64(rand.Uint64())
	}
	hasher := resolveHasher48(cfg)
	leafSer := leafSerializer[K, V]{keySer: cfg.KeySer, valueSer: cfg.ValueSer}
	hashSource := func(key K) int64 {
		return int64(uint64(hasher.Hash64(key, seed)) >> (64 - h48HashBits))
	}
	headerRecid := htreeBuild(s, concShift, dirShift, levels, entries,
		hashSource, cfg.KeySer,
		func(keys []K, vals []V) uint64 {
			return store.Put(s, hleaf[K, V]{keys: keys, vals: vals}, leafSer)
		},
		func(roots []uint64) uint64 {
			return store.Put(s, hHeader48{
				concShift: int32(concShift), dirShift: int32(dirShift), levels: int32(levels),
				hashSeed: seed, segmentRoots: roots,
			}, header48Serializer{})
		})
	return openHTreeMap48(s, headerRecid, cfg)
}

// ============ forwarded op set (semantics are HTreeMap's) ============

// HashSeed64 is the persisted 64-bit placement-hash seed.
func (m *HTreeMap48[K, V]) HashSeed64() int64 { return m.m.inner.hashSeed }

// Underlying returns the shared 32-bit-structured map handle (same inner
// state; the 48-bit map only differs in hashing and header codec).
func (m *HTreeMap48[K, V]) Underlying() *HTreeMap[K, V] { return m.m }

func (m *HTreeMap48[K, V]) HeaderRecid() store.Recid         { return m.m.HeaderRecid() }
func (m *HTreeMap48[K, V]) Segments() int                    { return m.m.Segments() }
func (m *HTreeMap48[K, V]) IsClosed() bool                   { return m.m.IsClosed() }
func (m *HTreeMap48[K, V]) KeySerializer() ser.Serializer[K] { return m.m.KeySerializer() }
func (m *HTreeMap48[K, V]) ValueSerializer() ser.Serializer[V] {
	return m.m.ValueSerializer()
}
func (m *HTreeMap48[K, V]) StateID() uint64              { return m.m.StateID() }
func (m *HTreeMap48[K, V]) Close() error                 { return m.m.Close() }
func (m *HTreeMap48[K, V]) CloseHandle()                 { m.m.CloseHandle() }
func (m *HTreeMap48[K, V]) Get(key K) (V, bool)          { return m.m.Get(key) }
func (m *HTreeMap48[K, V]) ContainsKey(key K) bool       { return m.m.ContainsKey(key) }
func (m *HTreeMap48[K, V]) Put(key K, value V) (V, bool) { return m.m.Put(key, value) }
func (m *HTreeMap48[K, V]) PutIfAbsent(key K, value V) (V, bool) {
	return m.m.PutIfAbsent(key, value)
}
func (m *HTreeMap48[K, V]) PutOnly(key K, value V) { m.m.PutOnly(key, value) }
func (m *HTreeMap48[K, V]) Remove(key K) (V, bool) { return m.m.Remove(key) }
func (m *HTreeMap48[K, V]) RemoveIfEquals(key K, value V) bool {
	return m.m.RemoveIfEquals(key, value)
}
func (m *HTreeMap48[K, V]) RemoveOnly(key K) bool { return m.m.RemoveOnly(key) }
func (m *HTreeMap48[K, V]) Replace(key K, value V) (V, bool) {
	return m.m.Replace(key, value)
}
func (m *HTreeMap48[K, V]) ReplaceIfEquals(key K, oldValue, newValue V) bool {
	return m.m.ReplaceIfEquals(key, oldValue, newValue)
}
func (m *HTreeMap48[K, V]) Clear()                            { m.m.Clear() }
func (m *HTreeMap48[K, V]) ClearWithoutNotification()         { m.m.ClearWithoutNotification() }
func (m *HTreeMap48[K, V]) ClearWithExpire()                  { m.m.ClearWithExpire() }
func (m *HTreeMap48[K, V]) IsEmpty() bool                     { return m.m.IsEmpty() }
func (m *HTreeMap48[K, V]) Len() int                          { return m.m.Len() }
func (m *HTreeMap48[K, V]) LenLong() int64                    { return m.m.LenLong() }
func (m *HTreeMap48[K, V]) All() iter.Seq2[K, V]              { return m.m.All() }
func (m *HTreeMap48[K, V]) Keys() iter.Seq[K]                 { return m.m.Keys() }
func (m *HTreeMap48[K, V]) Values() iter.Seq[V]               { return m.m.Values() }
func (m *HTreeMap48[K, V]) ForEach(f func(K, V))              { m.m.ForEach(f) }
func (m *HTreeMap48[K, V]) Segments2(n int) []iter.Seq2[K, V] { return m.m.Segments2(n) }
func (m *HTreeMap48[K, V]) ModificationListenerAdd(l mapdb5.MapModificationListener[K, V]) {
	m.m.ModificationListenerAdd(l)
}
func (m *HTreeMap48[K, V]) ModificationListenerRemove(l mapdb5.MapModificationListener[K, V]) {
	m.m.ModificationListenerRemove(l)
}
func (m *HTreeMap48[K, V]) ValueLoader(f func(K) (V, bool)) { m.m.ValueLoader(f) }
func (m *HTreeMap48[K, V]) Verify()                         { m.m.Verify() }
