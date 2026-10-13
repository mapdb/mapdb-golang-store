// Package benchkit is the shared benchmark harness behind the testing.B
// benchmarks (bench/) and the standalone runner (cmd/bench). It mirrors the
// Java JMH suite of mapdb-java-store (archived with its results): the same
// seeded dataset, the same subjects where Go has them, and the same workload
// names, so numbers line up across the implementations.
package benchkit

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/mapdb/mapdb5-go/btree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// Seed is the Java harness's dataset seed (0xC0FFEE).
const Seed = 0xC0FFEE

// MaxNodeSize is the BTree node size every mapdb5 subject uses (Java: 32).
const MaxNodeSize = 32

// BufferBytes is the BufferTreeMap directory buffer (Java: 4096).
const BufferBytes = 4096

// DataType is a key/value shape: Long→Long or String(24)→String(24).
type DataType string

const (
	Long   DataType = "LONG"
	String DataType = "STRING"
)

// Str24 is Java DataType.str24: 'k' + 23 zero-padded digits, so numeric
// order equals lexicographic order.
func Str24(i int64) string { return pad24('k', i) }

// Val24 is Java DataType.val24.
func Val24(i int64) string { return pad24('v', i) }

func pad24(prefix byte, i int64) string {
	if i < 0 {
		i = -i
	}
	s := strconv.FormatInt(i, 10)
	var b strings.Builder
	b.Grow(24)
	b.WriteByte(prefix)
	for p := 23 - len(s); p > 0; p-- {
		b.WriteByte('0')
	}
	b.WriteString(s)
	return b.String()
}

// Dataset is Java Dataset: pre-materialized keys in ascending order,
// values, a seeded permutation probe[] of [0,n), and n keys guaranteed
// absent (logical indices [n,2n)).
type Dataset[K, V any] struct {
	N        int
	Keys     []K
	Values   []V
	Probe    []int
	MissKeys []K
}

func newDataset[K, V any](n int, key func(int64) K, val func(int64) V) *Dataset[K, V] {
	d := &Dataset[K, V]{N: n, Keys: make([]K, n), Values: make([]V, n),
		Probe: make([]int, n), MissKeys: make([]K, n)}
	for i := 0; i < n; i++ {
		d.Keys[i] = key(int64(i))
		d.Values[i] = val(int64(i))
		d.MissKeys[i] = key(int64(n) + int64(i))
		d.Probe[i] = i
	}
	// Seeded Fisher-Yates (Go's PCG, not java.util.Random: the permutation
	// differs from Java's, its distribution does not).
	r := rand.New(rand.NewPCG(Seed, Seed))
	for i := n - 1; i > 0; i-- {
		j := r.IntN(i + 1)
		d.Probe[i], d.Probe[j] = d.Probe[j], d.Probe[i]
	}
	return d
}

// LongDataset builds the Long→Long dataset.
func LongDataset(n int) *Dataset[int64, int64] {
	return newDataset(n, func(i int64) int64 { return i }, func(i int64) int64 { return i })
}

// StringDataset builds the String(24)→String(24) dataset.
func StringDataset(n int) *Dataset[string, string] {
	return newDataset(n, Str24, Val24)
}

// Map is the operation surface a subject exposes (Java MapSubject).
type Map[K, V any] interface {
	Get(K) (V, bool)
	Put(K, V) (V, bool)
	// IterateChecksum walks every entry in order and folds it into a
	// checksum; ok=false for unordered subjects.
	IterateChecksum() (sum uint64, ok bool)
	Close()
}

// Subject is one map implementation under test.
type Subject struct {
	ID      string // Java-compatible display name
	Key     string // short CLI name
	Ordered bool
	// MultiThread: false means never run on more than one goroutine.
	MultiThread bool
	long        func() Map[int64, int64]
	str         func() Map[string, string]
}

// BuildLong builds a fresh Long→Long instance.
func (s Subject) BuildLong() Map[int64, int64] { return s.long() }

// BuildString builds a fresh String→String instance.
func (s Subject) BuildString() Map[string, string] { return s.str() }

// Subjects is the catalogue, in table order.
var Subjects = []Subject{
	{ID: "mapdb5-BTree/StoreOnHeap", Key: "onheap", Ordered: true, MultiThread: true,
		long: func() Map[int64, int64] { return btreeLong(store.NewStoreOnHeap(true)) },
		str:  func() Map[string, string] { return btreeString(store.NewStoreOnHeap(true)) }},
	{ID: "mapdb5-BTree/StoreDirect(heap)", Key: "direct", Ordered: true, MultiThread: true,
		long: func() Map[int64, int64] { return btreeLong(store.NewStoreDirectHeap(true)) },
		str:  func() Map[string, string] { return btreeString(store.NewStoreDirectHeap(true)) }},
	{ID: "mapdb5-BufferTree/StoreDirect(heap)", Key: "buffer-direct", Ordered: true, MultiThread: true,
		long: func() Map[int64, int64] { return bufferLong(store.NewStoreDirectHeap(true)) },
		str:  func() Map[string, string] { return bufferString(store.NewStoreDirectHeap(true)) }},
	{ID: "go map (unordered baseline)", Key: "gomap", Ordered: false, MultiThread: false,
		long: func() Map[int64, int64] { return goMap[int64, int64]{} },
		str:  func() Map[string, string] { return goMap[string, string]{} }},
}

// SubjectByKey finds a subject by its short name.
func SubjectByKey(key string) (Subject, error) {
	for _, s := range Subjects {
		if s.Key == key {
			return s, nil
		}
	}
	return Subject{}, fmt.Errorf("unknown subject %q", key)
}

// ---- mapdb5 BTreeMap ----

type btreeMap[K, V any] struct {
	m   *btree.BTreeMap[K, V]
	st  store.Store
	sum func(K, V) uint64
}

func (b *btreeMap[K, V]) Get(k K) (V, bool)      { return b.m.Get(k) }
func (b *btreeMap[K, V]) Put(k K, v V) (V, bool) { return b.m.Put(k, v) }
func (b *btreeMap[K, V]) IterateChecksum() (uint64, bool) {
	var acc uint64
	for k, v := range b.m.All() {
		acc += b.sum(k, v)
	}
	return acc, true
}
func (b *btreeMap[K, V]) Close() { _ = b.m.Close(); _ = b.st.Close() }

func btreeLong(st store.Store) Map[int64, int64] {
	m := btree.NewBTreeMap(st, btree.Config[int64, int64]{
		KeyFormat: ser.LongFormat, ValueFormat: ser.LongFormat, MaxNodeSize: MaxNodeSize})
	return &btreeMap[int64, int64]{m: m, st: st, sum: sumLong}
}

func btreeString(st store.Store) Map[string, string] {
	m := btree.NewBTreeMap(st, btree.Config[string, string]{
		KeyFormat: ser.StringGroupFormat, ValueFormat: ser.StringGroupFormat, MaxNodeSize: MaxNodeSize})
	return &btreeMap[string, string]{m: m, st: st, sum: sumString}
}

// ---- mapdb5 BufferTreeMap ----

type bufferMap[K, V any] struct {
	m   *btree.BufferTreeMap[K, V]
	st  store.Store
	sum func(K, V) uint64
}

func (b *bufferMap[K, V]) Get(k K) (V, bool)      { return b.m.Get(k) }
func (b *bufferMap[K, V]) Put(k K, v V) (V, bool) { return b.m.Put(k, v) }
func (b *bufferMap[K, V]) IterateChecksum() (uint64, bool) {
	var acc uint64
	for k, v := range b.m.All() {
		acc += b.sum(k, v)
	}
	return acc, true
}
func (b *bufferMap[K, V]) Close() { _ = b.m.Close(); _ = b.st.Close() }

func bufferLong(st store.StoreDelta) Map[int64, int64] {
	m := btree.NewBufferTreeMap(st, btree.BufferTreeConfig[int64, int64]{
		KeyFormat: ser.LongFormat, ValueFormat: ser.LongFormat,
		MaxNodeSize: MaxNodeSize, BufferBytes: BufferBytes})
	return &bufferMap[int64, int64]{m: m, st: st, sum: sumLong}
}

func bufferString(st store.StoreDelta) Map[string, string] {
	m := btree.NewBufferTreeMap(st, btree.BufferTreeConfig[string, string]{
		KeyFormat: ser.StringGroupFormat, ValueFormat: ser.StringGroupFormat,
		MaxNodeSize: MaxNodeSize, BufferBytes: BufferBytes})
	return &bufferMap[string, string]{m: m, st: st, sum: sumString}
}

// ---- Go builtin map ----

type goMap[K comparable, V any] map[K]V

func (g goMap[K, V]) Get(k K) (V, bool) { v, ok := g[k]; return v, ok }
func (g goMap[K, V]) Put(k K, v V) (V, bool) {
	old, ok := g[k]
	g[k] = v
	return old, ok
}
func (g goMap[K, V]) IterateChecksum() (uint64, bool) { return 0, false }
func (g goMap[K, V]) Close()                          {}

func sumLong(k, v int64) uint64 { return uint64(k) ^ uint64(v) }

func sumString(k, v string) uint64 {
	return uint64(len(k)) ^ uint64(k[len(k)-1]) ^ uint64(len(v))<<8 ^ uint64(v[len(v)-1])
}
