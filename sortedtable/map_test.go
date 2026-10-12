package sortedtable

import (
	"iter"
	"math/rand"
	"slices"
	"testing"

	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func build(t *testing.T, s store.Store, n, perPage int) *SortedTableMap[int64, int64] {
	t.Helper()
	sink := NewSinkN[int64, int64](s, ser.LongFormat, ser.LongFormat, perPage)
	for i := 0; i < n; i++ {
		sink.Put(int64(i*2), int64(i*10))
	}
	return sink.Create()
}

func sortedSeq(pairs [][2]int64) iter.Seq2[int64, int64] {
	return func(yield func(int64, int64) bool) {
		for _, p := range pairs {
			if !yield(p[0], p[1]) {
				return
			}
		}
	}
}

func TestSortedTableEmpty(t *testing.T) {
	m := build(t, store.NewStoreOnHeap(true), 0, 4)
	if !m.IsEmpty() || m.LenLong() != 0 {
		t.Fatal("not empty")
	}
	if _, ok := m.Get(5); ok {
		t.Fatal("present")
	}
	if _, _, ok := m.Min(); ok {
		t.Fatal("min")
	}
	if _, _, ok := m.Max(); ok {
		t.Fatal("max")
	}
	n := 0
	for range m.All() {
		n++
	}
	for range m.DescendingMap() {
		n++
	}
	if n != 0 {
		t.Fatal("iterated empty")
	}
}

func TestSortedTableSingle(t *testing.T) {
	m := build(t, store.NewStoreOnHeap(true), 1, 4)
	if v, ok := m.Get(0); !ok || v != 0 {
		t.Fatalf("get=%v,%v", v, ok)
	}
	if _, ok := m.Get(1); ok {
		t.Fatal("ghost")
	}
	k, v, ok := m.Min()
	if !ok || k != 0 || v != 0 {
		t.Fatalf("min=%d,%d,%v", k, v, ok)
	}
	k, v, ok = m.Max()
	if !ok || k != 0 || v != 0 {
		t.Fatalf("max=%d,%d,%v", k, v, ok)
	}
}

// TestSortedTablePageBoundaries: keys at/around page edges resolve.
func TestSortedTablePageBoundaries(t *testing.T) {
	for name, s := range map[string]store.Store{
		"heap":      store.NewStoreOnHeap(true),
		"bytearray": store.NewStoreByteArray(true),
	} {
		t.Run(name, func(t *testing.T) {
			m := build(t, s, 100, 7) // ragged page edge
			if m.LenLong() != 100 {
				t.Fatalf("len=%d", m.LenLong())
			}
			for i := int64(0); i < 100; i++ {
				v, ok := m.Get(i * 2)
				if !ok || v != i*10 {
					t.Fatalf("get(%d)=%v,%v", i*2, v, ok)
				}
			}
			// misses between keys and at edges
			for _, probe := range []int64{-3, -1, 1, 3, 197, 199, 200, 500} {
				if _, ok := m.Get(probe); ok {
					t.Fatalf("ghost get(%d)", probe)
				}
			}
		})
	}
}

// TestSortedTableRejectsUnsorted: duplicate or descending keys fail the
// build with pump.ErrNotSorted.
func TestSortedTableRejectsUnsorted(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	_, err := CreateFromSorted[int64, int64](s, ser.LongFormat, ser.LongFormat,
		sortedSeq([][2]int64{{1, 1}, {5, 5}, {4, 4}}))
	if err != pump.ErrNotSorted {
		t.Fatalf("err=%v", err)
	}
	_, err = CreateFromSorted[int64, int64](s, ser.LongFormat, ser.LongFormat,
		sortedSeq([][2]int64{{1, 1}, {1, 2}}))
	if err != pump.ErrNotSorted {
		t.Fatalf("dup err=%v", err)
	}
	// double create panics Misuse
	sink := NewSink[int64, int64](s, ser.LongFormat, ser.LongFormat)
	sink.Create()
	defer func() {
		r := recover()
		if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("panic=%v", r)
		}
	}()
	sink.Put(1, 1)
}

// TestSortedTableNavigationVsOracle: floor/ceiling/lower/higher vs a
// linear oracle over a ragged key set.
func TestSortedTableNavigationVsOracle(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	sink := NewSinkN[int64, int64](s, ser.LongFormat, ser.LongFormat, 8)
	var keys []int64
	for i := int64(0); i < 50; i++ {
		k := i*3 + 1
		keys = append(keys, k)
		sink.Put(k, k*2)
	}
	m := sink.Create()
	for probe := int64(-2); probe < 160; probe++ {
		// floor: greatest key <= probe
		var fwant int64 = -1
		for _, k := range keys {
			if k <= probe {
				fwant = k
			}
		}
		fk, _, fok := m.Floor(probe)
		if fok != (fwant >= 0) || (fok && fk != fwant) {
			t.Fatalf("floor(%d)=%d,%v want %d", probe, fk, fok, fwant)
		}
		// ceiling: least key >= probe
		var cwant int64 = -1
		for _, k := range keys {
			if k >= probe {
				cwant = k
				break
			}
		}
		ck, _, cok := m.Ceiling(probe)
		if cok != (cwant >= 0) || (cok && ck != cwant) {
			t.Fatalf("ceiling(%d)=%d,%v want %d", probe, ck, cok, cwant)
		}
		// lower: greatest key < probe
		var lwant int64 = -1
		for _, k := range keys {
			if k < probe {
				lwant = k
			}
		}
		lk, _, lok := m.Lower(probe)
		if lok != (lwant >= 0) || (lok && lk != lwant) {
			t.Fatalf("lower(%d)=%d,%v want %d", probe, lk, lok, lwant)
		}
		// higher: least key > probe
		var hwant int64 = -1
		for _, k := range keys {
			if k > probe {
				hwant = k
				break
			}
		}
		hk, _, hok := m.Higher(probe)
		if hok != (hwant >= 0) || (hok && hk != hwant) {
			t.Fatalf("higher(%d)=%d,%v want %d", probe, hk, hok, hwant)
		}
	}
}

// TestSortedTableIteration: both directions yield strictly ordered keys.
func TestSortedTableIteration(t *testing.T) {
	m := build(t, store.NewStoreOnHeap(true), 100, 6)
	var asc, desc []int64
	for k := range m.Keys() {
		asc = append(asc, k)
	}
	for k := range m.DescendingKeys() {
		desc = append(desc, k)
	}
	if !slices.IsSorted(asc) || len(asc) != 100 {
		t.Fatal("ascending")
	}
	if len(desc) != 100 {
		t.Fatal("descending count")
	}
	for i := range asc {
		if desc[len(desc)-1-i] != asc[i] {
			t.Fatal("descending not reverse")
		}
	}
	// early exit
	n := 0
	for range m.All() {
		n++
		break
	}
	if n != 1 {
		t.Fatal("early exit")
	}
}

// TestSortedTableViews: bounded ranges honor the same inclusive/
// exclusive conventions as btree's views.
func TestSortedTableViews(t *testing.T) {
	m := build(t, store.NewStoreOnHeap(true), 50, 4) // keys 0,2,...,98
	collect := func(seq iter.Seq2[int64, int64]) []int64 {
		var out []int64
		for k := range seq {
			out = append(out, k)
		}
		return out
	}
	if got := collect(m.HeadMap(20)); len(got) != 10 || got[len(got)-1] != 18 {
		t.Fatalf("head: %v", got)
	}
	if got := collect(m.TailMap(80)); len(got) != 10 || got[0] != 80 {
		t.Fatalf("tail: %v", got)
	}
	if got := collect(m.SubMap(10, 30)); len(got) != 10 || got[0] != 10 || got[len(got)-1] != 28 {
		t.Fatalf("sub: %v", got)
	}
	// empty ranges
	if got := collect(m.HeadMap(0)); len(got) != 0 {
		t.Fatal("empty head")
	}
	if got := collect(m.TailMap(99)); len(got) != 0 {
		t.Fatal("empty tail")
	}
}

// TestSortedTableMutatorsPanic: every mutator panics KindReadOnly.
func TestSortedTableMutatorsPanic(t *testing.T) {
	m := build(t, store.NewStoreOnHeap(true), 10, 4)
	mustReadOnly := func(f func()) {
		t.Helper()
		defer func() {
			r := recover()
			de, ok := mapdb5.AsDBError(r)
			if !ok || de.Kind != mapdb5.KindReadOnly {
				t.Fatalf("panic=%v want KindReadOnly", r)
			}
		}()
		f()
	}
	mustReadOnly(func() { m.Put(1, 1) })
	mustReadOnly(func() { m.PutIfAbsent(1, 1) })
	mustReadOnly(func() { m.Remove(0) })
	mustReadOnly(func() { m.RemoveIfEquals(0, 0) })
	mustReadOnly(func() { m.Replace(0, 1) })
	mustReadOnly(func() { m.ReplaceIfEquals(0, 0, 1) })
	mustReadOnly(func() { m.Clear() })
	mustReadOnly(func() { m.PollFirstEntry() })
	mustReadOnly(func() { m.PollLastEntry() })
}

// TestSortedTableNoMaterialize: on a byte store with binary-capable
// formats, Get resolves pages in place (this is the push-down contract —
// verified indirectly by asserting correctness across page boundaries;
// the format's SupportsBinary path is exercised).
func TestSortedTableBinaryPath(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := build(t, s, 200, 16)
	for i := int64(0); i < 200; i++ {
		if v, ok := m.Get(i * 2); !ok || v != i*10 {
			t.Fatalf("get(%d)=%v,%v", i*2, v, ok)
		}
	}
}

// TestSortedTableFuzzVsOracle: random key set, compare full content.
func TestSortedTableFuzzVsOracle(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	seen := map[int64]bool{}
	var pairs [][2]int64
	for i := 0; i < 3000; i++ {
		k := r.Int63n(100000)
		if seen[k] {
			continue
		}
		seen[k] = true
		pairs = append(pairs, [2]int64{k, k * 3})
	}
	slices.SortFunc(pairs, func(a, b [2]int64) int { return int(a[0] - b[0]) })
	m, err := CreateFromSortedN[int64, int64](store.NewStoreByteArray(true),
		ser.LongFormat, ser.LongFormat, 32, sortedSeq(pairs))
	if err != nil {
		t.Fatal(err)
	}
	if m.LenLong() != int64(len(pairs)) {
		t.Fatalf("len=%d", m.LenLong())
	}
	i := 0
	for k, v := range m.All() {
		if k != pairs[i][0] || v != pairs[i][1] {
			t.Fatalf("entry %d = %d,%d want %d,%d", i, k, v, pairs[i][0], pairs[i][1])
		}
		i++
	}
	if i != len(pairs) {
		t.Fatalf("iterated %d", i)
	}
	// point lookups
	for _, p := range pairs[:100] {
		if v, ok := m.Get(p[0]); !ok || v != p[1] {
			t.Fatalf("get(%d)", p[0])
		}
	}
}

// TestSortedTableReopen: committed table survives close/reopen.
func TestSortedTableReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	m := build(t, s, 500, 10)
	hdr := m.HeaderRecid()
	s.Close()

	s2, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := OpenSortedTableMap[int64, int64](s2, hdr, ser.LongFormat, ser.LongFormat)
	if err != nil {
		t.Fatal(err)
	}
	if m2.LenLong() != 500 {
		t.Fatalf("len=%d", m2.LenLong())
	}
	for i := int64(0); i < 500; i++ {
		if v, ok := m2.Get(i * 2); !ok || v != i*10 {
			t.Fatalf("get(%d)", i*2)
		}
	}
	// wrong-magic recid rejected
	_, err = OpenSortedTableMap[int64, int64](s2, hdr+1000, ser.LongFormat, ser.LongFormat)
	if err == nil {
		t.Fatal("bogus recid opened")
	}
}
