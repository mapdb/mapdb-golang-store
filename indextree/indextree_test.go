package indextree

import (
	"math/rand"
	"slices"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func llstores(t *testing.T) map[string]store.Store {
	return map[string]store.Store{
		"heap":      store.NewStoreOnHeap(true),
		"bytearray": store.NewStoreByteArray(true),
	}
}

func mustPanicMisuse(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("panic = %v, want KindMisuse", r)
		}
	}()
	f()
}

// TestLongLongBasicOps: get/put/contains/remove + default value.
func TestLongLongBasicOps(t *testing.T) {
	for name, s := range llstores(t) {
		t.Run(name, func(t *testing.T) {
			m := NewIndexTreeLongLongMapWithDefault(s, -1)
			if m.Get(5) != -1 {
				t.Fatalf("absent get=%d", m.Get(5))
			}
			m.Put(5, 55)
			if m.Get(5) != 55 || !m.ContainsKey(5) {
				t.Fatalf("get=%d contains=%v", m.Get(5), m.ContainsKey(5))
			}
			m.Put(5, 0) // zero is a legitimate value — the recid holds presence
			if m.Get(5) != 0 || !m.ContainsKey(5) {
				t.Fatalf("zero-value get=%d", m.Get(5))
			}
			m.Put(5, -77) // negatives too
			if m.Get(5) != -77 {
				t.Fatalf("negative get=%d", m.Get(5))
			}
			if m.GetOrDefault(999, 42) != 42 || m.GetOrDefault(5, 42) != -77 {
				t.Fatal("getOrDefault")
			}
			if !m.Remove(5) || m.Remove(5) || m.ContainsKey(5) {
				t.Fatal("remove semantics")
			}
			if m.Size() != 0 || !m.IsEmpty() {
				t.Fatal("size/isEmpty")
			}
		})
	}
}

// TestLongLongPutIfAbsentAddTo: conditional put and counter-style add.
func TestLongLongPutIfAbsentAddTo(t *testing.T) {
	m := NewIndexTreeLongLongMap(store.NewStoreOnHeap(true))
	if v := m.PutIfAbsent(7, 70); v != 70 {
		t.Fatalf("putIfAbsent fresh=%d", v)
	}
	if v := m.PutIfAbsent(7, 700); v != 70 {
		t.Fatalf("putIfAbsent existing=%d", v)
	}
	if v := m.AddTo(100, 5); v != 5 { // defaultValue(0) + 5
		t.Fatalf("addTo fresh=%d", v)
	}
	if v := m.AddTo(100, 3); v != 8 {
		t.Fatalf("addTo=%d", v)
	}
	if v := m.AddTo(7, -70); v != 0 {
		t.Fatalf("addTo negative=%d", v)
	}
	if m.Size() != 2 {
		t.Fatalf("size=%d", m.Size())
	}
}

// TestLongLongKeyDomain: keys outside [0, MaxKey] panic Misuse on
// get/put/remove and are absent for containsKey/getOrDefault.
func TestLongLongKeyDomain(t *testing.T) {
	m := NewIndexTreeLongLongMap(store.NewStoreOnHeap(true))
	mustPanicMisuse(t, func() { m.Put(-1, 1) })
	mustPanicMisuse(t, func() { m.Put(MaxKey+1, 1) })
	mustPanicMisuse(t, func() { m.Get(-1) })
	mustPanicMisuse(t, func() { m.Remove(MaxKey + 1) })
	if m.ContainsKey(-1) || m.ContainsKey(MaxKey+1) {
		t.Fatal("out-of-range containsKey")
	}
	if m.GetOrDefault(-5, 9) != 9 {
		t.Fatal("out-of-range getOrDefault")
	}
	// boundary keys work
	m.Put(0, 1)
	m.Put(MaxKey, 2)
	if m.Get(0) != 1 || m.Get(MaxKey) != 2 {
		t.Fatal("boundary keys")
	}
}

// TestLongLongForEach: unordered traversal sees every entry.
func TestLongLongForEach(t *testing.T) {
	m := NewIndexTreeLongLongMap(store.NewStoreOnHeap(true))
	want := map[int64]int64{}
	for i := int64(0); i < 500; i += 3 {
		want[i] = i * 7
		m.Put(i, i*7)
	}
	got := map[int64]int64{}
	m.ForEach(func(k, v int64) { got[k] = v })
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("forEach[%d]=%d", k, got[k])
		}
	}
	// iter.Seq2 early exit doesn't leak or panic
	n := 0
	for range m.All() {
		n++
		break
	}
	if n != 1 {
		t.Fatal("early exit")
	}
}

// TestLongLongFuzzVsMap: random ops against a Go map oracle.
func TestLongLongFuzzVsMap(t *testing.T) {
	m := NewIndexTreeLongLongMap(store.NewStoreOnHeap(true))
	oracle := map[int64]int64{}
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 20000; i++ {
		k := r.Int63n(3000)
		switch r.Intn(4) {
		case 0, 1:
			v := r.Int63()
			if r.Intn(3) == 0 {
				v = -v
			}
			m.Put(k, v)
			oracle[k] = v
		case 2:
			d := r.Int63n(100)
			m.AddTo(k, d)
			oracle[k] += d
		case 3:
			om := m.Remove(k)
			_, present := oracle[k]
			if om != present {
				t.Fatalf("remove(%d)=%v oracle=%v", k, om, present)
			}
			delete(oracle, k)
		}
	}
	if m.Size() != int64(len(oracle)) {
		t.Fatalf("size=%d oracle=%d", m.Size(), len(oracle))
	}
	for k, v := range oracle {
		if m.Get(k) != v {
			t.Fatalf("get(%d)=%d want %d", k, m.Get(k), v)
		}
	}
	m.Clear()
	if m.Size() != 0 || !m.IsEmpty() {
		t.Fatal("clear")
	}
}

// TestLongLongReopen: committed content survives close/reopen on a
// direct-file store.
func TestLongLongReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	m := NewIndexTreeLongLongMapWithDefault(s, -9)
	for i := int64(0); i < 1000; i++ {
		m.Put(i, i*11)
	}
	hdr := m.HeaderRecid()
	s.Close()

	s2, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := OpenIndexTreeLongLongMap(s2, hdr)
	if err != nil {
		t.Fatal(err)
	}
	if m2.DefaultValue() != -9 || m2.Size() != 1000 {
		t.Fatalf("default=%d size=%d", m2.DefaultValue(), m2.Size())
	}
	for i := int64(0); i < 1000; i++ {
		if m2.Get(i) != i*11 {
			t.Fatalf("get(%d)=%d", i, m2.Get(i))
		}
	}
}

// ==================== list ====================

func TestListBasicOps(t *testing.T) {
	for name, s := range llstores(t) {
		t.Run(name, func(t *testing.T) {
			l := NewIndexTreeList[int64](s, ser.Long)
			if !l.IsEmpty() || l.Len() != 0 {
				t.Fatal("empty")
			}
			for i := int64(0); i < 100; i++ {
				l.Add(i)
			}
			if l.Len() != 100 {
				t.Fatalf("len=%d", l.Len())
			}
			for i := 0; i < 100; i++ {
				if l.Get(i) != int64(i) {
					t.Fatalf("get(%d)=%d", i, l.Get(i))
				}
			}
			if old := l.Set(50, 500); old != 50 || l.Get(50) != 500 {
				t.Fatalf("set old=%d", old)
			}
			if l.IndexOf(500) != 50 || l.IndexOf(9999) != -1 {
				t.Fatal("indexOf")
			}
			if !l.Contains(500) || l.Contains(9999) {
				t.Fatal("contains")
			}
		})
	}
}

// TestListPositionalShift: AddAt/RemoveAt keep positions contiguous.
func TestListPositionalShift(t *testing.T) {
	l := NewIndexTreeList[int64](store.NewStoreOnHeap(true), ser.Long)
	for i := int64(0); i < 10; i++ {
		l.Add(i)
	}
	l.AddAt(0, 100)  // [100, 0..9]
	l.AddAt(5, 200)  // [100,0,1,2,3,200,4..9]
	l.AddAt(12, 300) // append via positional
	want := []int64{100, 0, 1, 2, 3, 200, 4, 5, 6, 7, 8, 9, 300}
	if got := l.ToSlice(); !slices.Equal(got, want) {
		t.Fatalf("after inserts: %v", got)
	}
	if r := l.RemoveAt(0); r != 100 {
		t.Fatalf("removeAt=%d", r)
	}
	if r := l.RemoveAt(4); r != 200 {
		t.Fatalf("removeAt=%d", r)
	}
	if r := l.RemoveAt(l.Len() - 1); r != 300 {
		t.Fatalf("removeAt last=%d", r)
	}
	want = []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if got := l.ToSlice(); !slices.Equal(got, want) {
		t.Fatalf("after removes: %v", got)
	}
	// bounds
	mustPanicMisuse(t, func() { l.Get(l.Len()) })
	mustPanicMisuse(t, func() { l.AddAt(-1, 0) })
	mustPanicMisuse(t, func() { l.AddAt(l.Len() + 1, 0) })
	mustPanicMisuse(t, func() { l.RemoveAt(l.Len()) })
}

// TestListRemoveFreesRecords: element records are freed on remove/clear —
// no unbounded recid growth on a byte store.
func TestListRemoveFreesRecords(t *testing.T) {
	l := NewIndexTreeList[int64](store.NewStoreByteArray(true), ser.Long)
	for i := int64(0); i < 50; i++ {
		l.Add(i)
	}
	for i := l.Len() - 1; i >= 0; i-- {
		l.RemoveAt(i)
	}
	if l.Len() != 0 || !l.IsEmpty() {
		t.Fatal("empty after removes")
	}
	for i := int64(0); i < 50; i++ {
		l.Add(i * 2)
	}
	l.Clear()
	if l.Len() != 0 || !l.IsEmpty() {
		t.Fatal("clear")
	}
}

// TestListFuzzVsSlice: random ops against a Go slice oracle.
func TestListFuzzVsSlice(t *testing.T) {
	l := NewIndexTreeList[int64](store.NewStoreOnHeap(true), ser.Long)
	var oracle []int64
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 4000; i++ {
		switch r.Intn(5) {
		case 0, 1:
			v := r.Int63n(10000)
			l.Add(v)
			oracle = append(oracle, v)
		case 2:
			if len(oracle) == 0 {
				continue
			}
			idx := r.Intn(len(oracle) + 1)
			v := r.Int63n(10000)
			l.AddAt(idx, v)
			oracle = slices.Insert(oracle, idx, v)
		case 3:
			if len(oracle) == 0 {
				continue
			}
			idx := r.Intn(len(oracle))
			got := l.RemoveAt(idx)
			if got != oracle[idx] {
				t.Fatalf("removeAt(%d)=%d want %d", idx, got, oracle[idx])
			}
			oracle = slices.Delete(oracle, idx, idx+1)
		case 4:
			if len(oracle) == 0 {
				continue
			}
			idx := r.Intn(len(oracle))
			v := r.Int63n(10000)
			old := l.Set(idx, v)
			if old != oracle[idx] {
				t.Fatalf("set(%d) old=%d want %d", idx, old, oracle[idx])
			}
			oracle[idx] = v
		}
	}
	if l.Len() != len(oracle) {
		t.Fatalf("len=%d oracle=%d", l.Len(), len(oracle))
	}
	got := l.ToSlice()
	if !slices.Equal(got, oracle) {
		t.Fatal("content mismatch")
	}
}

// TestListReopen: committed list survives close/reopen.
func TestListReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	l := NewIndexTreeList[int64](s, ser.Long)
	for i := int64(0); i < 200; i++ {
		l.Add(i * i)
	}
	hdr := l.HeaderRecid()
	s.Close()

	s2, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	l2, err := OpenIndexTreeList[int64](s2, hdr, ser.Long)
	if err != nil {
		t.Fatal(err)
	}
	if l2.Len() != 200 {
		t.Fatalf("len=%d", l2.Len())
	}
	for i := 0; i < 200; i++ {
		if l2.Get(i) != int64(i*i) {
			t.Fatalf("get(%d)=%d", i, l2.Get(i))
		}
	}
}
