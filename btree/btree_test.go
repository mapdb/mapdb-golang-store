package btree

import (
	"errors"
	"iter"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mapdb/mapdb-golang/object"
	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// BTreeMap must satisfy the sibling collections MutableMap contract.
var _ object.MutableMap[int64, int64] = (*BTreeMap[int64, int64])(nil)

// ---------- helpers ----------

func cfgI64() Config[int64, int64] {
	return Config[int64, int64]{
		KeyFormat:   ser.LongFormat,
		ValueFormat: ser.LongFormat,
		MaxNodeSize: 32,
	}
}

func newHeap() store.Store { return store.NewStoreOnHeap(true) }
func newBA() store.Store   { return store.NewStoreByteArray(true) }

func stores(t *testing.T) map[string]store.Store {
	return map[string]store.Store{
		"heap":      newHeap(),
		"bytearray": newBA(),
	}
}

// gv returns a getter that fatals on miss — avoids the "multi-value call as
// argument" restriction (f(t, m.Get(k)) is not legal Go).
func gv[K comparable, V any](t *testing.T, f func(K) (V, bool), k K) V {
	t.Helper()
	v, ok := f(k)
	if !ok {
		t.Fatalf("expected entry for %v", k)
	}
	return v
}

func collectAll[K comparable, V any](m *BTreeMap[K, V]) []K {
	var keys []K
	for k := range m.All() {
		keys = append(keys, k)
	}
	return keys
}

// ---------- basic ----------

func TestBTreeEmpty(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			m := NewBTreeMap[int64, int64](s, cfgI64())
			if !m.IsEmpty() {
				t.Fatal("not empty")
			}
			if _, ok := m.Get(7); ok {
				t.Fatal("get on empty returned entry")
			}
			if _, _, ok := m.Min(); ok {
				t.Fatal("min on empty")
			}
			if _, _, ok := m.Max(); ok {
				t.Fatal("max on empty")
			}
			if _, ok := m.Remove(7); ok {
				t.Fatal("remove on empty")
			}
			if m.Len() != 0 {
				t.Fatal("len != 0")
			}
			m.Verify()
		})
	}
}

func TestBTreePutGetRemove(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			m := NewBTreeMap[int64, int64](s, cfgI64())
			if _, ok := m.Put(5, 50); ok {
				t.Fatal("put returned prior")
			}
			if v := gv(t, m.Get, 5); v != 50 {
				t.Fatalf("got %d", v)
			}
			old, ok := m.Put(5, 55)
			if !ok || old != 50 {
				t.Fatalf("old %d", old)
			}
			if !m.ContainsKey(5) {
				t.Fatal("missing key")
			}
			old, ok = m.Remove(5)
			if !ok || old != 55 {
				t.Fatalf("removed %d", old)
			}
			if _, ok := m.Get(5); ok {
				t.Fatal("removed key still present")
			}
			m.Verify()
		})
	}
}

// ---------- splits / structure ----------

// fillSorted inserts keys 0..n-1 with value = key*10.
func fillSorted[K interface{ ~int64 }](m *BTreeMap[int64, int64], n int64) {
	for i := int64(0); i < n; i++ {
		m.Put(i, i*10)
	}
}

func TestBTreeSplitsAndIteration(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			cfg := cfgI64()
			cfg.MaxNodeSize = 8
			m := NewBTreeMap[int64, int64](s, cfg)
			const n = 500
			for i := int64(0); i < n; i++ {
				m.Put(i, i*10)
			}
			// verify full ascending order
			keys := collectAll(m)
			if len(keys) != n {
				t.Fatalf("iterated %d keys, want %d", len(keys), n)
			}
			for i := int64(0); i < n; i++ {
				if keys[i] != i {
					t.Fatalf("key %d at pos %d", keys[i], i)
				}
			}
			// spot check gets
			for _, i := range []int64{0, 7, 128, 499} {
				if v := gv(t, m.Get, i); v != i*10 {
					t.Fatalf("get(%d)=%d", i, v)
				}
			}
			m.Verify()
			if m.Len() != n {
				t.Fatalf("len %d", m.Len())
			}
		})
	}
}

func TestBTreeRandomInsertDelete(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			cfg := cfgI64()
			cfg.MaxNodeSize = 8
			m := NewBTreeMap[int64, int64](s, cfg)
			ref := make(map[int64]int64)
			r := rand.New(rand.NewSource(42))
			// random inserts
			for i := 0; i < 2000; i++ {
				k := int64(r.Intn(2000))
				v := int64(i)
				m.Put(k, v)
				ref[k] = v
			}
			// random deletes
			for i := 0; i < 1000; i++ {
				k := int64(r.Intn(2000))
				old, ok := m.Remove(k)
				exp, eok := ref[k]
				if ok != eok || (ok && old != exp) {
					t.Fatalf("remove(%d): got (%d,%v) want (%d,%v)", k, old, ok, exp, eok)
				}
				delete(ref, k)
			}
			// check all survivors
			var want []int64
			for k := range ref {
				want = append(want, k)
			}
			sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
			got := collectAll(m)
			if !slices.Equal(got, want) {
				t.Fatalf("iterated %d keys, want %d", len(got), len(want))
			}
			for k, v := range ref {
				if got := gv(t, m.Get, k); got != v {
					t.Fatalf("get(%d)=%d want %d", k, got, v)
				}
			}
			m.Verify()
		})
	}
}

// ---------- navigation ----------

func TestBTreeNavigation(t *testing.T) {
	s := newHeap()
	cfg := cfgI64()
	cfg.MaxNodeSize = 8
	m := NewBTreeMap[int64, int64](s, cfg)
	for i := int64(0); i < 200; i += 2 {
		m.Put(i, i)
	}
	// floor/ceiling/lower/higher
	if k, _, ok := m.Floor(5); !ok || k != 4 {
		t.Fatalf("floor(5)=%v,%v", k, ok)
	}
	if k, _, ok := m.Floor(4); !ok || k != 4 {
		t.Fatalf("floor(4)=%v,%v", k, ok)
	}
	if k, _, ok := m.Ceiling(5); !ok || k != 6 {
		t.Fatalf("ceiling(5)=%v,%v", k, ok)
	}
	if k, _, ok := m.Lower(4); !ok || k != 2 {
		t.Fatalf("lower(4)=%v,%v", k, ok)
	}
	if k, _, ok := m.Higher(4); !ok || k != 6 {
		t.Fatalf("higher(4)=%v,%v", k, ok)
	}
	if _, _, ok := m.Floor(-1); ok {
		t.Fatal("floor below min")
	}
	if _, _, ok := m.Ceiling(199); ok {
		t.Fatal("ceiling above max")
	}
	// min/max
	if k, v, ok := m.Min(); !ok || k != 0 || v != 0 {
		t.Fatalf("min=%v,%v,%v", k, v, ok)
	}
	if k, v, ok := m.Max(); !ok || k != 198 || v != 198 {
		t.Fatalf("max=%v,%v,%v", k, v, ok)
	}
	// rank/select
	if r := m.Rank(10); r != 5 {
		t.Fatalf("rank(10)=%d", r)
	}
	if k, ok := m.SelectKey(7); !ok || k != 14 {
		t.Fatalf("selectKey(7)=%v,%v", k, ok)
	}
	if k, v, ok := m.SelectEntry(50); !ok || k != 100 || v != 100 {
		t.Fatalf("selectEntry(50)=%v,%v,%v", k, v, ok)
	}
	// submap iteration [10,20) → 10,12,14,16,18
	var got []int64
	for k := range m.SubMap(10, 20) {
		got = append(got, k)
	}
	if !slices.Equal(got, []int64{10, 12, 14, 16, 18}) {
		t.Fatalf("submap=%v", got)
	}
	got = got[:0]
	for k := range m.HeadMap(6) {
		got = append(got, k)
	}
	if !slices.Equal(got, []int64{0, 2, 4}) {
		t.Fatalf("headmap=%v", got)
	}
	got = got[:0]
	for k := range m.TailMap(194) {
		got = append(got, k)
	}
	if !slices.Equal(got, []int64{194, 196, 198}) {
		t.Fatalf("tailmap=%v", got)
	}
	// descending
	got = got[:0]
	for k := range m.DescendingMap() {
		got = append(got, k)
		if len(got) > 5 {
			break
		}
	}
	if !slices.Equal(got, []int64{198, 196, 194, 192, 190, 188}) {
		t.Fatalf("descending=%v", got)
	}
}

func TestBTreeFullDescending(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			cfg := cfgI64()
			cfg.MaxNodeSize = 8
			m := NewBTreeMap[int64, int64](s, cfg)
			const n = 300
			for i := int64(0); i < n; i++ {
				m.Put(i, i)
			}
			var got []int64
			for k := range m.DescendingMap() {
				got = append(got, k)
			}
			if len(got) != n {
				t.Fatalf("desc len %d", len(got))
			}
			for i, k := range got {
				if k != int64(n-1-i) {
					t.Fatalf("desc[%d]=%d", i, k)
				}
			}
		})
	}
}

// ---------- conditional ops / poll ----------

func TestBTreeConditionalOps(t *testing.T) {
	s := newHeap()
	m := NewBTreeMap[int64, int64](s, cfgI64())
	m.Put(1, 10)
	if _, ok := m.PutIfAbsent(1, 99); !ok {
		t.Fatal("putIfAbsent ignored existing")
	}
	if _, ok := m.PutIfAbsent(2, 20); ok {
		t.Fatal("putIfAbsent inserted wrongly reported")
	}
	if !m.ReplaceIfEquals(1, 10, 11) {
		t.Fatal("replaceIf failed")
	}
	if m.ReplaceIfEquals(1, 10, 12) {
		t.Fatal("replaceIf on wrong old")
	}
	if _, ok := m.Replace(9, 9); ok {
		t.Fatal("replace on absent")
	}
	if !m.RemoveIfEquals(1, 11) {
		t.Fatal("removeIfEquals failed")
	}
	if _, ok := m.Get(1); ok {
		t.Fatal("key still present")
	}
}

func TestBTreePoll(t *testing.T) {
	s := newHeap()
	cfg := cfgI64()
	cfg.MaxNodeSize = 8
	m := NewBTreeMap[int64, int64](s, cfg)
	for i := int64(0); i < 50; i++ {
		m.Put(i, i)
	}
	if k, _, ok := m.PollFirstEntry(); !ok || k != 0 {
		t.Fatalf("pollFirst=%v,%v", k, ok)
	}
	if k, _, ok := m.PollLastEntry(); !ok || k != 49 {
		t.Fatalf("pollLast=%v,%v", k, ok)
	}
	if m.Len() != 48 {
		t.Fatalf("len %d", m.Len())
	}
	for m.Len() > 0 {
		m.PollFirstEntry()
	}
	if _, _, ok := m.PollFirstEntry(); ok {
		t.Fatal("poll on empty")
	}
}

// ---------- counter / external values ----------

func TestBTreeCounter(t *testing.T) {
	s := newHeap()
	cfg := cfgI64()
	cfg.MaxNodeSize = 8
	cfg.Counter = true
	m := NewBTreeMap[int64, int64](s, cfg)
	if m.CounterRecid() == 0 {
		t.Fatal("no counter")
	}
	for i := int64(0); i < 100; i++ {
		m.Put(i, i)
	}
	if m.LenLong() != 100 {
		t.Fatalf("lenLong %d", m.LenLong())
	}
	for i := int64(0); i < 40; i++ {
		m.Remove(i)
	}
	if m.LenLong() != 60 {
		t.Fatalf("lenLong %d", m.LenLong())
	}
	m.Verify()
}

func TestBTreeExternalValues(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			cfg := cfgI64()
			cfg.MaxNodeSize = 8
			cfg.ExternalValues = true
			m := NewBTreeMap[int64, int64](s, cfg)
			const n = 200
			for i := int64(0); i < n; i++ {
				m.Put(i, i*10)
			}
			for i := int64(0); i < n; i++ {
				if v := gv(t, m.Get, i); v != i*10 {
					t.Fatalf("get(%d)=%d", i, v)
				}
			}
			// iterate
			var cnt int
			for k, v := range m.All() {
				if v != k*10 {
					t.Fatalf("iter k=%d v=%d", k, v)
				}
				cnt++
			}
			if cnt != n {
				t.Fatalf("iterated %d", cnt)
			}
			// descending
			cnt = 0
			for range m.DescendingMap() {
				cnt++
			}
			if cnt != n {
				t.Fatalf("desc iterated %d", cnt)
			}
			// remove half
			for i := int64(0); i < n; i += 2 {
				m.Remove(i)
			}
			if m.Len() != n/2 {
				t.Fatalf("len %d", m.Len())
			}
			m.Verify()
		})
	}
}

// ---------- persistence ----------

func TestBTreeReopenDirect(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := cfgI64()
	cfg.MaxNodeSize = 8
	cfg.Counter = true
	m := NewBTreeMap[int64, int64](s, cfg)
	for i := int64(0); i < 300; i++ {
		m.Put(i, i*10)
	}
	rrr := m.RootRecidRecid()
	cr := m.CounterRecid()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	cfg2 := cfg
	cfg2.Counter = false
	cfg2.CounterRecid = cr
	m2, err := OpenBTreeMap[int64, int64](s2, rrr, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	if m2.LenLong() != 300 {
		t.Fatalf("reopened len %d", m2.LenLong())
	}
	for i := int64(0); i < 300; i++ {
		if v := gv(t, m2.Get, i); v != i*10 {
			t.Fatalf("get(%d)=%d", i, v)
		}
	}
	m2.Put(1000, 5)
	if v := gv(t, m2.Get, 1000); v != 5 {
		t.Fatal("post-reopen put")
	}
	m2.Verify()
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---------- WAL / transactional ----------

func TestBTreeWALCommitRollback(t *testing.T) {
	s, err := store.OpenStoreWAL(t.TempDir()+"/db", true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := cfgI64()
	cfg.MaxNodeSize = 8
	cfg.Counter = true
	m := NewBTreeMap[int64, int64](s, cfg)
	for i := int64(0); i < 100; i++ {
		m.Put(i, i*10)
	}
	s.Commit()
	// stage uncommitted writes, then roll back
	for i := int64(100); i < 200; i++ {
		m.Put(i, i*10)
	}
	s.Rollback()
	// only committed entries survive
	for i := int64(0); i < 100; i++ {
		if v := gv(t, m.Get, i); v != i*10 {
			t.Fatalf("get(%d)=%d", i, v)
		}
	}
	for i := int64(100); i < 200; i++ {
		if _, ok := m.Get(i); ok {
			t.Fatalf("rolled-back key %d still present", i)
		}
	}
	if m.LenLong() != 100 {
		t.Fatalf("lenLong %d after rollback", m.LenLong())
	}
	m.Verify()
	// regrow after rollback — structural caches must refresh
	for i := int64(100); i < 300; i++ {
		m.Put(i, i)
	}
	s.Commit()
	if m.Len() != 300 {
		t.Fatalf("len %d after regrow", m.Len())
	}
	m.Verify()
	s.Close()
}

func TestBTreeWALReopen(t *testing.T) {
	base := t.TempDir() + "/db"
	s, err := store.OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := cfgI64()
	cfg.MaxNodeSize = 8
	m := NewBTreeMap[int64, int64](s, cfg)
	for i := int64(0); i < 200; i++ {
		m.Put(i, i*10)
	}
	rrr := m.RootRecidRecid()
	s.Commit()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := OpenBTreeMap[int64, int64](s2, rrr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 200; i++ {
		if v := gv(t, m2.Get, i); v != i*10 {
			t.Fatalf("get(%d)=%d", i, v)
		}
	}
	m2.Verify()
	s2.Close()
}

// ---------- bulk build ----------

func TestBTreeCreateFromSorted(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			cfg := cfgI64()
			cfg.MaxNodeSize = 8
			const n = 500
			m, err := CreateFromSorted[int64, int64](s, cfg, func(yield func(int64, int64) bool) {
				for i := int64(0); i < n; i++ {
					if !yield(i, i*10) {
						return
					}
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			keys := collectAll(m)
			if len(keys) != n {
				t.Fatalf("iterated %d", len(keys))
			}
			for i := int64(0); i < n; i++ {
				if keys[i] != i {
					t.Fatalf("key %d at %d", keys[i], i)
				}
			}
			// splits still work on the pumped tree
			m.Put(n+1, 1)
			if v := gv(t, m.Get, n+1); v != 1 {
				t.Fatal("post-pump put")
			}
			m.Verify()
		})
	}
}

func TestBTreeCreateFromSortedNotSorted(t *testing.T) {
	s := newHeap()
	_, err := CreateFromSorted[int64, int64](s, cfgI64(), func(yield func(int64, int64) bool) {
		if !yield(1, 1) {
			return
		}
		if !yield(0, 0) { // out of order
			return
		}
	})
	if !errors.Is(err, pump.ErrNotSorted) {
		t.Fatalf("err=%v", err)
	}
}

func TestBTreeCreateFromSortedEmpty(t *testing.T) {
	s := newHeap()
	m, err := CreateFromSorted[int64, int64](s, cfgI64(), func(yield func(int64, int64) bool) {
		// nothing
	})
	if err != nil {
		t.Fatal(err)
	}
	if !m.IsEmpty() {
		t.Fatal("not empty")
	}
	m.Verify()
}

// ---------- lease ----------

func TestBTreeLeaseConflict(t *testing.T) {
	s := newHeap()
	m := NewBTreeMap[int64, int64](s, cfgI64())
	rrr := m.RootRecidRecid()
	if _, err := OpenBTreeMap[int64, int64](s, rrr, cfgI64()); err == nil {
		t.Fatal("second open succeeded while first live")
	}
	m.Close()
	if _, err := OpenBTreeMap[int64, int64](s, rrr, cfgI64()); err != nil {
		t.Fatalf("reopen after close failed: %v", err)
	}
}

func TestBTreeReadOnlyWrapper(t *testing.T) {
	s := newHeap()
	m := NewBTreeMap[int64, int64](s, cfgI64())
	m.Put(1, 10)
	rrr := m.RootRecidRecid()
	m.Close()
	ro := store.NewReadOnly(s)
	m2, err := OpenBTreeMap[int64, int64](ro, rrr, cfgI64())
	if err != nil {
		t.Fatal(err)
	}
	if v := gv(t, m2.Get, 1); v != 10 {
		t.Fatal("read via RO wrapper")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("mutating put on RO wrapper did not panic")
		} else if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindReadOnly {
			t.Fatalf("wrong panic: %v", r)
		}
	}()
	m2.Put(2, 2)
}

// ---------- corruption ----------

func TestBTreeCorruptRootPointer(t *testing.T) {
	s := newHeap()
	m := NewBTreeMap[int64, int64](s, cfgI64())
	rrr := m.RootRecidRecid()
	m.Close()
	// poison the root pointer
	store.Update(s, rrr, int64(0), ser.Long)
	_, err := OpenBTreeMap[int64, int64](s, rrr, cfgI64())
	if err == nil {
		t.Fatal("opened map with zero root pointer")
	}
	if de, ok := mapdb5.AsDBError(err); !ok || de.Kind != mapdb5.KindDataCorruption {
		t.Fatalf("err=%v", err)
	}
}

func TestBTreeNodeWireFormat(t *testing.T) {
	// Golden check on the wire format: hand-decode a leaf node's bytes.
	s := newHeap()
	m := NewBTreeMap[int64, int64](s, cfgI64())
	m.Put(1, 100)
	m.Put(2, 200)
	// byte-store round trip through serialization
	ns := newNodeSerializer[int64, int64](ser.LongFormat, ser.LongFormat, 32, true)
	n := node{
		flags:  flagLeft,
		link:   42,
		keys:   ser.LongFormat.FromSlice([]int64{1, 2}),
		values: ser.LongFormat.FromSlice([]int64{100, 200}),
		fence:  ser.LongFormat.FromSlice([]int64{2}),
	}
	b := ser.SerializeToBytes(ns, n)
	r := bin.NewReader(b)
	h := r.UnpackInt()
	if h&0xF != flagLeft {
		t.Fatalf("flags %x", h&0xF)
	}
	if int(uint32(h)>>4) != 2 {
		t.Fatalf("keysLen %d", uint32(h)>>4)
	}
	if link := r.UnpackLong(); link != 42 {
		t.Fatalf("link %d", link)
	}
	d := ns.Deserialize(bin.NewReader(b), len(b))
	if d.link != 42 || d.fence == nil {
		t.Fatalf("decoded %+v", d)
	}
	if got := ser.LongFormat.Size(d.keys); got != 2 {
		t.Fatalf("decoded keys %d", got)
	}
}

// ---------- concurrency ----------

func TestBTreeConcurrentWriters(t *testing.T) {
	s := newHeap()
	cfg := cfgI64()
	cfg.MaxNodeSize = 32
	m := NewBTreeMap[int64, int64](s, cfg)
	const workers = 4
	const per = 500
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := int64(w * per)
			for i := int64(0); i < per; i++ {
				m.Put(base+i, base+i)
			}
		}(w)
	}
	wg.Wait()
	if m.Len() != workers*per {
		t.Fatalf("len %d", m.Len())
	}
	// concurrent reads + writes
	var wg2 sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg2.Add(1)
		go func(w int) {
			defer wg2.Done()
			for i := int64(0); i < 200; i++ {
				m.Get(int64(i))
				m.Put(int64(2000+w*200)+i, i)
			}
		}(w)
	}
	wg2.Wait()
	m.Verify()
}

// Overlapping key ranges: all workers race inserts into the same sparse
// space, forcing concurrent splits of the same leaves/dirs. The final
// key set must be exactly the union and Verify must hold.
func TestBTreeConcurrentOverlappingInserts(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			cfg := cfgI64()
			cfg.MaxNodeSize = 8
			m := NewBTreeMap[int64, int64](s, cfg)
			const workers = 4
			const space = 800
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(w + 1)))
					for i := 0; i < 1500; i++ {
						k := int64(r.Intn(space))
						m.Put(k, k)
					}
				}(w)
			}
			wg.Wait()
			// iteration must be strictly ascending and count must match len
			var last int64 = -1
			var cnt int
			for k := range m.All() {
				if k <= last {
					t.Fatalf("iteration out of order: %d after %d", k, last)
				}
				last = k
				cnt++
			}
			if cnt != m.Len() {
				t.Fatalf("iterated %d, len %d", cnt, m.Len())
			}
			m.Verify()
		})
	}
}

// Mixed concurrent writes + iteration: iterators must never panic or see
// corrupt state while splits run (weak consistency allowed).
func TestBTreeConcurrentIterAndWrite(t *testing.T) {
	s := newHeap()
	cfg := cfgI64()
	cfg.MaxNodeSize = 8
	m := NewBTreeMap[int64, int64](s, cfg)
	for i := int64(0); i < 100; i++ {
		m.Put(i, i)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(100); i < 20000; i++ {
			select {
			case <-stop:
				return
			default:
			}
			m.Put(i, i)
		}
	}()
	for i := 0; i < 40; i++ {
		var last int64 = -1
		for k := range m.All() {
			if k <= last {
				t.Fatalf("iterator saw out-of-order %d after %d", k, last)
			}
			last = k
		}
		var dl int64 = -1
		first := true
		for k := range m.DescendingMap() {
			if !first && k >= dl {
				t.Fatalf("desc iterator out of order %d after %d", k, dl)
			}
			first = false
			dl = k
		}
	}
	close(stop)
	wg.Wait()
	m.Verify()
}

// ---------- corruption injection (byte path) ----------

// writeNode serializes n directly into recid (bypasses map locks).
func writeNode[K, V any](t *testing.T, s store.Store, recid store.Recid, n node, kf ser.GroupFormat[K], vf ser.GroupFormat[V], inline bool) {
	t.Helper()
	ns := newNodeSerializer(kf, vf, 8, inline)
	store.Update(s, recid, n, ns)
}

// A crafted checksum-valid non-right dir with 0 keys (0 children) on a
// routed path must be DataCorruption from Get, not a silent traversal —
// the on-bytes push-down must match deserialize's structural check.
func TestBTreeBytePathRejectsEmptyNonRightDir(t *testing.T) {
	s := newBA() // byte store exercises the OnBytes path
	leafRecid := s.Preallocate()
	badDirRecid := s.Preallocate()
	rootRecid := s.Preallocate()

	// leftmost leaf (non-right, link into the bad dir's position) with fence
	writeNode(t, s, leafRecid, node{
		flags:  flagLeft,
		link:   badDirRecid,
		keys:   ser.LongFormat.FromSlice([]int64{1}),
		values: ser.LongFormat.FromSlice([]int64{10}),
		fence:  ser.LongFormat.FromSlice([]int64{50}),
	}, ser.LongFormat, ser.LongFormat, true)
	// crafted empty non-right dir
	writeNode(t, s, badDirRecid, node{
		flags:    flagDir,
		link:     leafRecid,
		keys:     ser.LongFormat.Empty(),
		children: nil,
	}, ser.LongFormat, ser.LongFormat, true)
	// root dir routes keys > 50 to the bad dir
	writeNode(t, s, rootRecid, node{
		flags:    flagDir | flagLeft | flagRight,
		keys:     ser.LongFormat.FromSlice([]int64{50}),
		children: []uint64{leafRecid, badDirRecid},
	}, ser.LongFormat, ser.LongFormat, true)
	rrr := store.Put(s, int64(rootRecid), ser.Long)

	m, err := OpenBTreeMap[int64, int64](s, rrr, cfgI64())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("get(100) on crafted dir did not panic")
		}
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Fatalf("wrong panic: %v", r)
		}
	}()
	m.Get(100)
}

// A leaf-link cycle must terminate via the cycle guard, not hang.
func TestBTreeLeafLinkCycleDetected(t *testing.T) {
	s := newBA()
	l1 := s.Preallocate()
	l2 := s.Preallocate()
	writeNode(t, s, l1, node{
		flags:  flagLeft,
		link:   l2,
		keys:   ser.LongFormat.FromSlice([]int64{1}),
		values: ser.LongFormat.FromSlice([]int64{10}),
		fence:  ser.LongFormat.FromSlice([]int64{5}),
	}, ser.LongFormat, ser.LongFormat, true)
	// l2 points back to l1: following leaf links cycles l1→l2→l1 forever
	// unless the cycle guard intervenes.
	writeNode(t, s, l2, node{
		link:   l1,
		keys:   ser.LongFormat.FromSlice([]int64{10}),
		values: ser.LongFormat.FromSlice([]int64{100}),
		fence:  ser.LongFormat.FromSlice([]int64{50}),
	}, ser.LongFormat, ser.LongFormat, true)
	rootRecid := s.Preallocate()
	writeNode(t, s, rootRecid, node{
		flags:    flagDir | flagLeft | flagRight,
		keys:     ser.LongFormat.FromSlice([]int64{50}),
		children: []uint64{l1, l2},
	}, ser.LongFormat, ser.LongFormat, true)
	rrr := store.Put(s, int64(rootRecid), ser.Long)
	m, err := OpenBTreeMap[int64, int64](s, rrr, cfgI64())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("iteration over a leaf cycle did not panic")
		}
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Fatalf("wrong panic: %v", r)
		}
	}()
	for range m.All() {
	}
}

// ---------- lock table unit checks ----------

func TestNodeLockTableMutualExclusion(t *testing.T) {
	lt := newNodeLockTable(true)
	lt.lock(7)
	done := make(chan struct{})
	go func() {
		lt.lock(7)
		lt.unlock(7)
		close(done)
	}()
	// the contender must not acquire while we hold
	select {
	case <-done:
		t.Fatal("lock acquired while held")
	case <-time.After(50 * time.Millisecond):
	}
	lt.unlock(7)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken on release")
	}
}

func TestNodeLockTableReentrantPanics(t *testing.T) {
	lt := newNodeLockTable(true)
	lt.lock(9)
	defer lt.unlock(9)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("reentrant lock did not panic")
		}
	}()
	lt.lock(9)
}

var _ = sort.Ints // sort used in older helpers; keep stable import set

// ===================== r2 review: iterator lifetime =====================

// R2 finding 2: a lazy iterator resumed after handle retirement must
// panic — it cannot keep emitting buffered entries or following leaf
// links on a closed tree. Covers ascending inline (leaf-link walk),
// descending (buffered batches + retained stack) and external-value
// variants.
func TestIteratorResumeAfterClose(t *testing.T) {
	fill := func(m *BTreeMap[int64, int64]) {
		for i := int64(0); i < 20; i++ {
			m.Put(i, i*10)
		}
	}
	resumeMustPanic := func(t *testing.T, m *BTreeMap[int64, int64], seq iter.Seq2[int64, int64], firstK int64) {
		t.Helper()
		next, stop := iter.Pull2(seq)
		defer stop()
		k, _, ok := next()
		if !ok || k != firstK {
			t.Fatalf("first pull = %v,%v want %v,true", k, ok, firstK)
		}
		m.Close()
		defer func() {
			r := recover()
			de, ok := mapdb5.AsDBError(r)
			if !ok || de.Kind != mapdb5.KindClosed {
				t.Fatalf("resume after Close: panic = %v, want KindClosed", r)
			}
		}()
		next()
	}
	t.Run("ascending-inline", func(t *testing.T) {
		m := newLMap(newHeap(), 4, true)
		fill(m)
		resumeMustPanic(t, m, m.All(), 0)
	})
	t.Run("descending-inline", func(t *testing.T) {
		m := newLMap(newHeap(), 4, true)
		fill(m)
		resumeMustPanic(t, m, m.DescendingMap(), 19)
	})
	cfgExt := func() Config[int64, int64] {
		c := cfgI64()
		c.MaxNodeSize = 4
		c.ExternalValues = true
		return c
	}
	t.Run("ascending-external", func(t *testing.T) {
		m := NewBTreeMap[int64, int64](newHeap(), cfgExt())
		fill(m)
		resumeMustPanic(t, m, m.All(), 0)
	})
	t.Run("descending-external", func(t *testing.T) {
		m := NewBTreeMap[int64, int64](newHeap(), cfgExt())
		fill(m)
		resumeMustPanic(t, m, m.DescendingMap(), 19)
	})
}

// R2 finding 1: Close drains ops admitted before it — a Put paused inside
// its sync listener keeps the admission held, so Close must block until
// the Put returns, and the write itself lands cleanly (admitted ops
// finish; only NEW ops fail).
func TestCloseDrainsInFlightOp(t *testing.T) {
	m := newLMap(newHeap(), 32, true)
	m.Put(1, 1)

	entered := make(chan struct{})
	release := make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error {
			if armed.Swap(false) {
				close(entered)
				<-release
			}
			return nil
		}))

	putDone := make(chan struct{})
	go func() { defer close(putDone); m.Put(1, 99) }()
	<-entered

	closeDone := make(chan struct{})
	go func() { defer close(closeDone); _ = m.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close returned while an admitted op was still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-putDone
	<-closeDone

	// The admitted write completed; the handle is closed for new ops.
	defer func() {
		r := recover()
		if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindClosed {
			t.Fatalf("Get after Close: panic = %v, want KindClosed", r)
		}
	}()
	m.Get(1)
}
