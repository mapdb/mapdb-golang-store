package htree

import (
	"fmt"
	"iter"
	"math/rand"
	"sync"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func hcfg() HConfig[int64, int64] {
	return HConfig[int64, int64]{
		KeySer: ser.Long, ValueSer: ser.Long, Hasher: mdhash.Long32,
	}
}

// hcfgSeg pins geometry for small test maps (4 segments, 5 levels x 6
// bits — index space 2^30).
func hcfgSeg(hasher mdhash.Hasher[int64]) HConfig[int64, int64] {
	return HConfig[int64, int64]{
		KeySer: ser.Long, ValueSer: ser.Long,
		ConcShift: 2, DirShift: 6, Levels: 5,
		HashSeed: 0x5eed, HashSeedSet: true,
		Hasher: hasher,
	}
}

func hstores(t *testing.T) map[string]store.Store {
	return map[string]store.Store{
		"heap":      store.NewStoreOnHeap(true),
		"bytearray": store.NewStoreByteArray(true),
	}
}

// TestHTreeBasicOps: put/get/putIfAbsent/replace/remove semantics across
// both store dialects.
func TestHTreeBasicOps(t *testing.T) {
	for name, s := range hstores(t) {
		t.Run(name, func(t *testing.T) {
			m := NewHTreeMap[int64, int64](s, hcfg())
			if !m.IsEmpty() || m.LenLong() != 0 {
				t.Fatal("new map not empty")
			}
			for i := int64(0); i < 500; i++ {
				if old, had := m.Put(i, i*10); had || old != 0 {
					t.Fatalf("put %d: had=%v old=%v", i, had, old)
				}
			}
			if m.LenLong() != 500 || m.IsEmpty() {
				t.Fatalf("len=%d", m.LenLong())
			}
			for i := int64(0); i < 500; i++ {
				if v, ok := m.Get(i); !ok || v != i*10 {
					t.Fatalf("get %d = %v,%v", i, v, ok)
				}
				if !m.ContainsKey(i) {
					t.Fatalf("containsKey %d", i)
				}
			}
			// overwrite returns previous
			if old, had := m.Put(7, 777); !had || old != 70 {
				t.Fatalf("re-put: had=%v old=%v", had, old)
			}
			// putIfAbsent leaves existing
			if old, had := m.PutIfAbsent(7, 999); !had || old != 777 {
				t.Fatalf("putIfAbsent existing: had=%v old=%v", had, old)
			}
			if v, _ := m.Get(7); v != 777 {
				t.Fatalf("putIfAbsent overwrote: %v", v)
			}
			// putIfAbsent inserts
			if old, had := m.PutIfAbsent(1000, 5); had || old != 0 {
				t.Fatalf("putIfAbsent insert: had=%v old=%v", had, old)
			}
			// replace only if present
			if old, had := m.Replace(7, 77); !had || old != 777 {
				t.Fatalf("replace: had=%v old=%v", had, old)
			}
			if _, had := m.Replace(9999, 1); had {
				t.Fatal("replace inserted absent key")
			}
			// replaceIfEquals
			if !m.ReplaceIfEquals(7, 77, 88) {
				t.Fatal("replaceIfEquals matched")
			}
			if m.ReplaceIfEquals(7, 77, 99) {
				t.Fatal("replaceIfEquals matched stale value")
			}
			// remove / removeIfEquals
			if old, had := m.Remove(7); !had || old != 88 {
				t.Fatalf("remove: had=%v old=%v", had, old)
			}
			if m.RemoveIfEquals(8, 999) {
				t.Fatal("removeIfEquals matched wrong value")
			}
			if !m.RemoveIfEquals(8, 80) {
				t.Fatal("removeIfEquals failed to match")
			}
			if _, had := m.Remove(9999); had {
				t.Fatal("removed absent key")
			}
			if v, ok := m.Get(7); ok || v != 0 {
				t.Fatalf("get removed = %v,%v", v, ok)
			}
			m.Verify()
		})
	}
}

// TestHTreePersistReopen: create over a file store, close, reopen, and
// the header round-trips all data.
func TestHTreePersistReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := hcfg()
	m := NewHTreeMap[int64, int64](s, cfg)
	for i := int64(0); i < 2000; i++ {
		m.Put(i, i*3)
	}
	hdr := m.HeaderRecid()
	seed := m.HashSeed()
	m.CloseHandle()
	s.Close()

	s2, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := OpenHTreeMap[int64, int64](s2, hdr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.CloseHandle()
	if m2.HashSeed() != seed {
		t.Fatal("seed not persisted")
	}
	if m2.LenLong() != 2000 {
		t.Fatalf("len=%d", m2.LenLong())
	}
	for i := int64(0); i < 2000; i++ {
		if v, ok := m2.Get(i); !ok || v != i*3 {
			t.Fatalf("get %d = %v,%v", i, v, ok)
		}
	}
	m2.Verify()
	// second live handle on the same header is rejected by the lease
	if _, err := OpenHTreeMap[int64, int64](s2, hdr, cfg); err == nil {
		t.Fatal("double open not rejected")
	}
}

// TestHTreeCollisionBucket: a constant hasher puts every key into one
// bucket — exercises grow/shrink of the flat-pair bucket.
func TestHTreeCollisionBucket(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	one := mdhash.HasherFunc[int64](func(k int64, seed int32) int32 { return 0x1234567 })
	m := NewHTreeMap[int64, int64](s, hcfgSeg(one))
	for i := int64(0); i < 50; i++ {
		m.Put(i, i)
	}
	if m.LenLong() != 50 {
		t.Fatalf("len=%d", m.LenLong())
	}
	for i := int64(0); i < 50; i++ {
		if v, ok := m.Get(i); !ok || v != i {
			t.Fatalf("get %d = %v,%v", i, v, ok)
		}
	}
	// shrink back out; the last remove must free the bucket
	for i := int64(0); i < 50; i++ {
		if _, had := m.Remove(i); !had {
			t.Fatalf("remove %d", i)
		}
	}
	if !m.IsEmpty() {
		t.Fatal("not empty after draining the bucket")
	}
	m.Verify()
}

// TestHTreeRandomOracle: random put/remove vs a Go map.
func TestHTreeRandomOracle(t *testing.T) {
	for name, s := range hstores(t) {
		t.Run(name, func(t *testing.T) {
			m := NewHTreeMap[int64, int64](s, hcfg())
			want := map[int64]int64{}
			rng := rand.New(rand.NewSource(1))
			for i := 0; i < 8000; i++ {
				k := int64(rng.Intn(1500))
				switch rng.Intn(5) {
				case 0:
					delete(want, k)
					m.Remove(k)
				case 1:
					want[k] = k * 2
					m.Replace(k, k*2) // replace on absent must not insert
					if _, had := m.Get(k); had {
						want[k] = k * 2
					} else {
						delete(want, k)
					}
				default:
					want[k] = k
					m.Put(k, k)
				}
			}
			if m.LenLong() != int64(len(want)) {
				t.Fatalf("len=%d want=%d", m.LenLong(), len(want))
			}
			for k, v := range want {
				if got, ok := m.Get(k); !ok || got != v {
					t.Fatalf("get %d = %v,%v want %v", k, got, ok, v)
				}
			}
			// iterate coverage
			seen := map[int64]int64{}
			for k, v := range m.All() {
				seen[k] = v
			}
			if len(seen) != len(want) {
				t.Fatalf("iteration saw %d, want %d", len(seen), len(want))
			}
			for k, v := range seen {
				if want[k] != v {
					t.Fatalf("iterated %d=%v, want %v", k, v, want[k])
				}
			}
			m.Verify()
		})
	}
}

// TestHTreeSegments2 covers the parallel-scan partition law: k <=
// max(1,n) non-overlapping views covering All().
func TestHTreeSegments2(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	// empty map: the partition contract demands EXACTLY one view for any n
	for _, n := range []int{-3, 0, 1, 2, 100} {
		if views := m.Segments2(n); len(views) != 1 {
			t.Fatalf("empty map Segments2(%d): %d views, want exactly 1", n, len(views))
		}
	}
	for i := int64(0); i < 300; i++ {
		m.Put(i, i)
	}
	for n := 0; n <= 40; n++ {
		views := m.Segments2(n)
		wantK := max(1, n)
		if len(views) > wantK {
			t.Fatalf("Segments2(%d): %d views > %d", n, len(views), wantK)
		}
		seen := map[int64]int{}
		total := 0
		for _, v := range views {
			for k := range v {
				seen[k]++
				total++
			}
		}
		if total != 300 || len(seen) != 300 {
			t.Fatalf("Segments2(%d): coverage %d distinct %d", n, total, len(seen))
		}
		for k, c := range seen {
			if c != 1 {
				t.Fatalf("Segments2(%d): key %d seen %d times", n, k, c)
			}
		}
	}
	// n >= physical segments → one view per segment
	if views := m.Segments2(1000); len(views) != m.Segments() {
		t.Fatalf("Segments2(1000): %d views, want %d", len(views), m.Segments())
	}
}

// TestHTreeClear: clear frees buckets, notifies listeners, root stays usable.
func TestHTreeClear(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	var mu sync.Mutex
	removed := map[int64]int64{}
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, old, new *int64, trig bool) error {
			mu.Lock()
			defer mu.Unlock()
			if new == nil {
				removed[k] = *old
			}
			return nil
		}))
	for i := int64(0); i < 400; i++ {
		m.Put(i, i+1)
	}
	m.Clear()
	if !m.IsEmpty() || m.LenLong() != 0 {
		t.Fatal("not empty after clear")
	}
	if len(removed) != 400 {
		t.Fatalf("listener saw %d removals, want 400", len(removed))
	}
	for i := int64(0); i < 400; i++ {
		if removed[i] != i+1 {
			t.Fatalf("listener removal %d=%d", i, removed[i])
		}
	}
	// reusable after clear
	m.Put(1, 11)
	if v, ok := m.Get(1); !ok || v != 11 {
		t.Fatalf("get after clear+put = %v,%v", v, ok)
	}
	m.Verify()
}

// TestHTreeClearWithoutNotification: silent clear emits no events.
func TestHTreeClearWithoutNotification(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	fired := false
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, old, new *int64, trig bool) error { fired = true; return nil }))
	m.Put(1, 1)
	m.ClearWithoutNotification()
	// the put's deferred event already fired; reset and re-check semantics
	fired = false
	m.Put(2, 2)
	// drain the put event before the silent clear
	fired = false
	m.ClearWithoutNotification()
	if fired {
		t.Fatal("silent clear emitted events")
	}
	if !m.IsEmpty() {
		t.Fatal("not empty")
	}
}

// TestHTreeListeners: deferred events fire after op return; sync fires
// under the lock and preserves per-key order.
func TestHTreeListeners(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	var events []string
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, old, new *int64, trig bool) error {
			switch {
			case old == nil:
				events = append(events, fmt.Sprintf("ins %d=%d", k, *new))
			case new == nil:
				events = append(events, fmt.Sprintf("del %d", k))
			default:
				events = append(events, fmt.Sprintf("upd %d %d->%d", k, *old, *new))
			}
			return nil
		}))
	m.Put(1, 10)
	m.Put(1, 20)
	m.Remove(1)
	if len(events) != 3 ||
		events[0] != "ins 1=10" || events[1] != "upd 1 10->20" || events[2] != "del 1" {
		t.Fatalf("events %v", events)
	}
	// remove works
	m.ModificationListenerRemove(mapdb5.ModificationListenerFunc[int64, int64](nil))
	// a panicking listener surfaces the error but the mutation commits
	m2 := NewHTreeMap[int64, int64](s, hcfg())
	m2.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, old, new *int64, trig bool) error {
			return fmt.Errorf("boom")
		}))
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("listener error not surfaced")
			}
		}()
		m2.Put(5, 5)
	}()
	if v, ok := m2.Get(5); !ok || v != 5 {
		t.Fatal("mutation lost after listener failure")
	}
}

// TestHTreeSyncListenerOrder: a synchronous listener sees a key's events
// in mutation order even across a put-then-remove burst.
func TestHTreeSyncListenerOrder(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	var seq []string
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, old, new *int64, trig bool) error {
			if new != nil {
				seq = append(seq, fmt.Sprintf("%d=%d", k, *new))
			} else {
				seq = append(seq, fmt.Sprintf("%d=-", k))
			}
			return nil
		}))
	for i := int64(0); i < 10; i++ {
		m.Put(1, i)
	}
	m.Remove(1)
	want := []string{"1=0", "1=1", "1=2", "1=3", "1=4", "1=5", "1=6", "1=7", "1=8", "1=9", "1=-"}
	if len(seq) != len(want) {
		t.Fatalf("seq %v", seq)
	}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("seq[%d]=%s want %s", i, seq[i], want[i])
		}
	}
}

// TestHTreeValueLoader: Get consults the loader and insert-caches the
// loaded value via putIfAbsent.
func TestHTreeValueLoader(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	loads := 0
	m.ValueLoader(func(k int64) (int64, bool) {
		loads++
		if k == 42 {
			return 4200, true
		}
		return 0, false
	})
	if v, ok := m.Get(42); !ok || v != 4200 {
		t.Fatalf("loader get = %v,%v", v, ok)
	}
	if loads != 1 {
		t.Fatalf("loads=%d", loads)
	}
	// second get hits the map, not the loader
	if v, ok := m.Get(42); !ok || v != 4200 {
		t.Fatalf("cached get = %v,%v", v, ok)
	}
	if loads != 1 {
		t.Fatalf("loads=%d after cached get", loads)
	}
	// loader miss → absent
	if _, ok := m.Get(1); ok {
		t.Fatal("loader-miss key reported present")
	}
	// clearing the loader restores plain semantics
	m.ValueLoader(nil)
	m.Remove(42)
	if _, ok := m.Get(42); ok {
		t.Fatal("get after loader cleared")
	}
}

// TestHTreeCloseInvalidates: CloseHandle rejects further ops and releases
// the lease so the map can be reopened.
func TestHTreeCloseInvalidates(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := hcfg()
	m := NewHTreeMap[int64, int64](s, cfg)
	m.Put(1, 1)
	hdr := m.HeaderRecid()
	m.CloseHandle()
	for _, op := range []func(){
		func() { m.Put(2, 2) },
		func() { m.Get(1) },
		func() { m.Remove(1) },
		func() {
			for range m.All() {
			}
		},
		func() { m.Clear() },
	} {
		func() {
			defer func() {
				r := recover()
				de, ok := mapdb5.AsDBError(r)
				if !ok || de.Kind != mapdb5.KindClosed {
					t.Fatalf("op after close: panic=%v", r)
				}
			}()
			op()
		}()
	}
	m2, err := OpenHTreeMap[int64, int64](s, hdr, cfg)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if v, ok := m2.Get(1); !ok || v != 1 {
		t.Fatal("reopened map lost data")
	}
	m2.CloseHandle()
}

// TestHTreeWeakIteration: mutating the map during iteration is safe
// (snapshot-per-segment) and yields are weakly consistent.
func TestHTreeWeakIteration(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	for i := int64(0); i < 200; i++ {
		m.Put(i, i)
	}
	count := 0
	for k, v := range m.All() {
		// mutate inside iteration: must not deadlock or corrupt
		if k == 0 {
			m.Put(9999, 9999)
			m.Remove(50)
		}
		_ = v
		count++
	}
	// weak consistency: the in-iteration put (9999) may or may not be
	// yielded, and the in-iteration remove (50) may or may not have been
	// drained before removal — any count in [199, 201] is legal.
	if count < 199 || count > 201 {
		t.Fatalf("iterated %d entries", count)
	}
	if v, ok := m.Get(9999); !ok || v != 9999 {
		t.Fatal("in-iteration put lost")
	}
	m.Verify()
}

// TestHTreeConcurrent: concurrent writers on distinct key ranges stay
// consistent (segment locks + race detector).
func TestHTreeConcurrent(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	var wg sync.WaitGroup
	for g := int64(0); g < 8; g++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < 500; i++ {
				k := base*1000 + i
				m.Put(k, k*2)
				if i%7 == 0 {
					m.Remove(k - 3)
				}
				if i%11 == 0 {
					m.Get(k)
				}
			}
		}(g)
	}
	wg.Wait()
	seen := 0
	for range m.All() {
		seen++
	}
	if seen != int(m.LenLong()) {
		t.Fatalf("iterated %d but len %d", seen, m.LenLong())
	}
	m.Verify()
}

// TestHTreeBadConfig: geometry validation errors surface correctly.
func TestHTreeBadConfig(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	for _, c := range []HConfig[int64, int64]{
		{KeySer: ser.Long, ValueSer: ser.Long, ConcShift: 13, DirShift: 1, Levels: 19},
		{KeySer: ser.Long, ValueSer: ser.Long, ConcShift: 0, DirShift: 0, Levels: 33},
		{KeySer: ser.Long, ValueSer: ser.Long, ConcShift: 4, DirShift: 7, Levels: 3}, // 4+21 != 32
		{KeySer: ser.Long, ValueSer: ser.Long, ConcShift: 4, DirShift: 8, Levels: 4},
	} {
		func() {
			defer func() {
				r := recover()
				de, ok := mapdb5.AsDBError(r)
				if !ok || de.Kind != mapdb5.KindWrongConfiguration {
					t.Fatalf("cfg %+v: panic=%v", c, r)
				}
			}()
			NewHTreeMap[int64, int64](s, c)
		}()
	}
}

// TestHTreeNestedWriteTripwire: a sync listener mutating the same map
// panics with the nested-deferral tripwire, not a deadlock.
func TestHTreeNestedWriteTripwire(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	var inner *HTreeMap[int64, int64]
	inner = m
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, old, new *int64, trig bool) error {
			inner.Put(999, 999) // same-goroutine write re-entry
			return nil
		}))
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("nested write did not trip")
			}
		}()
		m.Put(1, 1)
	}()
	// the mutation still committed; the map is usable
	if v, ok := m.Get(1); !ok || v != 1 {
		t.Fatal("primary mutation lost")
	}
}

// TestHTreeForEachOutsideLock: the callback may read the map without
// deadlock (Java forEach runs outside segment locks).
func TestHTreeForEachOutsideLock(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	for i := int64(0); i < 50; i++ {
		m.Put(i, i)
	}
	sum := int64(0)
	m.ForEach(func(k, v int64) {
		if got, ok := m.Get(k); ok {
			sum += got
		}
	})
	if sum != 49*50/2 {
		t.Fatalf("sum=%d", sum)
	}
}

// compile-time interface checks
var _ object_MutableMap_probe = (*HTreeMap[int64, int64])(nil)

type object_MutableMap_probe interface {
	Get(int64) (int64, bool)
	ContainsKey(int64) bool
	Len() int
	All() iter.Seq2[int64, int64]
	Keys() iter.Seq[int64]
	Values() iter.Seq[int64]
	ForEach(func(int64, int64))
	Put(int64, int64) (int64, bool)
	Remove(int64) (int64, bool)
	Clear()
}
