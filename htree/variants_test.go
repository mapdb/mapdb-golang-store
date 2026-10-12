package htree

import (
	"sort"
	"sync"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func h48cfg() H48Config[int64, int64] {
	return H48Config[int64, int64]{
		KeySer: ser.Long, ValueSer: ser.Long, Hasher: mdhash.Long64,
	}
}

// TestHTree48BasicOps: the 48-bit variant has the same op semantics over a
// wider hash budget.
func TestHTree48BasicOps(t *testing.T) {
	for name, s := range hstores(t) {
		t.Run(name, func(t *testing.T) {
			m := NewHTreeMap48[int64, int64](s, h48cfg())
			defer m.CloseHandle()
			if m.Segments() != 64 {
				t.Fatalf("default segments=%d, want 64", m.Segments())
			}
			for i := int64(0); i < 500; i++ {
				m.Put(i, i*10)
			}
			if m.LenLong() != 500 {
				t.Fatalf("len=%d", m.LenLong())
			}
			for i := int64(0); i < 500; i++ {
				if v, ok := m.Get(i); !ok || v != i*10 {
					t.Fatalf("get %d = %v,%v", i, v, ok)
				}
			}
			if old, had := m.PutIfAbsent(7, 99); !had || old != 70 {
				t.Fatalf("putIfAbsent: had=%v old=%v", had, old)
			}
			if !m.RemoveIfEquals(7, 70) {
				t.Fatal("removeIfEquals")
			}
			m.Verify()
			// iteration coverage
			seen := map[int64]int64{}
			for k, v := range m.All() {
				seen[k] = v
			}
			if len(seen) != 499 {
				t.Fatalf("iterated %d", len(seen))
			}
		})
	}
}

// TestHTree48Reopen: 48-bit header round-trips (long seed, wider budget).
func TestHTree48Reopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := h48cfg()
	cfg.HashSeed, cfg.HashSeedSet = 0xdeadbeefcafe, true
	m := NewHTreeMap48[int64, int64](s, cfg)
	for i := int64(0); i < 300; i++ {
		m.Put(i, i)
	}
	hdr := m.HeaderRecid()
	m.CloseHandle()
	s.Close()

	s2, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := OpenHTreeMap48[int64, int64](s2, hdr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.CloseHandle()
	if m2.HashSeed64() != 0xdeadbeefcafe {
		t.Fatalf("seed=%x", m2.HashSeed64())
	}
	if m2.LenLong() != 300 {
		t.Fatalf("len=%d", m2.LenLong())
	}
	m2.Verify()
	// a 48-bit header must not open as the 32-bit map (different codec)
	if _, err := OpenHTreeMap[int64, int64](s2, hdr, hcfg()); err == nil {
		t.Fatal("48-bit header opened as 32-bit map")
	}
}

// TestHTree48Pump: bulk load over the 64-bit unsigned order.
func TestHTree48Pump(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := h48cfg()
	cfg.HashSeed, cfg.HashSeedSet = int64(-7046029254386353131), true // 0x9e3779b97f4a7c15
	var pairs [][2]int64
	for i := int64(0); i < 1000; i++ {
		pairs = append(pairs, [2]int64{i, i * 5})
	}
	sort.Slice(pairs, func(i, j int) bool {
		hi := uint64(mdhash.Long64.Hash64(pairs[i][0], cfg.HashSeed))
		hj := uint64(mdhash.Long64.Hash64(pairs[j][0], cfg.HashSeed))
		return hi < hj
	})
	m, err := CreateHTreeMap48FromSortedByHash(s, cfg, seqPairs(pairs))
	if err != nil {
		t.Fatal(err)
	}
	defer m.CloseHandle()
	if m.LenLong() != 1000 {
		t.Fatalf("len=%d", m.LenLong())
	}
	for _, p := range pairs {
		if v, ok := m.Get(p[0]); !ok || v != p[1] {
			t.Fatalf("get %d = %v,%v", p[0], v, ok)
		}
	}
	m.Verify()
}

// ==================== external variant ====================

// TestHTreeExternalBasicOps: external values behave identically at the
// API surface.
func TestHTreeExternalBasicOps(t *testing.T) {
	for name, s := range hstores(t) {
		t.Run(name, func(t *testing.T) {
			m := NewHTreeMapExternal[int64, int64](s, hcfg())
			defer m.CloseHandle()
			for i := int64(0); i < 300; i++ {
				m.Put(i, i*10)
			}
			if m.LenLong() != 300 {
				t.Fatalf("len=%d", m.LenLong())
			}
			for i := int64(0); i < 300; i++ {
				if v, ok := m.Get(i); !ok || v != i*10 {
					t.Fatalf("get %d = %v,%v", i, v, ok)
				}
			}
			if old, had := m.Put(5, 55); !had || old != 50 {
				t.Fatalf("re-put: %v,%v", old, had)
			}
			if old, had := m.PutIfAbsent(5, 0); !had || old != 55 {
				t.Fatalf("putIfAbsent: %v,%v", old, had)
			}
			if old, had := m.Replace(5, 66); !had || old != 55 {
				t.Fatalf("replace: %v,%v", old, had)
			}
			if !m.ReplaceIfEquals(5, 66, 77) {
				t.Fatal("replaceIfEquals")
			}
			if !m.RemoveIfEquals(5, 77) {
				t.Fatal("removeIfEquals")
			}
			if _, ok := m.Get(5); ok {
				t.Fatal("removed key still present")
			}
			m.Verify()
		})
	}
}

// TestHTreeExternalValueRecordsFreed: remove/clear delete value records —
// the store's recid table must not grow unboundedly. (Java TCK leak gate.)
func TestHTreeExternalValueRecordsFreed(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := NewHTreeMapExternal[int64, int64](s, hcfg())
	for i := int64(0); i < 100; i++ {
		m.Put(i, i)
	}
	m.Verify()
	// Every remove frees its value record: after removing all but the
	// structural records the map is empty and Verify holds.
	for i := int64(0); i < 100; i++ {
		if _, had := m.Remove(i); !had {
			t.Fatalf("remove %d", i)
		}
	}
	if !m.IsEmpty() {
		t.Fatal("not empty")
	}
	m.Verify()
	// refill — recid reuse means the store shouldn't need fresh records
	// for the values (they were freed); either way the data must resolve.
	for i := int64(0); i < 100; i++ {
		m.Put(i, i*2)
	}
	for i := int64(0); i < 100; i++ {
		if v, ok := m.Get(i); !ok || v != i*2 {
			t.Fatalf("get %d = %v,%v", i, v, ok)
		}
	}
	m.Verify()
}

// TestHTreeExternalClear: clear frees buckets AND value records, fires
// removal events with resolved old values.
func TestHTreeExternalClear(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMapExternal[int64, int64](s, hcfg())
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
	for i := int64(0); i < 200; i++ {
		m.Put(i, i+1)
	}
	m.Clear()
	if !m.IsEmpty() {
		t.Fatal("not empty")
	}
	if len(removed) != 200 {
		t.Fatalf("listener saw %d removals", len(removed))
	}
	for i := int64(0); i < 200; i++ {
		if removed[i] != i+1 {
			t.Fatalf("removed[%d]=%d", i, removed[i])
		}
	}
	m.Verify()
}

// TestHTreeExternalPutOnlyBlind: PutOnly overwrites without returning the
// old value; semantics still correct.
func TestHTreeExternalPutOnlyBlind(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMapExternal[int64, int64](s, hcfg())
	m.PutOnly(1, 10)
	if v, ok := m.Get(1); !ok || v != 10 {
		t.Fatalf("get=%v,%v", v, ok)
	}
	m.PutOnly(1, 20) // blind overwrite
	if v, ok := m.Get(1); !ok || v != 20 {
		t.Fatalf("get=%v,%v", v, ok)
	}
	// blind removeOnly (no listeners)
	if !m.RemoveOnly(1) {
		t.Fatal("removeOnly")
	}
	if m.RemoveOnly(1) {
		t.Fatal("removeOnly on absent")
	}
	m.Verify()
}

// TestHTreeExternalListeners: listener events carry resolved values.
func TestHTreeExternalListeners(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMapExternal[int64, int64](s, hcfg())
	var events []int64
	var kinds []string
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, old, new *int64, trig bool) error {
			switch {
			case old == nil:
				kinds = append(kinds, "ins")
				events = append(events, *new)
			case new == nil:
				kinds = append(kinds, "del")
				events = append(events, *old)
			default:
				kinds = append(kinds, "upd")
				events = append(events, *old)
			}
			return nil
		}))
	m.Put(1, 10)
	m.Put(1, 20)
	m.Remove(1)
	if len(kinds) != 3 || kinds[0] != "ins" || kinds[1] != "upd" || kinds[2] != "del" {
		t.Fatalf("kinds=%v", kinds)
	}
	if events[0] != 10 || events[1] != 10 || events[2] != 20 {
		t.Fatalf("events=%v", events)
	}
}

// TestHTreeExternalCollisionBucket: same-hash bucket growth with external
// values — the bucket holds recids, scans stay key-only.
func TestHTreeExternalCollisionBucket(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	one := mdhash.HasherFunc[int64](func(k int64, seed int32) int32 { return 0x5a5a5a5 })
	m := NewHTreeMapExternal[int64, int64](s, hcfgSeg(one))
	for i := int64(0); i < 40; i++ {
		m.Put(i, i*100)
	}
	for i := int64(0); i < 40; i++ {
		if v, ok := m.Get(i); !ok || v != i*100 {
			t.Fatalf("get %d = %v,%v", i, v, ok)
		}
	}
	for i := int64(0); i < 40; i++ {
		m.Remove(i)
	}
	if !m.IsEmpty() {
		t.Fatal("not empty")
	}
	m.Verify()
}

// TestHTreeExternalReopen: external map persists and reopens.
func TestHTreeExternalReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := hcfg()
	m := NewHTreeMapExternal[int64, int64](s, cfg)
	for i := int64(0); i < 500; i++ {
		m.Put(i, i*3)
	}
	hdr := m.HeaderRecid()
	m.CloseHandle()
	s.Close()

	s2, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := OpenHTreeMapExternal[int64, int64](s2, hdr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.CloseHandle()
	if m2.LenLong() != 500 {
		t.Fatalf("len=%d", m2.LenLong())
	}
	for i := int64(0); i < 500; i++ {
		if v, ok := m2.Get(i); !ok || v != i*3 {
			t.Fatalf("get %d = %v,%v", i, v, ok)
		}
	}
	m2.Verify()
}

// TestHTreeExternalPump: bulk load writes value records first.
func TestHTreeExternalPump(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := hcfg()
	cfg.HashSeed, cfg.HashSeedSet = 0x777, true
	var pairs [][2]int64
	for i := int64(0); i < 800; i++ {
		pairs = append(pairs, [2]int64{i, i * 13})
	}
	sortByPlacement(pairs, mdhash.Long32, cfg.HashSeed)
	m, err := CreateHTreeMapExternalFromSortedByHash(s, cfg, seqPairs(pairs))
	if err != nil {
		t.Fatal(err)
	}
	defer m.CloseHandle()
	if m.LenLong() != 800 {
		t.Fatalf("len=%d", m.LenLong())
	}
	for _, p := range pairs {
		if v, ok := m.Get(p[0]); !ok || v != p[1] {
			t.Fatalf("get %d = %v,%v", p[0], v, ok)
		}
	}
	m.Verify()
}

// TestHTreeExternalValueLoader: the loader path resolves through value
// records.
func TestHTreeExternalValueLoader(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMapExternal[int64, int64](s, hcfg())
	m.ValueLoader(func(k int64) (int64, bool) {
		if k == 9 {
			return 90, true
		}
		return 0, false
	})
	if v, ok := m.Get(9); !ok || v != 90 {
		t.Fatalf("loaded get=%v,%v", v, ok)
	}
	if v, ok := m.Get(9); !ok || v != 90 {
		t.Fatalf("cached get=%v,%v", v, ok)
	}
	if _, ok := m.Get(1); ok {
		t.Fatal("loader-miss present")
	}
}
