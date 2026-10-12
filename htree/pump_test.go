package htree

import (
	"errors"
	"iter"
	"math/rand"
	"sort"
	"testing"

	"github.com/mapdb/mapdb-golang/pump"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/store"
)

// sortByPlacement orders entries by non-decreasing unsigned 32-bit hash —
// the contract CreateHTreeMapFromSortedByHash requires.
func sortByPlacement(pairs [][2]int64, hasher interface {
	Hash(int64, int32) int32
}, seed int32) {
	sort.Slice(pairs, func(i, j int) bool {
		hi := uint32(hasher.Hash(pairs[i][0], seed))
		hj := uint32(hasher.Hash(pairs[j][0], seed))
		return hi < hj
	})
}

func seqPairs(pairs [][2]int64) iter.Seq2[int64, int64] {
	return func(yield func(int64, int64) bool) {
		for _, p := range pairs {
			if !yield(p[0], p[1]) {
				return
			}
		}
	}
}

// TestHTreePumpBasic: bulk-load produces a fully functional map equal to
// the incrementally built one.
func TestHTreePumpBasic(t *testing.T) {
	for name, s := range hstores(t) {
		t.Run(name, func(t *testing.T) {
			cfg := hcfg()
			cfg.HashSeed, cfg.HashSeedSet = 0xbeef, true
			var pairs [][2]int64
			for i := int64(0); i < 2000; i++ {
				pairs = append(pairs, [2]int64{i, i * 7})
			}
			sortByPlacement(pairs, mdhash.Long32, cfg.HashSeed)
			m, err := CreateHTreeMapFromSortedByHash(s, cfg, seqPairs(pairs))
			if err != nil {
				t.Fatal(err)
			}
			defer m.CloseHandle()
			if m.LenLong() != 2000 {
				t.Fatalf("len=%d", m.LenLong())
			}
			for _, p := range pairs {
				if v, ok := m.Get(p[0]); !ok || v != p[1] {
					t.Fatalf("get %d = %v,%v", p[0], v, ok)
				}
			}
			m.Verify()
			// live map: normal mutations still work
			m.Put(-1, 1)
			m.Remove(0)
			if _, ok := m.Get(0); ok {
				t.Fatal("remove on pumped map failed")
			}
		})
	}
}

// TestHTreePumpMatchesIncremental: the pumped dir shape resolves
// identically to an incrementally built map.
func TestHTreePumpMatchesIncremental(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := hcfg()
	cfg.HashSeed, cfg.HashSeedSet = 77, true
	var pairs [][2]int64
	rng := rand.New(rand.NewSource(3))
	seen := map[int64]bool{}
	for len(pairs) < 500 {
		k := int64(rng.Intn(10000))
		if seen[k] {
			continue
		}
		seen[k] = true
		pairs = append(pairs, [2]int64{k, k * 11})
	}
	sortByPlacement(pairs, mdhash.Long32, cfg.HashSeed)

	pumped, err := CreateHTreeMapFromSortedByHash(s, cfg, seqPairs(pairs))
	if err != nil {
		t.Fatal(err)
	}
	defer pumped.CloseHandle()

	s2 := store.NewStoreOnHeap(true)
	inc := NewHTreeMap[int64, int64](s2, cfg)
	defer inc.CloseHandle()
	for _, p := range pairs {
		inc.Put(p[0], p[1])
	}
	for _, p := range pairs {
		v1, ok1 := pumped.Get(p[0])
		v2, ok2 := inc.Get(p[0])
		if ok1 != ok2 || v1 != v2 {
			t.Fatalf("key %d: pumped %v,%v vs inc %v,%v", p[0], v1, ok1, v2, ok2)
		}
	}
}

// TestHTreePumpNotSorted: misordered hash input is rejected with
// pump.ErrNotSorted.
func TestHTreePumpNotSorted(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := hcfg()
	cfg.HashSeed, cfg.HashSeedSet = 9, true
	pairs := [][2]int64{{1, 1}, {2, 2}, {3, 3}}
	// descending placement order is very likely misordered; construct it
	// deterministically by reversing a sorted order (guaranteed violation
	// unless all hashes tie, which Long32 won't for distinct keys).
	sortByPlacement(pairs, mdhash.Long32, cfg.HashSeed)
	for i, j := 0, len(pairs)-1; i < j; i, j = i+1, j-1 {
		pairs[i], pairs[j] = pairs[j], pairs[i]
	}
	_, err := CreateHTreeMapFromSortedByHash(s, cfg, seqPairs(pairs))
	if !errors.Is(err, pump.ErrNotSorted) {
		t.Fatalf("err=%v, want ErrNotSorted", err)
	}
}

// TestHTreePumpDuplicate: a duplicate key inside a bucket is rejected.
func TestHTreePumpDuplicate(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := hcfg()
	cfg.HashSeed, cfg.HashSeedSet = 9, true
	pairs := [][2]int64{{5, 1}, {5, 2}}
	_, err := CreateHTreeMapFromSortedByHash(s, cfg, seqPairs(pairs))
	if !errors.Is(err, pump.ErrNotSorted) {
		t.Fatalf("err=%v, want ErrNotSorted", err)
	}
}

// TestHTreePumpCollisionBuckets: forced collisions bucket correctly.
func TestHTreePumpCollisionBuckets(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	one := mdhash.HasherFunc[int64](func(k int64, seed int32) int32 { return 42 })
	cfg := hcfgSeg(one)
	var pairs [][2]int64
	for i := int64(0); i < 60; i++ {
		pairs = append(pairs, [2]int64{i, i})
	}
	// all same hash — non-decreasing is satisfied
	m, err := CreateHTreeMapFromSortedByHash(s, cfg, seqPairs(pairs))
	if err != nil {
		t.Fatal(err)
	}
	defer m.CloseHandle()
	if m.LenLong() != 60 {
		t.Fatalf("len=%d", m.LenLong())
	}
	for i := int64(0); i < 60; i++ {
		if v, ok := m.Get(i); !ok || v != i {
			t.Fatalf("get %d = %v,%v", i, v, ok)
		}
	}
	m.Verify()
}

// TestHTreePumpEmpty: zero entries still produces a valid empty map.
func TestHTreePumpEmpty(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := hcfg()
	m, err := CreateHTreeMapFromSortedByHash(s, cfg, seqPairs(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer m.CloseHandle()
	if !m.IsEmpty() || m.LenLong() != 0 {
		t.Fatal("pumped empty map not empty")
	}
	m.Put(1, 2)
	if v, ok := m.Get(1); !ok || v != 2 {
		t.Fatal("put on pumped empty map failed")
	}
}

// TestHTreePumpReopen: a pumped map persists and reopens.
func TestHTreePumpReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	s, err := store.NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := hcfg()
	cfg.HashSeed, cfg.HashSeedSet = 0x1234, true
	var pairs [][2]int64
	for i := int64(0); i < 1000; i++ {
		pairs = append(pairs, [2]int64{i, i})
	}
	sortByPlacement(pairs, mdhash.Long32, cfg.HashSeed)
	m, err := CreateHTreeMapFromSortedByHash(s, cfg, seqPairs(pairs))
	if err != nil {
		t.Fatal(err)
	}
	hdr := m.HeaderRecid()
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
	if m2.LenLong() != 1000 {
		t.Fatalf("len=%d", m2.LenLong())
	}
	m2.Verify()
}
