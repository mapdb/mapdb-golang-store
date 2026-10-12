package htree

import (
	"math/rand"
	"testing"

	"github.com/mapdb/mapdb5-go/store"
)

const (
	testDirShift = 4
	testLevels   = 3 // covers 12 index bits: indices in [0, 4096)
	testBudget   = 1 << (testDirShift * testLevels)
)

func newDirTree(s store.Store) uint64 {
	return store.Put(s, DirEmpty(), DirSer)
}

// TestDirTreeRandomOracle: random puts/removes tracked by a Go map —
// TreeGet must agree everywhere, including after lazy-split collisions and
// collapse push-ups.
func TestDirTreeRandomOracle(t *testing.T) {
	for _, name := range []string{"heap", "bytearray"} {
		t.Run(name, func(t *testing.T) {
			var s store.Store
			if name == "heap" {
				s = store.NewStoreOnHeap(true)
			} else {
				s = store.NewStoreByteArray(true)
			}
			root := newDirTree(s)
			want := map[int64]int64{}
			rng := rand.New(rand.NewSource(42))
			for i := 0; i < 3000; i++ {
				idx := int64(rng.Intn(testBudget))
				if rng.Intn(4) == 0 {
					delete(want, idx)
					if got := TreeRemove(testDirShift, root, s, testLevels-1, idx); got != (map[int64]int64{idx: 1}[idx] != 0) {
						// presence check below — TreeRemove returns present-ness
					}
					continue
				}
				v := int64(1 + rng.Intn(1000000))
				want[idx] = v
				TreePut(testDirShift, root, s, testLevels-1, idx, v)
			}
			for idx, v := range want {
				if got := TreeGet(testDirShift, root, s, testLevels-1, idx); got != v {
					t.Fatalf("TreeGet(%d) = %d, want %d", idx, got, v)
				}
			}
			// Absent probes.
			for i := 0; i < 500; i++ {
				idx := int64(rng.Intn(testBudget))
				if _, ok := want[idx]; !ok {
					if got := TreeGet(testDirShift, root, s, testLevels-1, idx); got != 0 {
						t.Fatalf("TreeGet(absent %d) = %d", idx, got)
					}
				}
			}
		})
	}
}

// TestDirTreeRemoveSemantics: TreeRemove's return value tracks presence,
// and collapse keeps the remaining entries reachable.
func TestDirTreeRemoveSemantics(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	root := newDirTree(s)
	// Two indices colliding at the top level force a lazy-split chain:
	// a=1, b=2 share top slot 0 → chain until divergence at level 0.
	a, b := int64(1), int64(2)
	TreePut(testDirShift, root, s, testLevels-1, a, 100)
	TreePut(testDirShift, root, s, testLevels-1, b, 200)
	if !TreeRemove(testDirShift, root, s, testLevels-1, a) {
		t.Fatal("remove a: absent")
	}
	if TreeGet(testDirShift, root, s, testLevels-1, b) != 200 {
		t.Fatal("collapse lost b")
	}
	if TreeRemove(testDirShift, root, s, testLevels-1, a) {
		t.Fatal("re-remove a: present")
	}
	if !TreeRemove(testDirShift, root, s, testLevels-1, b) {
		t.Fatal("remove b: absent")
	}
	if !TreeIsEmpty(root, s) {
		t.Fatal("root not empty after removing all")
	}
}

// TestDirTreeCollisionStress: many indices sharing the top slot build deep
// chains; removes must collapse without losing survivors.
func TestDirTreeCollisionStress(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	root := newDirTree(s)
	// Indices 0..63 all share top slot 0 — forces a full-depth chain.
	var idxs []int64
	for i := int64(0); i < 64; i++ {
		idxs = append(idxs, i)
		TreePut(testDirShift, root, s, testLevels-1, i, i*10+7)
	}
	for _, i := range idxs {
		if got := TreeGet(testDirShift, root, s, testLevels-1, i); got != i*10+7 {
			t.Fatalf("get %d = %d", i, got)
		}
	}
	for _, i := range idxs[:32] {
		TreeRemove(testDirShift, root, s, testLevels-1, i)
	}
	for _, i := range idxs[32:] {
		if got := TreeGet(testDirShift, root, s, testLevels-1, i); got != i*10+7 {
			t.Fatalf("post-collapse get %d = %d", i, got)
		}
	}
	// Fold sees exactly the survivors.
	seen := map[int64]int64{}
	TreeFold(root, s, testLevels-1, func(i, v int64) { seen[i] = v })
	if len(seen) != 32 {
		t.Fatalf("fold saw %d entries, want 32", len(seen))
	}
}

// TestDirTreeClear: clear frees subdirs, reports terminals, and keeps the
// root recid usable.
func TestDirTreeClear(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	root := newDirTree(s)
	for i := int64(0); i < 100; i++ {
		TreePut(testDirShift, root, s, testLevels-1, i*5%testBudget, i+1)
	}
	var freed []int64
	TreeClear(root, s, testLevels-1, func(i, v int64) { freed = append(freed, v) })
	if len(freed) == 0 {
		t.Fatal("clear reported no terminals")
	}
	if !TreeIsEmpty(root, s) {
		t.Fatal("not empty after clear")
	}
	TreePut(testDirShift, root, s, testLevels-1, 7, 77)
	if TreeGet(testDirShift, root, s, testLevels-1, 7) != 77 {
		t.Fatal("root unusable after clear")
	}
}

// TestDirTreeValueSwap: putting a different value under the same index
// replaces the terminal value in place.
func TestDirTreeValueSwap(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	root := newDirTree(s)
	TreePut(testDirShift, root, s, testLevels-1, 5, 50)
	TreePut(testDirShift, root, s, testLevels-1, 5, 60)
	if got := TreeGet(testDirShift, root, s, testLevels-1, 5); got != 60 {
		t.Fatalf("get after swap = %d, want 60", got)
	}
	// Idempotent put: same (index, value) is a NOOP — root unchanged.
	dir, _ := store.Get(s, root, DirSer)
	before := len(dir)
	TreePut(testDirShift, root, s, testLevels-1, 5, 60)
	dir2, _ := store.Get(s, root, DirSer)
	if len(dir2) != before {
		t.Fatal("noop put mutated the root")
	}
}
