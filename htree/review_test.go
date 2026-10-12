package htree

// Regression tests for the M7 HTree Astra review findings (todo/reviews/
// m7_htree_review.md): retirement must stop a mid-flight Clear, the
// loader boundary must re-check retirement, iterators must not hold an
// admission across yields (Close deadlock), read locks must release on
// panic paths, and Segments2 must return exactly one view on an empty map.

import (
	"iter"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func mustPanicClosed(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindClosed {
			t.Fatalf("panic = %v, want KindClosed", r)
		}
	}()
	f()
}

// Finding 1: a deferred listener retiring the handle mid-Clear must stop
// the retired op before it touches later segments — and must not delete
// data written through a reopened handle.
func TestHTreeClearAbortsAfterClose(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	// key k lands in segment k (top concShift bits of the placement hash)
	segHasher := mdhash.HasherFunc[int64](func(k int64, seed int32) int32 {
		return int32(k << 28)
	})
	cfg := HConfig[int64, int64]{KeySer: ser.Long, ValueSer: ser.Long, Hasher: segHasher}
	m := NewHTreeMap[int64, int64](s, cfg)
	m.Put(0, 0)
	m.Put(1, 1)

	var m2 *HTreeMap[int64, int64]
	var once atomic.Bool
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error {
			if n == nil && once.CompareAndSwap(false, true) {
				m.Close()
				var err error
				m2, err = OpenHTreeMap[int64, int64](s, m.HeaderRecid(), cfg)
				if err != nil {
					t.Errorf("reopen: %v", err)
					return nil
				}
				m2.Put(2, 99) // segment 2 — not yet visited by the Clear
			}
			return nil
		}))

	// The Clear must die at the next segment boundary with KindClosed.
	mustPanicClosed(t, m.Clear)

	if m2 == nil {
		t.Fatal("listener never ran")
	}
	defer m2.CloseHandle()
	if v, ok := m2.Get(2); !ok || v != 99 {
		t.Fatalf("reopened get(2) = %v,%v — retired Clear deleted new data", v, ok)
	}
	// segment 1 was never reached by the aborted Clear
	if v, ok := m2.Get(1); !ok || v != 1 {
		t.Fatalf("get(1) = %v,%v", v, ok)
	}
}

// Finding 1 (external twin): same retirement boundary on the
// external-value variant.
func TestHTreeExternalClearAbortsAfterClose(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	segHasher := mdhash.HasherFunc[int64](func(k int64, seed int32) int32 {
		return int32(k << 28)
	})
	cfg := HConfig[int64, int64]{KeySer: ser.Long, ValueSer: ser.Long, Hasher: segHasher}
	m := NewHTreeMapExternal[int64, int64](s, cfg)
	m.Put(0, 0)
	m.Put(1, 1)

	var m2 *HTreeMapExternal[int64, int64]
	var once atomic.Bool
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error {
			if n == nil && once.CompareAndSwap(false, true) {
				m.Close()
				var err error
				m2, err = OpenHTreeMapExternal[int64, int64](s, m.HeaderRecid(), cfg)
				if err != nil {
					t.Errorf("reopen: %v", err)
					return nil
				}
				m2.Put(2, 99)
			}
			return nil
		}))

	mustPanicClosed(t, m.Clear)
	if m2 == nil {
		t.Fatal("listener never ran")
	}
	defer m2.CloseHandle()
	if v, ok := m2.Get(2); !ok || v != 99 {
		t.Fatalf("get(2) = %v,%v", v, ok)
	}
}

// Finding 1 (loader boundary): a value loader that retires the handle
// must stop Get before it commits the loaded value.
func TestHTreeLoaderCloseBoundary(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	m.ValueLoader(func(k int64) (int64, bool) {
		m.Close()
		return 42, true
	})
	mustPanicClosed(t, func() { m.Get(7) })
	// the closed map never committed the loaded value — reopen and check
	m2, err := OpenHTreeMap[int64, int64](s, m.HeaderRecid(), hcfg())
	if err != nil {
		t.Fatal(err)
	}
	defer m2.CloseHandle()
	if _, ok := m2.Get(7); ok {
		t.Fatal("retired Get committed the loaded value")
	}
}

// Finding 3: a suspended pull iterator must not hold an operation
// admission — Close has to drain and return, and resuming the iterator
// afterwards panics KindClosed.
func TestHTreeIteratorCloseNoDeadlock(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	for i := int64(0); i < 100; i++ {
		m.Put(i, i)
	}
	next, stop := iter.Pull2(m.All())
	defer stop()
	if _, _, ok := next(); !ok {
		t.Fatal("empty pull")
	}
	closeDone := make(chan struct{})
	go func() { defer close(closeDone); m.Close() }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked on a suspended pull iterator")
	}
	// resuming the retired iterator panics KindClosed
	mustPanicClosed(t, func() { next() })
}

// Finding 3 (Segments2 twin): a suspended view iterator must not block
// Close either.
func TestHTreeSegments2CloseNoDeadlock(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	m := NewHTreeMap[int64, int64](s, hcfg())
	for i := int64(0); i < 100; i++ {
		m.Put(i, i)
	}
	next, stop := iter.Pull2(m.Segments2(1)[0])
	defer stop()
	if _, _, ok := next(); !ok {
		t.Fatal("empty pull")
	}
	closeDone := make(chan struct{})
	go func() { defer close(closeDone); m.Close() }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked on a suspended Segments2 view")
	}
}

// Finding 4: a panic inside Verify must not strand the segment read
// lock — a subsequent write to the same segment must proceed.
func TestHTreeVerifyPanicReleasesLock(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	fail := &atomic.Bool{}
	hasher := mdhash.HasherFunc[int64](func(k int64, seed int32) int32 {
		if fail.Load() {
			panic("injected hash failure")
		}
		return mdhash.Long32.Hash(k, seed)
	})
	m := NewHTreeMap[int64, int64](s, HConfig[int64, int64]{
		KeySer: ser.Long, ValueSer: ser.Long, Hasher: hasher,
	})
	m.Put(1, 1)
	fail.Store(true)
	func() {
		defer func() { recover() }()
		m.Verify() // panics hashing the bucket key
	}()
	fail.Store(false)

	done := make(chan struct{})
	go func() { defer close(done); m.Put(1, 11); m.Put(2, 2) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Verify leaked a segment read lock — Put blocked")
	}
	if v, ok := m.Get(1); !ok || v != 11 {
		t.Fatalf("get = %v,%v", v, ok)
	}
}

// Finding 4 (LenLong path): a corruption panic inside the fold must also
// release the segment read lock — a write to the same segment must not
// block afterwards. (A leaked READ lock only blocks writers.)
func TestHTreeLenLongPanicReleasesLock(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	// every key lands in segment 0; index = key (distinct buckets)
	hasher := mdhash.HasherFunc[int64](func(k int64, seed int32) int32 { return int32(k) })
	cfg := HConfig[int64, int64]{KeySer: ser.Long, ValueSer: ser.Long, Hasher: hasher}
	m := NewHTreeMap[int64, int64](s, cfg)
	m.Put(1, 1)
	// delete key 1's bucket record out from under the dir tree, so the
	// next fold panics corruption mid-traversal
	var leafRecid int64
	TreeFold(m.inner.segmentRoots[0], s, m.inner.levels-1,
		func(index, l int64) { leafRecid = l })
	if leafRecid == 0 {
		t.Fatal("no bucket found")
	}
	store.Delete(s, uint64(leafRecid), m.inner.leafSer)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("LenLong did not panic on the missing bucket")
			}
		}()
		m.LenLong()
	}()

	done := make(chan struct{})
	go func() { defer close(done); m.Put(2, 2) }() // same segment, new index
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("LenLong leaked a segment read lock — Put blocked")
	}
}
