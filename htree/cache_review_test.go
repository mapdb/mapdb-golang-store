package htree

import (
	"errors"
	"iter"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func probePanic(f func()) (p any) { defer func() { p = recover() }(); f(); return }

type probeRecordSize struct{ store.RecordReadBase }

func (probeRecordSize) OnBytes(_ *bin.Reader, size int) int64 { return int64(size) }

func TestCacheReviewQueuePointerWire(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := NewHTreeCache(s, ccfgPlain(&fakeClock{}))
	q := m.inner.queues[0]
	for _, r := range []uint64{q.TailRecid(), q.HeadRecid(), q.HeadPrevRecid()} {
		if n := s.Read(r, probeRecordSize{}); n != 8 {
			t.Errorf("queue pointer %d: %d bytes, Java LONG requires 8", r, n)
		}
	}
	p := probePanic(func() { store.Get(s, q.TailRecid(), ser.Long) })
	t.Logf("Java LONG decoding Go tail pointer: %v", p)
	// Rewrite only the pointers with Java's encoding and reopen.
	tail, head, prev := q.Tail(), q.Head(), q.HeadPrev()
	hdr := m.HeaderRecid()
	m.CloseHandle()
	store.Update(s, q.TailRecid(), int64(tail), ser.Long)
	store.Update(s, q.HeadRecid(), int64(head), ser.Long)
	store.Update(s, q.HeadPrevRecid(), int64(prev), ser.Long)
	_, err := OpenHTreeCache(s, hdr, ccfgPlain(&fakeClock{}))
	if err != nil {
		t.Errorf("open Java-encoded queue pointers failed: %v", err)
	}
}

func TestCacheReviewGetPanicUnlock(t *testing.T) {
	bad := false
	cfg := ccfg1seg(&fakeClock{}, 10)
	cfg.Clock = func() int64 {
		if bad {
			panic("clock failed")
		}
		return 0
	}
	m := NewHTreeCache(store.NewStoreOnHeap(true), cfg)
	m.Put(1, 10)
	bad = true
	if p := probePanic(func() { m.Get(1) }); p == nil {
		t.Fatal("expected clock panic")
	}
	if !m.inner.seg.locks[0].TryLock() {
		m.inner.seg.locks[0].RUnlock() // clean leaked read lock after proving it
		t.Fatal("Get leaked RLock on panic; every subsequent writer blocks")
	}
	m.inner.seg.locks[0].Unlock()
}

func TestCacheReviewAllPanicAdmission(t *testing.T) {
	bad := false
	cfg := ccfg1seg(&fakeClock{}, 10)
	cfg.Clock = func() int64 {
		if bad {
			panic("clock failed")
		}
		return 0
	}
	m := NewHTreeCache(store.NewStoreOnHeap(true), cfg)
	m.Put(1, 10)
	bad = true
	recovered := make(chan struct{})
	cleanup := make(chan struct{})
	go func() {
		probePanic(func() { m.All()(func(int64, int64) bool { return true }) })
		close(recovered)
		<-cleanup
		m.inner.ops.DropSelf()
	}()
	<-recovered
	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()
	select {
	case <-closed:
		close(cleanup)
	case <-time.After(100 * time.Millisecond):
		close(cleanup)
		<-closed
		t.Fatal("Close blocked on admission leaked by iterator snapshot panic")
	}
}

func TestCacheReviewFailedOpenLease(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := ccfgPlain(&fakeClock{})
	m := NewHTreeCache(s, cfg)
	r := m.HeaderRecid()
	m.Close()
	h, _ := store.Get(s, r, cHeaderSerializer)
	originalTail := h.queueTails[0]
	h.queueTails[0] = h.queueHeads[0]
	store.Update(s, r, h, cHeaderSerializer)
	_, err := OpenHTreeCache(s, r, cfg)
	if err == nil {
		t.Fatal("corrupt queue should fail to open")
	}
	t.Logf("first open: %v", err)
	h.queueTails[0] = originalTail
	store.Update(s, r, h, cHeaderSerializer)
	_, err = OpenHTreeCache(s, r, cfg)
	if err != nil {
		t.Fatalf("repaired cache cannot open; failed open leaked lease: %v", err)
	}
}

type probeOverflow struct {
	get func(int64) (int64, bool)
	put func(int64, int64) (int64, bool)
}

func (o *probeOverflow) Get(k int64) (int64, bool) {
	if o.get != nil {
		return o.get(k)
	}
	return 0, false
}
func (o *probeOverflow) Put(k, v int64) (int64, bool) {
	if o.put != nil {
		return o.put(k, v)
	}
	return 0, false
}

func TestCacheReviewOverflowRewire(t *testing.T) {
	t.Run("clear", func(t *testing.T) {
		m := NewHTreeCache(store.NewStoreOnHeap(true), ccfgPlain(&fakeClock{}))
		m.SetOverflow(&probeOverflow{})
		if p := probePanic(func() { m.SetOverflow(nil) }); p != nil {
			t.Fatalf("SetOverflow(nil) panicked: %v", p)
		}
	})
	t.Run("change_type", func(t *testing.T) {
		m := NewHTreeCache(store.NewStoreOnHeap(true), ccfgPlain(&fakeClock{}))
		m.SetOverflow(&probeOverflow{})
		ov := NewHTreeMap(store.NewStoreOnHeap(true), hcfg())
		if p := probePanic(func() { m.SetOverflow(ov) }); p != nil {
			t.Fatalf("SetOverflow(new implementation) panicked: %v", p)
		}
	})
}

func TestCacheReviewEvictionCallbackMutation(t *testing.T) {
	clock := &fakeClock{}
	m := NewHTreeCache(store.NewStoreOnHeap(true), ccfg1seg(clock, 10))
	m.Put(1, 10)
	clock.advance(11)
	var callbackPanic any
	m.SetEvictionListener(func(k, v int64, c EvictionCause) { callbackPanic = probePanic(func() { m.Put(99, 99) }) })
	m.ExpireEvict()
	if callbackPanic == nil {
		t.Fatalf("eviction callback mutated cache without required reentry panic; ContainsKey(99)=%v", m.ContainsKey(99))
	}
}

func TestCacheReviewSyncEvictionError(t *testing.T) {
	clock := &fakeClock{}
	m := NewHTreeCache(store.NewStoreOnHeap(true), ccfg1seg(clock, 10))
	m.Put(1, 10)
	clock.advance(11)
	sentinel := errors.New("secondary index removal failed")
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](func(k int64, old, new *int64, trig bool) error {
		if trig {
			return sentinel
		}
		return nil
	}))
	p := probePanic(func() { m.ExpireEvict() })
	if p == nil {
		t.Fatal("ExpireEvict silently discarded synchronous removal listener error")
	}
}

func TestCacheReviewSyncEvictionClose(t *testing.T) {
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 10)
	s := store.NewStoreOnHeap(true)
	m := NewHTreeCache(s, cfg)
	m.Put(1, 10)
	clock.advance(11)
	var reopened *HTreeCache[int64, int64]
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](func(k int64, old, new *int64, trig bool) error {
		if trig {
			m.Close()
			var err error
			reopened, err = OpenHTreeCache(s, m.HeaderRecid(), cfg)
			return err
		}
		return nil
	}))
	p := probePanic(func() { m.Put(2, 20) })
	if reopened == nil {
		t.Fatal("did not reopen")
	}
	if reopened.ContainsKey(2) {
		t.Fatalf("retired Put wrote after callback closed/reopened; panic=%v, reopened LenLong=%d, actual key2 present", p, reopened.LenLong())
	}
}

func TestCacheReviewOverflowGetPanicGuard(t *testing.T) {
	m := NewHTreeCache(store.NewStoreOnHeap(true), ccfgPlain(&fakeClock{}))
	calls := 0
	m.SetOverflow(&probeOverflow{get: func(k int64) (int64, bool) {
		calls++
		if calls == 1 {
			panic(errors.New("transient overflow failure"))
		}
		return 42, true
	}})
	if p := probePanic(func() { m.Get(1) }); p == nil {
		t.Fatal("expected overflow panic")
	}
	v, ok := m.Get(1)
	if !ok || v != 42 {
		t.Fatalf("overflow remained disabled after panic: calls=%d, result=(%d,%v)", calls, v, ok)
	}
}

func TestCacheReviewOverflowPutPanicGuard(t *testing.T) {
	cfg := ccfgPlain(&fakeClock{})
	cfg.MaxSize = 1
	m := NewHTreeCache(store.NewStoreOnHeap(true), cfg)
	calls := 0
	m.SetOverflow(&probeOverflow{put: func(k, v int64) (int64, bool) {
		calls++
		if calls == 1 {
			panic("transient overflow failure")
		}
		return 0, false
	}})
	m.Put(1, 1)
	m.Put(2, 2)
	if p := probePanic(func() { m.Put(3, 3) }); p == nil {
		t.Fatal("expected overflow panic")
	}
	m.Put(4, 4)
	if calls != 2 {
		t.Fatalf("overflow remained disabled after panic: calls=%d, expected 2", calls)
	}
}

func TestCacheReviewOverflowFailureSkipsListener(t *testing.T) {
	cfg := ccfgPlain(&fakeClock{})
	cfg.MaxSize = 1
	m := NewHTreeCache(store.NewStoreOnHeap(true), cfg)
	m.SetOverflow(&probeOverflow{put: func(k, v int64) (int64, bool) { panic(errors.New("overflow failed")) }})
	called := false
	m.SetEvictionListener(func(k, v int64, c EvictionCause) { called = true })
	m.Put(1, 1)
	m.Put(2, 2)
	probePanic(func() { m.Put(3, 3) })
	if called {
		t.Fatal("eviction listener called despite failed overflow; Java skips listener batch")
	}
}

func TestCacheReviewPullIteratorClose(t *testing.T) {
	m := NewHTreeCache(store.NewStoreOnHeap(true), ccfgPlain(&fakeClock{}))
	m.Put(1, 1)
	next, stop := iter.Pull2(m.All())
	defer stop()
	next()
	done := make(chan struct{})
	go func() { m.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked on suspended iterator")
	}
}

func TestCacheReviewClearStopsAfterClose(t *testing.T) {
	clock := &fakeClock{}
	cfg := ccfgSeg(clock, 100)
	cfg.Hasher = probeSegmentHasher{}
	s := store.NewStoreOnHeap(true)
	m := NewHTreeCache(s, cfg)
	m.Put(0, 0)
	m.Put(1, 1)
	var reopened *HTreeCache[int64, int64]
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](func(k int64, old, new *int64, trig bool) error {
		if k == 0 && new == nil {
			m.Close()
			var err error
			reopened, err = OpenHTreeCache(s, m.HeaderRecid(), cfg)
			if err != nil {
				return err
			}
			reopened.Put(2, 99)
		}
		return nil
	}))
	p := probePanic(func() { m.Clear() })
	if p == nil {
		t.Fatal("Clear should detect retirement")
	}
	if v, ok := reopened.Get(2); !ok || v != 99 {
		t.Fatal("retired Clear deleted new handle entry")
	}
}

type probeSegmentHasher struct{}

func (probeSegmentHasher) Hash(k int64, _ int32) int32 { return int32(k << 30) }

func TestCacheReviewLoaderStopsAfterClose(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	cfg := ccfgPlain(&fakeClock{})
	m := NewHTreeCache(s, cfg)
	var reopened *HTreeCache[int64, int64]
	m.ValueLoader(func(k int64) (int64, bool) {
		m.Close()
		var err error
		reopened, err = OpenHTreeCache(s, m.HeaderRecid(), cfg)
		if err != nil {
			panic(err)
		}
		return 42, true
	})
	if p := probePanic(func() { m.Get(1) }); p == nil {
		t.Fatal("Get should detect retirement")
	}
	if reopened.ContainsKey(1) {
		t.Fatal("retired loader promoted into reopened cache")
	}
}

func TestCacheReviewChurnRecordsCounters(t *testing.T) {
	for _, collision := range []bool{false, true} {
		s := store.NewStoreByteArray(true)
		clock := &fakeClock{}
		cfg := ccfg1seg(clock, 10)
		cfg.MaxEvictPerOp = 1
		if collision {
			cfg.Hasher = probeConstantHasher{}
		}
		m := NewHTreeCache(s, cfg)
		count := func() int {
			n := 0
			for range s.AllRecids() {
				n++
			}
			return n
		}
		baseline := count()
		check := func() {
			physical := 0
			TreeFold(m.inner.segmentRoots[0], s, m.inner.levels-1, func(_, r int64) { physical += len(m.loadLeaf(r).keys) })
			if int64(physical) != m.LenLong() || uint64(physical) != m.inner.queues[0].Len() {
				t.Fatalf("counter/leaf/queue drift: physical=%d counter=%d queue=%d", physical, m.LenLong(), m.inner.queues[0].Len())
			}
			m.inner.queues[0].Verify()
		}
		for round := 0; round < 10; round++ {
			for k := int64(0); k < 30; k++ {
				m.Put(k, k)
				check()
			}
			for k := int64(0); k < 30; k += 2 {
				m.Put(k, k+10)
				check()
			}
			for k := int64(0); k < 10; k++ {
				m.Remove(k)
				check()
			}
			clock.advance(11)
			m.PutIfAbsent(29, 999)
			check() // expired target skipped by throttled sweep
			m.ExpireEvict()
			check()
			m.Clear()
			check()
			if got := count(); got != baseline {
				t.Fatalf("record leak after churn collision=%v: got=%d baseline=%d", collision, got, baseline)
			}
		}
	}
}

type probeConstantHasher struct{}

func (probeConstantHasher) Hash(int64, int32) int32 { return 0 }

// A backward Clock step must not reorder queue deadlines: expiry reads a
// monotone high-water clock, so key2 (written "at 0" after tick 100) is
// stamped from tick 100, both keys expire together, and an unthrottled
// sweep reclaims both. Liveness of a matched key is always checked.
func TestCacheReviewClockRollback(t *testing.T) {
	clock := &fakeClock{now: 100}
	m := NewHTreeCache(store.NewStoreOnHeap(true), ccfg1seg(clock, 10))
	m.Put(1, 10)
	clock.now = 0
	m.Put(2, 20)
	clock.now = 11
	if !m.ContainsKey(2) || !m.ContainsKey(1) {
		t.Fatal("rollback must not shorten lifetimes: both keys live until tick 110")
	}
	clock.now = 111
	if m.ContainsKey(2) {
		t.Fatal("expected key2 expired")
	}
	if old, had := m.PutIfAbsent(2, 99); had {
		t.Fatalf("PutIfAbsent treated expired key2 as live: old=%d", old)
	}
	m.ExpireEvict()
	if n := m.LenLong(); n != 1 {
		t.Fatalf("after ExpireEvict LenLong=%d, want 1 (only the fresh key2)", n)
	}
	if v, ok := m.Get(2); !ok || v != 99 {
		t.Fatalf("Get(2)=(%d,%v), want (99,true)", v, ok)
	}
}
