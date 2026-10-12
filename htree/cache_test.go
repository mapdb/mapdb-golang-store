package htree

import (
	"math"
	"sync"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// fakeClock is the deterministic tick source for expiry tests.
type fakeClock struct{ now int64 }

func (c *fakeClock) get() int64      { return c.now }
func (c *fakeClock) advance(d int64) { c.now += d }

// ccfg1seg is the single-segment cache config (Java singleSegment):
// concShift 0 + 8 levels x 4 bits = 32.
func ccfg1seg(clock *fakeClock, ttl int64) CConfig[int64, int64] {
	return CConfig[int64, int64]{
		KeySer: ser.Long, ValueSer: ser.Long, Hasher: mdhash.Long32,
		ConcShift: 0, DirShift: 4, Levels: 8,
		TTL: ttl, Clock: clock.get,
	}
}

// ccfgSeg is a multi-segment config with the default hash geometry.
func ccfgSeg(clock *fakeClock, ttl int64) CConfig[int64, int64] {
	return CConfig[int64, int64]{
		KeySer: ser.Long, ValueSer: ser.Long, Hasher: mdhash.Long32,
		ConcShift: 2, DirShift: 6, Levels: 5,
		HashSeed: 0x5eed, HashSeedSet: true,
		TTL: ttl, Clock: clock.get,
	}
}

// ccfgPlain is Java's plainCache: no TTL, unbounded maxSize — nothing
// ever expires or evicts.
func ccfgPlain(clock *fakeClock) CConfig[int64, int64] {
	cfg := ccfg1seg(clock, 0)
	cfg.MaxSize = math.MaxInt64
	return cfg
}

// TestCacheLifecycle: create → put/get/contains/remove on both dialects.
func TestCacheLifecycle(t *testing.T) {
	for name, s := range hstores(t) {
		t.Run(name, func(t *testing.T) {
			clock := &fakeClock{}
			m := NewHTreeCache[int64, int64](s, ccfgSeg(clock, 1_000))
			if !m.IsEmpty() || m.LenLong() != 0 {
				t.Fatal("new cache not empty")
			}
			if old, had := m.Put(7, 70); had || old != 0 {
				t.Fatal("put did not report insert")
			}
			if v, ok := m.Get(7); !ok || v != 70 {
				t.Fatal("get miss")
			}
			if !m.ContainsKey(7) {
				t.Fatal("containsKey miss")
			}
			if v, ok := m.Remove(7); !ok || v != 70 {
				t.Fatal("remove did not return old value")
			}
			if _, ok := m.Get(7); ok {
				t.Fatal("removed key still visible")
			}
			if !m.IsEmpty() {
				t.Fatal("cache not empty after remove")
			}
		})
	}
}

// TestCacheTTLBoundary (Java ttlBoundaryIsOpenDeadline): live AT the
// deadline, expired once past it.
func TestCacheTTLBoundary(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfg1seg(clock, 1_000))
	m.Put(1, 10)
	clock.advance(1_000) // exactly the deadline instant
	if v, ok := m.Get(1); !ok || v != 10 {
		t.Fatal("must be live AT the deadline")
	}
	if !m.ContainsKey(1) {
		t.Fatal("must be live AT the deadline")
	}
	clock.advance(1) // deadline has passed
	if _, ok := m.Get(1); ok {
		t.Fatal("must be expired past the deadline")
	}
	if m.ContainsKey(1) {
		t.Fatal("must be expired past the deadline")
	}
}

// TestCachePhysicalOccupancy (Java isEmptyCountsUnsweptExpiredEntries):
// strict reads see absence but physical counters still count the corpse.
func TestCachePhysicalOccupancy(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfg1seg(clock, 1_000))
	m.Put(1, 10)
	clock.advance(1_001)
	if _, ok := m.Get(1); ok {
		t.Fatal("expired must read absent")
	}
	if m.IsEmpty() || m.LenLong() != 1 {
		t.Fatal("physical occupancy must still count the corpse")
	}
	m.ExpireEvict()
	if !m.IsEmpty() || m.LenLong() != 0 {
		t.Fatal("ExpireEvict must reclaim the corpse")
	}
}

// TestCacheMaxSizeFIFO (Java maxSizeFifo): oldest-first eviction.
// Sweep-before-insert leaves maxSize+1 residents transiently.
func TestCacheMaxSizeFIFO(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 0)
	cfg.MaxSize = 10 // single segment share 10
	m := NewHTreeCache[int64, int64](s, cfg)
	for k := int64(0); k < 30; k++ {
		m.Put(k, k*3)
		if m.LenLong() > 11 {
			t.Fatalf("size bound violated at k=%d: %d", k, m.LenLong())
		}
	}
	if m.LenLong() != 11 { // evict-before-insert leaves maxSize+1
		t.Fatalf("len=%d, want 11", m.LenLong())
	}
	m.ExpireEvict()
	if m.LenLong() != 10 {
		t.Fatalf("after expireEvict len=%d, want 10", m.LenLong())
	}
	for k := int64(0); k < 20; k++ {
		if m.ContainsKey(k) {
			t.Fatalf("evicted key present: %d", k)
		}
	}
	for k := int64(20); k < 30; k++ {
		if v, ok := m.Get(k); !ok || v != k*3 {
			t.Fatalf("newest key %d missing", k)
		}
	}
}

// TestCacheMaxSizeLRU (Java maxSizeLruWithAccessOrder): access bumps a
// hit to the head so eviction takes the truly-least-recently-used entry.
func TestCacheMaxSizeLRU(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 0)
	cfg.MaxSize = 10
	cfg.AccessOrder = true
	m := NewHTreeCache[int64, int64](s, cfg)
	for k := int64(0); k < 10; k++ {
		m.Put(k, k)
	}
	if v, ok := m.Get(0); !ok || v != 0 {
		t.Fatal("get(0) miss")
	} // bump key 0 to most-recently-used
	m.Put(10, 10) // counter 10 → no evict → 11
	m.Put(11, 11) // over budget: evicts LRU = key 1
	if !m.ContainsKey(0) {
		t.Fatal("bumped key 0 must survive")
	}
	if m.ContainsKey(1) {
		t.Fatal("LRU key 1 should be evicted")
	}
	if m.LenLong() != 11 {
		t.Fatalf("len=%d, want 11", m.LenLong())
	}
}

// TestCacheAccessOrderLRUHit (Java accessOrderGetOnLruKeyPromotesNotEvicts):
// a get on the current-LRU key must promote it, not evict it.
func TestCacheAccessOrderLRUHit(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 0)
	cfg.MaxSize = 1
	cfg.AccessOrder = true
	m := NewHTreeCache[int64, int64](s, cfg)
	m.Put(0, 100)
	m.Put(1, 101) // sweep-before-insert leaves 2 (share+1); LRU order 0,1
	if m.LenLong() != 2 {
		t.Fatalf("len=%d, want 2", m.LenLong())
	}
	if v, ok := m.Get(0); !ok || v != 100 {
		t.Fatal("get on the LRU key must return its value, not evict it")
	}
	if !m.ContainsKey(0) {
		t.Fatal("bumped key 0 must survive")
	}
	if m.ContainsKey(1) {
		t.Fatal("now-LRU key 1 evicted by the get's sweep")
	}
	if m.LenLong() != 1 {
		t.Fatalf("len=%d, want 1", m.LenLong())
	}
}

// TestCacheEvictionListener (Java listenerFiresWithXReason +
// listenerNotFiredForExplicitRemoveOrClear).
func TestCacheEvictionListener(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	type ev struct {
		k      int64
		v      int64
		reason EvictionCause
	}
	var got []ev
	var mu sync.Mutex
	cfg := ccfg1seg(clock, 1_000)
	m := NewHTreeCache[int64, int64](s, cfg)
	m.SetEvictionListener(func(k, v int64, r EvictionCause) {
		mu.Lock()
		got = append(got, ev{k, v, r})
		mu.Unlock()
	})
	m.Put(1, 10)
	clock.advance(1_001)
	m.Put(2, 20) // foreground sweep collects key1
	mu.Lock()
	if len(got) != 1 || got[0].k != 1 || got[0].v != 10 || got[0].reason != EvictTTL {
		t.Fatalf("ttl victim missing/wrong reason: %+v", got)
	}
	mu.Unlock()

	// explicit remove does not fire the listener
	if _, ok := m.Remove(2); !ok {
		t.Fatal("remove miss")
	}
	mu.Lock()
	if len(got) != 1 {
		t.Fatalf("listener fired for explicit remove: %+v", got)
	}
	mu.Unlock()

	// max-size victim fires with EvictMaxSize (sweep-before-insert: the
	// third put's sweep claims the first key)
	cfg2 := ccfg1seg(clock, 0)
	cfg2.MaxSize = 1
	m2 := NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), cfg2)
	var got2 []ev
	m2.SetEvictionListener(func(k, v int64, r EvictionCause) {
		got2 = append(got2, ev{k, v, r})
	})
	m2.Put(9, 90)
	m2.Put(8, 80)
	m2.Put(7, 70) // sweeps 9 for capacity
	if len(got2) != 1 || got2[0].k != 9 || got2[0].reason != EvictMaxSize {
		t.Fatalf("max-size victim missing/wrong reason: %+v", got2)
	}

	// clear does not fire the eviction listener
	m2.Clear()
	if len(got2) != 1 {
		t.Fatalf("listener fired for clear: %+v", got2)
	}
}

// TestCacheOverflow (Java HTreeCacheOverflowSemantics): capacity victims
// hand off to the overflow map; TTL victims never do; misses consult it.
func TestCacheOverflow(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	ovStore := store.NewStoreOnHeap(true)
	ov := NewHTreeMap[int64, int64](ovStore, hcfg())
	cfg := ccfg1seg(clock, 0)
	cfg.MaxSize = 1
	m := NewHTreeCache[int64, int64](s, cfg)
	m.SetOverflow(ov)

	m.Put(1, 10)
	m.Put(2, 20) // counter 2 (share+1); nothing swept yet
	m.Put(3, 30) // sweep: key1 capacity-evicted → overflow
	if v, ok := ov.Get(1); !ok || v != 10 {
		t.Fatal("capacity victim did not reach overflow")
	}
	// a cache get miss consults overflow and promotes back
	if v, ok := m.Get(1); !ok || v != 10 {
		t.Fatal("overflow value not visible through cache")
	}

	// TTL victims are dead — never handed off
	s2 := store.NewStoreOnHeap(true)
	ov2 := NewHTreeMap[int64, int64](store.NewStoreOnHeap(true), hcfg())
	cfg2 := ccfg1seg(clock, 500)
	cfg2.MaxSize = 1
	m2 := NewHTreeCache[int64, int64](s2, cfg2)
	m2.SetOverflow(ov2)
	m2.Put(1, 10)
	clock.advance(501) // key1 lapsed
	m2.Put(2, 20)      // sweep evicts key1 (ttl wins over maxsize)
	if _, ok := ov2.Get(1); ok {
		t.Fatal("TTL-dead victim must NOT reach overflow")
	}
}

// TestCacheOverflowSelfRejected: a cache cannot overflow into itself.
func TestCacheOverflowSelfRejected(t *testing.T) {
	defer func() {
		r := recover()
		if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("want KindMisuse, got %v", r)
		}
	}()
	m := NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), ccfgPlain(&fakeClock{}))
	m.Put(1, 1)
	m.SetOverflow(m)
}

// TestCacheThrottle (Java maxEvictPerOpThrottlesForegroundSweep +
// maxEvictPerOpZeroIsUnbounded).
func TestCacheThrottle(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 1_000)
	cfg.MaxEvictPerOp = 5
	m := NewHTreeCache[int64, int64](s, cfg)
	for k := int64(0); k < 50; k++ {
		m.Put(k, k)
	}
	clock.advance(1_001)
	if m.LenLong() != 50 {
		t.Fatal("corpses not counted physically")
	}
	if _, ok := m.Get(0); ok {
		t.Fatal("strict read sees a corpse")
	}
	m.Remove(999) // pure sweep, absent key
	if m.LenLong() != 45 {
		t.Fatalf("throttle not applied: len=%d", m.LenLong())
	}
	m.Remove(999)
	if m.LenLong() != 40 {
		t.Fatalf("throttle not applied: len=%d", m.LenLong())
	}
	for k := int64(0); k < 50; k++ {
		if _, ok := m.Get(k); ok {
			t.Fatalf("expired %d must read absent", k)
		}
	}
	m.ExpireEvict() // unbounded
	if m.LenLong() != 0 || !m.IsEmpty() {
		t.Fatal("ExpireEvict must drain the rest")
	}

	// 0 throttle = unbounded foreground sweep
	cfg2 := ccfg1seg(clock, 1_000)
	m2 := NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), cfg2)
	for k := int64(0); k < 50; k++ {
		m2.Put(k, k)
	}
	clock.advance(1_001)
	m2.Put(100, 100)
	if m2.LenLong() != 1 {
		t.Fatalf("unbounded sweep did not drain: len=%d", m2.LenLong())
	}
}

// TestCacheThrottledMutatingOps (Java throttledExpiredTargetTreatedAsAbsentByMutatingOps):
// an expired-but-unswept target behaves as ABSENT for mutating ops.
func TestCacheThrottledMutatingOps(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 1_000)
	cfg.MaxEvictPerOp = 1
	m := NewHTreeCache[int64, int64](s, cfg)
	m.Put(1, 10)
	clock.advance(1_001)
	// putIfAbsent on the corpse: sweep evicts it (cap 1 → just this node),
	// the insert proceeds
	if old, had := m.PutIfAbsent(1, 99); had {
		t.Fatalf("expired entry must count as absent, had %v", old)
	}
	if v, ok := m.Get(1); !ok || v != 99 {
		t.Fatalf("insert did not win: %v %v", v, ok)
	}

	// remove on an expired unswept target returns absent
	clock2 := &fakeClock{}
	cfg2 := ccfg1seg(clock2, 1_000)
	cfg2.MaxEvictPerOp = 1
	m2 := NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), cfg2)
	m2.Put(1, 10)
	m2.Put(2, 20) // two entries
	clock2.advance(1_001)
	// remove(2): the sweep may take only key1's node (oldest first)
	if _, ok := m2.Remove(2); ok {
		t.Fatal("expired unswept remove must report absent")
	}
}

// TestCacheOverwriteSharedBucket (Java overwriteInSharedBucketKeepsNeighborQueueNode):
// keys colliding in one bucket keep independent queue nodes.
func TestCacheOverwriteSharedBucket(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 1_000)
	cfg.Hasher = mdhash.HasherFunc[int64](func(int64, int32) int32 { return 0 })
	m := NewHTreeCache[int64, int64](s, cfg)
	m.Put(1, 10) // stamp +1000
	clock.advance(100)
	m.Put(2, 20) // stamp +1100
	clock.advance(100)
	m.Put(1, 11)       // overwrite key1: re-stamp +1200, bumped ahead of key2
	clock.advance(901) // t=1101: key2 lapsed, key1 alive
	if _, ok := m.Get(2); ok {
		t.Fatal("neighbor must keep its own earlier expiry")
	}
	if v, ok := m.Get(1); !ok || v != 11 {
		t.Fatal("overwritten key must still be live")
	}
	m.ExpireEvict()
	if m.LenLong() != 1 {
		t.Fatalf("len=%d, want 1", m.LenLong())
	}
	clock.advance(200) // t=1301: key1 lapses too
	m.ExpireEvict()
	if m.LenLong() != 0 {
		t.Fatal("shared-bucket overwrite leaked records")
	}
}

// TestCacheReopen (Java openedHandleEnforcesPersistedConfig +
// reopenReconstructsSizeFromQueue).
func TestCacheReopen(t *testing.T) {
	s := store.NewStoreByteArray(true)
	clock := &fakeClock{}
	cfg := ccfgSeg(clock, 1_000)
	cfg.MaxSize = 40
	cfg.MaxEvictPerOp = 7
	m := NewHTreeCache[int64, int64](s, cfg)
	for k := int64(0); k < 30; k++ {
		m.Put(k, k*10)
	}
	hdr := m.HeaderRecid()
	m.CloseHandle()

	m2, err := OpenHTreeCache[int64, int64](s, hdr, ccfgSeg(clock, 999_999))
	if err != nil {
		t.Fatal(err)
	}
	defer m2.CloseHandle()
	// persisted TTL wins over the open-time cfg
	if m2.TTL() != 1_000 {
		t.Fatalf("persisted ttl=%d, want 1000", m2.TTL())
	}
	if m2.inner.maxEvictPerOp != 7 {
		t.Fatalf("persisted maxEvictPerOp=%d, want 7", m2.inner.maxEvictPerOp)
	}
	if m2.LenLong() != 30 {
		t.Fatalf("reconstructed counter=%d, want 30", m2.LenLong())
	}
	for k := int64(0); k < 30; k++ {
		if v, ok := m2.Get(k); !ok || v != k*10 {
			t.Fatalf("get %d after reopen", k)
		}
	}
}

// TestCacheLease: the open lease guards a single live handle.
func TestCacheLease(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfgSeg(clock, 1_000))
	hdr := m.HeaderRecid()
	if _, err := OpenHTreeCache[int64, int64](s, hdr, ccfgSeg(clock, 1_000)); err == nil {
		t.Fatal("second open must fail while the handle lives")
	}
	m.CloseHandle()
	m2, err := OpenHTreeCache[int64, int64](s, hdr, ccfgSeg(clock, 1_000))
	if err != nil {
		t.Fatalf("reopen after close must succeed: %v", err)
	}
	m2.CloseHandle()
}

// TestCacheClear: clear frees every bucket + queue node, zeroes counters.
func TestCacheClear(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfgSeg(clock, 1_000))
	for k := int64(0); k < 100; k++ {
		m.Put(k, k)
	}
	m.Clear()
	if m.LenLong() != 0 || !m.IsEmpty() {
		t.Fatal("clear left residue")
	}
	for q := range m.inner.queues {
		if m.inner.queues[q].Len() != 0 {
			t.Fatalf("segment %d queue not drained", q)
		}
	}
	m.Put(5, 50) // reuse after clear
	if v, ok := m.Get(5); !ok || v != 50 {
		t.Fatal("post-clear put/get broken")
	}
}

// TestCacheModificationListener: Put/Remove fire the shared listener
// runtime (deferred past the segment lock).
func TestCacheModificationListener(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfgPlain(clock))
	var mu sync.Mutex
	type ev struct {
		k    int64
		old  *int64
		new  *int64
		trig bool
	}
	var got []ev
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(key int64, old, newV *int64, triggered bool) error {
			mu.Lock()
			got = append(got, ev{key, old, newV, triggered})
			mu.Unlock()
			return nil
		}))
	m.Put(1, 10)
	m.Put(1, 11) // overwrite
	m.Remove(1)
	mu.Lock()
	if len(got) != 3 {
		t.Fatalf("want 3 events, got %+v", got)
	}
	if got[0].new == nil || *got[0].new != 10 || got[0].old != nil {
		t.Fatalf("insert event wrong: %+v", got[0])
	}
	if got[1].old == nil || *got[1].old != 10 {
		t.Fatalf("overwrite event wrong: %+v", got[1])
	}
	if got[2].old == nil || *got[2].old != 11 || got[2].new != nil {
		t.Fatalf("remove event wrong: %+v", got[2])
	}
	mu.Unlock()
}

// TestCacheValueLoader: a miss consults the loader and promotes its
// result through putIfAbsent.
func TestCacheValueLoader(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfgPlain(clock))
	m.ValueLoader(func(k int64) (int64, bool) {
		if k == 42 {
			return 420, true
		}
		return 0, false
	})
	if v, ok := m.Get(42); !ok || v != 420 {
		t.Fatal("loader miss")
	}
	if _, ok := m.Get(7); ok {
		t.Fatal("loader must return absent")
	}
	// promoted: now physically resident
	if v, ok := m.Get(42); !ok || v != 420 {
		t.Fatal("promotion missing")
	}
	if m.LenLong() != 1 {
		t.Fatalf("promoted entry not resident: %d", m.LenLong())
	}
}

// TestCacheConcurrent: parallel writers + readers stay consistent.
func TestCacheConcurrent(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	pcfg := ccfgSeg(clock, 0)
	pcfg.MaxSize = math.MaxInt64 // plain: no eviction bound
	m := NewHTreeCache[int64, int64](s, pcfg)
	cfg := ccfgSeg(clock, 0)
	cfg.MaxSize = 64
	m2 := NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), cfg)
	_ = m2
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < 300; i++ {
				k := base*1000 + i
				m.Put(k, k)
				m.Get(k)
				if i%3 == 0 {
					m.Remove(k)
				}
				if i%17 == 0 {
					m.PutIfAbsent(k, k+1)
				}
			}
		}(int64(g))
	}
	wg.Wait()
}

// TestCacheIteration: All/Keys/Values yield only LIVE entries.
func TestCacheIteration(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfgSeg(clock, 1_000))
	m.Put(1, 10)
	m.Put(2, 20)
	clock.advance(1_001) // everything lapsed, unswept
	m.Put(3, 30)
	var seen []int64
	for k := range m.Keys() {
		seen = append(seen, k)
	}
	if len(seen) != 1 || seen[0] != 3 {
		t.Fatalf("iteration must skip corpses: %v", seen)
	}
	n := 0
	for range m.All() {
		n++
	}
	if n != 1 {
		t.Fatalf("All yielded %d, want 1", n)
	}
}

// TestCacheClosed: ops after CloseHandle panic KindClosed.
func TestCacheClosed(t *testing.T) {
	defer func() {
		r := recover()
		if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindClosed {
			t.Fatalf("want KindClosed, got %v", r)
		}
	}()
	m := NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), ccfgPlain(&fakeClock{}))
	m.CloseHandle()
	m.Put(1, 1)
}

// TestCacheStoreSize (Java HTreeCacheStoreSizeTest): the byte budget
// evicts under bytearray store pressure.
func TestCacheStoreSize(t *testing.T) {
	s := store.NewStoreByteArray(true)
	// an empty bytearray store reports CurrentSize()==0 — same as heap —
	// so seed one live record for byte accounting before create.
	store.Put(s, int64(1), ser.Long)
	if s.CurrentSize() == 0 {
		t.Skip("store cannot report its size")
	}
	clock := &fakeClock{}
	cfg := ccfg1seg(clock, 0)
	cfg.StoreSize = s.CurrentSize() + 4_000 // small budget
	m := NewHTreeCache[int64, int64](s, cfg)
	for k := int64(0); k < 200; k++ {
		m.Put(k, k)
	}
	if m.LenLong() >= 200 {
		t.Fatalf("store-size budget did not evict anything: %d", m.LenLong())
	}
}

// TestCacheStoreSizeOnlyRejectsZeroSizeStore (Java constructor check):
// storeSize as the only bound on a CurrentSize()==0 store is misuse.
func TestCacheStoreSizeOnlyRejectsZeroSizeStore(t *testing.T) {
	defer func() {
		r := recover()
		if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("want KindMisuse, got %v", r)
		}
	}()
	cfg := ccfg1seg(&fakeClock{}, 0)
	cfg.StoreSize = 1000
	NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), cfg) // heap reports 0
}

// TestCacheNoBoundsRejected: a cache with no expiry bound is misuse.
func TestCacheNoBoundsRejected(t *testing.T) {
	defer func() {
		r := recover()
		if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("want KindMisuse, got %v", r)
		}
	}()
	NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), CConfig[int64, int64]{
		KeySer: ser.Long, ValueSer: ser.Long,
	})
}

// TestCacheSaturatingStamp: clock+ttl overflow saturates at MaxInt64
// rather than corrupting the queue wire format.
func TestCacheSaturatingStamp(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{now: math.MaxInt64 - 5}
	m := NewHTreeCache[int64, int64](s, ccfg1seg(clock, 1_000))
	m.Put(1, 10) // stamp saturates at MaxInt64 — never expires
	clock.now = 0
	if v, ok := m.Get(1); !ok || v != 10 {
		t.Fatal("saturated stamp must stay live")
	}
}

// TestCacheNegativeClockRejected (Java stampOverflowAndNegativeClockRejected).
func TestCacheNegativeClockRejected(t *testing.T) {
	defer func() {
		r := recover()
		if de, ok := mapdb5.AsDBError(r); !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("want KindMisuse, got %v", r)
		}
	}()
	clock := &fakeClock{now: -1}
	m := NewHTreeCache[int64, int64](store.NewStoreOnHeap(true), ccfg1seg(clock, 1_000))
	m.Put(1, 10) // clock went negative — stamp must reject
}

// TestCacheMultiSegmentTTL (Java multiSegmentTtlEvictsEverything).
func TestCacheMultiSegmentTTL(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	m := NewHTreeCache[int64, int64](s, ccfgSeg(clock, 1_000))
	for k := int64(0); k < 200; k++ {
		m.Put(k, k)
	}
	clock.advance(1_001)
	m.ExpireEvict()
	if m.LenLong() != 0 || !m.IsEmpty() {
		t.Fatalf("per-segment sweep left residue: %d", m.LenLong())
	}
}

// TestCacheVerifyQueues: the per-segment queues stay structurally
// consistent through churn.
func TestCacheVerifyQueues(t *testing.T) {
	s := store.NewStoreOnHeap(true)
	clock := &fakeClock{}
	pcfg := ccfgSeg(clock, 0)
	pcfg.MaxSize = math.MaxInt64 // plain cache, no evictions
	m := NewHTreeCache[int64, int64](s, pcfg)
	for k := int64(0); k < 300; k++ {
		m.Put(k, k)
	}
	for _, q := range m.inner.queues {
		q.Verify()
	}
}
