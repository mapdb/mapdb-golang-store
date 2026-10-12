package btree

// Listener tests, ported from Java BTreeMapCounterListenerTest (listener
// half), BTreeMapSyncListenerTest and BTreeMapExternalValuesTest. A
// listener failure is surfaced as a panic carrying the listener's error
// (Java rethrows the throwable; Go map ops are panic-based).

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func newLMap(s store.Store, maxNodeSize int, counter bool) *BTreeMap[int64, int64] {
	return NewBTreeMap[int64, int64](s, Config[int64, int64]{
		KeyFormat:   ser.LongFormat,
		ValueFormat: ser.LongFormat,
		MaxNodeSize: maxNodeSize,
		Counter:     counter,
	})
}

type levent struct {
	key  int64
	old  *int64
	new  *int64
	trig bool
}

func i64p(v int64) *int64 { return &v }

// recorder collects events; safe for listeners because each event is
// delivered on the mutating goroutine.
type recorder struct {
	mu     sync.Mutex
	events []levent
}

func (r *recorder) modify(key int64, o, n *int64, t bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := levent{key: key, trig: t}
	if o != nil {
		v := *o
		e.old = &v
	}
	if n != nil {
		v := *n
		e.new = &v
	}
	r.events = append(r.events, e)
	return nil
}

func (r *recorder) Modify(key int64, o, n *int64, t bool) error { return r.modify(key, o, n, t) }

func (r *recorder) snapshot() []levent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]levent(nil), r.events...)
}

func checkEvent(t *testing.T, e levent, key int64, old, new *int64) {
	t.Helper()
	if e.key != key {
		t.Fatalf("event key = %d, want %d", e.key, key)
	}
	if (e.old == nil) != (old == nil) || (old != nil && *e.old != *old) {
		t.Fatalf("event old = %v, want %v", e.old, old)
	}
	if (e.new == nil) != (new == nil) || (new != nil && *e.new != *new) {
		t.Fatalf("event new = %v, want %v", e.new, new)
	}
	if e.trig {
		t.Fatalf("triggered flag set on a user mutation")
	}
}

// TestListenerInsertUpdateRemove — Java listenerInsertUpdateRemove.
func TestListenerInsertUpdateRemove(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	rec := &recorder{}
	m.ModificationListenerAdd(rec)

	m.Put(5, 50) // insert: old=nil new=50
	m.Put(5, 60) // update: old=50 new=60
	m.Remove(5)  // remove: old=60 new=nil

	ev := rec.snapshot()
	if len(ev) != 3 {
		t.Fatalf("events = %d, want 3", len(ev))
	}
	checkEvent(t, ev[0], 5, nil, i64p(50))
	checkEvent(t, ev[1], 5, i64p(50), i64p(60))
	checkEvent(t, ev[2], 5, i64p(60), nil)
}

// TestListenerReplaceAndConditionalOps — Java
// listenerReplaceAndConditionalOps: failed conditional ops fire nothing.
func TestListenerReplaceAndConditionalOps(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	rec := &recorder{}
	m.ModificationListenerAdd(rec)

	m.Put(1, 10)
	m.Replace(1, 20)
	m.ReplaceIfEquals(1, 20, 30)
	// failed conditional ops fire NOTHING:
	m.ReplaceIfEquals(1, 999, 40)
	m.Replace(2, 5)
	m.PutIfAbsent(1, 77)
	m.RemoveIfEquals(1, 999)
	m.PutIfAbsent(2, 22)
	if !m.RemoveIfEquals(2, 22) {
		t.Fatal("conditional remove failed")
	}

	ev := rec.snapshot()
	if len(ev) != 5 {
		t.Fatalf("events = %d, want 5: %v", len(ev), ev)
	}
	checkEvent(t, ev[0], 1, nil, i64p(10))
	checkEvent(t, ev[1], 1, i64p(10), i64p(20))
	checkEvent(t, ev[2], 1, i64p(20), i64p(30))
	checkEvent(t, ev[3], 2, nil, i64p(22))
	checkEvent(t, ev[4], 2, i64p(22), nil)
}

// TestListenerClearFiresRemovalForEveryEntry — Java
// listenerClearFiresRemovalForEveryEntry.
func TestListenerClearFiresRemovalForEveryEntry(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 4, true)
	rec := &recorder{}
	m.ModificationListenerAdd(rec)

	for i := int64(0); i < 25; i++ {
		m.Put(i, i*10)
	}
	rec.events = nil
	m.Clear()

	ev := rec.snapshot()
	if len(ev) != 25 {
		t.Fatalf("events = %d, want 25", len(ev))
	}
	for i, e := range ev {
		checkEvent(t, e, int64(i), i64p(int64(i)*10), nil)
	}
	if m.LenLong() != 0 {
		t.Fatalf("sizeLong = %d, want 0", m.LenLong())
	}
}

// TestListenerMultipleAllFire — Java multipleListenersAllFire.
func TestListenerMultipleAllFire(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	var a, b atomic.Int64
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error { a.Add(1); return nil }))
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error { b.Add(1); return nil }))

	m.Put(1, 1)
	m.Put(1, 2)
	m.Remove(1)
	if a.Load() != 3 || b.Load() != 3 {
		t.Fatalf("counts = %d,%d want 3,3", a.Load(), b.Load())
	}
}

// TestListenerAddIfAbsentDedup — Java CopyOnWriteArrayList.addIfAbsent:
// registering the same listener twice delivers each event once.
func TestListenerAddIfAbsentDedup(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	rec := &recorder{}
	m.ModificationListenerAdd(rec)
	m.ModificationListenerAdd(rec) // duplicate registration: no-op

	m.Put(1, 1)
	if ev := rec.snapshot(); len(ev) != 1 {
		t.Fatalf("events = %d, want 1 (deduped registration)", len(ev))
	}
}

// TestListenerRemoveStopsEvents — Java modificationListenerRemove.
func TestListenerRemoveStopsEvents(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	rec := &recorder{}
	syncRec := &syncRecorder{}
	m.ModificationListenerAdd(rec)
	m.ModificationListenerAdd(syncRec)

	m.Put(1, 1)
	m.ModificationListenerRemove(rec)
	m.ModificationListenerRemove(syncRec)
	m.Put(2, 2)

	if ev := rec.snapshot(); len(ev) != 1 {
		t.Fatalf("deferred events = %d, want 1", len(ev))
	}
	if ev := syncRec.snapshot(); len(ev) != 1 {
		t.Fatalf("sync events = %d, want 1", len(ev))
	}
}

// TestListenerConcurrentEventCountMatchesOps — Java
// listenerConcurrentEventCountMatchesOps: every committed insert fires
// exactly one insert event (old == nil).
func TestListenerConcurrentEventCountMatchesOps(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 6, true)
	rec := &recorder{}
	m.ModificationListenerAdd(rec)

	const threads, perThread = 6, 1000
	var wg, ready sync.WaitGroup
	ready.Add(threads)
	start := make(chan struct{})
	for w := 0; w < threads; w++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			ready.Done()
			<-start
			for i := int64(0); i < perThread; i++ {
				m.Put(base+i, base+i)
			}
		}(int64(w) * perThread)
	}
	ready.Wait()
	close(start)
	wg.Wait()

	ev := rec.snapshot()
	want := threads * perThread
	if len(ev) != want {
		t.Fatalf("events = %d, want %d", len(ev), want)
	}
	for _, e := range ev {
		if e.old != nil {
			t.Fatalf("insert event has non-nil oldValue: %v", e)
		}
	}
	if m.LenLong() != int64(want) {
		t.Fatalf("sizeLong = %d, want %d", m.LenLong(), want)
	}
}

// ===================== synchronous listeners =====================

// syncRecorder is a synchronous listener (marker implemented).
type syncRecorder struct {
	recorder
	// checkCommit, when non-nil, runs INSIDE the under-lock fire — used to
	// prove the mutation is already committed/visible at sync-fire time.
	checkCommit func(key int64)
}

func (r *syncRecorder) Modify(key int64, o, n *int64, t bool) error {
	if r.checkCommit != nil {
		r.checkCommit(key)
	}
	return r.modify(key, o, n, t)
}

func (r *syncRecorder) Synchronous() {}

// TestSyncListenerSeesCommittedMutation — the sync fire point is AFTER the
// mutation committed but BEFORE the leaf lock releases: a sync listener
// doing an (unlocked) Get sees the new value on insert/update and absence
// on remove.
func TestSyncListenerSeesCommittedMutation(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, true)
	rec := &syncRecorder{}
	m.ModificationListenerAdd(rec) // classified sync via the marker

	var seen []string
	rec.checkCommit = func(key int64) {
		v, ok := m.Get(key)
		seen = append(seen, fmt.Sprintf("%d=%v,%v", key, v, ok))
	}
	m.Put(1, 10)
	m.Put(1, 20)
	m.Remove(1)

	want := []string{"1=10,true", "1=20,true", "1=0,false"}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("sync-time observations = %v, want %v", seen, want)
	}
}

// armedThrow fails once when armed, then disarms (Java ArmedThrow).
type armedThrow struct {
	armed atomic.Bool
	boom  error
}

func (l *armedThrow) Modify(k int64, o, n *int64, t bool) error {
	if l.armed.Swap(false) {
		return l.boom
	}
	return nil
}
func (l *armedThrow) Synchronous() {}

var errBoom = errors.New("listener boom")

// expectBoom asserts op panics carrying the listener's error (Java
// rethrows; Go map ops are panic-based).
func expectBoom(t *testing.T, l *armedThrow, op func()) {
	t.Helper()
	l.armed.Store(true)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("armed listener did not fail")
		}
		err, _ := r.(error)
		if err == nil || !errors.Is(err, errBoom) {
			t.Fatalf("wrong panic: %v", r)
		}
	}()
	op()
}

// assertCompletes runs op in a fresh goroutine with a BOUNDED wait: a
// leaked node lock spins forever, and this must fail the test rather than
// hang the suite (Java assertCompletes).
func assertCompletes(t *testing.T, op func()) {
	t.Helper()
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		op()
	}()
	select {
	case r := <-done:
		if r != nil {
			t.Fatalf("operation panicked: %v", r)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("operation did not complete within 30s (leaked node lock?)")
	}
}

func assertConsistent(t *testing.T, m *BTreeMap[int64, int64], want int64) {
	t.Helper()
	if got := m.LenLong(); got != want {
		t.Fatalf("counter desynced: sizeLong = %d, want %d", got, want)
	}
	var n int64
	for range m.All() {
		n++
	}
	if n != want {
		t.Fatalf("counter vs iteration: %d iterated, want %d", n, want)
	}
}

// drillSyncThrow runs the Java BTreeMapSyncListenerTest drill on a map:
// put-update, non-split insert, SPLITTING insert, remove and replace fire
// points, each with a throwing sync listener — the tree must stay fully
// operable and the counter consistent afterwards.
func drillSyncThrow(t *testing.T, m *BTreeMap[int64, int64]) {
	t.Helper()
	listener := &armedThrow{boom: errBoom}
	m.ModificationListenerAdd(listener)
	m.Put(0, 0)
	m.Put(1, 1)

	// non-split insert (leaf below maxNodeSize=4)
	expectBoom(t, listener, func() { m.Put(2, 2) })
	assertConsistent(t, m, 3)
	assertCompletes(t, func() { m.Put(2, 22) }) // SAME leaf: lock must not leak
	if v, ok := m.Get(2); !ok || v != 22 {
		t.Fatalf("get(2) = %v,%v", v, ok)
	}

	// put over an existing key (update branch)
	expectBoom(t, listener, func() { m.Put(0, 100) })
	if v, _ := m.Get(0); v != 100 {
		t.Fatalf("get(0) = %v", v)
	}
	assertConsistent(t, m, 3)
	assertCompletes(t, func() { m.Put(0, 101) })

	// SPLITTING insert: the 5th key overflows maxNodeSize=4 and splits the
	// ROOT leaf. The listener failure surfaces only AFTER separator/root
	// propagation completes, so the tree stays operable — including a LATER
	// split of the right half B, which under a skipped root propagation
	// would spin forever on the never-created level-1 left edge.
	m.Put(3, 3)
	expectBoom(t, listener, func() { m.Put(4, 4) })
	assertConsistent(t, m, 5)
	assertCompletes(t, func() { m.Put(5, 5) }) // fills B
	assertCompletes(t, func() { m.Put(6, 6) }) // overflows B: B's own split
	assertCompletes(t, func() { m.Put(7, 7) })
	assertConsistent(t, m, 8)
	m.Remove(6)
	m.Remove(7)
	assertConsistent(t, m, 6)

	// remove
	expectBoom(t, listener, func() { m.Remove(5) })
	if _, ok := m.Get(5); ok {
		t.Fatal("get(5) after remove")
	}
	assertConsistent(t, m, 5)
	assertCompletes(t, func() { m.Put(5, 55) })
	assertConsistent(t, m, 6)

	// replace
	expectBoom(t, listener, func() { m.Replace(1, 111) })
	if v, _ := m.Get(1); v != 111 {
		t.Fatalf("get(1) = %v", v)
	}
	assertConsistent(t, m, 6)
	assertCompletes(t, func() { m.Remove(1) })
	assertConsistent(t, m, 5)
}

// TestThrowingSyncListenerLeavesInlineMapUsable — Java
// throwingListenerLeavesInlineMapUsable.
func TestThrowingSyncListenerLeavesInlineMapUsable(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 4, true)
	drillSyncThrow(t, m)
	m.Verify()
}

// TestThrowingSyncListenerLeavesExternalValueMapUsable — Java
// throwingListenerLeavesExternalValueMapUsable.
func TestThrowingSyncListenerLeavesExternalValueMapUsable(t *testing.T) {
	m := NewBTreeMap[int64, int64](store.NewStoreOnHeap(true), Config[int64, int64]{
		KeyFormat:      ser.LongFormat,
		ValueFormat:    ser.LongFormat,
		MaxNodeSize:    4,
		Counter:        true,
		ExternalValues: true,
	})
	drillSyncThrow(t, m)
	m.Verify()
}

// TestPanickingSyncListenerOnRootSplitDoesNotHangLaterBSplit — the Rust
// port's panic variant: a PANICKING sync listener is caught, converted to
// a listener error, and must not skip separator/root propagation.
func TestPanickingSyncListenerOnRootSplitDoesNotHangLaterBSplit(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 4, true)
	var armed atomic.Bool
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error {
			if armed.Swap(false) {
				panic("listener kaboom")
			}
			return nil
		}))

	for i := int64(0); i < 4; i++ {
		m.Put(i, i)
	} // root leaf full (maxNodeSize=4)

	armed.Store(true)
	func() {
		defer func() {
			r := recover()
			err, _ := r.(error)
			if err == nil || !strings.Contains(err.Error(), "kaboom") {
				t.Fatalf("wrong panic: %v", r)
			}
		}()
		m.Put(4, 4)
	}()
	if v, ok := m.Get(4); !ok || v != 4 {
		t.Fatalf("insert not committed: get(4) = %v,%v", v, ok)
	}
	assertConsistent(t, m, 5)

	// Force B to split: under a skipped root propagation this spins forever
	// on the missing level-1 left edge.
	for i := int64(5); i < 12; i++ {
		i := i
		assertCompletes(t, func() { m.Put(i, i) })
	}
	assertConsistent(t, m, 12)
	m.Verify()
}

// TestThrowingSyncListenerStillDeliversToLaterSyncListener — Java
// throwingSyncListenerStillDeliversToLaterSyncListener.
func TestThrowingSyncListenerStillDeliversToLaterSyncListener(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 4, true)
	later := &syncRecorder{}
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error { return errBoom }))
	m.ModificationListenerAdd(later)

	func() {
		defer func() {
			r := recover()
			err, _ := r.(error)
			if err == nil || !errors.Is(err, errBoom) {
				t.Fatalf("wrong panic: %v", r)
			}
		}()
		m.Put(1, 10)
	}()
	ev := later.snapshot()
	if len(ev) != 1 {
		t.Fatalf("later sync listener events = %d, want 1", len(ev))
	}
	checkEvent(t, ev[0], 1, nil, i64p(10))
	if v, ok := m.Get(1); !ok || v != 10 {
		t.Fatal("mutation rolled back by listener failure")
	}
}

// TestThrowingDeferredListenerStillDeliversToLaterListener — Java
// throwingDeferredListenerStillDeliversToLaterListener.
func TestThrowingDeferredListenerStillDeliversToLaterListener(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 4, true)
	later := &recorder{}
	m.ModificationListenerAdd(mapdb5.ModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error { return errBoom }))
	m.ModificationListenerAdd(later)

	func() {
		defer func() {
			r := recover()
			err, _ := r.(error)
			if err == nil || !errors.Is(err, errBoom) {
				t.Fatalf("wrong panic: %v", r)
			}
		}()
		m.Put(1, 10)
	}()
	ev := later.snapshot()
	if len(ev) != 1 {
		t.Fatalf("later listener events = %d, want 1", len(ev))
	}
	checkEvent(t, ev[0], 1, nil, i64p(10))
	if v, ok := m.Get(1); !ok || v != 10 {
		t.Fatal("mutation rolled back by listener failure")
	}
}

// TestSyncFailureSkipsDeferredFire — Java: a sync throw inside the locked
// section propagates, skipping this op's deferred fireModified.
func TestSyncFailureSkipsDeferredFire(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 4, true)
	deferred := &recorder{}
	m.ModificationListenerAdd(deferred)
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error { return errBoom }))

	func() {
		defer func() { recover() }()
		m.Put(1, 10)
	}()
	if ev := deferred.snapshot(); len(ev) != 0 {
		t.Fatalf("deferred listener fired after a sync failure: %v", ev)
	}
	// Mutation still committed.
	if v, ok := m.Get(1); !ok || v != 10 {
		t.Fatal("mutation rolled back")
	}
}

// TestSyncListenerUnderLockOrdering — same-key mutations serialize at the
// leaf lock, so a sync listener (fired under that lock) sees the events in
// leaf-mutation order: each event's oldValue links to the previous new.
func TestSyncListenerUnderLockOrdering(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	rec := &syncRecorder{}
	m.ModificationListenerAdd(rec)

	m.Put(1, 10)
	m.Put(1, 20)
	m.Put(1, 30)
	m.Remove(1)
	ev := rec.snapshot()
	if len(ev) != 4 {
		t.Fatalf("events = %d, want 4", len(ev))
	}
	checkEvent(t, ev[0], 1, nil, i64p(10))
	checkEvent(t, ev[1], 1, i64p(10), i64p(20))
	checkEvent(t, ev[2], 1, i64p(20), i64p(30))
	checkEvent(t, ev[3], 1, i64p(30), nil)
}

// ---- review regression coverage (m7 findings) ----

// reentryListener re-enters the map inside a synchronous callback — on an
// external-value remove the barrier is held, so re-entry must trip the
// fail-fast check (and never deadlock) rather than reach the store.
type reentryListener struct {
	m   *BTreeMap[int64, int64]
	op  string
	ran atomic.Bool
}

func (l *reentryListener) Modify(key int64, o, n *int64, t bool) error {
	l.ran.Store(true)
	switch l.op {
	case "remove":
		l.m.Remove(key)
	case "get":
		l.m.Get(key)
	case "iterate":
		for range l.m.Keys() {
		}
	}
	return nil
}
func (l *reentryListener) Synchronous() {}

// runRemoveBounded runs m.Remove(1) with a deadline — a tripwire bug shows
// up as a deadlock, which the timeout surfaces instead of hanging.
func runRemoveBounded(t *testing.T, m *BTreeMap[int64, int64], l *reentryListener) (panicked any) {
	t.Helper()
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		m.Remove(1)
		done <- nil
	}()
	select {
	case r := <-done:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("external remove with a reentrant listener deadlocked")
		return nil
	}
}

// TestExternalSyncListenerReentryRemove — a sync listener calling Remove on
// an external-value map must fail fast (listener error surfaces), later
// listeners still run, and the map stays usable (finding 1 regression).
func TestExternalSyncListenerReentryRemove(t *testing.T) {
	m := NewBTreeMap[int64, int64](store.NewStoreOnHeap(true), Config[int64, int64]{
		KeyFormat: ser.LongFormat, ValueFormat: ser.LongFormat,
		MaxNodeSize: 8, ExternalValues: true,
	})
	m.Put(1, 10)
	l := &reentryListener{m: m, op: "remove"}
	second := &syncRecorder{}
	m.ModificationListenerAdd(l)
	m.ModificationListenerAdd(second)

	if r := runRemoveBounded(t, m, l); r == nil {
		t.Fatal("expected the reentry panic to surface from Remove")
	}
	if !l.ran.Load() {
		t.Fatal("reentrant listener never fired")
	}
	if ev := second.snapshot(); len(ev) == 0 {
		t.Fatal("listener after the failing reentrant one did not run")
	}
	// The map must remain usable — the barrier and leaf lock were released.
	m.ModificationListenerRemove(l)
	m.Put(2, 20)
	if v, ok := m.Get(2); !ok || v != 20 {
		t.Fatalf("map unusable after reentry panic: v=%d ok=%v", v, ok)
	}
}

// TestExternalSyncListenerReentryGet — a sync listener calling Get on an
// external-value map trips the same tripwire (read barrier path).
func TestExternalSyncListenerReentryGet(t *testing.T) {
	m := NewBTreeMap[int64, int64](store.NewStoreOnHeap(true), Config[int64, int64]{
		KeyFormat: ser.LongFormat, ValueFormat: ser.LongFormat,
		MaxNodeSize: 8, ExternalValues: true,
	})
	m.Put(1, 10)
	l := &reentryListener{m: m, op: "get"}
	m.ModificationListenerAdd(l)
	if r := runRemoveBounded(t, m, l); r == nil {
		t.Fatal("expected the reentry panic to surface from Remove")
	}
	// The remove committed before listeners fired — the key is gone and
	// the map stays usable.
	if _, ok := m.Get(1); ok {
		t.Fatal("key still present after a committed remove")
	}
	m.ModificationListenerRemove(l)
	m.Put(3, 30)
	if v, ok := m.Get(3); !ok || v != 30 {
		t.Fatalf("map unusable after reentry panic: v=%d ok=%v", v, ok)
	}
}

// TestExternalSyncListenerReentryIterate — a listener iterating the map
// re-enters through the iterator's read barrier (same tripwire).
func TestExternalSyncListenerReentryIterate(t *testing.T) {
	m := NewBTreeMap[int64, int64](store.NewStoreOnHeap(true), Config[int64, int64]{
		KeyFormat: ser.LongFormat, ValueFormat: ser.LongFormat,
		MaxNodeSize: 8, ExternalValues: true,
	})
	m.Put(1, 10)
	m.Put(2, 20)
	l := &reentryListener{m: m, op: "iterate"}
	m.ModificationListenerAdd(l)
	if r := runRemoveBounded(t, m, l); r == nil {
		t.Fatal("expected the reentry panic to surface from Remove")
	}
}

// valueMangler is a synchronous listener that writes through the event's
// optional-value pointers — it must NOT be able to rewrite the event other
// listeners see nor the operation's return value (finding 2 regression).
type valueMangler struct{}

func (valueMangler) Modify(key int64, o, n *int64, t bool) error {
	if o != nil {
		*o = 111
	}
	if n != nil {
		*n = 222
	}
	return nil
}
func (valueMangler) Synchronous() {}

// TestListenerValueIsolation — a listener mutating through oldValue/
// newValue must not affect the next listener's event or the operation's
// return value (Java delivers the observed/applied values).
func TestListenerValueIsolation(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	m.Put(1, 10)
	m.ModificationListenerAdd(valueMangler{})
	rec := &syncRecorder{}
	m.ModificationListenerAdd(rec)
	deferred := &recorder{}
	m.ModificationListenerAdd(deferred)

	old, had := m.Put(1, 20)
	if !had || old != 10 {
		t.Fatalf("Put return = (%d,%v), want (10,true) — listener rewrote the op's captured value", old, had)
	}
	// The sync recorder (registered AFTER the mangler) must see the real
	// event, and so must the deferred phase.
	if ev := rec.snapshot(); len(ev) != 1 {
		t.Fatalf("sync events = %d, want 1", len(ev))
	} else {
		checkEvent(t, ev[0], 1, i64p(10), i64p(20))
	}
	if ev := deferred.snapshot(); len(ev) != 1 {
		t.Fatalf("deferred events = %d, want 1", len(ev))
	} else {
		checkEvent(t, ev[0], 1, i64p(10), i64p(20))
	}
	if v, _ := m.Get(1); v != 20 {
		t.Fatalf("stored value = %d, want 20", v)
	}
}

// TestListenerValueIsolationRemove — same isolation on the remove path.
func TestListenerValueIsolationRemove(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)
	m.Put(1, 10)
	m.ModificationListenerAdd(valueMangler{})
	rec := &syncRecorder{}
	m.ModificationListenerAdd(rec)

	old, ok := m.Remove(1)
	if !ok || old != 10 {
		t.Fatalf("Remove return = (%d,%v), want (10,true)", old, ok)
	}
	if ev := rec.snapshot(); len(ev) != 1 {
		t.Fatalf("sync events = %d, want 1", len(ev))
	} else {
		checkEvent(t, ev[0], 1, i64p(10), nil)
	}
}

// ifaceFieldListener is a comparable-DECLARED struct whose interface field
// holds an uncomparable dynamic value — Type.Comparable() is true but ==
// would panic (finding 3 regression).
type ifaceFieldListener struct{ state any }

func (ifaceFieldListener) Modify(key int64, o, n *int64, t bool) error { return nil }

// sliceListener is a named slice type implementing Modify — non-comparable,
// non-pointer: no stable identity.
type sliceListener []int

func (sliceListener) Modify(key int64, o, n *int64, t bool) error { return nil }

// TestListenerIdentityDomain — listeners without a stable identity are
// rejected at registration rather than silently never deduping/removing
// (finding 3 regression).
func TestListenerIdentityDomain(t *testing.T) {
	m := newLMap(store.NewStoreOnHeap(true), 8, false)

	// Comparable-declared type with an uncomparable dynamic value.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("iface-field listener with uncomparable dynamic value was accepted")
			}
		}()
		m.ModificationListenerAdd(ifaceFieldListener{state: []int{1}})
	}()
	// A comparable dynamic value of the same type is fine.
	m.ModificationListenerAdd(ifaceFieldListener{state: 42})

	// Named slice: no stable identity.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("named-slice listener was accepted")
			}
		}()
		m.ModificationListenerAdd(sliceListener{1})
	}()

	// Pointer and func listeners still register, dedup, and remove.
	rec := &recorder{}
	m.ModificationListenerAdd(rec)
	m.ModificationListenerAdd(rec) // no-op dedup
	m.ModificationListenerRemove(rec)
	m.Put(9, 9)
	if ev := rec.snapshot(); len(ev) != 0 {
		t.Fatalf("removed listener still fired: %v", ev)
	}
}
