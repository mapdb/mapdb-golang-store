package queue

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func newHeap() store.Store      { return store.NewStoreOnHeap(true) }
func newByteArray() store.Store { return store.NewStoreByteArray(true) }

func mustCorrupt(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected DataCorruption panic")
		}
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Fatalf("wrong panic: %v", r)
		}
	}()
	f()
}

// ---------- QueueLong ----------

func TestQueueLongNodeGolden(t *testing.T) {
	// Node(prev=0, next=5, ts=10, value=1): pack(0)=0x80, 5=0x85, 10=0x8A, 1=0x81.
	b := ser.SerializeToBytes(LNodeSer, LNode{Prev: 0, Next: 5, Timestamp: 10, Value: 1})
	if !slices.Equal(b, []byte{0x80, 0x85, 0x8A, 0x81}) {
		t.Fatalf("golden mismatch: %x", b)
	}
	// multi-byte: prev=200 → [0x01,0xC8], next=1 → 0x81, ts=128 → [0x01,0x80], value=300 → [0x02,0xAC]
	n := LNode{Prev: 200, Next: 1, Timestamp: 128, Value: 300}
	b2 := ser.SerializeToBytes(LNodeSer, n)
	if !slices.Equal(b2, []byte{0x01, 0xC8, 0x81, 0x01, 0x80, 0x02, 0xAC}) {
		t.Fatalf("golden2 mismatch: %x", b2)
	}
	got := LNodeSer.Deserialize(bin.NewReader(b2), len(b2))
	if got != n {
		t.Fatalf("round-trip: %+v", got)
	}
}

func TestQueueLongFIFORemoveBumpReopen(t *testing.T) {
	for name, mk := range map[string]func() store.Store{"heap": newHeap, "bytearray": newByteArray} {
		t.Run(name, func(t *testing.T) {
			s := mk()
			q, err := NewQueueLong(s)
			if err != nil {
				t.Fatal(err)
			}
			a := q.Put(10, 1)
			b := q.Put(20, 2)
			c := q.Put(30, 3)
			if !slices.Equal(q.Values(), []uint64{1, 2, 3}) {
				t.Fatalf("values %v", q.Values())
			}
			if v := q.Remove(b, true); v.Value != 2 {
				t.Fatalf("removed %+v", v)
			}
			if !slices.Equal(q.Values(), []uint64{1, 3}) {
				t.Fatalf("values %v", q.Values())
			}
			q.Bump(a, 40)
			if !slices.Equal(q.Values(), []uint64{3, 1}) {
				t.Fatalf("values %v", q.Values())
			}
			q.Verify()
			r, err := OpenQueueLong(s, q.TailRecid(), q.HeadRecid(), q.HeadPrevRecid())
			if err != nil {
				t.Fatal(err)
			}
			if r.Tail() != uint64(c) {
				t.Fatalf("tail %d", r.Tail())
			}
			if n, ok := r.Take(); !ok || n.Value != 3 {
				t.Fatalf("take %+v %v", n, ok)
			}
			if n, ok := r.Take(); !ok || n.Value != 1 {
				t.Fatalf("take %+v %v", n, ok)
			}
			if _, ok := r.Take(); ok {
				t.Fatal("take on empty")
			}
			r.Verify()
		})
	}
}

func TestQueueLongTakeUntilForEach(t *testing.T) {
	s := newHeap()
	q, _ := NewQueueLong(s)
	q.Put(1, 10)
	q.Put(2, 20)
	q.Put(3, 30)
	var seen []uint64
	q.TakeUntil(func(_ store.Recid, n LNode) bool {
		seen = append(seen, n.Value)
		return n.Value <= 20 // consume while true; inspects 30, keeps it
	})
	if !slices.Equal(seen, []uint64{10, 20, 30}) {
		t.Fatalf("takeUntil saw %v", seen)
	}
	if !slices.Equal(q.Values(), []uint64{30}) {
		t.Fatalf("remaining %v", q.Values())
	}
	var fVals []uint64
	q.ForEach(func(_ store.Recid, v, ts uint64) {
		fVals = append(fVals, v)
	})
	if !slices.Equal(fVals, []uint64{30}) {
		t.Fatalf("forEach %v", fVals)
	}
	q.Clear()
	if q.Len() != 0 {
		t.Fatal("clear left nodes")
	}
	q.Verify()
}

func TestQueueLongPutPreallocated(t *testing.T) {
	s := newHeap()
	q, _ := NewQueueLong(s)
	r1 := s.Preallocate()
	q.PutPreallocated(5, 100, r1)
	r2 := s.Preallocate()
	q.PutPreallocated(6, 200, r2)
	if !slices.Equal(q.Values(), []uint64{100, 200}) {
		t.Fatalf("values %v", q.Values())
	}
	q.Verify()
}

func TestQueueLongBumpNewestInPlace(t *testing.T) {
	s := newHeap()
	q, _ := NewQueueLong(s)
	q.Put(10, 1)
	b := q.Put(20, 2) // newest == headPrev → in-place timestamp rewrite
	q.Bump(b, 99)
	if !slices.Equal(q.Values(), []uint64{1, 2}) {
		t.Fatalf("values %v", q.Values())
	}
	var ts []uint64
	q.ForEach(func(_ store.Recid, _, t uint64) { ts = append(ts, t) })
	if !slices.Equal(ts, []uint64{10, 99}) {
		t.Fatalf("timestamps %v", ts)
	}
	q.Verify()
}

func TestQueueLongRejectsFieldsAboveI64Max(t *testing.T) {
	s := newHeap()
	q, _ := NewQueueLong(s)
	mustCorrupt(t, func() { q.Put(0, math.MaxInt64+1) })
	mustCorrupt(t, func() { q.Put(math.MaxInt64+1, 0) })
	q.Put(math.MaxInt64, math.MaxInt64) // boundary accepted
}

func TestQueueLongReentrantCallbackPanics(t *testing.T) {
	s := newHeap()
	q, _ := NewQueueLong(s)
	q.Put(10, 1)
	reentered := false
	defer func() {
		if r := recover(); r != nil {
			// a panic inside the callback also surfaces — accept either, but
			// the re-entry must have happened
			if !reentered {
				t.Fatalf("panic without reentry: %v", r)
			}
		}
	}()
	q.TakeUntil(func(_ store.Recid, _ LNode) bool {
		defer func() {
			if r := recover(); r != nil {
				if de, ok := mapdb5.AsDBError(r); ok && de.Kind == mapdb5.KindDataCorruption {
					reentered = true
					panic(r) // still propagates
				}
			}
		}()
		q.Len() // must panic (re-entry guard), not deadlock
		return false
	})
	if !reentered {
		t.Fatal("re-entry did not panic")
	}
}

// ---------- PersistentBlockingQueue ----------

func TestBlockingFifoLifoCircularReopen(t *testing.T) {
	s := newByteArray()

	fifo, err := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	fifo.AddAll("a", "b", "c")
	if v, _ := fifo.Poll(); v != "a" {
		t.Fatalf("poll %q", v)
	}
	if !fifo.RemoveValue("b") {
		t.Fatal("removeValue b")
	}
	if v, _ := fifo.Peek(); v != "c" {
		t.Fatalf("peek %q", v)
	}
	fifo.Verify()
	reopened, err := OpenBlockingQueue[string](s, fifo.HeaderRecid(), ser.String)
	if err != nil {
		t.Fatal(err)
	}
	if v := reopened.Take(); v != "c" {
		t.Fatalf("take %q", v)
	}
	if _, ok := reopened.Poll(); ok {
		t.Fatal("poll on empty")
	}

	stack, err := CreateBlockingQueue[string](s, ser.String, ModeLIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	stack.AddAll("a", "b", "c")
	if v, _ := stack.Poll(); v != "c" {
		t.Fatalf("lifo poll %q", v)
	}
	if v, _ := stack.Poll(); v != "b" {
		t.Fatalf("lifo poll %q", v)
	}
	stack.Verify()

	circ, err := CreateBlockingQueue[string](s, ser.String, ModeCircular, 3)
	if err != nil {
		t.Fatal(err)
	}
	circ.AddAll("a", "b", "c", "d")
	if circ.Len() != 3 {
		t.Fatalf("circular len %d", circ.Len())
	}
	for _, want := range []string{"b", "c", "d"} {
		if v, _ := circ.Poll(); v != want {
			t.Fatalf("circular poll %q want %q", v, want)
		}
	}
	circ.Verify()
}

func TestBlockingTakeWakesOnPut(t *testing.T) {
	s := newByteArray()
	q, err := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan string, 1)
	go func() { done <- q.Take() }()
	time.Sleep(100 * time.Millisecond)
	q.Put("ready")
	select {
	case v := <-done:
		if v != "ready" {
			t.Fatalf("take %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("take never woke")
	}
}

func TestBlockingCloseHandleWakesTaker(t *testing.T) {
	s := newByteArray()
	q, err := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		q.Take()
		done <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	q.CloseHandle()
	select {
	case r := <-done:
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindClosed {
			t.Fatalf("expected KindClosed panic, got %v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("taker never woke")
	}
}

func TestBlockingPollTimeout(t *testing.T) {
	s := newByteArray()
	q, _ := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	if _, ok := q.PollTimeout(20 * time.Millisecond); ok {
		t.Fatal("poll_timeout returned element on empty queue")
	}
}

func TestBlockingOfferTimeout(t *testing.T) {
	s := newByteArray()
	// circular never blocks; use FIFO with capacity… FIFO is unbounded, so
	// offerTimeout can only block on a bounded queue — none of the three
	// modes bound FIFO/LIFO except circular (which never blocks). Exercise
	// the immediate-success path and drain/contains.
	q, _ := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	if !q.OfferTimeout("x", 10*time.Millisecond) {
		t.Fatal("offerTimeout on unbounded queue failed")
	}
	if !q.Contains("x") {
		t.Fatal("contains x")
	}
	got := q.DrainTo(10)
	if !slices.Equal(got, []string{"x"}) {
		t.Fatalf("drain %v", got)
	}
}

func TestBlockingRemoveHeadTailMiddle(t *testing.T) {
	s := newByteArray()
	q, _ := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	q.Add("only")
	if !q.RemoveValue("only") {
		t.Fatal("remove head-of-1")
	}
	if q.Len() != 0 {
		t.Fatalf("len %d", q.Len())
	}
	q.Verify()

	q.AddAll("a", "b", "c", "d")
	if !q.RemoveValue("c") { // middle
		t.Fatal("remove c")
	}
	if !q.RemoveValue("d") { // tail
		t.Fatal("remove d")
	}
	if !slices.Equal(q.Slice(), []string{"a", "b"}) {
		t.Fatalf("remaining %v", q.Slice())
	}
	if !q.RemoveValue("a") { // head of 2
		t.Fatal("remove a")
	}
	if q.Len() != 1 {
		t.Fatalf("len %d", q.Len())
	}
	q.Verify()
}

func TestBlockingCreateRejectsBadCapacity(t *testing.T) {
	s := newByteArray()
	if _, err := CreateBlockingQueue[string](s, ser.String, ModeCircular, 0); err == nil {
		t.Fatal("capacity 0 accepted")
	}
	if _, err := CreateBlockingQueue[string](s, ser.String, ModeCircular, math.MaxUint64); err == nil {
		t.Fatal("capacity > i64::MAX accepted")
	}
}

func TestBlockingVerify(t *testing.T) {
	s := newByteArray()
	q, _ := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	q.AddAll("a", "b")
	q.Verify()
	q.Poll()
	q.Verify()
	q.PurgeRecords()
}

func TestBlockingConcurrentPutTake(t *testing.T) {
	s := newHeap()
	q, _ := CreateBlockingQueue[int64](s, ser.Long, ModeFIFO, math.MaxUint64)
	const n = 500
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := int64(0); i < n; i++ {
			q.Put(i)
		}
	}()
	var got []int64
	go func() {
		defer wg.Done()
		for len(got) < n {
			got = append(got, q.Take())
		}
	}()
	wg.Wait()
	if !slices.Equal(got, func() []int64 {
		w := make([]int64, n)
		for i := range w {
			w[i] = int64(i)
		}
		return w
	}()) {
		t.Fatal("ordering violated")
	}
	q.Verify()
}

// ---------- regression: chCond + mutex release (review findings) ----------

// CloseHandle must release EVERY blocked waiter: the first panic to run
// under the mutex must not retain it and strand the rest.
func TestBlockingCloseHandleWakesAllWaiters(t *testing.T) {
	s := newByteArray()
	q, err := CreateBlockingQueue[string](s, ser.String, ModeFIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	const waiters = 4
	done := make(chan any, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			defer func() { done <- recover() }()
			q.Take()
			done <- nil
		}()
	}
	// Also block a Put-side waiter shape: not needed (unbounded FIFO never
	// blocks puts); takers exercise the same code path.
	time.Sleep(100 * time.Millisecond)
	q.CloseHandle()
	for i := 0; i < waiters; i++ {
		select {
		case r := <-done:
			de, ok := mapdb5.AsDBError(r)
			if !ok || de.Kind != mapdb5.KindClosed {
				t.Fatalf("waiter %d: expected KindClosed panic, got %v", i, r)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("waiter %d never woke (mutex retained after panic?)", i)
		}
	}
}

// Every signal must reach a DISTINCT live waiter (lost-wakeup regression):
// N blocked takers + N puts ⇒ all takers deliver.
func TestBlockingSignalReachesEachWaiter(t *testing.T) {
	s := newHeap()
	q, err := CreateBlockingQueue[int64](s, ser.Long, ModeFIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	const waiters = 4
	done := make(chan int64, waiters)
	for i := 0; i < waiters; i++ {
		go func() { done <- q.Take() }()
	}
	time.Sleep(100 * time.Millisecond)
	for i := int64(0); i < waiters; i++ {
		q.Put(i)
	}
	got := map[int64]bool{}
	for i := 0; i < waiters; i++ {
		select {
		case v := <-done:
			got[v] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d takers woke (lost wakeup)", i, waiters)
		}
	}
	if len(got) != waiters {
		t.Fatalf("took %v, want %d distinct values", got, waiters)
	}
}

// A timed-out waiter must deregister: a later signal must not be absorbed
// by its dead channel.
func TestBlockingTimeoutWaiterDeregisters(t *testing.T) {
	s := newHeap()
	q, err := CreateBlockingQueue[int64](s, ser.Long, ModeFIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.PollTimeout(20 * time.Millisecond); ok {
		t.Fatal("timeout on empty queue")
	}
	done := make(chan int64, 1)
	go func() { done <- q.Take() }()
	time.Sleep(50 * time.Millisecond)
	q.Put(7)
	select {
	case v := <-done:
		if v != 7 {
			t.Fatalf("take %d", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("signal absorbed by a timed-out waiter's dead channel")
	}
}

// A store/serializer panic mid-op must not leave the handle mutex held —
// the next op must proceed.
func TestBlockingPanicReleasesMutex(t *testing.T) {
	s := newByteArray()
	var armed int32
	panicSer := &armedPanicSer{inner: ser.String, armed: &armed}
	q, err := CreateBlockingQueue[string](s, panicSer, ModeFIFO, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt32(&armed, 1)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected serializer panic")
			}
		}()
		q.Offer("boom")
	}()
	// Handle must still function: mutex free, enqueue succeeds.
	if !q.Offer("fine") {
		t.Fatal("offer after recovered panic failed (mutex retained?)")
	}
	if v, ok := q.Poll(); !ok || v != "fine" {
		t.Fatalf("poll after recovered panic = %v,%v", v, ok)
	}
}

// armedPanicSer delegates to inner but panics on Serialize while armed.
type armedPanicSer struct {
	inner ser.Serializer[string]
	armed *int32
}

func (s *armedPanicSer) Serialize(w *bin.Writer, v string) {
	if atomic.CompareAndSwapInt32(s.armed, 1, 0) {
		panic("injected serializer boom")
	}
	s.inner.Serialize(w, v)
}
func (s *armedPanicSer) Deserialize(r *bin.Reader, size int) string {
	return s.inner.Deserialize(r, size)
}
func (s *armedPanicSer) FixedSize() int                  { return -1 }
func (s *armedPanicSer) SizeHint() int                   { return s.inner.SizeHint() }
func (s *armedPanicSer) Compare(a, b string) int         { return s.inner.Compare(a, b) }
func (s *armedPanicSer) Equals(a, b string) bool         { return s.inner.Equals(a, b) }
func (s *armedPanicSer) NaturalOrder() bool              { return true }
func (s *armedPanicSer) EqualsBySerializedBytes() bool   { return true }
