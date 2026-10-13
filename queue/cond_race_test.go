package queue

import (
	"sync"
	"testing"
	"time"

	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// Regression (fable release review #9): lost wakeup in chCond.waitTimeout: the timer fires, the waiter blocks on
// mu.Lock(); meanwhile a signal pops its channel and posts the token; the
// waiter then reports timeout and deregisters (no-op) — the signal is
// consumed by nobody. A second waiter registered afterwards never wakes.
func TestCondWaitTimeoutKeepsRacingSignal(t *testing.T) {
	var mu sync.Mutex
	c := newChCond()
	mu.Lock()
	done := make(chan bool)
	go func() {
		mu.Lock() // blocks until main releases
		ok := c.waitTimeout(&mu, 5*time.Millisecond)
		mu.Unlock()
		done <- ok
	}()
	// Let the waiter register and release mu, then hold mu across its timeout.
	mu.Unlock()
	time.Sleep(2 * time.Millisecond)
	mu.Lock()
	time.Sleep(20 * time.Millisecond) // the waiter's timer has fired; it is stuck on mu.Lock()
	c.signal()                        // pops the waiter's channel and posts the token
	mu.Unlock()
	ok := <-done
	t.Logf("timed waiter returned signaled=%v (signal was delivered)", ok)
	// Now a fresh untimed waiter must be woken by the signal above if it
	// was not consumed... it was consumed by the dead channel.
	mu.Lock()
	if n := len(c.waiters); n != 0 {
		t.Fatalf("waiters still registered: %d", n)
	}
	mu.Unlock()
	if !ok {
		t.Errorf("LOST WAKEUP: signal delivered to a timed-out waiter was dropped")
	}
}

// End-to-end on PBQ: consumer A polls with a timeout and is signaled while
// re-acquiring the lock; consumer B is blocked in Take; one element was
// Put. The element must reach exactly one of them, never neither.
func TestPBQTimedPollRacingSignal(t *testing.T) {
	st := store.NewStoreOnHeap(true)
	q, err := CreateBlockingQueue[int64](st, ser.Long, ModeFIFO, 0)
	if err != nil {
		t.Fatal(err)
	}
	type res struct {
		v  int64
		ok bool
	}
	aDone := make(chan res, 1)
	go func() { v, ok := q.PollTimeout(5 * time.Millisecond); aDone <- res{v, ok} }()
	time.Sleep(2 * time.Millisecond)
	got := make(chan int64, 1)
	go func() { got <- q.Take() }()
	time.Sleep(1 * time.Millisecond)
	q.mu.Lock()
	time.Sleep(20 * time.Millisecond) // A's timer fired; A is blocked on q.mu
	h := q.header()
	q.enqueue(h, 42)
	q.notEmpty.signal() // oldest waiter is A, whose timer already fired
	q.mu.Unlock()
	a := <-aDone
	if a.ok {
		if a.v != 42 {
			t.Fatalf("A got %d, want 42", a.v)
		}
		q.Put(7) // release B
		if v := <-got; v != 7 {
			t.Fatalf("B got %d, want 7", v)
		}
		return
	}
	select {
	case v := <-got:
		if v != 42 {
			t.Fatalf("B got %d, want 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("element lost: A timed out and B (Take) hung (len=%d)", q.Len())
	}
}
