package handle

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestTrackerStripeIsOneCacheLine(t *testing.T) {
	if sz := unsafe.Sizeof(trackerStripe{}); sz != 64 {
		t.Fatalf("tracker stripe is %d bytes, want 64", sz)
	}
}

func TestGoidDistinctAndStable(t *testing.T) {
	me := Goid()
	if me == 0 || me != Goid() {
		t.Fatal("Goid not stable on one goroutine")
	}
	other := make(chan uint64)
	go func() { other <- Goid() }()
	if <-other == me {
		t.Fatal("two live goroutines share a Goid")
	}
}

func TestTrackerNestedAdmissionShared(t *testing.T) {
	var tr Tracker
	rel := tr.Admit()
	inner := tr.Admit() // nested: shares the outer admission
	if !tr.Holds() {
		t.Fatal("Holds false inside an admission")
	}
	inner()
	if !tr.Holds() {
		t.Fatal("nested release dropped the outer admission")
	}
	rel()
	if tr.Holds() {
		t.Fatal("Holds true after release")
	}
	tr.Wait() // nothing admitted: returns at once
}

func TestTrackerDropSelf(t *testing.T) {
	var tr Tracker
	rel := tr.Admit()
	tr.DropSelf()
	done := make(chan struct{})
	go func() { tr.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait blocked on an admission DropSelf removed")
	}
	rel() // the outer release after DropSelf is a no-op
}

// Wait drains every admission taken before it started, across goroutines
// landing on many stripes, and admissions racing it are released by a
// closed re-check (the callers' protocol).
func TestTrackerWaitDrains(t *testing.T) {
	for round := 0; round < 50; round++ {
		var tr Tracker
		var closed atomic.Bool
		var inFlight atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := 0; g < 32; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 200; i++ {
					rel := tr.Admit()
					if closed.Load() {
						rel()
						return
					}
					inFlight.Add(1)
					time.Sleep(time.Microsecond)
					inFlight.Add(-1)
					rel()
				}
			}()
		}
		close(start)
		time.Sleep(time.Millisecond)
		closed.Store(true)
		tr.Wait()
		if n := inFlight.Load(); n != 0 {
			t.Fatalf("round %d: Wait returned with %d admitted ops in flight", round, n)
		}
		wg.Wait()
	}
}
