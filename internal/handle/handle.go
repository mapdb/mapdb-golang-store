// Package handle is the shared operation-lifetime machinery every
// collection type uses to implement drain-on-retire (Java Atomic's
// handleLock generalized). Extracted verbatim from btree/map.go so
// btree, htree and any later collection share the identical, reviewed
// implementation.
package handle

import (
	"runtime"
	"sync"
)

// Goid identifies the calling goroutine for the reentrancy-aware
// admission set. Parsed lazily — only contention paths pay for it.
// Returns 0 when the runtime stack format cannot be parsed (treated as
// "unidentifiable": callers skip the registry rather than risk a false
// collision — btree/queue convention).
func Goid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// format: "goroutine 123 [running]..."
	if n < 10 {
		return 0
	}
	var id uint64
	for i := 10; i < n && buf[i] >= '0' && buf[i] <= '9'; i++ {
		id = id*10 + uint64(buf[i]-'0')
	}
	return id
}

// Tracker is a per-goroutine admission set behind a WaitGroup. Tracking
// by goroutine (not per call) is what makes admission reentrancy-safe:
// a sync listener firing mid-op re-entering the map, or any nested public
// call, shares the outer admission instead of deadlocking on it.
type Tracker struct {
	wg     sync.WaitGroup
	active sync.Map // goid -> struct{}
}

// Admit registers the caller's goroutine; the returned release must run
// when the op ends. A nested call on the same goroutine shares the outer
// admission and releases nothing. goid()==0 ("unidentifiable") still
// tracks via key 0: two such goroutines would share an admission, which
// degrades to over-waiting on retire — safe, not wrong.
func (t *Tracker) Admit() func() {
	g := Goid()
	if _, loaded := t.active.LoadOrStore(g, struct{}{}); loaded {
		return func() {}
	}
	t.wg.Add(1)
	return func() {
		if t.active.CompareAndDelete(g, struct{}{}) {
			t.wg.Done()
		}
	}
}

// DropSelf removes the CALLING goroutine's admission before a drain
// begins (retirement invoked reentrantly from inside an admitted op —
// e.g. a listener calling Close — must not wait on itself).
func (t *Tracker) DropSelf() {
	if t.active.CompareAndDelete(Goid(), struct{}{}) {
		t.wg.Done()
	}
}

// Holds reports whether the CALLING goroutine currently holds an
// admission (i.e. it is inside an admitted op).
func (t *Tracker) Holds() bool {
	_, ok := t.active.Load(Goid())
	return ok
}

// Wait blocks until every admitted op has released.
func (t *Tracker) Wait() { t.wg.Wait() }
