// Package handle is the shared operation-lifetime machinery every
// collection type uses to implement drain-on-retire (Java Atomic's
// handleLock generalized). Extracted verbatim from btree/map.go so
// btree, htree and any later collection share the identical, reviewed
// implementation.
package handle

import (
	"sync"
)

// Goid (goid_fast.go / goid_slow.go) identifies the calling goroutine
// for the reentrancy-aware admission set and every re-entry tripwire in
// the module. An id is only meaningful while the goroutine that produced it
// is inside the scope that stored it: every stored id is cleared before
// that scope returns.

// Tracker is a per-goroutine admission set behind per-stripe counters.
// Tracking by goroutine (not per call) is what makes admission
// reentrancy-safe: a sync listener firing mid-op re-entering the map, or
// any nested public call, shares the outer admission instead of
// deadlocking on it.
//
// Admissions are spread over cache-line-padded stripes chosen by goroutine
// identity, so concurrent operations on one handle do not contend on a
// single lock (every map op admits). A goroutine always maps to the same
// stripe, so its admission, nested re-entry and release meet in one place.
//
// A sync.WaitGroup is deliberately not used: callers admit, then re-check
// their closed flag, so an Add can race a concurrent Wait — which
// WaitGroup forbids (it panics "WaitGroup is reused before previous Wait
// has returned" and -race reports it). A counter under a mutex has no such
// rule: an admission that arrives during a drain is counted, then released
// by the caller's closed re-check. An admission that lands on a stripe the
// drain already passed is released the same way: the caller stored closed
// before Wait, and Wait's lock/unlock of that stripe orders the store
// before the late admitter's re-check.
//
// The zero value is ready to use.
type Tracker struct {
	s [trackerStripes]trackerStripe
}

const trackerStripes = 64

type trackerStripe struct {
	mu     sync.Mutex
	n      int                 // admitted ops on this stripe (one per goroutine)
	active map[uint64]struct{} // goid -> admitted; lazily made
	cond   *sync.Cond          // on mu; lazily created by Wait
	_      [64 - 32]byte       // one stripe per 64-byte cache line
}

func (t *Tracker) stripe(g uint64) *trackerStripe {
	return &t.s[(g*0x9E3779B97F4A7C15)>>(64-6)]
}

// release drops g's admission if it is still held; mu held.
func (st *trackerStripe) release(g uint64) {
	if _, ok := st.active[g]; !ok {
		return
	}
	delete(st.active, g)
	st.n--
	if st.n < 0 {
		panic("handle: Tracker released more admissions than it admitted")
	}
	if st.n == 0 && st.cond != nil {
		st.cond.Broadcast()
	}
}

// Admit registers the caller's goroutine; the returned release must run
// when the op ends. A nested call on the same goroutine shares the outer
// admission and releases nothing. Goid()==0 ("unidentifiable", slow
// platforms only) still tracks via key 0: two such goroutines would share
// an admission, which degrades to over-waiting on retire — safe, not wrong.
func (t *Tracker) Admit() func() {
	g := Goid()
	st := t.stripe(g)
	st.mu.Lock()
	if _, nested := st.active[g]; nested {
		st.mu.Unlock()
		return func() {}
	}
	if st.active == nil {
		st.active = make(map[uint64]struct{})
	}
	st.active[g] = struct{}{}
	st.n++
	st.mu.Unlock()
	return func() {
		st.mu.Lock()
		defer st.mu.Unlock()
		st.release(g)
	}
}

// DropSelf removes the CALLING goroutine's admission before a drain
// begins (retirement invoked reentrantly from inside an admitted op —
// e.g. a listener calling Close — must not wait on itself).
func (t *Tracker) DropSelf() {
	g := Goid()
	st := t.stripe(g)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.release(g)
}

// Holds reports whether the CALLING goroutine currently holds an
// admission (i.e. it is inside an admitted op).
func (t *Tracker) Holds() bool {
	g := Goid()
	st := t.stripe(g)
	st.mu.Lock()
	defer st.mu.Unlock()
	_, ok := st.active[g]
	return ok
}

// Wait blocks until every admitted op has released.
// Admissions that arrive while it waits are counted too (and released by
// the caller's closed re-check), so it is safe to call concurrently with
// Admit.
func (t *Tracker) Wait() {
	for i := range t.s {
		st := &t.s[i]
		st.mu.Lock()
		if st.cond == nil {
			st.cond = sync.NewCond(&st.mu)
		}
		for st.n > 0 {
			st.cond.Wait()
		}
		st.mu.Unlock()
	}
}
