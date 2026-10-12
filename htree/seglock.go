package htree

// segRuntime is the per-segment concurrency + listener machinery shared by
// all HTree variants (HTreeMap, HTreeMap48, HTreeMapExternal, HTreeCache):
// a []sync.RWMutex per segment, a write-owner goid table emulating Java's
// ReentrantReadWriteLock owner re-entrancy, and the mapRuntime defer
// window.
//
// Go's RWMutex is non-reentrant where Java's ReentrantReadWriteLock is:
// a sync listener re-entering a WRITE is caught by the deferBegin
// tripwire before it can block, and a write owner's re-entrant READ is
// exempted via segOwners — same observable semantics.

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/mapdb/mapdb5-go/internal/handle"
)

type segRuntime[K, V any] struct {
	locks      []sync.RWMutex
	segOwners  []atomic.Uint64
	threadSafe bool
	runtime    mapRuntime[K, V]
}

func newSegRuntime[K, V any](nseg int, threadSafe bool) segRuntime[K, V] {
	return segRuntime[K, V]{
		locks:      make([]sync.RWMutex, nseg),
		segOwners:  make([]atomic.Uint64, nseg),
		threadSafe: threadSafe,
	}
}

func (r *segRuntime[K, V]) isSegOwner(seg int) bool {
	id := handle.Goid()
	return id != 0 && r.segOwners[seg].Load() == id
}

func (r *segRuntime[K, V]) lockRead(seg int) {
	if !r.threadSafe || r.isSegOwner(seg) {
		return // no locking, or write-owner re-entrant read (Java reentrancy)
	}
	r.locks[seg].RLock()
}

func (r *segRuntime[K, V]) unlockRead(seg int) {
	if !r.threadSafe || r.isSegOwner(seg) {
		return
	}
	r.locks[seg].RUnlock()
}

// lockWrite opens the goroutine's defer window FIRST (Java: deferBegin
// before writeLock) — a re-entrant write panics there instead of blocking
// forever on Go's non-reentrant Lock.
func (r *segRuntime[K, V]) lockWrite(seg int) *deferWindow[K, V] {
	win := r.runtime.deferBegin()
	if r.threadSafe {
		r.locks[seg].Lock()
		r.segOwners[seg].Store(handle.Goid())
	}
	return win
}

// unlockWrite releases the segment write lock THEN delivers the window's
// deferred events (Java unlockWrite); the delivery error is returned for
// the caller to surface or suppress behind an in-flight body error.
func (r *segRuntime[K, V]) unlockWrite(seg int, win *deferWindow[K, V]) error {
	if r.threadSafe {
		r.segOwners[seg].Store(0)
		r.locks[seg].Unlock()
	}
	return r.runtime.deferEnd(win)
}

// runWrite performs a mutation under the segment write lock with Java's
// try/finally error ordering: an error or panic in f is the in-flight
// "body"; a deferEnd delivery failure is suppressed behind it (first
// error primary, rest joined), otherwise it becomes the result. The
// original panic value is re-thrown unchanged.
func (r *segRuntime[K, V]) runWrite(seg int, f func(*deferWindow[K, V]) error) error {
	win := r.lockWrite(seg)
	var pval any
	var body error
	func() {
		defer func() {
			if r := recover(); r != nil {
				pval = r
			}
		}()
		body = f(win)
	}()
	if derr := r.unlockWrite(seg, win); derr != nil {
		if pval != nil {
			// Java suppresses the deferred-delivery failure behind the
			// in-flight throwable and rethrows the throwable.
			if e, ok := pval.(error); ok {
				panic(errors.Join(e, derr))
			}
			panic(pval)
		}
		body = suppressErr(body, derr)
	}
	if pval != nil {
		panic(pval)
	}
	return body
}
