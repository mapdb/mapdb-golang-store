package htree

// mapRuntime — port of Java org.mapdb.htree.MapRuntime: the shared
// runtime-only listener/value-loader state for HTree variants.
//
// Java scopes the deferred-event queue to a THREAD (ThreadLocal); Go has
// no thread locals, so the window is an explicit object created by
// deferBegin at lock acquisition and passed through fire calls — the
// goroutine-ID registry preserves Java's one-window-per-thread nesting
// tripwire across concurrent segments.

import (
	"errors"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/internal/handle"
	"github.com/mapdb/mapdb5-go/internal/listen"
)

// hevent is one queued modification event (Java MapRuntime.Event).
type hevent[K, V any] struct {
	key  K
	old  *V // nil = absent
	new  *V // nil = removed
	trig bool
}

// deferWindow is one open mutation-deferral window: deferred events queue
// here while the segment write lock is held and are delivered after it is
// released.
type deferWindow[K, V any] struct {
	events []hevent[K, V]
}

type mapRuntime[K, V any] struct {
	listenersMu sync.Mutex
	// Ordinary listeners fire after the segment write lock releases;
	// order-sensitive ones (SynchronousMapModificationListener) fire under
	// it — preserving per-key event order (Java MapRuntime listener split).
	deferredLs atomic.Pointer[[]mapdb5.MapModificationListener[K, V]]
	syncLs     atomic.Pointer[[]mapdb5.MapModificationListener[K, V]]
	// valueLoader is *func(K)(V,bool) so an unset loader is a nil pointer
	// load; the bool is Go's option return (Java: null = not found).
	valueLoader atomic.Pointer[func(K) (V, bool)]
	// One open deferral window per goroutine (Java ThreadLocal nesting
	// tripwire — "nested map mutation deferral"). Multiple goroutines may
	// hold windows concurrently (different segments).
	deferOwners sync.Map // goid -> *deferWindow
}

func (r *mapRuntime[K, V]) add(l mapdb5.MapModificationListener[K, V]) {
	if l == nil {
		panic(mapdb5.NewMisuse("htree: nil modification listener"))
	}
	listen.CheckIdentity(l)
	if _, ok := l.(mapdb5.SynchronousMapModificationListener[K, V]); ok {
		listen.Push(&r.listenersMu, &r.syncLs, l)
		return
	}
	listen.Push(&r.listenersMu, &r.deferredLs, l)
}

func (r *mapRuntime[K, V]) remove(l mapdb5.MapModificationListener[K, V]) {
	listen.Remove(&r.listenersMu, &r.deferredLs, l)
	listen.Remove(&r.listenersMu, &r.syncLs, l)
}

func (r *mapRuntime[K, V]) hasListeners() bool {
	return listen.Load(&r.deferredLs) != nil || listen.Load(&r.syncLs) != nil
}

// deferBegin opens the goroutine's deferral window (Java: one per thread;
// nested = IllegalStateException).
func (r *mapRuntime[K, V]) deferBegin() *deferWindow[K, V] {
	win := &deferWindow[K, V]{}
	// handle.Goid()==0 means unidentifiable (btree/queue convention): no
	// tripwire rather than a false collision.
	if id := handle.Goid(); id != 0 {
		if _, loaded := r.deferOwners.LoadOrStore(id, win); loaded {
			panic(mapdb5.NewCorruption("nested map mutation deferral"))
		}
	}
	return win
}

// deferEnd delivers EVERY queued event to every listener even when one
// fails — the goid registry is cleared first, the first error returned
// (the rest joined — Java "first throwable rethrown, remainder
// suppressed").
func (r *mapRuntime[K, V]) deferEnd(win *deferWindow[K, V]) error {
	if id := handle.Goid(); id != 0 {
		r.deferOwners.CompareAndDelete(id, win)
	}
	var first error
	for _, e := range win.events {
		if err := r.fireNow(e.key, e.old, e.new, e.trig); err != nil {
			first = suppressErr(first, err)
		}
	}
	return first
}

// fire is the full delivery: sync listeners immediately (caller MUST hold
// the segment write lock), ordinary listeners deferred until the lock is
// released. The deferred event is queued even when a sync listener failed
// — otherwise ordinary listeners would silently miss the mutation — and
// the first error is returned for the caller to surface.
func (r *mapRuntime[K, V]) fire(win *deferWindow[K, V], key K, oldV, newV *V, triggered bool) error {
	first := r.fireSync(key, oldV, newV, triggered)
	if err := r.fireDeferred(win, key, oldV, newV, triggered); err != nil {
		first = suppressErr(first, err)
	}
	return first
}

// fireSync delivers to synchronous listeners ONLY (Java fireSync — caller
// holds the segment write lock; that lock is what gives sync listeners
// their per-key ordering guarantee).
func (r *mapRuntime[K, V]) fireSync(key K, oldV, newV *V, triggered bool) error {
	return listen.Fire(listen.Load(&r.syncLs), key, oldV, newV, triggered)
}

// fireDeferred enqueues while a deferral window is open, fires immediately
// otherwise (e.g. the post-unlock eviction flush).
func (r *mapRuntime[K, V]) fireDeferred(win *deferWindow[K, V], key K, oldV, newV *V, triggered bool) error {
	if win != nil {
		// Value copies at enqueue: the caller's pointers alias its own
		// locals which may be reused before delivery.
		var o, n *V
		if oldV != nil {
			v := *oldV
			o = &v
		}
		if newV != nil {
			v := *newV
			n = &v
		}
		win.events = append(win.events, hevent[K, V]{key: key, old: o, new: n, trig: triggered})
		return nil
	}
	return r.fireNow(key, oldV, newV, triggered)
}

func (r *mapRuntime[K, V]) fireNow(key K, oldV, newV *V, triggered bool) error {
	return listen.Fire(listen.Load(&r.deferredLs), key, oldV, newV, triggered)
}

// fireRemovalBatch batch-delivers removal events, CONTINUING past a
// failing listener so every event reaches every listener (Java
// fireRemovalBatch); the first error is returned. syncOnly restricts
// delivery to the sync listeners (eviction victims, whose deferred events
// flush post-unlock). Callers pass the open defer window so ordinary
// listeners still queue for post-unlock delivery — Java's fire under a
// held write lock enqueues into the thread-local window.
func (r *mapRuntime[K, V]) fireRemovalBatch(win *deferWindow[K, V], entries []hevent[K, V], triggered, syncOnly bool) error {
	var first error
	for _, e := range entries {
		var err error
		if syncOnly {
			err = r.fireSync(e.key, e.old, nil, triggered)
		} else {
			err = r.fire(win, e.key, e.old, nil, triggered)
		}
		first = suppressErr(first, err)
	}
	return first
}

func (r *mapRuntime[K, V]) load(key K) (V, bool) {
	if lp := r.valueLoader.Load(); lp != nil {
		return (*lp)(key)
	}
	var z V
	return z, false
}

// suppressErr keeps the FIRST error and attaches the rest (Java
// Throwable.addSuppressed — first rethrown, remainder suppressed).
func suppressErr(first, err error) error {
	switch {
	case first == nil:
		return err
	case err == nil:
		return first
	default:
		return errors.Join(first, err)
	}
}
