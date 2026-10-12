package mapdb5

// Map modification listeners, ported from MapDB 3
// (org.mapdb.MapModificationListener, SynchronousMapModificationListener,
// ModificationAwareMap). These interfaces are collection-agnostic — any
// persistent concurrent map can implement them; the concrete
// registration/firing machinery lives on the map (e.g. btree.BTreeMap).
//
// Async vs synchronous delivery: an ordinary MapModificationListener is
// fired AFTER the covering leaf/segment write lock is released (deferred),
// which avoids re-entrancy and lock-ordering deadlocks but lets two
// same-key mutations deliver their events in the opposite order under
// contention. A SynchronousMapModificationListener is the marker refinement
// requesting delivery WHILE the covering lock is still held, preserving
// per-key event order for order-sensitive consumers (secondary indexes;
// see the Java Bind).

// MapModificationListener is a runtime map-mutation callback (Java
// MapModificationListener.modify). oldValue is nil for inserts, newValue
// is nil for removes; both are non-nil for an update of an existing key.
// triggered is true for automatic expiry/eviction and false for
// user-requested mutations — this port has no expiry, so it is always
// false.
//
// Return nil on success. A non-nil error — or a panic — is a listener
// FAILURE: it never rolls back the already-committed mutation, never
// leaks the covering lock, and never skips mandatory structural work.
// The map still delivers the event to the remaining listeners, then
// surfaces the failures to the caller (the first failure is primary; the
// rest are attached — Java "first throwable rethrown, remainder
// suppressed", surfaced here as an errors.Join chain).
type MapModificationListener[K, V any] interface {
	Modify(key K, oldValue, newValue *V, triggered bool) error
}

// SynchronousMapModificationListener is the marker refinement of
// MapModificationListener whose events fire SYNCHRONOUSLY — while the map
// still holds the covering leaf (BTreeMap) or segment write lock that
// serialized the mutation (Java SynchronousMapModificationListener).
// Registration classifies on this interface, so a listener either is or
// is not synchronous — the mode cannot be silently demoted.
//
// A synchronous listener runs UNDER the map's lock, so its body must not
// re-enter the primary map — this port's node locks are non-reentrant and
// panic on same-goroutine re-entry rather than deadlocking — and must not
// mutate another lock-holding MapDB map in any topology that can invert
// lock order (reciprocal Bind bindings are the canonical hazard). The
// listener fires after the primary mutation (and its size-counter update)
// has committed, so a failing listener leaves the primary consistent.
type SynchronousMapModificationListener[K, V any] interface {
	MapModificationListener[K, V]
	// Synchronous is a no-op marker method; its mere presence on the
	// listener's type requests under-lock delivery (Java's instanceof
	// classification). Implement it with an empty body.
	Synchronous()
}

// ModificationListenerFunc adapts a function to a deferred
// MapModificationListener (the Rust port's FnListener).
type ModificationListenerFunc[K, V any] func(key K, oldValue, newValue *V, triggered bool) error

// Modify implements MapModificationListener.
func (f ModificationListenerFunc[K, V]) Modify(key K, oldValue, newValue *V, triggered bool) error {
	return f(key, oldValue, newValue, triggered)
}

// SyncModificationListenerFunc adapts a function to a synchronous
// modification listener.
type SyncModificationListenerFunc[K, V any] func(key K, oldValue, newValue *V, triggered bool) error

// Modify implements MapModificationListener.
func (f SyncModificationListenerFunc[K, V]) Modify(key K, oldValue, newValue *V, triggered bool) error {
	return f(key, oldValue, newValue, triggered)
}

// Synchronous implements the marker: this listener fires under the
// covering lock.
func (f SyncModificationListenerFunc[K, V]) Synchronous() {}

// ModificationAwareMap is a map that supports runtime-only modification
// listeners (Java org.mapdb.ModificationAwareMap). Registration and
// removal are by listener identity.
type ModificationAwareMap[K, V any] interface {
	// ModificationListenerAdd registers a listener (Java
	// modificationListenerAdd). A listener implementing
	// SynchronousMapModificationListener is classified onto the
	// synchronous list; all others are deferred. Registering the same
	// listener twice is a no-op (Java CopyOnWriteArrayList.addIfAbsent).
	ModificationListenerAdd(l MapModificationListener[K, V])
	// ModificationListenerRemove deregisters a previously added listener
	// from whichever list classified it (Java modificationListenerRemove).
	ModificationListenerRemove(l MapModificationListener[K, V])
}
