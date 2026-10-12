package btree

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/internal/listen"
	"github.com/mapdb/mapdb5-go/ser"
)

// ===================== listener registration =====================
//
// Two runtime-only listener lists (Java syncModListeners +
// compatibilityModListeners; the legacy Feature-B
// BTreeMap.ModificationListener is not ported — gap-listed). Each list is
// an immutable slice published copy-on-write under listenersMu, so firing
// is lock-free: a mutation snapshots the list and invokes without holding
// listenersMu. Registration order is the fire order (Java
// CopyOnWriteArrayList). The shared machinery lives in internal/listen.

// ModificationListenerAdd registers a modification listener (Java
// modificationListenerAdd). A listener implementing
// mapdb5.SynchronousMapModificationListener is classified onto the
// synchronous list and fires under the covering leaf lock; all others are
// deferred to after the lock releases and split propagation completes.
// Registering the same listener twice is a no-op (Java addIfAbsent).
// Panics on a nil listener, and on a listener whose value has no stable
// identity (see listen.CheckIdentity).
func (m *BTreeMap[K, V]) ModificationListenerAdd(l mapdb5.MapModificationListener[K, V]) {
	m.checkOpen()
	if l == nil {
		panic("btree: nil modification listener")
	}
	listen.CheckIdentity(l)
	if _, ok := l.(mapdb5.SynchronousMapModificationListener[K, V]); ok {
		listen.Push(&m.inner.listenersMu, &m.inner.syncLs, l)
		return
	}
	listen.Push(&m.inner.listenersMu, &m.inner.deferredLs, l)
}

// ModificationListenerRemove deregisters a listener from whichever list
// classified it (Java removes from both).
func (m *BTreeMap[K, V]) ModificationListenerRemove(l mapdb5.MapModificationListener[K, V]) {
	listen.Remove(&m.inner.listenersMu, &m.inner.syncLs, l)
	listen.Remove(&m.inner.listenersMu, &m.inner.deferredLs, l)
}

// ===================== listener firing =====================

// fireSync invokes the SYNCHRONOUS listeners; callers run it while the
// covering leaf lock is still held, after the mutation and counter update
// committed (Java fireModifiedSync). Per-listener continuation: every
// listener sees the event even when an earlier one fails; the first
// failure is returned (the rest joined — Java "first throwable rethrown,
// remainder suppressed"). The caller releases the lock and completes any
// mandatory structural work before surfacing the returned error.
func (m *BTreeMap[K, V]) fireSync(key K, oldV, newV *V) error {
	return listen.Fire(listen.Load(&m.inner.syncLs), key, oldV, newV, false)
}

// fireDeferred invokes the ordinary listeners; callers run it AFTER the
// covering leaf lock released (and after any split propagation), so user
// code never runs under a node lock (Java fireModified).
func (m *BTreeMap[K, V]) fireDeferred(key K, oldV, newV *V) error {
	return listen.Fire(listen.Load(&m.inner.deferredLs), key, oldV, newV, false)
}

// ===================== MapExtra =====================

// MapExtra is the MapDB 3 extension surface (Java org.mapdb.MapExtra) that
// persistent concurrent maps carry beyond the MutableMap contract:
// listener registration, an O(1) size, closed state, and the element
// serializers. Placed here rather than in the root package because the
// serializer accessors require ser types (the ser package already imports
// the root for DBError — a root-level definition would cycle).
type MapExtra[K, V any] interface {
	mapdb5.ModificationAwareMap[K, V]
	LenLong() int64
	IsClosed() bool
	KeySerializer() ser.Serializer[K]
	ValueSerializer() ser.Serializer[V]
}

var _ MapExtra[int64, int64] = (*BTreeMap[int64, int64])(nil)
