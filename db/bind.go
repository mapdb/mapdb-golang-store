// Bind — secondary indexes and derived views over a primary BTreeMap
// (Java org.mapdb.Bind).
//
// Two listener classes, exactly as Java:
//   - Order-sensitive (SecondaryValue, SecondaryValues, SecondaryKey,
//     SecondaryKeys, MapInverse) install a SYNCHRONOUS listener, fired
//     under the covering leaf lock that serialized the mutation, so
//     same-key events are totally ordered with the mutations (last writer
//     wins in the index too).
//   - Ordinary/deferred (Size, Histogram, MapPutAfterDelete) install a
//     plain listener, fired after the leaf lock is released and split
//     propagation completes.
//
// # Initial population
//
// Every order-sensitive binding (and Histogram) first checks whether its
// secondary is empty and, if so, iterates the primary's EXISTING entries
// and replays the derive function (Java `if (secondary.isEmpty()) for (e :
// primary.entrySet()) ...`). The initial scan and the listener registration
// are NOT atomic against concurrent writers — install every binding while
// the primary is quiescent.
//
// # Secondary containers
//
// Secondaries are accepted through the SecMap / SecSet interfaces, so a
// binding can target either the in-memory SecondaryMap / SecondarySet (the
// Go stand-in for Java's ConcurrentMap / Set) OR a persistent *btree.BTreeMap
// (Java accepts any Map).
//
// # Self-binding and lock cycles
//
// Direct self-binding is rejected: RejectSelfBind compares two same-typed
// handles, and every order-sensitive install ALSO rejects a secondary
// whose SecIdentity matches the primary's StateID — a persistent secondary
// that is the primary itself. Binding a map to itself would re-enter the
// covering leaf lock a synchronous listener already holds. This does NOT
// detect longer/transitive cycles: a synchronous callback mutates a
// secondary while holding the primary leaf lock, so reciprocal or
// transitive bindings can deadlock. Install bindings while the primary is
// quiescent.
//
// # Exception hazard
//
// A binding listener that fails (SecondaryKey / MapInverse on a duplicate
// derived key) does so AFTER the primary has already been mutated; the
// primary and any already-updated secondaries are left mutated — the error
// only signals the constraint violation (Java parity).
package db

import (
	"sync"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/btree"
)

// ============================ secondary containers ============================

// SecondaryMap is a shareable in-memory secondary map (Java ConcurrentMap).
type SecondaryMap[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

// NewSecondaryMap creates an empty shared secondary map.
func NewSecondaryMap[K comparable, V any]() *SecondaryMap[K, V] {
	return &SecondaryMap[K, V]{m: make(map[K]V)}
}

// Get returns the mapped value.
func (s *SecondaryMap[K, V]) Get(k K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok
}

// ContainsKey reports membership.
func (s *SecondaryMap[K, V]) ContainsKey(k K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[k]
	return ok
}

// Len is the entry count.
func (s *SecondaryMap[K, V]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

// IsEmpty reports emptiness.
func (s *SecondaryMap[K, V]) IsEmpty() bool { return s.Len() == 0 }

// Keys snapshots the key set (unordered).
func (s *SecondaryMap[K, V]) Keys() []K {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]K, 0, len(s.m))
	for k := range s.m {
		out = append(out, k)
	}
	return out
}

// Put stores k→v (direct mutation, for population/consumers; bindings call
// it too).
func (s *SecondaryMap[K, V]) Put(k K, v V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = v
}

// SecondarySet is a shareable in-memory secondary set (Java Set).
type SecondarySet[T comparable] struct {
	mu sync.Mutex
	m  map[T]struct{}
}

// NewSecondarySet creates an empty shared secondary set.
func NewSecondarySet[T comparable]() *SecondarySet[T] {
	return &SecondarySet[T]{m: make(map[T]struct{})}
}

// Contains reports membership.
func (s *SecondarySet[T]) Contains(t T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[t]
	return ok
}

// Len is the element count.
func (s *SecondarySet[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

// IsEmpty reports emptiness.
func (s *SecondarySet[T]) IsEmpty() bool { return s.Len() == 0 }

// ============================ SecMap / SecSet ============================

// SecMap is a map-like secondary target (Java Map): either the in-memory
// SecondaryMap or a persistent *btree.BTreeMap. Bindings mutate it under
// the primary's leaf lock.
type SecMap[Kd, Vd any] interface {
	SecIsEmpty() bool
	SecGet(k Kd) (Vd, bool)
	SecPut(k Kd, v Vd)
	// SecPutIfAbsent is Java ConcurrentMap.putIfAbsent: atomically insert
	// (k, v) iff k is absent, returning the value already present (if any).
	// ONE critical section, so a unique-index install cannot lose a race
	// between check and insert.
	SecPutIfAbsent(k Kd, v Vd) (Vd, bool)
	SecRemove(k Kd)
	// SecRemoveIfValue is Java Map.remove(key, value): remove only if k
	// currently maps to a value eq-equal to v.
	SecRemoveIfValue(k Kd, v Vd, eq func(a, b Vd) bool)
	// SecIdentity is the primary's StateID when this secondary is a
	// persistent map (self-bind detection), else false.
	SecIdentity() (uint64, bool)
}

// SecSet is a set-like secondary target (Java Set) for the one-to-many
// indexes.
type SecSet[T any] interface {
	SecIsEmpty() bool
	SecAdd(t T)
	SecRemove(t T)
	SecIdentity() (uint64, bool)
}

// ---- SecondaryMap impl ----

func (s *SecondaryMap[K, V]) SecIsEmpty() bool     { return s.IsEmpty() }
func (s *SecondaryMap[K, V]) SecGet(k K) (V, bool) { return s.Get(k) }
func (s *SecondaryMap[K, V]) SecPut(k K, v V)      { s.Put(k, v) }
func (s *SecondaryMap[K, V]) SecPutIfAbsent(k K, v V) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.m[k]; ok {
		return old, true
	}
	s.m[k] = v
	var z V
	return z, false
}
func (s *SecondaryMap[K, V]) SecRemove(k K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, k)
}
func (s *SecondaryMap[K, V]) SecRemoveIfValue(k K, v V, eq func(a, b V) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.m[k]; ok && eq(cur, v) {
		delete(s.m, k)
	}
}
func (s *SecondaryMap[K, V]) SecIdentity() (uint64, bool) { return 0, false }

// ---- SecondarySet impl ----

func (s *SecondarySet[T]) SecIsEmpty() bool { return s.IsEmpty() }
func (s *SecondarySet[T]) SecAdd(t T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[t] = struct{}{}
}
func (s *SecondarySet[T]) SecRemove(t T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, t)
}
func (s *SecondarySet[T]) SecIdentity() (uint64, bool) { return 0, false }

// ---- persistent-map secondary adapters ----

// BTreeSecMap adapts a persistent *btree.BTreeMap into a SecMap (Java
// accepts any Map). SecIdentity exposes the map's shared-state id so a
// self-bind — the secondary handle being the primary itself — is rejected.
type BTreeSecMap[K, V any] struct{ M *btree.BTreeMap[K, V] }

func (s BTreeSecMap[K, V]) SecIsEmpty() bool                  { return s.M.IsEmpty() }
func (s BTreeSecMap[K, V]) SecGet(k K) (V, bool)              { return s.M.Get(k) }
func (s BTreeSecMap[K, V]) SecPut(k K, v V)                   { s.M.Put(k, v) }
func (s BTreeSecMap[K, V]) SecPutIfAbsent(k K, v V) (V, bool) { return s.M.PutIfAbsent(k, v) }
func (s BTreeSecMap[K, V]) SecRemove(k K)                     { s.M.Remove(k) }
func (s BTreeSecMap[K, V]) SecRemoveIfValue(k K, v V, _ func(a, b V) bool) {
	s.M.RemoveIfEquals(k, v) // the map's own serializer equality
}
func (s BTreeSecMap[K, V]) SecIdentity() (uint64, bool) { return s.M.StateID(), true }

// ============================ self-bind guard ============================

// RejectSelfBind rejects binding a map to a clone of itself (Java's
// self-cycle guard), comparing two handles of the SAME concrete type.
func RejectSelfBind[K, V any](primary, secondary *btree.BTreeMap[K, V]) {
	if primary.SharesStateWith(secondary) {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"cannot bind a map to itself (direct self-cycle)"))
	}
}

// rejectSelfBindID rejects a secondary whose identity is the primary itself
// (checked at every order-sensitive install, so a persistent-map self-bind
// is caught even when the secondary's generic value type differs).
func rejectSelfBindID[K, V any](primary *btree.BTreeMap[K, V], secondary interface {
	SecIdentity() (uint64, bool)
}) {
	if id, has := secondary.SecIdentity(); has && id == primary.StateID() {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"cannot bind a map to itself (direct self-cycle)"))
	}
}

// ============================ listener install ============================
//
// Each binding installs a FRESH pointer-typed adapter: func listeners dedup
// by code pointer, and every binding built from the same helper literal
// would share one code pointer and silently collapse into the first
// registration. A heap pointer has unique identity, so every installed
// binding is distinct.

type bindSyncListener[K, V any] struct{ f func(k K, old, new *V) error }

func (l *bindSyncListener[K, V]) Modify(k K, old, new *V, _ bool) error {
	return l.f(k, old, new)
}
func (l *bindSyncListener[K, V]) Synchronous() {}

type bindDeferredListener[K, V any] struct{ f func(k K, old, new *V) error }

func (l *bindDeferredListener[K, V]) Modify(k K, old, new *V, _ bool) error {
	return l.f(k, old, new)
}

func addSync[K, V any](primary *btree.BTreeMap[K, V], f func(k K, old, new *V) error) {
	primary.ModificationListenerAdd(&bindSyncListener[K, V]{f: f})
}

func addDeferred[K, V any](primary *btree.BTreeMap[K, V], f func(k K, old, new *V) error) {
	primary.ModificationListenerAdd(&bindDeferredListener[K, V]{f: f})
}

// putUnique is Java Bind.putUnique: insert derived → primary key,
// tolerating a duplicate that already maps to the SAME primary key, but
// rejecting one mapping to a different key.
func putUnique[Dk, Pk any](sec SecMap[Dk, Pk], derived Dk, primaryKey Pk, same func(a, b Pk) bool) {
	// Atomic check-and-insert: two writers on different primary leaves
	// whose derived keys collide can no longer both observe "absent" and
	// both insert — exactly one wins, the other sees the winner and errors.
	existing, was := sec.SecPutIfAbsent(derived, primaryKey)
	if !was {
		return
	}
	if !same(existing, primaryKey) {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"unique secondary index: duplicate derived key"))
	}
}

// ============================ order-sensitive bindings (synchronous) ============================

// SecondaryValue maintains secondary[key] = derive(key, value), removed
// when the primary key is removed (Java Bind.secondaryValue).
func SecondaryValue[K, V, Dv any, Sec SecMap[K, Dv]](
	primary *btree.BTreeMap[K, V],
	secondary Sec,
	derive func(k K, v V) Dv,
) {
	rejectSelfBindID(primary, secondary)
	if secondary.SecIsEmpty() {
		for k, v := range primary.All() {
			secondary.SecPut(k, derive(k, v))
		}
	}
	addSync(primary, func(k K, _, new *V) error {
		if new != nil {
			secondary.SecPut(k, derive(k, *new))
		} else {
			secondary.SecRemove(k)
		}
		return nil
	})
}

// SecondaryValues is a one-to-many value index: secondary holds
// (primary_key, derived_value) pairs for every value derive(key, value)
// yields (Java Bind.secondaryValues).
func SecondaryValues[K, V, Dv any, Sec SecSet[ValuePair[K, Dv]]](
	primary *btree.BTreeMap[K, V],
	secondary Sec,
	derive func(k K, v V) []Dv,
) {
	rejectSelfBindID(primary, secondary)
	if secondary.SecIsEmpty() {
		for k, v := range primary.All() {
			for _, dv := range derive(k, v) {
				secondary.SecAdd(ValuePair[K, Dv]{k, dv})
			}
		}
	}
	addSync(primary, func(k K, old, new *V) error {
		if old != nil {
			for _, dv := range derive(k, *old) {
				secondary.SecRemove(ValuePair[K, Dv]{k, dv})
			}
		}
		if new != nil {
			for _, dv := range derive(k, *new) {
				secondary.SecAdd(ValuePair[K, Dv]{k, dv})
			}
		}
		return nil
	})
}

// ValuePair is a (first, second) tuple element for the one-to-many
// secondary sets (Java Fun.Pair).
type ValuePair[A, B any] struct {
	A A
	B B
}

// SecondaryKey maintains a UNIQUE single-key secondary index:
// secondary[derive(key,value)] = key. A derived key already mapping to a
// different primary key is rejected (Java Bind.secondaryKey throws
// IllegalArgumentException from the listener).
func SecondaryKey[K, V, Dk any, Sec SecMap[Dk, K]](
	primary *btree.BTreeMap[K, V],
	secondary Sec,
	derive func(k K, v V) Dk,
) {
	rejectSelfBindID(primary, secondary)
	same := func(a, b K) bool { return primary.KeySerializer().Equals(a, b) }
	if secondary.SecIsEmpty() {
		for k, v := range primary.All() {
			putUnique(secondary, derive(k, v), k, same)
		}
	}
	addSync(primary, func(k K, old, new *V) error {
		if old != nil {
			secondary.SecRemoveIfValue(derive(k, *old), k, same)
		}
		if new != nil {
			putUnique(secondary, derive(k, *new), k, same)
		}
		return nil
	})
}

// SecondaryKeys is a multi-key secondary index: secondary holds
// (derived_key, primary_key) pairs for every derived key of a value (Java
// Bind.secondaryKeys).
func SecondaryKeys[K, V, Dk any, Sec SecSet[ValuePair[Dk, K]]](
	primary *btree.BTreeMap[K, V],
	secondary Sec,
	derive func(k K, v V) []Dk,
) {
	rejectSelfBindID(primary, secondary)
	if secondary.SecIsEmpty() {
		for k, v := range primary.All() {
			for _, dk := range derive(k, v) {
				secondary.SecAdd(ValuePair[Dk, K]{dk, k})
			}
		}
	}
	addSync(primary, func(k K, old, new *V) error {
		if old != nil {
			for _, dk := range derive(k, *old) {
				secondary.SecRemove(ValuePair[Dk, K]{dk, k})
			}
		}
		if new != nil {
			for _, dk := range derive(k, *new) {
				secondary.SecAdd(ValuePair[Dk, K]{dk, k})
			}
		}
		return nil
	})
}

// MapInverse maintains inverse[value] = key. Values must be UNIQUE — a
// value already mapping to a different key is rejected (Java Bind.mapInverse
// delegates to secondaryKey with the identity value projection).
func MapInverse[K, V any, Sec SecMap[V, K]](primary *btree.BTreeMap[K, V], inverse Sec) {
	SecondaryKey(primary, inverse, func(_ K, v V) V { return v })
}

// ============================ deferred bindings ============================

// SizeBinding maintains a running size in an AtomicLong (Java Bind.size).
// Seeds the counter from the primary's current size ONLY when the counter
// is 0 (Java `if (counter.get() == 0)`), then increments on insert /
// decrements on remove.
func SizeBinding[K, V any](primary *btree.BTreeMap[K, V], counter *AtomicLong) {
	if counter.Get() == 0 {
		counter.Set(primary.LenLong())
	}
	addDeferred(primary, func(k K, old, new *V) error {
		switch {
		case old == nil && new != nil:
			counter.IncrementAndGet()
		case old != nil && new == nil:
			counter.DecrementAndGet()
		}
		return nil
	})
}

// Histogram maintains a category histogram (Java Bind.histogram): counts
// values by category. A category whose count reaches EXACTLY 0 is removed;
// negative counts are kept (Java addCount: value == 0 ? null : value).
func Histogram[K, V, C comparable](
	primary *btree.BTreeMap[K, V],
	hist *SecondaryMap[C, int64],
	category func(k K, v V) C,
) {
	// Initial population: category is evaluated BEFORE the histogram lock
	// is held (Java evaluates category.apply before ConcurrentMap.compute)
	// so a closure reading the same histogram cannot self-deadlock the
	// non-reentrant mutex.
	if hist.IsEmpty() {
		for k, v := range primary.All() {
			c := category(k, v)
			histAdjust(hist, c, 1)
		}
	}
	addDeferred(primary, func(k K, old, new *V) error {
		var oldC, newC C
		hasOld, hasNew := old != nil, new != nil
		if hasOld {
			oldC = category(k, *old)
		}
		if hasNew {
			newC = category(k, *new)
		}
		if hasOld {
			histAdjust(hist, oldC, -1)
		}
		if hasNew {
			histAdjust(hist, newC, 1)
		}
		return nil
	})
}

// histAdjust applies delta to a histogram category with wrapping arithmetic
// (Java long parity); a count of exactly 0 removes the entry.
func histAdjust[C comparable](s *SecondaryMap[C, int64], c C, delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.m[c] + delta
	if n == 0 {
		delete(s.m, c)
		return
	}
	s.m[c] = n
}

// MapPutAfterDelete captures the removed value of every deleted key (Java
// Bind.mapPutAfterDelete). Accepts any SecMap target (in-memory or a
// persistent BTreeMap).
func MapPutAfterDelete[K, V any, Sec SecMap[K, V]](primary *btree.BTreeMap[K, V], deleted Sec) {
	addDeferred(primary, func(k K, old, new *V) error {
		if new == nil && old != nil {
			deleted.SecPut(k, *old)
		}
		return nil
	})
}
