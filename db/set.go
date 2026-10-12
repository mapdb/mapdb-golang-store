// Map-backed navigable set (Java DB.treeSet) built on a BTreeMap whose
// value format serializes nothing (Java Serializers.NO_VALUE inside an
// ObjectArrayFormat). Only the TreeSet catalog row is written; the value
// format is implicit and has no descriptor.
package db

import (
	"iter"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/btree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// noValueSer is the element serializer for the absent value: it reads and
// writes zero bytes (Java Serializers.NO_VALUE).
type noValueSer struct{}

func (noValueSer) Serialize(w *bin.Writer, v struct{})          {}
func (noValueSer) Deserialize(r *bin.Reader, size int) struct{} { return struct{}{} }
func (noValueSer) FixedSize() int                               { return 0 }
func (noValueSer) SizeHint() int                                { return 0 }
func (noValueSer) Compare(a, b struct{}) int                    { return 0 }
func (noValueSer) Equals(a, b struct{}) bool                    { return true }
func (noValueSer) NaturalOrder() bool                           { return true }
func (noValueSer) EqualsBySerializedBytes() bool                { return true }

// noValueFormat is a value group format that carries only a count and
// serializes to zero bytes (Java map-backed set's no-value format). The
// group is the element count; every element is struct{}.
type noValueFormat struct{}

func (noValueFormat) cast(g ser.Group) int {
	n, ok := g.(int)
	if !ok {
		panic(mapdb5.NewMisuse("noValueFormat group is not an int"))
	}
	return n
}

func (noValueFormat) Element() ser.Serializer[struct{}] { return noValueSer{} }
func (noValueFormat) Compare(a, b struct{}) int         { return 0 }
func (noValueFormat) SupportsBinary() bool              { return true }
func (noValueFormat) SupportsRangeCursor() bool         { return false }
func (noValueFormat) Empty() ser.Group                  { return 0 }
func (f noValueFormat) Size(g ser.Group) int            { return f.cast(g) }
func (noValueFormat) Get(g ser.Group, idx int) struct{} { return struct{}{} }
func (noValueFormat) Search(g ser.Group, key struct{}) (int, bool) {
	return 0, false // values are never searched
}
func (f noValueFormat) Insert(g ser.Group, idx int, key struct{}) ser.Group {
	return f.cast(g) + 1
}
func (f noValueFormat) Set(g ser.Group, idx int, key struct{}) ser.Group { return f.cast(g) }
func (f noValueFormat) Delete(g ser.Group, idx int) ser.Group {
	n := f.cast(g)
	if n > 0 {
		n--
	}
	return n
}
func (f noValueFormat) CopyRange(g ser.Group, from, to int) ser.Group { return to - from }
func (f noValueFormat) FromSlice(values []struct{}) ser.Group         { return len(values) }
func (f noValueFormat) ToSlice(g ser.Group) []struct{} {
	return make([]struct{}, f.cast(g))
}
func (f noValueFormat) SerializeGroup(w *bin.Writer, g ser.Group) {}
func (f noValueFormat) DeserializeGroup(r *bin.Reader, count int) ser.Group {
	return count
}
func (f noValueFormat) BytesSearch(r *bin.Reader, count int, key struct{}) (int, bool) {
	return 0, false
}
func (f noValueFormat) BytesGet(r *bin.Reader, count, idx int) struct{} { return struct{}{} }
func (f noValueFormat) RangeCursor(r *bin.Reader, count, from, to int) ser.GroupCursor[struct{}] {
	// Sets iterate keys via the materialized object side, never the value
	// byte cursor. Unreachable for the set surface.
	panic(mapdb5.New(mapdb5.KindUnsupported, "no-value format has no byte-side cursor"))
}

// NavigableSet is a persistent sorted set backed by a
// *btree.BTreeMap[K, struct{}] (Java DB.treeSet result).
type NavigableSet[K any] struct {
	m *btree.BTreeMap[K, struct{}]
}

func navigableSetFromMap[K any](m *btree.BTreeMap[K, struct{}]) *NavigableSet[K] {
	return &NavigableSet[K]{m: m}
}

// BackingMap exposes the backing map (facade accessors).
func (s *NavigableSet[K]) BackingMap() *btree.BTreeMap[K, struct{}] { return s.m }

// closeHandle retires the handle on DB delete/rollback/close: the shared
// backing-map state is invalidated (every later op panics KindClosed,
// including any live view) and its open lease released (Java
// closeRuntimeHandle).
func (s *NavigableSet[K]) closeHandle() { s.m.CloseHandle() }

// CounterRecid is the backing map's size-counter recid (0 = disabled).
func (s *NavigableSet[K]) CounterRecid() store.Recid { return s.m.CounterRecid() }

// RootRecidRecid is the backing map's root-pointer recid.
func (s *NavigableSet[K]) RootRecidRecid() store.Recid { return s.m.RootRecidRecid() }

// Add inserts element; returns true if it was not already present (Java
// add).
func (s *NavigableSet[K]) Add(e K) bool {
	_, existed := s.m.PutIfAbsent(e, struct{}{})
	return !existed
}

// Contains reports membership (Java contains).
func (s *NavigableSet[K]) Contains(e K) bool { return s.m.ContainsKey(e) }

// Remove deletes element; returns true if it was present (Java remove).
func (s *NavigableSet[K]) Remove(e K) bool {
	_, existed := s.m.Remove(e)
	return existed
}

// Size is the O(1) element count when the counter is enabled, else the
// counted size (Java size).
func (s *NavigableSet[K]) Size() int64 { return s.m.LenLong() }

// IsEmpty reports emptiness (Java isEmpty).
func (s *NavigableSet[K]) IsEmpty() bool { return s.m.IsEmpty() }

// Clear removes every element (Java clear).
func (s *NavigableSet[K]) Clear() { s.m.Clear() }

// First returns the least element (Java first).
func (s *NavigableSet[K]) First() (K, bool) {
	k, _, ok := s.m.FirstEntry()
	return k, ok
}

// Last returns the greatest element (Java last).
func (s *NavigableSet[K]) Last() (K, bool) {
	k, _, ok := s.m.LastEntry()
	return k, ok
}

// All yields every element ascending (weakly consistent).
func (s *NavigableSet[K]) All() iter.Seq[K] { return s.m.Keys() }

// Slice materializes every element ascending.
func (s *NavigableSet[K]) Slice() []K {
	var out []K
	for k := range s.All() {
		out = append(out, k)
	}
	return out
}

// ---- navigable surface (Java MapBackedNavigableSet) ----

// Lower returns the greatest element strictly less than e (Java lower).
func (s *NavigableSet[K]) Lower(e K) (K, bool) {
	k, _, ok := s.m.Lower(e)
	return k, ok
}

// Floor returns the greatest element less than or equal to e (Java floor).
func (s *NavigableSet[K]) Floor(e K) (K, bool) {
	k, _, ok := s.m.Floor(e)
	return k, ok
}

// Ceiling returns the least element greater than or equal to e (Java
// ceiling).
func (s *NavigableSet[K]) Ceiling(e K) (K, bool) {
	k, _, ok := s.m.Ceiling(e)
	return k, ok
}

// Higher returns the least element strictly greater than e (Java higher).
func (s *NavigableSet[K]) Higher(e K) (K, bool) {
	k, _, ok := s.m.Higher(e)
	return k, ok
}

// PollFirst removes and returns the least element (Java pollFirst).
func (s *NavigableSet[K]) PollFirst() (K, bool) {
	for {
		k, v, ok := s.m.FirstEntry()
		if !ok {
			var z K
			return z, false
		}
		if s.m.RemoveIfEquals(k, v) {
			return k, true
		}
	}
}

// PollLast removes and returns the greatest element (Java pollLast).
func (s *NavigableSet[K]) PollLast() (K, bool) {
	for {
		k, v, ok := s.m.LastEntry()
		if !ok {
			var z K
			return z, false
		}
		if s.m.RemoveIfEquals(k, v) {
			return k, true
		}
	}
}

// Descending returns a live descending view (Java descendingSet).
func (s *NavigableSet[K]) Descending() *NavigableSetView[K] {
	return &NavigableSetView[K]{set: s, desc: true}
}

// SubSet returns a live [from, to] view honoring the inclusivity flags
// (Java subSet). Panics Misuse when from > to.
func (s *NavigableSet[K]) SubSet(from K, fromInc bool, to K, toInc bool) *NavigableSetView[K] {
	v := &NavigableSetView[K]{set: s, lo: boundKV[K]{key: from, has: true, inc: fromInc},
		hi: boundKV[K]{key: to, has: true, inc: toInc}}
	v.checkOrdered()
	return v
}

// HeadSet returns a live view of the elements up to to (Java headSet).
func (s *NavigableSet[K]) HeadSet(to K, inc bool) *NavigableSetView[K] {
	return &NavigableSetView[K]{set: s, hi: boundKV[K]{key: to, has: true, inc: inc}}
}

// TailSet returns a live view of the elements from from onward (Java
// tailSet).
func (s *NavigableSet[K]) TailSet(from K, inc bool) *NavigableSetView[K] {
	return &NavigableSetView[K]{set: s, lo: boundKV[K]{key: from, has: true, inc: inc}}
}

// IsClosed reports whether the backing store is closed.
func (s *NavigableSet[K]) IsClosed() bool { return s.m.IsClosed() }

// ============================ bounded view ============================

type boundKV[K any] struct {
	key K
	has bool
	inc bool
}

// NavigableSetView is a live, bounded, orientation-aware view over a
// NavigableSet (Java NavigableSet sub/head/tail/descending views). It
// reflects concurrent backing-set changes and its mutators write through.
// Read/remove-only: there is no Add, matching Java's out-of-range-add ban
// on range views.
type NavigableSetView[K any] struct {
	set  *NavigableSet[K]
	lo   boundKV[K]
	hi   boundKV[K]
	desc bool
}

func (v *NavigableSetView[K]) cmp() func(a, b K) int { return v.set.m.KeySerializer().Compare }

func (v *NavigableSetView[K]) checkOrdered() {
	if v.lo.has && v.hi.has && v.cmp()(v.lo.key, v.hi.key) > 0 {
		panic(mapdb5.NewMisuse("subSet from > to"))
	}
}

// inBounds reports whether k is inside the view's bounds (in the
// view's orientation the same bounds apply — descending only flips
// iteration/nav direction, not membership).
func (v *NavigableSetView[K]) inBounds(k K) bool {
	c := v.cmp()
	if v.lo.has {
		r := c(k, v.lo.key)
		if r < 0 || (r == 0 && !v.lo.inc) {
			return false
		}
	}
	if v.hi.has {
		r := c(k, v.hi.key)
		if r > 0 || (r == 0 && !v.hi.inc) {
			return false
		}
	}
	return true
}

// seq yields the view's elements in orientation order (weakly consistent).
func (v *NavigableSetView[K]) seq() iter.Seq[K] {
	if v.desc {
		return func(yield func(K) bool) {
			c := v.cmp()
			for k := range v.set.m.DescendingKeys() {
				if v.hi.has {
					if r := c(k, v.hi.key); r > 0 || (r == 0 && !v.hi.inc) {
						continue // still above the view
					}
				}
				if !v.inBounds(k) {
					return // below the lo bound — done
				}
				if !yield(k) {
					return
				}
			}
		}
	}
	return func(yield func(K) bool) {
		c := v.cmp()
		var it iter.Seq2[K, struct{}]
		switch {
		case v.lo.has && v.hi.has:
			it = v.set.m.SubMap(v.lo.key, v.hi.key)
		case v.lo.has:
			it = v.set.m.TailMap(v.lo.key)
		case v.hi.has:
			it = v.set.m.HeadMap(v.hi.key)
		default:
			it = v.set.m.All()
		}
		for k := range it {
			if !v.inBounds(k) {
				continue // the map's fixed-inclusivity ranges are tightened here
			}
			if !yield(k) {
				return
			}
		}
		// An inclusive hi bound past the map's [ ,to) SubMap edge needs one
		// extra probe.
		if v.hi.has && v.hi.inc {
			if k, _, ok := v.set.m.Ceiling(v.hi.key); ok && c(k, v.hi.key) == 0 && v.inBounds(k) {
				yield(k)
			}
		}
	}
}

// Slice materializes the view's elements in orientation order.
func (v *NavigableSetView[K]) Slice() []K {
	var out []K
	for k := range v.seq() {
		out = append(out, k)
	}
	return out
}

// All yields the view's elements in orientation order.
func (v *NavigableSetView[K]) All() iter.Seq[K] { return v.seq() }

// Contains reports membership within the view's bounds.
func (v *NavigableSetView[K]) Contains(e K) bool {
	return v.inBounds(e) && v.set.Contains(e)
}

// Remove deletes e when it is present within the view's bounds; returns
// true if removed. Writes through to the backing set.
func (v *NavigableSetView[K]) Remove(e K) bool {
	return v.inBounds(e) && v.set.Remove(e)
}

// Clear removes every element within the view's bounds.
func (v *NavigableSetView[K]) Clear() {
	var keys []K
	for k := range v.seq() {
		keys = append(keys, k)
	}
	for _, k := range keys {
		v.set.Remove(k)
	}
}

// Size counts the view's elements (O(range)).
func (v *NavigableSetView[K]) Size() int64 {
	var n int64
	for range v.seq() {
		n++
	}
	return n
}

// IsEmpty reports whether the view has elements.
func (v *NavigableSetView[K]) IsEmpty() bool {
	for range v.seq() {
		return false
	}
	return true
}

// First returns the first element in view orientation (least for an
// ascending view, greatest for descending).
func (v *NavigableSetView[K]) First() (K, bool) {
	if !v.desc {
		k, _, ok := v.lowestEntry()
		return k, ok
	}
	k, _, ok := v.highestEntry()
	return k, ok
}

// Last returns the last element in view orientation.
func (v *NavigableSetView[K]) Last() (K, bool) {
	if !v.desc {
		k, _, ok := v.highestEntry()
		return k, ok
	}
	k, _, ok := v.lowestEntry()
	return k, ok
}

// lowestEntry is the least element satisfying the view bounds.
func (v *NavigableSetView[K]) lowestEntry() (K, struct{}, bool) {
	var k K
	var v0 struct{}
	var ok bool
	if v.lo.has {
		if v.lo.inc {
			k, v0, ok = v.set.m.Ceiling(v.lo.key)
		} else {
			k, v0, ok = v.set.m.Higher(v.lo.key)
		}
	} else {
		k, v0, ok = v.set.m.FirstEntry()
	}
	if !ok || !v.inBounds(k) {
		return k, v0, false
	}
	return k, v0, true
}

// highestEntry is the greatest element satisfying the view bounds.
func (v *NavigableSetView[K]) highestEntry() (K, struct{}, bool) {
	var k K
	var v0 struct{}
	var ok bool
	if v.hi.has {
		if v.hi.inc {
			k, v0, ok = v.set.m.Floor(v.hi.key)
		} else {
			k, v0, ok = v.set.m.Lower(v.hi.key)
		}
	} else {
		k, v0, ok = v.set.m.LastEntry()
	}
	if !ok || !v.inBounds(k) {
		return k, v0, false
	}
	return k, v0, true
}

// Lower returns the greatest element strictly before e in view orientation.
func (v *NavigableSetView[K]) Lower(e K) (K, bool) {
	if v.desc {
		return v.higherAsc(e)
	}
	return v.lowerAsc(e)
}

// Floor returns the greatest element at or before e in view orientation.
func (v *NavigableSetView[K]) Floor(e K) (K, bool) {
	if v.desc {
		return v.ceilingAsc(e)
	}
	return v.floorAsc(e)
}

// Ceiling returns the least element at or after e in view orientation.
func (v *NavigableSetView[K]) Ceiling(e K) (K, bool) {
	if v.desc {
		return v.floorAsc(e)
	}
	return v.ceilingAsc(e)
}

// Higher returns the least element strictly after e in view orientation.
func (v *NavigableSetView[K]) Higher(e K) (K, bool) {
	if v.desc {
		return v.lowerAsc(e)
	}
	return v.higherAsc(e)
}

// Bounded navigation (Java OrderedNavigableView's effLower/effUpper):
// a probe past the view's edge is clamped to the nearest in-view endpoint
// before the backing map is searched — an unrestricted map search would
// find a candidate outside the view and wrongly report absence.
func (v *NavigableSetView[K]) lowerAsc(e K) (K, bool) {
	var k K
	var ok bool
	if v.hi.has && v.cmp()(e, v.hi.key) > 0 {
		k, _, ok = v.inViewLast()
	} else {
		k, _, ok = v.set.m.Lower(e)
	}
	if !ok || !v.inBounds(k) {
		var z K
		return z, false
	}
	return k, true
}
func (v *NavigableSetView[K]) floorAsc(e K) (K, bool) {
	var k K
	var ok bool
	switch {
	case v.hi.has && v.cmp()(e, v.hi.key) > 0:
		k, _, ok = v.inViewLast()
	case v.hi.has && v.cmp()(e, v.hi.key) == 0 && !v.hi.inc:
		k, _, ok = v.set.m.Lower(e)
	default:
		k, _, ok = v.set.m.Floor(e)
	}
	if !ok || !v.inBounds(k) {
		var z K
		return z, false
	}
	return k, true
}
func (v *NavigableSetView[K]) ceilingAsc(e K) (K, bool) {
	var k K
	var ok bool
	switch {
	case v.lo.has && v.cmp()(e, v.lo.key) < 0:
		k, _, ok = v.inViewFirst()
	case v.lo.has && v.cmp()(e, v.lo.key) == 0 && !v.lo.inc:
		k, _, ok = v.set.m.Higher(e)
	default:
		k, _, ok = v.set.m.Ceiling(e)
	}
	if !ok || !v.inBounds(k) {
		var z K
		return z, false
	}
	return k, true
}
func (v *NavigableSetView[K]) higherAsc(e K) (K, bool) {
	var k K
	var ok bool
	if v.lo.has && v.cmp()(e, v.lo.key) < 0 {
		k, _, ok = v.inViewFirst()
	} else {
		k, _, ok = v.set.m.Higher(e)
	}
	if !ok || !v.inBounds(k) {
		var z K
		return z, false
	}
	return k, true
}

// inViewFirst is the least element inside the lo bound (inclusive →
// Ceiling, exclusive → Higher); hi membership is checked by the caller.
func (v *NavigableSetView[K]) inViewFirst() (K, struct{}, bool) {
	if v.lo.inc {
		return v.set.m.Ceiling(v.lo.key)
	}
	return v.set.m.Higher(v.lo.key)
}

// inViewLast is the greatest element inside the hi bound; lo membership is
// checked by the caller.
func (v *NavigableSetView[K]) inViewLast() (K, struct{}, bool) {
	if v.hi.inc {
		return v.set.m.Floor(v.hi.key)
	}
	return v.set.m.Lower(v.hi.key)
}

// PollFirst removes and returns the first element in view orientation.
func (v *NavigableSetView[K]) PollFirst() (K, bool) {
	for {
		k, v0, ok := v.firstEntry()
		if !ok {
			var z K
			return z, false
		}
		if v.set.m.RemoveIfEquals(k, v0) {
			return k, true
		}
	}
}

// PollLast removes and returns the last element in view orientation.
func (v *NavigableSetView[K]) PollLast() (K, bool) {
	for {
		k, v0, ok := v.lastEntry()
		if !ok {
			var z K
			return z, false
		}
		if v.set.m.RemoveIfEquals(k, v0) {
			return k, true
		}
	}
}

func (v *NavigableSetView[K]) firstEntry() (K, struct{}, bool) {
	if v.desc {
		return v.highestEntry()
	}
	return v.lowestEntry()
}
func (v *NavigableSetView[K]) lastEntry() (K, struct{}, bool) {
	if v.desc {
		return v.lowestEntry()
	}
	return v.highestEntry()
}

// Descending flips the view's orientation (Java descendingSet on a view).
func (v *NavigableSetView[K]) Descending() *NavigableSetView[K] {
	w := *v
	w.desc = !v.desc
	return &w
}

// checkBoundKey is Java OrderedNavigableView.checkBoundKey: a new sub-view
// bound must lie inside the parent's closed range, and an inclusive bound
// may not sit exactly on an EXCLUDED parent endpoint.
func (v *NavigableSetView[K]) checkBoundKey(k K, kInc bool) {
	c := v.cmp()
	if v.lo.has {
		r := c(k, v.lo.key)
		if r < 0 || (r == 0 && kInc && !v.lo.inc) {
			panic(mapdb5.NewMisuse("key out of range"))
		}
	}
	if v.hi.has {
		r := c(k, v.hi.key)
		if r > 0 || (r == 0 && kInc && !v.hi.inc) {
			panic(mapdb5.NewMisuse("key out of range"))
		}
	}
}

// SubSet nests a bounded view; args are in THIS view's orientation — on a
// descending view from is the higher (ascending-upper) bound.
func (v *NavigableSetView[K]) SubSet(from K, fromInc bool, to K, toInc bool) *NavigableSetView[K] {
	v.checkBoundKey(from, fromInc)
	v.checkBoundKey(to, toInc)
	w := &NavigableSetView[K]{set: v.set, desc: v.desc}
	if v.desc {
		w.lo = boundKV[K]{key: to, has: true, inc: toInc}
		w.hi = boundKV[K]{key: from, has: true, inc: fromInc}
	} else {
		w.lo = boundKV[K]{key: from, has: true, inc: fromInc}
		w.hi = boundKV[K]{key: to, has: true, inc: toInc}
	}
	w.checkOrdered()
	return w.intersect(v)
}

// HeadSet nests a head view (elements up to to in this view's orientation —
// the LOWER half of a descending view, i.e. an ascending lower bound).
func (v *NavigableSetView[K]) HeadSet(to K, inc bool) *NavigableSetView[K] {
	v.checkBoundKey(to, inc)
	w := &NavigableSetView[K]{set: v.set, desc: v.desc}
	if v.desc {
		w.lo = boundKV[K]{key: to, has: true, inc: inc}
	} else {
		w.hi = boundKV[K]{key: to, has: true, inc: inc}
	}
	return w.intersect(v)
}

// TailSet nests a tail view (elements from from onward in this view's
// orientation — the UPPER half of a descending view).
func (v *NavigableSetView[K]) TailSet(from K, inc bool) *NavigableSetView[K] {
	v.checkBoundKey(from, inc)
	w := &NavigableSetView[K]{set: v.set, desc: v.desc}
	if v.desc {
		w.hi = boundKV[K]{key: from, has: true, inc: inc}
	} else {
		w.lo = boundKV[K]{key: from, has: true, inc: inc}
	}
	return w.intersect(v)
}

// intersect returns the tighter-bounded merge of w with v's bounds: an
// element must satisfy both views' bounds.
func (v *NavigableSetView[K]) intersect(w *NavigableSetView[K]) *NavigableSetView[K] {
	c := v.cmp()
	mergeLo := func(a, b boundKV[K]) boundKV[K] {
		if !a.has {
			return b
		}
		if !b.has {
			return a
		}
		r := c(a.key, b.key)
		switch {
		case r > 0:
			return a
		case r < 0:
			return b
		default:
			return boundKV[K]{key: a.key, has: true, inc: a.inc && b.inc}
		}
	}
	mergeHi := func(a, b boundKV[K]) boundKV[K] {
		if !a.has {
			return b
		}
		if !b.has {
			return a
		}
		r := c(a.key, b.key)
		switch {
		case r < 0:
			return a
		case r > 0:
			return b
		default:
			return boundKV[K]{key: a.key, has: true, inc: a.inc && b.inc}
		}
	}
	out := &NavigableSetView[K]{set: v.set, desc: v.desc,
		lo: mergeLo(v.lo, w.lo), hi: mergeHi(v.hi, w.hi)}
	if out.lo.has && out.hi.has && c(out.lo.key, out.hi.key) > 0 {
		panic(mapdb5.NewMisuse("nested view bounds are disjoint"))
	}
	return out
}
