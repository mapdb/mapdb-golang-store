package ser

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// Group is a format-owned opaque materialized group (e.g. []int64 for
// LongFormat, a struct-of-slices for columnar). Only the format that created
// it may touch it; formats type-assert their own representation and panic
// DBError{Serialization} on foreign groups (spec 01 — groups are dynamically
// checked, Java parity).
type Group = any

// GroupFormat owns the representation AND access algorithm of a packed
// key/value array, with an object-side dialect (materialized groups) and a
// byte-side dialect (in-place search/get over serialized bytes).
//
// Mutating-shaped ops are COPY-ON-WRITE, as in Java: they return a new group
// and never mutate their input (published groups are shared with concurrent
// readers). Idx preconditions are programmer errors (panic Misuse).
type GroupFormat[K any] interface {
	// Element is the serializer for single-key operations (Java:
	// GroupFormat.element()).
	Element() Serializer[K]

	// Compare is the FORMAT-level stored order — which Java permits to differ
	// from Element().Compare. Search/BytesSearch and all tree routing use
	// THIS order, never the element's.
	Compare(a, b K) int

	// SupportsBinary: false forces materialized-group access; the byte-side
	// methods panic Unsupported (spec 01).
	SupportsBinary() bool
	// SupportsRangeCursor: RangeCursor additionally requires this; calling
	// RangeCursor when false panics Unsupported.
	SupportsRangeCursor() bool

	// ---- object-side dialect (materialized groups) ----
	Empty() Group
	Size(g Group) int
	Get(g Group, idx int) K
	// Search binary-searches the group in format order; returns the element
	// index when found, else the insertion point.
	Search(g Group, key K) (idx int, found bool)
	Insert(g Group, idx int, key K) Group
	Set(g Group, idx int, key K) Group
	Delete(g Group, idx int) Group
	CopyRange(g Group, from, to int) Group
	FromSlice(keys []K) Group
	ToSlice(g Group) []K

	// ---- group (de)serialization ----
	// Count travels out of band (node header): SerializeGroup writes
	// Size(g) elements; DeserializeGroup is told the count. Both leave the
	// cursor exactly at the end of the group encoding.
	SerializeGroup(w *bin.Writer, g Group)
	DeserializeGroup(r *bin.Reader, count int) Group

	// ---- byte-side dialect (SupportsBinary formats only) ----
	// All byte-side ops take r positioned at group start. On return the
	// reader is positioned at group end (satisfying the stricter Rust
	// contract; the Go spec leaves it unspecified).
	BytesSearch(r *bin.Reader, count int, key K) (idx int, found bool)
	BytesGet(r *bin.Reader, count int, idx int) K
	// RangeCursor iterates serialized elements with index in [from, to)
	// (0 <= from <= to <= count).
	RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[K]
}

// GroupCursor streams elements of a serialized group without materializing
// it. Single-goroutine, single-pass; after Next() returns false the cursor is
// dead — Index/Value panic Misuse.
type GroupCursor[K any] interface {
	// Next advances to the next element; false when the range is exhausted.
	Next() bool
	// Index is the in-group index of the current element (valid after
	// Next()==true).
	Index() int
	// Value is the current element (valid after Next()==true).
	Value() K
}

// ---------------------------------------------------------------------------
// Shared format helpers
// ---------------------------------------------------------------------------

// elemOff computes base + idx*width with overflow checks — a torn/oversize
// node must fail fast rather than wrap (GD8).
func elemOff(base, idx, width int) int {
	if idx < 0 || width < 0 || base < 0 {
		panic(mapdb5.NewCorruption("group seek overflow"))
	}
	o := idx * width
	if width != 0 && o/width != idx {
		panic(mapdb5.NewCorruption("group seek overflow"))
	}
	end := base + o
	if end < base {
		panic(mapdb5.NewCorruption("group seek overflow"))
	}
	return end
}

// checkGroupCount bounds a tainted group count before it feeds seek math or
// allocation (Go spec caps at math.MaxInt32/2 territory; the Rust port uses
// isize::MAX — the meaningful bound here is keeping count*width from
// overflowing int, which the elemOff checks enforce; we additionally reject
// negative counts which cannot arise from honest encoders).
func checkGroupCount(count int) {
	if count < 0 {
		panic(mapdb5.NewCorruption("group count negative"))
	}
}

// groupAlloc validates a tainted group count against the bytes actually
// present BEFORE it sizes an allocation (GD5/GD8): each element occupies at
// least minPerElem encoded bytes, so count*minPerElem > remaining is corrupt.
// Call this before every count-scaled make/append loop — a torn count must
// never reach make() as a multi-GB capacity.
func groupAlloc(count, minPerElem, remaining int) {
	checkGroupCount(count)
	if minPerElem > 0 && count > remaining/minPerElem {
		panic(mapdb5.NewCorruption("group count exceeds available bytes"))
	}
}

// sliceInsert returns a copy of g with v inserted at pos (copy-on-write).
func sliceInsert[T any](g []T, pos int, v T) []T {
	if pos < 0 || pos > len(g) {
		panic(mapdb5.NewMisuse("group insert index out of range"))
	}
	r := make([]T, 0, len(g)+1)
	r = append(r, g[:pos]...)
	r = append(r, v)
	return append(r, g[pos:]...)
}

// sliceSet returns a copy of g with g[pos] = v.
func sliceSet[T any](g []T, pos int, v T) []T {
	if pos < 0 || pos >= len(g) {
		panic(mapdb5.NewMisuse("group set index out of range"))
	}
	r := make([]T, len(g))
	copy(r, g)
	r[pos] = v
	return r
}

// sliceDelete returns a copy of g without element pos.
func sliceDelete[T any](g []T, pos int) []T {
	if pos < 0 || pos >= len(g) {
		panic(mapdb5.NewMisuse("group delete index out of range"))
	}
	r := make([]T, 0, len(g)-1)
	r = append(r, g[:pos]...)
	return append(r, g[pos+1:]...)
}

// sliceCopyRange returns g[from:to] as a new slice.
func sliceCopyRange[T any](g []T, from, to int) []T {
	if from < 0 || to < from || to > len(g) {
		panic(mapdb5.NewMisuse("group copy range out of bounds"))
	}
	r := make([]T, to-from)
	copy(r, g[from:to])
	return r
}

// bsearch is Arrays.binarySearch semantics over a sorted slice; returns
// (index, true) or (insertion point, false).
func bsearch[T any](g []T, key T, compare func(a, b T) int) (int, bool) {
	lo, hi := 0, len(g)-1
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		c := compare(g[mid], key)
		switch {
		case c < 0:
			lo = mid + 1
		case c > 0:
			hi = mid - 1
		default:
			return mid, true
		}
	}
	return lo, false
}

// binaryGetCursor is the default RangeCursor built on BytesGet — a
// correctness fallback for formats with random byte-side access but no
// special sequential layout. O(n·BytesGet) for a full scan.
type binaryGetCursor[K any] struct {
	format     GroupFormat[K]
	r          *bin.Reader
	groupStart int
	count      int
	to         int
	idx        int
	started    bool
	cur        K
	exhausted  bool
}

// newBinaryGetCursor: r must be at group start; 0 <= from <= to <= count.
func newBinaryGetCursor[K any](format GroupFormat[K], r *bin.Reader, count, from, to int) *binaryGetCursor[K] {
	return &binaryGetCursor[K]{
		format:     format,
		r:          r,
		groupStart: r.Pos(),
		count:      count,
		to:         to,
		idx:        from,
	}
}

func (c *binaryGetCursor[K]) Next() bool {
	if c.exhausted {
		return false
	}
	if c.started {
		c.idx++
	} else {
		c.started = true
	}
	if c.idx >= c.to {
		c.exhausted = true
		var zero K
		c.cur = zero
		// Snap input to group end.
		c.r.SetPos(c.groupStart)
		if c.count > 0 {
			c.format.BytesGet(c.r, c.count, c.count-1)
		}
		return false
	}
	c.r.SetPos(c.groupStart)
	c.cur = c.format.BytesGet(c.r, c.count, c.idx)
	return true
}

func (c *binaryGetCursor[K]) Index() int {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Index outside valid element"))
	}
	return c.idx
}

func (c *binaryGetCursor[K]) Value() K {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Value outside valid element"))
	}
	return c.cur
}

// rangeBounds validates a RangeCursor range.
func rangeBounds(count, from, to int) {
	if from < 0 || from > to || to > count {
		panic(mapdb5.NewCorruption("range_cursor bounds"))
	}
}
