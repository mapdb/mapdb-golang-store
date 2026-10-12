package ser

import (
	"cmp"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// Delta-packed group formats (Java LongDeltaFormat, IntDeltaFormat):
// packLong(zigzag(k0)) then packLong(zigzag(ki-ki-1)). Object side is
// identical to the fixed-stride formats ([]K); byte side is a sequential
// decode with early exit — no random access.

type deltaFormat[K cmp.Ordered] struct {
	elem    Serializer[K]
	pack    func(w *bin.Writer, delta K)
	unpack  func(r *bin.Reader) K
	sub     func(a, b K) K // a - b (wrapping)
	add     func(a, b K) K // a + b (wrapping)
	zero    K
	cmpFunc func(a, b K) int
	skipMax int // per-element packed-byte cap for the skip path
	desc    string
}

func (f deltaFormat[K]) cast(g Group) []K {
	gg, ok := g.([]K)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "foreign group passed to delta format"))
	}
	return gg
}

func (f deltaFormat[K]) Element() Serializer[K]    { return f.elem }
func (f deltaFormat[K]) Compare(a, b K) int        { return f.cmpFunc(a, b) }
func (f deltaFormat[K]) SupportsBinary() bool      { return true }
func (f deltaFormat[K]) SupportsRangeCursor() bool { return true }
func (f deltaFormat[K]) Empty() Group              { return []K{} }
func (f deltaFormat[K]) Size(g Group) int          { return len(f.cast(g)) }
func (f deltaFormat[K]) Get(g Group, idx int) K {
	gg := f.cast(g)
	if idx < 0 || idx >= len(gg) {
		panic(mapdb5.NewMisuse("group get index out of range"))
	}
	return gg[idx]
}
func (f deltaFormat[K]) Search(g Group, key K) (int, bool) {
	return bsearch(f.cast(g), key, f.cmpFunc)
}
func (f deltaFormat[K]) Insert(g Group, idx int, key K) Group {
	return Group(sliceInsert(f.cast(g), idx, key))
}
func (f deltaFormat[K]) Set(g Group, idx int, key K) Group {
	return Group(sliceSet(f.cast(g), idx, key))
}
func (f deltaFormat[K]) Delete(g Group, idx int) Group {
	return Group(sliceDelete(f.cast(g), idx))
}
func (f deltaFormat[K]) CopyRange(g Group, from, to int) Group {
	return Group(sliceCopyRange(f.cast(g), from, to))
}
func (f deltaFormat[K]) FromSlice(keys []K) Group {
	out := make([]K, len(keys))
	copy(out, keys)
	return Group(out)
}
func (f deltaFormat[K]) ToSlice(g Group) []K {
	gg := f.cast(g)
	out := make([]K, len(gg))
	copy(out, gg)
	return out
}

func (f deltaFormat[K]) SerializeGroup(w *bin.Writer, g Group) {
	prev := f.zero
	for _, v := range f.cast(g) {
		f.pack(w, f.sub(v, prev))
		prev = v
	}
}

func (f deltaFormat[K]) DeserializeGroup(r *bin.Reader, count int) Group {
	groupAlloc(count, 1, r.Remaining())
	out := make([]K, 0, count)
	v := f.zero
	for i := 0; i < count; i++ {
		v = f.add(v, f.unpack(r))
		out = append(out, v)
	}
	return Group(out)
}

func (f deltaFormat[K]) BytesSearch(r *bin.Reader, count int, key K) (int, bool) {
	checkGroupCount(count)
	v := f.zero
	for i := 0; i < count; i++ {
		v = f.add(v, f.unpack(r))
		if f.cmpFunc(v, key) >= 0 {
			r.UnpackUint64Skip(count-i-1, f.skipMax) // leave reader at group end
			if v == key {
				return i, true
			}
			return i, false
		}
	}
	return count, false
}

func (f deltaFormat[K]) BytesGet(r *bin.Reader, count int, idx int) K {
	checkGroupCount(count)
	if idx < 0 || idx >= count {
		panic(mapdb5.NewCorruption("group index out of range"))
	}
	v := f.zero
	for i := 0; i <= idx; i++ {
		v = f.add(v, f.unpack(r))
	}
	r.UnpackUint64Skip(count-idx-1, f.skipMax) // leave reader at group end
	return v
}

func (f deltaFormat[K]) RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[K] {
	checkGroupCount(count)
	rangeBounds(count, from, to)
	return &deltaCursor[K]{f: f, r: r, count: count, to: to, idx: from, acc: f.zero}
}

// deltaCursor is a single forward pass over the zigzag stream (O(n) scan).
type deltaCursor[K cmp.Ordered] struct {
	f         deltaFormat[K]
	r         *bin.Reader
	count     int
	to        int
	idx       int
	started   bool
	decoded   int // elements consumed from the stream
	acc       K   // running value; after decoding element k, acc == element[k]
	cur       K
	exhausted bool
}

func (c *deltaCursor[K]) consumeTo(count int) {
	for c.decoded < count {
		c.acc = c.f.add(c.acc, c.f.unpack(c.r))
		c.decoded++
	}
}

func (c *deltaCursor[K]) Next() bool {
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
		c.consumeTo(c.count) // drain to group end
		return false
	}
	c.consumeTo(c.idx + 1)
	c.cur = c.acc
	return true
}

func (c *deltaCursor[K]) Index() int {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Index outside valid element"))
	}
	return c.idx
}

func (c *deltaCursor[K]) Value() K {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Value outside valid element"))
	}
	return c.cur
}

// LongDeltaFormat: packLong(zigzag) delta-compressed int64 groups.
var LongDeltaFormat = deltaFormat[int64]{
	desc: "LONG_DELTA",
	elem: Long,
	pack: func(w *bin.Writer, d int64) { w.PackUint64(zigzag(d)) },
	unpack: func(r *bin.Reader) int64 {
		return unzigzag(r.UnpackUint64())
	},
	sub:     func(a, b int64) int64 { return a - b }, // int64 sub wraps in Go
	add:     func(a, b int64) int64 { return a + b },
	cmpFunc: cmp.Compare[int64],
	skipMax: bin.MaxPackedLongBytes,
}

// IntDeltaFormat: packInt(zigzag32) delta-compressed int32 groups.
var IntDeltaFormat = deltaFormat[int32]{
	desc: "INT_DELTA",
	elem: Int32,
	pack: func(w *bin.Writer, d int32) { w.PackInt(zigzag32(d)) },
	unpack: func(r *bin.Reader) int32 {
		return unzigzag32(r.UnpackInt())
	},
	sub:     func(a, b int32) int32 { return a - b },
	add:     func(a, b int32) int32 { return a + b },
	cmpFunc: cmp.Compare[int32],
	skipMax: bin.MaxPackedIntBytes,
}
