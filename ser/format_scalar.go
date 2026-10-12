package ser

import (
	"cmp"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// Fixed-stride group formats (Java ShortFormat, CharFormat, IntFormat,
// LongFormat, UUIDFormat): a packed array of fixed-width big-endian elements
// giving O(log n) true binary search over serialized bytes. Object side is
// []K; mutating ops are copy-on-write.

// fixedFormat is the generic fixed-width-stride implementation. Elements
// must be DECODED (sign-extended) before comparing — raw BE bytes memcmp
// the negative half after the non-negative half.
type fixedFormat[K any] struct {
	width int
	elem  Serializer[K]
	write func(w *bin.Writer, v K)
	read  func(r *bin.Reader) K
	cmp   func(a, b K) int
	desc  string
}

func (f fixedFormat[K]) cast(g Group) []K {
	gg, ok := g.([]K)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "foreign group passed to fixed format"))
	}
	return gg
}

func (f fixedFormat[K]) Element() Serializer[K]    { return f.elem }
func (f fixedFormat[K]) Compare(a, b K) int        { return f.cmp(a, b) }
func (f fixedFormat[K]) SupportsBinary() bool      { return true }
func (f fixedFormat[K]) SupportsRangeCursor() bool { return true }
func (f fixedFormat[K]) Empty() Group              { return []K{} }
func (f fixedFormat[K]) Size(g Group) int          { return len(f.cast(g)) }
func (f fixedFormat[K]) Get(g Group, idx int) K {
	gg := f.cast(g)
	if idx < 0 || idx >= len(gg) {
		panic(mapdb5.NewMisuse("group get index out of range"))
	}
	return gg[idx]
}
func (f fixedFormat[K]) Search(g Group, key K) (int, bool) {
	return bsearch(f.cast(g), key, f.cmp)
}
func (f fixedFormat[K]) Insert(g Group, idx int, key K) Group {
	return Group(sliceInsert(f.cast(g), idx, key))
}
func (f fixedFormat[K]) Set(g Group, idx int, key K) Group {
	return Group(sliceSet(f.cast(g), idx, key))
}
func (f fixedFormat[K]) Delete(g Group, idx int) Group {
	return Group(sliceDelete(f.cast(g), idx))
}
func (f fixedFormat[K]) CopyRange(g Group, from, to int) Group {
	return Group(sliceCopyRange(f.cast(g), from, to))
}
func (f fixedFormat[K]) FromSlice(keys []K) Group {
	out := make([]K, len(keys))
	copy(out, keys)
	return Group(out)
}
func (f fixedFormat[K]) ToSlice(g Group) []K {
	gg := f.cast(g)
	out := make([]K, len(gg))
	copy(out, gg)
	return out
}

func (f fixedFormat[K]) SerializeGroup(w *bin.Writer, g Group) {
	for _, v := range f.cast(g) {
		f.write(w, v)
	}
}

func (f fixedFormat[K]) DeserializeGroup(r *bin.Reader, count int) Group {
	groupAlloc(count, f.width, r.Remaining())
	out := make([]K, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, f.read(r))
	}
	return Group(out)
}

func (f fixedFormat[K]) BytesSearch(r *bin.Reader, count int, key K) (int, bool) {
	checkGroupCount(count)
	start := r.Pos()
	lo, hi := 0, count-1
	found, ins := -1, 0
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		r.Seek(elemOff(start, mid, f.width))
		v := f.read(r)
		c := f.cmp(v, key)
		switch {
		case c == 0:
			found = mid
			hi = -1 // break
		case c < 0:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	if found < 0 {
		ins = lo
	}
	r.Seek(elemOff(start, count, f.width)) // leave at group end
	if found >= 0 {
		return found, true
	}
	return ins, false
}

func (f fixedFormat[K]) BytesGet(r *bin.Reader, count int, idx int) K {
	checkGroupCount(count)
	if idx < 0 || idx >= count {
		panic(mapdb5.NewCorruption("group index out of range"))
	}
	start := r.Pos()
	r.Seek(elemOff(start, idx, f.width))
	v := f.read(r)
	r.Seek(elemOff(start, count, f.width)) // leave at group end
	return v
}

func (f fixedFormat[K]) RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[K] {
	checkGroupCount(count)
	rangeBounds(count, from, to)
	return newBinaryGetCursor[K](f, r, count, from, to)
}

// LongFormat: int64, 8-byte BE stride, signed order.
var LongFormat = fixedFormat[int64]{
	desc:  "LONG",
	width: 8,
	elem:  Long,
	write: func(w *bin.Writer, v int64) { w.WriteLong(v) },
	read:  func(r *bin.Reader) int64 { return r.ReadLong() },
	cmp:   cmp.Compare[int64],
}

// IntFormat: int32, 4-byte BE stride, signed order.
var IntFormat = fixedFormat[int32]{
	desc:  "INT",
	width: 4,
	elem:  Int32,
	write: func(w *bin.Writer, v int32) { w.WriteInt(v) },
	read:  func(r *bin.Reader) int32 { return r.ReadInt() },
	cmp:   cmp.Compare[int32],
}

// ShortFormat: int16, 2-byte BE stride, signed order.
var ShortFormat = fixedFormat[int16]{
	desc:  "SHORT",
	width: 2,
	elem:  Short,
	write: func(w *bin.Writer, v int16) { w.WriteShort(v) },
	read:  func(r *bin.Reader) int16 { return r.ReadShort() },
	cmp:   cmp.Compare[int16],
}

// CharFormat: uint16, 2-byte BE stride, unsigned order.
var CharFormat = fixedFormat[uint16]{
	desc:  "CHAR",
	width: 2,
	elem:  Char,
	write: func(w *bin.Writer, v uint16) { w.WriteChar(v) },
	read:  func(r *bin.Reader) uint16 { return r.ReadU16() },
	cmp:   cmp.Compare[uint16],
}

// UUIDFormat: [16]byte, 16-byte BE stride, signed (msb, then lsb) order.
var UUIDFormat = fixedFormat[[16]byte]{
	desc:  "UUID",
	width: 16,
	elem:  UUID,
	write: func(w *bin.Writer, v [16]byte) { w.WriteAll(v[:]) },
	read: func(r *bin.Reader) [16]byte {
		var u [16]byte
		r.ReadFully(u[:])
		return u
	},
	cmp: compareUUID,
}
