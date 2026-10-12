package ser

import (
	"math"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// TupleFormat — binary-capable group format for composite/tuple keys (Java
// TupleFormat + TupleComponent). Each tuple is encoded to an
// ORDER-PRESERVING (memcomparable) []byte by its component schema, so the
// UNSIGNED byte order of the encodings equals the logical tuple order. The
// group is then stored and searched exactly like a [][]byte group (blob +
// fixed-width i32 offset table, unsigned in-place compare with a length
// tie-break, giving (a) < (a,b) for prefix tuples).
//
// Per-component encoding:
//   - TupleInt / TupleLong: fixed-width big-endian, sign bit flipped — maps
//     the signed range onto the unsigned range monotonically.
//   - TupleStr / TupleBytes: escaped-terminated — each 0x00 payload byte
//     becomes 0x00 0xFF, others verbatim, then a 0x00 0x00 terminator.
//     STRING order is UTF-8 (code-point) order, which deliberately differs
//     from UTF-16 order for supplementary characters.

// TupleComponent is a typed component of a composite key providing an
// order-preserving per-component codec.
type TupleComponent int

const (
	// TupleInt: signed 32-bit int, 4-byte big-endian, sign bit flipped.
	TupleInt TupleComponent = iota
	// TupleLong: signed 64-bit long, 8-byte big-endian, sign bit flipped.
	TupleLong
	// TupleStr: UTF-8 string, escaped-terminated; code-point order.
	TupleStr
	// TupleBytes: raw bytes, escaped-terminated; unsigned lexicographic.
	TupleBytes
)

func (c TupleComponent) String() string {
	switch c {
	case TupleInt:
		return "int"
	case TupleLong:
		return "long"
	case TupleStr:
		return "string"
	case TupleBytes:
		return "bytes"
	default:
		return "unknown"
	}
}

// encode appends the memcomparable encoding of value to out. A value whose
// variant does not match this component is a caller bug (Java's
// ClassCastException).
func (c TupleComponent) encode(out *bin.Writer, v Value) {
	switch {
	case c == TupleInt && v.Kind == ValueInt:
		out.WriteInt(int32(v.Num) ^ math.MinInt32)
	case c == TupleLong && v.Kind == ValueLong:
		out.WriteLong(v.Num ^ math.MinInt64)
	case c == TupleStr && v.Kind == ValueString:
		writeEscaped(out, encodeStringUTF8(v.Str))
	case c == TupleBytes && v.Kind == ValueBytes:
		writeEscaped(out, v.Bytes)
	default:
		panic(mapdb5.NewMisuse("tuple component type mismatch: " + c.String()))
	}
}

// decode reads exactly one component from r, which must not advance past
// end (the exclusive end of the tuple's encoded bytes).
func (c TupleComponent) decode(r *bin.Reader, end int) Value {
	switch c {
	case TupleInt:
		if r.Pos()+4 > end {
			panic(mapdb5.NewCorruption("corrupt tuple: truncated int component"))
		}
		return IntValue(r.ReadInt() ^ math.MinInt32)
	case TupleLong:
		if r.Pos()+8 > end {
			panic(mapdb5.NewCorruption("corrupt tuple: truncated long component"))
		}
		return LongValue(r.ReadLong() ^ math.MinInt64)
	case TupleStr:
		return StringValue(lossyUTF8(readEscaped(r, end)))
	case TupleBytes:
		return BytesValue(readEscaped(r, end))
	default:
		panic(mapdb5.NewMisuse("unknown tuple component"))
	}
}

// compare is the logical order of this component, equal to the unsigned byte
// order of encode.
func (c TupleComponent) compare(a, b Value) int {
	switch {
	case c == TupleInt && a.Kind == ValueInt && b.Kind == ValueInt:
		return cmpInt32(int32(a.Num), int32(b.Num))
	case c == TupleLong && a.Kind == ValueLong && b.Kind == ValueLong:
		return cmpInt64(a.Num, b.Num)
	case c == TupleStr && a.Kind == ValueString && b.Kind == ValueString:
		// UTF-8 unsigned byte order == code-point order.
		return compareBytesUnsigned(encodeStringUTF8(a.Str), encodeStringUTF8(b.Str))
	case c == TupleBytes && a.Kind == ValueBytes && b.Kind == ValueBytes:
		return compareBytesUnsigned(a.Bytes, b.Bytes)
	default:
		panic(mapdb5.NewMisuse("tuple component type mismatch in compare"))
	}
}

func cmpInt32(a, b int32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// ---- escaped-terminated codec ----

func writeEscaped(out *bin.Writer, payload []byte) {
	for _, b := range payload {
		if b == 0x00 {
			out.WriteU8(0x00)
			out.WriteU8(0xFF)
		} else {
			out.WriteU8(b)
		}
	}
	// terminator 0x00 0x00 (never produced by an escaped 0x00, which is 0x00 0xFF)
	out.WriteU8(0x00)
	out.WriteU8(0x00)
}

func readEscaped(r *bin.Reader, end int) []byte {
	var buf []byte
	for {
		if r.Pos() >= end {
			panic(mapdb5.NewCorruption("corrupt tuple: unterminated component"))
		}
		b := r.ReadU8()
		if b == 0x00 {
			if r.Pos() >= end {
				panic(mapdb5.NewCorruption("corrupt tuple: dangling escape"))
			}
			b2 := r.ReadU8()
			if b2 == 0x00 {
				break // terminator
			}
			if b2 != 0xFF {
				panic(mapdb5.NewCorruption("corrupt tuple: bad escape"))
			}
			buf = append(buf, 0x00)
		} else {
			buf = append(buf, b)
		}
	}
	return buf
}

// encodeTuple returns the memcomparable encoding of tuple. Panics (Misuse)
// if the arity exceeds the schema.
func encodeTuple(schema []TupleComponent, tuple []Value) []byte {
	if len(tuple) > len(schema) {
		panic(mapdb5.NewMisuse("tuple arity exceeds schema"))
	}
	out := bin.NewWriter()
	for i, v := range tuple {
		schema[i].encode(out, v)
	}
	return out.CopyBytes()
}

// decodeTuple recovers a tuple from its memcomparable bytes, consuming
// components until the bytes are exhausted.
func decodeTuple(schema []TupleComponent, enc []byte) []Value {
	r := bin.NewReader(enc)
	var out []Value
	i := 0
	for r.Pos() < len(enc) {
		if i == len(schema) {
			panic(mapdb5.NewCorruption("corrupt tuple: more components than schema"))
		}
		out = append(out, schema[i].decode(r, len(enc)))
		i++
	}
	return out
}

// compareTuple is component-wise order; on an equal shared prefix the
// SHORTER tuple is smaller ((a) < (a,b)).
func compareTuple(schema []TupleComponent, a, b []Value) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if c := schema[i].compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return cmpIntLen(len(a), len(b))
}

// TupleFormat is a group format over an ordered component schema.
// Group = [][]byte holding each tuple's memcomparable encoding (== Java's
// byte[][]).
type TupleFormat struct {
	serializer TupleSerializer
}

// NewTupleFormat builds a tuple format over the given ordered component
// types. Panics (Misuse) on an empty schema (Java IllegalArgumentException).
func NewTupleFormat(components ...TupleComponent) TupleFormat {
	if len(components) == 0 {
		panic(mapdb5.NewMisuse("tuple schema must have at least one component"))
	}
	schema := make([]TupleComponent, len(components))
	copy(schema, components)
	return TupleFormat{serializer: TupleSerializer{Schema: schema}}
}

// Schema returns a defensive copy of the persisted component schema.
func (f TupleFormat) Schema() []TupleComponent {
	out := make([]TupleComponent, len(f.serializer.Schema))
	copy(out, f.serializer.Schema)
	return out
}

func (f TupleFormat) cast(g Group) [][]byte {
	gg, ok := g.([][]byte)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "foreign group passed to TupleFormat"))
	}
	return gg
}

func (f TupleFormat) Element() Serializer[[]Value] { return f.serializer }
func (f TupleFormat) Compare(a, b []Value) int {
	return compareTuple(f.serializer.Schema, a, b)
}
func (f TupleFormat) SupportsBinary() bool      { return true }
func (f TupleFormat) SupportsRangeCursor() bool { return true }
func (f TupleFormat) Empty() Group              { return [][]byte{} }
func (f TupleFormat) Size(g Group) int          { return len(f.cast(g)) }
func (f TupleFormat) Get(g Group, idx int) []Value {
	gg := f.cast(g)
	if idx < 0 || idx >= len(gg) {
		panic(mapdb5.NewMisuse("group get index out of range"))
	}
	return decodeTuple(f.serializer.Schema, gg[idx])
}
func (f TupleFormat) Search(g Group, key []Value) (int, bool) {
	probe := encodeTuple(f.serializer.Schema, key)
	return bsearch(f.cast(g), probe, compareBytesUnsigned)
}
func (f TupleFormat) Insert(g Group, idx int, key []Value) Group {
	return Group(sliceInsert(f.cast(g), idx, encodeTuple(f.serializer.Schema, key)))
}
func (f TupleFormat) Set(g Group, idx int, key []Value) Group {
	return Group(sliceSet(f.cast(g), idx, encodeTuple(f.serializer.Schema, key)))
}
func (f TupleFormat) Delete(g Group, idx int) Group {
	return Group(sliceDelete(f.cast(g), idx))
}
func (f TupleFormat) CopyRange(g Group, from, to int) Group {
	return Group(sliceCopyRange(f.cast(g), from, to))
}
func (f TupleFormat) FromSlice(keys [][]Value) Group {
	out := make([][]byte, len(keys))
	for i, t := range keys {
		out[i] = encodeTuple(f.serializer.Schema, t)
	}
	return Group(out)
}
func (f TupleFormat) ToSlice(g Group) [][]Value {
	gg := f.cast(g)
	out := make([][]Value, len(gg))
	for i, e := range gg {
		out[i] = decodeTuple(f.serializer.Schema, e)
	}
	return out
}

// Wire: blob + fixed-width i32 offset table (same shape as blobFormat).
func (f TupleFormat) SerializeGroup(w *bin.Writer, g Group) {
	gg := f.cast(g)
	blobLen := 0
	for _, e := range gg {
		blobLen += len(e)
	}
	w.WriteInt(int32(blobLen))
	off := int32(0)
	for _, e := range gg {
		w.WriteInt(off)
		off += int32(len(e))
	}
	for _, e := range gg {
		w.WriteAll(e)
	}
}

func (f TupleFormat) DeserializeGroup(r *bin.Reader, count int) Group {
	groupAlloc(count, 4, r.Remaining())
	l := readBlobLayout(r, count, "corrupt tuple group")
	off := make([]int32, count)
	for i := range off {
		off[i] = r.ReadInt()
	}
	blob := r.ReadBytes(l.blobLen)
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		s := off[i]
		var e int32
		if i+1 < count {
			e = off[i+1]
		} else {
			e = int32(l.blobLen)
		}
		if s < 0 || e < s || int(e) > l.blobLen {
			panic(mapdb5.NewCorruption("corrupt tuple group offsets"))
		}
		cell := make([]byte, e-s)
		copy(cell, blob[s:e])
		out = append(out, cell)
	}
	return Group(out)
}

func (f TupleFormat) BytesSearch(r *bin.Reader, count int, key []Value) (int, bool) {
	probe := encodeTuple(f.serializer.Schema, key)
	l := readBlobLayout(r, count, "corrupt tuple group")
	lo, hi := 0, count-1
	found, ins := -1, 0
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		s, e := blobRange(r, l, count, mid, "corrupt tuple group")
		r.Seek(l.blobBase + s)
		c := compareStoredUnsigned(r, e-s, probe)
		switch {
		case c == 0:
			found = mid
			hi = -1
		case c < 0:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	if found < 0 {
		ins = lo
	}
	r.Seek(l.blobEnd)
	if found >= 0 {
		return found, true
	}
	return ins, false
}

// compareStoredUnsigned: unsigned in-place compare of stored bytes vs probe.
func compareStoredUnsigned(r *bin.Reader, storedLen int, probe []byte) int {
	n := min(storedLen, len(probe))
	for i := 0; i < n; i++ {
		sb, pb := r.ReadU8(), probe[i]
		if sb != pb {
			if sb < pb {
				return -1
			}
			return 1
		}
	}
	return cmpIntLen(storedLen, len(probe))
}

func (f TupleFormat) BytesGet(r *bin.Reader, count int, idx int) []Value {
	l := readBlobLayout(r, count, "corrupt tuple group")
	if idx < 0 || idx >= count {
		panic(mapdb5.NewCorruption("tuple group index out of range"))
	}
	s, e := blobRange(r, l, count, idx, "corrupt tuple group")
	r.Seek(l.blobBase + s)
	b := r.ReadBytes(e - s)
	r.Seek(l.blobEnd)
	return decodeTuple(f.serializer.Schema, b)
}

func (f TupleFormat) RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[[]Value] {
	checkGroupCount(count)
	rangeBounds(count, from, to)
	return newBinaryGetCursor[[]Value](f, r, count, from, to)
}

// TupleSerializer is the self-delimiting standalone codec for a single
// tuple: packInt(len) + encoded.
type TupleSerializer struct {
	Schema []TupleComponent
}

func (s TupleSerializer) Serialize(w *bin.Writer, v []Value) {
	e := encodeTuple(s.Schema, v)
	w.PackInt(int32(len(e)))
	w.WriteAll(e)
}

func (s TupleSerializer) Deserialize(r *bin.Reader, _ int) []Value {
	length := r.UnpackInt()
	if length < 0 {
		panic(mapdb5.NewCorruption("corrupt tuple length"))
	}
	return decodeTuple(s.Schema, r.ReadBytes(int(length)))
}

func (s TupleSerializer) FixedSize() int { return -1 }
func (s TupleSerializer) SizeHint() int  { return 32 }
func (s TupleSerializer) Compare(a, b []Value) int {
	return compareTuple(s.Schema, a, b)
}
func (s TupleSerializer) Equals(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if s.Schema[i].compare(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}
func (s TupleSerializer) NaturalOrder() bool { return false }
func (s TupleSerializer) EqualsBySerializedBytes() bool {
	return true // memcomparable encoding is canonical
}
