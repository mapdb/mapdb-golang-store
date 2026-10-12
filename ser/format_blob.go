package ser

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// Blob + fixed-width i32 offset table group formats (Java StringGroupFormat,
// ByteArrayFormat). Wire: `i32 blobLen; i32 off[n]; blob[blobLen]`.
//
// StringGroupFormat orders by UTF-16 code units (String.compareTo) and
// compares stored UTF-8 in place via compareUTF8. ByteArrayFormat orders by
// UNSIGNED lexicographic (memcmp) on both sides — its element serializer is
// ByteArrayUnsigned, so element order == search order == byte-side order.

// blobLayout holds the decoded wire geometry of one blob group.
type blobLayout struct {
	start    int
	blobLen  int
	offBase  int
	blobBase int
	blobEnd  int
}

// readBlobLayout reads the blobLen header and computes the layout. r must be
// positioned at group start.
func readBlobLayout(r *bin.Reader, count int, corrupt string) blobLayout {
	checkGroupCount(count)
	start := r.Pos()
	blobLen := r.ReadInt()
	if blobLen < 0 {
		panic(mapdb5.NewCorruption(corrupt + " blobLen"))
	}
	offBase := start + 4
	blobBase := elemOff(offBase, count, 4)
	blobEnd := blobBase + int(blobLen)
	if blobEnd < blobBase {
		panic(mapdb5.NewCorruption(corrupt + " seek overflow"))
	}
	return blobLayout{start, int(blobLen), offBase, blobBase, blobEnd}
}

// blobRange returns the [s,e) byte range of element idx inside the blob,
// validating offsets (contiguous offsets: e reads the next slot in place).
func blobRange(r *bin.Reader, l blobLayout, count, idx int, corrupt string) (int, int) {
	r.Seek(elemOff(l.offBase, idx, 4))
	s := r.ReadInt()
	var e int32
	if idx+1 < count {
		e = r.ReadInt()
	} else {
		e = int32(l.blobLen)
	}
	if s < 0 || e < s || int(e) > l.blobLen {
		panic(mapdb5.NewCorruption(corrupt + " offsets"))
	}
	return int(s), int(e)
}

// blobFormat is the generic blob+offset-table implementation. The element is
// stored as UTF-8 bytes (String) or raw bytes (ByteArray); the materialized
// group is []K where K is string or []byte.
type blobFormat[K any] struct {
	elem   Serializer[K]
	encode func(v K) []byte // element → stored bytes
	decode func(b []byte) K // stored bytes → element (lossy for strings)
	cmp    func(a, b K) int // format-level order
	// clone deep-copies an element at group boundaries (FromSlice/ToSlice/
	// Get/Insert/Set) so []byte elements never alias caller or group storage
	// across the immutable-COW contract. nil for immutable K (string).
	clone func(v K) K
	// cmpStored compares a stored byte range in place against key, sign
	// convention stored - key. r is positioned at the element's blob bytes.
	cmpStored func(r *bin.Reader, storedLen int, key K) int
	name      string // for corruption messages
	desc      string // Java-registered catalog descriptor
}

func (f blobFormat[K]) cloneElem(v K) K {
	if f.clone != nil {
		return f.clone(v)
	}
	return v
}

func (f blobFormat[K]) cast(g Group) []K {
	gg, ok := g.([]K)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "foreign group passed to "+f.name))
	}
	return gg
}

func (f blobFormat[K]) Element() Serializer[K]    { return f.elem }
func (f blobFormat[K]) Compare(a, b K) int        { return f.cmp(a, b) }
func (f blobFormat[K]) SupportsBinary() bool      { return true }
func (f blobFormat[K]) SupportsRangeCursor() bool { return true }
func (f blobFormat[K]) Empty() Group              { return []K{} }
func (f blobFormat[K]) Size(g Group) int          { return len(f.cast(g)) }
func (f blobFormat[K]) Get(g Group, idx int) K {
	gg := f.cast(g)
	if idx < 0 || idx >= len(gg) {
		panic(mapdb5.NewMisuse("group get index out of range"))
	}
	return f.cloneElem(gg[idx])
}
func (f blobFormat[K]) Search(g Group, key K) (int, bool) {
	return bsearch(f.cast(g), key, f.cmp)
}
func (f blobFormat[K]) Insert(g Group, idx int, key K) Group {
	return Group(sliceInsert(f.cast(g), idx, f.cloneElem(key)))
}
func (f blobFormat[K]) Set(g Group, idx int, key K) Group {
	return Group(sliceSet(f.cast(g), idx, f.cloneElem(key)))
}
func (f blobFormat[K]) Delete(g Group, idx int) Group {
	return Group(sliceDelete(f.cast(g), idx))
}
func (f blobFormat[K]) CopyRange(g Group, from, to int) Group {
	return Group(sliceCopyRange(f.cast(g), from, to))
}
func (f blobFormat[K]) FromSlice(keys []K) Group {
	out := make([]K, len(keys))
	for i, k := range keys {
		out[i] = f.cloneElem(k)
	}
	return Group(out)
}
func (f blobFormat[K]) ToSlice(g Group) []K {
	gg := f.cast(g)
	out := make([]K, len(gg))
	for i, k := range gg {
		out[i] = f.cloneElem(k)
	}
	return out
}

func (f blobFormat[K]) SerializeGroup(w *bin.Writer, g Group) {
	gg := f.cast(g)
	blobLen := 0
	for _, v := range gg {
		blobLen += len(f.encode(v))
	}
	w.WriteInt(int32(blobLen))
	off := int32(0)
	for _, v := range gg {
		w.WriteInt(off)
		off += int32(len(f.encode(v)))
	}
	for _, v := range gg {
		w.WriteAll(f.encode(v))
	}
}

func (f blobFormat[K]) DeserializeGroup(r *bin.Reader, count int) Group {
	// The offset table alone needs 4 bytes per element (+4 for blobLen).
	groupAlloc(count, 4, r.Remaining())
	l := readBlobLayout(r, count, "corrupt "+f.name)
	off := make([]int32, count)
	for i := range off {
		off[i] = r.ReadInt()
	}
	blob := r.ReadBytes(l.blobLen)
	out := make([]K, 0, count)
	for i := 0; i < count; i++ {
		s := off[i]
		var e int32
		if i+1 < count {
			e = off[i+1]
		} else {
			e = int32(l.blobLen)
		}
		if s < 0 || e < s || int(e) > l.blobLen {
			panic(mapdb5.NewCorruption("corrupt " + f.name + " offsets"))
		}
		out = append(out, f.decode(blob[s:e]))
	}
	return Group(out)
}

func (f blobFormat[K]) BytesSearch(r *bin.Reader, count int, key K) (int, bool) {
	l := readBlobLayout(r, count, "corrupt "+f.name)
	lo, hi := 0, count-1
	found, ins := -1, 0
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		s, e := blobRange(r, l, count, mid, "corrupt "+f.name)
		r.Seek(l.blobBase + s)
		c := f.cmpStored(r, e-s, key)
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
	r.Seek(l.blobEnd) // leave at group end
	if found >= 0 {
		return found, true
	}
	return ins, false
}

func (f blobFormat[K]) BytesGet(r *bin.Reader, count int, idx int) K {
	l := readBlobLayout(r, count, "corrupt "+f.name)
	if idx < 0 || idx >= count {
		panic(mapdb5.NewCorruption(f.name + " index out of range"))
	}
	s, e := blobRange(r, l, count, idx, "corrupt "+f.name)
	r.Seek(l.blobBase + s)
	b := r.ReadBytes(e - s)
	r.Seek(l.blobEnd) // leave at group end
	return f.decode(b)
}

func (f blobFormat[K]) RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[K] {
	checkGroupCount(count)
	rangeBounds(count, from, to)
	return newBinaryGetCursor[K](f, r, count, from, to)
}

// StringGroupFormat is the string blob format: UTF-16 code-unit order,
// stored UTF-8 compared in place (strict RFC 3629 on stored bytes).
var StringGroupFormat = blobFormat[string]{
	desc:   "STRING",
	elem:   String,
	encode: encodeStringUTF8,
	decode: lossyUTF8,
	cmp:    compareUTF16,
	cmpStored: func(r *bin.Reader, storedLen int, key string) int {
		return compareUTF8(r, storedLen, utf16Units(key))
	},
	name: "string group",
}

// ByteArrayFormat is the []byte blob format: UNSIGNED lexicographic
// (memcmp) order on both sides; element serializer is ByteArrayUnsigned.
var ByteArrayFormat = blobFormat[[]byte]{
	desc:   "BYTE_ARRAY",
	elem:   ByteArrayUnsigned,
	encode: func(v []byte) []byte { return v },
	decode: func(b []byte) []byte {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	},
	cmp: func(a, b []byte) int { return compareBytesUnsigned(a, b) },
	clone: func(b []byte) []byte {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	},
	cmpStored: func(r *bin.Reader, storedLen int, key []byte) int {
		n := min(storedLen, len(key))
		for i := 0; i < n; i++ {
			c := int(r.ReadU8()) - int(key[i])
			if c != 0 {
				if c < 0 {
					return -1
				}
				return 1
			}
		}
		return cmpIntLen(storedLen, len(key))
	},
	name: "byte[] group",
}

func cmpIntLen(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
