package ser

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// Front-coded (shared-prefix compressed) group formats — Java
// StringPrefixFormat / ByteArrayPrefixFormat, the LevelDB/RocksDB block
// style. Every entry is `packInt(sharedPrefixLen) packInt(suffixLen) suffix`,
// where sharedPrefixLen is the byte length shared with the PREVIOUS entry.
// Entry i with i % RESTART_INTERVAL == 0 is a restart: shared is forced to 0
// so it decodes without history and is addressable via the restart table.
//
// Wire: `i32 blobLen; i32 restartOff[ceil(n/K)]; byte blob[blobLen]`.

// restartInterval is K — restart every K entries (LevelDB default).
const restartInterval = 16

// nRestarts = ceil(count / K), overflow-free (untrusted count).
func nRestarts(count int) int {
	return count/restartInterval + b2i(count%restartInterval != 0)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

type prefixFormat[K any] struct {
	elem       Serializer[K]
	encode     func(v K) []byte
	decode     func(b []byte) K
	cmp        func(a, b K) int
	clone      func(v K) K                                   // element deep-copy at group boundaries; nil for immutable K
	cmpStored  func(r *bin.Reader, storedLen int, key K) int // restart entry vs key
	cmpScratch func(scratch []byte, key K) int               // reconstructed entry vs key
	name       string
	desc       string // Java-registered catalog descriptor
}

func (f prefixFormat[K]) cloneElem(v K) K {
	if f.clone != nil {
		return f.clone(v)
	}
	return v
}

func (f prefixFormat[K]) cast(g Group) []K {
	gg, ok := g.([]K)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "foreign group passed to "+f.name))
	}
	return gg
}

func (f prefixFormat[K]) Element() Serializer[K]    { return f.elem }
func (f prefixFormat[K]) Compare(a, b K) int        { return f.cmp(a, b) }
func (f prefixFormat[K]) SupportsBinary() bool      { return true }
func (f prefixFormat[K]) SupportsRangeCursor() bool { return true }
func (f prefixFormat[K]) Empty() Group              { return []K{} }
func (f prefixFormat[K]) Size(g Group) int          { return len(f.cast(g)) }
func (f prefixFormat[K]) Get(g Group, idx int) K {
	gg := f.cast(g)
	if idx < 0 || idx >= len(gg) {
		panic(mapdb5.NewMisuse("group get index out of range"))
	}
	return f.cloneElem(gg[idx])
}
func (f prefixFormat[K]) Search(g Group, key K) (int, bool) {
	return bsearch(f.cast(g), key, f.cmp)
}
func (f prefixFormat[K]) Insert(g Group, idx int, key K) Group {
	return Group(sliceInsert(f.cast(g), idx, f.cloneElem(key)))
}
func (f prefixFormat[K]) Set(g Group, idx int, key K) Group {
	return Group(sliceSet(f.cast(g), idx, f.cloneElem(key)))
}
func (f prefixFormat[K]) Delete(g Group, idx int) Group {
	return Group(sliceDelete(f.cast(g), idx))
}
func (f prefixFormat[K]) CopyRange(g Group, from, to int) Group {
	return Group(sliceCopyRange(f.cast(g), from, to))
}
func (f prefixFormat[K]) FromSlice(keys []K) Group {
	out := make([]K, len(keys))
	for i, k := range keys {
		out[i] = f.cloneElem(k)
	}
	return Group(out)
}
func (f prefixFormat[K]) ToSlice(g Group) []K {
	gg := f.cast(g)
	out := make([]K, len(gg))
	for i, k := range gg {
		out[i] = f.cloneElem(k)
	}
	return out
}

func (f prefixFormat[K]) SerializeGroup(w *bin.Writer, g Group) {
	gg := f.cast(g)
	n := len(gg)
	nrest := (n + restartInterval - 1) / restartInterval
	restOff := make([]int32, nrest)
	blob := bin.NewWriterHint(max(n*8, 16))
	var prev []byte
	for i, v := range gg {
		enc := f.encode(v)
		var shared int
		if i%restartInterval == 0 {
			restOff[i/restartInterval] = int32(blob.Pos)
			shared = 0
		} else {
			shared = commonPrefixLen(prev, enc)
		}
		blob.PackInt(int32(shared))
		blob.PackInt(int32(len(enc) - shared))
		blob.WriteAll(enc[shared:])
		prev = enc
	}
	w.WriteInt(int32(blob.Pos))
	for _, off := range restOff {
		w.WriteInt(off)
	}
	w.WriteAll(blob.Bytes())
}

// prefixLayout is the decoded geometry of a front-coded group.
type prefixLayout struct {
	blobLen  int
	restBase int
	blobBase int
	end      int // one past blob end
	nrest    int
}

func (f prefixFormat[K]) readLayout(r *bin.Reader, count int) prefixLayout {
	checkGroupCount(count)
	start := r.Pos()
	blobLen := r.ReadInt()
	if blobLen < 0 {
		panic(mapdb5.NewCorruption("corrupt " + f.name + " blobLen"))
	}
	nrest := nRestarts(count)
	restBase := start + 4
	blobBase := elemOff(restBase, nrest, 4)
	end := blobBase + int(blobLen)
	if end < blobBase {
		panic(mapdb5.NewCorruption("corrupt " + f.name + " seek overflow"))
	}
	return prefixLayout{int(blobLen), restBase, blobBase, end, nrest}
}

// seekRestart positions r at restart rr's entry, validating the offset.
func (f prefixFormat[K]) seekRestart(r *bin.Reader, l prefixLayout, rr int) {
	r.Seek(elemOff(l.restBase, rr, 4))
	off := r.ReadInt()
	if off < 0 || int(off) > l.blobLen {
		panic(mapdb5.NewCorruption("corrupt " + f.name + " restart offset"))
	}
	r.Seek(l.blobBase + int(off))
}

// readEntry decodes one entry into scratch (scratch holds the previous
// entry's bytes on entry; restart entries must carry shared == 0). Every
// length is clamped: garbage errors, allocation stays bounded by the blob.
func (f prefixFormat[K]) readEntry(r *bin.Reader, scratch *[]byte, end int, restart bool) {
	shared := r.UnpackInt()
	if shared < 0 || (func() bool {
		if restart {
			return shared != 0
		}
		return int(shared) > len(*scratch)
	})() {
		panic(mapdb5.NewCorruption("corrupt " + f.name + " sharedLen"))
	}
	suffixLen := r.UnpackInt()
	// Subtraction form: pos+suffixLen could overflow on garbage. If pos > end
	// the remainder is negative, so any suffixLen errors (matches Java).
	rem := end - r.Pos()
	if suffixLen < 0 || rem < 0 || int(suffixLen) > rem {
		panic(mapdb5.NewCorruption("corrupt " + f.name + " suffixLen"))
	}
	newLen := int(shared) + int(suffixLen)
	s := *scratch
	if cap(s) < newLen {
		s = make([]byte, newLen, newLen+32)
		copy(s, (*scratch)[:shared])
	} else {
		s = s[:newLen]
	}
	r.ReadFully(s[shared:newLen])
	*scratch = s
}

// compareRestart compares restart entry rr against key in place (restarts
// carry shared == 0, so the stored suffix IS the full key).
func (f prefixFormat[K]) compareRestart(r *bin.Reader, l prefixLayout, rr int, key K) int {
	f.seekRestart(r, l, rr)
	if shared := r.UnpackInt(); shared != 0 {
		panic(mapdb5.NewCorruption("corrupt " + f.name + " restart sharedLen"))
	}
	suffixLen := r.UnpackInt()
	rem := l.end - r.Pos()
	if suffixLen < 0 || rem < 0 || int(suffixLen) > rem {
		panic(mapdb5.NewCorruption("corrupt " + f.name + " restart suffixLen"))
	}
	return f.cmpStored(r, int(suffixLen), key)
}

func (f prefixFormat[K]) DeserializeGroup(r *bin.Reader, count int) Group {
	groupAlloc(count, 1, r.Remaining())
	l := f.readLayout(r, count)
	r.SkipBytes(l.nrest * 4) // sequential decode does not need the restart table
	out := make([]K, 0, count)
	var cur []byte
	for i := 0; i < count; i++ {
		f.readEntry(r, &cur, l.end, i%restartInterval == 0)
		out = append(out, f.decode(cur))
	}
	return Group(out)
}

func (f prefixFormat[K]) BytesSearch(r *bin.Reader, count int, key K) (int, bool) {
	l := f.readLayout(r, count)

	// 1. Binary-search the restarts for the RIGHTMOST restart entry <= key.
	lo, hi := 0, l.nrest-1
	rr := -1
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		if f.compareRestart(r, l, mid, key) <= 0 {
			rr = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if rr < 0 {
		r.Seek(l.end) // key sorts below the first entry
		return 0, false
	}

	// 2. Roll forward through interval rr (<= K entries), reconstructing.
	first := rr * restartInterval
	limit := min(first+restartInterval, count)
	f.seekRestart(r, l, rr)
	var scratch []byte
	resultIdx, resultFound := limit, false // key above the whole interval
	for i := first; i < limit; i++ {
		f.readEntry(r, &scratch, l.end, i == first)
		c := f.cmpScratch(scratch, key)
		if c == 0 {
			resultIdx, resultFound = i, true
			break
		}
		if c > 0 {
			resultIdx = i
			break
		}
	}
	r.Seek(l.end)
	return resultIdx, resultFound
}

func (f prefixFormat[K]) BytesGet(r *bin.Reader, count int, idx int) K {
	l := f.readLayout(r, count)
	if idx < 0 || idx >= count {
		panic(mapdb5.NewCorruption(f.name + " index out of range"))
	}
	rr := idx / restartInterval
	f.seekRestart(r, l, rr)
	var scratch []byte
	for i := rr * restartInterval; i <= idx; i++ {
		f.readEntry(r, &scratch, l.end, i == rr*restartInterval)
	}
	r.Seek(l.end)
	return f.decode(scratch)
}

func (f prefixFormat[K]) RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[K] {
	checkGroupCount(count)
	rangeBounds(count, from, to)
	return newBinaryGetCursor[K](f, r, count, from, to)
}

// StringPrefixFormat: front-coded string group; UTF-16 code-unit order
// matched byte-side via compareUTF8.
var StringPrefixFormat = prefixFormat[string]{
	desc:   "STRING_PREFIX",
	elem:   String,
	encode: encodeStringUTF8,
	decode: lossyUTF8,
	cmp:    compareUTF16,
	cmpStored: func(r *bin.Reader, storedLen int, key string) int {
		return compareUTF8(r, storedLen, utf16Units(key))
	},
	cmpScratch: func(scratch []byte, key string) int {
		return compareUTF8(bin.NewReader(scratch), len(scratch), utf16Units(key))
	},
	name: "prefix group",
}

// ByteArrayPrefixFormat: front-coded []byte group; UNSIGNED lexicographic
// order on both sides.
var ByteArrayPrefixFormat = prefixFormat[[]byte]{
	desc:   "BYTE_ARRAY_PREFIX",
	elem:   ByteArrayUnsigned,
	encode: func(v []byte) []byte { return v },
	decode: func(b []byte) []byte {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	},
	cmp: compareBytesUnsigned,
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
	cmpScratch: func(scratch []byte, key []byte) int {
		return compareBytesUnsigned(scratch, key)
	},
	name: "byte[] prefix group",
}
