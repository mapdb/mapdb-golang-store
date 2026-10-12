package ser

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// ObjectArrayFormat is the generic fallback group format over any element
// serializer (Java ObjectArrayFormat). SupportsBinary is false: callers must
// deserialize before searching — there is no silent fallback.
type ObjectArrayFormat[K any] struct {
	Elem Serializer[K]
}

// NewObjectArrayFormat builds a generic group format over an element codec.
func NewObjectArrayFormat[K any](element Serializer[K]) ObjectArrayFormat[K] {
	return ObjectArrayFormat[K]{Elem: element}
}

func (f ObjectArrayFormat[K]) cast(g Group) []K {
	gg, ok := g.([]K)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "foreign group passed to ObjectArrayFormat"))
	}
	return gg
}

func (f ObjectArrayFormat[K]) Element() Serializer[K] { return f.Elem }
func (f ObjectArrayFormat[K]) Compare(a, b K) int     { return f.Elem.Compare(a, b) }
func (f ObjectArrayFormat[K]) SupportsBinary() bool   { return false }
func (f ObjectArrayFormat[K]) SupportsRangeCursor() bool {
	return false
}
func (f ObjectArrayFormat[K]) Empty() Group     { return []K{} }
func (f ObjectArrayFormat[K]) Size(g Group) int { return len(f.cast(g)) }
func (f ObjectArrayFormat[K]) Get(g Group, idx int) K {
	gg := f.cast(g)
	if idx < 0 || idx >= len(gg) {
		panic(mapdb5.NewMisuse("group get index out of range"))
	}
	return gg[idx]
}
func (f ObjectArrayFormat[K]) Search(g Group, key K) (int, bool) {
	return bsearch(f.cast(g), key, f.Elem.Compare)
}
func (f ObjectArrayFormat[K]) Insert(g Group, idx int, key K) Group {
	return Group(sliceInsert(f.cast(g), idx, key))
}
func (f ObjectArrayFormat[K]) Set(g Group, idx int, key K) Group {
	return Group(sliceSet(f.cast(g), idx, key))
}
func (f ObjectArrayFormat[K]) Delete(g Group, idx int) Group {
	return Group(sliceDelete(f.cast(g), idx))
}
func (f ObjectArrayFormat[K]) CopyRange(g Group, from, to int) Group {
	return Group(sliceCopyRange(f.cast(g), from, to))
}
func (f ObjectArrayFormat[K]) FromSlice(keys []K) Group {
	out := make([]K, len(keys))
	copy(out, keys)
	return Group(out)
}
func (f ObjectArrayFormat[K]) ToSlice(g Group) []K {
	gg := f.cast(g)
	out := make([]K, len(gg))
	copy(out, gg)
	return out
}

func (f ObjectArrayFormat[K]) SerializeGroup(w *bin.Writer, g Group) {
	for _, v := range f.cast(g) {
		f.Elem.Serialize(w, v)
	}
}

func (f ObjectArrayFormat[K]) DeserializeGroup(r *bin.Reader, count int) Group {
	// Each element occupies ≥1 encoded byte for any working serializer — a
	// 0-byte-per-element codec could not round-trip through a group at all.
	groupAlloc(count, 1, r.Remaining())
	out := make([]K, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, f.Elem.Deserialize(r, SelfDelimit))
	}
	return Group(out)
}

func (f ObjectArrayFormat[K]) BytesSearch(r *bin.Reader, count int, key K) (int, bool) {
	panic(mapdb5.New(mapdb5.KindUnsupported, "format does not support binary access"))
}

func (f ObjectArrayFormat[K]) BytesGet(r *bin.Reader, count int, idx int) K {
	panic(mapdb5.New(mapdb5.KindUnsupported, "format does not support binary access"))
}

func (f ObjectArrayFormat[K]) RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[K] {
	panic(mapdb5.New(mapdb5.KindUnsupported, "format does not support range cursor"))
}
