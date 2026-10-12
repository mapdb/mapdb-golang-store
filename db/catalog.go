// The DB name catalog — a sorted string map stored at recid 1, byte-for-byte
// compatible with Java DB.CATALOG_SER ("MDBC" v1).
//
// Wire format:
//
//	magic:   u32 big-endian  0x4D444243 ("MDBC")
//	version: u32 big-endian  1
//	repr:    u8              0            (REPR_INLINE)
//	count:   packInt         number of entries
//	entries: count × (key, value)         sorted ascending by key
//	  each string = packInt(utf8-byte-len) ++ utf8 bytes  (Serializers.STRING)
//
// MapDB's packInt puts 7 data bits per byte, most-significant group first,
// and sets the high bit 0x80 on the TERMINAL byte (NOT LEB128); packInt(0)
// is the single byte 0x80. An empty catalog is exactly the 10 bytes
// 4D 44 42 43 00 00 00 01 00 80.
//
// Entries serialize in ascending byte order, which equals Java's String
// natural (UTF-16 code-unit) order because all catalog keys are restricted
// to ASCII ([A-Za-z0-9._-] name + '#' + ASCII parameter suffix).
//
// The decoder is defensive: it rejects records shorter than 10 bytes, a bad
// magic/version/representation, a count over maxCatalogEntries, a string
// length crossing the record end, invalid UTF-8, duplicate keys, a
// non-canonical or overflowing packed value, and any trailing bytes.
package db

import (
	"sort"
	"unicode/utf8"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// RecidCatalog is the reserved recid holding the name catalog (Java
// DB.RECID_CATALOG).
const RecidCatalog uint64 = 1

const (
	catalogMagic      uint32 = 0x4D444243 // "MDBC"
	catalogVersion    int32  = 1
	reprInline        byte   = 0
	maxCatalogEntries uint64 = 10_000_000
	// magic(4) + version(4) + repr(1) + at least one count byte.
	minCatalogLen = 10
	// A valid packed u64 occupies at most ceil(64/7) == 10 bytes; a longer
	// run is corruption, not a value whose high bits we may discard.
	maxPackedBytes = 10
)

// NameCatalog maps `name#param` → value, the DB's persistent registry of
// named objects.
type NameCatalog map[string]string

// catalogSer is the catalog codec (Java DB.CATALOG_SER). Not a registered
// serializer; only the DB facade uses it, at recid 1.
type catalogSer struct{}

func writeCatalogString(w *bin.Writer, s string) {
	w.PackInt(int32(len(s)))
	w.WriteAll([]byte(s))
}

// readBoundedPacked decodes a packed varint bounded so it cannot read past
// end (Java DB.readBoundedPackedLong). Same bit layout as UnpackUint64, but
// rejects a non-canonical/overlong/overflowing run instead of shifting
// indefinitely.
func readBoundedPacked(r *bin.Reader, end int) uint64 {
	var ret uint64
	for i := 0; i < maxPackedBytes; i++ {
		if r.Pos() >= end {
			panic(mapdb5.NewCorruption("catalog packed value runs past record end"))
		}
		v := r.ReadU8()
		group := uint64(v & 0x7F)
		terminal := v&0x80 != 0
		// Canonicality: an MSB-first packed value never begins with an
		// all-zero group unless the value IS zero (a single terminal byte).
		if i == 0 && group == 0 && !terminal {
			panic(mapdb5.NewCorruption("catalog packed value has a non-canonical leading zero"))
		}
		// Checked accumulation on EVERY byte (MSB-first: the overflow can
		// land on any byte, not just the 10th).
		hi := ret >> (64 - 7)
		if hi != 0 {
			panic(mapdb5.NewCorruption("catalog packed value overflows 64 bits"))
		}
		ret = ret*128 + group
		if terminal {
			return ret
		}
	}
	panic(mapdb5.NewCorruption("catalog packed value is overlong (unterminated)"))
}

// readBoundedString reads a packInt-framed UTF-8 string bounded by end.
func readBoundedString(r *bin.Reader, end int) string {
	n := readBoundedPacked(r, end)
	if n > uint64(end-r.Pos()) {
		panic(mapdb5.NewCorruption("catalog string crosses record end"))
	}
	b := r.ReadBytes(int(n))
	// Strict UTF-8 decode: malformed bytes are catalog corruption, never
	// silently repaired (Java's encoder only emits valid UTF-8).
	if !utf8.Valid(b) {
		panic(mapdb5.NewCorruption("catalog string is not valid UTF-8"))
	}
	return string(b)
}

// Serialize writes the catalog: header, packed count, sorted entries.
func (catalogSer) Serialize(w *bin.Writer, cat NameCatalog) {
	w.WriteUint32(catalogMagic)
	w.WriteInt(catalogVersion)
	w.WriteU8(reprInline)
	w.PackInt(int32(len(cat)))
	keys := make([]string, 0, len(cat))
	for k := range cat {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		writeCatalogString(w, k)
		writeCatalogString(w, cat[k])
	}
}

// Deserialize decodes and defensively validates a catalog record. r is
// positioned at content start; size is the record's content length.
func (catalogSer) Deserialize(r *bin.Reader, size int) NameCatalog {
	if size < minCatalogLen {
		panic(mapdb5.NewCorruption("catalog record shorter than 10 bytes"))
	}
	start := r.Pos()
	end := start + size
	if end > r.Len() {
		panic(mapdb5.NewCorruption("catalog record end exceeds buffer"))
	}
	if r.ReadUint32() != catalogMagic {
		panic(mapdb5.NewCorruption("catalog magic mismatch (not a MapDB catalog)"))
	}
	if r.ReadInt() != catalogVersion {
		panic(mapdb5.NewCorruption("unsupported catalog version"))
	}
	if r.ReadU8() != reprInline {
		panic(mapdb5.NewCorruption("unsupported catalog representation"))
	}
	count := readBoundedPacked(r, end)
	if count > maxCatalogEntries {
		panic(mapdb5.NewCorruption("catalog entry count exceeds maximum"))
	}
	cat := make(NameCatalog, count)
	for i := uint64(0); i < count; i++ {
		k := readBoundedString(r, end)
		v := readBoundedString(r, end)
		if _, dup := cat[k]; dup {
			panic(mapdb5.NewCorruption("duplicate key in catalog"))
		}
		cat[k] = v
	}
	// No trailing or short bytes (Java asserts pos == start+size).
	if r.Pos() != end {
		panic(mapdb5.NewCorruption("catalog has trailing bytes"))
	}
	return cat
}

func (catalogSer) FixedSize() int { return -1 }
func (catalogSer) SizeHint() int  { return 256 }
func (catalogSer) Compare(a, b NameCatalog) int {
	// The catalog is never used as a key; ordering is irrelevant.
	return 0
}
func (catalogSer) Equals(a, b NameCatalog) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
func (catalogSer) NaturalOrder() bool            { return false }
func (catalogSer) EqualsBySerializedBytes() bool { return true }
