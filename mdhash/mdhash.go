// Package mdhash ports Java org.mapdb.hash: the (key,seed) → hash-bits
// functions HTreeMap variants use for segment + DirTree placement.
//
// Placement hashes are WIRE FORMAT: a Go HTree must land every key in the
// same bucket Java would choose, so the built-in hashers reproduce Java's
// hash functions bit-for-bit — including java.lang.String.hashCode and
// Arrays.hashCode for the 32-bit map, and the documented FNV-1a-64 +
// murmur3-fmix64 functions for the 64-bit variants.
//
// There is no hashCode() in Go: Java's default objectHasher
// (fmix32(key.hashCode() ^ seed)) has no universal equivalent. Only the
// hashers defined here are wire-stable across languages; a custom
// Mixing/HashingStrategy adapter is Go-only placement and must be called
// out as such (spec 04).
package mdhash

import (
	"unicode/utf16"

	"github.com/mapdb/mapdb-golang/object"
	"github.com/mapdb/mapdb5-go/ser"
)

// Hasher is Java hash.Hasher: a seeded 32-bit placement hash.
type Hasher[K any] interface {
	Hash(key K, seed int32) int32
}

// Hasher64 is Java hash.Hasher64: a seeded 64-bit placement hash.
type Hasher64[K any] interface {
	Hash64(key K, seed int64) int64
}

// HasherFunc adapts a function to Hasher.
type HasherFunc[K any] func(key K, seed int32) int32

// Hash implements Hasher.
func (f HasherFunc[K]) Hash(key K, seed int32) int32 { return f(key, seed) }

// Hasher64Func adapts a function to Hasher64.
type Hasher64Func[K any] func(key K, seed int64) int64

// Hash64 implements Hasher64.
func (f Hasher64Func[K]) Hash64(key K, seed int64) int64 { return f(key, seed) }

// ============================ finalizers ============================

// Fmix32 is the murmur3 32-bit finalizer (Java Hashers.fmix32). Computed in
// uint32 — Java's >>> is an unsigned shift and the multipliers are
// unsigned 32-bit constants.
func Fmix32(h int32) int32 {
	u := uint32(h)
	u ^= u >> 16
	u *= 0x85ebca6b
	u ^= u >> 13
	u *= 0xc2b2ae35
	u ^= u >> 16
	return int32(u)
}

// Fmix64 is the murmur3 64-bit finalizer (Java Hashers.fmix64). Computed in
// uint64 for the same unsigned-shift/constant reasons.
func Fmix64(h int64) int64 {
	u := uint64(h)
	u ^= u >> 33
	u *= 0xff51afd7ed558ccd
	u ^= u >> 33
	u *= 0xc4ceb9fe1a85ec53
	u ^= u >> 33
	return int64(u)
}

// ============================ Java content hashes ============================

// JavaStringHashCode reproduces java.lang.String.hashCode(): h = 31*h + c
// over UTF-16 code units (NOT runes — supplementary characters hash as
// their surrogate pair, exactly like Java's char loop).
func JavaStringHashCode(s string) int32 {
	var h int32
	for _, u := range utf16.Encode([]rune(s)) {
		// char is unsigned 16-bit: zero-extend into the int accumulator.
		h = 31*h + int32(u)
	}
	return h
}

// JavaByteArrayHashCode reproduces java.util.Arrays.hashCode(byte[]):
// result = 31*result + element, starting from 1. Java sign-extends the
// byte element into the int accumulator.
func JavaByteArrayHashCode(b []byte) int32 {
	h := int32(1)
	for _, x := range b {
		h = 31*h + int32(int8(x))
	}
	return h
}

// JavaLongHashCode reproduces java.lang.Long.hashCode(): v ^ (v >>> 32).
func JavaLongHashCode(v int64) int32 {
	return int32(v ^ int64(uint64(v)>>32))
}

// ============================ 32-bit hashers (HTreeMap) ============================

// Int32 is the Java Integer path: fmix32(key ^ seed) — Integer.hashCode is
// the value itself.
var Int32 = HasherFunc[int32](func(key int32, seed int32) int32 {
	return Fmix32(key ^ seed)
})

// Long32 is the Java Long path: Long.hashCode folds to 32 bits, then the
// seeded fmix32 (Java Hashers.objectHasher over Long).
var Long32 = HasherFunc[int64](func(key int64, seed int32) int32 {
	return Fmix32(JavaLongHashCode(key) ^ seed)
})

// String32 is the Java String path: String.hashCode over UTF-16 code
// units, then seeded fmix32 (Java Hashers.objectHasher over String).
var String32 = HasherFunc[string](func(key string, seed int32) int32 {
	return Fmix32(JavaStringHashCode(key) ^ seed)
})

// ByteArray32 is Java Hashers.mixing(Arrays::hashCode): the byte-array
// content hash, then seeded fmix32.
var ByteArray32 = HasherFunc[[]byte](func(key []byte, seed int32) int32 {
	return Fmix32(JavaByteArrayHashCode(key) ^ seed)
})

// Mixing wraps any content-hash function with seed-xor + fmix32 (Java
// Hashers.mixing). Placement from a custom content hash is NOT Java-wire
// stable unless the content hash reproduces a Java hashCode.
func Mixing[K any](contentHash func(K) int32) Hasher[K] {
	return HasherFunc[K](func(key K, seed int32) int32 {
		return Fmix32(contentHash(key) ^ seed)
	})
}

// ============================ 64-bit hashers (HTreeMap48/External) ============================

// Long64 is Java Hashers.LONG64: fmix64(key ^ seed) — full 64-bit entropy.
var Long64 = Hasher64Func[int64](func(key int64, seed int64) int64 {
	return Fmix64(key ^ seed)
})

// String64 is Java Hashers.STRING64: FNV-1a-64 over UTF-16 code units,
// then seeded fmix64.
var String64 = Hasher64Func[string](func(key string, seed int64) int64 {
	h := uint64(0xcbf29ce484222325)
	for _, u := range utf16.Encode([]rune(key)) {
		h = (h ^ uint64(u)) * 0x100000001b3
	}
	return Fmix64(int64(h) ^ seed)
})

// ByteArray64 is Java Hashers.BYTE_ARRAY64: FNV-1a-64 over the bytes,
// then seeded fmix64.
var ByteArray64 = Hasher64Func[[]byte](func(key []byte, seed int64) int64 {
	h := uint64(0xcbf29ce484222325)
	for _, b := range key {
		h = (h ^ uint64(b)) * 0x100000001b3
	}
	return Fmix64(int64(h) ^ seed)
})

// Mixing64 wraps any 64-bit content hash with seed-xor + fmix64 (Java
// Hashers.mixing64). Same wire-stability caveat as Mixing.
func Mixing64[K any](contentHash func(K) int64) Hasher64[K] {
	return Hasher64Func[K](func(key K, seed int64) int64 {
		return Fmix64(contentHash(key) ^ seed)
	})
}

// ============================ strategy adapter ============================

// FromHashingStrategy adapts a mapdb-golang object.HashingStrategy into a
// 64-bit placement hasher: strategy.HashCode(key) then seeded fmix64.
//
// The sibling module's built-in strategies use runtime-seeded maphash, so
// this adapter is NOT wire-stable across processes or languages — use it
// only for in-memory or Go-only placement (spec 04 calls this out loudly).
func FromHashingStrategy[K any](s object.HashingStrategy[K]) Hasher64[K] {
	return Hasher64Func[K](func(key K, seed int64) int64 {
		return Fmix64(int64(s.HashCode(key)) ^ seed)
	})
}

// ============================ serializer-bytes hashers ============================

// SerHasher32 hashes a key's SERIALIZED bytes through Java's
// Arrays.hashCode + seeded fmix32 dialect. It is deterministic across
// languages for a fixed serializer — the safe explicit default when no
// key-type-specific hasher exists — but it is NOT Java-hashCode-compatible
// (a Java file built with objectHasher placement will not resolve keys).
func SerHasher32[K any](s ser.Serializer[K]) Hasher[K] {
	return HasherFunc[K](func(key K, seed int32) int32 {
		return ByteArray32.Hash(ser.SerializeToBytes(s, key), seed)
	})
}

// SerHasher64 hashes a key's serialized bytes through the FNV-1a-64 +
// fmix64 dialect (Java BYTE_ARRAY64 over serialized content).
func SerHasher64[K any](s ser.Serializer[K]) Hasher64[K] {
	return Hasher64Func[K](func(key K, seed int64) int64 {
		return ByteArray64.Hash64(ser.SerializeToBytes(s, key), seed)
	})
}
