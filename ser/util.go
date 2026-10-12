package ser

import (
	"unicode/utf16"
	"unicode/utf8"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// Shared ser helpers: zigzag, Java-exact comparisons, in-place UTF-8 compare.
// Order coherence (top-risk #2): each comparison mirrors an exact Java
// semantic — compareUTF16 is String.compareTo (UTF-16 code-unit order),
// compareSignedBytes is Arrays.compare (signed), compareUTF8 matches
// String.compareTo against stored UTF-8 in place.

// zigzag maps signed → unsigned (Java (v<<1) ^ (v>>63)).
func zigzag(v int64) uint64 {
	return uint64((v << 1) ^ (v >> 63))
}

// unzigzag is the inverse: (v>>>1) ^ -(v&1).
func unzigzag(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}

// zigzag32 / unzigzag32 are the 32-bit forms (IntDeltaFormat).
func zigzag32(v int32) int32 {
	return (v << 1) ^ (v >> 31)
}

func unzigzag32(v int32) int32 {
	return int32(uint32(v)>>1) ^ -(v & 1)
}

// utf16Units returns the UTF-16 code units of s. Go strings may hold
// arbitrary bytes (GD12): the units are those of the LOSSY UTF-8 decode
// ([]rune semantics — one U+FFFD per malformed byte), which is what a Java
// reader would see when decoding those same bytes.
func utf16Units(s string) []uint16 {
	return utf16.Encode([]rune(s))
}

// lossyUTF8 materializes b as a Go string the way Java's
// new String(bytes, UTF_8) materializes it: malformed input becomes U+FFFDs
// ([]rune decode semantics — one U+FFFD per malformed byte). See GD12 /
// PORTING-GAPS for the maximal-subpart edge-case deviation.
func lossyUTF8(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return string([]rune(string(b)))
}

// encodeStringUTF8 returns the bytes StringSer writes for s: verbatim when s
// is already valid UTF-8, else the lossy re-encode (U+FFFD per malformed
// byte) — Go strings are arbitrary bytes, and stored string bytes must stay
// well-formed so strict byte-side compare can't fail on our own output (GD12).
func encodeStringUTF8(s string) []byte {
	if utf8.ValidString(s) {
		return []byte(s)
	}
	return []byte(lossyUTF8([]byte(s)))
}

// compareUTF16 is String.compareTo: UTF-16 code-unit lexicographic order.
// Differs from Go string (code-point) order only for supplementary
// characters and malformed bytes — do NOT substitute strings.Compare.
func compareUTF16(a, b string) int {
	au, bu := utf16Units(a), utf16Units(b)
	n := min(len(au), len(bu))
	for i := 0; i < n; i++ {
		if au[i] != bu[i] {
			if au[i] < bu[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(au) < len(bu):
		return -1
	case len(au) > len(bu):
		return 1
	default:
		return 0
	}
}

// compareSignedBytes is Arrays.compare(byte[], byte[]): signed-byte
// lexicographic, shorter-is-less on a shared prefix.
func compareSignedBytes(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		ai, bi := int8(a[i]), int8(b[i])
		if ai != bi {
			if ai < bi {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// compareBytesUnsigned is memcmp / Arrays.compareUnsigned.
func compareBytesUnsigned(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// compareUTF8 returns the sign of stored.compareTo(key) where stored is the
// UTF-8 string spanning exactly byteLen bytes at r's position and key16 is
// the key as UTF-16 code units. Consumes at most byteLen bytes (fewer on
// early difference; caller re-seeks). Zero allocation.
//
// STRICT: malformed/torn UTF-8 → panic DataCorruption (RFC 3629
// well-formedness), never a plausible-looking key.
func compareUTF8(r *bin.Reader, byteLen int, key16 []uint16) int {
	rem := byteLen
	ci := 0
	keyLen := len(key16)

	// cmpUnit compares one produced UTF-16 unit against the key; returns
	// (result, true) to stop, (0, false) to continue.
	cmpUnit := func(unit uint16) (int, bool) {
		if ci == keyLen {
			return 1, true // key is a strict prefix of stored
		}
		c := int(unit) - int(key16[ci])
		ci++
		if c != 0 {
			if c < 0 {
				return -1, true
			}
			return 1, true
		}
		return 0, false
	}

	for rem > 0 {
		b0 := uint32(r.ReadU8())
		rem--
		var cp uint32
		var need int
		switch {
		case b0 < 0x80:
			cp, need = b0, 0
		case b0&0xE0 == 0xC0:
			cp, need = b0&0x1F, 1
		case b0&0xF0 == 0xE0:
			cp, need = b0&0x0F, 2
		case b0&0xF8 == 0xF0:
			cp, need = b0&0x07, 3
		default:
			panic(mapdb5.NewCorruption("corrupt utf8 lead byte"))
		}
		if need > rem {
			panic(mapdb5.NewCorruption("corrupt utf8 truncated sequence"))
		}
		for i := 0; i < need; i++ {
			b := uint32(r.ReadU8())
			if b&0xC0 != 0x80 {
				panic(mapdb5.NewCorruption("corrupt utf8 continuation byte"))
			}
			cp = (cp << 6) | (b & 0x3F)
		}
		rem -= need
		// RFC 3629 well-formedness.
		switch need {
		case 1:
			if cp < 0x80 {
				panic(mapdb5.NewCorruption("corrupt utf8 overlong sequence"))
			}
		case 2:
			if cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF) {
				panic(mapdb5.NewCorruption("corrupt utf8 overlong or surrogate"))
			}
		case 3:
			if cp < 0x10000 || cp > 0x10FFFF {
				panic(mapdb5.NewCorruption("corrupt utf8 overlong or out-of-range"))
			}
		}
		if cp < 0x10000 {
			if res, done := cmpUnit(uint16(cp)); done {
				return res
			}
		} else {
			v := cp - 0x10000
			if res, done := cmpUnit(0xD800 | uint16(v>>10)); done {
				return res
			}
			if res, done := cmpUnit(0xDC00 | uint16(v&0x3FF)); done {
				return res
			}
		}
	}
	// stored exhausted: equal, or stored is a strict prefix of key
	if ci == keyLen {
		return 0
	}
	return -1
}

// commonPrefixLen is the length of the common byte prefix of a and b.
func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
