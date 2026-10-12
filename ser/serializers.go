package ser

import (
	"bytes"
	"cmp"
	"compress/zlib"
	"io"
	"math"
	"math/big"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// Built-in element serializers (Java Serializers), minus the skipped three
// (STRING_INTERN, CLASS, JAVA — PORTING-GAPS).
//
// Java → Go type map: Short→int16, Character→uint16, Integer→int32,
// Long→int64, UUID→[16]byte, String→string, byte[]→[]byte.
//
// Every singleton is an AnyOf[A] over a funcSer codec, so it satisfies BOTH
// Serializer[A] and AnySerializer without an adapter allocation (spec 01).

// funcSer is a codec-as-data Serializer implementation, used to keep the
// built-in table declarative. eq==nil defaults to cmp()==0.
type funcSer[A any] struct {
	ser     func(w *bin.Writer, v A)
	deser   func(r *bin.Reader, size int) A
	fixed   int
	cmp     func(a, b A) int
	eq      func(a, b A) bool
	natural bool
	canonic bool
	// desc is the Java-registered catalog descriptor ("" = custom codec).
	desc string
}

func (f funcSer[A]) Serialize(w *bin.Writer, v A)       { f.ser(w, v) }
func (f funcSer[A]) Deserialize(r *bin.Reader, s int) A { return f.deser(r, s) }
func (f funcSer[A]) FixedSize() int                     { return f.fixed }
func (f funcSer[A]) SizeHint() int {
	if f.fixed >= 0 {
		return f.fixed
	}
	return 128
}
func (f funcSer[A]) Compare(a, b A) int { return f.cmp(a, b) }
func (f funcSer[A]) Equals(a, b A) bool {
	if f.eq != nil {
		return f.eq(a, b)
	}
	return f.cmp(a, b) == 0
}
func (f funcSer[A]) NaturalOrder() bool            { return f.natural }
func (f funcSer[A]) EqualsBySerializedBytes() bool { return f.canonic }

// readFramedLen reads a packed length prefix and validates it against the
// record before it sizes an allocation (GD8): negative → corrupt; larger
// than the bytes left in the record → corrupt.
func readFramedLen(r *bin.Reader) int {
	raw := r.UnpackInt()
	if raw < 0 {
		panic(mapdb5.NewCorruption("negative length prefix"))
	}
	n := int(raw)
	if n > r.Remaining() {
		panic(mapdb5.NewCorruption("length prefix exceeds record"))
	}
	return n
}

// checkedCount is the element-count sibling of readFramedLen: each element
// needs at least one byte, so count > remaining is impossible in a valid
// record.
func checkedCount(r *bin.Reader) int {
	return readFramedLen(r)
}

// ---- fixed-width scalars ---------------------------------------------------

// Short is the int16 serializer: 2-byte big-endian, signed order.
var Short = NewAnyOf(funcSer[int16]{
	desc:    "SHORT",
	ser:     func(w *bin.Writer, v int16) { w.WriteShort(v) },
	deser:   func(r *bin.Reader, _ int) int16 { return r.ReadShort() },
	fixed:   2,
	cmp:     cmp.Compare[int16],
	natural: true, canonic: true,
})

// Char is the uint16 serializer: 2-byte big-endian, unsigned order.
var Char = NewAnyOf(funcSer[uint16]{
	desc:    "CHAR",
	ser:     func(w *bin.Writer, v uint16) { w.WriteChar(v) },
	deser:   func(r *bin.Reader, _ int) uint16 { return r.ReadU16() },
	fixed:   2,
	cmp:     cmp.Compare[uint16],
	natural: true, canonic: true,
})

// Int32 is the int32 serializer: 4-byte big-endian, signed order.
var Int32 = NewAnyOf(funcSer[int32]{
	desc:    "INTEGER",
	ser:     func(w *bin.Writer, v int32) { w.WriteInt(v) },
	deser:   func(r *bin.Reader, _ int) int32 { return r.ReadInt() },
	fixed:   4,
	cmp:     cmp.Compare[int32],
	natural: true, canonic: true,
})

// Long is the int64 serializer: 8-byte big-endian, signed order.
var Long = NewAnyOf(funcSer[int64]{
	desc:    "LONG",
	ser:     func(w *bin.Writer, v int64) { w.WriteLong(v) },
	deser:   func(r *bin.Reader, _ int) int64 { return r.ReadLong() },
	fixed:   8,
	cmp:     cmp.Compare[int64],
	natural: true, canonic: true,
})

// UUID is the [16]byte serializer: two big-endian signed longs (msb then
// lsb). Order is signed msb-then-lsb (Java UUID.compareTo).
var UUID = NewAnyOf(funcSer[[16]byte]{
	desc: "UUID",
	ser: func(w *bin.Writer, v [16]byte) {
		w.WriteAll(v[:])
	},
	deser: func(r *bin.Reader, _ int) [16]byte {
		var u [16]byte
		r.ReadFully(u[:])
		return u
	},
	fixed:   16,
	cmp:     compareUUID,
	natural: true, canonic: true,
})

// compareUUID: signed msb then signed lsb. The 16 bytes ARE two big-endian
// signed longs, so decode then compare signed.
func compareUUID(a, b [16]byte) int {
	amsb := int64(be64(a[:8]))
	bmsb := int64(be64(b[:8]))
	if amsb != bmsb {
		return cmp.Compare(amsb, bmsb)
	}
	return cmp.Compare(int64(be64(a[8:])), int64(be64(b[8:])))
}

func be64(b []byte) uint64 {
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v
}

// String is the UTF-8 string serializer: packInt(utf8len) + bytes; UTF-16
// code-unit order (Java String.compareTo). Encode is lossy for invalid UTF-8
// (GD12).
var String = NewAnyOf(funcSer[string]{
	desc: "STRING",
	ser: func(w *bin.Writer, v string) {
		b := encodeStringUTF8(v)
		w.PackInt(int32(len(b)))
		w.WriteAll(b)
	},
	deser: func(r *bin.Reader, _ int) string {
		n := readFramedLen(r)
		return lossyUTF8(r.ReadBytes(n))
	},
	fixed:   -1,
	cmp:     compareUTF16,
	natural: true, canonic: true,
})

// ByteArray is the []byte serializer: packInt(len) + bytes, SIGNED
// lexicographic order (Java Arrays.compare).
var ByteArray = NewAnyOf(funcSer[[]byte]{
	desc: "BYTE_ARRAY",
	ser: func(w *bin.Writer, v []byte) {
		w.PackInt(int32(len(v)))
		w.WriteAll(v)
	},
	deser: func(r *bin.Reader, _ int) []byte {
		return r.ReadBytes(readFramedLen(r))
	},
	fixed:   -1,
	cmp:     compareSignedBytes,
	eq:      eqBytes,
	canonic: true,
})

// ByteArrayUnsigned: same wire/equality as ByteArray but UNSIGNED (memcmp)
// order.
var ByteArrayUnsigned = NewAnyOf(funcSer[[]byte]{
	desc: "BYTE_ARRAY_UNSIGNED",
	ser: func(w *bin.Writer, v []byte) {
		w.PackInt(int32(len(v)))
		w.WriteAll(v)
	},
	deser: func(r *bin.Reader, _ int) []byte {
		return r.ReadBytes(readFramedLen(r))
	},
	fixed:   -1,
	cmp:     compareBytesUnsigned,
	eq:      eqBytes,
	canonic: true,
})

func eqBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---- small scalars ---------------------------------------------------------

// Boolean: 1 byte (0/1); other bytes rejected on decode.
var Boolean = NewAnyOf(funcSer[bool]{
	desc: "BOOLEAN",
	ser: func(w *bin.Writer, v bool) {
		if v {
			w.WriteU8(1)
		} else {
			w.WriteU8(0)
		}
	},
	deser: func(r *bin.Reader, _ int) bool {
		v := r.ReadUnsignedByte()
		if v > 1 {
			panic(mapdb5.NewCorruption("invalid boolean byte"))
		}
		return v != 0
	},
	fixed:   1,
	cmp:     func(a, b bool) int { return cmp.Compare(boolOrd(a), boolOrd(b)) },
	natural: true, canonic: true,
})

func boolOrd(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Byte: 1-byte signed byte.
var Byte = NewAnyOf(funcSer[int8]{
	desc:    "BYTE",
	ser:     func(w *bin.Writer, v int8) { w.WriteU8(byte(v)) },
	deser:   func(r *bin.Reader, _ int) int8 { return r.ReadI8() },
	fixed:   1,
	cmp:     cmp.Compare[int8],
	natural: true, canonic: true,
})

// Float: 4-byte IEEE-754 bits big-endian; Java Float.compare total order
// (-0.0 < 0.0, NaN greatest); equals is bitwise.
//
// Java floatToIntBits CANONICALIZES every NaN to 0x7fc00000 — on write, on
// compare, and on equals. A NaN payload is an in-memory detail, never a
// wire value; all NaNs compare and equal each other.
var Float = NewAnyOf(funcSer[float32]{
	desc:    "FLOAT",
	ser:     func(w *bin.Writer, v float32) { w.WriteUint32(float32CanonicalBits(v)) },
	deser:   func(r *bin.Reader, _ int) float32 { return math.Float32frombits(r.ReadUint32()) },
	fixed:   4,
	cmp:     func(a, b float32) int { return cmp.Compare(float32OrderKey(a), float32OrderKey(b)) },
	eq:      func(a, b float32) bool { return float32CanonicalBits(a) == float32CanonicalBits(b) },
	natural: true, canonic: true,
})

// float32CanonicalBits is Java Float.floatToIntBits: canonical NaN.
func float32CanonicalBits(f float32) uint32 {
	if f != f { // NaN — the only value unequal to itself
		return 0x7fc00000
	}
	return math.Float32bits(f)
}

// float32OrderKey maps a float to a monotonically-ordered uint32 matching
// Java Float.compare total order.
func float32OrderKey(f float32) uint32 {
	b := float32CanonicalBits(f)
	if b&0x80000000 != 0 {
		return ^b
	}
	return b | 0x80000000
}

// Double: 8-byte IEEE-754 bits big-endian; Double.compare total order, with
// the same NaN canonicalization as Float (0x7ff8000000000000).
var Double = NewAnyOf(funcSer[float64]{
	desc:    "DOUBLE",
	ser:     func(w *bin.Writer, v float64) { w.WriteUint64(float64CanonicalBits(v)) },
	deser:   func(r *bin.Reader, _ int) float64 { return math.Float64frombits(r.ReadUint64()) },
	fixed:   8,
	cmp:     func(a, b float64) int { return cmp.Compare(float64OrderKey(a), float64OrderKey(b)) },
	eq:      func(a, b float64) bool { return float64CanonicalBits(a) == float64CanonicalBits(b) },
	natural: true, canonic: true,
})

func float64CanonicalBits(f float64) uint64 {
	if f != f {
		return 0x7ff8000000000000
	}
	return math.Float64bits(f)
}

func float64OrderKey(f float64) uint64 {
	b := float64CanonicalBits(f)
	if b&0x8000000000000000 != 0 {
		return ^b
	}
	return b | 0x8000000000000000
}

// IntegerPacked: packed two's-complement int32 (packInt); negatives use 5 bytes.
var IntegerPacked = NewAnyOf(funcSer[int32]{
	desc:    "INTEGER_PACKED",
	ser:     func(w *bin.Writer, v int32) { w.PackInt(v) },
	deser:   func(r *bin.Reader, _ int) int32 { return r.UnpackInt() },
	fixed:   -1,
	cmp:     cmp.Compare[int32],
	natural: true, canonic: true,
})

// LongPacked: packed two's-complement int64; negatives use 10 bytes.
var LongPacked = NewAnyOf(funcSer[int64]{
	desc:    "LONG_PACKED",
	ser:     func(w *bin.Writer, v int64) { w.PackUint64(uint64(v)) },
	deser:   func(r *bin.Reader, _ int) int64 { return int64(r.UnpackUint64()) },
	fixed:   -1,
	cmp:     cmp.Compare[int64],
	natural: true, canonic: true,
})

// Recid: a positive record id as a packed long.
var Recid = NewAnyOf(funcSer[int64]{
	desc: "RECID",
	ser: func(w *bin.Writer, v int64) {
		if v <= 0 {
			panic(mapdb5.NewMisuse("recid must be positive"))
		}
		w.PackLong(v)
	},
	deser: func(r *bin.Reader, _ int) int64 {
		v := r.UnpackLong()
		if v <= 0 {
			panic(mapdb5.NewCorruption("invalid recid"))
		}
		return v
	},
	fixed:   -1,
	cmp:     cmp.Compare[int64],
	natural: true, canonic: true,
})

// ---- no-size / ascii families ----------------------------------------------

// ByteArrayNoSize: raw record bytes with no inner length prefix; requires a
// known record size.
var ByteArrayNoSize = NewAnyOf(funcSer[[]byte]{
	desc: "BYTE_ARRAY_NOSIZE",
	ser:  func(w *bin.Writer, v []byte) { w.WriteAll(v) },
	deser: func(r *bin.Reader, size int) []byte {
		if size < 0 {
			panic(mapdb5.NewCorruption("BYTE_ARRAY_NOSIZE requires record size"))
		}
		return r.ReadBytes(size)
	},
	fixed:   -1,
	cmp:     compareSignedBytes,
	eq:      eqBytes,
	canonic: true,
})

// StringNoSize: UTF-8 occupying the whole record, no inner length prefix.
var StringNoSize = NewAnyOf(funcSer[string]{
	desc: "STRING_NOSIZE",
	ser:  func(w *bin.Writer, v string) { w.WriteAll(encodeStringUTF8(v)) },
	deser: func(r *bin.Reader, size int) string {
		if size < 0 {
			panic(mapdb5.NewCorruption("STRING_NOSIZE requires record size"))
		}
		return lossyUTF8(r.ReadBytes(size))
	},
	fixed:   -1,
	cmp:     compareUTF16,
	natural: true, canonic: true,
})

// StringAscii: 7-bit ASCII; rejects code points above 0x7F (encode panics
// Misuse, matching Java's thrown exception; decode panics Corruption).
var StringAscii = NewAnyOf(funcSer[string]{
	desc: "STRING_ASCII",
	ser: func(w *bin.Writer, v string) {
		b := encodeStringUTF8(v)
		for _, c := range b {
			if c > 0x7F {
				panic(mapdb5.NewMisuse("STRING_ASCII requires 7-bit ASCII"))
			}
		}
		w.PackInt(int32(len(b)))
		w.WriteAll(b)
	},
	deser: func(r *bin.Reader, _ int) string {
		n := checkedCount(r)
		b := r.ReadBytes(n)
		for _, c := range b {
			if c > 0x7F {
				panic(mapdb5.NewCorruption("non-ASCII byte in STRING_ASCII"))
			}
		}
		return string(b)
	},
	fixed:   -1,
	cmp:     compareUTF16,
	natural: true, canonic: true,
})

// ---- primitive array families ----------------------------------------------
// All are packInt(len) + big-endian elements.

func packSliceSer[T any](w *bin.Writer, v []T, write func(*bin.Writer, T)) {
	w.PackInt(int32(len(v)))
	for _, e := range v {
		write(w, e)
	}
}

func packSliceDeser[T any](r *bin.Reader, read func(*bin.Reader) T) []T {
	n := checkedCount(r)
	out := make([]T, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, read(r))
	}
	return out
}

func eqSlice[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cmpSlice[T cmp.Ordered](a, b []T) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return cmp.Compare(a[i], b[i])
		}
	}
	return cmp.Compare(len(a), len(b))
}

// RecidArray: packInt(len) + Recid-encoded elements.
var RecidArray = NewAnyOf(funcSer[[]int64]{
	desc: "RECID_ARRAY",
	ser: func(w *bin.Writer, v []int64) {
		packSliceSer(w, v, func(w *bin.Writer, e int64) { Recid.Serialize(w, e) })
	},
	deser: func(r *bin.Reader, _ int) []int64 {
		return packSliceDeser(r, func(r *bin.Reader) int64 { return Recid.Deserialize(r, SelfDelimit) })
	},
	fixed:   -1,
	cmp:     cmpSlice[int64],
	eq:      eqSlice[int64],
	canonic: true,
})

// CharArray: packInt(len) + big-endian u16 elements.
var CharArray = NewAnyOf(funcSer[[]uint16]{
	desc:    "CHAR_ARRAY",
	ser:     func(w *bin.Writer, v []uint16) { packSliceSer(w, v, (*bin.Writer).WriteChar) },
	deser:   func(r *bin.Reader, _ int) []uint16 { return packSliceDeser(r, (*bin.Reader).ReadU16) },
	fixed:   -1,
	cmp:     cmpSlice[uint16],
	eq:      eqSlice[uint16],
	canonic: true,
})

// ShortArray: packInt(len) + big-endian i16 elements.
var ShortArray = NewAnyOf(funcSer[[]int16]{
	desc:    "SHORT_ARRAY",
	ser:     func(w *bin.Writer, v []int16) { packSliceSer(w, v, (*bin.Writer).WriteShort) },
	deser:   func(r *bin.Reader, _ int) []int16 { return packSliceDeser(r, (*bin.Reader).ReadShort) },
	fixed:   -1,
	cmp:     cmpSlice[int16],
	eq:      eqSlice[int16],
	canonic: true,
})

// IntArray: packInt(len) + big-endian i32 elements.
var IntArray = NewAnyOf(funcSer[[]int32]{
	desc:    "INT_ARRAY",
	ser:     func(w *bin.Writer, v []int32) { packSliceSer(w, v, (*bin.Writer).WriteInt) },
	deser:   func(r *bin.Reader, _ int) []int32 { return packSliceDeser(r, (*bin.Reader).ReadInt) },
	fixed:   -1,
	cmp:     cmpSlice[int32],
	eq:      eqSlice[int32],
	canonic: true,
})

// LongArray: packInt(len) + big-endian i64 elements.
var LongArray = NewAnyOf(funcSer[[]int64]{
	desc:    "LONG_ARRAY",
	ser:     func(w *bin.Writer, v []int64) { packSliceSer(w, v, (*bin.Writer).WriteLong) },
	deser:   func(r *bin.Reader, _ int) []int64 { return packSliceDeser(r, (*bin.Reader).ReadLong) },
	fixed:   -1,
	cmp:     cmpSlice[int64],
	eq:      eqSlice[int64],
	canonic: true,
})

// FloatArray: packInt(len) + big-endian f32 bits (canonical NaN, like the
// scalar serializer); element-wise total order.
var FloatArray = NewAnyOf(funcSer[[]float32]{
	desc: "FLOAT_ARRAY",
	ser: func(w *bin.Writer, v []float32) {
		packSliceSer(w, v, func(w *bin.Writer, e float32) { w.WriteUint32(float32CanonicalBits(e)) })
	},
	deser: func(r *bin.Reader, _ int) []float32 {
		return packSliceDeser(r, func(r *bin.Reader) float32 { return math.Float32frombits(r.ReadUint32()) })
	},
	fixed: -1,
	cmp: func(a, b []float32) int {
		n := min(len(a), len(b))
		for i := 0; i < n; i++ {
			if c := cmp.Compare(float32OrderKey(a[i]), float32OrderKey(b[i])); c != 0 {
				return c
			}
		}
		return cmp.Compare(len(a), len(b))
	},
	eq: func(a, b []float32) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if float32CanonicalBits(a[i]) != float32CanonicalBits(b[i]) {
				return false
			}
		}
		return true
	},
	canonic: true,
})

// DoubleArray: packInt(len) + big-endian f64 bits (canonical NaN); element-
// wise total order.
var DoubleArray = NewAnyOf(funcSer[[]float64]{
	desc: "DOUBLE_ARRAY",
	ser: func(w *bin.Writer, v []float64) {
		packSliceSer(w, v, func(w *bin.Writer, e float64) { w.WriteUint64(float64CanonicalBits(e)) })
	},
	deser: func(r *bin.Reader, _ int) []float64 {
		return packSliceDeser(r, func(r *bin.Reader) float64 { return math.Float64frombits(r.ReadUint64()) })
	},
	fixed: -1,
	cmp: func(a, b []float64) int {
		n := min(len(a), len(b))
		for i := 0; i < n; i++ {
			if c := cmp.Compare(float64OrderKey(a[i]), float64OrderKey(b[i])); c != 0 {
				return c
			}
		}
		return cmp.Compare(len(a), len(b))
	},
	eq: func(a, b []float64) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if float64CanonicalBits(a[i]) != float64CanonicalBits(b[i]) {
				return false
			}
		}
		return true
	},
	canonic: true,
})

// BooleanArray: packInt(len) + LSB-first bit-packed booleans.
var BooleanArray = NewAnyOf(funcSer[[]bool]{
	desc: "BOOLEAN_ARRAY",
	ser: func(w *bin.Writer, v []bool) {
		w.PackInt(int32(len(v)))
		for offset := 0; offset < len(v); offset += 8 {
			var bits byte
			for bit := 0; bit < 8 && offset+bit < len(v); bit++ {
				if v[offset+bit] {
					bits |= 1 << uint(bit)
				}
			}
			w.WriteU8(bits)
		}
	},
	deser: func(r *bin.Reader, _ int) []bool {
		// Element count is a BIT count; the packed byte run is ceil(len/8), so
		// bound the byte run (not the bit count) against the record.
		raw := r.UnpackInt()
		if raw < 0 {
			panic(mapdb5.NewCorruption("negative array length"))
		}
		n := int(raw)
		if byteRun := (n + 7) / 8; byteRun > r.Remaining() {
			panic(mapdb5.NewCorruption("array length exceeds record"))
		}
		v := make([]bool, n)
		for offset := 0; offset < n; offset += 8 {
			bits := r.ReadUnsignedByte()
			for bit := 0; bit < 8 && offset+bit < n; bit++ {
				v[offset+bit] = bits&(1<<uint(bit)) != 0
			}
		}
		return v
	},
	fixed: -1,
	cmp: func(a, b []bool) int {
		n := min(len(a), len(b))
		for i := 0; i < n; i++ {
			if a[i] != b[i] {
				return cmp.Compare(boolOrd(a[i]), boolOrd(b[i]))
			}
		}
		return cmp.Compare(len(a), len(b))
	},
	eq:      eqSlice[bool],
	canonic: true,
})

// ---- BigInteger / BigDecimal / Date ----------------------------------------

// BigInteger is the *big.Int serializer, wire-compatible with Java's
// BigInteger.toByteArray() (minimal two's-complement big-endian), framed by
// packInt(len) + bytes.
var BigInteger = NewAnyOf(funcSer[*big.Int]{
	desc: "BIG_INTEGER",
	ser: func(w *bin.Writer, v *big.Int) {
		b := bigintSignedBytes(v)
		w.PackInt(int32(len(b)))
		w.WriteAll(b)
	},
	deser: func(r *bin.Reader, _ int) *big.Int {
		b := r.ReadBytes(readFramedLen(r))
		return bigintFromSignedBytes(b)
	},
	fixed:   -1,
	cmp:     func(a, b *big.Int) int { return a.Cmp(b) },
	eq:      func(a, b *big.Int) bool { return a.Cmp(b) == 0 },
	natural: true, canonic: true,
})

// bigintSignedBytes is Java BigInteger.toByteArray(): minimal-length
// two's-complement big-endian. Zero is {0x00} — Java emits one byte, never
// an empty array.
func bigintSignedBytes(v *big.Int) []byte {
	if v.Sign() == 0 {
		return []byte{0x00}
	}
	// math/big.Bytes is unsigned magnitude; build two's-complement manually.
	if v.Sign() > 0 {
		mag := v.Bytes()
		if mag[0]&0x80 != 0 {
			return append([]byte{0x00}, mag...)
		}
		return mag
	}
	// Negative: two's complement over the minimal width.
	mag := v.Bytes() // magnitude of the negative value
	// minimal byte count to hold two's complement of -mag
	n := len(mag)
	two := new(big.Int).Lsh(big.NewInt(1), uint(n)*8)
	tc := new(big.Int).Add(two, v) // 2^(8n) + v, positive
	b := tc.Bytes()
	if len(b) < n {
		b = append(make([]byte, n-len(b)), b...)
	}
	if b[0]&0x80 == 0 {
		// need an extra leading 0xFF byte
		b = append([]byte{0xFF}, b...)
	}
	return b
}

// bigintFromSignedBytes decodes Java's two's-complement big-endian form.
func bigintFromSignedBytes(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}
	v := new(big.Int).SetBytes(b)
	if b[0]&0x80 != 0 {
		two := new(big.Int).Lsh(big.NewInt(1), uint(len(b))*8)
		v.Sub(v, two)
	}
	return v
}

// Decimal is an arbitrary-precision decimal unscaled * 10^-scale,
// wire-compatible with Java BigDecimal: BYTE_ARRAY(unscaled.toByteArray())
// then packInt(scale). Equals is SCALE-SENSITIVE (Java BigDecimal.equals:
// 1.0 != 1.00); Compare is value-based (Java compareTo: 1.0 == 1.00).
type Decimal struct {
	Unscaled *big.Int
	Scale    int32
}

// BigDecimal is the Decimal serializer.
var BigDecimal = NewAnyOf(funcSer[Decimal]{
	desc: "BIG_DECIMAL",
	ser: func(w *bin.Writer, v Decimal) {
		b := bigintSignedBytes(v.Unscaled)
		w.PackInt(int32(len(b)))
		w.WriteAll(b)
		w.PackInt(v.Scale)
	},
	deser: func(r *bin.Reader, _ int) Decimal {
		b := r.ReadBytes(readFramedLen(r))
		return Decimal{Unscaled: bigintFromSignedBytes(b), Scale: r.UnpackInt()}
	},
	fixed:   -1,
	cmp:     compareDecimal,
	eq:      func(a, b Decimal) bool { return a.Scale == b.Scale && a.Unscaled.Cmp(b.Unscaled) == 0 },
	natural: true, canonic: true,
})

// decimalDigits is the base-10 digit count of a non-zero big.Int.
func decimalDigits(u *big.Int) int64 {
	t := u.Text(10)
	if t[0] == '-' {
		return int64(len(t) - 1)
	}
	return int64(len(t))
}

// compareDecimal is Java BigDecimal.compareTo, computed the way
// compareMagnitudeNormalized does — never by constructing an unbounded
// power of ten:
//
//   - sign decides first (zero compares only to zero);
//   - then the ADJUSTED EXPONENT digits(unscaled) − scale, which brackets
//     value ∈ [10^(d−1), 10^d) and decides most pairs without any
//     arithmetic on the operands;
//   - only equal-adjusted-exponent pairs align unscaled, where the needed
//     power is bounded by the digit-count difference (data-proportional,
//     never 2^31).
func compareDecimal(a, b Decimal) int {
	if a.Scale == b.Scale {
		return a.Unscaled.Cmp(b.Unscaled)
	}
	sa, sb := a.Unscaled.Sign(), b.Unscaled.Sign()
	if sa != sb {
		return cmpInt32(int32(sa), int32(sb))
	}
	if sa == 0 {
		return 0
	}
	da := decimalDigits(a.Unscaled) - int64(a.Scale)
	db := decimalDigits(b.Unscaled) - int64(b.Scale)
	if da != db {
		c := cmpInt64(da, db)
		if sa < 0 {
			return -c
		}
		return c
	}
	// Same magnitude: align on the larger scale. |diff| is bounded by the
	// digit-count difference, so the power is proportional to input size.
	diff := int64(b.Scale) - int64(a.Scale)
	au := new(big.Int).Set(a.Unscaled)
	bu := new(big.Int).Set(b.Unscaled)
	ten := big.NewInt(10)
	if diff > 0 {
		au.Mul(au, new(big.Int).Exp(ten, big.NewInt(diff), nil))
	} else if diff < 0 {
		bu.Mul(bu, new(big.Int).Exp(ten, big.NewInt(-diff), nil))
	}
	return au.Cmp(bu)
}

// Date is an epoch-millis serializer (Java java.util.Date): 8-byte BE long.
var Date = NewAnyOf(funcSer[int64]{
	desc:    "DATE",
	ser:     func(w *bin.Writer, v int64) { w.WriteLong(v) },
	deser:   func(r *bin.Reader, _ int) int64 { return r.ReadLong() },
	fixed:   8,
	cmp:     cmp.Compare[int64],
	natural: true, canonic: true,
})

// ---- generic wrapper serializers -------------------------------------------

// MaxArrayLength bounds an array length prefix (Java MAX_ARRAY_LENGTH).
const MaxArrayLength = 16_000_000

// ArraySerializer is the length-framed homogeneous array codec
// (Java ArraySerializer<A>): packInt(len) + each element via the delegate.
// EqualsBySerializedBytes follows the element.
type ArraySerializer[A any] struct {
	Element Serializer[A]
}

// NewArraySerializer wraps an element serializer into an array codec.
func NewArraySerializer[A any](element Serializer[A]) ArraySerializer[A] {
	return ArraySerializer[A]{Element: element}
}

func (s ArraySerializer[A]) Serialize(w *bin.Writer, v []A) {
	if len(v) > MaxArrayLength {
		panic(mapdb5.NewMisuse("array length exceeds limit"))
	}
	w.PackInt(int32(len(v)))
	for _, e := range v {
		s.Element.Serialize(w, e)
	}
}

func (s ArraySerializer[A]) Deserialize(r *bin.Reader, size int) []A {
	start := r.Pos()
	raw := r.UnpackInt()
	if raw < 0 || int(raw) > MaxArrayLength {
		panic(mapdb5.NewCorruption("invalid array length"))
	}
	length := int(raw)
	// Frame check for fixed-size elements, mirroring Java's guard.
	if size >= 0 {
		if fixed := s.Element.FixedSize(); fixed > 0 {
			consumed := r.Pos() - start
			remaining := size - consumed
			if remaining < 0 || length*fixed > remaining {
				panic(mapdb5.NewCorruption("array length exceeds record frame"))
			}
		}
	}
	// Each element consumes at least one byte.
	if length > r.Remaining() {
		panic(mapdb5.NewCorruption("array length exceeds record"))
	}
	out := make([]A, 0, length)
	for i := 0; i < length; i++ {
		out = append(out, s.Element.Deserialize(r, SelfDelimit))
	}
	return out
}

func (s ArraySerializer[A]) FixedSize() int { return -1 }
func (s ArraySerializer[A]) SizeHint() int  { return 128 }

func (s ArraySerializer[A]) Compare(a, b []A) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if c := s.Element.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(a), len(b))
}

func (s ArraySerializer[A]) Equals(a, b []A) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !s.Element.Equals(a[i], b[i]) {
			return false
		}
	}
	return true
}

func (s ArraySerializer[A]) NaturalOrder() bool { return false }
func (s ArraySerializer[A]) EqualsBySerializedBytes() bool {
	return s.Element.EqualsBySerializedBytes()
}

// ---- compression wrapper ---------------------------------------------------

// MaxPlainLength bounds decompression (256 MiB, matching the Rust port).
const MaxPlainLength = 256 * 1024 * 1024

// MaxCompressedLength is MaxPlainLength + 1 MiB headroom.
const MaxCompressedLength = MaxPlainLength + 1024*1024

// Compression levels (Java Deflater domain; the Go API takes -1..9 verbatim).
const (
	// DefaultCompression is Java Deflater.DEFAULT_COMPRESSION (-1).
	DefaultCompression = -1
	// BestCompression is Java Deflater.BEST_COMPRESSION (9).
	BestCompression = 9
)

// CompressionSerializer is a zlib-DEFLATE wrapper over any element
// serializer (Java CompressionSerializer<A>). Wire format:
// packInt(plainLen) + packInt(compressedLen) + zlib-compressed bytes.
//
// The stored bytes are a standard zlib stream — cross-decodable in both
// directions — but the exact compressor output is NOT guaranteed identical
// to Java's Deflater, which is why EqualsBySerializedBytes is always false
// (GD11), regardless of the delegate.
type CompressionSerializer[A any] struct {
	Delegate Serializer[A]
	Level    int
}

// NewCompression wraps delegate at the default level (-1).
func NewCompression[A any](delegate Serializer[A]) CompressionSerializer[A] {
	return CompressionSerializer[A]{Delegate: delegate, Level: DefaultCompression}
}

// NewCompressionLevel wraps delegate at an explicit level (-1 or 0..9).
func NewCompressionLevel[A any](delegate Serializer[A], level int) CompressionSerializer[A] {
	if level < DefaultCompression || level > BestCompression {
		panic(mapdb5.NewMisuse("invalid compression level"))
	}
	return CompressionSerializer[A]{Delegate: delegate, Level: level}
}

func (s CompressionSerializer[A]) Serialize(w *bin.Writer, v A) {
	pw := bin.GetWriter()
	s.Delegate.Serialize(pw, v)
	plain := pw.CopyBytes()
	bin.PutWriter(pw)
	if len(plain) > MaxPlainLength {
		panic(mapdb5.NewMisuse("uncompressed value too large"))
	}
	// Buffer the compressed stream so its length can prefix it.
	cw := bin.GetWriter()
	zw, err := zlib.NewWriterLevel(cw, s.Level)
	if err != nil {
		panic(mapdb5.NewMisuse("invalid compression level"))
	}
	if _, err := zw.Write(plain); err != nil {
		panic(mapdb5.NewIO(err))
	}
	if err := zw.Close(); err != nil {
		panic(mapdb5.NewIO(err))
	}
	compressed := cw.CopyBytes()
	bin.PutWriter(cw)
	if len(compressed) > MaxCompressedLength {
		panic(mapdb5.NewMisuse("compressed value too large"))
	}
	w.PackInt(int32(len(plain)))
	w.PackInt(int32(len(compressed)))
	w.WriteAll(compressed)
}

func (s CompressionSerializer[A]) Deserialize(r *bin.Reader, size int) A {
	start := r.Pos()
	plainLen := r.UnpackInt()
	compressedLen := r.UnpackInt()
	if plainLen < 0 || int(plainLen) > MaxPlainLength ||
		compressedLen < 0 || int(compressedLen) > MaxCompressedLength {
		panic(mapdb5.NewCorruption("invalid compressed frame length"))
	}
	if size >= 0 {
		consumed := r.Pos() - start
		if remaining := size - consumed; remaining < 0 || int(compressedLen) > remaining {
			panic(mapdb5.NewCorruption("compressed length exceeds record frame"))
		}
	}
	if int(compressedLen) > r.Remaining() {
		panic(mapdb5.NewCorruption("compressed length exceeds record"))
	}
	compressed := r.ReadBytes(int(compressedLen))
	zr, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		panic(mapdb5.NewCorruption("invalid compressed data"))
	}
	plain, err := io.ReadAll(io.LimitReader(zr, int64(plainLen)+1))
	if err != nil {
		panic(mapdb5.NewCorruption("invalid compressed data"))
	}
	if len(plain) != int(plainLen) {
		panic(mapdb5.NewCorruption("invalid compressed frame length"))
	}
	return s.Delegate.Deserialize(bin.NewReader(plain), int(plainLen))
}

func (s CompressionSerializer[A]) FixedSize() int     { return -1 }
func (s CompressionSerializer[A]) SizeHint() int      { return 256 }
func (s CompressionSerializer[A]) Compare(a, b A) int { return s.Delegate.Compare(a, b) }
func (s CompressionSerializer[A]) Equals(a, b A) bool { return s.Delegate.Equals(a, b) }
func (s CompressionSerializer[A]) NaturalOrder() bool { return s.Delegate.NaturalOrder() }
func (s CompressionSerializer[A]) EqualsBySerializedBytes() bool {
	// DEFLATE output is not canonical — do NOT delegate (GD11).
	return false
}
