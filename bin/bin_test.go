package bin

import (
	"math"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

func packed(v int64) []byte {
	w := NewWriter()
	w.PackLong(v)
	return w.CopyBytes()
}

func packedU(v uint64) []byte {
	w := NewWriter()
	w.PackUint64(v)
	return w.CopyBytes()
}

// Independently-computed packed-long boundary vectors (spec 05 §1), ported
// from the Rust test constants (cross-validated against Java).
func TestPackLongBoundaries(t *testing.T) {
	cases := []struct {
		v    int64
		want []byte
	}{
		{0, []byte{0x80}},
		{1, []byte{0x81}},
		{127, []byte{0xFF}},
		{128, []byte{0x01, 0x80}},
		{300, []byte{0x02, 0xAC}}, // 300 = 0b1_0010_1100
		{16383, []byte{0x7F, 0xFF}},
		{16384, []byte{0x01, 0x00, 0x80}},
	}
	for _, c := range cases {
		if got := packed(c.v); !equal(got, c.want) {
			t.Fatalf("pack(%d) = %x, want %x", c.v, got, c.want)
		}
	}
	m := packed(math.MaxInt64)
	if len(m) != 9 || m[len(m)-1] != 0xFF {
		t.Fatalf("pack(MaxInt64) = %x", m)
	}
	if got := packedU(math.MaxUint64); len(got) != 10 {
		t.Fatalf("pack(MaxUint64) len = %d", len(got))
	}
}

func TestPackUnpackRoundtrip(t *testing.T) {
	vals := []int64{0, 1, 63, 64, 127, 128, 129, 255, 256, 16383, 16384,
		1 << 20, 1 << 35, math.MaxInt64}
	for _, v := range vals {
		b := packed(v)
		r := NewReader(b)
		if got := r.UnpackLong(); got != v {
			t.Fatalf("roundtrip %d: got %d", v, got)
		}
		if r.Pos() != len(b) {
			t.Fatalf("roundtrip %d: pos %d != %d", v, r.Pos(), len(b))
		}
	}
	// unsigned domain
	for _, v := range []uint64{math.MaxInt64 + 1, math.MaxUint64 - 1, math.MaxUint64} {
		b := packedU(v)
		if got := NewReader(b).UnpackUint64(); got != v {
			t.Fatalf("u64 roundtrip %d: got %d", v, got)
		}
	}
}

func TestPackIntMasks32(t *testing.T) {
	w := NewWriter()
	w.PackInt(-1)
	if got := NewReader(w.Bytes()).UnpackInt(); got != -1 {
		t.Fatalf("unpack(packInt(-1)) = %d", got)
	}
	w2 := NewWriter()
	w2.PackInt(5)
	if !equal(w2.Bytes(), packed(5)) {
		t.Fatalf("packInt(5) = %x", w2.Bytes())
	}
}

func TestPackLongNegativePanics(t *testing.T) {
	defer func() {
		v := recover()
		if de, ok := mapdb5.AsDBError(v); !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("panic = %v, want KindMisuse", v)
		}
	}()
	NewWriter().PackLong(-1)
}

func TestBigEndianInts(t *testing.T) {
	w := NewWriter()
	w.WriteInt(0x01020304)
	w.WriteLong(0x0102030405060708)
	w.WriteShort(-2)
	b := w.Bytes()
	want := []byte{1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 7, 8, 0xFF, 0xFE}
	if !equal(b, want) {
		t.Fatalf("got %x", b)
	}
	r := NewReader(b)
	if r.ReadInt() != 0x01020304 || r.ReadLong() != 0x0102030405060708 || r.ReadShort() != -2 {
		t.Fatal("big-endian decode mismatch")
	}
}

func TestUnpackLongSkipMatchesDecode(t *testing.T) {
	w := NewWriter()
	for _, v := range []int64{0, 200, 128, 99999, 7} {
		w.PackLong(v)
	}
	r := NewReader(w.Bytes())
	r.UnpackLongSkip(3)
	if r.UnpackLong() != 99999 || r.UnpackLong() != 7 {
		t.Fatal("skip/decode mismatch")
	}
}

func TestMatchBytesAlwaysAdvances(t *testing.T) {
	b := []byte("MDBS.SD1extra")
	r := NewReader(b)
	if !r.MatchBytes([]byte("MDBS.SD1")) || r.Pos() != 8 {
		t.Fatal("match failed")
	}
	r2 := NewReader(b)
	if r2.MatchBytes([]byte("XXXX.SD1")) || r2.Pos() != 8 {
		t.Fatal("mismatch must still advance")
	}
}

func mustPanicCorruption(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		v := recover()
		if v == nil {
			t.Fatalf("%s: expected panic", name)
		}
		if de, ok := mapdb5.AsDBError(v); !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Fatalf("%s: panic %v is not DataCorruption", name, v)
		}
	}()
	fn()
}

func TestReadsPanicNeverCorruptState(t *testing.T) {
	b := []byte{0, 0}
	mustPanicCorruption(t, "read_i32", func() { NewReader(b).ReadInt() })
	mustPanicCorruption(t, "read_i64", func() { NewReader(b).ReadLong() })
	mustPanicCorruption(t, "over-long run", func() {
		NewReader(make([]byte, 12)).UnpackLong()
	})
	mustPanicCorruption(t, "seek past end", func() { NewReader(b).Seek(3) })
}

// Decoder rejection vectors (spec 01): overflow, canonical 10-byte,
// overlong prefixes, non-canonical encodings, truncation.
func TestUnpackLongRejectionVectors(t *testing.T) {
	// 2^63 (overflow for the signed API)
	w := NewWriter()
	w.PackUint64(1 << 63)
	mustPanicCorruption(t, "2^63", func() { NewReader(w.Bytes()).UnpackLong() })
	// canonical 10-byte value (u64 max — valid for UnpackUint64, not UnpackLong)
	w2 := NewWriter()
	w2.PackUint64(math.MaxUint64)
	mustPanicCorruption(t, "10-byte canonical", func() { NewReader(w2.Bytes()).UnpackLong() })
	// non-canonical: leading zero group (value 5 encoded in 2 bytes)
	mustPanicCorruption(t, "non-canonical pad", func() {
		NewReader([]byte{0x00, 0x85}).UnpackLong()
	})
	// over-long prefix: 11 bytes without terminator
	mustPanicCorruption(t, "overlong prefix", func() {
		NewReader(make([]byte, 11)).UnpackLong()
	})
	// truncated multi-byte encoding
	mustPanicCorruption(t, "truncated", func() {
		NewReader([]byte{0x01}).UnpackLong()
	})
	// over-32-bit packed int (5-byte encoding of 2^32)
	w3 := NewWriter()
	w3.PackUint64(1 << 32)
	mustPanicCorruption(t, "int overflow", func() { NewReader(w3.Bytes()).UnpackInt() })
}

func equal(a, b []byte) bool {
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
