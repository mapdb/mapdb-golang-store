package ser

import (
	"bytes"
	"math"
	"math/big"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// serGroup serializes a group, returning the bytes.
func serGroup[K any](f GroupFormat[K], g Group) []byte {
	w := bin.NewWriter()
	f.SerializeGroup(w, g)
	return w.CopyBytes()
}

// checkCoherence asserts object-side Search/Get agree with the byte side for
// every probe, plus the positioning contract (reader left at group end).
func checkCoherence[K any](t *testing.T, f GroupFormat[K], g Group, probes []K) {
	t.Helper()
	count := f.Size(g)
	bytes := serGroup(f, g)
	for _, key := range probes {
		objIdx, objFound := f.Search(g, key)
		if !f.SupportsBinary() {
			continue
		}
		bIdx, bFound := f.BytesSearch(bin.NewReader(bytes), count, key)
		if objIdx != bIdx || objFound != bFound {
			t.Fatalf("coherence for key %v: object (%d,%v) vs bytes (%d,%v)", key, objIdx, objFound, bIdx, bFound)
		}
		r := bin.NewReader(bytes)
		f.BytesSearch(r, count, key)
		if r.Pos() != len(bytes) {
			t.Fatalf("BytesSearch leaves pos %d, want %d", r.Pos(), len(bytes))
		}
		for pos := 0; pos < count; pos++ {
			ig := bin.NewReader(bytes)
			v := f.BytesGet(ig, count, pos)
			if !f.Element().Equals(v, f.Get(g, pos)) {
				t.Fatalf("BytesGet pos %d: %v != %v", pos, v, f.Get(g, pos))
			}
			if ig.Pos() != len(bytes) {
				t.Fatalf("BytesGet leaves pos %d, want %d", ig.Pos(), len(bytes))
			}
		}
	}
}

// checkRoundTrip asserts DeserializeGroup(SerializeGroup(g)) == g and the
// reader consumed exactly the encoding.
func checkRoundTrip[K any](t *testing.T, f GroupFormat[K], g Group, count int) {
	t.Helper()
	bytes := serGroup(f, g)
	r := bin.NewReader(bytes)
	back := f.DeserializeGroup(r, count)
	if r.Pos() != len(bytes) {
		t.Fatalf("deserialize consumed %d of %d bytes", r.Pos(), len(bytes))
	}
	if f.Size(back) != f.Size(g) {
		t.Fatalf("size %d != %d", f.Size(back), f.Size(g))
	}
	for i := 0; i < f.Size(g); i++ {
		if !f.Element().Equals(f.Get(back, i), f.Get(g, i)) {
			t.Fatalf("element %d differs: %v != %v", i, f.Get(back, i), f.Get(g, i))
		}
	}
}

// checkCursor verifies a RangeCursor over [from,to) yields Get(from..to) in
// order and leaves the reader at group end.
func checkCursor[K any](t *testing.T, f GroupFormat[K], g Group, from, to int) {
	t.Helper()
	count := f.Size(g)
	bytes := serGroup(f, g)
	r := bin.NewReader(bytes)
	cur := f.RangeCursor(r, count, from, to)
	i := from
	for cur.Next() {
		if cur.Index() != i {
			t.Fatalf("cursor index %d, want %d", cur.Index(), i)
		}
		if !f.Element().Equals(cur.Value(), f.Get(g, i)) {
			t.Fatalf("cursor value %v != %v", cur.Value(), f.Get(g, i))
		}
		i++
	}
	if i != to {
		t.Fatalf("cursor visited %d elements, want %d", i-from, to-from)
	}
	if r.Pos() != len(bytes) {
		t.Fatalf("cursor leaves pos %d, want %d", r.Pos(), len(bytes))
	}
}

func TestScalarFormatsRoundTripCoherence(t *testing.T) {
	t.Run("long", func(t *testing.T) {
		g := LongFormat.FromSlice([]int64{-100, -1, 0, 1, 2, 5, 42, 1000, math.MaxInt64})
		checkRoundTrip(t, LongFormat, g, 9)
		checkCoherence(t, LongFormat, g, []int64{-100, -50, 0, 3, 42, 999, math.MaxInt64, math.MinInt64})
	})
	t.Run("int", func(t *testing.T) {
		g := IntFormat.FromSlice([]int32{math.MinInt32, -5, 0, 7, 100, math.MaxInt32})
		checkRoundTrip(t, IntFormat, g, 6)
		checkCoherence(t, IntFormat, g, []int32{math.MinInt32, -6, 0, 7, 8, math.MaxInt32})
	})
	t.Run("short", func(t *testing.T) {
		g := ShortFormat.FromSlice([]int16{math.MinInt16, -30000, -1000, -1, 0, 1, 42, 1000, 30000, math.MaxInt16})
		checkRoundTrip(t, ShortFormat, g, 10)
		checkCoherence(t, ShortFormat, g, []int16{math.MinInt16, -32768, -1, 0, 42, 30001, math.MaxInt16})
	})
	t.Run("char", func(t *testing.T) {
		g := CharFormat.FromSlice([]uint16{0, 1, 0x00FF, 0x0100, 0x7FFF, 0x8000, 0xABCD, 0xFFFE, 0xFFFF})
		checkRoundTrip(t, CharFormat, g, 9)
		checkCoherence(t, CharFormat, g, []uint16{0, 2, 0x8000, 0xFFFD, 0xFFFF})
	})
	t.Run("uuid", func(t *testing.T) {
		mk := func(msb, lsb int64) [16]byte {
			var u [16]byte
			for i := 0; i < 8; i++ {
				u[i] = byte(msb >> uint(56-8*i))
				u[8+i] = byte(lsb >> uint(56-8*i))
			}
			return u
		}
		g := UUIDFormat.FromSlice([][16]byte{
			mk(math.MinInt64, 0), mk(-1, 5), mk(-1, math.MaxInt64), mk(0, math.MinInt64), mk(5, 0), mk(math.MaxInt64, math.MaxInt64),
		})
		checkRoundTrip(t, UUIDFormat, g, 6)
		checkCoherence(t, UUIDFormat, g, [][16]byte{mk(0, 0), mk(-1, 6), mk(math.MinInt64, math.MinInt64), mk(math.MaxInt64, 0)})
	})
}

func TestDeltaFormats(t *testing.T) {
	g := LongDeltaFormat.FromSlice([]int64{-100, -1, 0, 1, 2, 5, 42, 1000, math.MaxInt64})
	checkRoundTrip(t, LongDeltaFormat, g, 9)
	checkCoherence(t, LongDeltaFormat, g, []int64{-100, -50, 0, 3, 42, 999, math.MaxInt64})
	checkCursor(t, LongDeltaFormat, g, 1, 4)

	gi := IntDeltaFormat.FromSlice([]int32{-50, -1, 0, 3, 99, math.MaxInt32})
	checkRoundTrip(t, IntDeltaFormat, gi, 6)
	checkCoherence(t, IntDeltaFormat, gi, []int32{-51, -1, 2, 99, math.MaxInt32})
	checkCursor(t, IntDeltaFormat, gi, 0, 6)
}

func TestStringGroupFormat(t *testing.T) {
	g := StringGroupFormat.FromSlice([]string{"", "a", "apple", "banana", "zebra", "｡"})
	checkRoundTrip(t, StringGroupFormat, g, 6)
	checkCoherence(t, StringGroupFormat, g,
		[]string{"", "a", "aardvark", "apple", "app", "banana", "zzz", "｡"})
	checkCursor(t, StringGroupFormat, g, 0, 6)
}

// UTF-16 vs code-point order: U+FF61 (one UTF-16 unit) sorts AFTER U+10000
// (surrogate pair D800 DC00) in UTF-16 order, unlike code-point order.
func TestUTF16SupplementaryOrder(t *testing.T) {
	supp := "\U00010000" // surrogate pair
	bmp := "\uFF61"
	if compareUTF16(supp, bmp) >= 0 {
		t.Fatal("UTF-16 order: supplementary must sort before U+FF61")
	}
	// code-point order would give the opposite — this is the spec-mandated
	// divergence from strings.Compare.
	g := StringGroupFormat.FromSlice([]string{supp, bmp})
	checkCoherence(t, StringGroupFormat, g, []string{supp, bmp, "\uFFFF"})
}

func TestByteArrayFormat(t *testing.T) {
	g := ByteArrayFormat.FromSlice([][]byte{{}, {0x01}, {0x01, 0x80}, {0x80}, {0xFF, 0x00}})
	checkRoundTrip(t, ByteArrayFormat, g, 5)
	checkCoherence(t, ByteArrayFormat, g, [][]byte{
		{}, {0x00}, {0x01}, {0x01, 0x7F}, {0x80}, {0xFF},
	})
	checkCursor(t, ByteArrayFormat, g, 1, 4)
}

func TestPrefixFormats(t *testing.T) {
	keys := []string{"apple", "apples", "appliance", "application", "apply",
		"banana", "band", "bandana", "zebra"}
	g := StringPrefixFormat.FromSlice(keys)
	checkRoundTrip(t, StringPrefixFormat, g, len(keys))
	checkCoherence(t, StringPrefixFormat, g,
		[]string{"app", "apple", "apples", "aq", "band", "bandz", "zzz", ""})
	checkCursor(t, StringPrefixFormat, g, 2, 7)

	// Spanning multiple restart intervals.
	var many []string
	for i := 0; i < 40; i++ {
		many = append(many, "key"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	gm := StringPrefixFormat.FromSlice(many)
	checkRoundTrip(t, StringPrefixFormat, gm, len(many))
	checkCoherence(t, StringPrefixFormat, gm, []string{"keyaa", "keyma", "keyzz", "a"})

	bkeys := [][]byte{{0x01, 0x02}, {0x01, 0x02, 0x03}, {0x01, 0x03}, {0x02}}
	bg := ByteArrayPrefixFormat.FromSlice(bkeys)
	checkRoundTrip(t, ByteArrayPrefixFormat, bg, len(bkeys))
	checkCoherence(t, ByteArrayPrefixFormat, bg, [][]byte{
		{0x01}, {0x01, 0x02}, {0x01, 0x02, 0x04}, {0x03},
	})
}

func TestTupleFormat(t *testing.T) {
	f := NewTupleFormat(TupleLong, TupleStr)
	mk := func(l int64, s string) []Value { return []Value{LongValue(l), StringValue(s)} }
	// sorted: (5) < (5,"zz") — a proper prefix sorts before the longer tuple.
	g := f.FromSlice([][]Value{
		mk(1, "a"), mk(1, "b"), mk(2, "a"), {LongValue(5)}, mk(5, "zz"),
	})
	checkRoundTrip(t, f, g, 5)
	checkCoherence(t, f, g, [][]Value{
		mk(1, "a"), mk(1, "c"), mk(3, "a"), {LongValue(5)}, mk(5, "a"), {LongValue(0)},
	})
}

func TestColumnarFormat(t *testing.T) {
	f := NewColumnarValueFormat(ColumnLong, ColumnInt, ColumnShort)
	mk := func(l int64, i int32, s int16) []Value { return []Value{LongValue(l), IntValue(i), ShortValue(s)} }
	g := f.FromSlice([][]Value{
		mk(-1, 0, 0), mk(0, -5, 9), mk(0, 7, -1), mk(10, 0, math.MaxInt16),
	})
	checkRoundTrip(t, f, g, 4)
	checkCoherence(t, f, g, [][]Value{
		mk(-2, 0, 0), mk(0, 0, 0), mk(0, 7, -1), mk(10, 0, 0), mk(11, 0, 0),
	})
	checkCursor(t, f, g, 0, 4)

	// single-column cursor reads only that column
	r := bin.NewReader(serGroup(f, g))
	cc := f.ColumnCursor(r, 4, 1, 0, 4)
	var seen []int64
	for cc.Next() {
		v, ok := cc.Value().AsInt()
		if !ok {
			t.Fatal("column cursor returned non-int cell")
		}
		seen = append(seen, int64(v))
	}
	want := []int64{0, -5, 7, 0}
	if len(seen) != len(want) {
		t.Fatalf("column cursor yielded %v", seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("column cursor %v != %v", seen, want)
		}
	}
	if r.Pos() != len(serGroup(f, g)) {
		t.Fatal("column cursor leaves reader at group end")
	}
}

func TestObjectArrayFormatNonBinary(t *testing.T) {
	f := NewObjectArrayFormat[int64](Long)
	g := f.FromSlice([]int64{1, 2, 3, 10, 20})
	checkRoundTrip(t, f, g, 5)
	if f.SupportsBinary() {
		t.Fatal("ObjectArrayFormat must not support binary access")
	}
	func() {
		defer func() {
			if de, ok := mapdb5.AsDBError(recover()); !ok || de.Kind != mapdb5.KindUnsupported {
				t.Fatalf("BytesSearch must panic Unsupported, got %v", recover())
			}
		}()
		f.BytesSearch(bin.NewReader(serGroup(f, g)), 5, int64(3))
	}()
	if idx, found := f.Search(g, 10); !found || idx != 3 {
		t.Fatal("object search")
	}
	if idx, found := f.Search(g, 4); found || idx != 3 {
		t.Fatal("object search insertion point")
	}
}

// COW: mutating ops return new groups; the input is untouched.
func TestCopyOnWriteGroups(t *testing.T) {
	g0 := LongFormat.FromSlice([]int64{1, 3, 5})
	g1 := LongFormat.Insert(g0, 1, 2)
	if LongFormat.Size(g0) != 3 || LongFormat.Size(g1) != 4 {
		t.Fatal("insert must not mutate the input group")
	}
	if LongFormat.Get(g0, 1) != 3 {
		t.Fatal("input group mutated")
	}
	g2 := LongFormat.Set(g1, 0, 99)
	if LongFormat.Get(g1, 0) != 1 || LongFormat.Get(g2, 0) != 99 {
		t.Fatal("set must not mutate the input group")
	}
	g3 := LongFormat.Delete(g2, 0)
	if LongFormat.Size(g2) != 4 || LongFormat.Size(g3) != 3 {
		t.Fatal("delete must not mutate the input group")
	}
	g4 := LongFormat.CopyRange(g3, 1, 3)
	if LongFormat.Size(g4) != 2 || LongFormat.Get(g4, 0) != 3 {
		t.Fatal("copy_range")
	}
}

func TestSerializerRoundTrips(t *testing.T) {
	w := bin.NewWriter()

	Long.Serialize(w, int64(-42))
	String.Serialize(w, "hello")
	ByteArray.Serialize(w, []byte{1, 2, 3})
	Int32.Serialize(w, math.MinInt32)
	UUID.Serialize(w, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	Boolean.Serialize(w, true)
	Float.Serialize(w, -0.0)
	Double.Serialize(w, math.NaN())
	IntegerPacked.Serialize(w, int32(-7))
	LongPacked.Serialize(w, math.MinInt64)
	Recid.Serialize(w, int64(12345))
	NewArraySerializer[int64](Long).Serialize(w, []int64{1, 2, 3})
	BigInteger.Serialize(w, big.NewInt(-123456789012345))
	BigDecimal.Serialize(w, Decimal{Unscaled: big.NewInt(12345), Scale: 2})
	NewCompression[int64](Long).Serialize(w, int64(777))

	b := w.CopyBytes()
	r := bin.NewReader(b)
	if Long.Deserialize(r, SelfDelimit) != -42 {
		t.Fatal("long")
	}
	if String.Deserialize(r, SelfDelimit) != "hello" {
		t.Fatal("string")
	}
	if ba := ByteArray.Deserialize(r, SelfDelimit); len(ba) != 3 || ba[2] != 3 {
		t.Fatal("bytearray")
	}
	if Int32.Deserialize(r, SelfDelimit) != math.MinInt32 {
		t.Fatal("int32")
	}
	if UUID.Deserialize(r, SelfDelimit) != [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16} {
		t.Fatal("uuid")
	}
	if !Boolean.Deserialize(r, SelfDelimit) {
		t.Fatal("bool")
	}
	if f := Float.Deserialize(r, SelfDelimit); math.Float32bits(f) != math.Float32bits(-0.0) {
		t.Fatal("float")
	}
	if d := Double.Deserialize(r, SelfDelimit); !math.IsNaN(d) {
		t.Fatal("double")
	}
	if IntegerPacked.Deserialize(r, SelfDelimit) != -7 {
		t.Fatal("intpacked")
	}
	if LongPacked.Deserialize(r, SelfDelimit) != math.MinInt64 {
		t.Fatal("longpacked")
	}
	if Recid.Deserialize(r, SelfDelimit) != 12345 {
		t.Fatal("recid")
	}
	if a := NewArraySerializer[int64](Long).Deserialize(r, SelfDelimit); len(a) != 3 || a[1] != 2 {
		t.Fatal("arrayser")
	}
	if bi := BigInteger.Deserialize(r, SelfDelimit); bi.Cmp(big.NewInt(-123456789012345)) != 0 {
		t.Fatal("bigint")
	}
	if bd := BigDecimal.Deserialize(r, SelfDelimit); bd.Scale != 2 || bd.Unscaled.Cmp(big.NewInt(12345)) != 0 {
		t.Fatal("bigdecimal")
	}
	if NewCompression[int64](Long).Deserialize(r, SelfDelimit) != 777 {
		t.Fatal("compression")
	}
	if r.Pos() != len(b) {
		t.Fatalf("consumed %d of %d", r.Pos(), len(b))
	}
}

// Order vectors that catch signed/unsigned confusion.
func TestSerializerOrderVectors(t *testing.T) {
	// signed byte-array order: 0x80 (-128) < 0x01
	if ByteArray.Compare([]byte{0x80}, []byte{0x01}) >= 0 {
		t.Fatal("signed bytearray order")
	}
	if ByteArrayUnsigned.Compare([]byte{0x80}, []byte{0x01}) <= 0 {
		t.Fatal("unsigned bytearray order")
	}
	// UUID: negative msb sorts below positive
	var a, b2 [16]byte
	a[0] = 0x80 // msb negative
	if UUID.Compare(a, b2) >= 0 {
		t.Fatal("uuid signed msb order")
	}
	// float total order: -0.0 < 0.0, NaN greatest, -Inf < all
	if Float.Compare(float32(math.Copysign(0, -1)), 0) >= 0 {
		t.Fatal("-0.0 < 0.0")
	}
	if Float.Compare(float32(math.NaN()), float32(math.Inf(1))) <= 0 {
		t.Fatal("NaN greatest")
	}
	// BigDecimal: 1.0 == 1.00 under compareTo but not equals
	d1 := Decimal{Unscaled: big.NewInt(10), Scale: 1}
	d2 := Decimal{Unscaled: big.NewInt(100), Scale: 2}
	if BigDecimal.Compare(d1, d2) != 0 || BigDecimal.Equals(d1, d2) {
		t.Fatal("bigdecimal compare/equals scale semantics")
	}
}

// Java floatToIntBits canonicalizes every NaN to 0x7fc00000 — wire, order,
// and equality all see the canonical form.
func TestNaNCanonicalization(t *testing.T) {
	nan1 := math.Float32frombits(0x7fc00001) // non-canonical payload
	nan2 := math.Float32frombits(0xffc00000) // negative NaN
	if !Float.Equals(nan1, nan2) || !Float.Equals(nan1, float32(math.NaN())) {
		t.Fatal("all NaNs must equal (Java floatToIntBits)")
	}
	if Float.Compare(nan1, nan2) != 0 {
		t.Fatal("all NaNs must compare equal")
	}
	// wire: canonical 0x7fc00000
	w := bin.NewWriter()
	Float.Serialize(w, nan1)
	if got := w.Bytes(); !bytes.Equal(got, []byte{0x7f, 0xc0, 0x00, 0x00}) {
		t.Fatalf("NaN wire %x, want 7fc00000", got)
	}
	// double: 0x7ff8000000000000
	dnan := math.Float64frombits(0x7ff0000000000001)
	w = bin.NewWriter()
	Double.Serialize(w, dnan)
	if got := w.Bytes(); !bytes.Equal(got, []byte{0x7f, 0xf8, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("double NaN wire %x", got)
	}
	if !Double.Equals(dnan, math.NaN()) {
		t.Fatal("double NaN equals")
	}
}

// Java BigInteger.ZERO.toByteArray() is {0x00} — wire `packInt(1) 0x00`.
func TestBigIntegerZeroWire(t *testing.T) {
	w := bin.NewWriter()
	BigInteger.Serialize(w, big.NewInt(0))
	if got := w.Bytes(); !bytes.Equal(got, []byte{0x81, 0x00}) {
		t.Fatalf("bigint zero wire %x, want 8100", got)
	}
	// BigDecimal zero unscaled inherits {0x00}
	w = bin.NewWriter()
	BigDecimal.Serialize(w, Decimal{Unscaled: big.NewInt(0), Scale: 0})
	if got := w.Bytes(); !bytes.Equal(got, []byte{0x81, 0x00, 0x80}) {
		t.Fatalf("bigdecimal zero wire %x", got)
	}
}

// Extreme int32 scales must not overflow the alignment exponent nor build
// an unbounded power of ten.
func TestBigDecimalExtremeScales(t *testing.T) {
	a := Decimal{Unscaled: big.NewInt(1), Scale: math.MinInt32}
	b := Decimal{Unscaled: big.NewInt(1), Scale: math.MaxInt32}
	if BigDecimal.Compare(a, b) <= 0 {
		t.Fatal("a = 1E+2^31 must exceed b = 1E-2^31")
	}
	if BigDecimal.Compare(b, a) >= 0 {
		t.Fatal("antisymmetry")
	}
	// equal-magnitude path aligns bounded powers
	x := Decimal{Unscaled: big.NewInt(12345), Scale: 2}
	y := Decimal{Unscaled: big.NewInt(123450), Scale: 3}
	if BigDecimal.Compare(x, y) != 0 {
		t.Fatal("123.45 == 123.450")
	}
}

// A corrupt count must panic DataCorruption BEFORE any count-scaled
// allocation — never reach make() as a huge capacity.
func TestGroupCountBound(t *testing.T) {
	mustCorrupt := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if de, ok := mapdb5.AsDBError(recover()); !ok || de.Kind != mapdb5.KindDataCorruption {
				t.Fatalf("%s: panic %v, want DataCorruption", name, recover())
			}
		}()
		fn()
	}
	empty := bin.NewReader(nil)
	mustCorrupt("fixed huge count", func() { LongFormat.DeserializeGroup(empty, 1<<30) })
	mustCorrupt("delta huge count", func() { LongDeltaFormat.DeserializeGroup(bin.NewReader(nil), 1<<30) })
	mustCorrupt("blob huge count", func() { StringGroupFormat.DeserializeGroup(bin.NewReader(nil), 1<<30) })
	mustCorrupt("columnar huge count", func() {
		NewColumnarValueFormat(ColumnLong).DeserializeGroup(bin.NewReader(nil), 1<<30)
	})
	mustCorrupt("object huge count", func() {
		NewObjectArrayFormat[int64](Long).DeserializeGroup(bin.NewReader(nil), 1<<30)
	})
	mustCorrupt("prefix huge count", func() { StringPrefixFormat.DeserializeGroup(bin.NewReader(nil), 1<<30) })
	mustCorrupt("tuple huge count", func() {
		NewTupleFormat(TupleLong).DeserializeGroup(bin.NewReader(nil), 1<<30)
	})
}

// Packed varints may not overflow their domain: a 10-byte run with top
// payload >1 exceeds u64 and must fail, not wrap.
func TestPackedOverflowDomain(t *testing.T) {
	defer func() {
		if de, ok := mapdb5.AsDBError(recover()); !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Fatalf("panic %v, want DataCorruption", recover())
		}
	}()
	over := []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}
	bin.NewReader(over).UnpackUint64()
}

// []byte elements must be deep-copied at group boundaries — mutating the
// caller's slice must not mutate the group, and mutating ToSlice output
// must not either.
func TestByteArrayGroupDeepCopy(t *testing.T) {
	src := [][]byte{{0x01, 0x02}, {0x03}}
	g := ByteArrayFormat.FromSlice(src)
	src[0][0] = 0xFF
	if got := ByteArrayFormat.Get(g, 0); got[0] != 0x01 {
		t.Fatal("FromSlice must deep-copy elements")
	}
	out := ByteArrayFormat.ToSlice(g)
	out[1][0] = 0xFF
	if got := ByteArrayFormat.Get(g, 1); got[0] != 0x03 {
		t.Fatal("ToSlice must deep-copy elements")
	}
	ins := []byte{0x02}
	g2 := ByteArrayFormat.Insert(g, 1, ins)
	ins[0] = 0xFF
	if got := ByteArrayFormat.Get(g2, 1); got[0] != 0x02 {
		t.Fatal("Insert must deep-copy the key")
	}
}

// Corruption rejection: malformed bytes → DataCorruption, never bare panic.
func TestCorruptionRejection(t *testing.T) {
	mustCorrupt := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if de, ok := mapdb5.AsDBError(recover()); !ok || de.Kind != mapdb5.KindDataCorruption {
				t.Fatalf("%s: panic %v, want DataCorruption", name, recover())
			}
		}()
		fn()
	}
	mustCorrupt("negative string len", func() {
		String.Deserialize(bin.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x8F}), SelfDelimit)
	})
	mustCorrupt("huge bytearray len", func() {
		ByteArray.Deserialize(bin.NewReader([]byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF}), SelfDelimit)
	})
	mustCorrupt("boolean != 0/1", func() {
		Boolean.Deserialize(bin.NewReader([]byte{2}), SelfDelimit)
	})
	mustCorrupt("long format truncated", func() {
		LongFormat.DeserializeGroup(bin.NewReader([]byte{1, 2}), 3)
	})
	mustCorrupt("blob offsets", func() {
		// blobLen=4, offsets {0, 9} — e out of range
		w := bin.NewWriter()
		w.WriteInt(4)
		w.WriteInt(0)
		w.WriteInt(9)
		w.WriteAll([]byte{1, 2, 3, 4})
		StringGroupFormat.DeserializeGroup(bin.NewReader(w.Bytes()), 2)
	})
	t.Run("foreign group", func(t *testing.T) {
		defer func() {
			if de, ok := mapdb5.AsDBError(recover()); !ok || de.Kind != mapdb5.KindSerialization {
				t.Fatalf("foreign group: panic %v, want Serialization", recover())
			}
		}()
		LongFormat.Size(Group("not a group"))
	})
}

// Erased-boundary contract (GD1): ValidateAny on typed values, nil bypass.
func TestErasedBoundary(t *testing.T) {
	var e AnySerializer = Long // built-in satisfies AnySerializer directly
	if !e.ValidateAny(int64(5)) {
		t.Fatal("long validates int64")
	}
	if e.ValidateAny("x") || e.ValidateAny(nil) {
		t.Fatal("long rejects non-int64/nil")
	}
	// erased serialize/deserialize roundtrip
	w := bin.NewWriter()
	e.SerializeAny(w, int64(7))
	if got := e.DeserializeAny(bin.NewReader(w.Bytes()), SelfDelimit); got != int64(7) {
		t.Fatal("erased roundtrip")
	}
	if e.CompareAny(int64(1), int64(2)) >= 0 || !e.EqualsAny(int64(3), int64(3)) {
		t.Fatal("erased compare/equals")
	}
	// Erase returns built-in adapters as-is (no double-wrap)
	if _, ok := Erase[int64](Long).(AnyOf[int64]); !ok {
		t.Fatal("Erase must return built-in AnyOf adapters as-is")
	}
}
