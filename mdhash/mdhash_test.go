package mdhash

import (
	"testing"

	"github.com/mapdb/mapdb-golang/object"
)

// Golden vectors produced by Java org.mapdb.hash.Hashers running on the
// mapdb-java-store classes (HashVectors.java). Placement hashes are wire
// format — these values must never drift.

func TestFmix32Golden(t *testing.T) {
	cases := []struct{ in, want int32 }{
		{0, 0},
		{1, 1364076727},
		{-1, -2114883783},
		{-2147483648, 1832674720},
		{2147483647, -104067416},
	}
	for _, c := range cases {
		if got := Fmix32(c.in); got != c.want {
			t.Fatalf("Fmix32(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestFmix64Golden(t *testing.T) {
	cases := []struct{ in, want int64 }{
		{0, 0},
		{1, -5451962507482445012},
		{-1, 7256831767414464289},
		{-9223372036854775808, -8108722261328812909},
		{9223372036854775807, -6072754518692401686},
		{-2401053089206453570, 8107210848246744204},
	}
	for _, c := range cases {
		if got := Fmix64(c.in); got != c.want {
			t.Fatalf("Fmix64(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestJavaStringHashCodeGolden(t *testing.T) {
	cases := []struct {
		in   string
		want int32
	}{
		{"", 0},
		{"a", 97},
		{"abc", 96354},
		{"hello world", 1794106052},
		{"é", 233},
		{"𝄞𝄞", 1705043028}, // supplementary chars hash as surrogate pairs
		{"日本語", 25921943},
	}
	for _, c := range cases {
		if got := JavaStringHashCode(c.in); got != c.want {
			t.Fatalf("JavaStringHashCode(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestJavaByteArrayHashCodeGolden(t *testing.T) {
	cases := []struct {
		in   []byte
		want int32
	}{
		{[]byte{}, 1},
		{[]byte{0}, 31},
		{[]byte{1, 2, 3}, 30817},
		{[]byte{0xff, 0xfe, 0xfd}, 28765}, // Java -1,-2,-3 sign-extends
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, -1631921466},
	}
	for _, c := range cases {
		if got := JavaByteArrayHashCode(c.in); got != c.want {
			t.Fatalf("JavaByteArrayHashCode(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestString32Golden(t *testing.T) {
	cases := []struct {
		in   string
		seed int32
		want int32
	}{
		{"", 0, 0}, {"", 42, 142593372},
		{"a", 0, -533258471}, {"a", 42, 1531168180},
		{"abc", 0, 2045100815}, {"abc", 42, -2027149040},
		{"hello world", 0, 14019678}, {"hello world", 42, 1082293980},
		{"é", 0, -710381526}, {"é", 42, -5642607},
		{"𝄞𝄞", 0, -817862110}, {"𝄞𝄞", 42, -1581155596},
		{"日本語", 0, 139258603}, {"日本語", 42, -645539685},
	}
	for _, c := range cases {
		if got := String32.Hash(c.in, c.seed); got != c.want {
			t.Fatalf("String32(%q,%d) = %d, want %d", c.in, c.seed, got, c.want)
		}
	}
}

func TestLong32Golden(t *testing.T) {
	cases := []struct {
		in   int64
		seed int32
		want int32
	}{
		{0, 0, 0}, {0, 42, 142593372},
		{1, 0, 1364076727}, {1, 42, -582727230},
		{-1, 0, 0}, {-1, 42, 142593372}, // Long(-1).hashCode() == 0
		{-9223372036854775808, 0, 1832674720}, {-9223372036854775808, 42, -1556718511},
		{9223372036854775807, 0, 1832674720}, {9223372036854775807, 42, -1556718511},
		{-2401053089206453570, 0, -1816785110}, {-2401053089206453570, 42, 840706870},
	}
	for _, c := range cases {
		if got := Long32.Hash(c.in, c.seed); got != c.want {
			t.Fatalf("Long32(%d,%d) = %d, want %d", c.in, c.seed, got, c.want)
		}
	}
}

func TestInt32Golden(t *testing.T) {
	cases := []struct {
		in   int32
		seed int32
		want int32
	}{
		{0, 0, 0}, {0, 42, 142593372},
		{1, 0, 1364076727}, {1, 42, -582727230},
		{-1, 0, -2114883783}, {-1, 42, 549382533},
		{-2147483648, 0, 1832674720}, {-2147483648, 42, -1556718511},
		{2147483647, 0, -104067416}, {2147483647, 42, -1896705235},
	}
	for _, c := range cases {
		if got := Int32.Hash(c.in, c.seed); got != c.want {
			t.Fatalf("Int32(%d,%d) = %d, want %d", c.in, c.seed, got, c.want)
		}
	}
}

func TestByteArray32Golden(t *testing.T) {
	cases := []struct {
		in   []byte
		seed int32
		want int32
	}{
		{[]byte{}, 0, 1364076727}, {[]byte{}, 42, -582727230},
		{[]byte{0}, 0, 1257683291}, {[]byte{0}, 42, 1956760449},
		{[]byte{1, 2, 3}, 0, -2068002827}, {[]byte{1, 2, 3}, 42, -642555967},
		{[]byte{0xff, 0xfe, 0xfd}, 0, 202933233}, {[]byte{0xff, 0xfe, 0xfd}, 42, -788372200},
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0, 732492651},
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 42, 10464954},
	}
	for _, c := range cases {
		if got := ByteArray32.Hash(c.in, c.seed); got != c.want {
			t.Fatalf("ByteArray32(%v,%d) = %d, want %d", c.in, c.seed, got, c.want)
		}
	}
}

func TestLong64Golden(t *testing.T) {
	cases := []struct {
		in   int64
		seed int64
		want int64
	}{
		{0, 0, 0}, {0, 42, -9148929187392628276},
		{1, 0, -5451962507482445012}, {1, 42, 2323475623862523413},
		{-1, 0, 7256831767414464289}, {-1, 42, 1504683213200391175},
		{-9223372036854775808, 0, -8108722261328812909}, {-9223372036854775808, 42, -6116407330888501106},
		{9223372036854775807, 0, -6072754518692401686}, {9223372036854775807, 42, -5832477917203686524},
		{-2401053089206453570, 0, 8107210848246744204}, {-2401053089206453570, 42, -7203131662668221472},
	}
	for _, c := range cases {
		if got := Long64.Hash64(c.in, c.seed); got != c.want {
			t.Fatalf("Long64(%d,%d) = %d, want %d", c.in, c.seed, got, c.want)
		}
	}
}

func TestString64Golden(t *testing.T) {
	cases := []struct {
		in   string
		seed int64
		want int64
	}{
		{"", 0, -1166397803181037274}, {"", 42, 7552429858295617514},
		{"a", 0, -9033471704281723301}, {"a", 42, 4082265923476294837},
		{"abc", 0, 3741276987793851837}, {"abc", 42, 129810338709877125},
		{"hello world", 0, 8965976371695117781}, {"hello world", 42, -6969009790409930522},
		{"é", 0, 3531542508362769526}, {"é", 42, 1175275416122363451},
		{"𝄞𝄞", 0, 9010382503579486634}, {"𝄞𝄞", 42, -3182119603768786461},
		{"日本語", 0, -4127575103361800853}, {"日本語", 42, 7428752394149067573},
	}
	for _, c := range cases {
		if got := String64.Hash64(c.in, c.seed); got != c.want {
			t.Fatalf("String64(%q,%d) = %d, want %d", c.in, c.seed, got, c.want)
		}
	}
}

func TestByteArray64Golden(t *testing.T) {
	cases := []struct {
		in   []byte
		seed int64
		want int64
	}{
		{[]byte{}, 0, -1166397803181037274}, {[]byte{}, 42, 7552429858295617514},
		{[]byte{0}, 0, -5115162479779449349}, {[]byte{0}, 42, -1978773809803392156},
		{[]byte{1, 2, 3}, 0, -173485098666215591}, {[]byte{1, 2, 3}, 42, 4770727031319030078},
		{[]byte{0xff, 0xfe, 0xfd}, 0, 5072975729522140368}, {[]byte{0xff, 0xfe, 0xfd}, 42, -1096027493544094386},
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0, -4200770210987653159},
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 42, 5084016168542979648},
	}
	for _, c := range cases {
		if got := ByteArray64.Hash64(c.in, c.seed); got != c.want {
			t.Fatalf("ByteArray64(%v,%d) = %d, want %d", c.in, c.seed, got, c.want)
		}
	}
}

func TestAdapters(t *testing.T) {
	// Mixing wraps a content hash with fmix32(content ^ seed).
	got := Mixing(func(s string) int32 { return int32(len(s)) }).Hash("abc", 0)
	want := Fmix32(3)
	if got != want {
		t.Fatalf("Mixing = %d, want %d", got, want)
	}
	got64 := Mixing64(func(s string) int64 { return int64(len(s)) }).Hash64("abc", 0)
	want64 := Fmix64(3)
	if got64 != want64 {
		t.Fatalf("Mixing64 = %d, want %d", got64, want64)
	}
	// HashingStrategy adapter: HashCode then seeded fmix64.
	st := object.HashingStrategy[string]{
		HashCode: func(s string) uint64 { return 7 },
		Equals:   func(a, b string) bool { return a == b },
	}
	if got := FromHashingStrategy(st).Hash64("x", 0); got != Fmix64(7) {
		t.Fatalf("FromHashingStrategy = %d, want %d", got, Fmix64(7))
	}
}
