package format

import (
	"encoding/hex"
	"math"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// Golden frames produced by the compiled Java reference
// (org.mapdb.format.BufferedPageFormat, mapdb-java-store target/classes)
// via a throwaway driver — machine cross-validated, not hand-derived.
const (
	goldPutHashCrc = "00000024010b81e1487788000000000000002a8800000000000000070dc691ca00000024"
	goldDelHashCrc = "0000001b020a81e1487788000000000000002a2c6a88080000001b"
	goldPutPlain   = "0000001c010188ffffffffffffffff887fffffffffffffff0000001c"
	goldDelPlain   = "00000013020088ffffffffffffffff00000013"
	goldPutStr     = "00000022010bd7ef83cc878668c3a96c6c6f878677c3b6726c64c995492b00000022"
	goldDelStr     = "0000001a020ad7ef83cc878668c3a96c6c6f943644f80000001a"
	goldBaseFp5    = "01850658690fffc7"
	goldBaseFpNone = "0180"
	goldBaseFp100  = "01e4068ef9a9498ece7ebceaf8f433c576fe6edfad839691c95917faf0f1bcd92f1e20a40ff7a13ef716630b842cc449af43727842723c3eea77fd2e84e3f9c1d772b24e46e21c375e90890f30dbfd64dcd421f1de41dd15d1d77f1012cb9109c3dc373d6cb03f"
	// LSN-stamped frame: decode-parity only — the LSN layout is NOT frozen
	// (Java: "no golden bytes may be frozen for ports until R3").
	javaPutHashCrcLsn = "0000002c010f000000000000000581e1487788000000000000002a880000000000000007068433e20000002c"
)

func longCodec(hash, crc bool, fp int) *BufferedPageFormat[int64, int64] {
	return New[int64, int64](ser.LongFormat, ser.LongFormat, hash, crc, fp)
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func eqHex(t *testing.T, name string, got []byte, want string) {
	t.Helper()
	if g := hex.EncodeToString(got); g != want {
		t.Fatalf("%s:\n got %s\nwant %s", name, g, want)
	}
}

func mustCorrupt(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Fatalf("expected DataCorruption panic, got %v", r)
		}
	}()
	f()
}

func TestGoldenFrames(t *testing.T) {
	f := longCodec(true, true, 8)
	eqHex(t, "put", f.Encode(OpPut, 42, 7, 0), goldPutHashCrc)
	eqHex(t, "del", f.Encode(OpDelete, 42, 0, 0), goldDelHashCrc)
	eqHex(t, "lsn", f.Encode(OpPut, 42, 7, 5), javaPutHashCrcLsn)
	g := longCodec(false, false, 0)
	eqHex(t, "put plain", g.Encode(OpPut, -1, math.MaxInt64, 0), goldPutPlain)
	eqHex(t, "del plain", g.Encode(OpDelete, -1, 0, 0), goldDelPlain)
	s := New[string, string](ser.StringGroupFormat, ser.StringGroupFormat, true, true, 8)
	eqHex(t, "put str", s.Encode(OpPut, "héllo", "wörld", 0), goldPutStr)
	eqHex(t, "del str", s.Encode(OpDelete, "héllo", "", 0), goldDelStr)
}

func TestGoldenBaseFp(t *testing.T) {
	f := longCodec(true, true, 8)
	w := bin.NewWriter()
	f.WriteBaseFp(w, ser.LongFormat.FromSlice([]int64{1, 2, 3, 4, 5}))
	eqHex(t, "fp5", w.Bytes(), goldBaseFp5)
	w = bin.NewWriter()
	f.WriteBaseFp(w, nil)
	eqHex(t, "fp none", w.Bytes(), goldBaseFpNone)
	keys := make([]int64, 100)
	for i := range keys {
		keys[i] = int64(i * 7)
	}
	w = bin.NewWriter()
	f.WriteBaseFp(w, ser.LongFormat.FromSlice(keys))
	eqHex(t, "fp100", w.Bytes(), goldBaseFp100)
}

// page builds [baseLen][base][entries...] with a minimal base (empty fp
// header + one filler byte standing in for the consumer node base).
func page(f *BufferedPageFormat[int64, int64], entries ...[]byte) ([]byte, int) {
	w := bin.NewWriter()
	w.WriteInt(3)
	w.WriteU8(FPVersion)
	w.PackInt(0)
	w.WriteU8(0x80)
	logStart := w.Pos
	for _, e := range entries {
		w.WriteAll(e)
	}
	return w.CopyBytes(), logStart
}

func TestDecodeAndProbe(t *testing.T) {
	for _, mode := range []struct{ hash, crc bool }{{true, true}, {false, false}, {true, false}, {false, true}} {
		f := longCodec(mode.hash, mode.crc, 8)
		buf, ls := page(f,
			f.Encode(OpPut, 1, 10, 0),
			f.Encode(OpPut, 2, 20, 0),
			f.Encode(OpDelete, 1, 0, 0),
			f.Encode(OpPut, 2, 21, 0),
			f.Encode(OpPut, 3, 30, 7), // LSN-stamped (decode parity)
		)
		r := bin.NewReader(buf)
		if got := f.ReadBaseLen(r, 0); got != 3 {
			t.Fatalf("baseLen %d", got)
		}
		var ops []int64
		f.ForEachEntryOldestFirst(r, ls, len(buf), func(op int, k, v, lsn int64) {
			ops = append(ops, int64(op), k, v, lsn)
		})
		want := []int64{1, 1, 10, 0, 1, 2, 20, 0, 2, 1, 0, 0, 1, 2, 21, 0, 1, 3, 30, 7}
		if len(ops) != len(want) {
			t.Fatalf("ops %v", ops)
		}
		for i := range want {
			if ops[i] != want[i] {
				t.Fatalf("ops %v want %v", ops, want)
			}
		}
		var pr PointResult[int64]
		for _, probe := range []bool{true, false} {
			pb := func(k int64) []byte {
				if probe {
					return f.SerializeKey(k)
				}
				return nil
			}
			if !f.ProbeNewestFirst(r, ls, len(buf), 2, pb(2), math.MaxInt64, &pr) || !pr.Put || pr.Value != 21 {
				t.Fatalf("probe 2: %+v", pr)
			}
			if !f.ProbeNewestFirst(r, ls, len(buf), 1, pb(1), math.MaxInt64, &pr) || pr.Put {
				t.Fatalf("probe 1 must be a DELETE: %+v", pr)
			}
			if f.ProbeNewestFirst(r, ls, len(buf), 9, pb(9), math.MaxInt64, &pr) {
				t.Fatal("probe 9 matched")
			}
			// snapshot below the stamped LSN hides key 3
			if f.ProbeNewestFirst(r, ls, len(buf), 3, pb(3), 6, &pr) {
				t.Fatal("invisible entry matched")
			}
		}
		// shadow + merge + consolidate
		sh := NewShadow[int64, int64](ser.LongFormat)
		f.NetEffect(r, ls, len(buf), math.MaxInt64, sh)
		if len(sh.Entries) != 3 || !sh.Entries[0].Tomb || sh.Entries[1].Value != 21 || sh.Entries[2].Value != 30 {
			t.Fatalf("net %+v", sh.Entries)
		}
		base := ser.LongFormat.FromSlice([]int64{0, 1, 5})
		vals := ser.LongFormat.FromSlice([]int64{100, 101, 105})
		c := f.Consolidate(base, vals, r, ls, len(buf), math.MaxInt64, 100)
		if got := ser.LongFormat.ToSlice(c.Keys); len(got) != 4 || got[0] != 0 || got[1] != 2 || got[2] != 3 || got[3] != 5 {
			t.Fatalf("consolidated keys %v", got)
		}
		if got := ser.LongFormat.ToSlice(c.Values); got[1] != 21 || got[3] != 105 {
			t.Fatalf("consolidated vals %v", got)
		}
		if c := f.Consolidate(base, vals, r, ls, len(buf), math.MaxInt64, 3); !c.SplitRequired || c.KeyCount != 4 {
			t.Fatalf("split verdict %+v", c)
		}
		var mk []int64
		f.Merge(&sliceCursor{keys: []int64{0, 1, 5}, vals: []int64{100, 101, 105}}, sh, func(k, v int64) bool {
			mk = append(mk, k, v)
			return true
		})
		wantM := []int64{0, 100, 2, 21, 3, 30, 5, 105}
		if len(mk) != len(wantM) {
			t.Fatalf("merge %v", mk)
		}
		for i := range wantM {
			if mk[i] != wantM[i] {
				t.Fatalf("merge %v", mk)
			}
		}
		// range shadow
		rs := NewShadow[int64, int64](ser.LongFormat)
		f.LogShadowInRange(r, ls, len(buf), Bound[int64]{Key: 2, Set: true, Incl: false}, Bound[int64]{Key: 3, Set: true, Incl: true}, math.MaxInt64, rs)
		if len(rs.Entries) != 1 || rs.Entries[0].Key != 3 {
			t.Fatalf("range shadow %+v", rs.Entries)
		}
	}
}

type sliceCursor struct {
	keys, vals []int64
	i          int
}

func (c *sliceCursor) HasNext() bool { return c.i < len(c.keys) }
func (c *sliceCursor) Next()         { c.i++ }
func (c *sliceCursor) Key() int64    { return c.keys[c.i-1] }
func (c *sliceCursor) Value() int64  { return c.vals[c.i-1] }

func TestBaseFpNoFalseNegatives(t *testing.T) {
	f := longCodec(true, true, 8)
	keys := make([]int64, 64)
	for i := range keys {
		keys[i] = int64(i*1000003 + 17)
	}
	w := bin.NewWriter()
	w.WriteInt(0) // baseLen placeholder
	f.WriteBaseFp(w, ser.LongFormat.FromSlice(keys))
	w.WriteU8(0x80) // node base stand-in
	buf := w.CopyBytes()
	patchInt(buf, 0, int32(len(buf)-4))
	r := bin.NewReader(buf)
	var h BaseHeader
	f.ParseBaseHeader(r, 4, len(buf)-4, &h)
	for _, k := range keys {
		if !f.BaseFpMightContain(r, &h, f.SerializeKey(k)) {
			t.Fatalf("false negative for %d", k)
		}
	}
	skipped := 0
	for k := int64(-1); k > -2000; k-- {
		if !f.BaseFpMightContain(r, &h, f.SerializeKey(k)) {
			skipped++
		}
	}
	if skipped < 1500 {
		t.Fatalf("filter useless: only %d/1999 skips", skipped)
	}
	SetFpModeForTest(FpForceSearch)
	defer SetFpModeForTest(FpNormal)
	if !f.BaseFpMightContain(r, &h, f.SerializeKey(-5)) {
		t.Fatal("FORCE_SEARCH must never skip")
	}
}

func TestParseBaseHeaderFailClosed(t *testing.T) {
	f := longCodec(true, true, 8)
	var h BaseHeader
	for name, b := range map[string]string{
		"short":       "01",
		"version":     "0280aa",
		"header only": "0180",
		"k zero":      "0181" + "00" + "ff" + "aa",
		"overrun":     "0185" + "06" + "ffff",
	} {
		buf := mustHex(t, b)
		mustCorrupt(t, func() { f.ParseBaseHeader(bin.NewReader(buf), 0, len(buf), &h) })
		_ = name
	}
}

func TestTornFramesRejected(t *testing.T) {
	f := longCodec(true, true, 8)
	e := f.Encode(OpPut, 42, 7, 0)
	cases := map[string]func([]byte){
		"trailer":  func(b []byte) { patchInt(b, len(b)-4, int32(len(b)+1)) },
		"lead":     func(b []byte) { patchInt(b, 0, int32(len(b)-1)) },
		"opcode":   func(b []byte) { b[4] = 0 },
		"keylen":   func(b []byte) { b[10] = 0xBF }, // packInt keyLen 63 > entry
		"valueflg": func(b []byte) { b[5] &^= fHasValue },
	}
	for name, mut := range cases {
		b := append([]byte(nil), e...)
		mut(b)
		buf, ls := page(f, b)
		r := bin.NewReader(buf)
		var pr PointResult[int64]
		mustCorrupt(t, func() { f.ProbeNewestFirst(r, ls, len(buf), 42, f.SerializeKey(42), math.MaxInt64, &pr) })
		mustCorrupt(t, func() {
			f.ForEachEntryOldestFirst(r, ls, len(buf), func(int, int64, int64, int64) {})
		})
		_ = name
	}
	// truncated log (a torn append the store never published would not be
	// visible; a size that cuts an entry is corruption)
	buf, ls := page(f, e)
	r := bin.NewReader(buf)
	mustCorrupt(t, func() {
		f.ForEachEntryOldestFirst(r, ls, len(buf)-1, func(int, int64, int64, int64) {})
	})
}

func TestVerifyEntryCRC(t *testing.T) {
	f := longCodec(true, true, 8)
	e := f.Encode(OpPut, 42, 7, 0)
	if !f.VerifyEntry(bin.NewReader(e), 0, len(e)) {
		t.Fatal("clean entry fails CRC")
	}
	e[16] ^= 1 // flip a key byte
	if f.VerifyEntry(bin.NewReader(e), 0, len(e)) {
		t.Fatal("bit flip not detected")
	}
	g := longCodec(true, false, 8)
	p := g.Encode(OpPut, 1, 1, 0)
	if !g.VerifyEntry(bin.NewReader(p), 0, len(p)) {
		t.Fatal("no-CRC entry must verify trivially")
	}
}

// A non-canonical key element disables keyHash and the fingerprint (both
// hash canonical bytes).
func TestNonCanonicalDisablesHash(t *testing.T) {
	f := New[int64, int64](nonCanonicalLong{ser.LongFormat}, ser.LongFormat, true, true, 8)
	if f.BaseFingerprintEnabled() || f.writeKeyHash {
		t.Fatal("non-canonical key must disable keyHash + fingerprint")
	}
	e := f.Encode(OpPut, 1, 2, 0)
	if e[5]&fHasKeyHash != 0 {
		t.Fatal("keyHash flag written for non-canonical key")
	}
}

type nonCanonicalLong struct{ ser.GroupFormat[int64] }

func (n nonCanonicalLong) Element() ser.Serializer[int64] {
	return nonCanonicalElem{n.GroupFormat.Element()}
}

type nonCanonicalElem struct{ ser.Serializer[int64] }

func (nonCanonicalElem) EqualsBySerializedBytes() bool { return false }
