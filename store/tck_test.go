package store

import (
	"bytes"
	"path/filepath"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// Store TCK (spec 05 §2): exercises every record state transition and calls
// Verify after each mutation. Runs generically against all stores. Exception
// contract → DBError kind assertions.

var longSer = ser.Erase[int64](ser.Long)

func mustPanic(t *testing.T, kind mapdb5.Kind, fn func()) {
	t.Helper()
	defer func() {
		de, ok := mapdb5.AsDBError(recover())
		if !ok || de.Kind != kind {
			t.Fatalf("panic %v, want %v", recover(), kind)
		}
	}()
	fn()
}

func collectRecids(s Store) []Recid {
	var out []Recid
	for r := range s.AllRecids() {
		out = append(out, r)
	}
	return out
}

func contains(hay []Recid, needle Recid) bool {
	for _, r := range hay {
		if r == needle {
			return true
		}
	}
	return false
}

// rawSer is the size-driven raw-bytes serializer (Java Fixtures.RAW):
// content == value, so appended delta bytes are observable (a length-prefixed
// serializer would hide the delta region). Used by the delta TCK.
type rawSer struct{}

func (rawSer) Serialize(w *bin.Writer, v []byte) { w.WriteAll(v) }
func (rawSer) Deserialize(r *bin.Reader, size int) []byte {
	if size < 0 {
		panic(mapdb5.NewMisuse("raw serializer needs a framed size"))
	}
	return r.ReadBytes(size)
}
func (rawSer) FixedSize() int                { return -1 }
func (rawSer) SizeHint() int                 { return 32 }
func (rawSer) Compare(a, b []byte) int       { return bytes.Compare(a, b) }
func (rawSer) Equals(a, b []byte) bool       { return bytes.Equal(a, b) }
func (rawSer) NaturalOrder() bool            { return true }
func (rawSer) EqualsBySerializedBytes() bool { return true }

var rawAny = ser.Erase[[]byte](rawSer{})

func randBytes(seed uint64, n int) []byte {
	x := seed*0x9E3779B97F4A7C15 + 1
	out := make([]byte, n)
	for i := range out {
		x = x*6364136223846793005 + 1442695040888963407
		out[i] = byte(x >> 33)
	}
	return out
}

// readProbe decodes an i64 from bytes (byte stores) or downcasts (heap).
type readProbe struct {
	sawNull bool
	value   int64
	sawVal  bool
}

func (p *readProbe) OnBytes(r *bin.Reader, _ int) int64 {
	v := r.ReadLong()
	p.value, p.sawVal = v, true
	return v
}
func (p *readProbe) OnObject(obj any) int64 {
	v := obj.(int64)
	p.value, p.sawVal = v, true
	return v
}
func (p *readProbe) OnNull() int64 { p.sawNull = true; return 0 }

// Core state machine, generic over any Store.
func tckStates(t *testing.T, s Store) {
	s.Verify()

	// Void: a never-allocated recid → GetVoid on every record operation.
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Get(Recid(999999), longSer) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Delete(Recid(999999), longSer) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Read(Recid(999999), &readProbe{}) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Update(Recid(999999), int64(1), longSer) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.CompareAndSwap(Recid(999999), nil, int64(1), longSer) })

	// put → Live
	r1 := s.Put(int64(42), longSer)
	s.Verify()
	if v := s.Get(r1, longSer); v != int64(42) {
		t.Fatalf("get %v", v)
	}
	p := &readProbe{}
	if s.Read(r1, p) != 42 || p.value != 42 {
		t.Fatal("read")
	}

	// preallocate → Preallocated: Get nil, excluded from AllRecids
	rp := s.Preallocate()
	s.Verify()
	if v := s.Get(rp, longSer); v != nil {
		t.Fatal("prealloc get must be nil")
	}
	p = &readProbe{}
	s.Read(rp, p)
	if !p.sawNull {
		t.Fatal("prealloc read must dispatch OnNull")
	}
	if contains(collectRecids(s), rp) {
		t.Fatal("preallocated recid must be excluded from AllRecids")
	}
	if !contains(collectRecids(s), r1) {
		t.Fatal("live recid missing from AllRecids")
	}

	// update preallocated → Live
	s.Update(rp, int64(7), longSer)
	s.Verify()
	if v := s.Get(rp, longSer); v != int64(7) {
		t.Fatal("update prealloc→live")
	}
	if !contains(collectRecids(s), rp) {
		t.Fatal("filled recid must appear in AllRecids")
	}

	// update Live → Null content: Get nil but record still exists
	s.Update(r1, nil, longSer)
	s.Verify()
	if v := s.Get(r1, longSer); v != nil {
		t.Fatal("update to null")
	}
	p = &readProbe{}
	s.Read(r1, p)
	if !p.sawNull {
		t.Fatal("null record must dispatch OnNull")
	}
	// explicit-null is still an existing record: present in AllRecids
	if !contains(collectRecids(s), r1) {
		t.Fatal("explicit-null recid must remain in AllRecids")
	}

	// delete → Deleted: every record op → GetVoid; CAS too
	s.Delete(r1, longSer)
	s.Verify()
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Get(r1, longSer) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Read(r1, &readProbe{}) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Update(r1, int64(1), longSer) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.CompareAndSwap(r1, nil, int64(1), longSer) })
	if contains(collectRecids(s), r1) {
		t.Fatal("deleted recid must leave AllRecids")
	}
}

// CAS logical-equality semantics.
func tckCas(t *testing.T, s Store) {
	r := s.Put(int64(100), longSer)
	if s.CompareAndSwap(r, int64(5), int64(6), longSer) {
		t.Fatal("wrong expected must fail")
	}
	if v := s.Get(r, longSer); v != int64(100) {
		t.Fatal("failed CAS must not change")
	}
	if !s.CompareAndSwap(r, int64(100), int64(200), longSer) {
		t.Fatal("right expected must swap")
	}
	if v := s.Get(r, longSer); v != int64(200) {
		t.Fatal("post-swap")
	}
	s.Verify()
	// swap Live → Null
	if !s.CompareAndSwap(r, int64(200), nil, longSer) {
		t.Fatal("swap to null")
	}
	if v := s.Get(r, longSer); v != nil {
		t.Fatal("post-null")
	}
	// Null matches nil expected → swap back to Live
	if !s.CompareAndSwap(r, nil, int64(9), longSer) {
		t.Fatal("null→live swap")
	}
	if v := s.Get(r, longSer); v != int64(9) {
		t.Fatal("post null→live")
	}
	if s.CompareAndSwap(r, nil, int64(1), longSer) {
		t.Fatal("expecting null on live value must fail")
	}
	s.Verify()
}

func tckRecidReuseAndClose(t *testing.T, s Store) {
	a := s.Put(int64(1), longSer)
	s.Delete(a, longSer)
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Get(a, longSer) })
	s.Verify()
	// delete frees the recid for reuse — a transactional store frees it at
	// COMMIT, so the guarantee holds only for non-tx stores.
	if !s.IsTx() {
		var used []Recid
		for i := 0; i < 4; i++ {
			used = append(used, s.Put(int64(i), longSer))
		}
		if !contains(used, a) {
			t.Fatal("deleted recid was not reused")
		}
	}
	// the iterator is a snapshot: mutations during iteration don't corrupt it
	r := s.Put(int64(99), longSer)
	var before []Recid
	for x := range s.AllRecids() {
		before = append(before, x)
	}
	s.Delete(r, longSer)
	if contains(before, r) != true {
		t.Fatal("snapshot iterator missing live recid")
	}
	if contains(collectRecids(s), r) {
		t.Fatal("fresh iterator still sees deleted recid")
	}
	s.Verify()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !s.IsClosed() {
		t.Fatal("IsClosed")
	}
	mustPanic(t, mapdb5.KindClosed, func() { s.Get(a, longSer) })
}

// Delta-capability TCK (spec 02 §1, port of Java DeltaTCK via the Rust suite).
func tckDelta(t *testing.T, s StoreDelta) {
	get := func(r Recid) []byte {
		v := s.Get(r, rawAny)
		if v == nil {
			return nil
		}
		return v.([]byte)
	}
	concat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

	// --- append grows content byte-exactly within provisioned capacity ---
	base := randBytes(1, 12)
	r := s.Put(base, rawAny)
	s.UpdateWithHeadroom(r, base, rawAny, 64)
	s.Verify()
	d1, d2, d3 := []byte{10, 11, 12}, []byte{20, 21}, []byte{30}
	if got := s.Append(r, d1); got != int64(len(base)+3) {
		t.Fatalf("append d1 → %d", got)
	}
	if got := s.Append(r, d2); got != int64(len(base)+5) {
		t.Fatalf("append d2 → %d", got)
	}
	if got := s.Append(r, d3); got != int64(len(base)+6) {
		t.Fatalf("append d3 → %d", got)
	}
	s.Verify()
	if merged := concat(base, d1, d2, d3); !bytes.Equal(get(r), merged) {
		t.Fatal("merged content")
	}

	// --- refused exactly at the capacity boundary ---
	base = randBytes(2, 8)
	rb := s.Put(base, rawAny)
	s.UpdateWithHeadroom(rb, base, rawAny, 40)
	s.Commit()
	capRem := s.CapacityRemaining(rb)
	if capRem < 40 {
		t.Fatalf("headroom must be honoured, got %d", capRem)
	}
	fill := make([]byte, capRem)
	for i := range fill {
		fill[i] = byte(i + 1)
	}
	if got := s.Append(rb, fill); got != int64(len(base))+capRem {
		t.Fatalf("fill append → %d", got)
	}
	if c := s.CapacityRemaining(rb); c != 0 {
		t.Fatalf("at boundary, cap=%d", c)
	}
	bmerged := concat(base, fill)
	if got := s.Append(rb, []byte{99}); got != AppendRefused {
		t.Fatal("overflow append must be refused")
	}
	if c := s.CapacityRemaining(rb); c != 0 {
		t.Fatal("refused append changes capacity")
	}
	if !bytes.Equal(get(rb), bmerged) {
		t.Fatal("refused append changes content")
	}
	s.Verify()

	// --- append on a preallocated record establishes it ---
	rp := s.Preallocate()
	if v := s.Get(rp, rawAny); v != nil {
		t.Fatal("prealloc reads null")
	}
	if contains(collectRecids(s), rp) {
		t.Fatal("prealloc in AllRecids")
	}
	d := randBytes(3, 14)
	if got := s.Append(rp, d); got != int64(len(d)) {
		t.Fatalf("prealloc append → %d", got)
	}
	s.Verify()
	if !bytes.Equal(get(rp), d) {
		t.Fatal("prealloc append content")
	}
	if !contains(collectRecids(s), rp) {
		t.Fatal("established recid in AllRecids")
	}

	// --- update resets the appended region ---
	base = randBytes(4, 8)
	ru := s.Put(base, rawAny)
	s.UpdateWithHeadroom(ru, base, rawAny, 32)
	s.Append(ru, []byte{7, 7, 7, 7})
	if !bytes.Equal(get(ru), concat(base, []byte{7, 7, 7, 7})) {
		t.Fatal("appended content")
	}
	base2 := randBytes(40, 10)
	s.Update(ru, base2, rawAny)
	if !bytes.Equal(get(ru), base2) {
		t.Fatal("update must reset appended region")
	}
	s.Verify()

	// --- update_with_headroom bytes are immediately appendable ---
	const H = 48
	base = randBytes(5, 6)
	rh := s.Put(base, rawAny)
	s.UpdateWithHeadroom(rh, base, rawAny, H)
	if c := s.CapacityRemaining(rh); c < H {
		t.Fatalf("headroom %d < %d", c, H)
	}
	block := make([]byte, H)
	for i := range block {
		block[i] = byte(i)
	}
	if got := s.Append(rh, block); got != int64(len(base)+H) {
		t.Fatal("headroom bytes must be immediately appendable")
	}
	if !bytes.Equal(get(rh), concat(base, block)) {
		t.Fatal("headroom append content")
	}
	s.Verify()

	// --- delete after appends → GetVoid on every delta op ---
	base = randBytes(6, 8)
	rd := s.Put(base, rawAny)
	s.UpdateWithHeadroom(rd, base, rawAny, 32)
	s.Append(rd, []byte{1, 2, 3})
	s.Delete(rd, rawAny)
	s.Verify()
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Get(rd, rawAny) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Append(rd, []byte{1}) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.CapacityRemaining(rd) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Update(rd, base, rawAny) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.UpdateWithHeadroom(rd, base, rawAny, 8) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.CompareAndSwap(rd, base, base, rawAny) })

	// --- zero-length append is a no-op returning the current size ---
	base = randBytes(7, 13)
	rz := s.Put(base, rawAny)
	if got := s.Append(rz, nil); got != int64(len(base)) {
		t.Fatal("zero append size")
	}
	if !bytes.Equal(get(rz), base) {
		t.Fatal("zero append content")
	}

	// --- CAS after appends compares the merged logical value ---
	base = randBytes(8, 8)
	rc := s.Put(base, rawAny)
	s.UpdateWithHeadroom(rc, base, rawAny, 64)
	s.Append(rc, d1)
	s.Append(rc, d2)
	merged := concat(base, d1, d2)
	if !bytes.Equal(get(rc), merged) {
		t.Fatal("merged")
	}
	if s.CompareAndSwap(rc, base, randBytes(80, 5), rawAny) {
		t.Fatal("CAS against pre-append image must fail")
	}
	replacement := randBytes(82, 7)
	if !s.CompareAndSwap(rc, merged, replacement, rawAny) {
		t.Fatal("CAS against merged image")
	}
	if !bytes.Equal(get(rc), replacement) {
		t.Fatal("post-CAS")
	}
	s.Verify()
}

func runAll(t *testing.T, make func() Store) {
	t.Run("states", func(t *testing.T) { tckStates(t, make()) })
	t.Run("cas", func(t *testing.T) { tckCas(t, make()) })
	t.Run("recid_close", func(t *testing.T) { tckRecidReuseAndClose(t, make()) })
}

func TestTCKHeap(t *testing.T) {
	runAll(t, func() Store { return NewStoreOnHeap(true) })
	runAll(t, func() Store { return NewStoreOnHeap(false) })
}

func TestTCKByteArray(t *testing.T) {
	runAll(t, func() Store { return NewStoreByteArray(true) })
	runAll(t, func() Store { return NewStoreByteArray(false) })
	t.Run("delta", func(t *testing.T) { tckDelta(t, NewStoreByteArray(true)) })
	t.Run("delta", func(t *testing.T) { tckDelta(t, NewStoreByteArray(false)) })
}

// StoreWAL is transactional (staged-until-commit): the generic state/CAS/delta
// TCK must pass identically because reads merge staged mutations over the inner
// committed image.
func TestTCKWAL(t *testing.T) {
	mk := func(ts bool) func() Store {
		return func() Store {
			s, err := openStoreWALCfg(filepath.Join(t.TempDir(), "db"),
				walOptions{threadSafe: ts})
			if err != nil {
				t.Fatalf("open WAL: %v", err)
			}
			return s
		}
	}
	runAll(t, mk(true))
	runAll(t, mk(false))
	t.Run("delta", func(t *testing.T) {
		s, err := openStoreWALCfg(filepath.Join(t.TempDir(), "db"),
			walOptions{threadSafe: true})
		if err != nil {
			t.Fatal(err)
		}
		tckDelta(t, s)
	})
}

// A non-i64 record type through the heap store's object path + logical CAS:
// the case-insensitive serializer proves CAS uses EqualsAny, not byte
// equality.
func TestHeapObjectAndLogicalCAS(t *testing.T) {
	ci := ser.Erase[string](caseInsSer{})
	s := NewStoreOnHeap(true)
	r := s.Put("Hello", ci)
	if v := s.Get(r, ci); v != "Hello" {
		t.Fatalf("get %v", v)
	}
	if !s.CompareAndSwap(r, "HELLO", "world", ci) {
		t.Fatal("case-insensitive CAS")
	}
	if v := s.Get(r, ci); v != "world" {
		t.Fatal("post-CAS")
	}
	s.Verify()
}

type caseInsSer struct{}

func (caseInsSer) Serialize(w *bin.Writer, v string) { ser.String.Serialize(w, v) }
func (caseInsSer) Deserialize(r *bin.Reader, n int) string {
	return ser.String.Deserialize(r, n)
}
func (caseInsSer) FixedSize() int { return -1 }
func (caseInsSer) SizeHint() int  { return 16 }
func (caseInsSer) Compare(a, b string) int {
	ca, cb := bytes.ToUpper([]byte(a)), bytes.ToUpper([]byte(b))
	return bytes.Compare(ca, cb)
}
func (caseInsSer) Equals(a, b string) bool {
	return caseInsSer{}.Compare(a, b) == 0
}
func (caseInsSer) NaturalOrder() bool            { return false }
func (caseInsSer) EqualsBySerializedBytes() bool { return true }

func TestReadOnlyWrapper(t *testing.T) {
	inner := NewStoreByteArray(true)
	r := inner.Put(int64(42), longSer)
	ro := NewReadOnly(inner)
	if !ro.IsReadOnly() {
		t.Fatal("IsReadOnly")
	}
	if v := ro.Get(r, longSer); v != int64(42) {
		t.Fatal("read passthrough")
	}
	ro.Verify()
	mustPanic(t, mapdb5.KindReadOnly, func() { ro.Preallocate() })
	mustPanic(t, mapdb5.KindReadOnly, func() { ro.Put(int64(1), longSer) })
	mustPanic(t, mapdb5.KindReadOnly, func() { ro.Update(r, int64(7), longSer) })
	mustPanic(t, mapdb5.KindReadOnly, func() { ro.CompareAndSwap(r, int64(42), int64(7), longSer) })
	mustPanic(t, mapdb5.KindReadOnly, func() { ro.Delete(r, longSer) })
	mustPanic(t, mapdb5.KindReadOnly, func() { ro.Compact() })
	ro.Commit() // no-op, tolerated
	if v := ro.Get(r, longSer); v != int64(42) {
		t.Fatal("rejected mutations must not change the record")
	}
}

func TestLeaseTable(t *testing.T) {
	lt := NewLeaseTable()
	rw, err := lt.Acquire(1, LeaseReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lt.Acquire(1, LeaseReadWrite); err == nil {
		t.Fatal("double RW must fail")
	}
	if _, err := lt.Acquire(1, LeaseReadOnly); err == nil {
		t.Fatal("RO while RW must fail")
	}
	rw.Release()
	ro1, _ := lt.Acquire(1, LeaseReadOnly)
	ro2, err := lt.Acquire(1, LeaseReadOnly)
	if err != nil {
		t.Fatal("RO+RO")
	}
	if _, err := lt.Acquire(1, LeaseReadWrite); err == nil {
		t.Fatal("RW while RO must fail")
	}
	ro1.Release()
	ro1.Release() // double Release is an idempotent no-op
	if _, err := lt.Acquire(1, LeaseReadWrite); err == nil {
		t.Fatal("RW while remaining RO must fail")
	}
	ro2.Release()
	ro2.Release() // idempotent: the second release must not free the slot
	if _, err := lt.Acquire(1, LeaseReadWrite); err != nil {
		t.Fatal("RW after all released")
	}
	if _, err := lt.Acquire(2, LeaseReadWrite); err != nil {
		t.Fatal("different header is independent")
	}
}

// Erased-boundary validation (GD1): a wrong-typed record is Serialization.
func TestErasedValidation(t *testing.T) {
	s := NewStoreOnHeap(true)
	mustPanic(t, mapdb5.KindSerialization, func() {
		s.Put("not a long", longSer)
	})
	r := s.Put(int64(1), longSer)
	mustPanic(t, mapdb5.KindSerialization, func() {
		s.Update(r, "x", longSer)
	})
	mustPanic(t, mapdb5.KindSerialization, func() {
		s.CompareAndSwap(r, "x", int64(2), longSer)
	})
	mustPanic(t, mapdb5.KindSerialization, func() {
		s.Get(r, ser.Erase[string](ser.String))
	})
	// nil bypasses validation
	s.Update(r, nil, longSer)
	if v := s.Get(r, longSer); v != nil {
		t.Fatal("nil update")
	}
}
