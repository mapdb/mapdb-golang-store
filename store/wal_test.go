package store

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

func walBase(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "db")
}

func mustWalPanic(t *testing.T, want mapdb5.Kind, what string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("%s: expected %s panic, got none", what, want)
		}
		de, ok := mapdb5.AsDBError(r)
		if !ok {
			t.Fatalf("%s: expected *DBError, got %v", what, r)
		}
		if de.Kind != want {
			t.Fatalf("%s: expected kind %s, got %s (%s)", what, want, de.Kind, de.Msg)
		}
	}()
	f()
}

func walSegFiles(t *testing.T, base string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Dir(base))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), filepath.Base(base)+".wal.") {
			out = append(out, filepath.Join(filepath.Dir(base), e.Name()))
		}
	}
	if len(out) == 0 {
		t.Fatalf("no WAL segments under %s", base)
	}
	return out
}

func TestWALCreateCommitReopen(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r1 := Put(s, "hello", ser.String)
	r2 := Put(s, "world", ser.String)
	s.Commit()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	segs := walSegFiles(t, base)
	if len(segs) != 1 {
		t.Fatalf("expected one segment, got %v", segs)
	}

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := Get(s, r1, ser.String); got != "hello" {
		t.Fatalf("r1 = %v", got)
	}
	if got, _ := Get(s, r2, ser.String); got != "world" {
		t.Fatalf("r2 = %v", got)
	}
	s.Verify()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALUncommittedLost(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r := Put(s, "committed", ser.String)
	s.Commit()
	Update(s, r, "dirty", ser.String)
	s.Close() // close without commit: the staged update is lost

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := Get(s, r, ser.String); got != "committed" {
		t.Fatalf("got %v, want committed", got)
	}
	s.Close()
}

func TestWALLockConflict(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStoreWAL(base, false); err == nil {
		t.Fatal("second open of a locked store should fail")
	} else if de, ok := mapdb5.AsDBError(err); !ok || de.Kind != mapdb5.KindLocked {
		t.Fatalf("expected KindLocked, got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStoreWAL(base, false); err != nil {
		t.Fatalf("reopen after close should succeed: %v", err)
	}
}

func TestWALReadOnly(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r := Put(s, "v", ser.String)
	s.Commit()
	s.Close()

	ro, err := OpenStoreWALRO(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if !ro.IsReadOnly() {
		t.Fatal("read-only store must report IsReadOnly")
	}
	if got, _ := Get(ro, r, ser.String); got != "v" {
		t.Fatalf("got %v", got)
	}
	mustWalPanic(t, mapdb5.KindReadOnly, "put on read-only", func() {
		Put(ro, "x", ser.String)
	})
	mustWalPanic(t, mapdb5.KindReadOnly, "update on read-only", func() {
		Update(ro, r, "x", ser.String)
	})
	mustWalPanic(t, mapdb5.KindReadOnly, "commit on read-only", func() {
		ro.Commit()
	})
	ro.Close()
}

func TestWALRollback(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r := Put(s, "keep", ser.String)
	s.Commit()
	gen := s.StructuralGeneration()
	Update(s, r, "dirty", ser.String)
	doomed := Put(s, "void", ser.String)
	s.Rollback()
	if s.StructuralGeneration() == gen {
		t.Fatal("rollback must bump StructuralGeneration")
	}
	if got, _ := Get(s, r, ser.String); got != "keep" {
		t.Fatalf("got %v", got)
	}
	mustWalPanic(t, mapdb5.KindGetVoid, "rolled-back recid", func() {
		Get(s, doomed, ser.String)
	})
	s.Commit() // the rollback itself is not a transaction; nothing logged
	s.Verify()
	s.Close()

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := Get(s, r, ser.String); got != "keep" {
		t.Fatalf("reopen got %v", got)
	}
	s.Close()
}

func TestWALDeletePrealloc(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	p := s.Preallocate()
	s.Commit()
	if got := s.Get(p, ser.Erase(ser.String)); got != nil {
		t.Fatalf("preallocated record must read nil, got %v", got)
	}
	Update(s, p, "filled", ser.String)
	d := Put(s, "doomed", ser.String)
	s.Commit()
	Delete(s, d, ser.String)
	s.Commit()
	mustWalPanic(t, mapdb5.KindGetVoid, "deleted recid", func() {
		Get(s, d, ser.String)
	})
	s.Close()

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := Get(s, p, ser.String); got != "filled" {
		t.Fatalf("prealloc+update replay: got %v", got)
	}
	mustWalPanic(t, mapdb5.KindGetVoid, "deleted recid after reopen", func() {
		Get(s, d, ser.String)
	})
	s.Verify()
	s.Close()
}

func TestWALAppendDelta(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r := s.Put([]byte("ab"), rawAny)
	s.Commit()
	if n := s.Append(r, []byte("cd")); n != 4 {
		t.Fatalf("append returned %d, want 4", n)
	}
	s.Commit()
	if got := s.Get(r, rawAny); string(got.([]byte)) != "abcd" {
		t.Fatalf("got %q", got)
	}
	s.Close()

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Get(r, rawAny); string(got.([]byte)) != "abcd" {
		t.Fatalf("append replay: got %q", got)
	}
	s.Close()
}

func TestWALAppendRefused(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	// "ab" fits in a 16-byte capacity: 4+2 used, 10 remaining.
	r := s.Put([]byte("ab"), rawAny)
	s.Commit()
	if n := s.Append(r, make([]byte, 11)); n != AppendRefused {
		t.Fatalf("oversize append returned %d, want AppendRefused", n)
	}
	if got := s.Get(r, rawAny); string(got.([]byte)) != "ab" {
		t.Fatalf("refused append changed content: %q", got)
	}
	s.Commit() // nothing staged by the refusal
	s.Close()

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Get(r, rawAny); string(got.([]byte)) != "ab" {
		t.Fatalf("reopen got %q", got)
	}
	s.Close()
}

func TestWALSegmentRollover(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWALSegmentBytes(base, false, 512)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 200)
	recids := make([]Recid, 0, 10)
	for i := 0; i < 10; i++ {
		recids = append(recids, Put(s, payload, ser.ByteArray))
		s.Commit()
	}
	if len(s.SegmentSeqs()) < 2 {
		t.Fatalf("expected rollover past 512B segments, seqs=%v", s.SegmentSeqs())
	}
	s.Close()

	s, err = OpenStoreWALSegmentBytes(base, false, 512)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recids {
		got, ok := Get(s, r, ser.ByteArray)
		if !ok || len(got) != 200 {
			t.Fatalf("recid %d: got ok=%v len=%d", i, ok, len(got))
		}
	}
	s.Verify()
	s.Close()
}

func TestWALTornTailRecovered(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r := Put(s, "data", ser.String)
	s.Commit()
	s.Close()

	// Simulate a crash mid-append: garbage tail past the last whole section.
	segs := walSegFiles(t, base)
	last := segs[len(segs)-1]
	f, err := os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{0xAB}, 100)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatalf("torn tail should be truncated, not refused: %v", err)
	}
	if got, _ := Get(s, r, ser.String); got != "data" {
		t.Fatalf("got %v", got)
	}
	// The store must still accept commits after a truncation recovery.
	r2 := Put(s, "more", ser.String)
	s.Commit()
	s.Close()

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := Get(s, r2, ser.String); got != "more" {
		t.Fatalf("post-truncate commit replay: got %v", got)
	}
	s.Close()
}

func TestWALCorruptNonFinalSegmentRefused(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWALSegmentBytes(base, false, 512)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("y"), 200)
	for i := 0; i < 10; i++ {
		Put(s, payload, ser.ByteArray)
		s.Commit()
	}
	seqs := s.SegmentSeqs()
	if len(seqs) < 2 {
		t.Fatal("need at least two segments for this test")
	}
	s.Close()

	// Corrupt one byte inside a NON-FINAL segment's body.
	victim := filepath.Join(filepath.Dir(base),
		filepath.Base(base)+".wal."+seqName(seqs[0]))
	f, err := os.OpenFile(victim, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0}
	if _, err := f.ReadAt(one, segHdr+secHdr+2); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xFF
	if _, err := f.WriteAt(one, segHdr+secHdr+2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := OpenStoreWAL(base, false); err == nil {
		t.Fatal("corrupt non-final segment must refuse the open")
	} else if de, ok := mapdb5.AsDBError(err); !ok || de.Kind != mapdb5.KindDataCorruption {
		t.Fatalf("expected KindDataCorruption, got %v", err)
	}
}

func seqName(seq int64) string {
	const digits = "0123456789abcdef"
	var b [16]byte
	for i := 15; i >= 0; i-- {
		b[i] = digits[seq&15]
		seq >>= 4
	}
	return string(b[:])
}

func TestWALCorruptSegmentHeaderRefused(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	Put(s, "x", ser.String)
	s.Commit()
	s.Close()

	// Flip a byte in the header's seq field: CRC still fails → torn verdict,
	// but the file is longer than the header → corruption, not residue.
	segs := walSegFiles(t, base)
	f, err := os.OpenFile(segs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0}
	f.ReadAt(one, 16)
	one[0] ^= 0xFF
	f.WriteAt(one, 16)
	f.Close()

	if _, err := OpenStoreWAL(base, false); err == nil {
		t.Fatal("corrupt segment header must refuse the open")
	}
}

func TestWALCheckpoint(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWALSegmentBytes(base, false, 512)
	if err != nil {
		t.Fatal(err)
	}
	s.SetMinLogBytes(1)
	s.SetSpaceAmplification(1)
	payload := bytes.Repeat([]byte("z"), 200)
	var recids []Recid
	for i := 0; i < 8; i++ {
		recids = append(recids, Put(s, payload, ser.ByteArray))
		s.Commit()
	}
	// Rewrite every record: everything in the old segments is now garbage.
	for _, r := range recids {
		Update(s, r, payload[:100], ser.ByteArray)
	}
	s.Commit()
	s.Checkpoint()
	s.Verify()

	s.Close()
	s, err = OpenStoreWALSegmentBytes(base, false, 512)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recids {
		got, ok := Get(s, r, ser.ByteArray)
		if !ok || len(got) != 100 {
			t.Fatalf("recid %d after checkpoint: ok=%v len=%d", i, ok, len(got))
		}
	}
	s.Verify()
	s.Close()
}

func TestWALDeleteOnClose(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	Put(s, "x", ser.String)
	s.Commit()
	s.SetDeleteOnClose(true)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(filepath.Dir(base))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), filepath.Base(base)) {
			t.Fatalf("delete-on-close left %s", e.Name())
		}
	}
	// A fresh open at the same base creates a brand-new store.
	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func TestWALLegacyRefused(t *testing.T) {
	base := walBase(t)
	// A regular file at <base>.wal (the v1 single-file shape) must refuse.
	if err := os.WriteFile(base+".wal", []byte("v1"), 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStoreWAL(base, false); err == nil {
		t.Fatal("v1 .wal file must refuse the open")
	}
}

func TestWALNullRecordRoundTrip(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r := s.Put(nil, ser.Erase(ser.String))
	s.Commit()
	s.Close()

	s, err = OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Get(r, ser.Erase(ser.String)); got != nil {
		t.Fatalf("null record replay: got %v", got)
	}
	s.Close()
}
