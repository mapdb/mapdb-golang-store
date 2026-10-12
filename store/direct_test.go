package store

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

var bytesSer = ser.Erase[[]byte](ser.ByteArray)

func TestTCKDirectHeap(t *testing.T) {
	runAll(t, func() Store { return NewStoreDirectHeap(true) })
	runAll(t, func() Store { return NewStoreDirectHeap(false) })
	t.Run("delta", func(t *testing.T) { tckDelta(t, NewStoreDirectHeap(true)) })
	t.Run("delta", func(t *testing.T) { tckDelta(t, NewStoreDirectHeap(false)) })
}

func TestDirectFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	r1 := s.Put("hello", ser.Erase[string](ser.String))
	r2 := s.Put(int64(42), ser.Erase[int64](ser.Long))
	r3 := s.Preallocate()
	s.Update(r3, []byte{1, 2, 3}, bytesSer)
	s.Put(nil, bytesSer)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v := s2.Get(r1, ser.Erase[string](ser.String)); v != "hello" {
		t.Fatalf("r1: %v", v)
	}
	if v := s2.Get(r2, ser.Erase[int64](ser.Long)); v != int64(42) {
		t.Fatalf("r2: %v", v)
	}
	if v := s2.Get(r3, bytesSer); !bytes.Equal(v.([]byte), []byte{1, 2, 3}) {
		t.Fatalf("r3: %v", v)
	}
	s2.Verify()
}

// Records larger than MAX_CAPACITY (~1 MiB) are stored as linked chains.
func TestDirectLinkedRecord(t *testing.T) {
	s := NewStoreDirectHeap(true)
	defer s.Close()
	big := make([]byte, 2*1024*1024+37)
	rnd := rand.New(rand.NewSource(1))
	rnd.Read(big)
	r := s.Put(big, bytesSer)
	if got := s.Get(r, bytesSer).([]byte); !bytes.Equal(got, big) {
		t.Fatal("linked roundtrip mismatch")
	}
	s.Verify()
	// update in place via a second linked write
	big2 := make([]byte, 3*1024*1024)
	rnd.Read(big2)
	s.Update(r, big2, bytesSer)
	if got := s.Get(r, bytesSer).([]byte); !bytes.Equal(got, big2) {
		t.Fatal("linked update mismatch")
	}
	s.Verify()
	// shrink below the linked threshold
	s.Update(r, []byte("small"), bytesSer)
	if v := s.Get(r, bytesSer).([]byte); string(v) != "small" {
		t.Fatal("shrink")
	}
	s.Delete(r, bytesSer)
	s.Verify()
}

// Linked (oversize) records must survive a durable close/reopen on a file
// volume — the heap-linked test above exercises memory only.
func TestDirectLinkedFileReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	s, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 2*1024*1024+37)
	rand.New(rand.NewSource(2)).Read(big)
	r := s.Put(big, bytesSer)
	s.Close()

	s2, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Get(r, bytesSer).([]byte); !bytes.Equal(got, big) {
		t.Fatal("linked record mismatch after reopen")
	}
	s2.Verify()
}

// Compact must rebuild a dense file that reopens identically.
func TestDirectCompactReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	s, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	var live []Recid
	for i := 0; i < 64; i++ {
		live = append(live, s.Put([]byte{byte(i), byte(i >> 8)}, bytesSer))
	}
	// Delete every other record so free space exists to reclaim; add one
	// linked record so the compacted file must rebuild chunk chains too.
	for i := 0; i < len(live); i += 2 {
		s.Delete(live[i], bytesSer)
		live[i] = 0
	}
	big := make([]byte, 2*1024*1024+11)
	rand.New(rand.NewSource(3)).Read(big)
	bigRecid := s.Put(big, bytesSer)
	s.Verify()
	s.Compact()
	s.Verify()
	s.Close()

	s2, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i, r := range live {
		if r == 0 {
			continue
		}
		want := []byte{byte(i), byte(i >> 8)}
		if got := s2.Get(r, bytesSer).([]byte); !bytes.Equal(got, want) {
			t.Fatalf("recid %d: got %v want %v", r, got, want)
		}
	}
	if got := s2.Get(bigRecid, bytesSer).([]byte); !bytes.Equal(got, big) {
		t.Fatal("linked record lost through compact")
	}
	s2.Verify()
}

func TestDirectNullAndPreallocPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	s, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	null := s.Put(nil, bytesSer)
	pre := s.Preallocate()
	s.Close()

	s2, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v := s2.Get(null, bytesSer); v != nil {
		t.Fatal("null record")
	}
	if v := s2.Get(pre, bytesSer); v != nil {
		t.Fatal("prealloc record")
	}
	// preallocated recid must not appear in AllRecids but can be updated
	for r := range s2.AllRecids() {
		if r == pre {
			t.Fatal("prealloc in allRecids")
		}
	}
	s2.Update(pre, []byte{9}, bytesSer)
	s2.Verify()
}

func TestDirectCorruptFileRejected(t *testing.T) {
	dir := t.TempDir()
	// garbage file
	bad := filepath.Join(dir, "bad.db")
	s, err := NewStoreDirectFile(bad, true)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	// flip a header byte inside the checksum-covered region
	f, _ := openFileVolume(bad)
	f.ensureAvailable(pageSize)
	p := f.slice(100)
	p.data[100] ^= 0xFF
	p.dirty.Store(true)
	f.sync()
	f.closeVol(-1)
	if _, err := NewStoreDirectFile(bad, true); err == nil {
		t.Fatal("corrupt header accepted")
	} else if de, ok := mapdb5.AsDBError(err); !ok || de.Kind != mapdb5.KindDataCorruption {
		t.Fatalf("err %v, want DataCorruption", err)
	}
}

// Ported from Rust corrupt_plain_extent_is_rejected_before_release: a crafted
// index value must yield DataCorruption before any extent is released.
func TestDirectCorruptIndexReleaseRejected(t *testing.T) {
	for _, oc := range [][2]int64{{0, 1}, {pageSize * 2, 1}, {pageSize*2 - 16, 2}} {
		s := NewStoreDirectHeap(true)
		recid := s.Put([]byte{1, 2, 3, 4, 5, 6, 7, 8}, bytesSer)
		s.indexSet(recid, ivCompose(uint32(oc[1]), uint64(oc[0]), 0))
		before := s.rawIndexGet(recid)
		mustPanic(t, mapdb5.KindDataCorruption, func() { s.Delete(recid, bytesSer) })
		if s.rawIndexGet(recid) != before {
			t.Fatal("index slot mutated on rejected release")
		}
		mustPanic(t, mapdb5.KindDataCorruption, func() {
			s.Update(recid, nil, bytesSer)
		})
		s.Close()
	}
}

func TestDirectReadPushDown(t *testing.T) {
	s := NewStoreDirectHeap(true)
	defer s.Close()
	r := s.Put(int64(7), ser.Erase[int64](ser.Long))
	got := s.Read(r, recordReader{})
	if got != 7 {
		t.Fatalf("read %d", got)
	}
	null := s.Put(nil, ser.Erase[int64](ser.Long))
	if v := s.Read(null, recordReader{}); v != 0 {
		t.Fatalf("null read %d", v)
	}
}

type recordReader struct{ RecordReadBase }

func (recordReader) OnBytes(r *bin.Reader, size int) int64 {
	return ser.Long.Deserialize(r, size)
}
