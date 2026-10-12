package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

func TestTCKAppendOnly(t *testing.T) {
	runAll(t, func() Store { return NewStoreAppendOnly() })
	t.Run("delta", func(t *testing.T) { tckDelta(t, NewStoreAppendOnly()) })
	// eager compaction must not change any TCK-visible behaviour
	runAll(t, func() Store { return NewStoreAppendOnlyMinGarbage(0) })
	t.Run("delta_eager", func(t *testing.T) { tckDelta(t, NewStoreAppendOnlyMinGarbage(0)) })
}

// Index-entry packing is the store's only layout contract (non-durable).
func TestAppendOnlyPacking(t *testing.T) {
	if aoUsedBits != 21 || aoMaxOffset != 1<<43-1 || aoIndexPageSize != 1<<17 {
		t.Fatal("packing constants")
	}
	if ivMaxCapacity >= 1<<aoUsedBits {
		t.Fatal("used field cannot hold max capacity")
	}
	iv := aoPack(aoMaxOffset&^15, int(aoUsedMask))
	if aoOffset(iv) != aoMaxOffset&^15 || aoUsed(iv) != int(aoUsedMask) {
		t.Fatal("pack roundtrip")
	}
	for _, sentinel := range []uint64{aoIVVoid, aoIVPrealloc, aoIVNull, aoIVDeleted} {
		if aoIsLive(sentinel) {
			t.Fatal("sentinel reads as live")
		}
	}
}

const mb = int64(1) << 20

// aoPayload mirrors Java Fixtures.payload: [slot][version] BE + a fill
// derived deterministically from both.
func aoPayload(slot, version, n int) []byte {
	b := make([]byte, 8+n)
	binary.BigEndian.PutUint32(b, uint32(slot))
	binary.BigEndian.PutUint32(b[4:], uint32(version))
	x := uint32(slot)*2654435761 ^ uint32(version)*40503
	for i := 8; i < len(b); i++ {
		x = x*1664525 + 1013904223
		b[i] = byte(x >> 24)
	}
	return b
}

func aoCheckPayload(slot int, b []byte) bool {
	if len(b) < 8 || int(binary.BigEndian.Uint32(b)) != slot {
		return false
	}
	return bytes.Equal(b, aoPayload(slot, int(binary.BigEndian.Uint32(b[4:])), len(b)-8))
}

func getRaw(s Store, r Recid) []byte {
	v := s.Get(r, rawAny)
	if v == nil {
		return nil
	}
	return v.([]byte)
}

// Rewriting one large record forever must not grow the footprint.
func TestAppendOnlyFootprintBounded(t *testing.T) {
	s := NewStoreAppendOnlyMinGarbage(2 * mb)
	defer s.Close()
	big := aoPayload(1, 0, 100_000)
	r := s.Put(big, rawAny)
	for v := 1; v <= 1000; v++ {
		big = aoPayload(1, v, 100_000)
		s.Update(r, big, rawAny)
	}
	if s.AllocatedBytes() <= 90*mb {
		t.Fatalf("address space consumed: %d", s.AllocatedBytes())
	}
	if s.OccupiedBytes() >= 8*mb {
		t.Fatalf("footprint not bounded: %d", s.OccupiedBytes())
	}
	if !bytes.Equal(getRaw(s, r), big) {
		t.Fatal("content")
	}
	s.Verify()
}

// Random put/update/append/delete against an oracle, sweeps firing constantly.
func TestAppendOnlyContentsSurviveCompaction(t *testing.T) {
	s := NewStoreAppendOnlyMinGarbage(mb / 4)
	defer s.Close()
	rnd := rand.New(rand.NewSource(42))
	oracle := map[Recid][]byte{}
	recids := make([]Recid, 300)
	for i := range recids {
		p := aoPayload(i, 0, rnd.Intn(2000))
		recids[i] = s.Put(p, rawAny)
		oracle[recids[i]] = p
	}
	for op := 0; op < 20_000; op++ {
		i := rnd.Intn(len(recids))
		r := recids[i]
		cur, live := oracle[r]
		if r == 0 {
			live = false
		}
		choices := 3
		if !live {
			choices = 1
		}
		switch rnd.Intn(choices) {
		case 0:
			p := aoPayload(i, op, rnd.Intn(2000))
			switch {
			case !live:
				r = s.Put(p, rawAny)
				recids[i] = r
			case rnd.Intn(2) == 0:
				s.Update(r, p, rawAny)
			default:
				s.UpdateWithHeadroom(r, p, rawAny, rnd.Intn(256))
			}
			oracle[r] = p
		case 1:
			extra := aoPayload(i, op, rnd.Intn(64))
			if s.Append(r, extra) != AppendRefused {
				oracle[r] = append(append([]byte{}, cur...), extra...)
			}
		case 2:
			s.Delete(r, rawAny)
			delete(oracle, r)
			recids[i] = 0
		}
		if op%1000 == 0 {
			s.Verify()
		}
	}
	s.Compact()
	s.Verify()
	for r, want := range oracle {
		if !bytes.Equal(getRaw(s, r), want) {
			t.Fatalf("recid %d content", r)
		}
	}
	if len(oracle) <= 100 {
		t.Fatal("oracle population too small")
	}
	if s.OccupiedBytes() >= 8*mb {
		t.Fatalf("footprint not bounded: %d", s.OccupiedBytes())
	}
	if s.AllocatedBytes() <= s.OccupiedBytes() {
		t.Fatal("compaction never retired a slice")
	}
}

// Capacity (and so append headroom) must survive the copy-move.
func TestAppendOnlyHeadroomSurvivesMove(t *testing.T) {
	s := NewStoreAppendOnlyMinGarbage(math.MaxInt64)
	defer s.Close()
	base := aoPayload(7, 0, 100)
	r := s.Put(base, rawAny)
	s.UpdateWithHeadroom(r, base, rawAny, 512)
	capBefore := s.CapacityRemaining(r)
	if capBefore < 512 {
		t.Fatalf("headroom %d", capBefore)
	}
	junk := s.Put(aoPayload(8, 0, 800_000), rawAny)
	s.Update(junk, aoPayload(8, 1, 800_000), rawAny)
	s.Compact()
	s.Verify()
	if s.OccupiedBytes() >= s.AllocatedBytes() {
		t.Fatal("compaction did not run")
	}
	merged := base
	for a := 0; a < 4; a++ {
		extra := aoPayload(7, a+1, 64)
		if s.Append(r, extra) == AppendRefused {
			t.Fatalf("append %d refused after move", a)
		}
		merged = append(append([]byte{}, merged...), extra...)
	}
	if !bytes.Equal(getRaw(s, r), merged) {
		t.Fatal("merged content")
	}
	if got := s.CapacityRemaining(r); got != capBefore-4*(8+64) {
		t.Fatalf("capacity after appends %d", got)
	}
	s.Verify()
}

type aoCapture struct {
	RecordReadBase
	b []byte
}

func (c *aoCapture) OnBytes(r *bin.Reader, size int) int64 {
	c.b = append(c.b[:0], r.ReadBytes(size)...)
	return 1
}
func (c *aoCapture) OnNull() int64 { c.b = nil; return -1 }

// Readers hammer Get/Read while the writer rewrites with compaction firing
// constantly: every observed value must be an internally consistent payload.
// Under -race this is also the memory-model proof of the publication design.
func TestAppendOnlyConcurrentReadersDuringCompaction(t *testing.T) {
	s := NewStoreAppendOnlyMinGarbage(mb / 4)
	defer s.Close()
	const slots = 128
	recids := make([]Recid, slots)
	for i := range recids {
		recids[i] = s.Put(aoPayload(i, 0, 512), rawAny)
	}
	dur := 1500 * time.Millisecond
	if testing.Short() {
		dur = 300 * time.Millisecond
	}
	deadline := time.Now().Add(dur)
	var failed atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rnd := rand.New(rand.NewSource(7))
		for v := 1; time.Now().Before(deadline) && failed.Load() == nil; v++ {
			i := rnd.Intn(slots)
			s.Update(recids[i], aoPayload(i, v, 512), rawAny)
		}
	}()
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			c := &aoCapture{}
			for time.Now().Before(deadline) && failed.Load() == nil {
				i := rnd.Intn(slots)
				var b []byte
				if rnd.Intn(2) == 0 {
					b = getRaw(s, recids[i])
				} else if s.Read(recids[i], c) >= 0 {
					b = c.b
				}
				if !aoCheckPayload(i, b) {
					failed.Store("torn or missing payload")
				}
			}
		}(int64(100 + g))
	}
	wg.Wait()
	if f := failed.Load(); f != nil {
		t.Fatal(f)
	}
	s.Verify()
	if s.AllocatedBytes() <= s.OccupiedBytes() {
		t.Fatal("compaction never ran")
	}
}

// Overlapping writers are serialized (Go deviation from Java's -ea assert):
// concurrent puts/appends/deletes from many goroutines stay consistent.
func TestAppendOnlyConcurrentWriters(t *testing.T) {
	s := NewStoreAppendOnlyMinGarbage(mb / 8)
	defer s.Close()
	var wg sync.WaitGroup
	results := make([]map[Recid][]byte, 8)
	for g := range results {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			own := map[Recid][]byte{}
			rnd := rand.New(rand.NewSource(int64(g)))
			for op := 0; op < 2000; op++ {
				p := aoPayload(g, op, rnd.Intn(1500))
				r := s.Put(p, rawAny)
				own[r] = p
				if op%3 == 0 {
					s.Delete(r, rawAny)
					delete(own, r)
				}
			}
			results[g] = own
		}(g)
	}
	wg.Wait()
	s.Verify()
	for _, own := range results {
		for r, want := range own {
			if !bytes.Equal(getRaw(s, r), want) {
				t.Fatalf("recid %d", r)
			}
		}
	}
}

func TestAppendOnlyStatesAndErrors(t *testing.T) {
	s := NewStoreAppendOnly()
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Get(0, rawAny) })
	mustPanic(t, mapdb5.KindGetVoid, func() { s.Get(1<<40, rawAny) })
	p := s.Preallocate()
	if s.CapacityRemaining(p) != 0 {
		t.Fatal("prealloc capacity")
	}
	// first append establishes a preallocated record with exact capacity
	if got := s.Append(p, []byte{1, 2, 3}); got != 3 {
		t.Fatalf("establishing append %d", got)
	}
	if s.CapacityRemaining(p) != 16-4-3 {
		t.Fatal("establishing append capacity")
	}
	mustPanic(t, mapdb5.KindRecordTooLarge, func() { s.Put(make([]byte, ivMaxCapacity), rawAny) })
	mustPanic(t, mapdb5.KindRecordTooLarge, func() {
		s.UpdateWithHeadroom(p, []byte{1}, rawAny, math.MaxInt)
	})
	mustPanic(t, mapdb5.KindMisuse, func() { s.UpdateWithHeadroom(p, []byte{1}, rawAny, -1) })
	// largest plain record fits in one slice
	big := make([]byte, ivMaxCapacity-4)
	r := s.Put(big, rawAny)
	if len(getRaw(s, r)) != len(big) {
		t.Fatal("max record")
	}
	s.Verify()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal("close not idempotent")
	}
	mustPanic(t, mapdb5.KindClosed, func() { s.Get(r, rawAny) })
	mustPanic(t, mapdb5.KindClosed, func() { s.Put([]byte{1}, rawAny) })
	mustPanic(t, mapdb5.KindClosed, func() { s.CapacityRemaining(r) })
}

// A reader racing Close sees Closed, never GetVoid or a runtime panic.
func TestAppendOnlyCloseDuringReads(t *testing.T) {
	for round := 0; round < 20; round++ {
		s := NewStoreAppendOnly()
		r := s.Put(aoPayload(1, 1, 64), rawAny)
		var wg sync.WaitGroup
		var bad atomic.Value
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					stop := func() (stop bool) {
						defer func() {
							if rec := recover(); rec != nil {
								stop = true
								if e, ok := rec.(*mapdb5.DBError); !ok || e.Kind != mapdb5.KindClosed {
									bad.Store(rec)
								}
							}
						}()
						getRaw(s, r)
						return false
					}()
					if stop {
						return
					}
				}
			}()
		}
		time.Sleep(time.Millisecond)
		s.Close()
		wg.Wait()
		if b := bad.Load(); b != nil {
			t.Fatalf("reader racing close: %v", b)
		}
	}
}

// Concurrent Append + readers + compaction: readers must see a prefix-
// consistent image (base payload followed by whole appended chunks).
func TestAppendOnlyConcurrentAppendReaders(t *testing.T) {
	s := NewStoreAppendOnlyMinGarbage(mb / 8)
	defer s.Close()
	const slots = 32
	recids := make([]Recid, slots)
	for i := range recids {
		recids[i] = s.Put(aoPayload(i, 0, 100), rawAny)
		s.UpdateWithHeadroom(recids[i], aoPayload(i, 0, 100), rawAny, 4096)
	}
	chunk := func(i int) []byte { return bytes.Repeat([]byte{byte(i + 1)}, 16) }
	deadline := time.Now().Add(500 * time.Millisecond)
	var bad atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rnd := rand.New(rand.NewSource(3))
		for v := 1; time.Now().Before(deadline); v++ {
			i := rnd.Intn(slots)
			if s.Append(recids[i], chunk(i)) == AppendRefused {
				s.UpdateWithHeadroom(recids[i], aoPayload(i, v, 100), rawAny, 4096)
			}
			if v%50 == 0 { // churn so sweeps move the appended records
				s.Update(s.Put(aoPayload(999, v, 3000), rawAny), aoPayload(999, v, 10), rawAny)
			}
		}
	}()
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			for time.Now().Before(deadline) {
				i := rnd.Intn(slots)
				b := getRaw(s, recids[i])
				if len(b) < 108 || !aoCheckPayload(i, b[:108]) || (len(b)-108)%16 != 0 ||
					!bytes.Equal(b[108:], bytes.Repeat([]byte{byte(i + 1)}, len(b)-108)) {
					bad.Store("inconsistent appended image")
					return
				}
				if c := s.CapacityRemaining(recids[i]); c < 0 {
					bad.Store("negative capacity")
					return
				}
			}
		}(int64(g))
	}
	wg.Wait()
	if b := bad.Load(); b != nil {
		t.Fatal(b)
	}
	s.Verify()
}

// Index growth across page boundaries while readers resolve published recids.
func TestAppendOnlyIndexPageGrowth(t *testing.T) {
	s := NewStoreAppendOnly()
	defer s.Close()
	first := s.Put([]byte{42}, rawAny)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if b := getRaw(s, first); len(b) != 1 || b[0] != 42 {
				panic("first record lost during index growth")
			}
		}
	}()
	var last Recid
	for i := 0; i < 2*aoIndexPageSize+10; i++ {
		last = s.Preallocate()
	}
	close(done)
	wg.Wait()
	if last>>aoIndexPageShift < 2 {
		t.Fatalf("did not cross two index pages: %d", last)
	}
	s.Update(last, []byte{7}, rawAny)
	if b := getRaw(s, last); len(b) != 1 || b[0] != 7 {
		t.Fatal("record on third index page")
	}
	s.Verify()
}
