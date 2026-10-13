package store

// Incremental compaction (CompactStep / CompactIncremental) — ports of Java
// StoreDirectIncrementalCompactionTest and StoreDirectCompactionTailTest plus
// Go-specific differential, crash, concurrency and index-chain-order tests.
// Verify() is the double-use / lost-extent oracle throughout.

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// ---------- helpers ----------

// payload is the Fixtures.payload analogue: deterministic bytes per (a, b).
func payload(a, b, n int) []byte {
	return randBytes(uint64(a)*1_000_003+uint64(b)*7919+uint64(n), n)
}

// putB stores v (nil = null record) with the raw serializer.
func putB(s *StoreDirect, v []byte) Recid {
	if v == nil {
		return s.Put(nil, rawAny)
	}
	return s.Put(v, rawAny)
}

func updateB(s *StoreDirect, r Recid, v []byte) {
	if v == nil {
		s.Update(r, nil, rawAny)
		return
	}
	s.Update(r, v, rawAny)
}

func getB(s *StoreDirect, r Recid) []byte {
	v := s.Get(r, rawAny)
	if v == nil {
		return nil
	}
	return v.([]byte)
}

// sameContent compares treating nil (null) and non-nil (possibly empty) as
// distinct.
func sameContent(want, got []byte) bool {
	if (want == nil) != (got == nil) {
		return false
	}
	return bytes.Equal(want, got)
}

func openDirect(t *testing.T, path string) *StoreDirect {
	t.Helper()
	s, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	return s
}

func newDirectFile(t *testing.T) (*StoreDirect, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cs.db")
	return openDirect(t, path), path
}

func checkAllB(t *testing.T, s *StoreDirect, ref map[Recid][]byte, ctx string) {
	t.Helper()
	for r, want := range ref {
		if got := getB(s, r); !sameContent(want, got) {
			t.Fatalf("%s: content mismatch recid=%d (want len %d nil=%v, got len %d nil=%v)",
				ctx, r, len(want), want == nil, len(got), got == nil)
		}
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func indexPagesOf(s *StoreDirect) []uint64 { return append([]uint64(nil), *s.indexPages.Load()...) }

func ensureIndexCapacity(s *StoreDirect, recid uint64) {
	s.withStructural(func() { s.ensureIndexCapacityLocked(recid) })
}

func assertRefusesReopen(t *testing.T, path string) {
	t.Helper()
	s, err := NewStoreDirectFile(path, true)
	if err == nil {
		s.Close()
		t.Fatal("expected reopen of a mid-compaction crash to refuse")
	}
	if de, ok := mapdb5.AsDBError(err); !ok || de.Kind != mapdb5.KindDataCorruption {
		t.Fatalf("expected DataCorruption on reopen, got %v", err)
	}
}

func copyFile(t *testing.T, from string) string {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	to := filepath.Join(t.TempDir(), "snap.db")
	if err := os.WriteFile(to, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return to
}

// ---------- argument / lifecycle contract ----------

func TestCompactStepArgumentAndClosed(t *testing.T) {
	s := NewStoreDirectHeap(true)
	mustPanic(t, mapdb5.KindMisuse, func() { s.CompactStep(0) })
	mustPanic(t, mapdb5.KindMisuse, func() { s.CompactStep(-3) })
	if n := s.CompactStep(4); n != 0 {
		t.Fatalf("empty store reclaimed %d pages", n)
	}
	if n := s.CompactIncremental(); n != 0 {
		t.Fatalf("empty store reclaimed %d pages", n)
	}
	s.Close()
	mustPanic(t, mapdb5.KindClosed, func() { s.CompactStep(1) })
	// Java checks the argument before checkClosed.
	mustPanic(t, mapdb5.KindMisuse, func() { s.CompactStep(0) })
}

// ---------- Java StoreDirectIncrementalCompactionTest ----------

func TestCompactStepReclaimsTrailingFreePages(t *testing.T) {
	s, path := newDirectFile(t)
	keepA := payload(1, 1, 90_000)
	keepB := payload(2, 1, 90_000)
	a := putB(s, keepA)
	b := putB(s, keepB)
	var doomed []Recid
	for i := 0; i < 60; i++ {
		doomed = append(doomed, putB(s, payload(100+i, 1, 100_000)))
	}
	s.Commit()
	fullTail := s.fileTail()
	fullSize := fileSize(t, path)
	for _, r := range doomed {
		s.Delete(r, rawAny)
	}
	s.Verify()

	reclaimed := s.CompactIncremental()
	s.Verify()
	if reclaimed <= 0 {
		t.Fatal("should reclaim trailing pages")
	}
	if s.fileTail() >= fullTail {
		t.Fatal("file tail should shrink")
	}
	if got := fileSize(t, path); got >= fullSize || got != s.fileTail() {
		t.Fatalf("file not truncated: size %d (was %d), fileTail %d", got, fullSize, s.fileTail())
	}
	if !bytes.Equal(getB(s, a), keepA) || !bytes.Equal(getB(s, b), keepB) {
		t.Fatal("kept records changed")
	}
	for _, r := range doomed {
		mustPanic(t, mapdb5.KindGetVoid, func() { s.Get(r, rawAny) })
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	re := openDirect(t, path)
	defer re.Close()
	re.Verify()
	if fileSize(t, path) != re.fileTail() {
		t.Fatal("close truncates to logical tail")
	}
	if !bytes.Equal(getB(re, a), keepA) || !bytes.Equal(getB(re, b), keepB) {
		t.Fatal("kept records changed after reopen")
	}
}

func TestCompactStepStepwiseSameContent(t *testing.T) {
	s, _ := newDirectFile(t)
	defer s.Close()
	ref := map[Recid][]byte{}
	rnd := rand.New(rand.NewSource(0xBEEF))
	for i := 0; i < 200; i++ {
		v := payload(i, 1, rnd.Intn(40_000))
		ref[putB(s, v)] = v
	}
	for k := range ref {
		if rnd.Intn(2) == 0 {
			s.Delete(k, rawAny)
			delete(ref, k)
		}
	}
	s.Verify()
	steps := 0
	for s.CompactStep(1) > 0 {
		s.Verify()
		checkAllB(t, s, ref, fmt.Sprintf("after step %d", steps))
		steps++
		if steps > 10_000 {
			t.Fatal("CompactStep did not terminate")
		}
	}
	if steps == 0 {
		t.Fatal("expected at least one productive step")
	}
	s.Verify()
	checkAllB(t, s, ref, "final")
}

func TestCompactStepPreservesCapacityAndAppend(t *testing.T) {
	s, _ := newDirectFile(t)
	defer s.Close()
	var doomed []Recid
	for i := 0; i < 40; i++ {
		doomed = append(doomed, putB(s, payload(i, 1, 120_000)))
	}
	base := payload(500, 1, 200)
	rec := putB(s, base)
	s.UpdateWithHeadroom(rec, base, rawAny, 4096)
	for _, r := range doomed {
		s.Delete(r, rawAny)
	}
	capBefore := s.CapacityRemaining(rec)
	if capBefore < 4096 {
		t.Fatalf("capacity %d < 4096", capBefore)
	}
	oldOff := ivOffset(s.rawIndexGet(rec))
	if s.CompactIncremental() <= 0 {
		t.Fatal("expected reclamation")
	}
	s.Verify()
	if ivOffset(s.rawIndexGet(rec)) == oldOff {
		t.Fatal("record was expected to relocate")
	}
	if !bytes.Equal(getB(s, rec), base) {
		t.Fatal("content changed")
	}
	if got := s.CapacityRemaining(rec); got != capBefore {
		t.Fatalf("capacity not preserved: %d != %d", got, capBefore)
	}
	extra := payload(500, 2, 1000)
	if n := s.Append(rec, extra); n != int64(len(base)+len(extra)) {
		t.Fatalf("append returned %d", n)
	}
	if !bytes.Equal(getB(s, rec), append(append([]byte(nil), base...), extra...)) {
		t.Fatal("append content mismatch")
	}
	s.Verify()
}

func TestCompactStepReusesFreedRecids(t *testing.T) {
	s, _ := newDirectFile(t)
	defer s.Close()
	a := putB(s, payload(1, 1, 50_000))
	var doomed []Recid
	for i := 0; i < 40; i++ {
		doomed = append(doomed, putB(s, payload(i, 1, 120_000)))
	}
	for _, r := range doomed {
		s.Delete(r, rawAny)
	}
	s.CompactIncremental()
	s.Verify()
	reused := putB(s, payload(9, 9, 10))
	if !contains(doomed, reused) {
		t.Fatalf("freed recid not reused: got %d", reused)
	}
	if !bytes.Equal(getB(s, a), payload(1, 1, 50_000)) {
		t.Fatal("content changed")
	}
	s.Verify()
}

// crashStep runs CompactStep(maxPages) with a crash hook that first flushes
// every dirty page (an OS writing back arbitrary relocated pages) and
// snapshots the file AS IT STANDS mid-step — before the poison handler can
// touch the checksum — then panics. Returns the mid-step snapshot path.
func crashStep(t *testing.T, s *StoreDirect, path string, maxPages int) string {
	t.Helper()
	var snap string
	s.cstep.crashHook = func() {
		s.vol.sync()
		snap = copyFile(t, path)
		panic("simulated crash")
	}
	defer func() { s.cstep.crashHook = nil }()
	func() {
		defer func() {
			if r := recover(); r != "simulated crash" {
				t.Fatalf("expected simulated crash, got %v", r)
			}
		}()
		s.CompactStep(maxPages)
	}()
	if snap == "" {
		t.Fatal("crash hook never ran")
	}
	return snap
}

func assertPoisonedAndRefuses(t *testing.T, s *StoreDirect, path, snap string) {
	t.Helper()
	// barrier alone (snapshot taken before the poison handler ran) refuses
	assertRefusesReopen(t, snap)
	// the faulted LIVE store is poisoned: closed, refuses use, and Close must
	// not reseal the half-relocated state as clean.
	if !s.IsClosed() {
		t.Fatal("store not closed after compaction fault")
	}
	mustPanic(t, mapdb5.KindClosed, func() { s.Get(1, rawAny) })
	mustPanic(t, mapdb5.KindClosed, func() { s.CompactStep(1) })
	_ = s.Close() // must not stamp
	assertRefusesReopen(t, path)
}

func TestCompactStepCrashRefusesReopen(t *testing.T) {
	s, path := newDirectFile(t)
	putB(s, payload(1, 1, 80_000))
	var doomed []Recid
	for i := 0; i < 40; i++ {
		doomed = append(doomed, putB(s, payload(i, 1, 120_000)))
	}
	// a live record on the tail so the step really relocates something
	tail := putB(s, payload(77, 1, 5_000))
	for _, r := range doomed {
		s.Delete(r, rawAny)
	}
	s.Commit()
	s.Verify()
	if ivOffset(s.rawIndexGet(tail)) < uint64(s.fileTail()-pageSize) {
		t.Fatal("tail record expected on the last page")
	}
	snap := crashStep(t, s, path, 64)
	assertPoisonedAndRefuses(t, s, path, snap)
}

// runPhasedConcurrency: each round first ALLOCATES junk and frees it on the
// test goroutine (quiesced), then runs readers + in-place writers on other
// goroutines while a compactor goroutine loops CompactStep until it returns
// 0. Allocation is never concurrent with reads: StoreDirect has a
// PRE-EXISTING Put/Get data race (readers read the fileTail header word in
// checkReleaseExtent without the structural lock while allocateNewPageLocked
// writes it) that -race flags with no compaction involved. In-place updates
// never touch fileTail, so writers here race only against the compactor.
func runPhasedConcurrency(t *testing.T, s *StoreDirect, readOnly, owned map[Recid][]byte,
	rounds int, junk func(round int), budget func(*rand.Rand) int) int {
	t.Helper()
	var roKeys []Recid
	for k := range readOnly {
		roKeys = append(roKeys, k)
	}
	var ownKeys []Recid
	for k := range owned {
		ownKeys = append(ownKeys, k)
	}
	const writers = 2
	var errMu sync.Mutex
	var firstErr string
	report := func(msg string) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == "" {
			firstErr = msg
		}
	}
	failed := func() bool {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr != ""
	}
	rnd := rand.New(rand.NewSource(0x5EED))
	reclaimed := 0
	for round := 0; round < rounds && !failed(); round++ {
		junk(round) // quiesced allocation + free

		var stop atomic.Bool
		var wg sync.WaitGroup
		for g := 0; g < 3; g++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						report(fmt.Sprintf("reader panic: %v", r))
					}
				}()
				r := rand.New(rand.NewSource(seed))
				for !stop.Load() {
					k := roKeys[r.Intn(len(roKeys))]
					if !bytes.Equal(getB(s, k), readOnly[k]) {
						report(fmt.Sprintf("torn read recid=%d", k))
						return
					}
				}
			}(int64(round*10 + g))
		}
		// each writer owns a private shard (keys i%writers==g), merged back
		// after the round.
		shards := make([]map[Recid][]byte, writers)
		for i, k := range ownKeys {
			if shards[i%writers] == nil {
				shards[i%writers] = map[Recid][]byte{}
			}
			shards[i%writers][k] = owned[k]
		}
		for g := 0; g < writers; g++ {
			wg.Add(1)
			go func(g int, shard map[Recid][]byte) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						report(fmt.Sprintf("writer panic: %v", r))
					}
				}()
				var keys []Recid
				for k := range shard {
					keys = append(keys, k)
				}
				r := rand.New(rand.NewSource(int64(round*100 + g)))
				for op := 0; !stop.Load(); op++ {
					k := keys[r.Intn(len(keys))]
					old := shard[k]
					if !bytes.Equal(getB(s, k), old) {
						report(fmt.Sprintf("owned read mismatch recid=%d", k))
						return
					}
					v := payload(round*1000+g, op, len(old)) // same size: in place
					updateB(s, k, v)
					shard[k] = v
				}
			}(g, shards[g])
		}
		// compactor goroutine: drains this round's reclaimable pages.
		var got atomic.Int64
		var cwg sync.WaitGroup
		cwg.Add(1)
		go func(b int) {
			defer cwg.Done()
			defer func() {
				if r := recover(); r != nil {
					report(fmt.Sprintf("compactor panic: %v", r))
				}
			}()
			for i := 0; i < 1000; i++ {
				n := s.CompactStep(b)
				got.Add(int64(n))
				if n == 0 {
					return
				}
			}
		}(budget(rnd))
		cwg.Wait()
		stop.Store(true)
		wg.Wait()
		for _, shard := range shards {
			for k, v := range shard {
				owned[k] = v
			}
		}
		reclaimed += int(got.Load())
	}
	if failed() {
		t.Fatal(firstErr)
	}
	return reclaimed
}

// Readers + in-place writers on other goroutines while a compactor goroutine
// loops CompactStep (Java concurrent_reads_consistent_while_compacting, plus
// writers); race-clean under -race and every value intact.
func TestCompactStepConcurrentReadersWriters(t *testing.T) {
	s, path := newDirectFile(t)
	readOnly := map[Recid][]byte{}
	owned := map[Recid][]byte{}
	// filler UNDER the permanent records, freed in round 0, so compaction
	// must relocate records that readers read and writers update in place.
	var filler []Recid
	for i := 0; i < 6; i++ {
		filler = append(filler, putB(s, payload(3000+i, 1, 900_000)))
	}
	for i := 0; i < 160; i++ {
		v := payload(i, 7, 200+(i%5)*3000)
		r := putB(s, v)
		if i%2 == 0 {
			readOnly[r] = v
		} else {
			owned[r] = v
		}
	}
	offsBefore := map[Recid]uint64{}
	for r := range owned {
		offsBefore[r] = ivOffset(s.rawIndexGet(r))
	}
	reclaimed := runPhasedConcurrency(t, s, readOnly, owned, 25,
		func(round int) {
			if round == 0 {
				for _, f := range filler {
					s.Delete(f, rawAny)
				}
			}
			var junk []Recid
			for i := 0; i < 20; i++ {
				junk = append(junk, putB(s, payload(round, i, 60_000)))
			}
			for _, j := range junk {
				s.Delete(j, rawAny)
			}
		},
		func(r *rand.Rand) int { return 1 + r.Intn(3) })
	if reclaimed == 0 {
		t.Fatal("compaction never reclaimed a page under concurrency")
	}
	moved := 0
	for r, o := range offsBefore {
		if ivOffset(s.rawIndexGet(r)) != o {
			moved++
		}
	}
	if moved == 0 {
		t.Fatal("no writer-owned record was relocated")
	}
	s.Verify()
	checkAllB(t, s, readOnly, "read-only")
	checkAllB(t, s, owned, "owned")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	re := openDirect(t, path)
	defer re.Close()
	re.Verify()
	checkAllB(t, re, readOnly, "read-only after reopen")
	checkAllB(t, re, owned, "owned after reopen")
}

// ---------- fuzz / randomized differential ----------

func fuzzBytes(size int, seed int64) []byte { return randBytes(uint64(seed), size) }

func pickRecid(ref map[Recid][]byte, rnd *rand.Rand) Recid {
	idx := rnd.Intn(len(ref))
	for r := range ref {
		if idx == 0 {
			return r
		}
		idx--
	}
	panic("unreachable")
}

type fuzzCfg struct {
	ops, stepEvery, maxBudget int
	reopenAt                  map[int]bool
	pickSize                  func(*rand.Rand) int
	nullOneIn                 int
	commitOneIn               int
}

// runCompactFuzz drives random put/update/delete/get/commit interleaved with
// CompactStep(random budget) against a shadow map, with Close+reopen round
// trips. Returns the linked-move telemetry of the final handle plus the
// running total of reclaimed pages.
func runCompactFuzz(t *testing.T, seed int64, cfg fuzzCfg) (linkedMoves int64, reclaimed int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fuzz.db")
	rnd := rand.New(rand.NewSource(seed))
	ref := map[Recid][]byte{}
	s := openDirect(t, path)
	defer func() {
		if !s.IsClosed() {
			s.Close()
		}
	}()
	pickV := func(op int) []byte {
		if rnd.Intn(cfg.nullOneIn) == 0 {
			return nil
		}
		return fuzzBytes(cfg.pickSize(rnd), int64(op)^seed)
	}
	for op := 0; op < cfg.ops; op++ {
		kind := rnd.Intn(100)
		switch {
		case kind < 45 || len(ref) == 0:
			v := pickV(op)
			ref[putB(s, v)] = v
		case kind < 60:
			r := pickRecid(ref, rnd)
			v := pickV(op ^ 0x9999)
			updateB(s, r, v)
			ref[r] = v
		case kind < 80:
			r := pickRecid(ref, rnd)
			s.Delete(r, rawAny)
			delete(ref, r)
		default:
			r := pickRecid(ref, rnd)
			if got := getB(s, r); !sameContent(ref[r], got) {
				t.Fatalf("seed %x op %d: get mismatch recid=%d", seed, op, r)
			}
		}
		if cfg.commitOneIn > 0 && rnd.Intn(cfg.commitOneIn) == 0 {
			s.Commit()
		}
		if op%cfg.stepEvery == cfg.stepEvery-1 {
			reclaimed += s.CompactStep(1 + rnd.Intn(cfg.maxBudget))
			s.Verify()
			checkAllB(t, s, ref, fmt.Sprintf("seed %x op %d", seed, op))
		}
		if cfg.reopenAt[op] {
			reclaimed += int(s.CompactIncremental())
			s.Verify()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openDirect(t, path)
			s.Verify()
			checkAllB(t, s, ref, fmt.Sprintf("seed %x reopen op %d", seed, op))
		}
	}
	reclaimed += int(s.CompactIncremental())
	s.Verify()
	checkAllB(t, s, ref, "final")
	linkedMoves = s.cstep.linkedMovesApplied
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDirect(t, path)
	s.Verify()
	checkAllB(t, s, ref, "final reopen")
	return linkedMoves, reclaimed
}

// Java fuzz_incremental_compact_with_reopen (op count scaled for -race).
func TestCompactStepFuzzWithReopen(t *testing.T) {
	_, reclaimed := runCompactFuzz(t, 0x1CEC0FFEE, fuzzCfg{
		ops: 2000, stepEvery: 200, maxBudget: 4, nullOneIn: 15,
		reopenAt: map[int]bool{650: true, 1300: true},
		pickSize: func(rnd *rand.Rand) int {
			r := rnd.Intn(100)
			switch {
			case r < 65:
				return rnd.Intn(500)
			case r < 92:
				return 500 + rnd.Intn(60_000)
			}
			return 200_000 + rnd.Intn(700_000)
		},
	})
	if reclaimed == 0 {
		t.Fatal("fuzz never reclaimed a page")
	}
}

// Randomized differential: frequent CompactStep(1..8) interleaved with
// mutations and commits, small-to-oversize values, several seeds.
func TestCompactStepRandomDifferential(t *testing.T) {
	for i := int64(0); i < 4; i++ {
		seed := 0xD1FF + i*0x9E3779B9
		t.Run(fmt.Sprintf("seed-%x", seed), func(t *testing.T) {
			_, reclaimed := runCompactFuzz(t, seed, fuzzCfg{
				ops: 900, stepEvery: 25, maxBudget: 8, nullOneIn: 12, commitOneIn: 40,
				reopenAt: map[int]bool{300: true, 600: true},
				pickSize: func(rnd *rand.Rand) int {
					r := rnd.Intn(100)
					switch {
					case r < 60:
						return rnd.Intn(300)
					case r < 90:
						return 300 + rnd.Intn(150_000)
					case r < 97:
						return 300_000 + rnd.Intn(600_000)
					}
					return 1_100_000 + rnd.Intn(900_000) // linked
				},
			})
			if reclaimed == 0 {
				t.Fatal("never reclaimed a page")
			}
		})
	}
}

// ---------- Java StoreDirectCompactionTailTest ----------

// buildTopIndexPage: TOP page is a relocatable index page (ordinal 0) with
// >=2 fully free data pages below it. Returns (d3 recid on a retained page,
// recid 65529 whose slot lives on the moved index page).
func buildTopIndexPage(t *testing.T, s *StoreDirect, d3, small []byte) (Recid, Recid) {
	t.Helper()
	r1 := putB(s, payload(1, 1, 1_000_000)) // page1
	r2 := putB(s, payload(2, 1, 1_000_000)) // page2
	r3 := putB(s, d3)                       // page3
	for i := 0; i < 65525; i++ {            // recids 4..65528
		s.Put(nil, rawAny)
	}
	rs := putB(s, small) // recid 65529 -> allocates the top index page
	if rs != 65529 || len(indexPagesOf(s)) != 1 {
		t.Fatalf("index page must be the single top page (rs=%d pages=%v)", rs, indexPagesOf(s))
	}
	if int64(indexPagesOf(s)[0]) != s.fileTail()-pageSize {
		t.Fatal("index page is not the top page")
	}
	s.Delete(r1, rawAny)
	s.Delete(r2, rawAny)
	return r3, rs
}

// buildTrailingLinked: ~2.5 MiB 3-chunk linked record on the trailing pages
// and two fully-free data pages below it.
func buildTrailingLinked(s *StoreDirect, big []byte) Recid {
	r1 := putB(s, payload(1, 1, 1_000_000))
	r2 := putB(s, payload(2, 1, 1_000_000))
	rl := putB(s, big)
	s.Delete(r1, rawAny)
	s.Delete(r2, rawAny)
	return rl
}

func TestCompactStepIndexPageMoveAndReopen(t *testing.T) {
	s, path := newDirectFile(t)
	d3 := payload(3, 1, 1_000_000)
	small := payload(7, 7, 100)
	r3, rs := buildTopIndexPage(t, s, d3, small)
	fullTail := s.fileTail()
	oldIndexPage := indexPagesOf(s)[0]
	s.Verify()

	reclaimed := s.CompactIncremental()
	s.Verify()
	if s.cstep.indexPageMovesApplied == 0 {
		t.Fatal("index page not relocated")
	}
	if reclaimed <= 0 || s.fileTail() >= fullTail {
		t.Fatal("no reclamation")
	}
	if indexPagesOf(s)[0] >= oldIndexPage {
		t.Fatal("index page did not move lower")
	}
	if !bytes.Equal(getB(s, rs), small) || !bytes.Equal(getB(s, r3), d3) {
		t.Fatal("content changed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	re := openDirect(t, path)
	defer re.Close()
	re.Verify()
	if !bytes.Equal(getB(re, 65529), small) || !bytes.Equal(getB(re, 3), d3) {
		t.Fatal("content changed after reopen")
	}
	// new recids still allocate correctly through the relocated page
	r := putB(re, []byte("after"))
	if !bytes.Equal(getB(re, r), []byte("after")) {
		t.Fatal("post-move put")
	}
	re.Verify()
}

func TestCompactStepMultiAdjacentIndexPageMove(t *testing.T) {
	s, path := newDirectFile(t)
	var big []Recid
	for i := 0; i < 5; i++ {
		big = append(big, putB(s, payload(i, 1, 1_000_000))) // pages 1..5
	}
	for i := 0; i < 4; i++ {
		s.Delete(big[i], rawAny)
	}
	keep := big[4]
	ensureIndexCapacity(s, 65528+131070+1)
	oldPages := indexPagesOf(s)
	if len(oldPages) != 2 {
		t.Fatalf("want two index pages, got %v", oldPages)
	}
	fullTail := s.fileTail()
	s.Verify()

	reclaimed := s.CompactStep(2)
	s.Verify()
	if s.cstep.indexPageMovesApplied != 2 || reclaimed != 2 {
		t.Fatalf("moves=%d reclaimed=%d", s.cstep.indexPageMovesApplied, reclaimed)
	}
	if s.fileTail() >= fullTail {
		t.Fatal("tail did not shrink")
	}
	newPages := indexPagesOf(s)
	if len(newPages) != 2 || newPages[0] >= oldPages[0] || newPages[1] >= oldPages[1] {
		t.Fatalf("index pages not moved lower: %v -> %v", oldPages, newPages)
	}
	if !bytes.Equal(getB(s, keep), payload(4, 1, 1_000_000)) {
		t.Fatal("content changed")
	}
	s.Close()
	re := openDirect(t, path)
	defer re.Close()
	re.Verify()
}

// GO-CS1: a moved index page must never land BELOW a retained one (the Go
// opener requires a strictly increasing chain). Java would take page 1 here.
func TestCompactStepIndexChainStaysIncreasing(t *testing.T) {
	s, path := newDirectFile(t)
	r1 := putB(s, payload(1, 1, 1_000_000)) // page1 (freed below)
	ensureIndexCapacity(s, recidsPerZero+1) // ordinal 0 -> page2
	r2 := putB(s, payload(2, 1, 1_000_000)) // page3 (kept)
	r3 := putB(s, payload(3, 1, 1_000_000)) // page4 (freed)
	ensureIndexCapacity(s, recidsPerZero+recidsPerPage+1)
	pages := indexPagesOf(s) // ordinal 1 -> page5 (top)
	if len(pages) != 2 || int64(pages[0]) != 2*pageSize || int64(pages[1]) != 5*pageSize {
		t.Fatalf("unexpected layout %v", pages)
	}
	s.Delete(r1, rawAny)
	s.Delete(r3, rawAny)
	s.Verify()

	if n := s.CompactStep(1); n != 1 {
		t.Fatalf("reclaimed %d", n)
	}
	s.Verify()
	pages = indexPagesOf(s)
	if int64(pages[1]) != 4*pageSize {
		t.Fatalf("ordinal 1 should move to page4 (above ordinal 0), got %v", pages)
	}
	s.CompactIncremental()
	s.Verify()
	pages = indexPagesOf(s)
	for i := 1; i < len(pages); i++ {
		if pages[i] <= pages[i-1] {
			t.Fatalf("index chain not increasing: %v", pages)
		}
	}
	if !bytes.Equal(getB(s, r2), payload(2, 1, 1_000_000)) {
		t.Fatal("content changed")
	}
	s.Close()
	re := openDirect(t, path)
	defer re.Close()
	re.Verify()
	if !bytes.Equal(getB(re, r2), payload(2, 1, 1_000_000)) {
		t.Fatal("content changed after reopen")
	}
}

func TestCompactStepLinkedChunkMoveAndReopen(t *testing.T) {
	s, path := newDirectFile(t)
	big := payload(9, 9, 2_500_000)
	rl := buildTrailingLinked(s, big)
	fullTail := s.fileTail()
	s.Verify()

	reclaimed := s.CompactIncremental()
	s.Verify()
	if s.cstep.linkedMovesApplied == 0 {
		t.Fatal("no linked chunk relocated")
	}
	if reclaimed <= 0 || s.fileTail() >= fullTail {
		t.Fatal("no reclamation")
	}
	if !bytes.Equal(getB(s, rl), big) {
		t.Fatal("linked content changed")
	}
	if !ivIsLinked(s.rawIndexGet(rl)) {
		t.Fatal("linked record re-planed to plain")
	}
	if s.CapacityRemaining(rl) != 0 {
		t.Fatal("linked records take no appends")
	}
	if s.Append(rl, []byte{1}) != AppendRefused {
		t.Fatal("append on a linked record must be refused")
	}
	s.Verify()
	s.Close()
	re := openDirect(t, path)
	defer re.Close()
	re.Verify()
	if !bytes.Equal(getB(re, rl), big) {
		t.Fatal("linked content changed after reopen")
	}
}

func TestCompactStepCrashDuringIndexPageMove(t *testing.T) {
	s, path := newDirectFile(t)
	buildTopIndexPage(t, s, payload(3, 1, 1_000_000), payload(7, 7, 100))
	s.Commit()
	s.Verify()
	snap := crashStep(t, s, path, 1)
	if s.cstep.indexPageMovesApplied != 0 {
		t.Fatal("telemetry must not count a crashed step")
	}
	assertPoisonedAndRefuses(t, s, path, snap)
}

func TestCompactStepCrashDuringLinkedMove(t *testing.T) {
	s, path := newDirectFile(t)
	buildTrailingLinked(s, payload(9, 9, 2_500_000))
	s.Commit()
	s.Verify()
	snap := crashStep(t, s, path, 64)
	assertPoisonedAndRefuses(t, s, path, snap)
}

// A failure in the I/O path itself (not the hook) must poison too: the
// truncate at the end of the step fails because the file was closed under it.
func TestCompactStepIOFailurePoisons(t *testing.T) {
	s, path := newDirectFile(t)
	putB(s, payload(1, 1, 50_000))
	var doomed []Recid
	for i := 0; i < 12; i++ {
		doomed = append(doomed, putB(s, payload(i, 2, 200_000)))
	}
	for _, r := range doomed {
		s.Delete(r, rawAny)
	}
	s.Commit()
	s.cstep.crashHook = func() { _ = s.vol.file.Close() } // later sync/truncate fail
	func() {
		defer func() {
			de, ok := mapdb5.AsDBError(recover())
			if !ok || de.Kind != mapdb5.KindVolumeIO {
				t.Fatalf("expected VolumeIO panic, got %v", de)
			}
		}()
		s.CompactStep(4)
	}()
	if !s.IsClosed() {
		t.Fatal("not poisoned")
	}
	_ = s.Close()
	assertRefusesReopen(t, path)
}

func TestCompactStepBadPlanNoopUniversal(t *testing.T) { assertBadPlanNoop(t, 1, false) }
func TestCompactStepBadPlanNoopIndex(t *testing.T)     { assertBadPlanNoop(t, 2, false) }
func TestCompactStepBadPlanNoopLinked(t *testing.T)    { assertBadPlanNoop(t, 3, true) }

// Go-only tamper mode 4: FLAG_LINKED cleared on a planned linked move. The
// tiling is unaffected, so ONLY validateLinkedChainsAfterMove can reject it
// (mode 3 is also caught by the exact-tiling validator).
func TestCompactStepBadPlanNoopLinkedFlag(t *testing.T) { assertBadPlanNoop(t, 4, true) }

// With tamperMode active every plan that would relocate the tampered kind is
// rejected by the pre-write validators (never applied, never corrupts); mode 1
// is a strict 0-page no-op. After clearing the tamper the blocked kind applies.
func assertBadPlanNoop(t *testing.T, tamperMode int, linked bool) {
	universal := tamperMode == 1
	s, path := newDirectFile(t)
	var probe Recid
	var probeVal []byte
	if linked {
		probeVal = payload(9, 9, 2_500_000)
		probe = buildTrailingLinked(s, probeVal)
	} else {
		probeVal = payload(7, 7, 100)
		_, probe = buildTopIndexPage(t, s, payload(3, 1, 1_000_000), probeVal)
	}
	s.Verify()
	s.Commit()
	tailBefore := s.fileTail()
	linkedBefore := s.cstep.linkedMovesApplied
	idxBefore := s.cstep.indexPageMovesApplied
	var before []byte
	if universal {
		before, _ = os.ReadFile(path)
	}

	s.cstep.tamperMode = tamperMode
	budget := 1
	if linked {
		budget = 64
	}
	reclaimed := 0
	for guard := 0; ; guard++ {
		n := s.CompactStep(budget)
		if n <= 0 {
			break
		}
		reclaimed += n
		s.Verify()
		if !bytes.Equal(getB(s, probe), probeVal) {
			t.Fatal("probe changed under tamper")
		}
		if guard > 50 {
			t.Fatal("CompactStep did not terminate under tamper")
		}
	}
	s.cstep.tamperMode = 0

	if linked && s.cstep.linkedMovesApplied != linkedBefore {
		t.Fatal("linked move applied under tamper")
	}
	if !linked && s.cstep.indexPageMovesApplied != idxBefore {
		t.Fatal("index move applied under tamper")
	}
	if universal {
		if reclaimed != 0 || s.fileTail() != tailBefore {
			t.Fatal("universal tamper must be a strict no-op")
		}
		// no volume write at all: the on-disk bytes are unchanged
		if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
			t.Fatal("rejected plan wrote to the file")
		}
	}
	s.Verify()
	if !bytes.Equal(getB(s, probe), probeVal) {
		t.Fatal("probe changed")
	}

	s.CompactIncremental()
	s.Verify()
	if linked && s.cstep.linkedMovesApplied <= linkedBefore {
		t.Fatal("linked move not applied after clearing tamper")
	}
	if !linked && s.cstep.indexPageMovesApplied <= idxBefore {
		t.Fatal("index move not applied after clearing tamper")
	}
	if !bytes.Equal(getB(s, probe), probeVal) {
		t.Fatal("probe changed after compaction")
	}
	s.Close()
	re := openDirect(t, path)
	defer re.Close()
	re.Verify()
}

// Java verify_oracle_fuzz_multi_seed_with_linked (op count scaled for -race).
func TestCompactStepLinkedFuzzMultiSeed(t *testing.T) {
	var total int64
	for i := int64(0); i < 4; i++ {
		seed := 0xA5A51234 + i*0x9E3779B9
		m, _ := runCompactFuzz(t, seed, fuzzCfg{
			ops: 900, stepEvery: 150, maxBudget: 4, nullOneIn: 20,
			reopenAt: map[int]bool{500: true},
			pickSize: func(rnd *rand.Rand) int {
				r := rnd.Intn(100)
				switch {
				case r < 70:
					return rnd.Intn(2000)
				case r < 90:
					return 2000 + rnd.Intn(300_000)
				}
				return 1_100_000 + rnd.Intn(2_000_000)
			},
		})
		total += m
	}
	if total == 0 {
		t.Fatal("linked-chunk relocation never exercised across the fuzz seeds")
	}
}

// Java concurrent_reads_consistent_with_linked_while_compacting: large
// LINKED records are read concurrently while compaction relocates them.
func TestCompactStepConcurrentReadsWithLinked(t *testing.T) {
	s, _ := newDirectFile(t)
	defer s.Close()
	readOnly := map[Recid][]byte{}
	owned := map[Recid][]byte{}
	for i := 0; i < 40; i++ {
		v := payload(i, 3, 300+(i%4)*2000)
		if i%2 == 0 {
			readOnly[putB(s, v)] = v
		} else {
			owned[putB(s, v)] = v
		}
	}
	// filler UNDER the permanent linked records, freed in round 0, so
	// compaction must relocate linked chunks while readers read them.
	var filler []Recid
	for i := 0; i < 8; i++ {
		filler = append(filler, putB(s, payload(2000+i, 1, 1_000_000)))
	}
	for i := 0; i < 3; i++ {
		v := payload(1000+i, 5, 1_200_000+i*200_000)
		readOnly[putB(s, v)] = v
	}
	linkedBefore := s.cstep.linkedMovesApplied
	runPhasedConcurrency(t, s, readOnly, owned, 12,
		func(round int) {
			if round == 0 {
				for _, f := range filler {
					s.Delete(f, rawAny)
				}
			}
			var junk []Recid
			for i := 0; i < 8; i++ {
				junk = append(junk, putB(s, payload(round, i, 700_000)))
			}
			for i := 0; i < 2; i++ {
				junk = append(junk, putB(s, payload(round, 90+i, 1_300_000)))
			}
			for _, j := range junk {
				s.Delete(j, rawAny)
			}
		},
		func(*rand.Rand) int { return 3 })
	if s.cstep.linkedMovesApplied == linkedBefore {
		t.Fatal("no linked relocation happened while readers ran")
	}
	s.Verify()
	checkAllB(t, s, readOnly, "read-only")
	checkAllB(t, s, owned, "owned")
}
