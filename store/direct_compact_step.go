package store

// ---------- incremental compaction ----------
//
// Port of Java StoreDirect's bounded, structure-aware relocation compactor
// (compactStep / compactIncremental). Instead of rebuilding the WHOLE store
// (as Compact does), CompactStep reclaims a bounded run of TRAILING pages by
// relocating their live records into free space in the retained region, then
// truncating. The whole step runs under the EXCLUSIVE commitLock (plus the
// structural lock) — coarse but BOUNDED, so the store is fully available
// BETWEEN steps.
//
// Relocation protocol (per record): write the new copy at its planned target,
// then FLIP the recid index slot (one parity1 putU64) to the new offset; the
// old extent is reclaimed by the free-list rebuild / truncate. recids never
// change.
//
// Crash safety follows the header-checksum model exactly like Compact: a
// step-0 barrier durably invalidates the checksum, the step mutates the
// volume, then (and only at the very end) the header is stamped, synced and
// the file truncated. A crash before the final stamp reopens as a checksum
// mismatch and is REFUSED; the completed step is the first accepted state.
//
// The free lists (free-data size-class stacks and the free-recid stack) are
// rebuilt from scratch over the retained region. The ENTIRE final tiling
// (live extents + relocation targets + rebuilt long-stack chunks + free-data
// extents) is planned IN MEMORY and validated BEFORE any volume write (exact
// tiling + index-page + linked-chain validators). An infeasible or invalid
// plan returns nil and the step retains one more page; worst case it reclaims
// nothing and returns 0. A planning bug degrades to a no-op, never corruption.
//
// Index pages (IndexPageMove) and linked/oversize chunks (LinkedChainMove) in
// the drop region are relocated with the same write-copy-then-flip primitive.
//
// Go deviations from Java (see also the comments at each site):
//   - GO-CS1: the Go (and Rust) opener requires the index-page chain to be
//     strictly increasing (loadIndexPages) and membership tests binary-search
//     the mirror. Java has no such invariant, and its target choice can
//     produce a non-increasing chain. The Go planner therefore picks, for each
//     moved ordinal, the lowest candidate page ABOVE the previous ordinal's
//     final page, and validateIndexPagesAfterMove additionally rejects a
//     non-increasing final layout. When every moved ordinal lies above all
//     retained index pages (the common case, and every Java test) the choice
//     is identical to Java's.
//   - GO-CS2: free-data size classes are emitted in ascending order (Java
//     iterates a HashMap). Layout only; the tiling is the same set.
//   - GO-CS3: a plain record whose decoded extent fails geometry (tainted
//     offset) makes the plan infeasible (nil) instead of reading a wild
//     offset; Java reads vol.getInt(off) unchecked.

import (
	"slices"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// DefaultCompactStepPages is the default per-step page budget of
// CompactIncremental.
const DefaultCompactStepPages = 4

// compactStepTestHooks carries TEST-ONLY knobs and telemetry (Java
// testCompactCrashHook / testTamperMode / test*MovesApplied). All fields are
// read and written only under the exclusive commitLock + structural lock, or
// by a test between steps.
type compactStepTestHooks struct {
	// crashHook runs inside executeCompaction after every relocation is
	// applied and before the free lists are rebuilt (the dangerous window).
	crashHook func()
	// tamperMode != 0 corrupts each freshly planned plan (see tamperPlan).
	tamperMode int
	// telemetry: applied relocations of the two structural move kinds.
	linkedMovesApplied    int64
	indexPageMovesApplied int64
}

// csMove is a plain record that must move off a dropped page.
type csMove struct {
	recid     uint64
	oldOffset int64
	capUnits  int64 // capacity preserved across the move
	copyBytes int64 // 4 + used: header+content actually copied
	flags     uint64
	newOffset int64
}

// csChunk is one rebuilt long-stack chunk (16-aligned, <=160B).
type csChunk struct {
	offset    int64
	chunkSize int64
	values    []uint64 // raw parity-encoded, push order
}

// csStack is a rebuilt long stack: master link offset + chunk chain (0 = bottom).
type csStack struct {
	masterLinkOffset int64
	chain            []*csChunk
}

// csIndexPageMove relocates a whole 1 MiB index page, preserving its ordinal.
type csIndexPageMove struct {
	ordinal int
	oldPage int64
	newPage int64
}

// csLinkedMove is a linked record with at least one chunk in the drop region.
// The FULL chain is captured; newOffsets[k] >= 0 marks a moved chunk.
type csLinkedMove struct {
	recid        uint64
	flags        uint64 // preserved index flags (linked, archive)
	oldOffsets   []int64
	capUnits     []int64
	copyBytes    []int64 // linkedChunkHdr + dataLen
	newOffsets   []int64 // target, or -1 when the chunk stays put
	totalDataLen int64
}

func (lm *csLinkedMove) finalOffset(k int) int64 {
	if lm.newOffsets[k] >= 0 {
		return lm.newOffsets[k]
	}
	return lm.oldOffsets[k]
}

type csPlan struct {
	retainedTail   int64
	moves          []*csMove
	linkedMoves    []*csLinkedMove
	indexPageMoves []csIndexPageMove
	// index pages AFTER the move: excluded from data tiling.
	retainedIndexPagesAfterMove map[int64]struct{}
	freeDataExtents             [][2]int64 // {offset, capUnits}
	stacks                      []*csStack
}

// CompactStep reclaims up to maxPages trailing pages by relocating their live
// records (including index pages and linked chunks) into retained free space
// and truncating. Returns the number of pages reclaimed (0 = nothing more to
// reclaim, or no feasible plan). Bounded, steppable and crash-safe; safe to
// interleave with normal reads/writes/commits BETWEEN calls. maxPages <= 0
// panics KindMisuse (Java IllegalArgumentException). An execution failure
// poisons and closes the store.
func (s *StoreDirect) CompactStep(maxPages int) int {
	if maxPages <= 0 {
		panic(mapdb5.NewMisuse("maxPages must be positive"))
	}
	s.checkClosed()
	s.commitLock.Lock()
	defer s.commitLock.Unlock()
	s.checkClosed()
	s.structural.Lock()
	defer s.structural.Unlock()
	return s.compactStepLocked(maxPages)
}

// CompactIncremental drives CompactStep(DefaultCompactStepPages) to
// completion. Returns the total pages reclaimed.
func (s *StoreDirect) CompactIncremental() int64 {
	var total int64
	for {
		n := s.CompactStep(DefaultCompactStepPages)
		if n <= 0 {
			return total
		}
		total += int64(n)
	}
}

// compactStepLocked: commitLock exclusive + structural lock held.
func (s *StoreDirect) compactStepLocked(maxPages int) int {
	oldFileTail := s.fileTail()
	if oldFileTail <= pageSize {
		return 0 // no data pages
	}
	// widest drop region: up to maxPages trailing pages, never dropping page 0.
	retainedTail := oldFileTail - pageSize
	want := 1
	for want < maxPages {
		next := retainedTail - pageSize
		if next < pageSize {
			break
		}
		retainedTail = next
		want++
	}
	for retainedTail < oldFileTail {
		if plan := s.planCompaction(retainedTail, oldFileTail); plan != nil {
			s.executeCompactionPoisoning(plan)
			return int((oldFileTail - retainedTail) / pageSize)
		}
		retainedTail += pageSize // retain one more page, drop fewer
	}
	return 0
}

// executeCompactionPoisoning applies a validated plan. Planning is read-only,
// but execution mutates the volume with no rollback (the step-0 barrier made
// the on-disk checksum durably invalid): any panic poisons and closes the
// store so no later mutation/Close can reseal a half-relocated state as clean
// (same pattern as Compact).
func (s *StoreDirect) executeCompactionPoisoning(plan *csPlan) {
	defer func() {
		if r := recover(); r != nil {
			s.poisoned.Store(true)
			s.closed.Store(true)
			func() {
				defer func() { _ = recover() }()
				s.vol.putI32(oHeadChecksum, ^s.headChecksum())
				s.vol.sync()
			}()
			panic(r)
		}
	}()
	s.executeCompaction(plan)
}

// planExtentOK is the non-panicking geometry predicate for an index-derived
// plain extent (GO-CS3): tainted offset/size checked before any read.
func (s *StoreDirect) planExtentOK(off, size, fileTail int64) bool {
	if off < pageSize || off&15 != 0 || size < 16 || size&15 != 0 ||
		size/16 > maxCapUnits || (off%pageSize)+size > pageSize {
		return false
	}
	end := off + size
	if end < off || end > fileTail {
		return false
	}
	pages := *s.indexPages.Load()
	_, inIndex := slices.BinarySearch(pages, uint64(off-off%pageSize))
	return !inIndex
}

// planCompaction builds (in memory) and validates the full retained-region
// tiling for dropping [retainedTail, oldFileTail). Returns nil when this
// retainedTail is infeasible. NO volume writes.
func (s *StoreDirect) planCompaction(retainedTail, oldFileTail int64) *csPlan {
	maxRecid := s.maxRecid()
	var liveRetained [][2]int64 // {offset, sizeBytes} staying put
	var moves []*csMove
	var linkedMoves []*csLinkedMove
	var freeRecidValues []uint64
	oldPages := *s.indexPages.Load() // current mirror (never mutated in place)

	for recid := uint64(1); recid <= maxRecid; recid++ {
		iv := s.rawIndexGet(recid)
		if iv == 0 {
			freeRecidValues = append(freeRecidValues, Parity1Set(recid<<1))
			continue
		}
		if !ivParityOK(iv) {
			return nil // corruption: leave it to Verify()/normal paths
		}
		cap := ivCapUnits(iv)
		if cap == ivCapDeleted {
			freeRecidValues = append(freeRecidValues, Parity1Set(recid<<1))
			continue
		}
		if cap == ivCapNull {
			continue // null/prealloc: index slot only
		}
		if ivIsLinked(iv) {
			chunks := s.linkedChain(iv) // fully validated {off, dataLen, capBytes}
			n := len(chunks)
			lm := &csLinkedMove{
				recid:      recid,
				flags:      iv & (ivFlagLinked | ivFlagArchive),
				oldOffsets: make([]int64, n),
				capUnits:   make([]int64, n),
				copyBytes:  make([]int64, n),
				newOffsets: make([]int64, n),
			}
			anyMove := false
			for k, c := range chunks {
				lm.oldOffsets[k] = c[0]
				lm.capUnits[k] = c[2] / 16
				lm.copyBytes[k] = linkedChunkHdr + c[1]
				lm.newOffsets[k] = -1
				lm.totalDataLen += c[1]
				if c[0] >= retainedTail {
					anyMove = true
				} else {
					liveRetained = append(liveRetained, [2]int64{c[0], c[2]})
				}
			}
			if anyMove {
				linkedMoves = append(linkedMoves, lm)
			}
			continue
		}
		off := int64(ivOffset(iv))
		capBytes := int64(cap) * 16
		if off >= retainedTail {
			if !s.planExtentOK(off, capBytes, oldFileTail) {
				return nil // GO-CS3
			}
			used := int64(s.vol.getI32(off))
			if used < 0 || 4+used > capBytes {
				return nil
			}
			moves = append(moves, &csMove{
				recid: recid, oldOffset: off, capUnits: int64(cap),
				copyBytes: 4 + used, flags: iv & ivFlagArchive, newOffset: -1,
			})
		} else {
			liveRetained = append(liveRetained, [2]int64{off, capBytes})
		}
	}

	// ---- index-page relocation: every index page in the drop region needs a lower target ----
	oldIndexSet := make(map[int64]struct{}, len(oldPages))
	for _, p := range oldPages {
		oldIndexSet[int64(p)] = struct{}{}
	}
	// pages holding ANY retained live byte cannot host a relocated index page.
	pagesWithLive := make(map[int64]struct{})
	for _, e := range liveRetained {
		pagesWithLive[e[0]-e[0]%pageSize] = struct{}{}
	}
	var candidateEmptyPages []int64
	for page := int64(pageSize); page < retainedTail; page += pageSize {
		_, isIdx := oldIndexSet[page]
		_, hasLive := pagesWithLive[page]
		if isIdx || hasLive {
			continue
		}
		candidateEmptyPages = append(candidateEmptyPages, page)
	}
	newMirror := slices.Clone(oldPages)
	var indexPageMoves []csIndexPageMove
	nextCand := 0
	for i := range oldPages {
		if int64(oldPages[i]) < retainedTail {
			continue // retained index page: not moved
		}
		// GO-CS1: keep the chain strictly increasing — skip candidates at or
		// below the previous ordinal's final page.
		if i > 0 {
			for nextCand < len(candidateEmptyPages) &&
				candidateEmptyPages[nextCand] <= int64(newMirror[i-1]) {
				nextCand++
			}
		}
		if nextCand >= len(candidateEmptyPages) {
			return nil // no free target -> no-op degrade
		}
		newPage := candidateEmptyPages[nextCand]
		nextCand++
		newMirror[i] = uint64(newPage)
		indexPageMoves = append(indexPageMoves, csIndexPageMove{
			ordinal: i, oldPage: int64(oldPages[i]), newPage: newPage,
		})
	}
	retainedIndexAfter := make(map[int64]struct{}, len(newMirror))
	for _, p := range newMirror {
		if int64(p) < retainedTail {
			retainedIndexAfter[int64(p)] = struct{}{}
		}
	}

	// retained free runs = retained data pages MINUS live records (old free
	// extents and long-stack chunks become free after the rebuild).
	runs := computeFreeRuns(retainedTail, liveRetained, retainedIndexAfter)

	// place plain moves AND moved linked chunks (largest first) into the
	// lowest run that fits.
	type placeable struct {
		capBytes int64
		linked   bool
		i, k     int
	}
	var place []placeable
	for i, m := range moves {
		place = append(place, placeable{capBytes: m.capUnits * 16, i: i})
	}
	for i, lm := range linkedMoves {
		for k := range lm.oldOffsets {
			if lm.oldOffsets[k] >= retainedTail {
				place = append(place, placeable{capBytes: lm.capUnits[k] * 16, linked: true, i: i, k: k})
			}
		}
	}
	// Java List.sort is stable; SliceStable keeps equal-size order identical.
	slices.SortStableFunc(place, func(a, b placeable) int { return cmpInt64(b.capBytes, a.capBytes) })
	slices.SortStableFunc(runs, func(a, b *[2]int64) int { return cmpInt64(a[0], b[0]) })
	for _, p := range place {
		var best *[2]int64
		for _, r := range runs {
			if r[1] >= p.capBytes {
				best = r
				break
			}
		}
		if best == nil {
			return nil // no retained hole fits
		}
		newOffset := best[0]
		best[0] += p.capBytes
		best[1] -= p.capBytes
		if !p.linked {
			moves[p.i].newOffset = newOffset
		} else {
			linkedMoves[p.i].newOffsets[p.k] = newOffset
		}
	}
	runs = slices.DeleteFunc(runs, func(r *[2]int64) bool { return r[1] == 0 })

	plan := &csPlan{
		retainedTail:                retainedTail,
		moves:                       moves,
		linkedMoves:                 linkedMoves,
		indexPageMoves:              indexPageMoves,
		retainedIndexPagesAfterMove: retainedIndexAfter,
	}
	if !buildFreeLists(runs, freeRecidValues, plan) {
		return nil
	}

	// TEST HOOK: inject a deliberate inconsistency the validators must reject.
	if s.cstep.tamperMode != 0 {
		s.tamperPlanForTest(plan)
	}

	// PRE-WRITE validators: turn any planning bug into a no-op.
	if !validateIndexPagesAfterMove(plan, oldPages, retainedTail) {
		return nil
	}
	if !validateLinkedChainsAfterMove(plan, retainedTail) {
		return nil
	}
	if !validateTiling(retainedTail, retainedIndexAfter, liveRetained, plan) {
		return nil
	}
	return plan
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// computeFreeRuns: per retained data page, the gaps not covered by live
// extents. Runs are pointers so placement can consume them in place (Java
// mutates long[] elements).
func computeFreeRuns(retainedTail int64, live [][2]int64, indexPageSet map[int64]struct{}) []*[2]int64 {
	byPage := make(map[int64][][2]int64)
	for _, e := range live {
		page := e[0] - e[0]%pageSize
		byPage[page] = append(byPage[page], e)
	}
	var runs []*[2]int64
	for page := int64(pageSize); page < retainedTail; page += pageSize {
		if _, idx := indexPageSet[page]; idx {
			continue
		}
		cursor := page
		if list := byPage[page]; list != nil {
			slices.SortStableFunc(list, func(a, b [2]int64) int { return cmpInt64(a[0], b[0]) })
			for _, e := range list {
				if e[0] > cursor {
					runs = append(runs, &[2]int64{cursor, e[0] - cursor})
				}
				cursor = e[0] + e[1]
			}
		}
		if end := page + pageSize; cursor < end {
			runs = append(runs, &[2]int64{cursor, end - cursor})
		}
	}
	return runs
}

// buildFreeLists plans the rebuilt free lists whose chunks live inside one
// reserved "arena" run (the largest), iterating the arena leftover to a
// fixpoint. Fills plan.stacks and plan.freeDataExtents.
func buildFreeLists(runs []*[2]int64, freeRecidValues []uint64, plan *csPlan) bool {
	recidVals := freeRecidValues

	slices.SortStableFunc(runs, func(a, b *[2]int64) int { return cmpInt64(b[1], a[1]) }) // largest first
	var arena *[2]int64
	other := runs
	if len(runs) > 0 {
		arena = runs[0]
		other = runs[1:]
	}
	var baseExtents [][2]int64
	for _, r := range other {
		baseExtents = splitMaxExtents(r[0], r[1], baseExtents)
	}
	if arena == nil {
		return len(recidVals) == 0 // no space: fine iff nothing to store
	}

	arenaOff, arenaLen := arena[0], arena[1]
	var extraExtents [][2]int64
	for iter := 0; iter < 8; iter++ {
		dataExtents := append(slices.Clone(baseExtents), extraExtents...)
		stacks := buildStacks(recidVals, dataExtents)
		var used int64
		for _, st := range stacks {
			for _, c := range st.chain {
				used += c.chunkSize
			}
		}
		if used > arenaLen {
			return false // arena too small: retain more
		}
		leftover := arenaLen - used
		var newExtra [][2]int64
		if leftover > 0 {
			newExtra = splitMaxExtents(arenaOff+used, leftover, nil)
		}
		if slices.Equal(newExtra, extraExtents) {
			cursor := arenaOff
			for _, st := range stacks {
				for _, c := range st.chain {
					c.offset = cursor
					cursor += c.chunkSize
				}
			}
			plan.stacks = append(plan.stacks, stacks...)
			plan.freeDataExtents = append(plan.freeDataExtents, baseExtents...)
			plan.freeDataExtents = append(plan.freeDataExtents, extraExtents...)
			return true
		}
		extraExtents = newExtra
	}
	return false // did not converge: retain more
}

// buildStacks builds the free-recid stack plus per-size-class free-data stacks.
func buildStacks(recidVals []uint64, dataExtents [][2]int64) []*csStack {
	var stacks []*csStack
	if len(recidVals) > 0 {
		stacks = append(stacks, &csStack{masterLinkOffset: oFreeRecidStack, chain: packChain(recidVals)})
	}
	byClass := make(map[int64][]uint64)
	var classes []int64
	for _, e := range dataExtents {
		capUnits := e[1]
		if _, ok := byClass[capUnits]; !ok {
			classes = append(classes, capUnits)
		}
		byClass[capUnits] = append(byClass[capUnits], Parity1Set(uint64(e[0])>>3))
	}
	slices.Sort(classes) // GO-CS2: deterministic class order
	for _, u := range classes {
		stacks = append(stacks, &csStack{masterLinkOffset: masterLinkOffset(u), chain: packChain(byClass[u])})
	}
	return stacks
}

// packChain packs raw values into a chunk chain (<=160B chunks, matching the
// live long-stack format).
func packChain(values []uint64) []*csChunk {
	var chain []*csChunk
	i := 0
	for i < len(values) {
		bytes := 0
		start := i
		for i < len(values) {
			vs := packLongSize(values[i])
			if i > start && 8+bytes+vs > longStackPrefSize-8 {
				break
			}
			bytes += vs
			i++
		}
		chunkSize := int64((8 + bytes + 15) &^ 15)
		chain = append(chain, &csChunk{offset: -1, chunkSize: chunkSize, values: slices.Clone(values[start:i])})
	}
	return chain
}

// splitMaxExtents splits a 16-aligned run into {offset, capUnits} extents,
// each <= ivMaxCapacity.
func splitMaxExtents(off, length int64, out [][2]int64) [][2]int64 {
	for length > 0 {
		take := min(length, ivMaxCapacity)
		out = append(out, [2]int64{off, take / 16})
		off += take
		length -= take
	}
	return out
}

// validateTiling confirms the plan tiles every retained data page exactly
// once (no gap/overlap/out-of-range).
func validateTiling(retainedTail int64, indexPageSet map[int64]struct{}, liveRetained [][2]int64, plan *csPlan) bool {
	extents := slices.Clone(liveRetained)
	for _, m := range plan.moves {
		if m.newOffset < pageSize || m.newOffset >= retainedTail {
			return false
		}
		extents = append(extents, [2]int64{m.newOffset, m.capUnits * 16})
	}
	for _, lm := range plan.linkedMoves {
		for k := range lm.oldOffsets {
			if lm.newOffsets[k] < 0 {
				continue // stays put (already in liveRetained)
			}
			if lm.newOffsets[k] < pageSize || lm.newOffsets[k] >= retainedTail {
				return false
			}
			extents = append(extents, [2]int64{lm.newOffsets[k], lm.capUnits[k] * 16})
		}
	}
	for _, st := range plan.stacks {
		for _, c := range st.chain {
			if c.offset < pageSize || c.offset >= retainedTail {
				return false
			}
			extents = append(extents, [2]int64{c.offset, c.chunkSize})
		}
	}
	for _, e := range plan.freeDataExtents {
		extents = append(extents, [2]int64{e[0], e[1] * 16})
	}

	for _, e := range extents {
		off, size := e[0], e[1]
		if off&15 != 0 || size&15 != 0 || size < 16 {
			return false
		}
		if off < pageSize || off+size > retainedTail {
			return false
		}
		if (off%pageSize)+size > pageSize {
			return false
		}
		if _, idx := indexPageSet[off-off%pageSize]; idx {
			return false
		}
	}
	byPage := make(map[int64][][2]int64)
	for _, e := range extents {
		pg := e[0] - e[0]%pageSize
		byPage[pg] = append(byPage[pg], e)
	}
	for page := int64(pageSize); page < retainedTail; page += pageSize {
		if _, idx := indexPageSet[page]; idx {
			continue
		}
		list := byPage[page]
		delete(byPage, page)
		cursor := page
		slices.SortStableFunc(list, func(a, b [2]int64) int { return cmpInt64(a[0], b[0]) })
		for _, e := range list {
			if e[0] != cursor {
				return false // gap or overlap
			}
			cursor = e[0] + e[1]
		}
		if cursor != page+pageSize {
			return false // page not fully covered
		}
	}
	return len(byPage) == 0
}

// validateIndexPagesAfterMove checks the SYNTHETIC final index-page layout
// (old mirror + planned moves; no volume reads): page count preserved, all
// final pages distinct, page-aligned and in [pageSize, retainedTail), and the
// excluded-from-tiling set equals the final pages. GO-CS1: the final chain
// must also be strictly increasing (the Go opener's invariant).
func validateIndexPagesAfterMove(plan *csPlan, oldPages []uint64, retainedTail int64) bool {
	n := len(oldPages)
	finalPages := make([]int64, n)
	for i, p := range oldPages {
		finalPages[i] = int64(p)
	}
	for _, mv := range plan.indexPageMoves {
		if mv.ordinal < 0 || mv.ordinal >= n {
			return false
		}
		if finalPages[mv.ordinal] != mv.oldPage {
			return false // ordinal/oldPage disagree with mirror
		}
		finalPages[mv.ordinal] = mv.newPage
	}
	seen := make(map[int64]struct{}, n)
	for i, p := range finalPages {
		if p%pageSize != 0 || p < pageSize || p >= retainedTail {
			return false
		}
		if _, dup := seen[p]; dup {
			return false
		}
		seen[p] = struct{}{}
		if i > 0 && p <= finalPages[i-1] {
			return false // GO-CS1
		}
	}
	if len(seen) != len(plan.retainedIndexPagesAfterMove) {
		return false
	}
	for p := range plan.retainedIndexPagesAfterMove {
		if _, ok := seen[p]; !ok {
			return false
		}
	}
	return true
}

// validateLinkedChainsAfterMove guards content preservation of planned
// linked-chunk relocations: FLAG_LINKED kept; every final chunk aligned, in
// range, within one page; copied body fits the preserved capacity; and
// sum(dataLen) unchanged.
func validateLinkedChainsAfterMove(plan *csPlan, retainedTail int64) bool {
	for _, lm := range plan.linkedMoves {
		n := len(lm.oldOffsets)
		if n == 0 {
			return false
		}
		if lm.flags&ivFlagLinked == 0 {
			return false // must stay linked
		}
		var sum int64
		for k := 0; k < n; k++ {
			fo := lm.finalOffset(k)
			cu := lm.capUnits[k]
			if cu < 1 || cu > maxCapUnits {
				return false
			}
			capBytes := cu * 16
			if fo&15 != 0 || fo < pageSize || fo+capBytes > retainedTail {
				return false
			}
			if (fo%pageSize)+capBytes > pageSize {
				return false
			}
			if lm.copyBytes[k] < linkedChunkHdr || lm.copyBytes[k] > capBytes {
				return false
			}
			sum += lm.copyBytes[k] - linkedChunkHdr
		}
		if sum != lm.totalDataLen {
			return false // a chunk was dropped/duplicated
		}
	}
	return true
}

// tamperPlanForTest corrupts a freshly-planned plan so the validators must
// reject it (TEST-ONLY; Java tamperPlanForTest).
func (s *StoreDirect) tamperPlanForTest(plan *csPlan) {
	switch s.cstep.tamperMode {
	case 1: // UNIVERSAL: out-of-range free extent (offset 16 is in the header page)
		plan.freeDataExtents = append(plan.freeDataExtents, [2]int64{16, 1})
	case 2: // index target un-aligned
		if len(plan.indexPageMoves) > 0 {
			plan.indexPageMoves[0].newPage += 16
		}
	case 3: // un-move a drop-region linked chunk
		for _, lm := range plan.linkedMoves {
			for k := range lm.oldOffsets {
				if lm.newOffsets[k] >= 0 {
					lm.newOffsets[k] = -1
					return
				}
			}
		}
	case 4: // Go-only: drop FLAG_LINKED (tiling-neutral; only the linked validator sees it)
		if len(plan.linkedMoves) > 0 {
			plan.linkedMoves[0].flags &^= ivFlagLinked
		}
	}
}

// executeCompaction applies a validated plan: copy+flip plain/linked/index
// moves, rebuild free lists, shrink, stamp, sync, truncate.
func (s *StoreDirect) executeCompaction(plan *csPlan) {
	// 0) CRASH BARRIER: the header checksum covers header words only, so
	// relocation alone changes nothing it protects. Invalidate it durably
	// first: any crash until the final stamp reopens as a mismatch.
	s.vol.putI32(oHeadChecksum, ^s.headChecksum())
	s.vol.sync()

	// 1) relocate PLAIN records: write the new copy, THEN re-point the slot
	// (through the OLD index mirror; index pages are published in step 3).
	for _, m := range plan.moves {
		tmp := make([]byte, m.copyBytes)
		s.vol.getData(m.oldOffset, tmp)
		s.vol.putData(m.newOffset, tmp)
		s.indexSet(m.recid, ivCompose(uint32(m.capUnits), uint64(m.newOffset), m.flags))
	}

	// 2) relocate LINKED chunks: copy each moved chunk, then rewrite the
	// WHOLE chain's links to final offsets (head via the index slot, interior
	// via predecessor +4, tail explicitly terminated).
	for _, lm := range plan.linkedMoves {
		n := len(lm.oldOffsets)
		for k := 0; k < n; k++ {
			if lm.newOffsets[k] < 0 {
				continue
			}
			tmp := make([]byte, lm.copyBytes[k])
			s.vol.getData(lm.oldOffsets[k], tmp)
			s.vol.putData(lm.newOffsets[k], tmp)
		}
		s.indexSet(lm.recid, ivCompose(uint32(lm.capUnits[0]), uint64(lm.finalOffset(0)), lm.flags))
		for k := 1; k < n; k++ {
			s.vol.putU64(lm.finalOffset(k-1)+4,
				Parity1Set(uint64(lm.capUnits[k])<<48|uint64(lm.finalOffset(k))))
		}
		s.vol.putU64(lm.finalOffset(n-1)+4, Parity1Set(0))
	}

	// 3) relocate INDEX pages LAST (captures every slot flip above), rewrite
	// the whole chain, then publish the mirror.
	if len(plan.indexPageMoves) > 0 {
		mirror := slices.Clone(*s.indexPages.Load())
		page := make([]byte, pageSize)
		for _, mv := range plan.indexPageMoves {
			s.vol.getData(mv.oldPage, page)
			s.vol.putData(mv.newPage, page)
			mirror[mv.ordinal] = uint64(mv.newPage)
		}
		ptr := int64(zeroPageLink)
		for _, p := range mirror {
			s.vol.putU64(ptr, Parity16Set(p))
			ptr = int64(p) + 8
		}
		s.vol.putU64(ptr, Parity16Set(0)) // terminator
		s.indexPages.Store(&mirror)       // publish AFTER the on-volume chain flip
	}

	// Test hook: simulate a crash in the dangerous window (relocations
	// applied, free lists not yet rebuilt).
	if hook := s.cstep.crashHook; hook != nil {
		hook()
	}

	// 4) rebuild free lists: reset every master, then write the planned chains.
	s.vol.putU64(oFreeRecidStack, Parity4Set(0))
	for u := int64(1); u <= maxCapUnits; u++ {
		s.vol.putU64(masterLinkOffset(u), Parity4Set(0))
	}
	for _, st := range plan.stacks {
		s.writeStack(st)
	}
	// 5) shrink: all free space is published, so no bump frontier.
	s.setDataTail(0)
	s.setFileTail(plan.retainedTail)
	s.recomputeFreeDataBytes()

	// 6) durability point (same ordering as Compact): data, stamp, truncate.
	s.stampHeaderDurable()
	s.vol.truncate(plan.retainedTail)

	s.cstep.linkedMovesApplied += int64(len(plan.linkedMoves))
	s.cstep.indexPageMovesApplied += int64(len(plan.indexPageMoves))
}

// writeStack writes one rebuilt long-stack chain and sets its master link.
func (s *StoreDirect) writeStack(st *csStack) {
	var prev, topOffset, topPos int64
	for _, c := range st.chain {
		s.vol.clear(c.offset, c.offset+c.chunkSize) // zero tails delimit the value area
		s.vol.putU64(c.offset, Parity4Set(uint64(c.chunkSize)<<48|uint64(prev)))
		p := c.offset + 8
		for _, v := range c.values {
			p += s.putPackedLong(p, v)
		}
		prev = c.offset
		topOffset = c.offset
		topPos = p - c.offset
	}
	if topOffset != 0 {
		s.vol.putU64(st.masterLinkOffset, Parity4Set(uint64(topPos)<<48|uint64(topOffset)))
	}
}
