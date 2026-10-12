package store

import (
	"encoding/binary"
	"iter"
	"slices"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// StoreAppendOnly — append-only StoreDelta (Java StoreAppendOnly, M6): ONE
// writer at a time, lock-free readers. Built for single-writer structures
// (BufferTreeMap) where StoreDirect's segment locks are pure overhead.
//
// The data area is append-only: an extent, once written, is never mutated
// below its published length and its bytes are never overwritten. The only
// mutable word per record is its index entry, one packed uint64:
//
//	bit 63..21  byte offset of the extent (43 bits)
//	bit 20..0   used content length (21 bits; ivMaxCapacity < 2 MiB)
//	offset == 0 is a state sentinel: 0 void, 1 preallocated, 2 null, 3 deleted.
//
// Extent layout: [int32 capBytes][content .. capacity); 16-aligned, never
// crosses a 1 MiB slice. Non-durable (heap only) — there is no file format,
// so G2 byte compatibility does not apply.
//
// Go memory model mapping of the Java release/acquire design (G3):
//   - Writers plain-write extent bytes, then atomically store the index entry;
//     readers atomically load the entry, then read [off+4, off+4+used). The
//     atomic store/load pair is the happens-before edge; published bytes are
//     never written again, Append writes only beyond the published length.
//   - Compaction copies live extents to the tail, republishes the entry, and
//     RETIRES victim slices by nil-ing their slot. A reader that resolved the
//     page before retirement keeps a reference (the GC is the reclamation
//     epoch, as in Java); one that resolves after sees nil and retries
//     against the re-pointed entry.
//   - Deviation from Java: overlapping writers are serialized by a writer
//     mutex instead of an -ea-only assertion. An unsynchronized overlap would
//     be a data race on the allocator state, which G3 forbids in every build;
//     the uncontended mutex costs nanoseconds. Readers never take it.
type StoreAppendOnly struct {
	indexPages atomic.Pointer[[]*aoIndexPage]
	vol        aoVolume
	maxRecid   atomic.Uint64
	closed     atomic.Bool
	lease      *LeaseTable

	// ---- writer-owned state, guarded by wmu ----
	wmu             sync.Mutex
	freeRecids      []Recid
	dataTail        int64
	minGarbageBytes int64
	totalLive       int64   // sum of live extent capacities
	sliceLive       []int64 // live extent bytes per slice
	retired         []bool
	retiredSlices   int64
	compacting      bool
	copyBuf         []byte
}

const (
	aoIndexPageShift = 17
	aoIndexPageSize  = 1 << aoIndexPageShift
	aoIndexPageMask  = aoIndexPageSize - 1

	aoUsedBits  = 21
	aoUsedMask  = uint64(1)<<aoUsedBits - 1
	aoMaxOffset = int64(1)<<(64-aoUsedBits) - 1

	aoIVVoid     uint64 = 0
	aoIVPrealloc uint64 = 1
	aoIVNull     uint64 = 2
	aoIVDeleted  uint64 = 3

	// aoDataStart: offset 0..15 is reserved so live offsets are never 0.
	aoDataStart int64 = 16

	// DefaultMinGarbageBytes is the auto-compaction floor: no sweep until at
	// least this much garbage exists (Java DEFAULT_MIN_GARBAGE_BYTES).
	DefaultMinGarbageBytes int64 = 64 << 20
)

type aoIndexPage [aoIndexPageSize]atomic.Uint64

// aoVolume is a growable table of 1 MiB pages whose slots can be retired.
// Slots are stable once allocated; growth copies only slot pointers.
type aoVolume struct {
	slots atomic.Pointer[[]*aoSlot]
}

type aoSlot struct {
	page atomic.Pointer[[sliceSize]byte]
}

// NewStoreAppendOnly creates an append-only store with the default
// auto-compaction floor.
func NewStoreAppendOnly() *StoreAppendOnly {
	return NewStoreAppendOnlyMinGarbage(DefaultMinGarbageBytes)
}

// NewStoreAppendOnlyMinGarbage sets the auto-compaction floor: a sweep
// triggers only once garbage exceeds max(minGarbageBytes, live bytes). Small
// values make compaction eager (tests); math.MaxInt64 disables
// auto-compaction (Compact still works).
func NewStoreAppendOnlyMinGarbage(minGarbageBytes int64) *StoreAppendOnly {
	s := &StoreAppendOnly{
		dataTail:        aoDataStart,
		minGarbageBytes: minGarbageBytes,
		lease:           NewLeaseTable(),
	}
	s.indexPages.Store(&[]*aoIndexPage{})
	s.vol.slots.Store(&[]*aoSlot{})
	return s
}

// LeaseTable is this store's lease registry.
func (s *StoreAppendOnly) LeaseTable() *LeaseTable { return s.lease }

// ---------- packing ----------

func aoPack(offset int64, used int) uint64 {
	return uint64(offset)<<aoUsedBits | uint64(used)
}
func aoOffset(iv uint64) int64 { return int64(iv >> aoUsedBits) }
func aoUsed(iv uint64) int     { return int(iv & aoUsedMask) }
func aoIsLive(iv uint64) bool  { return aoOffset(iv) != 0 }

// ---------- volume ----------

// ensureAvailable grows the page table to cover [0, size). Writer only.
func (v *aoVolume) ensureAvailable(size int64) {
	need := int((size + sliceMask) >> sliceShift)
	cur := *v.slots.Load()
	if need <= len(cur) {
		return
	}
	grown := make([]*aoSlot, need)
	copy(grown, cur)
	for i := len(cur); i < need; i++ {
		sl := &aoSlot{}
		sl.page.Store(new([sliceSize]byte))
		grown[i] = sl
	}
	v.slots.Store(&grown)
}

// writerView is a view for the writer, which never touches retired slices.
func (v *aoVolume) writerView(off, size int64) []byte {
	p := (*v.slots.Load())[off>>sliceShift].page.Load()
	o := off & sliceMask
	return p[o : o+size]
}

// readerView returns the immutable bytes, or nil when the slice retired (or
// the store closed) — the caller re-reads the entry and retries.
func (v *aoVolume) readerView(off, size int64) []byte {
	slots := *v.slots.Load()
	s := off >> sliceShift
	if s >= int64(len(slots)) {
		return nil
	}
	p := slots[s].page.Load()
	if p == nil {
		return nil
	}
	o := off & sliceMask
	return p[o : o+size]
}

func (v *aoVolume) retire(s int) { (*v.slots.Load())[s].page.Store(nil) }

func (v *aoVolume) close() { v.slots.Store(&[]*aoSlot{}) }

func (v *aoVolume) capAt(off int64) int64 {
	return int64(binary.BigEndian.Uint32(v.writerView(off, 4)))
}

// ---------- helpers ----------

func (s *StoreAppendOnly) checkClosed() {
	if s.closed.Load() {
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
}

// lockWriter takes the writer mutex and re-checks closed under it, so no
// mutation runs after Close returned.
func (s *StoreAppendOnly) lockWriter() {
	s.wmu.Lock()
	if s.closed.Load() {
		s.wmu.Unlock()
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
}

// ivGet is the acquire-load; void when never allocated / out of range.
func (s *StoreAppendOnly) ivGet(recid Recid) uint64 {
	pages := *s.indexPages.Load()
	p := recid >> aoIndexPageShift
	if recid < 1 || p >= uint64(len(pages)) {
		return aoIVVoid
	}
	return pages[p][recid&aoIndexPageMask].Load()
}

// ivGetChecked enforces the record-state contract: GetVoid on void/deleted.
func (s *StoreAppendOnly) ivGetChecked(recid Recid) uint64 {
	iv := s.ivGet(recid)
	if iv == aoIVVoid || iv == aoIVDeleted {
		s.checkClosed() // Close empties the index: report Closed, not GetVoid
		panic(mapdb5.NewGetVoid(recid))
	}
	return iv
}

// ivSet is the release-store publishing every prior plain byte write.
func (s *StoreAppendOnly) ivSet(recid Recid, iv uint64) {
	(*s.indexPages.Load())[recid>>aoIndexPageShift][recid&aoIndexPageMask].Store(iv)
}

func (s *StoreAppendOnly) ensureIndexPage(recid Recid) {
	p := int(recid >> aoIndexPageShift)
	pages := *s.indexPages.Load()
	if p < len(pages) {
		return
	}
	grown := make([]*aoIndexPage, p+1)
	copy(grown, pages)
	for i := len(pages); i <= p; i++ {
		grown[i] = new(aoIndexPage)
	}
	s.indexPages.Store(&grown)
}

func (s *StoreAppendOnly) allocRecid() Recid {
	var recid Recid
	if n := len(s.freeRecids); n > 0 {
		recid = s.freeRecids[n-1]
		s.freeRecids = s.freeRecids[:n-1]
	} else {
		recid = s.maxRecid.Load() + 1
	}
	s.ensureIndexPage(recid)
	if recid > s.maxRecid.Load() {
		s.maxRecid.Store(recid)
	}
	return recid
}

// allocExtent allocates a fresh 16-aligned extent (never reused, never
// crosses a slice) and writes its immutable capacity header. May run a
// compaction sweep first — callers holding a record's location across this
// call must re-read the index entry afterwards.
func (s *StoreAppendOnly) allocExtent(capBytes int64) int64 {
	if !s.compacting {
		s.maybeCompact()
	}
	if rem := sliceSize - (s.dataTail & sliceMask); rem < capBytes {
		s.dataTail += rem // abandon the remainder (append-only: no free lists)
	}
	off := s.dataTail
	if off > aoMaxOffset { // aoPack would silently truncate — refuse first
		panic(mapdb5.New(mapdb5.KindStoreFull, "append-only data area exceeds packed-offset limit"))
	}
	s.dataTail += capBytes
	s.vol.ensureAvailable(s.dataTail)
	sl := int(off >> sliceShift)
	if sl >= len(s.sliceLive) {
		s.sliceLive = append(s.sliceLive, make([]int64, max(sl+1, 2*len(s.sliceLive))-len(s.sliceLive))...)
	}
	s.sliceLive[sl] += capBytes
	s.totalLive += capBytes
	binary.BigEndian.PutUint32(s.vol.writerView(off, 4), uint32(capBytes))
	return off
}

// abandon un-accounts a no-longer-current extent; its bytes stay intact for
// in-flight readers.
func (s *StoreAppendOnly) abandon(iv uint64) {
	if !aoIsLive(iv) {
		return
	}
	off := aoOffset(iv)
	c := s.vol.capAt(off)
	s.sliceLive[off>>sliceShift] -= c
	s.totalLive -= c
}

func (s *StoreAppendOnly) maybeCompact() {
	// full slices only (tail excluded): the partial tail is not collectable
	fullSliceBytes := ((s.dataTail >> sliceShift) - s.retiredSlices) << sliceShift
	garbage := fullSliceBytes - s.totalLive // may go negative when live sits in the tail
	if garbage > s.minGarbageBytes && garbage > s.totalLive {
		s.compactSweep()
	}
}

func (s *StoreAppendOnly) isRetired(sl int) bool {
	return sl < len(s.retired) && s.retired[sl]
}

// compactSweep evacuates majority-dead slices: one index scan plus copies
// of their live extents, then retirement. Writer context.
func (s *StoreAppendOnly) compactSweep() {
	s.compacting = true
	defer func() { s.compacting = false }()

	tailSlice := int(s.dataTail >> sliceShift)
	nSlices := int((s.dataTail + sliceMask) >> sliceShift)
	victim := make([]bool, nSlices)
	anyVictim := false
	for sl := 0; sl < nSlices; sl++ {
		if sl == tailSlice || s.isRetired(sl) {
			continue
		}
		var live int64
		if sl < len(s.sliceLive) {
			live = s.sliceLive[sl]
		}
		if live <= sliceSize/2 {
			victim[sl] = true
			anyVictim = true
		}
	}
	if !anyVictim {
		return
	}
	maxRecid := s.maxRecid.Load()
	for recid := Recid(1); recid <= maxRecid; recid++ {
		iv := s.ivGet(recid)
		if !aoIsLive(iv) {
			continue
		}
		off := aoOffset(iv)
		sl := int(off >> sliceShift)
		if sl >= len(victim) || !victim[sl] {
			continue
		}
		used := aoUsed(iv)
		c := s.vol.capAt(off)
		if len(s.copyBuf) < used {
			s.copyBuf = make([]byte, max(used, 2*len(s.copyBuf)))
		}
		copy(s.copyBuf, s.vol.writerView(off+4, int64(used)))
		newOff := s.allocExtent(c) // capacity preserved: append headroom survives
		copy(s.vol.writerView(newOff+4, int64(used)), s.copyBuf[:used])
		s.abandon(iv)
		s.ivSet(recid, aoPack(newOff, used))
	}
	for sl := 0; sl < nSlices; sl++ {
		if !victim[sl] {
			continue
		}
		if sl < len(s.sliceLive) && s.sliceLive[sl] != 0 {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "append-only victim slice still live"))
		}
		s.vol.retire(sl)
		if sl >= len(s.retired) {
			s.retired = append(s.retired, make([]bool, sl+1-len(s.retired))...)
		}
		s.retired[sl] = true
		s.retiredSlices++
	}
}

// writeExtent: fresh extent with content + headroom capacity; NOT published.
func (s *StoreAppendOnly) writeExtent(content []byte, capBytes int64) int64 {
	off := s.allocExtent(capBytes)
	copy(s.vol.writerView(off+4, int64(len(content))), content)
	return off
}

// aoCapacity: checked capacity incl. the 4-byte header (RecordTooLarge on
// overflow), rounded up to 16.
func aoCapacity(contentLen, headroom int64) int64 {
	return capBytesFor(plainNeed(contentLen, headroom))
}

// serializeTo serializes outside the writer lock; nil record → nil writer.
func aoSerialize(record any, sr ser.AnySerializer) *bin.Writer {
	if record == nil {
		return nil
	}
	w := bin.GetWriter()
	sr.SerializeAny(w, record)
	return w
}

func aoRelease(w *bin.Writer) {
	if w != nil {
		bin.PutWriter(w)
	}
}

// ---------- Store ----------

func (s *StoreAppendOnly) Preallocate() Recid {
	s.checkClosed()
	s.lockWriter()
	defer s.wmu.Unlock()
	recid := s.allocRecid()
	s.ivSet(recid, aoIVPrealloc)
	return recid
}

func (s *StoreAppendOnly) PreallocateN(into []Recid) {
	for i := range into {
		into[i] = s.Preallocate()
	}
}

func (s *StoreAppendOnly) Put(record any, sr ser.AnySerializer) Recid {
	s.checkClosed()
	validateRecord(sr, record)
	w := aoSerialize(record, sr)
	defer aoRelease(w)
	var capBytes int64
	if w != nil {
		capBytes = aoCapacity(int64(w.Pos), 0)
	}
	s.lockWriter()
	defer s.wmu.Unlock()
	recid := s.allocRecid()
	if w == nil {
		s.ivSet(recid, aoIVNull)
	} else {
		s.ivSet(recid, aoPack(s.writeExtent(w.Buf[:w.Pos], capBytes), w.Pos))
	}
	return recid
}

// resolve is the lock-free read: the bytes of a live record, or ok=false
// for null/preallocated content. Retries only when it lost a race with the
// retirement of the exact slice it targets.
func (s *StoreAppendOnly) resolve(recid Recid) (b []byte, ok bool) {
	for {
		s.checkClosed()
		iv := s.ivGetChecked(recid)
		if !aoIsLive(iv) {
			return nil, false
		}
		used := aoUsed(iv)
		if v := s.vol.readerView(aoOffset(iv)+4, int64(used)); v != nil {
			return v, true
		}
	}
}

func (s *StoreAppendOnly) Get(recid Recid, sr ser.AnySerializer) any {
	b, ok := s.resolve(recid)
	if !ok {
		return nil
	}
	v := sr.DeserializeAny(bin.NewReader(b), len(b))
	if v != nil {
		validateOperand(sr, v)
	}
	return v
}

// Read runs the action exactly once over immutable published bytes — no
// lock, no validation, never torn.
func (s *StoreAppendOnly) Read(recid Recid, action RecordRead) int64 {
	b, ok := s.resolve(recid)
	if !ok {
		return action.OnNull()
	}
	return action.OnBytes(bin.NewReader(b), len(b))
}

func (s *StoreAppendOnly) Update(recid Recid, record any, sr ser.AnySerializer) {
	s.UpdateWithHeadroom(recid, record, sr, 0)
}

func (s *StoreAppendOnly) UpdateWithHeadroom(recid Recid, record any, sr ser.AnySerializer, headroom int) {
	s.checkClosed()
	if headroom < 0 {
		panic(mapdb5.NewMisuse("negative headroom"))
	}
	validateRecord(sr, record)
	w := aoSerialize(record, sr)
	defer aoRelease(w)
	s.lockWriter()
	defer s.wmu.Unlock()
	s.updateImpl(recid, w, int64(headroom))
}

// updateImpl always writes a fresh extent — published bytes are never
// rewritten. Writer only.
func (s *StoreAppendOnly) updateImpl(recid Recid, w *bin.Writer, headroom int64) {
	s.ivGetChecked(recid)
	if w == nil {
		s.abandon(s.ivGet(recid))
		s.ivSet(recid, aoIVNull)
		return
	}
	capBytes := aoCapacity(int64(w.Pos), headroom)
	off := s.writeExtent(w.Buf[:w.Pos], capBytes) // may compact — can MOVE this record
	s.abandon(s.ivGet(recid))                     // so re-read the entry only now
	s.ivSet(recid, aoPack(off, w.Pos))
}

func (s *StoreAppendOnly) CompareAndSwap(recid Recid, expectedOld, newRecord any, sr ser.AnySerializer) bool {
	s.checkClosed()
	validateRecord(sr, expectedOld)
	validateRecord(sr, newRecord)
	w := aoSerialize(newRecord, sr)
	defer aoRelease(w)
	s.lockWriter()
	defer s.wmu.Unlock()
	iv := s.ivGetChecked(recid)
	var current any
	if aoIsLive(iv) {
		used := aoUsed(iv)
		current = sr.DeserializeAny(bin.NewReader(s.vol.writerView(aoOffset(iv)+4, int64(used))), used)
		if current != nil {
			validateOperand(sr, current)
		}
	}
	var eq bool
	switch {
	case current == nil && expectedOld == nil:
		eq = true
	case current != nil && expectedOld != nil:
		eq = sr.EqualsAny(current, expectedOld)
	}
	if !eq {
		return false
	}
	s.updateImpl(recid, w, 0)
	return true
}

func (s *StoreAppendOnly) Delete(recid Recid, _ ser.AnySerializer) {
	s.checkClosed()
	s.lockWriter()
	defer s.wmu.Unlock()
	s.abandon(s.ivGetChecked(recid))
	s.ivSet(recid, aoIVDeleted)
	s.freeRecids = append(s.freeRecids, recid)
}

// ---------- StoreDelta ----------

func (s *StoreAppendOnly) Append(recid Recid, data []byte) int64 {
	s.checkClosed()
	s.lockWriter()
	defer s.wmu.Unlock()
	iv := s.ivGetChecked(recid)
	if !aoIsLive(iv) {
		// first append establishes the record: capacity == exactly what is needed
		capBytes := aoCapacity(int64(len(data)), 0)
		s.ivSet(recid, aoPack(s.writeExtent(data, capBytes), len(data)))
		return int64(len(data))
	}
	off := aoOffset(iv)
	used := int64(aoUsed(iv))
	capBytes := s.vol.capAt(off)
	if 4+used+int64(len(data)) > capBytes {
		return AppendRefused
	}
	// bytes beyond `used` were never published — plain writes, then publish
	copy(s.vol.writerView(off+4+used, int64(len(data))), data)
	s.ivSet(recid, aoPack(off, int(used)+len(data)))
	return used + int64(len(data))
}

func (s *StoreAppendOnly) CapacityRemaining(recid Recid) int64 {
	for {
		s.checkClosed()
		iv := s.ivGetChecked(recid)
		if !aoIsLive(iv) {
			return 0
		}
		if h := s.vol.readerView(aoOffset(iv), 4); h != nil {
			return int64(binary.BigEndian.Uint32(h)) - 4 - int64(aoUsed(iv))
		}
	}
}

// ---------- lifecycle ----------

// Commit is a no-op: the store is non-durable.
func (s *StoreAppendOnly) Commit() { s.checkClosed() }

// Compact runs a copying sweep over majority-dead slices.
func (s *StoreAppendOnly) Compact() {
	s.checkClosed()
	s.lockWriter()
	defer s.wmu.Unlock()
	s.compactSweep()
}

// Close is idempotent. It waits for an in-flight writer; concurrent readers
// fail KindClosed on their next access.
func (s *StoreAppendOnly) Close() error {
	s.closed.Store(true)
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.vol.close()
	s.indexPages.Store(&[]*aoIndexPage{})
	s.freeRecids = nil
	return nil
}

func (s *StoreAppendOnly) IsClosed() bool { return s.closed.Load() }

// AllocatedBytes is the data-area high-water mark — MONOTONIC address-space
// consumption (compaction retires slices but never revisits offsets).
func (s *StoreAppendOnly) AllocatedBytes() int64 {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return s.dataTail
}

// OccupiedBytes is the actual data-area footprint: bytes in non-retired
// slices (live extents plus not-yet-collected garbage).
func (s *StoreAppendOnly) OccupiedBytes() int64 {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	n := (s.dataTail + sliceMask) >> sliceShift
	return (n - s.retiredSlices) << sliceShift
}

// Verify checks live extents are sane, disjoint, in non-retired slices, and
// match the compaction accounting exactly. Holds the writer lock.
func (s *StoreAppendOnly) Verify() {
	s.checkClosed()
	s.lockWriter()
	defer s.wmu.Unlock()
	fail := func(msg string) { panic(mapdb5.New(mapdb5.KindVerifyFailed, msg)) }
	type ext struct{ off, cap int64 }
	var extents []ext
	liveBySlice := make([]int64, (s.dataTail+sliceMask)>>sliceShift)
	var liveSum int64
	maxRecid := s.maxRecid.Load()
	for recid := Recid(1); recid <= maxRecid; recid++ {
		iv := s.ivGet(recid)
		if !aoIsLive(iv) {
			continue
		}
		off := aoOffset(iv)
		sl := int(off >> sliceShift)
		if off&15 != 0 {
			fail("offset not aligned")
		}
		if off+4 > s.dataTail {
			fail("record beyond dataTail")
		}
		if s.isRetired(sl) {
			fail("live record in retired slice")
		}
		c := s.vol.capAt(off)
		if off+c > s.dataTail {
			fail("record beyond dataTail")
		}
		if (off&sliceMask)+c > sliceSize {
			fail("record crosses slice")
		}
		if 4+int64(aoUsed(iv)) > c {
			fail("used beyond capacity")
		}
		extents = append(extents, ext{off, c})
		liveBySlice[sl] += c
		liveSum += c
	}
	slices.SortFunc(extents, func(a, b ext) int { return int(a.off - b.off) })
	prevEnd := aoDataStart
	for _, e := range extents {
		if e.off < prevEnd {
			fail("overlapping extents")
		}
		prevEnd = e.off + e.cap
	}
	if liveSum != s.totalLive {
		fail("totalLive accounting off")
	}
	for sl := range liveBySlice {
		var tracked int64
		if sl < len(s.sliceLive) {
			tracked = s.sliceLive[sl]
		}
		if tracked != liveBySlice[sl] {
			fail("sliceLive accounting off")
		}
	}
	for _, r := range s.freeRecids {
		if iv := s.ivGet(r); iv != aoIVDeleted {
			fail("free recid is not deleted")
		}
	}
}

// AllRecids yields live and null recids ascending, excluding preallocated
// ones. Weak snapshot under a concurrent writer.
func (s *StoreAppendOnly) AllRecids() iter.Seq[Recid] {
	s.checkClosed()
	var out []Recid
	maxRecid := s.maxRecid.Load()
	for recid := Recid(1); recid <= maxRecid; recid++ {
		iv := s.ivGet(recid)
		if iv == aoIVVoid || iv == aoIVDeleted || iv == aoIVPrealloc {
			continue
		}
		out = append(out, recid)
	}
	return func(yield func(Recid) bool) {
		for _, r := range out {
			if !yield(r) {
				return
			}
		}
	}
}

// IsThreadSafe: concurrent readers alongside writers (serialized by wmu).
func (s *StoreAppendOnly) IsThreadSafe() bool           { return true }
func (s *StoreAppendOnly) IsReadOnly() bool             { return false }
func (s *StoreAppendOnly) IsTx() bool                   { return false }
func (s *StoreAppendOnly) StructuralGeneration() uint64 { return 0 }

// CurrentSize returns 0 ("unsupported"), as Java's StoreAppendOnly inherits
// the Store default.
func (s *StoreAppendOnly) CurrentSize() int64 { return 0 }

var _ StoreDelta = (*StoreAppendOnly)(nil)
var _ StoreLease = (*StoreAppendOnly)(nil)
