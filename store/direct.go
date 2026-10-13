package store

// StoreDirect — durable direct store: recid index, free lists, allocator
// metadata and record data all live on the volume (Java StoreDirect, spec 02
// §5). On-volume format v1, magic "MDBS.SD1".
//
// Reads take the segment read lock (accepted deviation D9.5 — Java's
// optimistic seqlock has no sound Go equivalent). All methods panic
// *mapdb5.DBError except the constructor and Close, which return error.

import (
	"iter"
	"slices"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// ---------- on-volume geometry ----------

const (
	pageSize = sliceSize
	// "MDBS.SD1" big-endian.
	magic = 0x4D44_4253_2E53_4431

	oFeatures       = 8
	oHeadChecksum   = 16
	oDataTail       = 24
	oMaxRecid       = 32
	oFileTail       = 40
	oFreeRecidStack = 64
	oFreeDataStacks = 72
	maxCapUnits     = int64(ivCapMax)
	headEnd         = oFreeDataStacks + 8*maxCapUnits // 524336
	zeroPageLink    = headEnd
	zeroSlotsStart  = headEnd + 16
	recidsPerZero   = uint64((pageSize - zeroSlotsStart) / 8) // 65528
	recidsPerPage   = uint64((pageSize - 16) / 8)             // 131070

	headChecksumSeed = 0x5D1B_A5E1

	longStackPrefSize = 160
	longStackMaxSize  = 256

	linkedChunkHdr = 12
	maxChunkData   = ivMaxCapacity - linkedChunkHdr
	maxVolumeSize  = int64(1) << 44
)

// Record states for the WAL merge logic (consumed by StoreWAL).
const (
	stateVoid = 0
	stateNull = 1
	stateLive = 2
)

const segmentCount = 64

// StoreDirect is the durable byte-resident store.
type StoreDirect struct {
	vol        *volume
	indexPages atomic.Pointer[[]uint64] // offsets of non-zero index pages, increasing
	threadSafe bool
	structural sync.Mutex
	commitLock sync.RWMutex
	segs       [segmentCount]sync.RWMutex
	closed     atomic.Bool
	poisoned   atomic.Bool
	freeBytes  atomic.Int64
	lease      *LeaseTable
}

// NewStoreDirectHeap creates an anonymous heap-backed direct store.
func NewStoreDirectHeap(threadSafe bool) *StoreDirect {
	s := newEmptyDirect(newHeapVolume(), threadSafe)
	s.initCreate()
	return s
}

// NewStoreDirectFile opens or creates a file-backed direct store. I/O and
// corruption failures are returned as *DBError, not panicked.
func NewStoreDirectFile(path string, threadSafe bool) (s *StoreDirect, err error) {
	var vol *volume
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				err = de
			} else {
				panic(r)
			}
		}
		if err != nil && vol != nil {
			// Never leak the descriptor on a rejected open.
			_ = vol.closeVol(-1)
		}
	}()
	vol, err = openFileVolume(path)
	if err != nil {
		return nil, err
	}
	s = newEmptyDirect(vol, threadSafe)
	length := vol.length()
	switch {
	case length == 0:
		s.initCreate()
	case length < pageSize:
		panic(mapdb5.NewCorruption("store file smaller than the header page"))
	default:
		vol.ensureAvailable(pageSize)
		s.initOpen()
	}
	return s, nil
}

func newEmptyDirect(vol *volume, threadSafe bool) *StoreDirect {
	s := &StoreDirect{vol: vol, threadSafe: threadSafe, lease: NewLeaseTable()}
	s.indexPages.Store(&[]uint64{})
	return s
}

// LeaseTable is this store's lease registry.
func (s *StoreDirect) LeaseTable() *LeaseTable { return s.lease }

// ---------- header init / open ----------

func (s *StoreDirect) initCreate() {
	s.vol.ensureAvailable(pageSize)
	s.vol.putU64(0, magic)
	s.vol.putI32(oFeatures, 0)
	s.vol.putI32(oFeatures+4, 0)
	s.vol.putI32(oHeadChecksum+4, 0)
	s.setDataTail(0)
	s.setMaxRecid(0)
	s.setFileTail(pageSize)
	s.vol.putU64(oFreeRecidStack, Parity4Set(0))
	for u := int64(1); u <= maxCapUnits; u++ {
		s.vol.putU64(masterLinkOffset(u), Parity4Set(0))
	}
	s.vol.putU64(zeroPageLink, Parity16Set(0))
	s.vol.putI32(oHeadChecksum, s.headChecksum())
	s.vol.sync()
}

func (s *StoreDirect) initOpen() {
	if s.vol.length() < pageSize {
		panic(mapdb5.NewCorruption("store file smaller than the header page"))
	}
	if s.vol.getU64(0) != magic {
		panic(mapdb5.NewCorruption("not a MapDB StoreDirect file (bad magic)"))
	}
	if s.vol.getI32(oFeatures) != 0 {
		panic(mapdb5.NewCorruption("store uses unsupported feature bits"))
	}
	if s.vol.getI32(oHeadChecksum) != s.headChecksum() {
		panic(mapdb5.NewCorruption("header checksum mismatch: store was not closed cleanly or is corrupted"))
	}
	ft := s.fileTail()
	if ft < pageSize || ft%pageSize != 0 {
		panic(mapdb5.NewCorruption("bad fileTail"))
	}
	if s.vol.length() < ft {
		panic(mapdb5.NewCorruption("store file truncated"))
	}
	s.vol.ensureAvailable(ft)
	// dataTail must satisfy the same geometry verifyLocked enforces BEFORE the
	// allocator trusts it (D4).
	if dt := s.dataTail(); !dataTailGeometryOK(dt, ft) {
		panic(mapdb5.NewCorruption("bad dataTail"))
	}
	mr := s.maxRecid() // validates parity
	s.loadIndexPages(ft)
	// maxRecid must have an addressable index slot in the loaded mirror (D4).
	if !s.maxRecidGeometryOK(mr) {
		panic(mapdb5.NewCorruption("bad maxRecid"))
	}
	s.recomputeFreeDataBytes()
}

func (s *StoreDirect) maxRecidGeometryOK(maxRecid uint64) bool {
	return maxRecid == 0 || s.recidToOffset(maxRecid) >= 0
}

func (s *StoreDirect) loadIndexPages(fileTail int64) {
	var pages []uint64
	ptr := int64(zeroPageLink)
	for {
		page := int64(Parity16Get(s.vol.getU64(ptr)))
		if page == 0 {
			break
		}
		if page%pageSize != 0 || page >= fileTail {
			panic(mapdb5.NewCorruption("bad index page pointer"))
		}
		if len(pages) > 0 && page <= int64(pages[len(pages)-1]) {
			panic(mapdb5.NewCorruption("index page chain is not increasing"))
		}
		pages = append(pages, uint64(page))
		if len(pages) > 1<<24 {
			panic(mapdb5.NewCorruption("index page chain loop"))
		}
		ptr = page + 8
	}
	s.indexPages.Store(&pages)
}

// headChecksum mixes every header word the allocator depends on; stamped by
// commit/close.
func (s *StoreDirect) headChecksum() int32 {
	c := int32(headChecksumSeed)
	for o := int64(oDataTail); o < zeroSlotsStart; o += 8 {
		v := s.vol.getU64(o)
		c = c*31 + int32(v^(v>>32))
	}
	return c
}

// ---------- header accessors ----------

func (s *StoreDirect) dataTail() int64 {
	return int64(Parity4Get(s.vol.getU64(oDataTail)))
}
func (s *StoreDirect) setDataTail(v int64) {
	s.vol.putU64(oDataTail, Parity4Set(uint64(v)))
}
func (s *StoreDirect) maxRecid() uint64 {
	return Parity4Get(s.vol.getU64(oMaxRecid)) >> 4
}
func (s *StoreDirect) setMaxRecid(v uint64) {
	s.vol.putU64(oMaxRecid, Parity4Set(v<<4))
}
func (s *StoreDirect) fileTail() int64 {
	return int64(Parity16Get(s.vol.getU64(oFileTail)))
}
func (s *StoreDirect) setFileTail(v int64) {
	s.vol.putU64(oFileTail, Parity16Set(uint64(v)))
}

// ---------- recid index ----------

// recidToOffset returns the volume offset of the recid's index slot, or -1
// when its index page does not exist.
func (s *StoreDirect) recidToOffset(recid uint64) int64 {
	r0 := recid - 1
	if r0 < recidsPerZero {
		return zeroSlotsStart + int64(r0)*8
	}
	r0 -= recidsPerZero
	page := r0 / recidsPerPage
	pages := *s.indexPages.Load()
	if page >= uint64(len(pages)) {
		return -1
	}
	return int64(pages[page]) + 16 + int64(r0%recidsPerPage)*8
}

// rawIndexGet returns the raw (parity1-encoded) index slot; 0 when never
// allocated / out of range.
func (s *StoreDirect) rawIndexGet(recid uint64) uint64 {
	if recid < 1 {
		return 0
	}
	off := s.recidToOffset(recid)
	if off < 0 {
		return 0
	}
	return s.vol.getU64(off)
}

func (s *StoreDirect) indexGetChecked(recid uint64) uint64 {
	v := s.rawIndexGet(recid)
	if v == 0 {
		panic(mapdb5.NewGetVoid(recid))
	}
	if !ivParityOK(v) {
		panic(mapdb5.NewCorruption("index slot parity broken"))
	}
	if ivCapUnits(v) == ivCapDeleted {
		panic(mapdb5.NewGetVoid(recid))
	}
	return v
}

func (s *StoreDirect) indexSet(recid uint64, ivval uint64) {
	off := s.recidToOffset(recid)
	if off < 0 {
		panic(mapdb5.New(mapdb5.KindMisuse, "index slot not allocated for recid"))
	}
	s.vol.putU64(off, Parity1Set(ivval))
}

// ensureIndexCapacityLocked allocates index pages until recid has a slot.
// structural lock held.
func (s *StoreDirect) ensureIndexCapacityLocked(recid uint64) {
	for s.recidToOffset(recid) < 0 {
		s.allocateNewIndexPageLocked()
	}
}

// checkRecoveryIndexLimit bounds the dense index cost of a replayed recid
// before pages are allocated (WAL cleaner may leave only a high recid).
func checkRecoveryIndexLimit(recid, maxBytes uint64) {
	var extra uint64
	if recid > recidsPerZero {
		extra = recid - recidsPerZero
	}
	pages := 1 + (extra+recidsPerPage-1)/recidsPerPage
	if pages > maxBytes/uint64(pageSize) {
		panic(mapdb5.New(mapdb5.KindStoreFull, "recid exceeds recovery index limit"))
	}
}

func (s *StoreDirect) allocateNewIndexPageLocked() {
	page := s.allocateNewPageLocked()
	s.vol.clear(page, page+pageSize)
	s.vol.putU64(page+8, Parity16Set(0))
	pages := *s.indexPages.Load()
	var ptr int64
	if len(pages) == 0 {
		ptr = zeroPageLink
	} else {
		ptr = int64(pages[len(pages)-1]) + 8
	}
	s.vol.putU64(ptr, Parity16Set(uint64(page)))
	grown := make([]uint64, len(pages)+1)
	copy(grown, pages)
	grown[len(pages)] = uint64(page)
	s.indexPages.Store(&grown)
}

// ---------- allocator (structural lock held) ----------

func (s *StoreDirect) allocateNewPageLocked() int64 {
	eof := s.fileTail()
	newEOF := eof + pageSize
	if newEOF > maxVolumeSize {
		panic(mapdb5.New(mapdb5.KindStoreFull, "store size limit reached"))
	}
	s.vol.ensureAvailable(newEOF)
	s.setFileTail(newEOF)
	return eof
}

func (s *StoreDirect) allocRecidLocked() uint64 {
	if v := s.longStackTake(oFreeRecidStack); v != 0 {
		recid := Parity1Get(v) >> 1
		// A persisted free-recid value must name a live index slot; the reuse
		// branch never calls ensureIndexCapacityLocked, so a recid with no page
		// would panic in indexSet. Graceful corruption instead (D4).
		if recid == 0 || recid > s.maxRecid() || s.recidToOffset(recid) < 0 {
			panic(mapdb5.NewCorruption("free recid out of range"))
		}
		return recid
	}
	recid := s.maxRecid() + 1
	s.ensureIndexCapacityLocked(recid)
	s.setMaxRecid(recid)
	return recid
}

func (s *StoreDirect) freeRecidLocked(recid uint64) {
	s.longStackPut(oFreeRecidStack, Parity1Set(recid<<1))
}

// allocateDataLocked: capBytes is 16-aligned within [16, ivMaxCapacity].
func (s *StoreDirect) allocateDataLocked(capBytes int64, recursive bool) int64 {
	if !recursive {
		if v := s.longStackTake(masterLinkOffset(capBytes / 16)); v != 0 {
			off := int64(Parity1Get(v) << 3)
			// The popped free extent is persisted state; it must satisfy the
			// tiling invariants of its size class before being written through
			// (D4). checked-add guards a crafted near-u64::MAX offset.
			fileTail := s.fileTail()
			end := off + capBytes
			if off < pageSize || off&15 != 0 ||
				(off%pageSize)+capBytes > pageSize ||
				end < off || end > fileTail {
				panic(mapdb5.NewCorruption("free-list extent out of range"))
			}
			s.vol.checkRange(off, capBytes)
			s.freeBytes.Add(-capBytes)
			return off
		}
	}
	tail := s.dataTail()
	if tail == 0 {
		page := s.allocateNewPageLocked()
		s.advanceDataTail(page, capBytes)
		return page
	}
	if (tail%pageSize)+capBytes <= pageSize {
		s.advanceDataTail(tail, capBytes)
		return tail
	}
	rem := pageSize - (tail % pageSize)
	page := s.allocateNewPageLocked()
	s.advanceDataTail(page, capBytes)
	s.releaseDataLocked(rem, tail)
	return page
}

func (s *StoreDirect) advanceDataTail(start, capBytes int64) {
	newTail := start + capBytes
	if newTail < start {
		panic(mapdb5.NewCorruption("dataTail arithmetic overflow"))
	}
	if newTail%pageSize == 0 {
		s.setDataTail(0)
	} else {
		s.setDataTail(newTail)
	}
}

func (s *StoreDirect) releaseDataLocked(sizeBytes, offset int64) {
	s.longStackPut(masterLinkOffset(sizeBytes/16), Parity1Set(uint64(offset)>>3))
	s.freeBytes.Add(sizeBytes)
}

// ---------- long stacks (structural lock held) ----------

func (s *StoreDirect) putPackedLong(offset int64, v uint64) int64 {
	size := int64(packLongSize(v))
	p := offset
	for shift := (size - 1) * 7; shift > 0; shift -= 7 {
		s.vol.putByte(p, byte(v>>shift)&0x7F)
		p++
	}
	s.vol.putByte(p, byte(v&0x7F)|0x80)
	return size
}

// getPackedLong decodes a packed long at offset, probing at most limit bytes
// (and never past the 10-byte format max). The probe must not run past the
// caller-validated extent (D4/D5).
func (s *StoreDirect) getPackedLong(offset, limit int64) uint64 {
	var ret uint64
	for i := int64(0); i < min(limit, 10); i++ {
		b := uint64(s.vol.getU8(offset + i))
		ret = ret<<7 | (b & 0x7F)
		if b&0x80 != 0 {
			return ret
		}
	}
	panic(mapdb5.NewCorruption("unterminated packed long"))
}

// loadStackChunkChecked validates and loads the header of a long-stack chunk
// whose offset came from a persisted master/prev link word. Returns
// (chunkSize, prevChunkOffset).
func (s *StoreDirect) loadStackChunkChecked(chunkOffset int64) (int64, int64) {
	s.checkStackChunkOff(chunkOffset)
	hdr := Parity4Get(s.vol.getU64(chunkOffset))
	chunkSize := int64(hdr >> 48)
	if chunkSize < 16 || chunkSize > longStackMaxSize || chunkSize&15 != 0 {
		panic(mapdb5.NewCorruption("bad long stack chunk size"))
	}
	s.vol.checkRange(chunkOffset, chunkSize)
	// The full extent must tile one data page below fileTail and must not name
	// an index page — a crafted master/prev link into index space would let the
	// allocator read or free index slots as stack data.
	end := chunkOffset + chunkSize
	page := chunkOffset - chunkOffset%pageSize
	pages := *s.indexPages.Load()
	_, inIndex := slices.BinarySearch(pages, uint64(page))
	if (chunkOffset%pageSize)+chunkSize > pageSize || end > s.fileTail() || inIndex {
		panic(mapdb5.NewCorruption("long stack chunk extent out of range"))
	}
	return chunkSize, int64(hdr & ivMOffset)
}

func (s *StoreDirect) longStackPut(masterOffset int64, value uint64) {
	master := Parity4Get(s.vol.getU64(masterOffset))
	if master == 0 {
		s.longStackNewChunk(masterOffset, 0, value)
		return
	}
	chunkOffset := int64(master & ivMOffset)
	currPos := int64(master >> 48)
	chunkSize, _ := s.loadStackChunkChecked(chunkOffset)
	if currPos < 8 || currPos > chunkSize {
		panic(mapdb5.NewCorruption("bad long stack position"))
	}
	valueSize := int64(packLongSize(value))
	if currPos+valueSize > chunkSize {
		s.longStackNewChunk(masterOffset, chunkOffset, value)
		return
	}
	s.putPackedLong(chunkOffset+currPos, value)
	s.vol.putU64(masterOffset, Parity4Set(uint64((currPos+valueSize)<<48)|uint64(chunkOffset)))
}

// longStackNewChunk: prevChunkOffset is 0 or already validated; it is only
// WRITTEN into the new header, never dereferenced here.
func (s *StoreDirect) longStackNewChunk(masterOffset, prevChunkOffset int64, value uint64) {
	tail := s.dataTail()
	chunkSize := int64(longStackPrefSize)
	if tail != 0 {
		chunkSize = min(pageSize-(tail%pageSize), longStackPrefSize)
	}
	valueSize := int64(packLongSize(value))
	chunkOffset := s.allocateDataLocked(chunkSize, true)
	s.vol.clear(chunkOffset, chunkOffset+chunkSize)
	s.vol.putU64(chunkOffset, Parity4Set(uint64(chunkSize)<<48|uint64(prevChunkOffset)))
	s.putPackedLong(chunkOffset+8, value)
	s.vol.putU64(masterOffset, Parity4Set(uint64((8+valueSize)<<48)|uint64(chunkOffset)))
}

// longStackTake pops the most recent value (raw, still parity1-encoded), or 0.
func (s *StoreDirect) longStackTake(masterOffset int64) uint64 {
	master := Parity4Get(s.vol.getU64(masterOffset))
	if master == 0 {
		return 0
	}
	chunkOffset := int64(master & ivMOffset)
	chunkSize, prevChunkOffset := s.loadStackChunkChecked(chunkOffset)
	masterPos := int64(master >> 48)
	if masterPos < 8 || masterPos > chunkSize {
		panic(mapdb5.NewCorruption("bad long stack position"))
	}
	pos := max(masterPos-1, 8)
	for pos > 8 && s.vol.getU8(chunkOffset+pos-1)&0x80 == 0 {
		pos--
	}
	value := s.getPackedLong(chunkOffset+pos, chunkSize-pos)
	s.vol.clear(chunkOffset+pos, chunkOffset+pos+int64(packLongSize(value)))
	if pos > 8 {
		s.vol.putU64(masterOffset, Parity4Set(uint64(pos)<<48|uint64(chunkOffset)))
		return value
	}
	// Chunk emptied: relink master to the previous chunk, free this one.
	var prevPos int64
	if prevChunkOffset != 0 {
		prevSize, _ := s.loadStackChunkChecked(prevChunkOffset)
		prevPos = s.longStackFindEnd(prevChunkOffset, prevSize)
	}
	s.vol.putU64(masterOffset, Parity4Set(uint64(prevPos)<<48|uint64(prevChunkOffset)))
	s.releaseDataLocked(chunkSize, chunkOffset)
	return value
}

func (s *StoreDirect) longStackFindEnd(chunkOffset, pos int64) int64 {
	for pos > 8 && s.vol.getU8(chunkOffset+pos-1) == 0 {
		pos--
	}
	return pos
}

// ---------- helpers ----------

func (s *StoreDirect) checkClosed() {
	if s.closed.Load() {
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
}

// mutateEnter enters a volume-touching op: shared commit barrier + closed
// re-check. The caller MUST defer s.commitLock.RUnlock().
func (s *StoreDirect) mutateEnter() {
	s.commitLock.RLock()
	if s.closed.Load() {
		s.commitLock.RUnlock()
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
}

func (s *StoreDirect) segRead(recid uint64) { s.segs[recid&(segmentCount-1)].RLock() }
func (s *StoreDirect) segReadEnd(recid uint64) {
	s.segs[recid&(segmentCount-1)].RUnlock()
}
func (s *StoreDirect) segWrite(recid uint64) { s.segs[recid&(segmentCount-1)].Lock() }
func (s *StoreDirect) segWriteEnd(recid uint64) {
	s.segs[recid&(segmentCount-1)].Unlock()
}

// linkedChain returns (offset, dataLen, capBytes) per chunk, fully validated.
func (s *StoreDirect) linkedChain(ivval uint64) [][3]int64 {
	var chunks [][3]int64
	capUnits := int64(ivCapUnits(ivval))
	off := int64(ivOffset(ivval))
	var total int64
	for {
		capBytes := capUnits * 16
		s.checkReleaseExtent(off, capBytes)
		l := int64(s.vol.getI32(off))
		if l < 0 || linkedChunkHdr+l > capBytes {
			panic(mapdb5.NewCorruption("linked chunk length out of range"))
		}
		chunks = append(chunks, [3]int64{off, l, capBytes})
		total += l
		if total > 1<<31-1 || len(chunks) > 1<<22 {
			panic(mapdb5.NewCorruption("linked chain too long"))
		}
		next := Parity1Get(s.vol.getU64(off + 4))
		if next == 0 {
			break
		}
		capUnits = int64(next >> 48)
		off = int64(next & ivMOffset)
		if capUnits < 1 || capUnits > maxCapUnits || off < pageSize {
			panic(mapdb5.NewCorruption("bad linked chunk pointer"))
		}
	}
	return chunks
}

func (s *StoreDirect) linkedGet(ivval uint64) []byte {
	chunks := s.linkedChain(ivval)
	var total int
	for _, c := range chunks {
		total += int(c[1])
	}
	out := make([]byte, 0, total)
	for _, c := range chunks {
		start := len(out)
		out = out[:start+int(c[1])]
		s.vol.getData(c[0]+linkedChunkHdr, out[start:])
	}
	return out
}

// withStructural runs f under the structural lock with a deferred unlock, so
// a panic inside f (corrupt free-list, full store) cannot leak the lock.
func (s *StoreDirect) withStructural(f func()) {
	s.structural.Lock()
	defer s.structural.Unlock()
	f()
}

// writeNewData allocates and writes a plain record. segment write lock held.
func (s *StoreDirect) writeNewData(recid uint64, buf []byte, capBytes int64, flags uint64) {
	var off int64
	s.withStructural(func() { off = s.allocateDataLocked(capBytes, false) })
	s.vol.putI32(off, int32(len(buf)))
	s.vol.putData(off+4, buf)
	s.indexSet(recid, ivCompose(uint32(capBytes/16), uint64(off), flags))
}

// writeNewLinked writes an oversize record as a linked chunk chain, tail-first.
func (s *StoreDirect) writeNewLinked(recid uint64, buf []byte) {
	length := len(buf)
	tailData := length % int(maxChunkData)
	if tailData == 0 {
		tailData = int(maxChunkData)
	}
	pos := length - tailData
	chunkDataLen := tailData
	nextPtr := Parity1Set(0)
	for {
		capBytes := capBytesFor(linkedChunkHdr + int64(chunkDataLen))
		var off int64
		s.withStructural(func() { off = s.allocateDataLocked(capBytes, false) })
		s.vol.putI32(off, int32(chunkDataLen))
		s.vol.putU64(off+4, nextPtr)
		s.vol.putData(off+linkedChunkHdr, buf[pos:pos+chunkDataLen])
		if pos == 0 {
			s.indexSet(recid, ivCompose(uint32(capBytes/16), uint64(off), ivFlagLinked))
			return
		}
		nextPtr = Parity1Set(uint64(capBytes/16)<<48 | uint64(off))
		chunkDataLen = int(maxChunkData)
		pos -= int(maxChunkData)
	}
}

// releaseOldData frees the data area of ivval if it has one. segment write
// lock held.
func (s *StoreDirect) releaseOldData(ivval uint64) {
	cap := ivCapUnits(ivval)
	if cap == ivCapNull || cap == ivCapDeleted {
		return
	}
	if cap < 1 || cap > ivCapMax {
		panic(mapdb5.NewCorruption("record capacity out of range"))
	}
	if ivIsLinked(ivval) {
		chunks := s.linkedChain(ivval)
		// Validate the whole chain before freeing any chunk.
		for _, c := range chunks {
			s.checkReleaseExtent(c[0], c[2])
		}
		s.structural.Lock()
		defer s.structural.Unlock()
		for _, c := range chunks {
			s.releaseDataLocked(c[2], c[0])
		}
	} else {
		off := int64(ivOffset(ivval))
		s.checkReleaseExtent(off, int64(cap)*16)
		s.structural.Lock()
		defer s.structural.Unlock()
		s.releaseDataLocked(int64(cap)*16, off)
	}
}

// checkReleaseExtent: an index-derived extent must fit its size class, one
// data page, and fileTail before it can be returned to the allocator.
func (s *StoreDirect) checkReleaseExtent(off, size int64) {
	end := off + size
	pages := *s.indexPages.Load()
	page := off - off%pageSize
	_, inIndex := slices.BinarySearch(pages, uint64(page))
	if off < pageSize || off&15 != 0 || size < 16 || size&15 != 0 ||
		size/16 > maxCapUnits || (off%pageSize)+size > pageSize ||
		end < off || end > s.fileTail() || inIndex {
		panic(mapdb5.NewCorruption("record extent out of range"))
	}
	s.vol.checkRange(off, size)
}

// readUsed validates the extent and returns (offset, usedLen) for a
// non-linked live index value.
func (s *StoreDirect) readUsed(ivval uint64) (int64, int64) {
	off := int64(ivOffset(ivval))
	capBytes := int64(ivCapUnits(ivval)) * 16
	s.checkReleaseExtent(off, capBytes)
	used := int64(s.vol.getI32(off))
	if used < 0 || 4+used > capBytes {
		panic(mapdb5.NewCorruption("used beyond capacity"))
	}
	s.vol.checkRange(off, 4+used)
	return off, used
}

// ---------- durability ----------

func (s *StoreDirect) stampHeaderDurable() {
	s.vol.sync()
	s.vol.putI32(oHeadChecksum, s.headChecksum())
	s.vol.syncHeader()
}

func (s *StoreDirect) recomputeFreeDataBytes() {
	var total int64
	for u := int64(1); u <= maxCapUnits; u++ {
		var count int64
		s.forEachLongStack(masterLinkOffset(u), func(uint64) { count++ })
		total += count * (u * 16)
	}
	s.freeBytes.Store(total)
}

// ---------- verify ----------

func (s *StoreDirect) checkStackChunkOff(off int64) {
	if off < pageSize || off&15 != 0 {
		panic(mapdb5.NewCorruption("long stack chunk offset in header/misaligned"))
	}
	s.vol.checkRange(off, 8)
}

// forEachLongStack walks a long stack, calling check with each decoded
// (parity-stripped) value; returns the chunk extents (offset, size).
func (s *StoreDirect) forEachLongStack(masterOffset int64, check func(uint64)) [][2]int64 {
	var extents [][2]int64
	master := Parity4Get(s.vol.getU64(masterOffset))
	if master == 0 {
		return extents
	}
	chunkOffset := int64(master & ivMOffset)
	pos := int64(master >> 48)
	for guard := 0; chunkOffset != 0; guard++ {
		if guard > 1<<24 {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "long stack chunk loop"))
		}
		chunkSize, prev := s.loadStackChunkChecked(chunkOffset)
		if pos < 8 || pos > chunkSize {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "bad long stack position"))
		}
		extents = append(extents, [2]int64{chunkOffset, chunkSize})
		end := chunkOffset + pos
		for p := chunkOffset + 8; p < end; {
			if s.vol.getU8(p) == 0 {
				panic(mapdb5.New(mapdb5.KindVerifyFailed, "zero byte in long stack value area"))
			}
			raw := s.getPackedLong(p, end-p)
			p += int64(packLongSize(raw))
			if p > end {
				panic(mapdb5.New(mapdb5.KindVerifyFailed, "long stack value overruns chunk"))
			}
			check(Parity1Get(raw))
		}
		chunkOffset = prev
		if prev != 0 {
			prevSize, _ := s.loadStackChunkChecked(prev)
			pos = s.longStackFindEnd(prev, prevSize)
		}
	}
	return extents
}

func (s *StoreDirect) verifyLocked() {
	fileTail := s.fileTail()
	dataTail := s.dataTail()
	maxRecid := s.maxRecid()
	if fileTail < pageSize || fileTail%pageSize != 0 {
		panic(mapdb5.New(mapdb5.KindVerifyFailed, "bad fileTail"))
	}
	if !dataTailGeometryOK(dataTail, fileTail) {
		panic(mapdb5.New(mapdb5.KindVerifyFailed, "bad dataTail"))
	}
	if !s.maxRecidGeometryOK(maxRecid) {
		panic(mapdb5.New(mapdb5.KindVerifyFailed, "maxRecid beyond index pages"))
	}

	// index page chain must match the mirror
	indexSet := make(map[uint64]struct{})
	mirror := *s.indexPages.Load()
	ptr := int64(zeroPageLink)
	var n int
	for {
		page := Parity16Get(s.vol.getU64(ptr))
		if page == 0 {
			break
		}
		if n >= len(mirror) || mirror[n] != page {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "index page chain diverges from mirror"))
		}
		if int64(page)%pageSize != 0 || int64(page) >= fileTail {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "index page out of range"))
		}
		if _, dup := indexSet[page]; dup {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "index page loop"))
		}
		indexSet[page] = struct{}{}
		ptr = int64(page) + 8
		n++
	}
	if n != len(mirror) {
		panic(mapdb5.New(mapdb5.KindVerifyFailed, "index page mirror longer than chain"))
	}

	var extents [][2]int64
	for recid := uint64(1); recid <= maxRecid; recid++ {
		ivval := s.rawIndexGet(recid)
		if ivval == 0 {
			continue
		}
		if !ivParityOK(ivval) {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "index parity broken"))
		}
		cap := ivCapUnits(ivval)
		if cap == ivCapDeleted || cap == ivCapNull {
			if ivOffset(ivval) != 0 {
				panic(mapdb5.New(mapdb5.KindVerifyFailed, "sentinel index value with offset"))
			}
			continue
		}
		if ivIsLinked(ivval) {
			for _, c := range s.linkedChain(ivval) {
				extents = append(extents, [2]int64{c[0], c[2]})
			}
		} else {
			off, _ := s.readUsed(ivval)
			extents = append(extents, [2]int64{off, int64(cap) * 16})
		}
	}

	// free recid stack: chunks are extents; each value a deleted recid.
	freeRecids := make(map[uint64]struct{})
	recidChunks := s.forEachLongStack(oFreeRecidStack, func(v uint64) {
		recid := v >> 1
		if recid < 1 || recid > maxRecid {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "free recid out of range"))
		}
		if ivval := s.rawIndexGet(recid); ivval != 0 &&
			(!ivParityOK(ivval) || ivCapUnits(ivval) != ivCapDeleted) {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "free-list recid is live"))
		}
		if _, dup := freeRecids[recid]; dup {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "duplicate free recid"))
		}
		freeRecids[recid] = struct{}{}
	})
	extents = append(extents, recidChunks...)

	// free data stacks: chunks AND freed value extents tile.
	var freeSum int64
	for u := int64(1); u <= maxCapUnits; u++ {
		size := u * 16
		var valueOffsets []int64
		chunkExts := s.forEachLongStack(masterLinkOffset(u), func(v uint64) {
			valueOffsets = append(valueOffsets, int64(v<<3))
		})
		extents = append(extents, chunkExts...)
		for _, off := range valueOffsets {
			extents = append(extents, [2]int64{off, size})
			freeSum += size
		}
	}
	if freeSum != s.freeBytes.Load() {
		panic(mapdb5.New(mapdb5.KindVerifyFailed, "freeDataBytes drift"))
	}

	// geometry + exact tiling
	for _, e := range extents {
		off, size := e[0], e[1]
		if off&15 != 0 || size&15 != 0 || size < 16 {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "unaligned extent"))
		}
		if off < pageSize || off+size > fileTail {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "extent out of bounds"))
		}
		if (off%pageSize)+size > pageSize {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "extent crosses page boundary"))
		}
		if _, inIdx := indexSet[uint64(off-off%pageSize)]; inIdx {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "extent inside an index page"))
		}
	}
	byPage := make(map[int64][][2]int64)
	for _, e := range extents {
		pg := e[0] - e[0]%pageSize
		byPage[pg] = append(byPage[pg], e)
	}
	dataTailPage := int64(-1)
	if dataTail != 0 {
		dataTailPage = dataTail - dataTail%pageSize
	}
	for page := int64(pageSize); page < fileTail; page += pageSize {
		if _, inIdx := indexSet[uint64(page)]; inIdx {
			continue
		}
		coverEnd := page + pageSize
		if page == dataTailPage {
			coverEnd = dataTail
		}
		cursor := page
		if list, ok := byPage[page]; ok {
			slices.SortFunc(list, func(a, b [2]int64) int {
				return int(a[0] - b[0])
			})
			for _, e := range list {
				if e[0] < cursor {
					panic(mapdb5.New(mapdb5.KindVerifyFailed, "overlapping extents"))
				}
				if e[0] > cursor {
					panic(mapdb5.New(mapdb5.KindVerifyFailed, "lost extent: gap"))
				}
				cursor = e[0] + e[1]
			}
			delete(byPage, page)
		}
		if cursor != coverEnd {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "lost extent: page not fully covered"))
		}
	}
	if len(byPage) != 0 {
		panic(mapdb5.New(mapdb5.KindVerifyFailed, "extents on unallocated pages"))
	}
}

// ---------- full compact ----------

type compactEntry struct {
	recid    uint64
	prealloc bool
	capBytes int64
	content  []byte
}

func (s *StoreDirect) compactSnapshot() []compactEntry {
	var entries []compactEntry
	for recid := uint64(1); recid <= s.maxRecid(); recid++ {
		ivval := s.rawIndexGet(recid)
		if ivval == 0 {
			continue
		}
		if !ivParityOK(ivval) {
			panic(mapdb5.NewCorruption("index slot parity broken"))
		}
		cap := ivCapUnits(ivval)
		switch {
		case cap == ivCapDeleted:
		case cap == ivCapNull:
			entries = append(entries, compactEntry{recid: recid, prealloc: ivIsPrealloc(ivval)})
		case ivIsLinked(ivval):
			entries = append(entries, compactEntry{recid: recid, content: s.linkedGet(ivval)})
		default:
			off, used := s.readUsed(ivval)
			content := make([]byte, used)
			s.vol.getData(off+4, content)
			entries = append(entries, compactEntry{
				recid: recid, capBytes: int64(cap) * 16, content: content,
			})
		}
	}
	return entries
}

// compactInner rebuilds the store densely from a pre-taken snapshot. Past the
// crash barrier every failure poisons the store (the caller's job).
func (s *StoreDirect) compactInner(entries []compactEntry, max uint64) {
	// crash barrier: invalidate the on-disk checksum durably.
	s.vol.putI32(oHeadChecksum, ^s.headChecksum())
	s.vol.sync()

	s.withStructural(func() {
		s.setDataTail(0)
		s.setFileTail(pageSize)
		s.vol.putU64(oFreeRecidStack, Parity4Set(0))
		for u := int64(1); u <= maxCapUnits; u++ {
			s.vol.putU64(masterLinkOffset(u), Parity4Set(0))
		}
		s.vol.putU64(zeroPageLink, Parity16Set(0))
		s.vol.clear(zeroSlotsStart, pageSize)
		s.freeBytes.Store(0)
		emptyPages := []uint64{}
		s.indexPages.Store(&emptyPages)
		s.setMaxRecid(max)
		if max > 0 {
			s.ensureIndexCapacityLocked(max)
		}
	})

	for _, e := range entries {
		switch {
		case e.content == nil:
			var flags uint64
			if e.prealloc {
				flags = ivFlagPrealloc
			}
			s.indexSet(e.recid, ivCompose(ivCapNull, 0, flags))
		case needsLinked(int64(len(e.content))):
			s.writeNewLinked(e.recid, e.content)
		default:
			s.writeNewData(e.recid, e.content, e.capBytes, 0)
		}
	}
	s.rebuildFreeRecidsInner()
	s.stampHeaderDurable()
	s.vol.truncate(s.fileTail())
}

// ---------- free functions ----------

// dataTailGeometryOK: a valid dataTail is 0, or 16-aligned, in the data area,
// strictly below fileTail, and NOT page-aligned (D4 oracle shared by
// initOpen and verifyLocked).
func dataTailGeometryOK(dataTail, fileTail int64) bool {
	return dataTail == 0 ||
		(dataTail%16 == 0 && dataTail%pageSize != 0 &&
			dataTail >= pageSize && dataTail < fileTail)
}

func masterLinkOffset(capUnits int64) int64 {
	return oFreeDataStacks + 8*(capUnits-1)
}

func packLongSize(v uint64) int {
	c := 1
	for v >>= 7; v != 0; v >>= 7 {
		c++
	}
	return c
}

func needsLinked(contentLen int64) bool {
	return 4+contentLen > ivMaxCapacity
}

func capBytesFor(need int64) int64 {
	rounded := need + 15
	if rounded < need {
		panic(mapdb5.New(mapdb5.KindRecordTooLarge, "record too large"))
	}
	rounded &^= 15
	if rounded > ivMaxCapacity {
		panic(mapdb5.New(mapdb5.KindRecordTooLarge, "record too large"))
	}
	return rounded
}

// plainNeed: header(4) + content + headroom as checked arithmetic — a huge
// headroom must yield RecordTooLarge, never wrap to a small capacity.
func plainNeed(contentLen, headroom int64) int64 {
	need := 4 + contentLen
	if need < 4 {
		panic(mapdb5.New(mapdb5.KindRecordTooLarge, "record too large"))
	}
	need += headroom
	if need < 4+contentLen {
		panic(mapdb5.New(mapdb5.KindRecordTooLarge, "record too large"))
	}
	return need
}

func serializeErased(record any, sr ser.AnySerializer) []byte {
	w := bin.GetWriter()
	defer bin.PutWriter(w)
	sr.SerializeAny(w, record)
	out := make([]byte, w.Pos)
	copy(out, w.Buf[:w.Pos])
	return out
}

// ---------- Store implementation ----------

func (s *StoreDirect) Preallocate() Recid {
	s.checkClosed()
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	var recid Recid
	s.withStructural(func() { recid = s.allocRecidLocked() })
	s.segWrite(recid)
	s.indexSet(recid, ivCompose(ivCapNull, 0, ivFlagPrealloc))
	s.segWriteEnd(recid)
	return recid
}

func (s *StoreDirect) PreallocateN(into []Recid) {
	for i := range into {
		into[i] = s.Preallocate()
	}
}

func (s *StoreDirect) Put(record any, sr ser.AnySerializer) Recid {
	s.checkClosed()
	validateRecord(sr, record)
	var buf []byte
	if record != nil {
		buf = serializeErased(record, sr)
	}
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	var recid Recid
	s.withStructural(func() { recid = s.allocRecidLocked() })
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	switch {
	case record == nil:
		s.indexSet(recid, ivCompose(ivCapNull, 0, 0))
	case needsLinked(int64(len(buf))):
		s.writeNewLinked(recid, buf)
	default:
		s.writeNewData(recid, buf, capBytesFor(4+int64(len(buf))), 0)
	}
	return recid
}

// readCopy returns a private copy of recid's content (null=true for a null
// record). Get and Read decode the copy AFTER every store lock is released:
// a serializer or RecordRead action may re-enter the store, and Go's
// RWMutex — unlike Java's ReentrantReadWriteLock — blocks a nested read
// once a Commit/Close/Compact writer is queued, which would deadlock.
func (s *StoreDirect) readCopy(recid Recid) (b []byte, null bool) {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segRead(recid)
	defer s.segReadEnd(recid)
	ivval := s.indexGetChecked(recid)
	if ivCapUnits(ivval) == ivCapNull {
		return nil, true
	}
	if ivIsLinked(ivval) {
		return s.linkedGet(ivval), false
	}
	off, used := s.readUsed(ivval)
	return append([]byte(nil), s.vol.view(off+4, used)...), false
}

func (s *StoreDirect) Get(recid Recid, sr ser.AnySerializer) any {
	b, null := s.readCopy(recid)
	if null {
		return nil
	}
	v := sr.DeserializeAny(bin.NewReader(b), len(b))
	if v != nil {
		validateOperand(sr, v)
	}
	return v
}

func (s *StoreDirect) Read(recid Recid, action RecordRead) int64 {
	b, null := s.readCopy(recid)
	if null {
		return action.OnNull()
	}
	return action.OnBytes(bin.NewReader(b), len(b))
}

func (s *StoreDirect) Update(recid Recid, record any, sr ser.AnySerializer) {
	s.updateWithHeadroom(recid, record, sr, 0)
}

func (s *StoreDirect) CompareAndSwap(recid Recid, expectedOld, newRecord any, sr ser.AnySerializer) bool {
	s.checkClosed()
	validateRecord(sr, expectedOld)
	validateRecord(sr, newRecord)
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	ivval := s.indexGetChecked(recid)
	var current any
	switch {
	case ivCapUnits(ivval) == ivCapNull:
	case ivIsLinked(ivval):
		b := s.linkedGet(ivval)
		current = sr.DeserializeAny(bin.NewReader(b), len(b))
	default:
		off, used := s.readUsed(ivval)
		current = sr.DeserializeAny(bin.NewReader(s.vol.view(off+4, used)), int(used))
	}
	if current != nil {
		validateOperand(sr, current)
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
	var out []byte
	if newRecord != nil {
		out = serializeErased(newRecord, sr)
	}
	s.updateLocked(recid, ivval, out, newRecord != nil, 0)
	return true
}

func (s *StoreDirect) Delete(recid Recid, _ ser.AnySerializer) {
	s.checkClosed()
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	ivval := s.indexGetChecked(recid)
	s.releaseOldData(ivval)
	s.withStructural(func() { s.freeRecidLocked(recid) })
	s.indexSet(recid, ivCompose(ivCapDeleted, 0, 0))
}

func (s *StoreDirect) Commit() {
	s.checkClosed()
	s.commitLock.Lock()
	defer s.commitLock.Unlock()
	s.checkClosed()
	s.stampHeaderDurable()
}

func (s *StoreDirect) Compact() {
	s.checkClosed()
	s.commitLock.Lock()
	defer s.commitLock.Unlock()
	s.checkClosed()
	// Snapshot BEFORE the crash barrier — a corruption error here must not
	// poison the store or touch the checksum.
	entries := s.compactSnapshot()
	max := s.maxRecid()
	defer func() {
		if r := recover(); r != nil {
			s.poisoned.Store(true)
			s.closed.Store(true)
			s.vol.putI32(oHeadChecksum, ^s.headChecksum())
			func() {
				defer func() { _ = recover() }()
				s.vol.sync()
			}()
			panic(r)
		}
	}()
	s.compactInner(entries, max)
}

func (s *StoreDirect) Close() error {
	if s.closed.Load() && !s.poisoned.Load() {
		return nil
	}
	s.commitLock.Lock()
	defer s.commitLock.Unlock()
	if s.closed.Load() && !s.poisoned.Load() {
		return nil
	}
	stamp := !s.poisoned.Load()
	s.closed.Store(true)
	s.poisoned.Store(false)
	var tail int64 = -1
	if stamp {
		tail = s.fileTail()
		s.stampHeaderDurable()
	}
	var err error
	s.withStructural(func() { err = s.vol.closeVol(tail) })
	emptyPages := []uint64{}
	s.indexPages.Store(&emptyPages)
	return err
}

func (s *StoreDirect) IsClosed() bool { return s.closed.Load() }

func (s *StoreDirect) Verify() {
	s.checkClosed()
	// Stop-the-world: verify reads whole-store geometry; the exclusive commit
	// lock is the correct oracle scope (mutators hold the shared side).
	s.commitLock.Lock()
	defer s.commitLock.Unlock()
	s.checkClosed()
	s.structural.Lock()
	defer s.structural.Unlock()
	defer func() {
		// Corruption found by the oracle is reported as VerifyFailed.
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok && de.Kind == mapdb5.KindDataCorruption {
				panic(mapdb5.New(mapdb5.KindVerifyFailed, de.Msg))
			}
			panic(r)
		}
	}()
	s.verifyLocked()
}

func (s *StoreDirect) AllRecids() iter.Seq[Recid] {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	var max uint64
	s.withStructural(func() { max = s.maxRecid() })
	var out []Recid
	for recid := uint64(1); recid <= max; recid++ {
		s.segRead(recid)
		ivval := s.rawIndexGet(recid)
		s.segReadEnd(recid)
		if ivval == 0 {
			continue
		}
		if cap := ivCapUnits(ivval); cap == ivCapDeleted || ivIsPrealloc(ivval) {
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

func (s *StoreDirect) IsThreadSafe() bool { return s.threadSafe }
func (s *StoreDirect) IsReadOnly() bool   { return false }
func (s *StoreDirect) IsTx() bool         { return false }

// StructuralGeneration increments on Rollback — a non-tx store is constant.
func (s *StoreDirect) StructuralGeneration() uint64 { return 0 }

// CurrentSize is fileTail minus free-data bytes; 0 after close. Takes only
// the structural lock — serializer callbacks may query size under the
// commit-read side and a nested commit read could deadlock behind a waiting
// close/commit writer.
func (s *StoreDirect) CurrentSize() int64 {
	s.structural.Lock()
	defer s.structural.Unlock()
	if s.closed.Load() {
		return 0
	}
	var ft int64
	func() {
		defer func() { _ = recover() }()
		ft = s.fileTail()
	}()
	return max(ft-s.freeBytes.Load(), 0)
}

// updateWithHeadroom is Update with ≥ headroom trailing bytes provisioned.
func (s *StoreDirect) updateWithHeadroom(recid Recid, record any, sr ser.AnySerializer, headroom int64) {
	s.checkClosed()
	validateRecord(sr, record)
	if headroom < 0 {
		panic(mapdb5.NewMisuse("negative headroom"))
	}
	var out []byte
	if record != nil {
		out = serializeErased(record, sr)
		if !needsLinked(int64(len(out))) {
			capBytesFor(plainNeed(int64(len(out)), headroom))
		}
	}
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	ivval := s.indexGetChecked(recid)
	s.updateLocked(recid, ivval, out, record != nil, headroom)
}

// updateLocked: segment write lock held; ivval is the current checked index
// value. hasContent distinguishes a nil record (null content) from empty.
func (s *StoreDirect) updateLocked(recid uint64, ivval uint64, out []byte, hasContent bool, headroom int64) {
	oldCap := ivCapUnits(ivval)
	if !hasContent {
		s.releaseOldData(ivval)
		s.indexSet(recid, ivCompose(ivCapNull, 0, 0))
		return
	}
	if needsLinked(int64(len(out))) {
		s.releaseOldData(ivval)
		s.writeNewLinked(recid, out)
		return
	}
	need := plainNeed(int64(len(out)), headroom)
	if !ivIsLinked(ivval) && oldCap != ivCapNull && need <= int64(oldCap)*16 {
		// in-place: capacity retained. Validate the value-derived offset BEFORE
		// writing — a corrupt index could otherwise clobber header words (D4/D5).
		off := int64(ivOffset(ivval))
		s.checkReleaseExtent(off, int64(oldCap)*16)
		s.vol.putI32(off, int32(len(out)))
		s.vol.putData(off+4, out)
		s.indexSet(recid, ivCompose(oldCap, uint64(off), 0))
	} else {
		s.releaseOldData(ivval)
		s.writeNewData(recid, out, capBytesFor(need), 0)
	}
}

// ---------- StoreDelta ----------

func (s *StoreDirect) Append(recid Recid, data []byte) int64 {
	s.checkClosed()
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	ivval := s.indexGetChecked(recid)
	if len(data) == 0 {
		// Appending nothing cannot fail and needs no capacity: a no-op
		// returning the current length for EVERY record shape (Java
		// appendInner). Establishing a record here would diverge from
		// StoreWAL, which stages nothing for an empty append.
		return s.contentLen(ivval)
	}
	if ivIsLinked(ivval) {
		return AppendRefused
	}
	if ivCapUnits(ivval) == ivCapNull {
		// first append establishes the record
		if needsLinked(int64(len(data))) {
			s.writeNewLinked(recid, data)
		} else {
			s.writeNewData(recid, data, capBytesFor(4+int64(len(data))), 0)
		}
		return int64(len(data))
	}
	off, used := s.readUsed(ivval)
	capBytes := int64(ivCapUnits(ivval)) * 16
	if 4+used+int64(len(data)) > capBytes {
		return AppendRefused
	}
	s.vol.checkRange(off, 4+used+int64(len(data)))
	s.vol.putData(off+4+used, data)
	s.vol.putI32(off, int32(used)+int32(len(data)))
	return used + int64(len(data))
}

// contentLen is the record's current content length (Java
// currentContentLen). Segment lock must be held.
func (s *StoreDirect) contentLen(ivval uint64) int64 {
	if ivCapUnits(ivval) == ivCapNull {
		return 0
	}
	if ivIsLinked(ivval) {
		var total int64
		for _, c := range s.linkedChain(ivval) {
			total += c[1]
		}
		return total
	}
	_, used := s.readUsed(ivval)
	return used
}

func (s *StoreDirect) CapacityRemaining(recid Recid) int64 {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segRead(recid)
	defer s.segReadEnd(recid)
	ivval := s.indexGetChecked(recid)
	if ivCapUnits(ivval) == ivCapNull || ivIsLinked(ivval) {
		return 0
	}
	_, used := s.readUsed(ivval)
	return max(int64(ivCapUnits(ivval))*16-4-used, 0)
}

func (s *StoreDirect) UpdateWithHeadroom(recid Recid, record any, sr ser.AnySerializer, headroom int) {
	s.updateWithHeadroom(recid, record, sr, int64(headroom))
}

// ---------- WAL hooks (used by StoreWAL) ----------

// walPrealloc establishes a preallocated slot for recid during replay.
func (s *StoreDirect) walPrealloc(recid uint64) {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.withStructural(func() {
		s.ensureIndexCapacityLocked(recid)
		if recid > s.maxRecid() {
			s.setMaxRecid(recid)
		}
	})
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	if ivval := s.rawIndexGet(recid); ivval == 0 || ivCapUnits(ivval) == ivCapDeleted {
		s.indexSet(recid, ivCompose(ivCapNull, 0, ivFlagPrealloc))
	}
}

// walPut replays a put: capBytes==0 sizes from content; content nil = null.
func (s *StoreDirect) walPut(recid uint64, capBytes int64, content []byte, hasContent bool) {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.withStructural(func() {
		s.ensureIndexCapacityLocked(recid)
		if recid > s.maxRecid() {
			s.setMaxRecid(recid)
		}
	})
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	if ivval := s.rawIndexGet(recid); ivval != 0 {
		if !ivParityOK(ivval) {
			panic(mapdb5.NewCorruption("index slot parity broken"))
		}
		s.releaseOldData(ivval)
	}
	switch {
	case !hasContent:
		s.indexSet(recid, ivCompose(ivCapNull, 0, 0))
	case needsLinked(int64(len(content))):
		s.writeNewLinked(recid, content)
	default:
		cap := capBytes
		if cap == 0 {
			cap = capBytesFor(4 + int64(len(content)))
		}
		if cap > ivMaxCapacity || cap < 4+int64(len(content)) || cap&15 != 0 {
			panic(mapdb5.NewCorruption("bad record capacity"))
		}
		s.writeNewData(recid, content, cap, 0)
	}
}

// walDelete is Delete but a no-op on void/deleted recids: a cleaned WAL may
// legitimately contain a delete for a recid it already removed.
func (s *StoreDirect) walDelete(recid uint64) {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segWrite(recid)
	defer s.segWriteEnd(recid)
	ivval := s.rawIndexGet(recid)
	if ivval == 0 {
		return
	}
	if !ivParityOK(ivval) {
		panic(mapdb5.NewCorruption("index slot parity broken"))
	}
	if ivCapUnits(ivval) == ivCapDeleted {
		return
	}
	s.releaseOldData(ivval)
	s.withStructural(func() { s.freeRecidLocked(recid) })
	s.indexSet(recid, ivCompose(ivCapDeleted, 0, 0))
}

func (s *StoreDirect) rebuildFreeRecids() {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.rebuildFreeRecidsInner()
}

func (s *StoreDirect) rebuildFreeRecidsInner() {
	s.structural.Lock()
	defer s.structural.Unlock()
	master := Parity4Get(s.vol.getU64(oFreeRecidStack))
	chunkOffset := int64(master & ivMOffset)
	var chunks [][2]int64
	for chunkOffset != 0 {
		size, prev := s.loadStackChunkChecked(chunkOffset)
		chunks = append(chunks, [2]int64{chunkOffset, size})
		chunkOffset = prev
		if len(chunks) > 1<<24 {
			panic(mapdb5.NewCorruption("free recid stack loop"))
		}
	}
	s.vol.putU64(oFreeRecidStack, Parity4Set(0))
	for _, c := range chunks {
		s.releaseDataLocked(c[1], c[0])
	}
	for recid := uint64(1); recid <= s.maxRecid(); recid++ {
		if ivval := s.rawIndexGet(recid); ivval == 0 || ivCapUnits(ivval) == ivCapDeleted {
			s.freeRecidLocked(recid)
		}
	}
}

// recState reports STATE_* for the WAL merge logic.
func (s *StoreDirect) recState(recid uint64) int {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segRead(recid)
	defer s.segReadEnd(recid)
	ivval := s.rawIndexGet(recid)
	if ivval == 0 {
		return stateVoid
	}
	if !ivParityOK(ivval) {
		panic(mapdb5.NewCorruption("index slot parity broken"))
	}
	if ivCapUnits(ivval) == ivCapDeleted {
		return stateVoid
	}
	if ivCapUnits(ivval) == ivCapNull {
		return stateNull
	}
	return stateLive
}

// rawGet returns a copy of record content (nil for null/prealloc); GetVoid
// on void/deleted.
func (s *StoreDirect) rawGet(recid uint64) []byte {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	s.segRead(recid)
	defer s.segReadEnd(recid)
	ivval := s.indexGetChecked(recid)
	if ivCapUnits(ivval) == ivCapNull {
		return nil
	}
	if ivIsLinked(ivval) {
		return s.linkedGet(ivval)
	}
	off, used := s.readUsed(ivval)
	r := make([]byte, used)
	s.vol.getData(off+4, r)
	return r
}

// walSnapshotOne snapshots one recid for the WAL cleaner, then invokes sink
// after the per-recid lock is released. Returns whether the record exists.
func (s *StoreDirect) walSnapshotOne(recid uint64, sink func(prealloc bool, capBytes int64, content []byte)) bool {
	s.mutateEnter()
	defer s.commitLock.RUnlock()
	var emit, prealloc bool
	var capBytes int64
	var content []byte
	s.segRead(recid)
	func() {
		// The per-recid read lock is released before sink runs, and on panic.
		defer s.segReadEnd(recid)
		ivval := s.rawIndexGet(recid)
		if ivval != 0 && !ivParityOK(ivval) {
			panic(mapdb5.NewCorruption("index slot parity broken"))
		}
		cap := ivCapUnits(ivval)
		if ivval != 0 && cap != ivCapDeleted {
			emit = true
			switch {
			case cap == ivCapNull:
				prealloc = ivIsPrealloc(ivval)
			case ivIsLinked(ivval):
				content = s.linkedGet(ivval)
			default:
				off, used := s.readUsed(ivval)
				content = make([]byte, used)
				s.vol.getData(off+4, content)
				capBytes = int64(cap) * 16
			}
		}
	}()
	if emit {
		sink(prealloc, capBytes, content)
	}
	return emit
}
