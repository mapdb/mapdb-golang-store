package store

// The WAL's v3 codec and recovery state machine — sections, entries, the 'K'
// clean mark, and the two-pass replay (R3-R7) that turns a segment set into
// an in-memory store. Port of wal_recover.rs (Java StoreWAL recovery half).
//
//	section := tag u8 ('S' commit | 'C' image | 'K' clean mark)
//	         | lsn i64 | bodyLen i64 | hdrCrc i32 | bodyCrc i32      // 25 bytes
//	         | body
//	mark    := cleanedThroughSeq i64 | logStartLsn i64               // 16 bytes
//	entry   := T_PREALLOC recid
//	         | T_RECORD   recid cap len+1|0 payload?
//	         | T_APPEND   recid (lsn - baseLsn) len payload
//	         | T_DELETE   recid                       // packLong framing
//
// Both CRCs are domain-bound: each is CRC-32 fed the segment's 36 header
// bytes and the section's own offset as a prefix, so a section byte-copied to
// another segment or offset fails its checksums.
//
// Pass 1 (scanSegment) establishes boundaries only; pass 2 (applySection)
// replays entries in ascending LSN order. A verdict found in pass 1 is HELD,
// not thrown: the segment may be below a clean mark and about to be deleted.

import (
	"encoding/binary"
	"fmt"
	"os"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

const (
	secHdr       = 25 // tag(1) + lsn(8) + bodyLen(8) + hdrCrc(4) + bodyCrc(4)
	secHdrCRCLen = 17 // bytes of the section header covered by hdrCrc

	tagSection = byte('S') // a committed transaction
	tagImage   = byte('C') // a cleaner-written image; semantically identical
	tagMark    = byte('K') // a clean mark; no entries
	markBodyLn = 16        // cleanedThroughSeq i64 | logStartLsn i64

	tPrealloc = 1
	tRecord   = 2
	tAppend   = 3
	tDelete   = 4
)

// 'S', 'C' and 'K' — ALL THREE, in the main scan and the lookahead alike: a
// 'K' after a rotted section must still be recognised as valid, or deliberate
// mid-log rot would be silently truncated away as a torn tail.
func validTag(t byte) bool { return t == tagSection || t == tagImage || t == tagMark }

func be64(b []byte, off int) int64 { return int64(binary.BigEndian.Uint64(b[off : off+8])) }
func be32(b []byte, off int) int32 { return int32(binary.BigEndian.Uint32(b[off : off+4])) }

func parseSecHdr(hdr *[secHdr]byte) (tag byte, lsn, bodyLen int64, hdrCrc, bodyCrc int32) {
	return hdr[0], be64(hdr[:], 1), be64(hdr[:], 9), be32(hdr[:], 17), be32(hdr[:], 21)
}

// sealSecHdr builds the 25 header bytes for a section whose body has been
// MEASURED — a length and a body CRC, not the bytes themselves. This is the
// ONE encoding of a section header, shared by writer and test kit.
func sealSecHdr(segHeader *[segHdr]byte, off int64, tag byte, lsn, bodyLen int64, bodyCRC int32) [secHdr]byte {
	var hdr [secHdr]byte
	hdr[0] = tag
	binary.BigEndian.PutUint64(hdr[1:9], uint64(lsn))
	binary.BigEndian.PutUint64(hdr[9:17], uint64(bodyLen))
	h := newCRC()
	crcDomainOf(h, segHeader, off)
	h.write(hdr[:secHdrCRCLen])
	binary.BigEndian.PutUint32(hdr[17:21], uint32(h.sum()))
	binary.BigEndian.PutUint32(hdr[21:25], uint32(bodyCRC))
	return hdr
}

// buildMarkBody is the 16-byte 'K' body. Written by the cleaner.
func buildMarkBody(cleanedThroughSeq, logStartLsn int64) [16]byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(cleanedThroughSeq))
	binary.BigEndian.PutUint64(b[8:], uint64(logStartLsn))
	return b
}

// hdrCRC computes a section header's CRC-32 in its domain.
func hdrCRC(seg *walSegment, off int64, hdr *[secHdr]byte) int32 {
	h := newCRC()
	seg.crcDomain(h, off)
	h.write(hdr[:secHdrCRCLen])
	return h.sum()
}

func readSecHdr(seg *walSegment, pos int64) (*[secHdr]byte, error) {
	var hdr [secHdr]byte
	ok, err := readAtOpt(seg.segFile(), hdr[:], pos)
	if err != nil {
		return nil, walIOErr(err)
	}
	if !ok {
		return nil, nil
	}
	return &hdr, nil
}

// bodyCRC streams a section body's CRC in its domain through a bounded
// window; nil means the file was shorter than the body claims (torn tail).
func bodyCRC(seg *walSegment, sectionOff, start, end, replayBuf int64) (*int32, error) {
	file := seg.segFile()
	crc := newCRC()
	seg.crcDomain(crc, sectionOff)
	if start < end {
		cap := end - start
		max := replayBuf
		if max < 16 {
			max = 16
		}
		if cap > max {
			cap = max
		}
		buf := make([]byte, cap)
		for p := start; p < end; {
			n := end - p
			if n > int64(len(buf)) {
				n = int64(len(buf))
			}
			ok, err := readAtOpt(file, buf[:n], p)
			if err != nil {
				return nil, walIOErr(err)
			}
			if !ok {
				return nil, nil
			}
			crc.write(buf[:n])
			p += n
		}
	}
	c := crc.sum()
	return &c, nil
}

// ---------- the streaming entry decoder ----------

// secIn is a fixed-size window over one section body, with int64 file
// positions. It never materializes a body, so a commit larger than memory
// replays. It folds no CRC into its reads: v3 CRCs are verified in pass 1
// before a single entry is decoded ("garbage never allocates").
type secIn struct {
	file      *os.File
	limit     int64 // SOFT end: the section being decoded
	hardLimit int64 // HARD end: how far the window may read ahead
	win       []byte
	winStart  int64
	winPos    int
	winLen    int
}

func newSecIn(file *os.File, bufsize int64) *secIn {
	if bufsize < 16 {
		bufsize = 16
	}
	return &secIn{file: file, win: make([]byte, bufsize)}
}

// reset positions the reader over [start,end) and DROPS the window.
func (r *secIn) reset(start, end int64) {
	r.winStart = start
	r.limit = end
	r.hardLimit = end
	r.winPos = 0
	r.winLen = 0
}

// rebound narrows the soft bound to [start,end), KEEPING the window when it
// already covers start.
func (r *secIn) rebound(start, end int64) {
	r.limit = end
	if start >= r.winStart && start < r.winStart+int64(r.winLen) {
		r.winPos = int(start - r.winStart)
	} else {
		r.winStart = start
		r.winPos = 0
		r.winLen = 0
	}
}

// resetHard positions the reader over [start, hardEnd) as its hard bound.
func (r *secIn) resetHard(start, hardEnd int64) { r.reset(start, hardEnd) }

// seek moves to pos within the current bounds, keeping the window when it
// covers the target.
func (r *secIn) seek(pos int64) { r.rebound(pos, r.limit) }

func (r *secIn) pos() int64 { return r.winStart + int64(r.winPos) }

func (r *secIn) remaining() int64 { return r.limit - r.pos() }

// A read past the section's end is corruption, not a torn tail: pass 1
// already proved this section whole and CRC-valid.
func (r *secIn) refill() {
	r.winStart = r.pos()
	r.winPos = 0
	if r.winStart >= r.limit {
		panic(walCorrupt(fmt.Sprintf("WAL entry overran its section body at %d", r.limit)))
	}
	hi := r.hardLimit
	if hi < r.limit {
		hi = r.limit
	}
	n := hi - r.winStart
	if n > int64(len(r.win)) {
		n = int64(len(r.win))
	}
	mustIO(readAtOptE(r.file, r.win[:n], r.winStart))
	r.winLen = int(n)
}

func readAtOptE(f *os.File, b []byte, off int64) error {
	ok, err := readAtOpt(f, b, off)
	if err != nil {
		return walIOErr(err)
	}
	if !ok {
		return walIOErr(fmt.Errorf("short read on WAL segment"))
	}
	return nil
}

func (r *secIn) readByte() byte {
	if r.pos() >= r.limit || r.winPos >= r.winLen {
		r.refill()
	}
	b := r.win[r.winPos]
	r.winPos++
	return b
}

// packLong: MSB-first 7-bit groups, high bit terminates. Capped at 10 bytes
// where Java's decoder loops to the terminator — a deliberate, recorded
// strictness that changes only which MALFORMED input is refused.
func (r *secIn) unpackLong() uint64 {
	var ret uint64
	for i := 0; i < 10; i++ {
		v := r.readByte()
		ret = ret<<7 | uint64(v&0x7F)
		if v&0x80 != 0 {
			return ret
		}
	}
	panic(walCorrupt("WAL packed long too long"))
}

func (r *secIn) readFully(dst []byte) {
	off := 0
	for off < len(dst) {
		if r.pos() >= r.limit || r.winPos >= r.winLen {
			r.refill()
		}
		n := r.winLen - r.winPos
		if rem := len(dst) - off; n > rem {
			n = rem
		}
		copy(dst[off:off+n], r.win[r.winPos:r.winPos+n])
		r.winPos += n
		off += n
	}
}

// capValid: capacity as the writer encodes it — 0 for null content, else
// 16-aligned, big enough for header+content, within the plain-record limit —
// EXCEPT oversize (linked) records, encoded with capacity 0.
func capValid(cap uint64, data []byte, hasData bool) bool {
	max := uint64(ivCapMax) * 16
	if !hasData {
		return cap == 0
	}
	if cap == 0 {
		return 4+uint64(len(data)) > max
	}
	return cap >= 4+uint64(len(data)) && cap <= max && cap&15 == 0
}

// ---------- the two per-recid identities ----------

// walIdentities are the per-recid identities replay maintains, plus the
// deferred skip audit. Derived purely from what has been applied; a wrong one
// can only refuse the open, never recover different data.
type walIdentities struct {
	contentBaseLsn map[uint64]int64
	stateLsn       map[uint64]int64
	skippedAppends map[uint64]struct{}
}

func newWalIdentities() *walIdentities {
	return &walIdentities{
		contentBaseLsn: map[uint64]int64{},
		stateLsn:       map[uint64]int64{},
		skippedAppends: map[uint64]struct{}{},
	}
}

func (i *walIdentities) content(recid uint64, lsn int64) {
	i.contentBaseLsn[recid] = lsn
	i.stateLsn[recid] = lsn
	delete(i.skippedAppends, recid)
}

func (i *walIdentities) stateOnly(recid uint64, lsn int64) {
	delete(i.contentBaseLsn, recid)
	i.stateLsn[recid] = lsn
	delete(i.skippedAppends, recid)
}

func (i *walIdentities) void(recid uint64) {
	delete(i.contentBaseLsn, recid)
	delete(i.stateLsn, recid)
	delete(i.skippedAppends, recid)
}

// End of replay: every skipped append must have been superseded. A recid
// still present means the retained log holds a delta whose base is gone and
// nothing later re-established it — refuse the open.
func (i *walIdentities) audit() {
	for recid := range i.skippedAppends {
		panic(walCorrupt(fmt.Sprintf(
			"WAL replay skipped %d append(s) whose base image is absent and which no later "+
				"entry superseded (recid %d): the log is missing sections it depends on",
			len(i.skippedAppends), recid)))
	}
}

// ---------- R3: pass 1 ----------

// hold records a verdict against a segment; R4 decides whether it matters.
// First message wins.
func hold(seg *walSegment, msg string) {
	if seg.held == "" {
		seg.held = msg
	}
}

// scanSegment is R3 for one segment: leaves validEnd, firstLsn, lastLsn and
// at most one held verdict on seg; returns the highest cleanedThroughSeq
// attested by a valid 'K' INSIDE this segment, updating markLogStart with the
// last segment-local maximum's logStartLsn.
func scanSegment(seg *walSegment, lookLast int64, isActive bool, replayBuf int64, markLogStart *int64) int64 {
	var segThrough int64
	length := seg.fileLen
	pos := int64(segHdr)
	seg.validEnd = pos
	for pos+secHdr <= length {
		hdrp, err := readSecHdr(seg, pos)
		if err != nil {
			panic(err)
		}
		if hdrp == nil {
			if !isActive {
				hold(seg, "non-final segment is shorter than its own sections claim")
			}
			return segThrough
		}
		hdr := *hdrp
		tag, lsn, bodyLen, storedHdrCrc, storedBodyCrc := parseSecHdr(&hdr)
		bodyStart := pos + secHdr

		if hdrCRC(seg, pos, &hdr) != storedHdrCrc || !validTag(tag) {
			// S3. Every byte of the failed header is untrusted; search from the
			// next byte for a section carrying the exact successor LSN.
			if !isActive {
				hold(seg, fmt.Sprintf("section header damaged at offset %d in a non-final segment", pos))
				return segThrough
			}
			if laterSectionOrScanLimit(seg, pos+1, length, lookLast, replayBuf) {
				hold(seg, fmt.Sprintf(
					"mid-log corruption: section header damaged at offset %d (later section or scan limit)", pos))
			}
			return segThrough // torn tail
		}
		if bodyLen < 0 || bodyLen > length-bodyStart {
			// S5: a verified header whose body runs past the end of the file is
			// a torn tail by construction.
			if !isActive {
				hold(seg, fmt.Sprintf("section body extends past the end of a non-final segment at offset %d", pos))
			}
			return segThrough
		}
		bodyEnd := bodyStart + bodyLen
		c, err := bodyCRC(seg, pos, bodyStart, bodyEnd, replayBuf)
		if err != nil {
			panic(err)
		}
		if c == nil {
			if !isActive {
				hold(seg, "non-final segment is shorter than its own sections claim")
			}
			return segThrough
		}
		if *c != storedBodyCrc {
			// S4. bodyEnd IS trusted — the header sealed it — so the lookahead
			// starts at a real section boundary.
			if !isActive {
				hold(seg, fmt.Sprintf("section body CRC mismatch at offset %d in a non-final segment", pos))
				return segThrough
			}
			if anyValidSectionFrom(seg, bodyEnd, length, lookLast, replayBuf) {
				hold(seg, fmt.Sprintf(
					"mid-log corruption: section body CRC mismatch at offset %d but valid sections follow", pos))
			}
			return segThrough
		}

		// The section is whole. From here is a WRITER-defect class: CRC-valid
		// means deliberate bytes — corruption, still HELD because this segment
		// may be superseded.
		if seg.lastLsn != 0 {
			// Both density checks live under this guard — frozen reference
			// behaviour: 0 doubles as the "no section seen" sentinel.
			if lsn <= seg.lastLsn {
				hold(seg, fmt.Sprintf("section LSN %d at offset %d does not follow %d", lsn, pos, seg.lastLsn))
				return segThrough
			}
			if lsn != seg.lastLsn+1 {
				// S9: LSNs are DENSE by construction — a gap detects a clean
				// whose 'C' sections vanished wholly.
				hold(seg, fmt.Sprintf("section LSNs must be consecutive: %d at offset %d after %d",
					lsn, pos, seg.lastLsn))
				return segThrough
			}
		}
		if tag == tagMark {
			through, logStart, fault := readMark(seg, bodyStart, bodyLen, lsn, seg.seq)
			if fault != "" {
				hold(seg, fault)
				return segThrough
			}
			// Per-segment-scan and strict: an equal through later in the same
			// segment does not displace the first one's logStartLsn.
			if through > segThrough {
				segThrough = through
				*markLogStart = logStart
			}
		}
		if seg.firstLsn == 0 {
			seg.firstLsn = lsn
		}
		seg.lastLsn = lsn
		lookLast = lsn
		pos = bodyEnd
		seg.validEnd = pos
	}
	if pos < length && !isActive {
		// S6: W3 leaves no trailing bytes below the highest name.
		hold(seg, fmt.Sprintf("non-final segment has %d trailing bytes past its last section", length-pos))
	}
	return segThrough
}

// readMark validates one 'K' body. A non-empty fault is a held verdict.
func readMark(seg *walSegment, bodyStart, bodyLen, lsn, segSeq int64) (through, logStart int64, fault string) {
	if bodyLen != markBodyLn {
		return 0, 0, fmt.Sprintf("clean mark body is %d bytes, not %d", bodyLen, markBodyLn)
	}
	var body [markBodyLn]byte
	ok, err := readAtOpt(seg.segFile(), body[:], bodyStart)
	if err != nil {
		panic(walIOErr(err))
	}
	if !ok {
		return 0, 0, "clean mark body is truncated"
	}
	through = be64(body[:], 0)
	logStart = be64(body[:], 8)
	if through <= 0 {
		return 0, 0, fmt.Sprintf("clean mark attests cleanedThroughSeq %d", through)
	}
	if logStart <= 0 || logStart > lsn {
		return 0, 0, fmt.Sprintf(
			"clean mark attests logStartLsn %d, which is not an LSN at or below the mark's own %d",
			logStart, lsn)
	}
	if through >= segSeq {
		// K4: a mark may never authorize removing its own segment.
		return 0, 0, fmt.Sprintf(
			"clean mark in segment %d authorizes removing segment %d, including itself", segSeq, through)
	}
	return through, logStart, ""
}

// anyValidSectionFrom is the trusted-boundary S4 walk: true when [from,limit)
// holds at least one fully valid section carrying a strictly future LSN —
// proving durable committed sections follow a bad one. A framed candidate
// walk: framing-valid on header CRC, tag and body length alone; 'K' body
// constraints and entry decoding are deliberately NOT checked.
func anyValidSectionFrom(seg *walSegment, from, limit, lastLsn, replayBuf int64) bool {
	for pos := from; pos+secHdr <= limit; {
		hdrp, err := readSecHdr(seg, pos)
		if err != nil {
			panic(err)
		}
		if hdrp == nil {
			return false
		}
		hdr := *hdrp
		tag, lsn, bodyLen, storedHdrCrc, storedBodyCrc := parseSecHdr(&hdr)
		bodyStart := pos + secHdr
		if hdrCRC(seg, pos, &hdr) != storedHdrCrc || !validTag(tag) ||
			bodyLen < 0 || bodyLen > limit-bodyStart {
			return false
		}
		bodyEnd := bodyStart + bodyLen
		// Wrapping like the reference: lastLsn is a disk number.
		if lsn > lastLsn+1 {
			c, err := bodyCRC(seg, pos, bodyStart, bodyEnd, replayBuf)
			if err != nil {
				panic(err)
			}
			if c == nil {
				return false
			}
			if *c == storedBodyCrc {
				return true
			}
		}
		pos = bodyEnd
	}
	return false
}

// laterSectionOrScanLimit is S3's untrusted-header successor search. Windows
// overlap by secHdr-1 bytes so a candidate crossing a window edge is tested
// at its actual offset; aggregate body reads are limited to the searched
// remainder, and exhausting that budget is a conservative corruption verdict.
func laterSectionOrScanLimit(seg *walSegment, from, limit, lastLsn, replayBuf int64) bool {
	const window = 64 * 1024
	buf := make([]byte, window)
	budget := limit - from
	pos := from
	for limit-pos >= secHdr {
		count := limit - pos
		if count > window {
			count = window
		}
		ok, err := readAtOpt(seg.segFile(), buf[:count], pos)
		if err != nil {
			panic(walIOErr(err))
		}
		if !ok {
			return false
		}
		candidates := int(count) - secHdr + 1
		for i := 0; i < candidates; i++ {
			var hdr [secHdr]byte
			copy(hdr[:], buf[i:i+secHdr])
			tag, lsn, bodyLen, storedHdrCrc, storedBodyCrc := parseSecHdr(&hdr)
			// exact successor LSN, wrapping like the reference
			if !validTag(tag) || lsn != lastLsn+2 {
				continue
			}
			sectionPos := pos + int64(i)
			bodyStart := sectionPos + secHdr
			if bodyLen < 0 || bodyLen > limit-bodyStart {
				continue
			}
			if hdrCRC(seg, sectionPos, &hdr) != storedHdrCrc {
				continue
			}
			if bodyLen > budget {
				return true
			}
			budget -= bodyLen
			c, err := bodyCRC(seg, sectionPos, bodyStart, bodyStart+bodyLen, replayBuf)
			if err != nil {
				panic(err)
			}
			if c == nil {
				return false
			}
			if *c == storedBodyCrc {
				return true
			}
		}
		pos += int64(candidates)
	}
	return false
}

// ---------- R4: adjudicate ----------

// adjudicate returns the index at which the RETAINED suffix begins — the
// segments above cleanedThrough. Verdicts and discontinuities below it are
// DISCARDED: those segments are superseded and about to be deleted.
//
// Every check is an equality between two recorded numbers: the lowest
// retained segment's stated start vs the newest mark's logStartLsn (or 1),
// each later segment's stated start vs its predecessor's end (or stated start
// when that predecessor is empty), and each segment's first LSN vs its own
// header's promise.
func adjudicate(segments []*walSegment, cleanedThrough, markLogStart int64) int {
	start := 0
	for start < len(segments) && segments[start].seq <= cleanedThrough {
		start++
	}
	if start == len(segments) {
		// Unreachable through K4; checked because everything below depends on it.
		panic(walCorrupt(fmt.Sprintf("WAL clean mark %d retires the whole segment set", cleanedThrough)))
	}
	retained := segments[start:]
	for _, s := range retained {
		if s.held != "" {
			panic(walCorrupt(fmt.Sprintf("WAL segment %s: %s", segFileName(s), s.held)))
		}
	}
	// The floor runs ALWAYS, not only when there is no anchor: the chain
	// witnesses LSN continuity, the floor the mark-image contract.
	var expectedStart int64 = 1
	if markLogStart > 0 {
		expectedStart = markLogStart
	}
	var prev *walSegment
	for _, s := range retained {
		stated := s.headerFirstLsn()
		if prev == nil {
			if stated != expectedStart {
				var why string
				if markLogStart > 0 {
					why = fmt.Sprintf("the clean mark attests it begins at %d", markLogStart)
				} else {
					why = "an unmarked log must begin at LSN 1"
				}
				panic(walCorrupt(fmt.Sprintf(
					"WAL retained log begins at LSN %d in %s but %s: sections below it are gone",
					stated, segFileName(s), why)))
			}
		} else {
			var after int64
			if prev.lastLsn != 0 {
				after = prev.lastLsn + 1
			} else {
				after = prev.headerFirstLsn()
			}
			if stated != after {
				panic(walCorrupt(fmt.Sprintf(
					"WAL segment %s states it begins at LSN %d but %s accounts for LSNs up to %d: "+
						"sections between them are gone",
					segFileName(s), stated, segFileName(prev), after-1)))
			}
		}
		// A segment must also hold what its own header promised. The gate is
		// firstLsn != 0, NOT "nonempty": a segment holding only crafted lsn==0
		// sections is nonempty with firstLsn 0 and skips the self-check —
		// frozen reference behaviour.
		if s.firstLsn != 0 && s.firstLsn != stated {
			panic(walCorrupt(fmt.Sprintf(
				"WAL segment %s states it begins at LSN %d but its first section is %d: "+
					"its leading sections are gone", segFileName(s), stated, s.firstLsn)))
		}
		prev = s
	}
	return start
}

func segFileName(seg *walSegment) string {
	base := seg.path
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '/' {
			return base[i+1:]
		}
	}
	return base
}

// ---------- R6: pass 2 ----------

// pass2 replays one segment's validated section prefix. Pass 1 is the sole
// authority on boundaries; headers are re-read but never re-validated.
func pass2(seg *walSegment, inner *StoreDirect, ids *walIdentities, replayBuf, recoveryIndexBytes int64) {
	file := seg.segFile()
	input := newSecIn(file, replayBuf)
	pos := int64(segHdr)
	for pos < seg.validEnd {
		var hdr [secHdr]byte
		mustIO(readAtOptE(file, hdr[:], pos))
		tag, lsn, bodyLen, _, _ := parseSecHdr(&hdr)
		bodyStart := pos + secHdr
		if bodyStart > seg.validEnd || bodyLen < 0 || bodyLen > seg.validEnd-bodyStart {
			panic(walCorrupt(fmt.Sprintf(
				"WAL segment %s changed between recovery passes at offset %d", segFileName(seg), pos)))
		}
		if tag != tagMark {
			applySection(inner, input, bodyStart, bodyStart+bodyLen, lsn, ids, recoveryIndexBytes)
		}
		pos = bodyStart + bodyLen
	}
}

// applySection decodes and applies one CRC-verified section body as the §4.2
// state transition table; a malformed entry is a writer bug or corruption,
// never a torn tail.
//
//	entry             precondition                      action        contentBase  state    skip
//	T_RECORD content  —                                 walPut        = lsn        = lsn    clear
//	T_RECORD null     —                                 walPut(null)  cleared      = lsn    clear
//	T_PREALLOC        R is not content-live             walPrealloc   cleared      = lsn    clear
//	T_PREALLOC        R IS content-live                 DataCorruption
//	T_DELETE          —                                 walDelete     cleared      cleared  clear
//	T_APPEND          baseLsn == contentBase[R]         append        unchanged    unch.    unch.
//	T_APPEND          contentBase[R] absent or > base   SKIP          unchanged    unch.    add R
//	T_APPEND          contentBase[R] < baseLsn          DataCorruption
func applySection(inner *StoreDirect, input *secIn, start, end, lsn int64, ids *walIdentities, recoveryIndexBytes int64) {
	input.reset(start, end)
	// At most one entry per recid per section, 'C' sections included.
	seen := map[uint64]struct{}{}
	for input.pos() < end {
		ty := input.readByte()
		switch ty {
		case tPrealloc:
			recid := entryRecid(seen, input.unpackLong())
			checkRecoveryIndexLimit(recid, uint64(recoveryIndexBytes))
			// walPrealloc no-ops on ANY set slot: on a content-live record it
			// would silently leave a record the identities describe as
			// preallocated.
			if inner.recState(recid) == stateLive {
				panic(walCorrupt(fmt.Sprintf("WAL PREALLOC over a content-live record, recid=%d", recid)))
			}
			inner.walPrealloc(recid)
			ids.stateOnly(recid, lsn)
		case tDelete:
			recid := entryRecid(seen, input.unpackLong())
			inner.walDelete(recid) // tolerant of a void target on purpose
			ids.void(recid)
		case tRecord:
			recid := entryRecid(seen, input.unpackLong())
			checkRecoveryIndexLimit(recid, uint64(recoveryIndexBytes))
			cap := input.unpackLong()
			lenPlus := input.unpackLong()
			var data []byte
			hasData := lenPlus != 0
			if hasData {
				ln := lenPlus - 1
				if ln > uint64(^uint32(0)>>1) || ln > uint64(input.remaining()) {
					panic(walCorrupt(fmt.Sprintf("bad WAL record length %d", ln)))
				}
				data = make([]byte, ln)
				input.readFully(data)
			}
			if !capValid(cap, data, hasData) {
				panic(walCorrupt(fmt.Sprintf("bad WAL record capacity %d", cap)))
			}
			inner.walPut(recid, int64(cap), data, hasData)
			if hasData {
				ids.content(recid, lsn)
			} else {
				ids.stateOnly(recid, lsn)
			}
		case tAppend:
			recid := entryRecid(seen, input.unpackLong())
			baseLsn := decodeBaseLsn(input.unpackLong(), lsn, recid)
			ln := input.unpackLong()
			if ln > uint64(^uint32(0)>>1) || ln > uint64(input.remaining()) {
				panic(walCorrupt(fmt.Sprintf("bad WAL append length %d", ln)))
			}
			base, hasBase := ids.contentBaseLsn[recid]
			if hasBase && base < baseLsn {
				panic(walCorrupt(fmt.Sprintf(
					"WAL append cites base LSN %d above the applied base %d, recid=%d: sections are missing",
					baseLsn, base, recid)))
			}
			b := make([]byte, ln)
			input.readFully(b) // consumed either way: the frame is still framed
			if hasBase && base == baseLsn {
				if inner.Append(Recid(recid), b) == AppendRefused {
					panic(walCorrupt(fmt.Sprintf("WAL append refused, recid=%d", recid)))
				}
			} else {
				// The base this delta extends is gone (cleaned, or superseded
				// by a newer image that already contains these bytes): skip
				// and remember.
				ids.skippedAppends[recid] = struct{}{}
			}
		default:
			panic(walCorrupt(fmt.Sprintf("bad WAL entry tag %d", ty)))
		}
	}
}

// entryRecid checks the two rules that apply to an entry's recid before any
// of its other fields are read: one entry per recid per section, and recid 0
// is reserved.
func entryRecid(seen map[uint64]struct{}, recid uint64) uint64 {
	if recid == 0 {
		panic(walCorrupt("WAL entry references reserved recid 0"))
	}
	if _, dup := seen[recid]; dup {
		panic(walCorrupt(fmt.Sprintf("two WAL entries for recid %d in one section", recid)))
	}
	seen[recid] = struct{}{}
	return recid
}

// decodeBaseLsn turns packLong(lsn - baseLsn) back into an absolute base LSN:
// the delta must be >= 1 and must leave a base LSN >= 1.
func decodeBaseLsn(delta uint64, lsn int64, recid uint64) int64 {
	d := int64(delta)
	if d < 1 || d > lsn-1 {
		panic(walCorrupt(fmt.Sprintf("bad WAL append base delta %d in section LSN %d, recid=%d", d, lsn, recid)))
	}
	return lsn - d
}

// ---------- the ordered algorithm ----------

// walRecovered is what recovery hands back to the store.
type walRecovered struct {
	nextLsn    int64
	identities *walIdentities
}

// recover is the ordered recovery algorithm, R3-R7; R0-R2 already ran inside
// openWalSegmentSet. Read-only recovery takes every decision identically and
// performs none of the mutations.
func walRecover(set *walSegmentSet, inner *StoreDirect, replayBuf, recoveryIndexBytes int64) *walRecovered {
	if len(set.segments) == 0 {
		// N1: a fresh store. The writable open creates segment 1 (or the
		// successor of whatever sequence numbers residue burned) with
		// firstLsn = 1; the read-only open creates nothing.
		if !set.readOnly {
			set.createSegment(1)
		}
		inner.rebuildFreeRecids()
		return &walRecovered{nextLsn: 1, identities: newWalIdentities()}
	}

	// ---- R3: pass 1 — boundaries only, no per-recid state ----
	n := len(set.segments)
	activeIdx := n - 1
	var cleanedThrough, markLogStart, carry int64
	for i, seg := range set.segments {
		seg.ensureOpen()
		scanned := scanSegment(seg, carry, i == activeIdx, replayBuf, &markLogStart)
		// Released the moment this segment's scan is done; pass 2 reopens the
		// ones it needs — this is what bounds the descriptor count to O(1).
		seg.release()
		if scanned > cleanedThrough {
			cleanedThrough = scanned
		}
		// A segment ending in an accepted lsn==0 section must NOT erase the
		// preceding segment's anchor — frozen sentinel behaviour.
		if seg.lastLsn != 0 {
			carry = seg.lastLsn
		}
	}

	// ---- R4: adjudicate ----
	retainedFrom := adjudicate(set.segments, cleanedThrough, markLogStart)

	// R7's answer is computed over the RETAINED set, deliberately.
	var maxValidLsn int64
	for _, s := range set.segments[retainedFrom:] {
		if s.lastLsn > maxValidLsn {
			maxValidLsn = s.lastLsn
		}
	}
	if maxValidLsn == 0 {
		// An all-empty retained set holds no LSN to count from; the lowest
		// segment's header says where the log begins.
		maxValidLsn = set.segments[retainedFrom].headerFirstLsn() - 1
	}

	// ---- R5: unlink the superseded segments, then fsync the directory ----
	set.unlinkThrough(cleanedThrough)

	// ---- R6: pass 2 — apply in ascending (segment, offset) order ----
	ids := newWalIdentities()
	for i, seg := range set.segments {
		if seg.seq <= cleanedThrough {
			continue // read-only: superseded segments are still on disk
		}
		isActive := i == len(set.segments)-1
		seg.ensureOpen()
		func() {
			defer func() {
				if !isActive {
					seg.release()
				}
			}()
			pass2(seg, inner, ids, replayBuf, recoveryIndexBytes)
		}()
	}
	// The audit runs BEFORE R7's truncate.
	ids.audit()

	// ---- R7 ----
	// A recorded divergence: the reference adds without checking and opens
	// the store with nextLsn == i64::MIN; the port refuses — nothing on disk
	// is damaged, the LSN space is used up.
	if maxValidLsn == int64(^uint64(0)>>1) {
		panic(mapdb5.New(mapdb5.KindStoreFull, "WAL LSN space exhausted"))
	}
	nextLsn := maxValidLsn + 1
	if !set.readOnly {
		active := set.active()
		if active.validEnd < active.fileLen {
			// W7: truncate the torn tail, force the SIZE, then rotate so later
			// appends never reuse this segment's checksum domain.
			active.ensureOpen()
			seq, validEnd := active.seq, active.validEnd
			walIOReport(set.io, walOpTruncate, seq, validEnd, 0, 0)
			mustIO(active.segFile().Truncate(validEnd))
			active.fileLen = validEnd
			walIOReport(set.io, walOpForceFull, seq, validEnd, 0, 0)
			mustIO(active.segFile().Sync())
			set.segSyncs++
			// The reference never releases the truncated predecessor — a
			// recorded divergence; the port releases it (O(1)-descriptor rule).
			active.release()
			set.createSegment(nextLsn)
		}
	}
	// Replay of delete-then-reuse histories leaves stale free-list entries
	// for revived recids: rebuild the free list from the final index.
	inner.rebuildFreeRecids()
	return &walRecovered{nextLsn: nextLsn, identities: ids}
}
