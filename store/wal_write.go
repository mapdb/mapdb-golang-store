package store

// The section writer — the only place a v3 section reaches the device.
// Port of Java StoreWAL.appendSection/BodySink/rollover via wal_write.rs.
//
//   - W1/W4: a section's force completes before this returns; recovery's
//     mid-log-rot inference is sound only under that ordering.
//   - W3: rollover happens only at a section boundary, after the sealed
//     segment's last section is forced with a SIZE-persisting force.
//   - W9: a failed or partial write/force fails the store CLOSED.
//
// The body is emitted TWICE — a measure pass (length + CRC, no I/O) and a
// write pass — so one commit may exceed 2 GiB with no whole-body allocation.
// The pass-divergence check runs BEFORE the force, so a nondeterministic
// emitter fails the commit rather than acknowledging a section replay would
// reject as bit rot.

import (
	"os"
	"sync/atomic"
)

// Java's BodySink buffer, byte for byte: entry framing is coalesced through
// it while a payload at or past this size bypasses it.
const sinkBuf = 64 << 10

// bodySink is one pass over a section body. The measure pass (file == nil)
// accumulates length and CRC only; the write pass also writes the bytes at
// increasing offsets. Offsets and count are int64: a body may exceed 2 GiB.
type bodySink struct {
	file  *os.File
	crc   *crc32Digest
	pos   int64
	count int64
	buf   []byte
}

func measureSink(crc *crc32Digest) *bodySink {
	return &bodySink{crc: crc}
}

func writerSink(file *os.File, bodyStart int64, crc *crc32Digest) *bodySink {
	return &bodySink{file: file, crc: crc, pos: bodyStart, buf: make([]byte, 0, sinkBuf)}
}

// write emits b into this pass; the CRC and length advance in both.
func (s *bodySink) write(b []byte) {
	s.crc.write(b)
	s.count += int64(len(b))
	if s.file == nil {
		return
	}
	if len(b) >= sinkBuf {
		s.flush()
		mustIO(writeAllAt(s.file, b, s.pos))
		s.pos += int64(len(b))
		return
	}
	if len(s.buf)+len(b) > sinkBuf {
		s.flush()
	}
	s.buf = append(s.buf, b...)
}

func (s *bodySink) flush() {
	if len(s.buf) == 0 {
		return
	}
	mustIO(writeAllAt(s.file, s.buf, s.pos))
	s.pos += int64(len(s.buf))
	s.buf = s.buf[:0]
}

// appendSection appends one complete section to the active segment and
// forces it, rolling over first when the segment is full. `emit` runs TWICE
// and must produce identical bytes both times.
//
// Every panic out of here is a W9 failure; the caller closes the store.
// `closed` is armed while I/O is in flight so a *panic* mid-write also marks
// the store closed (Java's "Error after I/O began" class).
func appendSection(set *walSegmentSet, segmentBytes int64, io walIO, closed *atomic.Bool,
	tag byte, lsn int64, emit func(sink *bodySink)) {

	active := set.active()
	if active == nil {
		panic(walWrongConfig("WAL store has no active segment"))
	}
	armed := false
	defer func() {
		if armed {
			closed.Store(true)
		}
	}()

	// W3: rollover only at a section boundary, and only when the active
	// segment is nonempty — so one section may exceed segmentBytes and an
	// oversize section gets a segment to itself.
	if active.fileLen >= segmentBytes && !active.empty() {
		armed = true
		active.ensureOpen()
		walIOReport(io, walOpForceFull, active.seq, active.fileLen, 0, 0)
		// force(true), never a data-only sync: this seals the segment and its
		// SIZE is the payload.
		mustIO(active.segFile().Sync())
		set.segSyncs++
		// Nothing reads a sealed segment again; releasing keeps the descriptor
		// count O(1) rather than O(segments).
		active.release()
		set.createSegment(lsn)
		active = set.active()
	}
	segHeader := active.header
	off := active.fileLen
	seq := active.seq

	// pass 1: measure. No I/O, no allocation proportional to the body.
	bcrc := newCRC()
	crcDomainOf(bcrc, &segHeader, off)
	measure := measureSink(bcrc)
	emit(measure)
	bodyLen := measure.count
	bodyCRC := measure.crc.sum()
	hdr := sealSecHdr(&segHeader, off, tag, lsn, bodyLen, bodyCRC)

	active.ensureOpen()
	armed = true
	file := active.segFile()

	walIOReport(io, walOpSecHeader, seq, off, secHdr, tag)
	mustIO(writeAllAt(file, hdr[:], off))
	bodyStart := off + secHdr
	walIOReport(io, walOpSecBody, seq, bodyStart, bodyLen, tag)

	// pass 2: write. Must reproduce pass 1's bytes exactly.
	wcrc := newCRC()
	crcDomainOf(wcrc, &segHeader, off)
	writer := writerSink(file, bodyStart, wcrc)
	emit(writer)
	writer.flush()
	if writer.count != bodyLen || writer.crc.sum() != bodyCRC {
		panic(walCorrupt("WAL section body diverged between the CRC pass and the write pass; refusing to acknowledge"))
	}

	// force(false) — a DATA sync. POSIX fdatasync persists the metadata needed
	// to retrieve the data, which for an append includes the new size. Where
	// the SIZE itself is the payload (create, seal, post-truncate) a full
	// sync is used instead — the distinction is spec (D5).
	end := bodyStart + bodyLen
	walIOReport(io, walOpForceData, seq, end, 0, tag)
	mustIO(fdatasync(file))

	active.fileLen = end
	active.validEnd = end
	armed = false
}
