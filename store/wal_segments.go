package store

// The WAL's on-disk namespace: N/W2/W5/W6/H + the store lock.
// Port of Java WalSegmentSet + the namespace half of StoreWAL (format v3),
// via the Rust port's wal_segments.rs.
//
//	name    := <base> ".wal." <16 lowercase hex digits of segmentSeq>
//	header  := magic "MDBS.WAL"(8) | version i32 = 3 | flags i32 = 0
//	         | segmentSeq i64 | firstLsn i64 | headerCrc i32        // 36 bytes
//
// All integers big-endian; headerCrc is zlib CRC-32 over header bytes [0,32).
// Durability is two parts: a forced SEGMENT (contents survive), and a forced
// DIRECTORY (the name survives — after create/unlink). The lock is
// `<base>.lock` under a POSIX/OFD fcntl record lock — matching Java's
// FileChannel.tryLock; flock() is a different lock class and is never used
// on Linux.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

const (
	segHdr       = 36 // magic 8 + version 4 + flags 4 + seq 8 + firstLsn 8 + crc 4
	segHdrCRCLen = 32
	firstSeq     = int64(1) // 0 is reserved for "no clean mark"
)

var walMagic = []byte("MDBS.WAL")

const walFormatVersion = 3

func walIOErr(err error) *mapdb5.DBError { return mapdb5.NewIO(err) }

func walLocked(msg string) *mapdb5.DBError {
	return mapdb5.New(mapdb5.KindLocked, msg)
}

func walCorrupt(msg string) *mapdb5.DBError {
	return mapdb5.New(mapdb5.KindDataCorruption, msg)
}

func walWrongConfig(msg string) *mapdb5.DBError {
	return mapdb5.New(mapdb5.KindWrongConfiguration, msg)
}

// ---------- the store lock ----------

// processClaims tracks locks THIS process holds so a second open of the same
// store is refused even where the syscall would succeed — OFD read locks are
// compatible by construction, and Java's JVM-wide table refuses ANY
// overlapping second lock whatever its mode.
var processClaims = struct {
	sync.Mutex
	m map[[2]uint64]string
}{m: make(map[[2]uint64]string)}

func claimLock(key [2]uint64, path string) bool {
	processClaims.Lock()
	defer processClaims.Unlock()
	if _, ok := processClaims.m[key]; ok {
		return false
	}
	processClaims.m[key] = path
	return true
}

func releaseClaim(key [2]uint64) {
	processClaims.Lock()
	delete(processClaims.m, key)
	processClaims.Unlock()
}

type walStoreLock struct {
	file *os.File
	path string
	key  [2]uint64 // (dev, ino) of the lock FILE
}

// takeStoreLock acquires the two-sided store lock or panics *DBError. The
// shared/exclusive split matters: a reader must be refused while a writer
// holds the exclusive lock AND a writer while a reader holds a shared one, so
// a read-only open still creates the lock file when the directory allows it.
// On a positively read-only medium (lock file absent, directory not writable)
// the open proceeds lockless — Java's read-only-medium branch.
func takeStoreLock(base, dir string, readOnly bool) *walStoreLock {
	lockPath := base + ".lock"
	var f *os.File
	if readOnly {
		var err error
		f, err = os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o666)
		if err != nil {
			if _, statErr := os.Lstat(lockPath); statErr == nil {
				// The file is there: a shared lock is still attainable on a
				// read-only handle. Not a fallback to lockless.
				f, err = os.Open(lockPath)
				if err != nil {
					panic(walIOErr(err))
				}
			} else if !isWritableDir(dir) {
				return nil // read-only medium: no writer can take the lock either
			} else {
				panic(walLocked(fmt.Sprintf(
					"cannot take a shared store lock on %s and the directory is writable, "+
						"so a writer may be running (%v)", lockPath, err)))
			}
		}
	} else {
		var err error
		// Missing parent directories are created — Java does the same on open.
		f, err = os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o666)
		if err != nil && errors.Is(err, fs.ErrNotExist) {
			if mkErr := os.MkdirAll(dir, 0o777); mkErr != nil {
				panic(walIOErr(mkErr))
			}
			f, err = os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o666)
		}
		if err != nil {
			panic(walIOErr(err))
		}
	}
	// Identity of the lock FILE, not the path used to reach it: two opens
	// naming the same store through different paths must collide.
	key, err := lockIdentity(f)
	if err != nil {
		f.Close()
		panic(walIOErr(err))
	}
	// The in-process claim runs first and refuses ANY overlap, whatever the
	// mode of either open — two read-only opens of one store are refused too.
	if !claimLock(key, lockPath) {
		f.Close()
		panic(walLocked(fmt.Sprintf("WAL store %s is already open in this process", base)))
	}
	locked, err := tryOFDLock(f, !readOnly)
	if err != nil {
		releaseClaim(key)
		f.Close()
		panic(walIOErr(err))
	}
	if !locked {
		releaseClaim(key)
		f.Close()
		panic(walLocked(fmt.Sprintf("WAL store %s is locked by another process", base)))
	}
	return &walStoreLock{file: f, path: lockPath, key: key}
}

func (l *walStoreLock) release() {
	if l == nil {
		return
	}
	// File first, then the claim — no window in which this process has
	// forgotten the store while the kernel still holds its lock.
	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
	releaseClaim(l.key)
}

// ---------- small I/O helpers ----------

// writeAllAt keeps writing until done — WriteAt may legally return a short
// count, which is a partial write, not a complete one.
func writeAllAt(f *os.File, b []byte, off int64) error {
	for len(b) > 0 {
		n, err := f.WriteAt(b, off)
		if err != nil {
			return err
		}
		b = b[n:]
		off += int64(n)
	}
	return nil
}

// readAtOpt reads exactly len(b); false means the file was shorter than the
// read demands — recovery treats that exactly as a torn tail.
func readAtOpt(f *os.File, b []byte, off int64) (bool, error) {
	_, err := f.ReadAt(b, off)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return false, nil
	default:
		return false, err
	}
}

func mustIO(err error) {
	if err != nil {
		panic(walIOErr(err))
	}
}

// fsyncDir persists a directory change (create/unlink).
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// ---------- the segment ----------

// walSegment is one log file: its validated header, the recovery scan's
// results (first/last LSN, the offset the valid prefix extends to, one HELD
// verdict) and a lazily-opened file handle.
type walSegment struct {
	seq      int64
	path     string
	readOnly bool
	file     *os.File // opened on demand, released when a pass is done
	header   [segHdr]byte
	fileLen  int64
	validEnd int64
	firstLsn int64
	lastLsn  int64
	held     string // first held verdict; "" = none
}

func (s *walSegment) crcDomain(h *crc32Digest, off int64) {
	crcDomainOf(h, &s.header, off)
}

func (s *walSegment) empty() bool { return s.validEnd == segHdr }

func (s *walSegment) headerFirstLsn() int64 {
	return int64(binary.BigEndian.Uint64(s.header[24:32]))
}

func (s *walSegment) ensureOpen() {
	if s.file != nil {
		return
	}
	var f *os.File
	var err error
	if s.readOnly {
		f, err = os.OpenFile(s.path, os.O_RDONLY, 0)
	} else {
		f, err = os.OpenFile(s.path, os.O_RDWR, 0)
	}
	if err != nil {
		panic(walIOErr(err))
	}
	s.file = f
}

// segFile is the open handle, panicking if it was released mid-pass — a
// sequencing bug in this module, not corruption.
func (s *walSegment) segFile() *os.File {
	if s.file == nil {
		panic("WAL segment handle released mid-pass: caller must ensureOpen")
	}
	return s.file
}

func (s *walSegment) release() {
	if s.file != nil {
		s.file.Close()
		s.file = nil
	}
}

// ---------- the segment set ----------

// walSegmentSet is the discovered, sorted segment set plus the store lock,
// which is held from open until close — cleanup between them never drops it.
type walSegmentSet struct {
	base      string // absolute
	dir       string
	prefix    string // "<abs-base>.wal." — full path prefix, for building paths
	namePfx   string // "<base>.wal." — file-NAME prefix, for enumeration
	segments  []*walSegment
	lock      *walStoreLock // nil only in the read-only-medium case
	readOnly  bool
	io        walIO
	closed    bool
	nextSeq   int64 // one above the highest name seen, residue included (W6)
	sealedLen int64 // fileLen of every segment except the highest (O(1) logBytes)
	dirFsyncs uint64
	segSyncs  uint64
}

// openWalSegmentSet acquires the lock, enforces the legacy boundary,
// enumerates, classifies (R0/R1) and removes create-crash residue (R2).
// Panics *DBError on any failure and releases the lock as it unwinds.
func openWalSegmentSet(basePath string, readOnly bool, io walIO) *walSegmentSet {
	abs, err := filepath.Abs(basePath)
	if err != nil {
		panic(walIOErr(err))
	}
	set := &walSegmentSet{
		base:     abs,
		dir:      filepath.Dir(abs),
		prefix:   abs + ".wal.",
		namePfx:  filepath.Base(abs) + ".wal.",
		readOnly: readOnly,
		io:       io,
		nextSeq:  firstSeq,
	}
	defer func() {
		if p := recover(); p != nil {
			set.close()
			panic(p)
		}
	}()
	set.lock = takeStoreLock(abs, set.dir, readOnly)

	// D1, the legacy boundary: three pre-existing artifacts REFUSE rather
	// than being ignored, and none is ever deleted — a regular file at
	// `<base>.wal` (Java's N6), a regular file at `<base>` itself (a v1 caller
	// passed the WAL FILE path, so the same call site now passes a BASE), and
	// `<base>.ckpt`, which refuses on EXISTENCE whatever it is, because after
	// a v1 crash it may be the only recoverable copy.
	for _, row := range []struct {
		path    string
		what    string
		anyKind bool
	}{
		{abs + ".wal", "v1 single-file WAL", false},
		{abs, "regular file at the WAL base path (the v3 opener takes a base, not a log file)", false},
		{abs + ".ckpt", "v1 checkpoint temp, possibly the only recoverable copy after a v1 crash", true},
	} {
		var present bool
		if row.anyKind {
			_, e := os.Lstat(row.path)
			present = e == nil
		} else {
			present = isRegularFile(row.path)
		}
		if present {
			panic(walCorrupt(fmt.Sprintf(
				"%s present at %s: no migration to v3 — open it with the release that "+
					"wrote it and copy the data across, or move it aside", row.what, row.path)))
		}
	}
	set.classify(set.enumerate())
	return set
}

func isRegularFile(path string) bool {
	fi, err := os.Lstat(path)
	return err == nil && fi.Mode().IsRegular()
}

// R0/N4: every regular file named `<base>.wal.<16 lowercase hex>` with a
// non-negative i64 value. Directories, symlinks, uppercase hex, wrong lengths
// and the `.lock` file are ignored — a store directory may contain other
// things. Sequence GAPS are legal.
//
// Open is lenient: an unreadable/absent directory reads as "no segments" and
// a fresh store then surfaces the real problem on create.
func (s *walSegmentSet) enumerate() []int64 {
	found, err := s.enumerateChecked()
	if err != nil {
		return nil
	}
	return found
}

// enumerateChecked without the leniency: an unreadable directory is an
// ERROR, not an empty namespace — the one caller that deletes must not guess.
func (s *walSegmentSet) enumerateChecked() ([]int64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, walIOErr(err)
	}
	var found []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, s.namePfx) {
			continue
		}
		suffix := name[len(s.namePfx):]
		if len(suffix) != 16 || !isLowerHex(suffix) {
			continue
		}
		// file_type does not follow symlinks, so a symlink to a valid segment
		// is not a segment.
		if !e.Type().IsRegular() {
			continue
		}
		seq, err := strconv.ParseUint(suffix, 16, 63)
		if err != nil {
			continue // value >= 2^63 (negative as i64) is not a segment
		}
		found = append(found, int64(seq))
	}
	sort.Slice(found, func(i, j int) bool { return found[i] < found[j] })
	return found, nil
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (s *walSegmentSet) segmentPath(seq int64) string {
	return s.prefix + fmt.Sprintf("%016x", seq)
}

// ---------- table H ----------

const (
	hdrOK = iota
	hdrTorn
	hdrCorrupt
)

// readSegHeader validates one segment header; returns (verdict, fault).
// H1-H4 are the torn-create shapes; H5-H7/H9 are CRC-valid wrong content —
// corruption wherever they appear.
func readSegHeader(f *os.File, length int64, hdr *[segHdr]byte, nameSeq int64) (int, string) {
	if length == 0 {
		return hdrTorn, "empty segment file" // H1
	}
	if length < segHdr {
		return hdrTorn, fmt.Sprintf("segment header truncated at %d bytes", length) // H2
	}
	var read int
	for read < segHdr {
		n, err := f.ReadAt(hdr[read:], int64(read))
		if err != nil && !errors.Is(err, io.EOF) {
			panic(walIOErr(err))
		}
		if n == 0 {
			return hdrTorn, "segment header short read"
		}
		read += n
	}
	if int32(crc32.ChecksumIEEE(hdr[:segHdrCRCLen])) != int32(binary.BigEndian.Uint32(hdr[segHdrCRCLen:])) {
		return hdrTorn, "segment header CRC mismatch" // H3
	}
	if string(hdr[:8]) != string(walMagic) {
		return hdrTorn, "not a mapdb WAL segment" // H4
	}
	if v := int32(binary.BigEndian.Uint32(hdr[8:12])); v != walFormatVersion {
		return hdrCorrupt, fmt.Sprintf("unsupported WAL format version %d", v) // H5
	}
	if fl := int32(binary.BigEndian.Uint32(hdr[12:16])); fl != 0 {
		return hdrCorrupt, fmt.Sprintf("unknown segment flags %d", fl) // H6
	}
	if seq := int64(binary.BigEndian.Uint64(hdr[16:24])); seq != nameSeq {
		return hdrCorrupt, fmt.Sprintf("header sequence %d does not match its name", seq) // H7
	}
	if fl := int64(binary.BigEndian.Uint64(hdr[24:32])); fl <= 0 {
		return hdrCorrupt, fmt.Sprintf("header firstLsn %d is not a valid LSN", fl) // H9
	}
	return hdrOK, ""
}

// R1 then R2. Applies table H to every enumerated name, unlinks create-crash
// residue, and records the maximum sequence over ALL names — including the
// residue it removes (W6), so a stale directory entry can never alias a
// segment a later create reuses.
func (s *walSegmentSet) classify(found []int64) {
	var maxObserved int64
	for _, seq := range found {
		if seq > maxObserved {
			maxObserved = seq
		}
	}
	if maxObserved == int64(^uint64(0)>>1) { // MaxInt64
		panic(mapdb5.New(mapdb5.KindStoreFull, "WAL segment sequence space exhausted"))
	}
	s.nextSeq = maxObserved + 1

	var highest int64 = -1
	if n := len(found); n > 0 {
		highest = found[n-1]
	}
	var residue []int64
	for _, seq := range found {
		// Sequence 0 is reserved for "no clean mark" — no conforming writer
		// can ever create it.
		if seq == 0 {
			panic(walCorrupt(fmt.Sprintf(
				"WAL segment %s: sequence 0 is reserved for \"no clean mark\" and is never a segment",
				filepath.Base(s.segmentPath(0)))))
		}
		path := s.segmentPath(seq)
		var f *os.File
		var err error
		if s.readOnly {
			f, err = os.OpenFile(path, os.O_RDONLY, 0)
		} else {
			f, err = os.OpenFile(path, os.O_RDWR, 0)
		}
		if err != nil {
			panic(walIOErr(err))
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			panic(walIOErr(err))
		}
		var hdr [segHdr]byte
		verdict, fault := readSegHeader(f, st.Size(), &hdr, seq)
		f.Close() // the handle is NOT retained: passes reopen on demand
		switch verdict {
		case hdrOK:
			s.segments = append(s.segments, &walSegment{
				seq: seq, path: path, readOnly: s.readOnly,
				header: hdr, fileLen: st.Size(), validEnd: segHdr,
			})
		case hdrCorrupt:
			panic(walCorrupt(fmt.Sprintf("WAL segment %s: %s", filepath.Base(path), fault)))
		case hdrTorn:
			// Only a highest-name file no longer than the header can be a torn
			// create; a longer invalid-header file may hold committed data.
			if seq == highest && st.Size() <= segHdr {
				residue = append(residue, seq)
			} else {
				why := "not the highest segment, so its create completed"
				if seq == highest {
					why = "longer than the segment header, so it may contain committed sections"
				}
				panic(walCorrupt(fmt.Sprintf("WAL segment %s: %s (%s)",
					filepath.Base(path), fault, why)))
			}
		}
	}
	if !s.readOnly && len(residue) > 0 {
		for _, seq := range residue {
			walIOReport(s.io, walOpUnlink, seq, 0, 0, 0)
			if err := os.Remove(s.segmentPath(seq)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				panic(walIOErr(err))
			}
		}
		s.fsyncDir()
	}
	s.recomputeSealed()
}

// ---------- the namespace mutations ----------

// W2: create → write header → force(true) → fsync the directory, and only
// then may a section be appended. Returns the new active segment.
func (s *walSegmentSet) createSegment(firstLsn int64) *walSegment {
	if s.closed {
		panic(mapdb5.New(mapdb5.KindClosed, "store closed"))
	}
	if s.readOnly {
		panic(mapdb5.New(mapdb5.KindReadOnly, "WAL store opened read-only"))
	}
	if firstLsn <= 0 {
		panic(walWrongConfig(fmt.Sprintf("segment firstLsn must be positive: %d", firstLsn)))
	}
	seq := s.nextSeq
	// The name is burned here, BEFORE any I/O (W6) — a failed create must
	// never hand its sequence number to the next one.
	if seq == int64(^uint64(0)>>1) {
		panic(mapdb5.New(mapdb5.KindStoreFull, "WAL segment sequence space exhausted"))
	}
	s.nextSeq = seq + 1
	path := s.segmentPath(seq)
	hdr := buildSegHeader(seq, firstLsn)

	walIOReport(s.io, walOpCreate, seq, 0, 0, 0)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		panic(walIOErr(err))
	}
	// Panic-safe cleanup: the injected walIOReport calls panic rather than
	// returning, so error-return cleanup alone would leak the descriptor and
	// leave the partial file behind.
	keep := false
	defer func() {
		if !keep {
			f.Close()
			os.Remove(path)
		}
	}()

	walIOReport(s.io, walOpSegHeader, seq, 0, segHdr, 0)
	mustIO(writeAllAt(f, hdr[:], 0))
	walIOReport(s.io, walOpForceFull, seq, segHdr, 0, 0)
	// The file's SIZE is part of the payload here: never a data sync.
	mustIO(f.Sync())
	s.segSyncs++
	walIOReport(s.io, walOpDirSync, 0, 0, 0, 0)
	mustIO(fsyncDir(s.dir))
	s.dirFsyncs++

	keep = true
	f.Close() // the segment reopens on demand, like every other one

	// The segment this one displaces stops growing here: its length joins the
	// sealed total.
	if n := len(s.segments); n > 0 {
		s.sealedLen += s.segments[n-1].fileLen
	}
	seg := &walSegment{
		seq: seq, path: path, readOnly: s.readOnly,
		header: hdr, fileLen: segHdr, validEnd: segHdr,
	}
	s.segments = append(s.segments, seg)
	return seg
}

// W5: unlink every segment at or below throughSeq, then fsync the directory.
// Called only after the 'K' authorizing it is forced. A failed unlink is a
// leak the next open retries, never permission to advance an unproven mark.
func (s *walSegmentSet) unlinkThrough(throughSeq int64) {
	if s.closed {
		panic(mapdb5.New(mapdb5.KindClosed, "store closed"))
	}
	if s.readOnly || throughSeq <= 0 {
		return
	}
	n := 0
	for n < len(s.segments) && s.segments[n].seq <= throughSeq {
		n++
	}
	if n == 0 {
		return // no delete and NO directory fsync
	}
	// Drop them from the live set BEFORE any delete can fail: their handles
	// are released here, so a failed unlink must not leave a closed-but-listed
	// segment behind for some later reader to use.
	retiring := s.segments[:n]
	s.segments = append([]*walSegment{}, s.segments[n:]...)
	s.recomputeSealed()
	for _, seg := range retiring {
		seg.release()
		walIOReport(s.io, walOpUnlink, seg.seq, 0, 0, 0)
		if err := os.Remove(seg.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			panic(walIOErr(err))
		}
	}
	s.fsyncDir()
}

func (s *walSegmentSet) fsyncDir() {
	walIOReport(s.io, walOpDirSync, 0, 0, 0, 0)
	if err := fsyncDir(s.dir); err != nil {
		panic(walIOErr(err))
	}
	s.dirFsyncs++
}

func (s *walSegmentSet) recomputeSealed() {
	s.sealedLen = 0
	for i := 0; i+1 < len(s.segments); i++ {
		s.sealedLen += s.segments[i].fileLen
	}
}

// logBytes is what the log costs on the device. O(1).
func (s *walSegmentSet) logBytes() int64 {
	if len(s.segments) == 0 {
		return 0
	}
	return s.sealedLen + s.segments[len(s.segments)-1].fileLen
}

func (s *walSegmentSet) active() *walSegment {
	if len(s.segments) == 0 {
		return nil
	}
	return s.segments[len(s.segments)-1]
}

// D2's lock-owning namespace cleanup: delete every file this base owns — the
// segments by the same enumeration rule the open used, plus `<base>.lock` —
// then fsync once and release the lock. Runs WHILE THE LOCK IS STILL HELD.
func (s *walSegmentSet) deleteNamespace() {
	if s.closed {
		panic(mapdb5.New(mapdb5.KindClosed, "store closed"))
	}
	if s.readOnly {
		panic(mapdb5.New(mapdb5.KindReadOnly, "WAL store opened read-only"))
	}
	// RE-enumerated, checked: the live list holds what recovery retained, and
	// the directory may hold names this open left behind. An unreadable
	// directory is an error, never an empty namespace.
	found, err := s.enumerateChecked()
	if err != nil {
		panic(err)
	}
	for _, seq := range found {
		walIOReport(s.io, walOpUnlink, seq, 0, 0, 0)
		if err := os.Remove(s.segmentPath(seq)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			panic(walIOErr(err))
		}
	}
	s.segments = nil
	s.sealedLen = 0
	// The lock file is unlinked while its lock is still held, so no opener
	// can be between "created the inode" and "locked it" for THIS pathname.
	if s.lock != nil {
		if err := os.Remove(s.lock.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			panic(walIOErr(err))
		}
	}
	s.fsyncDir()
	s.close()
}

// close releases every segment handle, then the lock file, then the claim —
// in that order. Idempotent.
func (s *walSegmentSet) close() {
	if s.closed {
		return
	}
	s.closed = true
	for _, seg := range s.segments {
		seg.release()
	}
	s.segments = nil
	if s.lock != nil {
		s.lock.release()
		s.lock = nil
	}
}

// ---------- the codec shared with wal_recover.go ----------

// crc32Digest wraps hash/crc32 for the domain-separated checksums.
type crc32Digest struct {
	h interface {
		Write([]byte) (int, error)
		Sum32() uint32
	}
}

func newCRC() *crc32Digest { return &crc32Digest{h: crc32.NewIEEE()} }
func (c *crc32Digest) write(b []byte) {
	_, _ = c.h.Write(b) // hash.Hash Write never errors
}
func (c *crc32Digest) sum() int32 { return int32(c.h.Sum32()) }

// crcDomainOf seeds a hasher with the segment's 36 header bytes and the
// section offset — the v3 CRC domain binding a section to its segment+offset.
func crcDomainOf(h *crc32Digest, segHeader *[segHdr]byte, off int64) {
	h.write(segHeader[:])
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(off))
	h.write(b[:])
}

// buildSegHeader produces a segment's 36-byte header.
func buildSegHeader(seq, firstLsn int64) [segHdr]byte {
	var h [segHdr]byte
	copy(h[:8], walMagic)
	binary.BigEndian.PutUint32(h[8:12], walFormatVersion)
	binary.BigEndian.PutUint32(h[12:16], 0)
	binary.BigEndian.PutUint64(h[16:24], uint64(seq))
	binary.BigEndian.PutUint64(h[24:32], uint64(firstLsn))
	binary.BigEndian.PutUint32(h[32:36], crc32.ChecksumIEEE(h[:segHdrCRCLen]))
	return h
}
