package store

// StoreWAL — the transactional store (spec 02 §7): an in-memory StoreDirect
// image kept durable by a write-ahead log. Every mutation is staged; a commit
// writes one forced section and only then applies it to the inner store.
//
// Port of Java StoreWAL + the Rust wal.rs, over the v3 segment-set namespace
// (wal_segments.go), the two-pass section writer (wal_write.go) and the
// recovery state machine (wal_recover.go).

import (
	"iter"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// tTransient is not an entry type: "created and deleted inside one
// transaction", which is applied (the preallocated recid is freed) and never
// logged.
const tTransient = 0

const (
	defaultReplayBuf          int64 = 1 << 20
	defaultRecoveryIndexBytes int64 = 64 << 20

	// DefaultSegmentBytes: the writer seals and rolls PAST this, at a section
	// boundary, so one section may exceed it and an oversize section gets a
	// segment to itself.
	DefaultSegmentBytes int64 = 64 << 20
	// MinSegmentBytes is the smallest legal segment size: a header plus one
	// section header.
	MinSegmentBytes int64 = segHdr + secHdr
	// DefaultMinLogBytes is the floor under the cleaning trigger: a log
	// smaller than this is never cleaned, however small the live data.
	DefaultMinLogBytes int64 = 1 << 30
	// DefaultSpaceAmplification: clean once the log exceeds this multiple of
	// the live data. It bounds SPACE, not write amplification.
	DefaultSpaceAmplification int64 = 2
)

// walStaged is the per-recid staged mutation set (uncommitted). Content =
// (base or inner) ++ appends.
type walStaged struct {
	created    bool
	baseSet    bool
	base       []byte // nil with baseSet means explicit null content
	headroom   int
	deleted    bool
	appends    [][]byte
	appendsLen int
}

// walOp is a classified commit operation, computed before any apply (state
// must not shift mid-apply).
type walOp struct {
	op       int    // one of t*, or tTransient
	recid    uint64 // plain recid
	capBytes int64
	data     []byte
	hasData  bool
	baseLsn  int64 // tAppend only: LSN of the content image this delta extends
}

// walOptions carries the open-time knobs; zero fields take defaults.
type walOptions struct {
	threadSafe         bool
	readOnly           bool
	segmentBytes       int64
	replayBuf          int64
	recoveryIndexBytes int64
	io                 walIO
}

func (o *walOptions) norm() {
	if o.segmentBytes == 0 {
		o.segmentBytes = DefaultSegmentBytes
	}
	if o.replayBuf == 0 {
		o.replayBuf = defaultReplayBuf
	}
	if o.recoveryIndexBytes == 0 {
		o.recoveryIndexBytes = defaultRecoveryIndexBytes
	}
}

// walBudget is one cleaning tick's allowance; 0 means "no limit" in every
// field (what an explicit Checkpoint runs under).
type walBudget struct {
	maxRecords int
	maxBytes   int64
	maxNanos   int64
}

// foregroundBudget is the budget a COMMIT pays (Java's measured values).
var foregroundBudget = walBudget{maxRecords: 256, maxBytes: 512 << 10, maxNanos: 500_000}

var unboundedBudget = walBudget{}

// entries one scan unit walks when the budget names no record limit
const scanUnitEntries = 256

// scanBuf is the window for the two cleaner scans — small on purpose: they
// read entry HEADERS and seek over payloads.
const scanBuf = 4096

// cycleWidthCap bounds a cycle's width: a cycle's CLOSE is not budgeted —
// summing the retiring prefix, then closing, deleting and fsyncing every file
// in it, all under the write lock.
const cycleWidthCap = 64

// walState is the lock-guarded mutable state (Java's single ReadWriteLock
// covers all of it).
type walState struct {
	inner      *StoreDirect
	segs       *walSegmentSet
	staged     map[uint64]*walStaged
	nextLsn    int64
	segBytes   int64
	minLog     int64
	spaceAmp   int64
	readOnly   bool
	ids        *walIdentities
	io         walIO
	closedFlag *atomic.Bool

	committedStateChanges int64

	// ---- the cleaner ----
	cleaner            *walCleaner
	cleanFloorSeq      int64
	episodeRetired     int64
	episodeWritten     int64
	episodeRecords     int64
	episodeSegments    int
	cycleWidth         int
	cycleRetiredAt     int64
	cycleWrittenAt     int64
	cycleSaturated     bool
	lastCycleSaturated bool
	futileAtBytes      int64
	futileAtTarget     int64
	futileAtChanges    int64
	futileRecords      int64
	cleanerWritten     int64
	cleanerRetired     int64
	foreground         walBudget
}

var (
	_ Store      = (*StoreWAL)(nil)
	_ StoreTx    = (*StoreWAL)(nil)
	_ StoreDelta = (*StoreWAL)(nil)
)

// StoreWAL is the transactional store.
type StoreWAL struct {
	mu            sync.RWMutex // guards walState (Java's single ReadWriteLock)
	st            *walState
	innerStats    *StoreDirect // same store as st.inner, for size reads outside st
	base          string
	lease         *LeaseTable
	closed        atomic.Bool
	structGen     atomic.Uint64
	deleteOnClose atomic.Bool
	threadSafe    bool
}

// OpenStoreWAL opens the WAL store at base; its segments are
// `<base>.wal.<16 hex>`. Errors (lock held, corruption, I/O) are returned,
// not panicked.
func OpenStoreWAL(base string, threadSafe bool) (*StoreWAL, error) {
	return openStoreWALCfg(base, walOptions{threadSafe: threadSafe})
}

// OpenStoreWALRO opens the store read-only.
func OpenStoreWALRO(base string, threadSafe bool) (*StoreWAL, error) {
	return openStoreWALCfg(base, walOptions{threadSafe: threadSafe, readOnly: true})
}

// OpenStoreWALSegmentBytes opens with a non-default segment size.
func OpenStoreWALSegmentBytes(base string, threadSafe bool, segmentBytes int64) (*StoreWAL, error) {
	return openStoreWALCfg(base, walOptions{threadSafe: threadSafe, segmentBytes: segmentBytes})
}

// OpenStoreWALRecoveryLimit opens with an explicit ceiling for the dense
// in-memory index built by WAL recovery.
func OpenStoreWALRecoveryLimit(base string, threadSafe bool, maxIndexBytes int64) (*StoreWAL, error) {
	return openStoreWALCfg(base, walOptions{threadSafe: threadSafe, recoveryIndexBytes: maxIndexBytes})
}

func openStoreWALCfg(base string, opts walOptions) (s *StoreWAL, err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				err = de
			} else {
				panic(r)
			}
		}
	}()
	opts.norm()
	if opts.recoveryIndexBytes < pageSize {
		return nil, mapdb5.New(mapdb5.KindStoreFull, "recovery index limit below one page")
	}
	if opts.segmentBytes < MinSegmentBytes {
		return nil, walWrongConfig("WAL segment size is below the " +
			"minimum (a segment header plus one section header)")
	}
	inner := NewStoreDirectHeap(opts.threadSafe)
	segs := openWalSegmentSet(base, opts.readOnly, opts.io)
	var rec *walRecovered
	func() {
		defer func() {
			if p := recover(); p != nil {
				// A failed recovery drops the set, releasing the store lock —
				// Java's `finally { closeQuietly() }`.
				segs.close()
				inner.Close()
				panic(p)
			}
		}()
		rec = walRecover(segs, inner, opts.replayBuf, opts.recoveryIndexBytes)
	}()
	w := &StoreWAL{
		st: &walState{
			inner: inner, segs: segs, staged: map[uint64]*walStaged{},
			nextLsn: rec.nextLsn, segBytes: opts.segmentBytes,
			minLog: DefaultMinLogBytes, spaceAmp: DefaultSpaceAmplification,
			readOnly: opts.readOnly, ids: rec.identities, io: opts.io,
			cycleWidth: 1, foreground: foregroundBudget,
		},
		innerStats: inner, base: base,
		lease: NewLeaseTable(), threadSafe: opts.threadSafe,
	}
	w.st.closedFlag = &w.closed
	return w, nil
}

func (s *StoreWAL) LeaseTable() *LeaseTable { return s.lease }

// readOpen takes the read guard and re-checks closed under it — close
// publishes `closed` under the WRITE lock, so a check taken before acquiring
// the read lock can be overtaken.
func (s *StoreWAL) readOpen() *walState {
	s.mu.RLock()
	if s.closed.Load() {
		s.mu.RUnlock()
		panic(mapdb5.New(mapdb5.KindClosed, "store closed"))
	}
	return s.st
}

func (s *StoreWAL) readOpenEnd() { s.mu.RUnlock() }

// writeOpen takes the write guard and re-checks closed and read-only under
// it — every write path is linearized with close this way.
func (s *StoreWAL) writeOpen() *walState {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		panic(mapdb5.New(mapdb5.KindClosed, "store closed"))
	}
	if s.st.readOnly {
		s.mu.Unlock()
		panic(mapdb5.New(mapdb5.KindReadOnly, "WAL store opened read-only"))
	}
	return s.st
}

func (s *StoreWAL) writeOpenEnd() { s.mu.Unlock() }

// LogBytes is what the log currently costs on the device.
func (s *StoreWAL) LogBytes() int64 {
	st := s.readOpen()
	defer s.readOpenEnd()
	return st.segs.logBytes()
}

// SetMinLogBytes sets the floor under the cleaning trigger.
func (s *StoreWAL) SetMinLogBytes(bytes int64) {
	st := s.writeOpen()
	defer s.writeOpenEnd()
	st.minLog = bytes
	// A configuration change invalidates every observation the current
	// episode made, latch included.
	st.abandonEpisode()
}

// SetSegmentBytes sets the size past which the writer seals the active
// segment and rolls to the next one. Rollover is always at a section
// boundary, so a single section larger than this gets a segment of its own.
func (s *StoreWAL) SetSegmentBytes(bytes int64) {
	if bytes < MinSegmentBytes {
		panic(walWrongConfig("WAL segment size is below the minimum (a segment header plus one section header)"))
	}
	st := s.writeOpen()
	defer s.writeOpenEnd()
	st.segBytes = bytes
}

// SetSpaceAmplification sets the space-amplification target: clean once the
// log exceeds this multiple of the live data.
func (s *StoreWAL) SetSpaceAmplification(factor int64) {
	if factor < 1 {
		panic(walWrongConfig("space amplification must be at least 1"))
	}
	st := s.writeOpen()
	defer s.writeOpenEnd()
	st.spaceAmp = factor
	st.abandonEpisode()
}

// SetDeleteOnClose deletes this base's whole segment namespace inside Close,
// while the store lock is still held (D2).
func (s *StoreWAL) SetDeleteOnClose(delete bool) { s.deleteOnClose.Store(delete) }

// Checkpoint cleans the log all the way down: retire every segment below a
// freshly rolled one by re-emitting, above them, a self-contained image of
// every record they still own, then a forced 'K' mark authorizing their
// removal, then the unlink. Staged (uncommitted) mutations are untouched.
func (s *StoreWAL) Checkpoint() {
	st := s.writeOpen()
	defer s.writeOpenEnd()
	st.cleanWholeLog()
}

// CleaningExhausted reports whether the futility latch is armed: the log is
// fully compacted and still cannot meet the configured ratio.
func (s *StoreWAL) CleaningExhausted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.futileAtBytes > 0
}

// CleanerBytes is the lifetime cleaner accounting: (re-emitted, retired).
func (s *StoreWAL) CleanerBytes() (int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.cleanerWritten, s.st.cleanerRetired
}

// NextLsn is the LSN the next section will carry.
func (s *StoreWAL) NextLsn() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.nextLsn
}

// SegmentSeqs are the live segments' sequence numbers, ascending.
func (s *StoreWAL) SegmentSeqs() []int64 {
	st := s.readOpen()
	defer s.readOpenEnd()
	out := make([]int64, len(st.segs.segments))
	for i, seg := range st.segs.segments {
		out[i] = seg.seq
	}
	return out
}

// ---------- merge / stage ----------

// merged returns (staged base or inner content) ++ staged appends;
// hasContent=false means null.
func (st *walState) merged(recid uint64, s *walStaged) ([]byte, bool) {
	var base []byte
	hasBase := false
	if s.baseSet {
		base = s.base
		hasBase = s.base != nil
	} else if st.inner.recState(recid) == stateLive {
		base = st.inner.rawGet(recid)
		hasBase = base != nil
	}
	if !hasBase && len(s.appends) == 0 {
		return nil, false
	}
	m := make([]byte, 0, len(base)+s.appendsLen)
	m = append(m, base...)
	for _, a := range s.appends {
		m = append(m, a...)
	}
	return m, true
}

// stagedForWrite returns the staged entry for a write, establishing GetVoid
// on deleted/void recids.
func (st *walState) stagedForWrite(recid uint64) *walStaged {
	if s, ok := st.staged[recid]; ok {
		if s.deleted {
			panic(mapdb5.NewGetVoid(recid))
		}
		return s
	}
	if st.inner.recState(recid) == stateVoid {
		panic(mapdb5.NewGetVoid(recid))
	}
	s := &walStaged{}
	st.staged[recid] = s
	return s
}

// classify turns the staged set into the ops one commit section will carry.
// Runs BEFORE any apply, because applying shifts the inner store's state.
func (st *walState) classify() []walOp {
	recids := make([]uint64, 0, len(st.staged))
	for r := range st.staged {
		recids = append(recids, r)
	}
	sortUint64s(recids)
	ops := make([]walOp, 0, len(recids))
	for _, recid := range recids {
		s := st.staged[recid]
		switch {
		case s.deleted:
			// created+deleted in one transaction: apply-only cleanup, not
			// logged.
			op := tDelete
			if s.created {
				op = tTransient
			}
			ops = append(ops, walOp{op: op, recid: recid})
		case !s.baseSet && len(s.appends) == 0:
			// T_PREALLOC makes a NEWLY ALLOCATED recid durable; on a committed
			// record an empty staged entry means nothing changed.
			if s.created {
				ops = append(ops, walOp{op: tPrealloc, recid: recid})
			}
		case s.baseSet || st.inner.recState(recid) != stateLive:
			m, has := st.merged(recid, s)
			ops = append(ops, walOp{op: tRecord, recid: recid,
				capBytes: walRecordCap(m, has, s.headroom), data: m, hasData: has})
		default:
			// Live plain base in inner: log only the appended tail.
			var m []byte
			for _, a := range s.appends {
				m = append(m, a...)
			}
			// This branch is reached only when the record is committed,
			// content-bearing and plain — exactly the shape that has a content
			// base. Its absence is a writer bug, and a fabricated base is a
			// silent-loss channel.
			baseLsn, ok := st.ids.contentBaseLsn[recid]
			if !ok {
				panic(mapdb5.New(mapdb5.KindMisuse,
					"no content base LSN for appended recid"))
			}
			ops = append(ops, walOp{op: tAppend, recid: recid, data: m, hasData: true, baseLsn: baseLsn})
		}
	}
	return ops
}

// reserveLsn is the LSN the next section may use, refusing BEFORE anything is
// written when the space is exhausted.
func (st *walState) reserveLsn() (lsn, after int64) {
	lsn = st.nextLsn
	if lsn == int64(^uint64(0)>>1) {
		panic(mapdb5.New(mapdb5.KindStoreFull, "WAL LSN space exhausted"))
	}
	return lsn, lsn + 1
}

func (st *walState) commitLocked() {
	if len(st.staged) == 0 {
		return
	}
	ops := st.classify()
	sectionLsn, lsnAfter := st.reserveLsn()
	// Validated BEFORE appendSection can roll over or write.
	for _, op := range ops {
		if op.op == tAppend && sectionLsn <= op.baseLsn {
			panic(mapdb5.New(mapdb5.KindMisuse,
				"append base LSN is not below its section LSN"))
		}
	}
	// The emitter runs TWICE (measure + write) over this immutable ops
	// snapshot, so a commit staging more than 2 GiB emits one huge section
	// instead of dying in a doubling buffer.
	func() {
		defer func() {
			if p := recover(); p != nil {
				// W9: a failed or partial write/force fails the store CLOSED.
				st.failClosed()
				panic(p)
			}
		}()
		appendSection(st.segs, st.segBytes, st.io, st.closedFlag, tagSection, sectionLsn,
			func(sink *bodySink) { emitWalEntries(ops, sectionLsn, sink) })
	}()
	st.nextLsn = lsnAfter
	// The cleaner's staleness clock: SELF-CONTAINED entries only.
	for _, op := range ops {
		if op.op == tRecord || op.op == tDelete || op.op == tPrealloc {
			st.committedStateChanges++
		}
	}
	// Apply to the inner volume. PAST THE DURABILITY POINT: if any apply
	// fails, memory and log have diverged and this handle can never be made
	// consistent — fail closed; the durable state is intact and reopen
	// replays it.
	func() {
		defer func() {
			if p := recover(); p != nil {
				st.failClosed()
				panic(walCorrupt("WAL commit failed after the durability point; store closed, reopen to recover the committed section"))
			}
		}()
		st.applyCommitted(ops, sectionLsn)
	}()
	st.staged = map[uint64]*walStaged{}
	// Inline cleaning runs after the section is forced, applied and no longer
	// staged. On failure the store is closed: the transaction already
	// committed, so an error return with this handle open would invite a
	// retry that replays it twice.
	func() {
		defer func() {
			if p := recover(); p != nil {
				st.failClosed()
				panic(p)
			}
		}()
		st.autoCleanLocked()
	}()
}

// applyCommitted applies one committed section's ops and moves the
// identities by the SAME §4.2 transition row replay would take.
func (st *walState) applyCommitted(ops []walOp, sectionLsn int64) {
	for _, op := range ops {
		switch op.op {
		case tTransient:
			st.inner.Delete(op.recid, nil) // created+deleted: free the P recid
			st.ids.void(op.recid)
		case tPrealloc:
			st.ids.stateOnly(op.recid, sectionLsn) // already P in inner
		case tRecord:
			st.inner.walPut(op.recid, op.capBytes, op.data, op.hasData)
			if op.hasData {
				st.ids.content(op.recid, sectionLsn)
			} else {
				st.ids.stateOnly(op.recid, sectionLsn)
			}
		case tAppend:
			if st.inner.Append(op.recid, op.data) == AppendRefused {
				panic(walCorrupt("commit append refused"))
			}
			// An append leaves both identities where they are.
		case tDelete:
			st.inner.Delete(op.recid, nil)
			st.ids.void(op.recid)
		}
	}
}

// failClosed: the store cannot be made consistent again — close it rather
// than let a caller retry into a segment holding partial bytes. Durable
// state on disk is intact; reopen replays it.
func (st *walState) failClosed() {
	st.closedFlag.Store(true)
	st.segs.close()
	st.inner.Close()
}

// ioFailClosed converts ONLY an I/O failure into failClosed, re-panicking it;
// other DBErrors (a W10 refusal, an identity disagreement, a pass
// divergence) propagate with the handle kept — cleanTick rewinds the cursor
// for those (StoreWAL.java:2628-2660). Used around the cleaner's writes;
// commit's own section uses the stricter W9 rule (any failure → closed).
func (st *walState) ioFailClosed() {
	if p := recover(); p != nil {
		if de, ok := mapdb5.AsDBError(p); ok && de.Kind == mapdb5.KindVolumeIO {
			st.failClosed()
		}
		panic(p)
	}
}

// rollActiveIfNonempty seals the active segment and starts a successor, if
// the active one holds any section. W3's force flavour applies: the seal
// persists SIZE.
func (st *walState) rollActiveIfNonempty() {
	active := st.segs.active()
	if active == nil || active.empty() {
		return
	}
	func() {
		defer func() {
			if p := recover(); p != nil {
				// A half-created segment is not recoverable in place.
				st.failClosed()
				panic(p)
			}
		}()
		active.ensureOpen()
		walIOReport(st.io, walOpForceFull, active.seq, active.fileLen, 0, 0)
		mustIO(active.segFile().Sync())
		st.segs.segSyncs++
		active.release()
		st.segs.createSegment(st.nextLsn)
	}()
}

// ---------- the cleaning trigger ----------

// cleaningTarget is the size the log is allowed to reach. `live` is the
// inner store's CurrentSize — PAGE-GRANULAR (header + whole slices), so the
// ratio is log-versus-footprint: exact at scale, conservative below ~2 MiB.
func (st *walState) cleaningTarget() int64 {
	live := st.inner.CurrentSize()
	scaled := live * st.spaceAmp
	if scaled < live { // overflow
		scaled = int64(^uint64(0) >> 1)
	}
	if st.minLog > scaled {
		return st.minLog
	}
	return scaled
}

// cleaningDue: the log exceeds its allowed size. It bounds SPACE, not write
// amplification — oldest-first cleaning is FIFO, not cost-benefit.
func (st *walState) cleaningDue() bool {
	return st.minLog > 0 && st.segs.logBytes() > st.cleaningTarget()
}

// cleaningUrgent is the hard ceiling: past TWICE the target, bounding the
// pause has stopped being the priority and the committing writer
// participates until it is back under.
func (st *walState) cleaningUrgent() bool {
	return st.minLog > 0 && st.segs.logBytes() > 2*st.cleaningTarget()
}

func (st *walState) clearLatch() {
	st.futileAtBytes = 0
	st.futileAtTarget = 0
	st.futileAtChanges = 0
	st.futileRecords = 0
}

// abandonEpisode abandons the episode WITHOUT judging it — for a
// configuration change or an explicit Checkpoint.
func (st *walState) abandonEpisode() {
	st.clearLatch()
	st.cleanFloorSeq = 0
	st.episodeRetired = 0
	st.episodeWritten = 0
	st.episodeRecords = 0
	st.episodeSegments = 0
	st.cycleWidth = 1
	st.lastCycleSaturated = false
}

// endEpisode ends the episode, having walked its whole range — the bounded
// window the no-net-progress terminal is measured over.
func (st *walState) endEpisode() {
	futile := st.cleanFloorSeq != 0 &&
		!paidForItself(st.cleanerRetired-st.episodeRetired, st.cleanerWritten-st.episodeWritten)
	if futile && st.lastCycleSaturated {
		st.futileAtBytes = max64(st.segs.logBytes(), 1)
		st.futileAtTarget = st.cleaningTarget()
		st.futileAtChanges = st.committedStateChanges
		st.futileRecords = max64(st.episodeRecords, 1)
	}
	st.cleanFloorSeq = 0
	st.episodeRetired = 0
	st.episodeWritten = 0
	st.episodeRecords = 0
	st.episodeSegments = 0
	// Reset WITH the episode, not across it. cycleWidth persists
	// deliberately — the width a log needs is a property of the log.
	st.lastCycleSaturated = false
}

// beginCycleIfDue opens a cycle over the oldest retirable segments IF the
// trigger is live. Returns whether a cycle is now open.
func (st *walState) beginCycleIfDue() bool {
	if !st.cleaningDue() {
		// The trigger went quiet. The EPISODE is not over: keeping its floor
		// across a dip stops a workload hovering around the target paying a
		// fresh seal+create+dir fsync every few commits. Only the latch is
		// released.
		st.clearLatch()
		return false
	}
	if st.futileAtBytes > 0 {
		room := st.cleaningTarget()
		retry := st.futileAtBytes + room
		if retry < st.futileAtBytes {
			retry = int64(^uint64(0) >> 1)
		}
		// A MATERIAL drop, not any drop — the target is a page-granular
		// footprint and an ordinary update moves it by a few hundred bytes.
		dropped := st.futileAtTarget - (st.futileAtTarget >> 3)
		grew := st.segs.logBytes() >= retry
		shrank := room <= dropped
		// Neither sees a state-only mass delete, which obsoletes every image
		// in the log while moving neither number.
		churned := st.committedStateChanges-st.futileAtChanges >= st.futileRecords
		if !grew && !shrank && !churned {
			return false
		}
		st.clearLatch()
	}
	if st.cleanFloorSeq == 0 {
		// The seal is CLEANING's cost: this rollover exists only to give the
		// episode a floor.
		logBefore, retiredBefore := st.segs.logBytes(), st.cleanerRetired
		st.rollActiveIfNonempty()
		st.chargeCleaner(logBefore, retiredBefore)
		st.cleanFloorSeq = st.segs.active().seq
		st.episodeRetired = st.cleanerRetired
		st.episodeWritten = st.cleanerWritten
	}
	// BINARY search for the floor, not a walk.
	below := 0
	for below < len(st.segs.segments) && st.segs.segments[below].seq < st.cleanFloorSeq {
		below++
	}
	if below == 0 || below >= len(st.segs.segments) {
		st.endEpisode() // the episode has rewritten everything it could
		return false
	}
	if st.episodeSegments == 0 {
		st.episodeSegments = below
	}
	// One segment per cycle, or as many as the width search has reached.
	width := st.cycleWidth
	if width < 1 {
		width = 1
	}
	if width > cycleWidthCap {
		width = cycleWidthCap
	}
	if width > below {
		width = below
	}
	// SATURATED = as wide as this episode is ever allowed to go: the width
	// cap, or the range the episode STARTED with, whichever is smaller.
	ceiling := st.episodeSegments
	if cycleWidthCap < ceiling {
		ceiling = cycleWidthCap
	}
	st.cycleSaturated = width >= ceiling
	target := st.segs.segments[width-1].seq
	st.startCycle(target)
	return true
}

// startCycle opens a cycle retiring everything at or below targetSeq; the
// caller must have established a segment above it exists. O(1): candidates
// are discovered by WALKING THE RETIRING RANGE, one bounded unit at a time.
func (st *walState) startCycle(targetSeq int64) {
	st.cycleRetiredAt = st.cleanerRetired
	st.cycleWrittenAt = st.cleanerWritten
	var successor *walSegment
	for _, seg := range st.segs.segments {
		if seg.seq > targetSeq {
			successor = seg
			break
		}
	}
	if successor == nil {
		// K4: a mark may not authorize removing its own segment.
		panic(mapdb5.New(mapdb5.KindMisuse, "a cycle always leaves a segment above its target"))
	}
	st.cleaner = newWalCleaner(targetSeq, successor.headerFirstLsn())
}

// chargeCleaner charges one unit of cleaning: what the log grew by, plus
// what the unit retired. Both halves are in FILE bytes — the sections a tick
// appends are not what they cost the log.
func (st *walState) chargeCleaner(logBefore, retiredBefore int64) int64 {
	// Charges NOTHING once the store is closed.
	if len(st.segs.segments) == 0 && st.cleaner == nil {
		return 0
	}
	charge := st.segs.logBytes() - logBefore + (st.cleanerRetired - retiredBefore)
	st.cleanerWritten += charge
	return charge
}

// ---------- the cleaner phases ----------

// publishUnit is phase 1, one bounded unit: walk the retiring range and
// publish, as a single 'C' section, an image of every record met whose state
// still lives inside it. Check, copy and publish are one serialized unit
// under the WAL write lock — splitting them lets a stale image land above a
// concurrent commit.
func (st *walState) publishUnit(budget *walBudget, writtenSoFar int64, recordsSoFar int) (steps int) {
	byteRoom := int64(1 << 20)
	if budget.maxBytes > 0 {
		byteRoom = budget.maxBytes - writtenSoFar
		if byteRoom < 1 {
			byteRoom = 1
		}
	}
	recRoom := scanUnitEntries
	if budget.maxRecords > 0 {
		recRoom = budget.maxRecords - recordsSoFar
		if recRoom < 1 {
			recRoom = 1
		}
	}
	cap := byteRoom
	if cap > 1<<20 {
		cap = 1 << 20
	}
	lsn, lsnAfter := st.reserveLsn()
	type emittedRec struct {
		recid   uint64
		content bool
	}
	var emittedList []emittedRec
	inBatch := map[uint64]struct{}{}

	// Emit into the pooled writer; an image larger than one unit's allowance
	// still goes whole.
	body := func() []byte {
		out := bin.GetWriter()
		defer bin.PutWriter(out)
		c := st.cleaner
		boundary := c.boundaryLsn
		steps = scanWalUnit(c, st.segs, recRoom, func(recid uint64) bool {
			if _, dup := inBatch[recid]; dup {
				return true
			}
			// Across units no dedup is needed: a recid re-emitted by an earlier
			// unit has a stateLsn above the boundary.
			sl, ok := st.ids.stateLsn[recid]
			if !ok || sl > boundary {
				return true
			}
			if s, ok := st.staged[recid]; ok && s.created {
				// Both at once would mean inner's slot was overwritten with a
				// preallocation while committed content is still attested.
				panic(walCorrupt("WAL cleaner: recid has committed state and is also allocated by an in-flight transaction"))
			}
			var content bool
			live := st.inner.walSnapshotOne(recid, func(prealloc bool, capBytes int64, data []byte) {
				if prealloc {
					out.WriteU8(tPrealloc)
					out.PackLong(int64(recid))
				} else {
					out.WriteU8(tRecord)
					out.PackLong(int64(recid))
					out.PackLong(capBytes)
					if data == nil {
						out.PackLong(0)
					} else {
						out.PackLong(int64(len(data)) + 1)
						out.WriteAll(data)
					}
					content = data != nil
				}
			})
			if !live {
				// stateLsn present means "committed non-void", and inner IS
				// the committed state.
				panic(walCorrupt("WAL cleaner: recid has committed state but the inner store holds nothing for it"))
			}
			emittedList = append(emittedList, emittedRec{recid, content})
			inBatch[recid] = struct{}{}
			return int64(out.Pos) < cap
		})
		return out.CopyBytes()
	}()

	if len(emittedList) > 0 {
		func() {
			defer st.ioFailClosed()
			appendSection(st.segs, st.segBytes, st.io, st.closedFlag, tagImage, lsn,
				func(sink *bodySink) { sink.write(body) })
		}()
		st.nextLsn = lsnAfter
		// IMAGES, not entries walked: the staleness clock compares committed
		// self-contained entries against the live set the futile episode had
		// to preserve.
		st.episodeRecords += int64(len(emittedList))
		// Identities move by each entry's §4.2 row, AFTER the section is
		// durable and atomically with it.
		for _, e := range emittedList {
			if e.content {
				st.ids.content(e.recid, lsn)
			} else {
				st.ids.stateOnly(e.recid, lsn)
			}
		}
	}
	if st.cleaner.rangeDone {
		st.cleaner.published = true
		st.cleaner.rewind() // also clears rangeDone, for the verify walk
	}
	return steps
}

// verifyUnit is phase 2 — W10, one bounded unit: re-walk the retiring range
// and assert that every recid it mentions has been re-homed above it.
// Chunking across ticks is sound because the predicate is MONOTONE.
func (st *walState) verifyUnit(budget *walBudget, recordsSoFar int) int {
	recRoom := scanUnitEntries
	if budget.maxRecords > 0 {
		recRoom = budget.maxRecords - recordsSoFar
		if recRoom < 1 {
			recRoom = 1
		}
	}
	c := st.cleaner
	boundary := c.boundaryLsn
	steps := scanWalUnit(c, st.segs, recRoom, func(recid uint64) bool {
		if sl, ok := st.ids.stateLsn[recid]; ok && sl <= boundary {
			panic(walCorrupt("WAL cleaner would retire a segment while a recid still has its only " +
				"self-contained entry inside it: refusing to write the clean mark. " +
				"Nothing has been deleted; the durable log is intact."))
		}
		return true
	})
	if c.rangeDone {
		c.verified = true
	}
	return steps
}

// finishCycle closes a cycle: append the forced 'K', then unlink. The 'K' is
// forced before the unlink (W5): a failed unlink is a leak the next open
// retries, never permission to advance an unproven mark.
func (st *walState) finishCycle() {
	c := st.cleaner
	target, logStart := c.targetSeq, c.logStartLsn
	st.appendMark(target, logStart)
	var retired int64
	for _, seg := range st.segs.segments {
		if seg.seq > target {
			break
		}
		retired += seg.fileLen
	}
	func() {
		defer st.ioFailClosed()
		st.segs.unlinkThrough(target)
	}()
	st.cleanerRetired += retired
	st.cleaner = nil
}

// cleanTick is one cleaning tick: re-emit, then verify (W10), then close the
// cycle — as far as budget allows. At most ONE cycle closes per tick.
func (st *walState) cleanTick(budget *walBudget) {
	t0 := time.Now()
	var written int64
	var records int
	closedCycle := false
	for st.cleaner != nil {
		logBefore, retiredBefore := st.segs.logBytes(), st.cleanerRetired
		c := st.cleaner
		var steps int
		stepErr := func() (err *mapdb5.DBError) {
			defer func() {
				if p := recover(); p != nil {
					de, ok := mapdb5.AsDBError(p)
					if !ok {
						panic(p)
					}
					if de.Kind == mapdb5.KindVolumeIO {
						// I/O goes through failClosed; only a DBException —
						// a W10 refusal or identity disagreement — rewinds and
						// keeps the handle (StoreWAL.java:2628-2660).
						st.failClosed()
					} else if st.cleaner != nil {
						// A unit refused — W10 caught an under-re-emission.
						// The cursor already stepped past it, so REWIND so
						// any retry re-walks the range from the bottom.
						st.cleaner.rewind()
					}
					err = de
				}
			}()
			switch {
			case !c.published:
				steps = st.publishUnit(budget, written, records)
			case !c.verified:
				steps = st.verifyUnit(budget, records)
			default:
				st.finishCycle()
				closedCycle = true
			}
			return nil
		}()
		if stepErr != nil {
			panic(stepErr)
		}
		written += st.chargeCleaner(logBefore, retiredBefore)
		if closedCycle {
			break
		}
		records += steps
		if budget.maxRecords > 0 && records >= budget.maxRecords {
			break
		}
		if budget.maxBytes > 0 && written >= budget.maxBytes {
			break
		}
		if budget.maxNanos > 0 && time.Since(t0).Nanoseconds() >= budget.maxNanos {
			break
		}
	}
	if closedCycle {
		// The cycle is closed and its whole cost is now charged: THREE bands,
		// not two — widen when it does not pay, hold when it pays modestly,
		// give width back only when it pays HANDSOMELY.
		cost := st.cleanerWritten - st.cycleWrittenAt
		gain := st.cleanerRetired - st.cycleRetiredAt - cost
		st.cycleWidth = nextCycleWidth(st.cycleWidth, cost, gain)
		st.lastCycleSaturated = st.cycleSaturated
	}
}

// autoCleanLocked is commit's inline clean: ONE bounded slice per commit,
// except past the hard ceiling where the writer participates until the log
// is back under.
func (st *walState) autoCleanLocked() {
	if !st.cleaningDue() {
		st.clearLatch()
		return
	}
	budget := st.foreground
	if st.cleaner != nil || st.beginCycleIfDue() {
		st.cleanTick(&budget)
	}
	for st.cleaningUrgent() && (st.cleaner != nil || st.beginCycleIfDue()) {
		st.cleanTick(&budget)
	}
}

// cleanWholeLog is Checkpoint's body: roll first (every section-bearing
// segment is then strictly below the active one), then a single unbudgeted
// cycle retires all of them.
func (st *walState) cleanWholeLog() {
	for st.cleaner != nil {
		st.cleanTick(&unboundedBudget)
	}
	logBefore, retiredBefore := st.segs.logBytes(), st.cleanerRetired
	st.rollActiveIfNonempty()
	st.chargeCleaner(logBefore, retiredBefore)
	target := st.segs.active().seq - 1
	if target < firstSeq || len(st.segs.segments) < 2 {
		return // nothing below the first sequence number to retire
	}
	st.startCycle(target)
	for st.cleaner != nil {
		st.cleanTick(&unboundedBudget)
	}
	st.abandonEpisode() // an explicit full clean re-arms the automatic one
}

// appendMark writes a 'K' mark: everything at or below cleanedThroughSeq may
// be removed, and where the retained log begins. Forced before any unlink
// (W5).
func (st *walState) appendMark(cleanedThroughSeq, logStartLsn int64) {
	body := buildMarkBody(cleanedThroughSeq, logStartLsn)
	lsn, lsnAfter := st.reserveLsn()
	func() {
		defer st.ioFailClosed()
		appendSection(st.segs, st.segBytes, st.io, st.closedFlag, tagMark, lsn,
			func(sink *bodySink) { sink.write(body[:]) })
	}()
	st.nextLsn = lsnAfter
}

// ---------- the cleaner cursor ----------

// walCleaner is a cleaning cycle in progress: retire every segment with
// seq <= targetSeq by re-emitting, above them, a self-contained image of
// every record whose state still lives inside them.
type walCleaner struct {
	targetSeq   int64 // cleanedThroughSeq the closing 'K' will attest
	logStartLsn int64 // logStartLsn the 'K' will attest — the successor's STATED start
	boundaryLsn int64 // the last LSN the retiring range accounts for, logStartLsn-1
	published   bool  // phase 1 (re-emit) walked the whole range
	verified    bool  // phase 2 (W10) walked it again

	// the scan cursor: phase 1 uses it, phase 2 rewinds and reuses it
	seg       int   // index into segs.segments; the retiring range is a prefix
	offset    int64 // next SECTION to enter within seg
	entryPos  int64 // next ENTRY inside the section being walked; -1 between sections
	bodyEnd   int64
	rangeDone bool
}

func newWalCleaner(targetSeq, logStartLsn int64) *walCleaner {
	return &walCleaner{
		targetSeq: targetSeq, logStartLsn: logStartLsn,
		boundaryLsn: logStartLsn - 1, entryPos: -1,
	}
}

func (c *walCleaner) rewind() {
	c.seg = 0
	c.offset = 0
	c.entryPos = -1
	c.rangeDone = false
}

// paidForItself: did re-emitting `written` bytes to retire `retired` pay for
// itself? A GAIN OF AN EIGHTH of what was written, not merely positive —
// epsilon progress at minimum segment size is a treadmill.
func paidForItself(retired, written int64) bool {
	return retired-written > written>>3
}

// nextCycleWidth: widen when gain is poor, hold when modest, halve when
// handsome — THREE bands: halving on any gain oscillates around break-even.
func nextCycleWidth(width int, cost, gain int64) int {
	if gain <= cost>>3 {
		w := width
		if w < 1 {
			w = 1
		}
		w *= 2
		if w > cycleWidthCap {
			w = cycleWidthCap
		}
		return w
	}
	if gain > cost>>1 {
		if w := width / 2; w >= 1 {
			return w
		}
		return 1
	}
	return width
}

// scanWalUnit walks up to maxSteps entries of the retiring range, handing
// each recid to visit; stops early when visit returns false. Sets
// c.rangeDone once the range is exhausted. The unit is AN ENTRY, not a
// section — a section may be arbitrarily large.
func scanWalUnit(c *walCleaner, segs *walSegmentSet, maxSteps int, visit func(uint64) bool) int {
	steps := 0
	for steps < maxSteps {
		if c.seg >= len(segs.segments) || segs.segments[c.seg].seq > c.targetSeq {
			c.rangeDone = true
			return steps
		}
		segs.segments[c.seg].ensureOpen()
		seg := segs.segments[c.seg]
		validEnd, seq := seg.validEnd, seg.seq
		r := newSecIn(seg.segFile(), scanBuf)
		// The HARD bound is the segment, set once; rebound narrows the soft
		// bound per section without dropping the window.
		r.resetHard(segHdr, validEnd)
		if c.offset < segHdr {
			c.offset = segHdr
		}
		leaveSegment := false
		stop := false
		for {
			if steps >= maxSteps {
				break
			}
			if c.entryPos < 0 {
				if c.offset >= validEnd {
					leaveSegment = true
					steps++
					break
				}
				if validEnd-c.offset < secHdr {
					panic(walCorrupt("WAL section header does not fit before the segment's validated end"))
				}
				r.rebound(c.offset, validEnd)
				var hdr [secHdr]byte
				r.readFully(hdr[:])
				tag, _, bodyLen, _, _ := parseSecHdr(&hdr)
				bodyStart := c.offset + secHdr
				if bodyLen < 0 || bodyLen > validEnd-bodyStart {
					panic(walCorrupt("WAL section claims a body running past the segment's validated end"))
				}
				c.bodyEnd = bodyStart + bodyLen
				c.offset = c.bodyEnd // where the NEXT section begins
				// Entering a section costs a header read and is charged like
				// an entry — else a range of mark-only sections is walked
				// entirely within one unit.
				steps++
				if tag == tagMark {
					continue // a 'K' body carries no entries
				}
				c.entryPos = bodyStart
			}
			r.rebound(c.entryPos, c.bodyEnd)
			for r.pos() < c.bodyEnd && steps < maxSteps {
				recid := nextWalEntryRecid(r, seq)
				c.entryPos = r.pos()
				steps++
				if !visit(recid) {
					stop = true
					break
				}
			}
			if c.entryPos >= c.bodyEnd {
				c.entryPos = -1 // section done
			}
			if stop {
				break
			}
		}
		if leaveSegment {
			// Released the moment the walk leaves it: keeps the descriptor
			// count O(1) rather than O(segments in the range).
			segs.segments[c.seg].release()
			c.seg++
			c.offset = 0
			c.entryPos = -1
			continue
		}
		if stop {
			break
		}
	}
	return steps
}

// nextWalEntryRecid decodes one entry for its recid alone, seeking over the
// payload.
func nextWalEntryRecid(r *secIn, seq int64) uint64 {
	ty := r.readByte()
	recid := r.unpackLong()
	switch ty {
	case tPrealloc, tDelete:
	case tRecord:
		r.unpackLong() // capacity
		if lenPlus := r.unpackLong(); lenPlus != 0 {
			// The length goes in a local FIRST — an inline pos() would land
			// short by the packed length's width.
			r.seek(r.pos() + int64(lenPlus-1))
		}
	case tAppend:
		r.unpackLong() // base delta
		ln := r.unpackLong()
		r.seek(r.pos() + int64(ln))
	default:
		panic(walCorrupt("bad WAL entry tag"))
	}
	return recid
}

// emitWalEntries emits the entry stream of one commit section. Called once
// per pass; the two calls produce identical bytes because ops and sectionLsn
// are fixed before the first one.
func emitWalEntries(ops []walOp, sectionLsn int64, sink *bodySink) {
	frame := bin.GetWriter()
	defer bin.PutWriter(frame)
	for _, op := range ops {
		frame.Pos = 0
		switch op.op {
		case tPrealloc, tDelete:
			frame.WriteU8(byte(op.op))
			frame.PackLong(int64(op.recid))
		case tRecord:
			frame.WriteU8(tRecord)
			frame.PackLong(int64(op.recid))
			frame.PackLong(op.capBytes)
			if op.hasData {
				frame.PackLong(int64(len(op.data)) + 1)
			} else {
				frame.PackLong(0)
			}
		case tAppend:
			frame.WriteU8(tAppend)
			frame.PackLong(int64(op.recid))
			// Base identity as a delta against this section's own LSN.
			frame.PackLong(sectionLsn - op.baseLsn)
			frame.PackLong(int64(len(op.data)))
		case tTransient:
			continue // not logged
		}
		sink.write(frame.Buf[:frame.Pos])
		if (op.op == tRecord || op.op == tAppend) && op.hasData {
			sink.write(op.data)
		}
	}
}

// walRecordCap computes capacity as the writer encodes it: 0 for null and
// for genuinely oversize content (stored linked), else 16-aligned and big
// enough for header+content. Headroom is a HINT clamped to the plain-record
// maximum — falling to capacity 0 there would make the writer acknowledge a
// commit the decoder rejects.
func walRecordCap(m []byte, hasM bool, headroom int) int64 {
	if !hasM {
		return 0
	}
	max := uint64(ivCapMax) * 16
	plain := uint64(4 + len(m))
	cap := plain + uint64(headroom) + 15 // saturating in u64
	if cap < plain {
		cap = ^uint64(0)
	}
	cap &^= 15
	if cap > max {
		if plain <= max {
			return int64(max)
		}
		return 0 // genuinely oversize content: stored as a linked chain
	}
	return int64(cap)
}

func sortUint64s(v []uint64) {
	// small insertion sort is fine for typical transaction sizes; use sort for large
	if len(v) > 32 {
		sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
		return
	}
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ---------- Store ----------

func (s *StoreWAL) Preallocate() Recid {
	st := s.writeOpen()
	defer s.writeOpenEnd()
	recid := st.inner.Preallocate()
	st.staged[recid] = &walStaged{created: true}
	return recid
}

func (s *StoreWAL) PreallocateN(into []Recid) {
	for i := range into {
		into[i] = s.Preallocate()
	}
}

func (s *StoreWAL) Put(record any, sr ser.AnySerializer) Recid {
	validateRecord(sr, record)
	var buf []byte
	if record != nil {
		buf = serializeErased(record, sr)
	}
	st := s.writeOpen()
	defer s.writeOpenEnd()
	recid := st.inner.Preallocate()
	st.staged[recid] = &walStaged{created: true, baseSet: true, base: buf}
	return recid
}

func (s *StoreWAL) Get(recid Recid, sr ser.AnySerializer) any {
	st := s.readOpen()
	defer s.readOpenEnd()
	staged, ok := st.staged[recid]
	if !ok {
		return st.inner.Get(recid, sr)
	}
	if staged.deleted {
		panic(mapdb5.NewGetVoid(recid))
	}
	m, has := st.merged(recid, staged)
	if !has {
		return nil
	}
	return sr.DeserializeAny(bin.NewReader(m), len(m))
}

func (s *StoreWAL) Read(recid Recid, action RecordRead) int64 {
	st := s.readOpen()
	defer s.readOpenEnd()
	staged, ok := st.staged[recid]
	if !ok {
		return st.inner.Read(recid, action)
	}
	if staged.deleted {
		panic(mapdb5.NewGetVoid(recid))
	}
	m, has := st.merged(recid, staged)
	if !has {
		return action.OnNull()
	}
	return action.OnBytes(bin.NewReader(m), len(m))
}

func (s *StoreWAL) Update(recid Recid, record any, sr ser.AnySerializer) {
	s.updateWithHeadroomOpt(recid, record, sr, 0)
}

func (s *StoreWAL) updateWithHeadroomOpt(recid Recid, record any, sr ser.AnySerializer, headroom int) {
	validateRecord(sr, record)
	if headroom < 0 {
		panic(mapdb5.NewMisuse("negative headroom"))
	}
	var buf []byte
	if record != nil {
		buf = serializeErased(record, sr)
		// Fail at update time, not commit time. Oversize CONTENT is fine
		// (stored linked at commit) — but content that fits a plain record
		// must also fit with its headroom. Computed in u64 so a huge headroom
		// cannot wrap to a small capacity.
		plain := uint64(4 + len(buf))
		max := uint64(ivCapMax) * 16
		cap16 := plain + uint64(headroom) + 15
		if cap16 < plain {
			cap16 = ^uint64(0)
		}
		cap16 &^= 15
		if plain <= max && cap16 > max {
			panic(mapdb5.New(mapdb5.KindRecordTooLarge, "record with headroom exceeds maximum capacity"))
		}
	}
	st := s.writeOpen()
	defer s.writeOpenEnd()
	w := st.stagedForWrite(recid)
	w.baseSet = true
	w.base = buf
	w.headroom = headroom
	w.appends = nil
	w.appendsLen = 0
}

func (s *StoreWAL) CompareAndSwap(recid Recid, expectedOld, newRecord any, sr ser.AnySerializer) bool {
	validateRecord(sr, expectedOld)
	validateRecord(sr, newRecord)
	st := s.writeOpen()
	defer s.writeOpenEnd()
	var current any
	if staged, ok := st.staged[recid]; !ok {
		if st.inner.recState(recid) == stateVoid {
			panic(mapdb5.NewGetVoid(recid))
		}
		current = st.inner.Get(recid, sr)
	} else {
		if staged.deleted {
			panic(mapdb5.NewGetVoid(recid))
		}
		if m, has := st.merged(recid, staged); has {
			current = sr.DeserializeAny(bin.NewReader(m), len(m))
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
	var newBytes []byte
	if newRecord != nil {
		newBytes = serializeErased(newRecord, sr)
	}
	w := st.stagedForWrite(recid)
	w.baseSet = true
	w.base = newBytes
	w.headroom = 0
	w.appends = nil
	w.appendsLen = 0
	return true
}

func (s *StoreWAL) Delete(recid Recid, _ ser.AnySerializer) {
	st := s.writeOpen()
	defer s.writeOpenEnd()
	w := st.stagedForWrite(recid)
	w.deleted = true
	w.baseSet = false
	w.base = nil
	w.appends = nil
	w.appendsLen = 0
}

func (s *StoreWAL) Commit() {
	st := s.writeOpen()
	defer s.writeOpenEnd()
	st.commitLocked()
}

func (s *StoreWAL) Compact() { s.Checkpoint() }

// Rollback discards all uncommitted mutations, including appends.
func (s *StoreWAL) Rollback() {
	st := s.writeOpen()
	defer s.writeOpenEnd()
	var created []uint64
	for r, w := range st.staged {
		if w.created {
			created = append(created, r)
		}
	}
	for _, recid := range created {
		st.inner.Delete(recid, nil) // free the P recid
	}
	st.staged = map[uint64]*walStaged{}
	// NEITHER IDENTITY MOVES ON ROLLBACK: both are set only by a committed
	// apply, so a transaction that never committed established nothing.
	// Signal open collections that their append-only structural caches may
	// describe a taller-than-real tree.
	s.structGen.Add(1)
}

func (s *StoreWAL) Close() error {
	// Acquire the write lock BEFORE publishing closed, so an in-flight commit
	// that rechecks closed under the lock observes the close atomically.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Swap(true) {
		return nil
	}
	st := s.st
	var deleted error
	if s.deleteOnClose.Load() && !st.readOnly {
		func() {
			defer func() {
				if p := recover(); p != nil {
					if de, ok := mapdb5.AsDBError(p); ok {
						deleted = de
					} else {
						panic(p)
					}
				}
			}()
			st.segs.deleteNamespace()
		}()
	}
	st.segs.close()
	innerErr := st.inner.Close()
	if deleted != nil {
		return deleted
	}
	return innerErr
}

func (s *StoreWAL) IsClosed() bool { return s.closed.Load() }

func (s *StoreWAL) Verify() {
	st := s.readOpen()
	defer s.readOpenEnd()
	st.inner.Verify()
}

func (s *StoreWAL) AllRecids() iter.Seq[Recid] {
	st := s.readOpen()
	defer s.readOpenEnd()
	set := map[uint64]struct{}{}
	for r := range st.inner.AllRecids() {
		set[r] = struct{}{}
	}
	for recid, w := range st.staged {
		switch {
		case w.deleted:
			delete(set, recid)
		case w.baseSet || len(w.appends) > 0:
			set[recid] = struct{}{}
		default:
			delete(set, recid) // pure prealloc
		}
	}
	out := make([]Recid, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sortUint64s(out)
	return func(yield func(Recid) bool) {
		for _, r := range out {
			if !yield(r) {
				return
			}
		}
	}
}

func (s *StoreWAL) IsThreadSafe() bool { return s.threadSafe }
func (s *StoreWAL) IsReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.readOnly
}
func (s *StoreWAL) IsTx() bool { return true }

func (s *StoreWAL) StructuralGeneration() uint64 { return s.structGen.Load() }

// CurrentSize is the approximate byte footprint — from innerStats rather
// than under st (serializer callbacks run under st and may query the size).
func (s *StoreWAL) CurrentSize() int64 {
	if s.closed.Load() {
		return 0
	}
	return s.innerStats.CurrentSize()
}

// ---------- StoreDelta ----------

func (s *StoreWAL) Append(recid Recid, data []byte) int64 {
	st := s.writeOpen()
	defer s.writeOpenEnd()
	_, wasStaged := st.staged[recid]
	// Runs for its GetVoid validation even when nothing will be staged.
	st.stagedForWrite(recid)
	if len(data) == 0 {
		// A zero-length append stages NOTHING — not even the empty staged
		// entry stagedForWrite just left behind.
		if !wasStaged {
			delete(st.staged, recid)
		}
		var baseLen int
		if s2, ok := st.staged[recid]; ok && s2.baseSet {
			baseLen = len(s2.base)
		} else if st.inner.recState(recid) == stateLive {
			baseLen = len(st.inner.rawGet(recid))
		}
		var appLen int
		if s2, ok := st.staged[recid]; ok {
			appLen = s2.appendsLen
		}
		return int64(baseLen + appLen)
	}
	baseLive := !st.staged[recid].baseSet && st.inner.recState(recid) == stateLive
	var capRem int64
	if baseLive {
		capRem = st.inner.CapacityRemaining(recid)
		if int64(st.staged[recid].appendsLen+len(data)) > capRem {
			// REFUSED is a no-op: it must stage NOTHING (an empty staged entry
			// would be classified T_PREALLOC at commit and burn an LSN).
			if !wasStaged {
				delete(st.staged, recid)
			}
			return AppendRefused
		}
	}
	var baseLen int
	if baseLive {
		baseLen = len(st.inner.rawGet(recid))
	} else {
		baseLen = len(st.staged[recid].base)
	}
	w := st.staged[recid]
	w.appends = append(w.appends, append([]byte(nil), data...))
	w.appendsLen += len(data)
	return int64(baseLen + w.appendsLen)
}

func (s *StoreWAL) CapacityRemaining(recid Recid) int64 {
	st := s.readOpen()
	defer s.readOpenEnd()
	staged, ok := st.staged[recid]
	if !ok {
		return st.inner.CapacityRemaining(recid)
	}
	if staged.deleted {
		panic(mapdb5.NewGetVoid(recid))
	}
	if staged.baseSet {
		return int64(^uint64(0) >> 1) // capacity established at commit
	}
	if st.inner.recState(recid) == stateLive {
		r := st.inner.CapacityRemaining(recid) - int64(staged.appendsLen)
		if r < 0 {
			return 0
		}
		return r
	}
	return int64(^uint64(0) >> 1)
}

func (s *StoreWAL) UpdateWithHeadroom(recid Recid, record any, sr ser.AnySerializer, headroom int) {
	s.updateWithHeadroomOpt(recid, record, sr, headroom)
}
