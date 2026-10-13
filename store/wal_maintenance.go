package store

import (
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// ---- background maintenance wiring (R6) ----

// StartMaintenance enables background maintenance: an OWNED
// MaintenanceExecutor running the budgeted WAL cleaner task. Idempotent;
// returns the executor. The task does the same work commit does, one
// bounded tick at a time; commit's inline clean remains the synchronous
// fallback (P7), so a store with no maintenance goroutine is still bounded.
// Close shuts the owned executor down.
func (s *StoreWAL) StartMaintenance() *MaintenanceExecutor {
	m := &s.maint
	m.mu.Lock()
	defer m.mu.Unlock()
	s.checkMaintainable()
	if m.ex == nil {
		m.ex = NewMaintenanceExecutor("wal")
		m.owns = true
		m.handle = m.ex.Register(&walCleanerTask{store: s})
	}
	return m.ex
}

// AttachMaintenance registers the cleaner task into an externally-owned
// executor; Close cancels the task but does not shut the executor down.
func (s *StoreWAL) AttachMaintenance(executor *MaintenanceExecutor) {
	m := &s.maint
	m.mu.Lock()
	defer m.mu.Unlock()
	s.checkMaintainable()
	if executor == nil {
		panic(mapdb5.NewMisuse("maintenance executor is nil"))
	}
	if m.handle != nil {
		panic(mapdb5.NewMisuse("maintenance already attached"))
	}
	m.ex = executor
	m.owns = false
	m.handle = executor.Register(&walCleanerTask{store: s})
}

// checkMaintainable is Java's checkClosed + checkWritable +
// maintenanceShutdown guard; maint.mu held.
func (s *StoreWAL) checkMaintainable() {
	if s.closed.Load() || s.maint.shutdown {
		panic(mapdb5.New(mapdb5.KindClosed, "store closed"))
	}
	if s.st.readOnly {
		panic(mapdb5.New(mapdb5.KindReadOnly, "WAL store opened read-only"))
	}
}

// stopMaintenanceForClose stops the owned executor (or cancels the attached
// task) BEFORE Close takes the WAL write lock; concurrent closers all wait
// for it (maintOwner.stopForClose).
func (s *StoreWAL) stopMaintenanceForClose() { s.maint.stopForClose(nil) }

// maintenanceCleanStep is one budgeted cleaning tick, called by the cleaner
// task off the executor goroutine. Returns the bytes it wrote, or -1 when
// there was nothing to do — which the task reports as "no progress" so the
// executor backs off.
//
// A cycle already in progress is continued even if the log has since
// dropped back under target: its images are already written and paid for.
//
// Java also defers while a delta transaction holds an LSN reservation (C15);
// the Go StoreWAL has no delta transactions, so there is no reservation to
// strand.
func (s *StoreWAL) maintenanceCleanStep(budget MaintenanceBudget) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() || s.st.readOnly {
		return -1
	}
	// Every unit of cleaning ends in a forced section; there is no useful
	// work under a no-fsync budget.
	if !budget.MayFsync {
		return -1
	}
	st := s.st
	// START a cycle when one is due, exactly as the commit path does —
	// without this the task finds no cycle, reports 0 bytes as progress and
	// spins while the log grows (Java
	// the_background_task_retires_a_segment_on_its_own).
	if st.cleaner == nil && !st.beginCycleIfDue() {
		return -1
	}
	wb := walBudget{
		maxRecords: budget.MaxRecords,
		maxBytes:   int64(budget.MaxBytes),
		maxNanos:   budget.MaxNanos,
	}
	return st.cleanTick(&wb)
}

// walCleanerIdle is the backoff after a tick that found nothing to do.
const walCleanerIdle = int64(time.Second)

// walCleanerTask is the WAL cleaner as an R6 maintenance task: each tick
// asks the store for one budgeted slice of cleaning. P7-safe: a cycle only
// becomes visible when its 'K' mark is forced, and commit's inline clean
// remains the fallback.
type walCleanerTask struct {
	store             *StoreWAL
	nextEligibleNanos int64 // touched only by the executor goroutine
}

func (t *walCleanerTask) Name() string           { return "wal-cleaner" }
func (t *walCleanerTask) Kind() MaintenanceClass { return MaintenanceCheckpoint }

func (t *walCleanerTask) WantsRun(nowNanos int64) bool {
	return !t.store.IsClosed() && nowNanos >= t.nextEligibleNanos
}

func (t *walCleanerTask) Run(budget MaintenanceBudget) (r MaintenanceResult, err error) {
	defer func() {
		if p := recover(); p != nil {
			if de, ok := mapdb5.AsDBError(p); ok && de.Kind == mapdb5.KindClosed {
				t.nextEligibleNanos = MaintenanceNow() + walCleanerIdle
				r, err = MaintenanceNoProgress(walCleanerIdle), nil
				return
			}
			panic(p)
		}
	}()
	written := t.store.maintenanceCleanStep(budget)
	if written < 0 {
		// Nothing due. Back off; the next commit that crosses the trigger
		// makes this worth running again.
		t.nextEligibleNanos = MaintenanceNow() + walCleanerIdle
		return MaintenanceNoProgress(walCleanerIdle), nil
	}
	// Zero bytes with a cycle still open is progress: the tick spent its
	// budget on the W10 verification scan.
	t.nextEligibleNanos = 0
	return MaintenanceProgressBytes(written), nil
}

func (t *walCleanerTask) OnShutdown(bool) {}
