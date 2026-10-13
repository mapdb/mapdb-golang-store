package store

import (
	"sync/atomic"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// directMaintenance is StoreDirect's optional background maintenance
// (R6; disabled by default). Guarded by mu (Java: synchronized(this));
// active is the lock-free fast path for the churn signal.
type directMaintenance struct {
	maintOwner
	task   *directCompactionTask // guarded by mu
	active atomic.Bool
}

// StartMaintenance enables background maintenance for this store, creating
// an OWNED MaintenanceExecutor running a compaction task that drives
// CompactStep. Idempotent; returns the executor. The store is fully correct
// with maintenance disabled — this only moves the (already synchronous)
// compaction work to a bounded background goroutine (P7). Close shuts the
// owned executor down.
func (s *StoreDirect) StartMaintenance() *MaintenanceExecutor {
	m := &s.maint
	m.mu.Lock()
	defer m.mu.Unlock()
	s.checkClosed()
	if m.shutdown {
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
	if m.ex == nil {
		m.ex = NewMaintenanceExecutor("direct")
		m.owns = true
		m.task = newDirectCompactionTask(s)
		m.handle = m.ex.Register(m.task)
		m.active.Store(true)
	}
	return m.ex
}

// AttachMaintenance registers this store's compaction task into an
// EXTERNALLY-owned executor (e.g. one shared by several stores). Close
// cancels the task but does not shut the executor down.
func (s *StoreDirect) AttachMaintenance(executor *MaintenanceExecutor) {
	m := &s.maint
	m.mu.Lock()
	defer m.mu.Unlock()
	s.checkClosed()
	if m.shutdown {
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
	if executor == nil {
		panic(mapdb5.NewMisuse("maintenance executor is nil"))
	}
	if m.handle != nil {
		panic(mapdb5.NewMisuse("maintenance already attached"))
	}
	m.ex = executor
	m.owns = false
	m.task = newDirectCompactionTask(s)
	m.handle = executor.Register(m.task)
	m.active.Store(true)
}

// signalCompactionChurn hints the compaction task that fresh fragmentation
// exists (no-op when maintenance is disabled).
func (s *StoreDirect) signalCompactionChurn() {
	m := &s.maint
	if !m.active.Load() {
		return // fast path: maintenance disabled
	}
	m.mu.Lock()
	t, ex := m.task, m.ex
	m.mu.Unlock()
	if t != nil && ex != nil {
		t.signalChurn()
		ex.Signal()
	}
}

// stopMaintenanceForClose is Close's first step: (owned) shut the
// executor down and join it — letting an in-flight bounded CompactStep
// finish and release the commit lock — or (attached) cancel the task.
// Done BEFORE Close takes the commit lock; concurrent closers all wait for
// it (maintOwner.stopForClose).
func (s *StoreDirect) stopMaintenanceForClose() {
	m := &s.maint
	m.stopForClose(func() {
		m.task = nil
		m.active.Store(false)
	})
}

// compactStepper is what the compaction task drives (StoreDirect; a test
// double injects faults — Java overrides compactStep in a subclass).
type compactStepper interface {
	CompactStep(maxPages int) int
	IsClosed() bool
}

const (
	compactBaseBackoff = int64(20 * time.Millisecond)
	compactMaxBackoff  = int64(2 * time.Second)
)

// directCompactionTask drives CompactStep in bounded batches. Each tick
// reclaims up to budget.MaxPages trailing pages; consecutive zero-progress
// ticks back off exponentially until a mutator signals new fragmentation.
// P7-safe: the store is correct whether this runs, is skipped or stops
// between steps (each step is crash-safe and bounded).
type directCompactionTask struct {
	store             compactStepper
	nextEligibleNanos atomic.Int64
	// failureBackoffUntil is the backoff from a FAILED step, kept separate
	// so signalChurn (fired on every delete) cannot erase it — otherwise
	// delete churn retries a faulting step at tick rate. Only a step with
	// progress clears it early; otherwise it expires.
	failureBackoffUntil atomic.Int64
	zeroStreak          atomic.Int32 // Java resets it from mutators too (signalChurn)
}

func newDirectCompactionTask(s compactStepper) *directCompactionTask {
	return &directCompactionTask{store: s}
}

func (t *directCompactionTask) Name() string           { return "direct-compaction" }
func (t *directCompactionTask) Kind() MaintenanceClass { return MaintenanceCompact }

func (t *directCompactionTask) WantsRun(nowNanos int64) bool {
	return !t.store.IsClosed() &&
		nowNanos >= max(t.nextEligibleNanos.Load(), t.failureBackoffUntil.Load())
}

func (t *directCompactionTask) Run(budget MaintenanceBudget) (r MaintenanceResult, err error) {
	var n int
	failed := true
	defer func() {
		if !failed {
			return
		}
		p := recover()
		if de, ok := mapdb5.AsDBError(p); ok && de.Kind == mapdb5.KindClosed {
			r, err = MaintenanceNoProgress(compactMaxBackoff), nil
			return
		}
		// A faulting step (e.g. DataCorruption) backs off like a
		// zero-progress tick; without this the executor would retry it
		// every tick forever.
		t.zeroStreak.Store(6)
		until := MaintenanceNow() + compactMaxBackoff
		t.nextEligibleNanos.Store(until)
		t.failureBackoffUntil.Store(until) // churn-proof: see field doc
		panic(p)                           // the executor records it as the task's last failure
	}()
	n = t.store.CompactStep(max(1, budget.MaxPages))
	failed = false
	now := MaintenanceNow()
	if n > 0 {
		t.zeroStreak.Store(0)
		t.nextEligibleNanos.Store(now)
		t.failureBackoffUntil.Store(0) // real progress: the fault is behind us
		return MaintenanceProgress(n), nil
	}
	streak := min(t.zeroStreak.Load()+1, 6)
	t.zeroStreak.Store(streak)
	backoff := min(compactMaxBackoff, compactBaseBackoff<<streak)
	t.nextEligibleNanos.Store(now + backoff)
	return MaintenanceNoProgress(backoff), nil
}

func (t *directCompactionTask) OnShutdown(bool) {}

// signalChurn is a mutator hint that fresh fragmentation exists: clear the
// zero-progress backoff so the next tick runs. Deliberately does NOT touch
// failureBackoffUntil — churn says there is new work, not that a faulting
// step stopped faulting.
func (t *directCompactionTask) signalChurn() {
	t.zeroStreak.Store(0)
	t.nextEligibleNanos.Store(0)
}
