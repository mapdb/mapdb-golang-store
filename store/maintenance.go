package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/internal/handle"
)

// MaintenanceMode is whether a store runs a background MaintenanceExecutor
// (R6). MaintenanceDisabled is the default and keeps everything correct via
// synchronous fallbacks ("disabled = still correct"): compaction and
// checkpoint remain callable inline; correctness never depends on the
// background goroutine — only file/log size and latency do.
type MaintenanceMode int

const (
	MaintenanceDisabled MaintenanceMode = iota
	MaintenanceEnabled
)

// MaintenanceClass is the kind of deferred work a MaintenanceTask performs.
type MaintenanceClass int

const (
	// MaintenanceCompact is page consolidation / incremental compaction
	// (StoreDirect.CompactStep).
	MaintenanceCompact MaintenanceClass = iota
	// MaintenanceDemote is hybrid-cache demotion; owned by a cache layer
	// that does not exist yet (Java R5).
	MaintenanceDemote
	// MaintenanceCheckpoint is WAL checkpoint / log truncation.
	MaintenanceCheckpoint
	// MaintenanceReclaim is free-space / recid reclamation.
	MaintenanceReclaim
)

func (c MaintenanceClass) String() string {
	switch c {
	case MaintenanceCompact:
		return "COMPACT"
	case MaintenanceDemote:
		return "DEMOTE"
	case MaintenanceCheckpoint:
		return "CHECKPOINT"
	case MaintenanceReclaim:
		return "RECLAIM"
	}
	return fmt.Sprintf("MaintenanceClass(%d)", int(c))
}

// MaintenanceBudget is the bounded work allowance handed to
// MaintenanceTask.Run. Budgets keep each background tick short so the store
// stays available between ticks. A task honours whichever fields apply to
// it and stops at the first limit reached.
type MaintenanceBudget struct {
	// MaxPages is the most whole pages a page-oriented task (compaction)
	// may touch this tick.
	MaxPages int
	// MaxRecords is the most records a record-oriented task may touch
	// (0 = unbounded / not applicable).
	MaxRecords int
	// MaxBytes is the most bytes a byte-oriented task may move
	// (0 = unbounded / not applicable).
	MaxBytes int
	// MaxNanos is a soft wall-clock ceiling for the tick (0 = none).
	MaxNanos int64
	// MayFsync is whether the task may fsync during this tick.
	MayFsync bool
}

// DefaultMaintenanceBudget is the conservative default:
// DefaultCompactStepPages pages, ~20 ms, fsync allowed.
func DefaultMaintenanceBudget() MaintenanceBudget {
	return MaintenanceBudget{
		MaxPages: DefaultCompactStepPages,
		MaxNanos: int64(20 * time.Millisecond),
		MayFsync: true,
	}
}

// MaintenanceResult is the outcome of one MaintenanceTask.Run tick.
type MaintenanceResult struct {
	PagesDone    int
	BytesDone    int64
	MadeProgress bool
	// MoreSoon hints that useful work likely remains and the executor
	// should reschedule promptly.
	MoreSoon bool
	// RetryAfterNanos is the suggested delay before the task is worth
	// running again (advisory).
	RetryAfterNanos int64
}

// MaintenanceProgress reports progress over pagesDone pages, probably with
// more to do.
func MaintenanceProgress(pagesDone int) MaintenanceResult {
	return MaintenanceResult{PagesDone: pagesDone, MadeProgress: true, MoreSoon: true}
}

// MaintenanceProgressBytes reports progress moving bytesDone bytes,
// probably with more to do.
func MaintenanceProgressBytes(bytesDone int64) MaintenanceResult {
	return MaintenanceResult{BytesDone: bytesDone, MadeProgress: true, MoreSoon: true}
}

// MaintenanceNoProgress reports no progress this tick; back off for at least
// retryAfterNanos.
func MaintenanceNoProgress(retryAfterNanos int64) MaintenanceResult {
	return MaintenanceResult{RetryAfterNanos: retryAfterNanos}
}

// MaintenanceTask is a unit of deferred, droppable work registered with a
// MaintenanceExecutor. Every task must be P7-safe: skipping it, or the
// executor stopping between ticks, leaves the store correct — only
// unperformed maintenance results. Run enters its store only through public
// store APIs that already enforce the lock order; the executor holds no
// store lock while selecting, sleeping or dispatching.
type MaintenanceTask interface {
	Name() string
	Kind() MaintenanceClass
	// WantsRun is a cheap predicate: is this task worth running at
	// nowNanos (MaintenanceNow's clock)?
	WantsRun(nowNanos int64) bool
	// Run does a bounded slice of work within budget. A returned error or
	// a panic is recorded by the executor (LastTaskFailure), which keeps
	// running.
	Run(budget MaintenanceBudget) (MaintenanceResult, error)
	// OnShutdown is called once when the executor stops; graceful is a
	// clean shutdown vs an abrupt drop.
	OnShutdown(graceful bool)
}

var monoBase = time.Now()

// MaintenanceNow is the monotonic clock maintenance tasks are scheduled on
// (Java System.nanoTime: an arbitrary origin, only differences mean
// anything).
func MaintenanceNow() int64 { return int64(time.Since(monoBase)) }

// MaintenanceHandle is returned by Register; Cancel deregisters the task
// without shutting the executor down.
type MaintenanceHandle interface {
	Cancel()
}

const (
	execRunning int32 = iota
	execStopping
	execStopped
)

// DefaultMaintenanceTick is how long an idle executor sleeps between passes
// when nothing signalled it.
const DefaultMaintenanceTick = 50 * time.Millisecond

// MaintenanceExecutor is the single owner of a store's deferred work (R6).
// One goroutine drives a small registry of MaintenanceTasks under bounded
// MaintenanceBudgets.
//
// P7, disabled = still correct: the executor is entirely optional. A store
// without one relies on synchronous fallbacks (CompactStep / Compact /
// Checkpoint and commit's inline WAL clean); with maintenance enabled the
// SAME operations are merely also driven in the background.
//
// Locks: the executor holds no store lock while selecting tasks, sleeping
// or dispatching; a task enters its store only through public APIs.
//
// Shutdown flips to stopping, wakes the goroutine and joins it: a running
// bounded task finishes (or reaches its budget), then the loop exits — the
// executor never interrupts a task mid-write. OnShutdown then fires once
// for each task. A store's Close shuts down its owned executor BEFORE
// taking any store lock, so an in-flight task holding a store lock can
// finish and release it.
type MaintenanceExecutor struct {
	name   string
	tick   time.Duration
	budget MaintenanceBudget

	mu    sync.Mutex // serializes register vs the stopping transition
	state atomic.Int32
	tasks atomic.Pointer[[]*maintEntry] // copy-on-write registry
	wake  chan struct{}                 // cap 1: a pending signal (Java's `pending`)
	done  chan struct{}                 // closed when the loop goroutine exits
	goid  atomic.Uint64                 // the loop goroutine, for the self-join tripwire

	failMu       sync.Mutex
	lastFailure  error
	failureCount atomic.Int64

	shutdownCallbacksRun atomic.Bool
}

type maintEntry struct {
	task MaintenanceTask
}

type maintHandle struct {
	ex *MaintenanceExecutor
	e  *maintEntry
}

func (h maintHandle) Cancel() { h.ex.remove(h.e) }

// NewMaintenanceExecutor starts an executor with the default tick and
// budget.
func NewMaintenanceExecutor(name string) *MaintenanceExecutor {
	return NewMaintenanceExecutorWith(name, DefaultMaintenanceTick, DefaultMaintenanceBudget())
}

// NewMaintenanceExecutorWith starts an executor with an explicit tick and
// budget. Its goroutine runs until Shutdown.
func NewMaintenanceExecutorWith(name string, tick time.Duration, budget MaintenanceBudget) *MaintenanceExecutor {
	if tick <= 0 {
		panic(mapdb5.NewMisuse("maintenance tick must be positive"))
	}
	ex := &MaintenanceExecutor{
		name: name, tick: tick, budget: budget,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	empty := []*maintEntry{}
	ex.tasks.Store(&empty)
	go ex.loop()
	return ex
}

// Name is the name the executor was created with.
func (ex *MaintenanceExecutor) Name() string { return ex.name }

// Register adds a task; the executor wakes to consider it immediately.
// Panics KindClosed once the executor is stopping or stopped.
func (ex *MaintenanceExecutor) Register(task MaintenanceTask) MaintenanceHandle {
	if task == nil {
		panic(mapdb5.NewMisuse("maintenance task is nil"))
	}
	e := &maintEntry{task: task}
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if ex.state.Load() != execRunning {
		panic(mapdb5.New(mapdb5.KindClosed, "maintenance executor is not running"))
	}
	old := *ex.tasks.Load()
	next := make([]*maintEntry, len(old), len(old)+1)
	copy(next, old)
	next = append(next, e)
	ex.tasks.Store(&next)
	ex.Signal()
	return maintHandle{ex: ex, e: e}
}

func (ex *MaintenanceExecutor) remove(e *maintEntry) {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	old := *ex.tasks.Load()
	for i, x := range old {
		if x == e {
			next := make([]*maintEntry, 0, len(old)-1)
			next = append(next, old[:i]...)
			next = append(next, old[i+1:]...)
			ex.tasks.Store(&next)
			return
		}
	}
}

// Signal wakes the loop (e.g. after churn that likely created new work).
// Never blocks; signals arriving while one is pending coalesce.
func (ex *MaintenanceExecutor) Signal() {
	select {
	case ex.wake <- struct{}{}:
	default:
	}
}

// IsRunning reports whether the executor still accepts and runs tasks.
func (ex *MaintenanceExecutor) IsRunning() bool { return ex.state.Load() == execRunning }

// LastTaskFailure is the last error a task returned (or panicked with) from
// Run, or nil; the executor keeps running.
func (ex *MaintenanceExecutor) LastTaskFailure() error {
	ex.failMu.Lock()
	defer ex.failMu.Unlock()
	return ex.lastFailure
}

// TaskFailureCount is the number of task runs that ended in a failure.
func (ex *MaintenanceExecutor) TaskFailureCount() int64 { return ex.failureCount.Load() }

func (ex *MaintenanceExecutor) recordFailure(err error) {
	ex.failMu.Lock()
	ex.lastFailure = err
	ex.failMu.Unlock()
	ex.failureCount.Add(1)
}

func (ex *MaintenanceExecutor) loop() {
	ex.goid.Store(handle.Goid())
	defer close(ex.done)
	defer ex.goid.Store(0) // goroutine identities are recycled: never outlive the loop
	defer ex.state.Store(execStopped)
	timer := time.NewTimer(ex.tick)
	defer timer.Stop()
	for ex.state.Load() == execRunning {
		moreSoon := false
		now := MaintenanceNow()
		for _, e := range *ex.tasks.Load() {
			if ex.state.Load() != execRunning {
				break
			}
			if !ex.wants(e.task, now) {
				continue
			}
			if r, ok := ex.runTask(e.task); ok && r.MoreSoon {
				moreSoon = true
			}
		}
		if ex.state.Load() != execRunning {
			break
		}
		if moreSoon {
			// Java: a pending signal is consumed without waiting.
			select {
			case <-ex.wake:
			default:
			}
			continue
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(ex.tick)
		select {
		case <-ex.wake:
		case <-timer.C:
		}
	}
}

// wants evaluates WantsRun; a misbehaving predicate must not kill the
// executor (P7).
func (ex *MaintenanceExecutor) wants(t MaintenanceTask, now int64) (want bool) {
	defer func() {
		if recover() != nil {
			want = false
		}
	}()
	return t.WantsRun(now)
}

// runTask runs one tick, recording an error or panic as the task's failure
// (P7: a failing task must not crash the executor or the store). Go has no
// separate Error hierarchy, so every panic is recorded — an unrecovered
// panic on this goroutine would take the whole process down.
func (ex *MaintenanceExecutor) runTask(t MaintenanceTask) (r MaintenanceResult, ok bool) {
	defer func() {
		if p := recover(); p != nil {
			err, isErr := p.(error)
			if !isErr {
				err = fmt.Errorf("maintenance task %q panicked: %v", t.Name(), p)
			}
			ex.recordFailure(err)
			ok = false
		}
	}()
	r, err := t.Run(ex.budget)
	if err != nil {
		ex.recordFailure(err)
		return r, false
	}
	return r, true
}

// Shutdown stops the executor: no new task runs start, the currently
// running bounded task finishes, the goroutine is joined, then OnShutdown
// fires exactly once for each task (even under concurrent Shutdown calls).
// Calling it from inside a task (the executor's own goroutine) panics
// KindMisuse instead of joining itself forever.
func (ex *MaintenanceExecutor) Shutdown(graceful bool) {
	ex.checkNotOwnTask()
	ex.mu.Lock()
	if ex.state.Load() != execStopped {
		ex.state.Store(execStopping)
	}
	ex.mu.Unlock()
	ex.Signal()
	<-ex.done
	if !ex.shutdownCallbacksRun.CompareAndSwap(false, true) {
		return
	}
	for _, e := range *ex.tasks.Load() {
		func() {
			defer func() { _ = recover() }() // shutdown callbacks are best-effort
			e.task.OnShutdown(graceful)
		}()
	}
	empty := []*maintEntry{}
	ex.tasks.Store(&empty)
}

// checkNotOwnTask panics Misuse when called from one of this executor's
// tasks (its own goroutine), where a join could never return. Owners call
// it BEFORE changing any shutdown bookkeeping, so a rejected Close leaves
// the executor reachable for a later Close.
func (ex *MaintenanceExecutor) checkNotOwnTask() {
	if g := ex.goid.Load(); g != 0 && g == handle.Goid() {
		panic(mapdb5.NewMisuse("maintenance executor shut down from its own task"))
	}
}

// maintOwner is a store's maintenance ownership state (Java: the
// synchronized maintenance fields of StoreDirect / StoreWAL), shared by
// both stores so their Close paths cannot drift apart.
type maintOwner struct {
	mu       sync.Mutex
	ex       *MaintenanceExecutor
	owns     bool
	handle   MaintenanceHandle
	shutdown bool // refuses Start/Attach racing Close
	// stopped is closed once the Close that began the shutdown finished
	// stopping (joined the owned executor or cancelled the task); later
	// closers wait on it so no Close returns before maintenance is gone.
	stopped chan struct{}
	joining *MaintenanceExecutor // the owned executor being joined, for the self-task check
	// stopper is the goroutine running the shutdown (0 when none). Its
	// OnShutdown callbacks run on it after the join; a Close they make
	// must not wait for the very call that is running them.
	stopper uint64
}

// stopForClose is a store Close's first step, run BEFORE the store takes
// its own locks so an in-flight task can finish and release them. The
// first caller shuts an owned executor down (join) or cancels an attached
// task; concurrent and later callers wait until that has completed. A call
// from one of the owned executor's own tasks panics Misuse before changing
// anything (it could never be joined), leaving a later Close able to finish.
// onBegin runs under mu when the shutdown begins (store-specific fields).
func (o *maintOwner) stopForClose(onBegin func()) {
	o.mu.Lock()
	if !o.shutdown && o.owns && o.ex != nil {
		ex := o.ex
		o.mu.Unlock()
		ex.checkNotOwnTask()
		o.mu.Lock()
		// Start returns the existing owner and Attach rejects a held
		// handle, so ownership cannot change in this window; only another
		// Close can have begun the shutdown, which the next branch handles.
	}
	if o.shutdown {
		ch, joining, stopper := o.stopped, o.joining, o.stopper
		o.mu.Unlock()
		if stopper != 0 && stopper == handle.Goid() {
			// Re-entered from an OnShutdown callback of the shutdown this
			// goroutine is running: the executor is already joined (the
			// callbacks run after the join), so there is nothing to wait for.
			return
		}
		if joining != nil {
			joining.checkNotOwnTask() // waiting on its own join would hang
		}
		<-ch
		return
	}
	o.shutdown = true
	o.stopped = make(chan struct{})
	o.stopper = handle.Goid()
	ex, owns, h := o.ex, o.owns, o.handle
	if owns {
		o.joining = ex
	}
	o.ex, o.handle = nil, nil
	if onBegin != nil {
		onBegin()
	}
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		o.stopper = 0 // goroutine identities are recycled: never outlive the call
		o.mu.Unlock()
		close(o.stopped)
	}()
	if ex == nil {
		return
	}
	if owns {
		ex.Shutdown(true)
	} else if h != nil {
		h.Cancel()
	}
}

// ---- test hooks ----

func (ex *MaintenanceExecutor) testGoroutineAlive() bool {
	select {
	case <-ex.done:
		return false
	default:
		return true
	}
}

func (ex *MaintenanceExecutor) testTaskCount() int { return len(*ex.tasks.Load()) }
