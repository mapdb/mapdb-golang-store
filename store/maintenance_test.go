package store

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

// maintPayload is Java Fixtures.payload(a, b, n): deterministic bytes.
func maintPayload(a, b, n int) []byte {
	out := make([]byte, n)
	x := uint32(a*7919 + b*104729 + n)
	for i := range out {
		x = x*1664525 + 1013904223
		out[i] = byte(x >> 24)
	}
	return out
}

type funcTask struct {
	name     string
	run      func(MaintenanceBudget) (MaintenanceResult, error)
	want     func(int64) bool
	shutdown func(bool)
}

func (f *funcTask) Name() string           { return f.name }
func (f *funcTask) Kind() MaintenanceClass { return MaintenanceReclaim }
func (f *funcTask) WantsRun(n int64) bool {
	if f.want == nil {
		return true
	}
	return f.want(n)
}
func (f *funcTask) Run(b MaintenanceBudget) (MaintenanceResult, error) { return f.run(b) }
func (f *funcTask) OnShutdown(g bool) {
	if f.shutdown != nil {
		f.shutdown(g)
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestMaintenanceExecutorRunsTaskAndShutsDown(t *testing.T) {
	ex := NewMaintenanceExecutorWith("unit", 5*time.Millisecond, DefaultMaintenanceBudget())
	var runs, shutdowns atomic.Int32
	ex.Register(&funcTask{
		name: "unit-task",
		run: func(MaintenanceBudget) (MaintenanceResult, error) {
			if runs.Add(1) < 3 {
				return MaintenanceProgress(1), nil
			}
			return MaintenanceNoProgress(int64(time.Second)), nil
		},
		shutdown: func(bool) { shutdowns.Add(1) },
	})
	if !waitFor(t, 5*time.Second, func() bool { return runs.Load() >= 3 }) {
		t.Fatalf("task ran %d times, want >= 3", runs.Load())
	}
	if !ex.IsRunning() {
		t.Fatal("executor not running")
	}
	ex.Shutdown(true)
	if ex.testGoroutineAlive() {
		t.Fatal("goroutine not joined after Shutdown")
	}
	if shutdowns.Load() != 1 {
		t.Fatalf("OnShutdown fired %d times, want 1", shutdowns.Load())
	}
	// A second (and a concurrent) Shutdown is a no-op: callbacks exactly once.
	ex.Shutdown(true)
	if shutdowns.Load() != 1 {
		t.Fatalf("OnShutdown fired again on a second Shutdown")
	}
	mustWalPanic(t, mapdb5.KindClosed, "Register after Shutdown", func() {
		ex.Register(&funcTask{name: "late", run: func(MaintenanceBudget) (MaintenanceResult, error) {
			return MaintenanceResult{}, nil
		}})
	})
}

func TestMaintenanceExecutorFailingTaskDoesNotCrash(t *testing.T) {
	ex := NewMaintenanceExecutorWith("unit-fail", 5*time.Millisecond, DefaultMaintenanceBudget())
	defer ex.Shutdown(false)
	var good atomic.Int32
	ex.Register(&funcTask{name: "boom", run: func(MaintenanceBudget) (MaintenanceResult, error) {
		return MaintenanceResult{}, errors.New("boom")
	}})
	ex.Register(&funcTask{name: "panics", run: func(MaintenanceBudget) (MaintenanceResult, error) {
		var m map[int]int
		m[1] = 1 // runtime panic: must be recorded, not kill the process
		return MaintenanceResult{}, nil
	}})
	ex.Register(&funcTask{name: "bad-predicate", want: func(int64) bool { panic("predicate") },
		run: func(MaintenanceBudget) (MaintenanceResult, error) {
			t.Error("Run called although WantsRun panicked")
			return MaintenanceResult{}, nil
		}})
	ex.Register(&funcTask{name: "good", run: func(MaintenanceBudget) (MaintenanceResult, error) {
		good.Add(1)
		return MaintenanceNoProgress(0), nil
	}})
	if !waitFor(t, 3*time.Second, func() bool { return good.Load() >= 3 }) {
		t.Fatal("executor stopped running past failing tasks")
	}
	if !ex.IsRunning() {
		t.Fatal("executor not running")
	}
	if ex.TaskFailureCount() < 2 {
		t.Fatalf("failures recorded = %d, want >= 2", ex.TaskFailureCount())
	}
	if ex.LastTaskFailure() == nil {
		t.Fatal("LastTaskFailure is nil")
	}
}

func TestMaintenanceExecutorCancelAndSignal(t *testing.T) {
	ex := NewMaintenanceExecutorWith("unit-cancel", time.Hour, DefaultMaintenanceBudget())
	defer ex.Shutdown(true)
	var runs atomic.Int32
	h := ex.Register(&funcTask{name: "t", run: func(MaintenanceBudget) (MaintenanceResult, error) {
		runs.Add(1)
		return MaintenanceNoProgress(0), nil
	}})
	// Registration wakes the loop although the tick is an hour.
	if !waitFor(t, 3*time.Second, func() bool { return runs.Load() >= 1 }) {
		t.Fatal("Register did not wake the executor")
	}
	before := runs.Load()
	ex.Signal()
	if !waitFor(t, 3*time.Second, func() bool { return runs.Load() > before }) {
		t.Fatal("Signal did not wake the executor")
	}
	h.Cancel()
	if ex.testTaskCount() != 0 {
		t.Fatalf("task count after Cancel = %d", ex.testTaskCount())
	}
	stable := runs.Load()
	ex.Signal()
	time.Sleep(50 * time.Millisecond)
	if runs.Load() != stable {
		t.Fatal("cancelled task still runs")
	}
}

func TestMaintenanceExecutorShutdownFromOwnTaskPanics(t *testing.T) {
	ex := NewMaintenanceExecutorWith("unit-self", 5*time.Millisecond, DefaultMaintenanceBudget())
	defer ex.Shutdown(true)
	done := make(chan struct{})
	ex.Register(&funcTask{name: "self", run: func(MaintenanceBudget) (MaintenanceResult, error) {
		select {
		case <-done:
			return MaintenanceNoProgress(0), nil
		default:
		}
		defer close(done)
		ex.Shutdown(true) // must panic Misuse, not join itself forever
		return MaintenanceResult{}, nil
	}})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("self-shutdown hung")
	}
	if !waitFor(t, 3*time.Second, func() bool { return ex.TaskFailureCount() > 0 }) {
		t.Fatal("self-shutdown not recorded as a failure")
	}
	de, ok := mapdb5.AsDBError(ex.LastTaskFailure())
	if !ok || de.Kind != mapdb5.KindMisuse {
		t.Fatalf("self-shutdown failure = %v, want Misuse", ex.LastTaskFailure())
	}
	if !ex.IsRunning() {
		t.Fatal("executor stopped after a self-shutdown attempt")
	}
}

// A running bounded task finishes; Shutdown waits for it and never
// interrupts it.
func TestMaintenanceExecutorShutdownWaitsForRunningTask(t *testing.T) {
	ex := NewMaintenanceExecutorWith("unit-wait", 5*time.Millisecond, DefaultMaintenanceBudget())
	entered := make(chan struct{})
	var finished atomic.Bool
	var once atomic.Bool
	ex.Register(&funcTask{name: "slow", run: func(MaintenanceBudget) (MaintenanceResult, error) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			time.Sleep(100 * time.Millisecond)
			finished.Store(true)
		}
		return MaintenanceNoProgress(0), nil
	}})
	<-entered
	ex.Shutdown(true)
	if !finished.Load() {
		t.Fatal("Shutdown returned before the running task finished")
	}
}

func walTestCleaning(s *StoreWAL) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.cleaner != nil
}

// Java StoreWALCleanerTest.the_background_task_retires_a_segment_on_its_own:
// driven through the task directly, so the assertions are about what the
// task returns rather than goroutine timing.
func TestWALBackgroundTaskRetiresASegmentOnItsOwn(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	s.SetMinLogBytes(0) // no inline cleaning while we build
	s.SetSegmentBytes(256 << 10)
	last := maintPayload(1, 0, 60_000)
	r := Put(s, last, ser.ByteArray)
	s.Commit()
	for i := 1; i <= 80; i++ {
		last = maintPayload(1, i, 60_000)
		Update(s, r, last, ser.ByteArray)
		s.Commit()
	}
	grown := s.LogBytes()
	segsBefore := len(s.SegmentSeqs())
	if grown <= s.CurrentSize() {
		t.Fatalf("precondition: log %d must exceed the store footprint %d", grown, s.CurrentSize())
	}
	s.SetMinLogBytes(1)
	s.SetSpaceAmplification(1) // the trigger is now live
	if walTestCleaning(s) {
		t.Fatal("precondition: no cycle may be open")
	}

	task := &walCleanerTask{store: s}
	var res MaintenanceResult
	for ticks := 0; ticks < 5000; ticks++ {
		res, err = task.Run(DefaultMaintenanceBudget())
		if err != nil {
			t.Fatal(err)
		}
		if !res.MadeProgress {
			break
		}
	}
	if res.MadeProgress {
		t.Fatal("the task must reach a quiet state, not spin")
	}
	if got := len(s.SegmentSeqs()); got >= segsBefore {
		t.Fatalf("segments %d -> %d: the task retired nothing", segsBefore, got)
	}
	if s.LogBytes() >= grown {
		t.Fatalf("log did not shrink: %d -> %d", grown, s.LogBytes())
	}
	written, retired := s.CleanerBytes()
	if written <= 0 || retired <= 0 {
		t.Fatalf("cleaner accounting written=%d retired=%d", written, retired)
	}
	if got, _ := Get(s, r, ser.ByteArray); !bytes.Equal(got, last) {
		t.Fatal("committed state not intact")
	}
	// An idle run must report NO progress, or the executor never sleeps.
	if res, _ := task.Run(DefaultMaintenanceBudget()); res.MadeProgress {
		t.Fatal("an idle run must not claim progress")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, _ := Get(s2, r, ser.ByteArray); !bytes.Equal(got, last) {
		t.Fatal("reopened state differs")
	}
	s2.Verify()
}

// The same workload through a real executor: the background goroutine
// retires segments without any further commit.
func TestWALMaintenanceExecutorShrinksLog(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	s.SetMinLogBytes(0)
	s.SetSegmentBytes(256 << 10)
	last := maintPayload(2, 0, 60_000)
	r := Put(s, last, ser.ByteArray)
	s.Commit()
	for i := 1; i <= 80; i++ {
		last = maintPayload(2, i, 60_000)
		Update(s, r, last, ser.ByteArray)
		s.Commit()
	}
	grown := s.LogBytes()
	s.SetMinLogBytes(1)
	s.SetSpaceAmplification(1)
	ex := s.StartMaintenance()
	if ex2 := s.StartMaintenance(); ex2 != ex {
		t.Fatal("StartMaintenance is not idempotent")
	}
	if !waitFor(t, 10*time.Second, func() bool { return s.LogBytes() < grown/2 }) {
		t.Fatalf("background cleaner did not shrink the log: %d -> %d (failures %d, last %v)",
			grown, s.LogBytes(), ex.TaskFailureCount(), ex.LastTaskFailure())
	}
	if got, _ := Get(s, r, ser.ByteArray); !bytes.Equal(got, last) {
		t.Fatal("committed state not intact")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if ex.testGoroutineAlive() || ex.IsRunning() {
		t.Fatal("owned executor survived Close")
	}
	mustWalPanic(t, mapdb5.KindClosed, "StartMaintenance after Close", func() { s.StartMaintenance() })
	s2, err := OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.Verify()
	if got, _ := Get(s2, r, ser.ByteArray); !bytes.Equal(got, last) {
		t.Fatal("reopened state differs")
	}
}

// Java wal_maintenance_starts_and_shuts_down, plus concurrent writers while
// the executor runs (race detector), plus the attached (shared executor)
// mode: Close cancels the task, the shared executor keeps running.
func TestWALMaintenanceConcurrentAndAttached(t *testing.T) {
	shared := NewMaintenanceExecutorWith("shared", 2*time.Millisecond, DefaultMaintenanceBudget())
	defer shared.Shutdown(true)
	base := walBase(t)
	s, err := OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	s.SetSegmentBytes(MinSegmentBytes * 4)
	s.SetMinLogBytes(1)
	s.SetSpaceAmplification(1)
	s.AttachMaintenance(shared)
	mustWalPanic(t, mapdb5.KindMisuse, "double attach", func() { s.AttachMaintenance(shared) })
	recids := make([]Recid, 8)
	for i := range recids {
		recids[i] = Put(s, maintPayload(i, 0, 500), ser.ByteArray)
	}
	s.Commit()
	done := make(chan struct{})
	errc := make(chan any, 4)
	for w := 0; w < 4; w++ {
		go func(w int) {
			defer func() { errc <- recover() }()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				r := recids[(w*2+i)%len(recids)]
				if _, ok := Get(s, r, ser.ByteArray); !ok {
					panic("record vanished")
				}
			}
		}(w)
	}
	final := make([][]byte, len(recids))
	for round := 1; round <= 200; round++ {
		for i, r := range recids {
			final[i] = maintPayload(i, round, 500+round)
			Update(s, r, final[i], ser.ByteArray)
		}
		s.Commit()
	}
	close(done)
	for w := 0; w < 4; w++ {
		if p := <-errc; p != nil {
			t.Fatalf("reader panicked: %v", p)
		}
	}
	s.Verify()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !shared.IsRunning() {
		t.Fatal("Close shut down a shared executor it does not own")
	}
	if shared.testTaskCount() != 0 {
		t.Fatalf("Close left %d task(s) registered on the shared executor", shared.testTaskCount())
	}
	if shared.TaskFailureCount() != 0 {
		t.Fatalf("cleaner task failed: %v", shared.LastTaskFailure())
	}
	s2, err := OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.Verify()
	for i, r := range recids {
		if got, _ := Get(s2, r, ser.ByteArray); !bytes.Equal(got, final[i]) {
			t.Fatalf("recid %d differs after reopen", r)
		}
	}
}

func TestWALMaintenanceReadOnlyRefused(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	Put(s, []byte("x"), ser.ByteArray)
	s.Commit()
	s.Close()
	ro, err := OpenStoreWALRO(base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	mustWalPanic(t, mapdb5.KindReadOnly, "StartMaintenance read-only", func() { ro.StartMaintenance() })
	if n := ro.maintenanceCleanStep(DefaultMaintenanceBudget()); n != -1 {
		t.Fatalf("read-only clean step = %d, want -1", n)
	}
}
