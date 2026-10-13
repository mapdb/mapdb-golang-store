package store

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

func openDirectFile(t *testing.T, path string) *StoreDirect {
	t.Helper()
	s, err := NewStoreDirectFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func reopenVerify(t *testing.T, path string) {
	t.Helper()
	re := openDirectFile(t, path)
	defer re.Close()
	re.Verify()
}

// Java MaintenanceExecutorTest.disabled_maintenance_is_correct.
func TestDirectDisabledMaintenanceIsCorrect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.db")
	s := openDirectFile(t, path)
	keeper := Put(s, maintPayload(1, 1, 1000), ser.ByteArray)
	var doomed []Recid
	for i := 0; i < 30; i++ {
		doomed = append(doomed, Put(s, maintPayload(100+i, 1, 120_000), ser.ByteArray))
	}
	full := s.fileTail()
	for _, r := range doomed {
		s.Delete(r, nil)
	}
	reclaimed := s.CompactIncremental() // no goroutine exists; synchronous compaction still works
	s.Verify()
	if reclaimed <= 0 || s.fileTail() >= full {
		t.Fatalf("reclaimed %d, tail %d -> %d", reclaimed, full, s.fileTail())
	}
	if got, _ := Get(s, keeper, ser.ByteArray); !bytes.Equal(got, maintPayload(1, 1, 1000)) {
		t.Fatal("keeper changed")
	}
	s.Close()
	reopenVerify(t, path)
}

// Java executor_drives_compaction_to_completion + clean_shutdown.
func TestDirectExecutorDrivesCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.db")
	s := openDirectFile(t, path)
	ex := s.StartMaintenance()
	if s.StartMaintenance() != ex {
		t.Fatal("StartMaintenance is not idempotent")
	}
	keeper := Put(s, maintPayload(1, 1, 1000), ser.ByteArray)
	var doomed []Recid
	for i := 0; i < 50; i++ {
		doomed = append(doomed, Put(s, maintPayload(200+i, 1, 150_000), ser.ByteArray))
	}
	full := s.fileTail()
	for _, r := range doomed {
		s.Delete(r, nil) // deletes signal the compaction task
	}
	if !waitFor(t, 10*time.Second, func() bool { return s.fileTail() < full/2 }) {
		t.Fatalf("background compaction did not shrink the file: %d -> %d (failures %d, last %v)",
			full, s.fileTail(), ex.TaskFailureCount(), ex.LastTaskFailure())
	}
	s.Verify()
	if got, _ := Get(s, keeper, ser.ByteArray); !bytes.Equal(got, maintPayload(1, 1, 1000)) {
		t.Fatal("keeper changed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if ex.testGoroutineAlive() {
		t.Fatal("maintenance goroutine survived Close")
	}
	mustWalPanic(t, mapdb5.KindClosed, "StartMaintenance after Close", func() { s.StartMaintenance() })
	reopenVerify(t, path)
}

// Writers, readers and deletes race the background compactor (-race); the
// shadow map must match at the end and after reopen.
func TestDirectMaintenanceUnderLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.db")
	s := openDirectFile(t, path)
	shared := NewMaintenanceExecutorWith("shared", time.Millisecond, DefaultMaintenanceBudget())
	defer shared.Shutdown(true)
	s.AttachMaintenance(shared)
	mustWalPanic(t, mapdb5.KindMisuse, "double attach", func() { s.AttachMaintenance(shared) })
	shadow := map[Recid][]byte{}
	var live []Recid
	stop := make(chan struct{})
	readerErr := make(chan error, 1)
	var keep atomic.Pointer[[]Recid]
	go func() {
		defer close(readerErr)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if rs := keep.Load(); rs != nil {
				for _, r := range *rs {
					if _, ok := Get(s, r, ser.ByteArray); !ok {
						readerErr <- errors.New("stable record vanished")
						return
					}
				}
			}
		}
	}()
	stable := []Recid{}
	for i := 0; i < 20; i++ {
		v := maintPayload(9000+i, 0, 300)
		r := Put(s, v, ser.ByteArray)
		shadow[r] = v
		stable = append(stable, r)
	}
	keep.Store(&stable)
	for round := 0; round < 30; round++ {
		for i := 0; i < 20; i++ {
			v := maintPayload(round, i, 20_000+i*1000)
			r := Put(s, v, ser.ByteArray)
			shadow[r] = v
			live = append(live, r)
		}
		for len(live) > 5 {
			r := live[0]
			live = live[1:]
			s.Delete(r, nil)
			delete(shadow, r)
		}
		if round%5 == 0 {
			s.Commit()
		}
	}
	close(stop)
	if err := <-readerErr; err != nil {
		t.Fatal(err)
	}
	s.Verify()
	for r, v := range shadow {
		if got, _ := Get(s, r, ser.ByteArray); !bytes.Equal(got, v) {
			t.Fatalf("recid %d differs", r)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !shared.IsRunning() || shared.testTaskCount() != 0 {
		t.Fatal("Close must cancel the task and leave the shared executor running")
	}
	if shared.TaskFailureCount() != 0 {
		t.Fatalf("compaction task failed: %v", shared.LastTaskFailure())
	}
	re := openDirectFile(t, path)
	defer re.Close()
	re.Verify()
	for r, v := range shadow {
		if got, _ := Get(re, r, ser.ByteArray); !bytes.Equal(got, v) {
			t.Fatalf("recid %d differs after reopen", r)
		}
	}
}

// faultableStepper is Java's FaultableStore: CompactStep throws an
// injectable fault before touching the store.
type faultableStepper struct {
	*StoreDirect
	fault atomic.Pointer[mapdb5.DBError]
}

func (f *faultableStepper) CompactStep(maxPages int) int {
	if e := f.fault.Load(); e != nil {
		panic(e)
	}
	return f.StoreDirect.CompactStep(maxPages)
}

// Java failure_backoff_survives_churn_and_clears_on_progress.
func TestDirectFailureBackoffSurvivesChurn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.db")
	s := &faultableStepper{StoreDirect: openDirectFile(t, path)}
	defer s.Close()
	var doomed []Recid
	for i := 0; i < 30; i++ {
		doomed = append(doomed, Put(s, maintPayload(i, 1, 120_000), ser.ByteArray))
	}
	for _, r := range doomed {
		s.Delete(r, nil)
	}
	task := newDirectCompactionTask(s)
	if !task.WantsRun(MaintenanceNow()) {
		t.Fatal("fresh task not eligible")
	}
	// (a) a faulting step backs the task off (and re-panics for the executor)
	s.fault.Store(mapdb5.NewCorruption("step fault"))
	func() {
		defer func() {
			de, ok := mapdb5.AsDBError(recover())
			if !ok || de.Msg != "step fault" {
				t.Fatalf("expected the injected step fault, got %v", de)
			}
		}()
		task.Run(DefaultMaintenanceBudget())
	}()
	if task.WantsRun(MaintenanceNow()) {
		t.Fatal("failed step must back off")
	}
	// (b) delete churn must NOT erase the failure backoff
	task.signalChurn()
	if task.WantsRun(MaintenanceNow()) {
		t.Fatal("churn must not erase the failure backoff")
	}
	// (c1) the backoff is bounded
	if !task.WantsRun(MaintenanceNow() + int64(3*time.Second)) {
		t.Fatal("failure backoff must expire")
	}
	// (c2) a step with progress clears the failure backoff immediately
	s.fault.Store(nil)
	r, err := task.Run(DefaultMaintenanceBudget())
	if err != nil || !r.MadeProgress || r.PagesDone <= 0 {
		t.Fatalf("recovered step: %+v %v", r, err)
	}
	if !task.WantsRun(MaintenanceNow()) {
		t.Fatal("progress must clear the failure backoff")
	}
	// zero-progress ticks back off exponentially; churn clears that
	for task.WantsRun(MaintenanceNow()) {
		if r, _ := task.Run(DefaultMaintenanceBudget()); !r.MadeProgress {
			break
		}
	}
	if task.WantsRun(MaintenanceNow()) {
		t.Fatal("zero-progress tick must back off")
	}
	task.signalChurn()
	if !task.WantsRun(MaintenanceNow()) {
		t.Fatal("churn must clear a zero-progress backoff")
	}
}
