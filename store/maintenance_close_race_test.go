package store

// Regression tests for the M8 review round 2 (sol finding 1): concurrent
// closers all wait for maintenance to stop; Start/Attach racing Close.
import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentCloseWaitsForMaintenanceJoin(t *testing.T) {
	for _, kind := range []string{"direct", "wal", "wal-failClosed"} {
		t.Run(kind, func(t *testing.T) {
			var closeStore func() error
			var ex *MaintenanceExecutor
			var closed func() bool
			var fail func()
			if kind == "direct" {
				s := NewStoreDirectHeap(true)
				ex, closeStore, closed = s.StartMaintenance(), s.Close, s.IsClosed
			} else {
				opts := walOptions{threadSafe: true}
				if kind == "wal-failClosed" {
					opts.io = failForceIO{}
				}
				s, err := openStoreWALCfg(filepath.Join(t.TempDir(), "wal"), opts)
				if err != nil {
					t.Fatal(err)
				}
				ex, closeStore, closed = s.StartMaintenance(), s.Close, s.IsClosed
				if kind == "wal-failClosed" {
					fail = func() {
						Put(s, int64(42), ser.Long)
						mustWalPanic(t, mapdb5.KindVolumeIO, "failed commit", s.Commit)
					}
				}
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var ran atomic.Bool
			ex.Register(&funcTask{name: "blocked-bounded-task", want: func(int64) bool { return !ran.Load() }, run: func(MaintenanceBudget) (MaintenanceResult, error) {
				ran.Store(true)
				close(entered)
				<-release
				return MaintenanceResult{}, nil
			}})
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("task did not start")
			}
			if fail != nil {
				fail()
			}
			first := make(chan error, 1)
			go func() { first <- closeStore() }()
			if !waitFor(t, time.Second, func() bool { return ex.state.Load() == execStopping }) {
				t.Fatal("first Close did not start shutdown")
			}
			second := make(chan error, 1)
			go func() { second <- closeStore() }()
			returnedEarly := false
			select {
			case err := <-second:
				if err != nil {
					t.Error(err)
				}
				returnedEarly = true
				t.Logf("second Close returned: storeClosed=%v executorAlive=%v firstClosePending=true", closed(), ex.testGoroutineAlive())
			case <-time.After(100 * time.Millisecond):
			}
			close(release)
			if err := <-first; err != nil {
				t.Error(err)
			}
			if !returnedEarly {
				if err := <-second; err != nil {
					t.Error(err)
				}
			}
			ex.Shutdown(false)
			if returnedEarly {
				t.Fatal("concurrent Close returned before the owned executor finished")
			}
		})
	}
}

func TestStartAttachRaceClose(t *testing.T) {
	for _, kind := range []string{"direct", "wal"} {
		for _, attached := range []bool{false, true} {
			for n := 0; n < 150; n++ {
				var start func() *MaintenanceExecutor
				var attach func(*MaintenanceExecutor)
				var closeStore func() error
				if kind == "direct" {
					s := NewStoreDirectHeap(true)
					start, attach, closeStore = s.StartMaintenance, s.AttachMaintenance, s.Close
				} else {
					s, err := OpenStoreWAL(filepath.Join(t.TempDir(), "wal"), true)
					if err != nil {
						t.Fatal(err)
					}
					start, attach, closeStore = s.StartMaintenance, s.AttachMaintenance, s.Close
				}
				var owned *MaintenanceExecutor
				external := NewMaintenanceExecutor("external")
				if attached {
					attach(external)
				} else {
					owned = start()
				}
				gate := make(chan struct{})
				results := make(chan any, 3)
				for _, op := range []func(){func() { start() }, func() { attach(external) }, func() {
					if err := closeStore(); err != nil {
						panic(err)
					}
				}} {
					go func(op func()) {
						var p any
						defer func() {
							if r := recover(); r != nil {
								p = r
							}
							results <- p
						}()
						<-gate
						op()
					}(op)
				}
				close(gate)
				for k := 0; k < 3; k++ {
					if p := <-results; p != nil {
						de, ok := mapdb5.AsDBError(p)
						if !ok || (de.Kind != mapdb5.KindClosed && de.Kind != mapdb5.KindMisuse) {
							t.Fatalf("%s: %v", kind, p)
						}
					}
				}
				if err := closeStore(); err != nil {
					t.Fatal(err)
				}
				if owned != nil && owned.testGoroutineAlive() {
					t.Fatal("owned executor survived single Close")
				}
				if !external.IsRunning() || external.testTaskCount() != 0 {
					t.Fatal("external executor was stopped or retained a task")
				}
				external.Shutdown(false)
			}
		}
	}
}

// A task of the owned executor that calls Close while another goroutine's
// Close is joining that executor must fail fast (Misuse), not wait on its
// own join forever.
func TestCloseFromTaskDuringJoinFailsFast(t *testing.T) {
	s := NewStoreDirectHeap(true)
	ex := s.StartMaintenance()
	entered, release := make(chan struct{}), make(chan struct{})
	taskErr := make(chan any, 1)
	var ran atomic.Bool
	ex.Register(&funcTask{name: "closer", want: func(int64) bool { return !ran.Load() },
		run: func(MaintenanceBudget) (MaintenanceResult, error) {
			ran.Store(true)
			close(entered)
			<-release
			func() {
				defer func() { taskErr <- recover() }()
				s.Close()
			}()
			return MaintenanceResult{}, nil
		}})
	<-entered
	first := make(chan error, 1)
	go func() { first <- s.Close() }()
	if !waitFor(t, time.Second, func() bool { return ex.state.Load() == execStopping }) {
		t.Fatal("first Close did not start shutdown")
	}
	close(release)
	select {
	case p := <-taskErr:
		if de, ok := mapdb5.AsDBError(p); !ok || de.Kind != mapdb5.KindMisuse {
			t.Fatalf("Close from the task during the join: %v, want Misuse", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close from the task hung on its own join")
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if ex.testGoroutineAlive() {
		t.Fatal("executor survived Close")
	}
}
