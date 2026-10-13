package store

// Regression tests from the M8 review round 3 (gpt-6.1-sol): Close from
// OnShutdown callbacks and tasks, install races, panicking callbacks.

import (
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

type r3Store struct {
	start  func() *MaintenanceExecutor
	attach func(*MaintenanceExecutor)
	close  func() error
	closed func() bool
	owner  *maintOwner
}

func r3NewStore(t *testing.T, kind string) r3Store {
	t.Helper()
	if kind == "direct" {
		s := NewStoreDirectHeap(true)
		return r3Store{s.StartMaintenance, s.AttachMaintenance, s.Close, s.IsClosed, &s.maint.maintOwner}
	}
	s, err := OpenStoreWAL(filepath.Join(t.TempDir(), "wal"), true)
	if err != nil {
		t.Fatal(err)
	}
	return r3Store{s.StartMaintenance, s.AttachMaintenance, s.Close, s.IsClosed, &s.maint}
}

func r3Wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case x := <-ch:
		return x
	case <-time.After(2 * time.Second):
		t.Fatal("operation hung")
	}
	var zero T
	return zero
}

// Public-API reproducer: a cleanup callback closes the store that owns it.
// No production state is changed or mocked. Run each case in a separate
// go test process, since the defect leaves Close callers blocked forever.
func TestMaintR3ShutdownCallbackReentersClose(t *testing.T) {
	for _, kind := range []string{"direct", "wal"} {
		t.Run(kind, func(t *testing.T) {
			s := r3NewStore(t, kind)
			ex := s.start()
			entered := make(chan struct{})
			ex.Register(&funcTask{name: "cleanup-close", want: func(int64) bool { return false },
				shutdown: func(bool) { close(entered); s.close() }})
			finished := make(chan error, 1)
			go func() { finished <- s.close() }()
			r3Wait(t, entered)
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(200 * time.Millisecond):
				buf := make([]byte, 32768)
				n := runtime.Stack(buf, true)
				t.Logf("executorAlive=%v, storeClosed=%v\n%s", ex.testGoroutineAlive(), s.closed(), buf[:n])
				t.Fatal("OnShutdown -> store.Close deadlocked the initiating Close")
			}
		})
	}
}

func TestMaintR3TaskCloseMatrix(t *testing.T) {
	for _, kind := range []string{"direct", "wal"} {
		for _, owned := range []bool{true, false} {
			for _, stage := range []string{"before", "during", "after"} {
				if owned && stage == "after" {
					continue
				} // a joined executor runs no more tasks
				name := kind + "/attached/" + stage
				if owned {
					name = kind + "/owned/" + stage
				}
				t.Run(name, func(t *testing.T) {
					s := r3NewStore(t, kind)
					var ex *MaintenanceExecutor
					if owned {
						ex = s.start()
					} else {
						ex = NewMaintenanceExecutor("r3-shared")
						s.attach(ex)
					}
					entered, release, attempted := make(chan struct{}), make(chan struct{}), make(chan struct{})
					result := make(chan any, 1)
					var ran atomic.Bool
					ex.Register(&funcTask{name: "task-close", want: func(int64) bool { return !ran.Load() }, run: func(MaintenanceBudget) (MaintenanceResult, error) {
						ran.Store(true)
						close(entered)
						<-release
						close(attempted)
						func() {
							defer func() { result <- recover() }()
							if err := s.close(); err != nil {
								panic(err)
							}
						}()
						return MaintenanceResult{}, nil
					}})
					r3Wait(t, entered)
					first := make(chan error, 1)
					if stage == "during" {
						// Pause attached Cancel using the executor's mutex; no store lock is
						// held and the task itself does not hold this mutex.
						if !owned {
							ex.mu.Lock()
						}
						go func() { first <- s.close() }()
						if !waitFor(t, time.Second, func() bool { s.owner.mu.Lock(); defer s.owner.mu.Unlock(); return s.owner.shutdown }) {
							t.Fatal("ordinary Close did not begin")
						}
						close(release)
						r3Wait(t, attempted)
						if !owned {
							ex.mu.Unlock()
						}
					} else if stage == "after" {
						if err := s.close(); err != nil {
							t.Fatal(err)
						}
						close(release)
					} else {
						close(release)
					}
					p := r3Wait(t, result)
					if owned {
						if de, ok := mapdb5.AsDBError(p); !ok || de.Kind != mapdb5.KindMisuse {
							t.Fatalf("task Close = %v, want Misuse", p)
						}
					} else if p != nil {
						t.Fatalf("attached task Close panicked: %v", p)
					}
					if stage == "during" {
						if err := r3Wait(t, first); err != nil {
							t.Fatal(err)
						}
					}
					if err := s.close(); err != nil {
						t.Fatal(err)
					}
					if owned && ex.testGoroutineAlive() {
						t.Fatal("Close left owned executor alive")
					}
					if !owned && !ex.IsRunning() {
						t.Fatal("Close stopped shared executor")
					}
					ex.Shutdown(false)
				})
			}
		}
	}
}

func TestMaintR3ShutdownCallbackPanics(t *testing.T) {
	for _, kind := range []string{"direct", "wal"} {
		t.Run(kind, func(t *testing.T) {
			s := r3NewStore(t, kind)
			ex := s.start()
			ex.Register(&funcTask{name: "panic-on-shutdown", want: func(int64) bool { return false }, shutdown: func(bool) { panic("shutdown panic") }})
			done := make(chan error, 1)
			go func() { done <- s.close() }()
			if err := r3Wait(t, done); err != nil {
				t.Fatal(err)
			}
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-s.owner.stopped:
			default:
				t.Fatal("stopped channel left open")
			}
			if ex.testGoroutineAlive() {
				t.Fatal("executor left alive")
			}
		})
	}
}

type r3PanicHandle struct{}

func (r3PanicHandle) Cancel() { panic("injected Cancel panic") }

// Internal fault injection, not a claim that real maintHandle.Cancel panics.
func TestMaintR3CancelPanicClosesStopped(t *testing.T) {
	ex := NewMaintenanceExecutor("r3-panic-cancel")
	defer ex.Shutdown(false)
	o := maintOwner{ex: ex, handle: r3PanicHandle{}}
	var p any
	func() { defer func() { p = recover() }(); o.stopForClose(nil) }()
	if p == nil {
		t.Fatal("injected panic missing")
	}
	select {
	case <-o.stopped:
	default:
		t.Fatal("stopped left open after Cancel panic")
	}
	done := make(chan struct{})
	go func() { o.stopForClose(nil); close(done) }()
	r3Wait(t, done)
}

func TestMaintR3InstallRaceFirstClose(t *testing.T) {
	for _, kind := range []string{"direct", "wal"} {
		for _, attached := range []bool{false, true} {
			for n := 0; n < 100; n++ {
				s := r3NewStore(t, kind)
				external := NewMaintenanceExecutor("r3-install")
				gate := make(chan struct{})
				result := make(chan any, 2)
				ownedResult := make(chan *MaintenanceExecutor, 1)
				go func() {
					defer func() { result <- recover() }()
					<-gate
					if attached {
						s.attach(external)
					} else {
						ownedResult <- s.start()
					}
				}()
				go func() {
					defer func() { result <- recover() }()
					<-gate
					if err := s.close(); err != nil {
						panic(err)
					}
				}()
				close(gate)
				for k := 0; k < 2; k++ {
					if p := r3Wait(t, result); p != nil {
						if de, ok := mapdb5.AsDBError(p); !ok || de.Kind != mapdb5.KindClosed {
							t.Fatalf("%s: %v", kind, p)
						}
					}
				}
				if err := s.close(); err != nil {
					t.Fatal(err)
				}
				select {
				case ex := <-ownedResult:
					if ex.testGoroutineAlive() {
						t.Fatal("installed owner survived Close")
					}
				default:
				}
				if !external.IsRunning() || external.testTaskCount() != 0 {
					t.Fatal("shared executor stopped or retained store task")
				}
				external.Shutdown(false)
			}
		}
	}
}
