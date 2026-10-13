package store

// Regression tests for the M8 review (astra findings 1 and 2).

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

type failForceIO struct{}

func (failForceIO) Before(e walIoEvent) error {
	if e.kind == walOpForceData {
		return errors.New("injected force failure")
	}
	return nil
}

func TestWALFailClosedStillJoinsMaintenance(t *testing.T) {
	s, err := openStoreWALCfg(filepath.Join(t.TempDir(), "wal"), walOptions{threadSafe: true, io: failForceIO{}})
	if err != nil {
		t.Fatal(err)
	}
	ex := s.StartMaintenance()
	defer ex.Shutdown(false)
	Put(s, int64(42), ser.Long)
	mustWalPanic(t, mapdb5.KindVolumeIO, "failed commit", s.Commit)
	if !s.IsClosed() {
		t.Fatal("failure did not close WAL")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if ex.IsRunning() || ex.testGoroutineAlive() {
		t.Fatal("Close returned after a failed commit with the owned maintenance goroutine still running")
	}
}

func TestCloseFromOwnTaskKeepsExecutorJoinable(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "wal"}[wal], func(t *testing.T) {
			var closeStore func() error
			var ex *MaintenanceExecutor
			if wal {
				s, err := OpenStoreWAL(filepath.Join(t.TempDir(), "wal"), true)
				if err != nil {
					t.Fatal(err)
				}
				ex, closeStore = s.StartMaintenance(), s.Close
			} else {
				s := NewStoreDirectHeap(true)
				ex, closeStore = s.StartMaintenance(), s.Close
			}
			defer ex.Shutdown(false)
			defer closeStore()
			var ran atomic.Bool
			ex.Register(&funcTask{name: "close-owner", want: func(int64) bool { return !ran.Load() }, run: func(MaintenanceBudget) (MaintenanceResult, error) {
				ran.Store(true)
				closeStore()
				return MaintenanceResult{}, nil
			}})
			if !waitFor(t, time.Second, func() bool { return ex.TaskFailureCount() > 0 }) {
				t.Fatal("self-close did not report misuse")
			}
			if err := closeStore(); err != nil {
				t.Fatal(err)
			}
			if ex.IsRunning() || ex.testGoroutineAlive() {
				t.Fatal("ordinary Close cannot join the executor after rejected task Close")
			}
		})
	}
}
