package btree

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// Regression (fable release review #1): handle.Tracker used a WaitGroup whose
// Add raced Wait; CloseHandle racing concurrent Gets panicked "WaitGroup is reused".
func TestCloseHandleConcurrentOpsRace(t *testing.T) {
	const iters = 1000
	st := store.NewStoreOnHeap(true)
	var nonDBPanics atomic.Int64
	var first atomic.Value
	record := func(r any) {
		if _, ok := mapdb5.AsDBError(r); ok {
			return // expected KindClosed
		}
		nonDBPanics.Add(1)
		first.CompareAndSwap(nil, fmt.Sprint(r))
	}
	for it := 0; it < iters; it++ {
		m := NewBTreeMap(st, Config[int64, int64]{KeyFormat: ser.LongFormat, ValueFormat: ser.LongFormat, MaxNodeSize: 32})
		m.Put(1, 1)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						record(r)
					}
				}()
				<-start
				for {
					m.Get(1)
				}
			}()
		}
		close(start)
		runtime.Gosched()
		func() {
			defer func() {
				if r := recover(); r != nil {
					record(r)
				}
			}()
			m.CloseHandle()
		}()
		wg.Wait()
	}
	if n := nonDBPanics.Load(); n != 0 {
		msg := fmt.Sprint(first.Load())
		if !strings.Contains(msg, "WaitGroup") {
			t.Logf("unexpected panic kind: %s", msg)
		}
		t.Fatalf("%d non-DBError panics in %d iterations; first: %s", n, iters, msg)
	}
}
