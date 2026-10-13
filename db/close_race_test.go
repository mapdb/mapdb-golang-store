package db

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

// Regression (fable release review #1): DB.Close while another goroutine reads a cached
// TreeMap handle. Expected: readers panic KindClosed; Close returns nil.
func TestDBCloseConcurrentReadersRace(t *testing.T) {
	const iters = 1000
	var bad atomic.Int64
	var first atomic.Value
	record := func(r any) {
		if _, ok := mapdb5.AsDBError(r); ok {
			return
		}
		bad.Add(1)
		first.CompareAndSwap(nil, fmt.Sprint(r))
	}
	for it := 0; it < iters; it++ {
		d, err := HeapDB().Make()
		if err != nil {
			t.Fatal(err)
		}
		m, err := TreeMap[int64, int64](d, "m", ser.LongFormat, ser.LongFormat).Create()
		if err != nil {
			t.Fatal(err)
		}
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
			if err := d.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		}()
		wg.Wait()
	}
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d non-DBError panics in %d iterations; first: %v", n, iters, first.Load())
	}
}
