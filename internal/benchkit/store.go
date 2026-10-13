package benchkit

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// StoreSubjects are the stores measured by the store-level read benchmark
// (the M3b gate: locked StoreDirect reads, isolated from the BTree).
var StoreSubjects = []struct {
	ID    string
	Build func() store.Store
}{
	{"StoreOnHeap", func() store.Store { return store.NewStoreOnHeap(true) }},
	{"StoreDirect(heap)", func() store.Store { return store.NewStoreDirectHeap(true) }},
}

var longSer = ser.Erase[int64](ser.Long)

// StoreGetWorkload is the store-level workload name.
const StoreGetWorkload = "storeGet"

// PopulateStore puts n 8-byte records and returns their recids in a
// seeded random probe order.
func PopulateStore(s store.Store, n int) []store.Recid {
	recids := make([]store.Recid, n)
	for i := range recids {
		recids[i] = s.Put(int64(i), longSer)
	}
	r := rand.New(rand.NewPCG(Seed, Seed))
	r.Shuffle(n, func(i, j int) { recids[i], recids[j] = recids[j], recids[i] })
	return recids
}

// RunStoreGet reads random recids on `threads` goroutines for dur.
func RunStoreGet(s store.Store, recids []store.Recid, threads int, dur time.Duration) Sample {
	var stop atomic.Bool
	var total atomic.Int64
	var wg, ready sync.WaitGroup
	gate := make(chan struct{})
	wg.Add(threads)
	ready.Add(threads)
	for t := 0; t < threads; t++ {
		go func() {
			defer wg.Done()
			i := rand.IntN(len(recids))
			var n int64
			var acc uint64
			ready.Done()
			<-gate
			for !stop.Load() {
				for k := 0; k < 64; k++ {
					v := s.Get(recids[i], longSer)
					acc += uint64(v.(int64))
					i++
					if i == len(recids) {
						i = 0
					}
				}
				n += 64
			}
			total.Add(n)
			Sink.Add(acc)
		}()
	}
	ready.Wait()
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	t0 := time.Now()
	close(gate)
	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
	el := time.Since(t0)
	runtime.ReadMemStats(&ms1)
	return Sample{Ops: total.Load(), Elapsed: el, AllocBytes: ms1.TotalAlloc - ms0.TotalAlloc}
}

// StoreGet is one store-level read; callers fold the result locally.
func StoreGet(s store.Store, recid store.Recid) uint64 {
	return uint64(s.Get(recid, longSer).(int64))
}
