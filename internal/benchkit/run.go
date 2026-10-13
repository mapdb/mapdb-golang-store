package benchkit

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Workload names, identical to the Java JMH method names.
const (
	GetHit        = "getHit"
	GetMiss       = "getMiss"
	Overwrite     = "overwrite"
	Mixed90r10w   = "mixed90r10w"
	Mixed50r50w   = "mixed50r50w"
	Iterate       = "iterate"
	PutSequential = "putSequential"
	PutRandom     = "putRandom"
)

// PointWorkloads run per-op over a pre-populated map and may run on many
// goroutines (Java ReadWriteBench).
var PointWorkloads = []string{GetHit, GetMiss, Overwrite, Mixed90r10w, Mixed50r50w}

// AllWorkloads in table order.
var AllWorkloads = []string{GetHit, GetMiss, Overwrite, Mixed90r10w, Mixed50r50w, Iterate, PutSequential, PutRandom}

// IsPoint reports whether w is a per-op workload.
func IsPoint(w string) bool {
	for _, p := range PointWorkloads {
		if p == w {
			return true
		}
	}
	return false
}

// Sink defeats dead-code elimination of benchmark results.
var Sink atomic.Uint64

// cursor is Java's per-thread Cursor: an advancing index into the shared
// probe order, starting at a random per-goroutine offset so goroutines do
// not walk the permutation in lockstep.
type cursor struct{ i, n int }

func newCursor(n int) cursor { return cursor{i: rand.IntN(n), n: n} }

func (c *cursor) next() int {
	i := c.i
	c.i++
	if c.i == c.n {
		c.i = 0
	}
	return i
}

// Op returns the single-op body for a point workload; it reports whether
// the op found / replaced something, folded into Sink by the caller.
func Op[K, V any](w string, m Map[K, V], d *Dataset[K, V]) func(c *cursor) bool {
	switch w {
	case GetHit:
		return func(c *cursor) bool {
			_, ok := m.Get(d.Keys[d.Probe[c.next()]])
			return ok
		}
	case GetMiss:
		return func(c *cursor) bool {
			p := c.next()
			idx := d.Probe[p]
			if p&1 == 0 {
				_, ok := m.Get(d.Keys[idx])
				return ok
			}
			_, ok := m.Get(d.MissKeys[idx])
			return ok
		}
	case Overwrite:
		return func(c *cursor) bool {
			idx := d.Probe[c.next()]
			_, ok := m.Put(d.Keys[idx], d.Values[idx])
			return ok
		}
	case Mixed90r10w, Mixed50r50w:
		pct := 10
		if w == Mixed50r50w {
			pct = 50
		}
		return func(c *cursor) bool {
			p := c.next()
			idx := d.Probe[p]
			if p%100 < pct {
				_, ok := m.Put(d.Keys[idx], d.Values[idx])
				return ok
			}
			_, ok := m.Get(d.Keys[idx])
			return ok
		}
	}
	panic("not a point workload: " + w)
}

// Populate inserts the whole dataset in ascending order (Java: trial setup).
func Populate[K, V any](m Map[K, V], d *Dataset[K, V]) {
	for i := 0; i < d.N; i++ {
		m.Put(d.Keys[i], d.Values[i])
	}
}

// Sample is one measured iteration.
type Sample struct {
	Ops     int64
	Elapsed time.Duration
	// AllocBytes is the heap allocated during the iteration (all
	// goroutines); divide by Ops for bytes/op.
	AllocBytes uint64
}

// PerSec is entries (ops) per second.
func (s Sample) PerSec() float64 { return float64(s.Ops) / s.Elapsed.Seconds() }

// RunPoint runs a point workload on `threads` goroutines for `dur` and
// returns what it measured. The map must already be populated.
func RunPoint[K, V any](w string, m Map[K, V], d *Dataset[K, V], threads int, dur time.Duration) Sample {
	op := Op(w, m, d)
	var stop atomic.Bool
	var total atomic.Int64
	var wg sync.WaitGroup
	var startWg sync.WaitGroup
	gate := make(chan struct{})
	startWg.Add(threads)
	wg.Add(threads)
	for t := 0; t < threads; t++ {
		go func() {
			defer wg.Done()
			c := newCursor(d.N)
			var n int64
			var acc uint64
			startWg.Done()
			<-gate
			for !stop.Load() {
				for k := 0; k < 64; k++ {
					if op(&c) {
						acc++
					}
				}
				n += 64
			}
			total.Add(n)
			Sink.Add(acc)
		}()
	}
	startWg.Wait()
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

// RunIterate walks the whole map repeatedly for at least dur; Ops counts
// entries visited (Java: score converted to entries/s).
func RunIterate[K, V any](m Map[K, V], d *Dataset[K, V], dur time.Duration) (Sample, error) {
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	t0 := time.Now()
	var ops int64
	for time.Since(t0) < dur || ops == 0 {
		sum, ok := m.IterateChecksum()
		if !ok {
			return Sample{}, fmt.Errorf("unordered subject has no iterate")
		}
		Sink.Add(sum)
		ops += int64(d.N)
	}
	el := time.Since(t0)
	runtime.ReadMemStats(&ms1)
	return Sample{Ops: ops, Elapsed: el, AllocBytes: ms1.TotalAlloc - ms0.TotalAlloc}, nil
}

// RunLoad inserts the whole dataset into a FRESH map built by build
// (sequential = ascending keys, otherwise probe order); building and
// closing the map are outside the measured region (Java LoadBench).
func RunLoad[K, V any](build func() Map[K, V], d *Dataset[K, V], sequential bool) Sample {
	m := build()
	defer m.Close()
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	t0 := time.Now()
	var acc uint64
	for i := 0; i < d.N; i++ {
		idx := i
		if !sequential {
			idx = d.Probe[i]
		}
		if _, ok := m.Put(d.Keys[idx], d.Values[idx]); ok {
			acc++
		}
	}
	el := time.Since(t0)
	runtime.ReadMemStats(&ms1)
	Sink.Add(acc)
	return Sample{Ops: int64(d.N), Elapsed: el, AllocBytes: ms1.TotalAlloc - ms0.TotalAlloc}
}

// NewOp is Op with its own cursor: a func performing one operation, for
// testing.B loops (one per goroutine).
func NewOp[K, V any](w string, m Map[K, V], d *Dataset[K, V]) func() {
	op := Op(w, m, d)
	c := newCursor(d.N)
	return func() {
		if op(&c) {
			Sink.Add(1)
		}
	}
}
