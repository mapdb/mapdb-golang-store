package btree

import (
	"runtime"
	"sync"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// ===================== node lock table =====================
//
// Per-node write locks keyed by EXACT recid (mapdb1/2/3 lineage), as a
// sharded lock table per spec 03: each shard is a sync.Mutex + held set +
// sync.Cond. Distinct recids on the same shard contend only for the map
// operation itself, never while a lock is held.
//
// DEADLOCK-FREEDOM (Sagiv 1986, same as the Rust port): every acquisition
// happens while holding NO node lock — move-right releases before
// re-locking, a split releases the child before locking the parent, root
// grow holds only the root-pointer recid. Hold-and-wait never occurs, so no
// waits-for cycle can form.
//
// NON-REENTRANT by design: a same-goroutine re-acquire panics rather than
// masking an aliasing or acquisition-order bug (the historical failure mode
// this table exists to surface).
const lockShards = 64

type nodeLockShard struct {
	mu   sync.Mutex
	cond sync.Cond
	held map[uint64]uint64 // recid → owner goroutine id
}

type nodeLockTable struct {
	shards  [lockShards]nodeLockShard
	enabled bool
}

func newNodeLockTable(enabled bool) *nodeLockTable {
	t := &nodeLockTable{enabled: enabled}
	for i := range t.shards {
		t.shards[i].cond.L = &t.shards[i].mu
		t.shards[i].held = make(map[uint64]uint64)
	}
	return t
}

// goid identifies the calling goroutine for the reentrancy tripwire. Parsed
// lazily — only on the contention path (a free recid never pays for it).
func goid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// format: "goroutine 123 [running]..."
	if n < 10 {
		return 0
	}
	var id uint64
	for i := 10; i < n && buf[i] >= '0' && buf[i] <= '9'; i++ {
		id = id*10 + uint64(buf[i]-'0')
	}
	return id
}

func (t *nodeLockTable) lock(recid uint64) {
	if !t.enabled {
		return
	}
	sh := &t.shards[recid%lockShards]
	sh.mu.Lock()
	var me uint64
	for {
		owner, held := sh.held[recid]
		if !held {
			if me == 0 {
				me = goid()
			}
			sh.held[recid] = me
			sh.mu.Unlock()
			return
		}
		if me == 0 {
			me = goid()
		}
		if owner == me {
			sh.mu.Unlock()
			panic(mapdb5.NewCorruption("reentrant btree node lock"))
		}
		sh.cond.Wait()
	}
}

func (t *nodeLockTable) unlock(recid uint64) {
	if !t.enabled {
		return
	}
	sh := &t.shards[recid%lockShards]
	sh.mu.Lock()
	delete(sh.held, recid)
	sh.cond.Broadcast()
	sh.mu.Unlock()
}

// nodeGuard is a releaseable node lock: release on ALL paths (Go has no
// RAII, so every caller defers release or hands the guard to a function
// that does). release is idempotent — move-right and split hand-off call
// it explicitly before re-locking elsewhere.
type nodeGuard struct {
	table *nodeLockTable
	recid uint64
	held  bool
}

func (g *nodeGuard) release() {
	if g.held {
		g.table.unlock(g.recid)
		g.held = false
	}
}

// ===================== cycle guard =====================

// cycleGuard is a cheap cycle detector over persisted recid graphs. Below
// the soft threshold it costs one increment; above it (never reached by a
// valid tree) it records visited recids and reports DataCorruption on
// repetition, so a crafted child/link cycle terminates instead of hanging.
type cycleGuard struct {
	steps uint64
	soft  uint64
	seen  map[uint64]struct{}
}

func newCycleGuard(soft uint64) *cycleGuard {
	return &cycleGuard{soft: soft}
}

func (c *cycleGuard) visit(recid uint64) {
	c.steps++
	if c.steps > c.soft {
		if c.seen == nil {
			c.seen = make(map[uint64]struct{})
		}
		if _, dup := c.seen[recid]; dup {
			panic(mapdb5.NewCorruption("cycle in btree recid graph"))
		}
		c.seen[recid] = struct{}{}
	}
}

// Valid tree depth / move-right hops are tiny; anything past this is a cycle.
const cycleDescentSoft = 4096

// Leaf-link scans can legitimately be long; only enormous chains (past this
// many leaves) begin tracking visited recids.
const cycleScanSoft = 1 << 16

// recidOrCorrupt converts an externally-derived recid, mapping a 0 (which a
// valid tree never produces) to DataCorruption.
func recidOrCorrupt(x uint64) uint64 {
	if x == 0 {
		panic(mapdb5.NewCorruption("btree recid is zero"))
	}
	return x
}

// leftEdgeSpin is the park interval while waiting for a concurrent root
// split to publish a new left-edge level.
const leftEdgeSpin = 100 * time.Nanosecond
