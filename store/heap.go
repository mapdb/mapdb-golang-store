package store

import (
	"iter"
	"slices"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

// StoreOnHeap — records are live `any` values, never serialized (spec 02 §4).
// Read dispatches OnObject/OnNull only. Sharded map (the shard lock is the
// record lock) + a global allocation mutex.
//
// OWNERSHIP CONTRACT: records are immutable after handoff — Get returns the
// SAME reference the caller stored. Mutating a stored slice/map/pointer races
// CAS/equality and other readers. Checked nowhere at runtime (as Java's
// collection layer assumes); the no-race guarantee is scoped to callers
// honoring it.

const heapShards = 64

type heapRecState int

const (
	heapNull heapRecState = iota
	heapPrealloc
	heapLive
)

type heapRec struct {
	state heapRecState
	obj   any
}

type heapShard struct {
	mu sync.RWMutex
	m  map[Recid]heapRec
}

type heapAlloc struct {
	mu       sync.Mutex
	maxRecid Recid
	free     []Recid
}

// StoreOnHeap is the object-resident store.
type StoreOnHeap struct {
	shards     [heapShards]heapShard
	alloc      heapAlloc
	threadSafe bool
	closed     atomic.Bool
	lease      *LeaseTable
}

// NewStoreOnHeap creates a heap store. threadSafe=false only reports through
// IsThreadSafe (locking is unconditional — Go collections never opt out).
func NewStoreOnHeap(threadSafe bool) *StoreOnHeap {
	s := &StoreOnHeap{threadSafe: threadSafe, lease: NewLeaseTable()}
	for i := range s.shards {
		s.shards[i].m = make(map[Recid]heapRec)
	}
	return s
}

// LeaseTable is this store's lease registry (collections acquire at open).
func (s *StoreOnHeap) LeaseTable() *LeaseTable { return s.lease }

func (s *StoreOnHeap) shard(recid Recid) *heapShard {
	return &s.shards[recid&(heapShards-1)]
}

func (s *StoreOnHeap) checkClosed() {
	if s.closed.Load() {
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
}

func (s *StoreOnHeap) allocRecid() Recid {
	s.alloc.mu.Lock()
	defer s.alloc.mu.Unlock()
	if n := len(s.alloc.free); n > 0 {
		r := s.alloc.free[n-1]
		s.alloc.free = s.alloc.free[:n-1]
		return r
	}
	s.alloc.maxRecid++
	return s.alloc.maxRecid
}

func (s *StoreOnHeap) Preallocate() Recid {
	s.checkClosed()
	recid := s.allocRecid()
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed() // admission re-check under the record lock (close may have run)
	sh.m[recid] = heapRec{state: heapPrealloc}
	return recid
}

func (s *StoreOnHeap) PreallocateN(into []Recid) {
	for i := range into {
		into[i] = s.Preallocate()
	}
}

func (s *StoreOnHeap) Put(record any, sr ser.AnySerializer) Recid {
	s.checkClosed()
	validateRecord(sr, record)
	recid := s.allocRecid()
	rec := heapRec{state: heapNull, obj: nil}
	if record != nil {
		rec = heapRec{state: heapLive, obj: record}
	}
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	sh.m[recid] = rec
	return recid
}

func (s *StoreOnHeap) Get(recid Recid, sr ser.AnySerializer) any {
	s.checkClosed()
	sh := s.shard(recid)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	s.checkClosed()
	rec, ok := sh.m[recid]
	if !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	if rec.state != heapLive {
		return nil
	}
	validateOperand(sr, rec.obj)
	return rec.obj
}

func (s *StoreOnHeap) Read(recid Recid, action RecordRead) int64 {
	s.checkClosed()
	sh := s.shard(recid)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	s.checkClosed()
	rec, ok := sh.m[recid]
	if !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	// The action runs under the record lock: spec 02 requires the lock to
	// cover the complete dispatch — public actions may be arbitrary and must
	// observe a stable record. defer above also survives action panics.
	if rec.state != heapLive {
		return action.OnNull()
	}
	// The stored object was type-validated at write time; Read has no
	// serializer, so the action receives it unchecked.
	return action.OnObject(rec.obj)
}

func (s *StoreOnHeap) Update(recid Recid, record any, sr ser.AnySerializer) {
	s.checkClosed()
	validateRecord(sr, record)
	rec := heapRec{state: heapNull}
	if record != nil {
		rec = heapRec{state: heapLive, obj: record}
	}
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	if _, ok := sh.m[recid]; !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	sh.m[recid] = rec
}

func (s *StoreOnHeap) CompareAndSwap(recid Recid, expectedOld, newRecord any, sr ser.AnySerializer) bool {
	s.checkClosed()
	validateRecord(sr, expectedOld)
	validateRecord(sr, newRecord)
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	rec, ok := sh.m[recid]
	if !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	var current any
	if rec.state == heapLive {
		current = rec.obj
	}
	var eq bool
	switch {
	case current == nil && expectedOld == nil:
		eq = true
	case current != nil && expectedOld != nil:
		validateOperand(sr, current)
		eq = sr.EqualsAny(current, expectedOld)
	}
	if !eq {
		return false
	}
	nr := heapRec{state: heapNull}
	if newRecord != nil {
		nr = heapRec{state: heapLive, obj: newRecord}
	}
	sh.m[recid] = nr
	return true
}

func (s *StoreOnHeap) Delete(recid Recid, _ ser.AnySerializer) {
	s.checkClosed()
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	if _, ok := sh.m[recid]; !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	delete(sh.m, recid)
	s.alloc.mu.Lock()
	s.alloc.free = append(s.alloc.free, recid)
	s.alloc.mu.Unlock()
}

func (s *StoreOnHeap) Commit() { s.checkClosed() }

func (s *StoreOnHeap) Compact() { s.checkClosed() }

func (s *StoreOnHeap) Close() error {
	s.closed.Store(true)
	for i := range s.shards {
		s.shards[i].mu.Lock()
		clear(s.shards[i].m)
		s.shards[i].mu.Unlock()
	}
	s.alloc.mu.Lock()
	s.alloc.free = nil
	s.alloc.mu.Unlock()
	return nil
}

func (s *StoreOnHeap) IsClosed() bool { return s.closed.Load() }

func (s *StoreOnHeap) Verify() {
	s.checkClosed()
	s.alloc.mu.Lock()
	maxRecid := s.alloc.maxRecid
	free := make([]Recid, len(s.alloc.free))
	copy(free, s.alloc.free)
	s.alloc.mu.Unlock()
	for _, r := range free {
		if r > maxRecid {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "free recid beyond maxRecid"))
		}
		sh := s.shard(r)
		sh.mu.RLock()
		_, live := sh.m[r]
		sh.mu.RUnlock()
		if live {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "free recid is live"))
		}
	}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for recid := range sh.m {
			if recid < 1 || recid > maxRecid {
				sh.mu.RUnlock()
				panic(mapdb5.New(mapdb5.KindVerifyFailed, "recid out of range"))
			}
		}
		sh.mu.RUnlock()
	}
}

func (s *StoreOnHeap) AllRecids() iter.Seq[Recid] {
	s.checkClosed()
	var out []Recid
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for recid, rec := range sh.m {
			if rec.state != heapPrealloc {
				out = append(out, recid)
			}
		}
		sh.mu.RUnlock()
	}
	slices.Sort(out)
	return func(yield func(Recid) bool) {
		for _, r := range out {
			if !yield(r) {
				return
			}
		}
	}
}

func (s *StoreOnHeap) IsThreadSafe() bool           { return s.threadSafe }
func (s *StoreOnHeap) IsReadOnly() bool             { return false }
func (s *StoreOnHeap) IsTx() bool                   { return false }
func (s *StoreOnHeap) StructuralGeneration() uint64 { return 0 }
func (s *StoreOnHeap) CurrentSize() int64           { return 0 }
