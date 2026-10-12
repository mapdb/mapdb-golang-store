package store

import (
	"iter"
	"slices"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// StoreByteArray — the reference StoreDelta (spec 02 §4): one []byte per
// record; buf capacity is capacity, used is content length. Everything
// explicit — this store doubles as the differential-fuzz oracle; its value
// is being obviously correct. Serialization happens outside the record lock.

const baShards = 64

type baState int

const (
	baLive baState = iota
	baNull
	baPrealloc
)

type baRec struct {
	buf   []byte
	used  int
	state baState
}

type baShard struct {
	mu sync.RWMutex
	m  map[Recid]baRec
}

// StoreByteArray is the byte-resident oracle store.
type StoreByteArray struct {
	shards     [baShards]baShard
	alloc      heapAlloc
	threadSafe bool
	closed     atomic.Bool
	lease      *LeaseTable
}

// NewStoreByteArray creates the oracle store.
func NewStoreByteArray(threadSafe bool) *StoreByteArray {
	s := &StoreByteArray{threadSafe: threadSafe, lease: NewLeaseTable()}
	for i := range s.shards {
		s.shards[i].m = make(map[Recid]baRec)
	}
	return s
}

// LeaseTable is this store's lease registry.
func (s *StoreByteArray) LeaseTable() *LeaseTable { return s.lease }

func (s *StoreByteArray) shard(recid Recid) *baShard {
	return &s.shards[recid&(baShards-1)]
}

func (s *StoreByteArray) checkClosed() {
	if s.closed.Load() {
		panic(mapdb5.New(mapdb5.KindClosed, "store is closed"))
	}
}

func (s *StoreByteArray) allocRecid() Recid {
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

// newRec serializes record (or builds a null record), reserving headroom
// trailing bytes. Runs outside any record lock.
func newRec(record any, sr ser.AnySerializer, headroom int) baRec {
	if record == nil {
		return baRec{buf: make([]byte, headroom), state: baNull}
	}
	w := bin.GetWriter()
	defer bin.PutWriter(w)
	sr.SerializeAny(w, record)
	used := w.Pos
	buf := make([]byte, used+headroom)
	copy(buf, w.Buf[:used])
	return baRec{buf: buf, used: used, state: baLive}
}

func (s *StoreByteArray) Preallocate() Recid {
	s.checkClosed()
	recid := s.allocRecid()
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	sh.m[recid] = baRec{state: baPrealloc}
	return recid
}

func (s *StoreByteArray) PreallocateN(into []Recid) {
	for i := range into {
		into[i] = s.Preallocate()
	}
}

func (s *StoreByteArray) Put(record any, sr ser.AnySerializer) Recid {
	s.checkClosed()
	validateRecord(sr, record)
	rec := newRec(record, sr, 0)
	recid := s.allocRecid()
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	sh.m[recid] = rec
	return recid
}

func (s *StoreByteArray) Get(recid Recid, sr ser.AnySerializer) any {
	s.checkClosed()
	sh := s.shard(recid)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	s.checkClosed()
	rec, ok := sh.m[recid]
	if !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	if rec.state != baLive {
		return nil
	}
	v := sr.DeserializeAny(bin.NewReader(rec.buf[:rec.used]), rec.used)
	if v != nil {
		validateOperand(sr, v)
	}
	return v
}

func (s *StoreByteArray) Read(recid Recid, action RecordRead) int64 {
	s.checkClosed()
	sh := s.shard(recid)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	s.checkClosed()
	rec, ok := sh.m[recid]
	if !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	// The action runs under the record lock (spec 02 §RecordRead).
	if rec.state != baLive {
		return action.OnNull()
	}
	return action.OnBytes(bin.NewReader(rec.buf[:rec.used]), rec.used)
}

func (s *StoreByteArray) Update(recid Recid, record any, sr ser.AnySerializer) {
	s.updateWithHeadroomOpt(recid, record, sr, 0)
}

func (s *StoreByteArray) updateWithHeadroomOpt(recid Recid, record any, sr ser.AnySerializer, headroom int) {
	s.checkClosed()
	validateRecord(sr, record)
	if headroom < 0 {
		panic(mapdb5.NewMisuse("negative headroom"))
	}
	rec := newRec(record, sr, headroom)
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	if _, ok := sh.m[recid]; !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	sh.m[recid] = rec
}

func (s *StoreByteArray) CompareAndSwap(recid Recid, expectedOld, newRecord any, sr ser.AnySerializer) bool {
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
	if rec.state == baLive {
		current = sr.DeserializeAny(bin.NewReader(rec.buf[:rec.used]), rec.used)
		if current != nil {
			validateOperand(sr, current)
		}
	}
	var eq bool
	switch {
	case current == nil && expectedOld == nil:
		eq = true
	case current != nil && expectedOld != nil:
		eq = sr.EqualsAny(current, expectedOld)
	}
	if !eq {
		return false
	}
	sh.m[recid] = newRec(newRecord, sr, 0)
	return true
}

func (s *StoreByteArray) Delete(recid Recid, _ ser.AnySerializer) {
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

func (s *StoreByteArray) Commit()  { s.checkClosed() }
func (s *StoreByteArray) Compact() { s.checkClosed() }

func (s *StoreByteArray) Close() error {
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

func (s *StoreByteArray) IsClosed() bool { return s.closed.Load() }

func (s *StoreByteArray) Verify() {
	s.checkClosed()
	s.alloc.mu.Lock()
	maxRecid := s.alloc.maxRecid
	free := make([]Recid, len(s.alloc.free))
	copy(free, s.alloc.free)
	s.alloc.mu.Unlock()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for recid, r := range sh.m {
			if recid < 1 || recid > maxRecid {
				sh.mu.RUnlock()
				panic(mapdb5.New(mapdb5.KindVerifyFailed, "recid out of range"))
			}
			if r.used > len(r.buf) {
				sh.mu.RUnlock()
				panic(mapdb5.New(mapdb5.KindVerifyFailed, "used beyond capacity"))
			}
		}
		sh.mu.RUnlock()
	}
	for _, r := range free {
		sh := s.shard(r)
		sh.mu.RLock()
		_, live := sh.m[r]
		sh.mu.RUnlock()
		if live {
			panic(mapdb5.New(mapdb5.KindVerifyFailed, "free recid is live"))
		}
	}
}

func (s *StoreByteArray) AllRecids() iter.Seq[Recid] {
	s.checkClosed()
	var out []Recid
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for recid, rec := range sh.m {
			if rec.state != baPrealloc {
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

func (s *StoreByteArray) IsThreadSafe() bool           { return s.threadSafe }
func (s *StoreByteArray) IsReadOnly() bool             { return false }
func (s *StoreByteArray) IsTx() bool                   { return false }
func (s *StoreByteArray) StructuralGeneration() uint64 { return 0 }

// CurrentSize is the total live content bytes — decreases on delete/update
// (spec's HTreeCache budget hook).
func (s *StoreByteArray) CurrentSize() int64 {
	if s.closed.Load() {
		return 0
	}
	var n int64
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, rec := range sh.m {
			n += int64(rec.used)
		}
		sh.mu.RUnlock()
	}
	return n
}

// ---- StoreDelta ----

func (s *StoreByteArray) Append(recid Recid, data []byte) int64 {
	s.checkClosed()
	sh := s.shard(recid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.checkClosed()
	rec, ok := sh.m[recid]
	if !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	if rec.used+len(data) > len(rec.buf) {
		neverProvisioned := rec.state != baLive && len(rec.buf) == 0
		if !neverProvisioned {
			return AppendRefused
		}
		// first append establishes the record: capacity == len
		rec.buf = make([]byte, len(data))
	}
	copy(rec.buf[rec.used:rec.used+len(data)], data)
	rec.used += len(data)
	rec.state = baLive
	sh.m[recid] = rec
	return int64(rec.used)
}

func (s *StoreByteArray) CapacityRemaining(recid Recid) int64 {
	s.checkClosed()
	sh := s.shard(recid)
	sh.mu.RLock()
	rec, ok := sh.m[recid]
	sh.mu.RUnlock()
	if !ok {
		panic(mapdb5.NewGetVoid(recid))
	}
	return int64(len(rec.buf) - rec.used)
}

func (s *StoreByteArray) UpdateWithHeadroom(recid Recid, record any, sr ser.AnySerializer, headroom int) {
	s.updateWithHeadroomOpt(recid, record, sr, headroom)
}
