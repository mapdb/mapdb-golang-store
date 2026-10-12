package queue

import (
	"sync"
	"sync/atomic"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// unbounded is the capacity stored for non-circular queues (Java
// Long.MAX_VALUE) — kept identical for header byte parity.
const unbounded = uint64(1<<63 - 1)

// Mode is the persistent queue discipline. The ordinal is persisted, so the
// values are wire-fixed (FIFO=0, LIFO=1, CIRCULAR=2).
type Mode int32

const (
	ModeFIFO     Mode = 0
	ModeLIFO     Mode = 1
	ModeCircular Mode = 2
)

func modeFromI32(v int32) (Mode, bool) {
	switch v {
	case 0:
		return ModeFIFO, true
	case 1:
		return ModeLIFO, true
	case 2:
		return ModeCircular, true
	}
	return 0, false
}

// qHeader is the mutable queue header (persisted in one record):
// packInt(mode) ++ packLong(head) ++ packLong(tail) ++ packLong(size) ++
// packLong(capacity) — byte-for-byte with Java.
type qHeader struct {
	mode     int32
	head     uint64
	tail     uint64
	size     uint64
	capacity uint64
}

type qHeaderSer struct{}

var qHeaderSerializer = ser.NewAnyOf(qHeaderSer{})

func (qHeaderSer) Serialize(w *bin.Writer, h qHeader) {
	w.PackInt(h.mode)
	w.PackUint64(h.head)
	w.PackUint64(h.tail)
	w.PackUint64(h.size)
	w.PackUint64(h.capacity)
}

func (qHeaderSer) Deserialize(r *bin.Reader, _ int) qHeader {
	return qHeader{
		mode:     r.UnpackInt(),
		head:     uint64(r.UnpackLong()),
		tail:     uint64(r.UnpackLong()),
		size:     uint64(r.UnpackLong()),
		capacity: uint64(r.UnpackLong()),
	}
}

func (qHeaderSer) FixedSize() int { return -1 }
func (qHeaderSer) SizeHint() int  { return 45 }
func (qHeaderSer) Compare(a, b qHeader) int {
	if a.mode != b.mode {
		if a.mode < b.mode {
			return -1
		}
		return 1
	}
	if c := cmpUint64(a.head, b.head); c != 0 {
		return c
	}
	if c := cmpUint64(a.tail, b.tail); c != 0 {
		return c
	}
	if c := cmpUint64(a.size, b.size); c != 0 {
		return c
	}
	return cmpUint64(a.capacity, b.capacity)
}
func (qHeaderSer) Equals(a, b qHeader) bool      { return a == b }
func (qHeaderSer) NaturalOrder() bool            { return false }
func (qHeaderSer) EqualsBySerializedBytes() bool { return true }

// qNode is a singly-linked node: next recid (0 = end) plus the element.
type qNode[V any] struct {
	next  uint64
	value V
}

// qNodeSer is the wire codec packLong(next) ++ element(value), borrowing the
// element serializer.
type qNodeSer[V any] struct{ elem ser.Serializer[V] }

func (s qNodeSer[V]) Serialize(w *bin.Writer, n qNode[V]) {
	w.PackUint64(n.next)
	s.elem.Serialize(w, n.value)
}

func (s qNodeSer[V]) Deserialize(r *bin.Reader, _ int) qNode[V] {
	return qNode[V]{next: uint64(r.UnpackLong()), value: s.elem.Deserialize(r, -1)}
}

func (s qNodeSer[V]) FixedSize() int { return -1 } // packed next recid is variable-width

func (s qNodeSer[V]) SizeHint() int { return 9 + s.elem.SizeHint() }

func (s qNodeSer[V]) Compare(a, b qNode[V]) int {
	if c := cmpUint64(a.next, b.next); c != 0 {
		return c
	}
	return s.elem.Compare(a.value, b.value)
}

func (s qNodeSer[V]) Equals(a, b qNode[V]) bool {
	return a.next == b.next && s.elem.Equals(a.value, b.value)
}

func (s qNodeSer[V]) NaturalOrder() bool            { return false }
func (s qNodeSer[V]) EqualsBySerializedBytes() bool { return s.elem.EqualsBySerializedBytes() }

// PBQ is a store-backed blocking FIFO / LIFO stack / overwrite-on-full
// circular queue (Java PersistentBlockingQueue).
//
// Mode, head/tail/size pointers and circular capacity all live in the
// HEADER record. Blocking Take/Put coordinate only goroutines sharing this
// one live handle — contents are durable, the condition signals are not
// cross-process. Use one writable handle per header.
//
// Ops panic *mapdb5.DBError; Create/Open return errors.
type PBQ[V any] struct {
	st          store.Store
	headerRecid store.Recid
	elemSer     ser.Serializer[V]
	mu          sync.Mutex
	notEmpty    *chCond
	notFull     *chCond
	closed      atomic.Bool
}

// CreateBlockingQueue creates a fresh queue of the given mode. capacity is
// used only for ModeCircular; FIFO/LIFO are unbounded (Long.MAX_VALUE). A
// circular capacity must be in 1..=MaxInt64.
func CreateBlockingQueue[V any](s store.Store, elemSer ser.Serializer[V], mode Mode, capacity uint64) (q *PBQ[V], err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				err = de
				return
			}
			panic(r)
		}
	}()
	actual := unbounded
	if mode == ModeCircular {
		actual = capacity
	}
	// Capacity is packed as an unsigned long here but read back by Java as a
	// signed long that must be > 0; reject anything Java could not reopen.
	if actual == 0 || actual > unbounded {
		return nil, mapdb5.NewCorruption("blocking queue: capacity must be in 1..=MaxInt64")
	}
	headerRecid := store.Put(s, qHeader{mode: int32(mode), capacity: actual}, qHeaderSerializer)
	return openPBQ(s, headerRecid, elemSer)
}

// OpenBlockingQueue reopens an existing queue from its header recid.
func OpenBlockingQueue[V any](s store.Store, headerRecid store.Recid, elemSer ser.Serializer[V]) (q *PBQ[V], err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				err = de
				return
			}
			panic(r)
		}
	}()
	return openPBQ(s, headerRecid, elemSer)
}

func openPBQ[V any](s store.Store, headerRecid store.Recid, elemSer ser.Serializer[V]) (*PBQ[V], error) {
	q := &PBQ[V]{
		st:          s,
		headerRecid: headerRecid,
		elemSer:     elemSer,
		notEmpty:    newChCond(),
		notFull:     newChCond(),
	}
	h := q.header()
	if _, ok := modeFromI32(h.mode); !ok {
		return nil, mapdb5.NewCorruption("invalid persistent queue header: mode")
	}
	// Capacity must be a value Java could reopen (a signed long in
	// 1..=MaxInt64), and the stored size must not exceed it. A hostile
	// header with size > capacity would underflow RemainingCapacity and
	// the full/size-1 paths.
	if h.capacity == 0 || h.capacity > unbounded {
		return nil, mapdb5.NewCorruption("invalid persistent queue header: capacity out of 1..=MaxInt64")
	}
	if h.size > h.capacity {
		return nil, mapdb5.NewCorruption("invalid persistent queue header: size exceeds capacity")
	}
	return q, nil
}

func (q *PBQ[V]) nodeSer() qNodeSer[V] { return qNodeSer[V]{elem: q.elemSer} }

// header reads the header (caller must hold the handle lock). Panics
// KindClosed once the handle is closed, matching Java header().
func (q *PBQ[V]) header() qHeader {
	if q.closed.Load() {
		panic(mapdb5.New(mapdb5.KindClosed, "blocking queue handle is closed"))
	}
	h, ok := store.Get(q.st, q.headerRecid, qHeaderSerializer)
	if !ok {
		panic(mapdb5.NewCorruption("blocking queue header missing"))
	}
	return h
}

func (q *PBQ[V]) node(recid store.Recid) qNode[V] {
	n, ok := store.Get(q.st, recid, q.nodeSer())
	if !ok {
		panic(mapdb5.NewCorruption("blocking queue node missing"))
	}
	return n
}

func (q *PBQ[V]) full(h qHeader) bool { return h.size >= h.capacity }

// enqueue (caller holds the lock). For a full circular queue the head is
// overwritten (oldest discarded) first.
func (q *PBQ[V]) enqueue(h qHeader, value V) {
	mode, _ := modeFromI32(h.mode)
	if mode == ModeCircular && q.full(h) {
		h = q.dequeue(h, nil)
	}
	if mode == ModeLIFO {
		recid := store.Put(q.st, qNode[V]{next: h.head, value: value}, q.nodeSer())
		tail := h.tail
		if h.size == 0 {
			tail = uint64(recid)
		}
		store.Update(q.st, q.headerRecid, qHeader{
			mode: h.mode, head: uint64(recid), tail: tail,
			size: h.size + 1, capacity: h.capacity}, qHeaderSerializer)
		return
	}
	recid := store.Put(q.st, qNode[V]{next: 0, value: value}, q.nodeSer())
	if h.tail != 0 {
		tailRecid := qnz(h.tail)
		tailNode := q.node(tailRecid)
		store.Update(q.st, tailRecid, qNode[V]{next: uint64(recid), value: tailNode.value}, q.nodeSer())
	}
	head := h.head
	if h.size == 0 {
		head = uint64(recid)
	}
	store.Update(q.st, q.headerRecid, qHeader{
		mode: h.mode, head: head, tail: uint64(recid),
		size: h.size + 1, capacity: h.capacity}, qHeaderSerializer)
}

// dequeue removes the head node; out receives its value when non-nil.
// Returns the updated header.
func (q *PBQ[V]) dequeue(h qHeader, out *V) qHeader {
	if h.size == 0 {
		return h
	}
	headRecid := qnz(h.head)
	n := q.node(headRecid)
	tail := h.tail
	if h.size == 1 {
		tail = 0
	}
	next := qHeader{mode: h.mode, head: n.next, tail: tail, size: h.size - 1, capacity: h.capacity}
	store.Update(q.st, q.headerRecid, next, qHeaderSerializer)
	store.Delete(q.st, headRecid, q.nodeSer())
	if out != nil {
		*out = n.value
	}
	return next
}

func (q *PBQ[V]) removeHeadLocked() V {
	h := q.header()
	var v V
	q.dequeue(h, &v)
	q.notFull.signal()
	return v
}

// ===================== non-blocking API =====================

// Offer inserts value; false if a bounded (FIFO/LIFO) queue is full.
func (q *PBQ[V]) Offer(value V) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	h := q.header()
	m, _ := modeFromI32(h.mode)
	if q.full(h) && m != ModeCircular {
		return false
	}
	q.enqueue(h, value)
	q.notEmpty.signal()
	return true
}

// Add inserts value, panicking KindUnsupported if a bounded queue is full
// (Java AbstractQueue.add / IllegalStateException).
func (q *PBQ[V]) Add(value V) {
	if !q.Offer(value) {
		panic(mapdb5.New(mapdb5.KindUnsupported, "blocking queue is full"))
	}
}

// AddAll inserts every element.
func (q *PBQ[V]) AddAll(values ...V) {
	for _, v := range values {
		q.Add(v)
	}
}

// Poll removes and returns the head, or false if empty.
func (q *PBQ[V]) Poll() (V, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	h := q.header()
	if h.size == 0 {
		var zero V
		return zero, false
	}
	var v V
	q.dequeue(h, &v)
	q.notFull.signal()
	return v, true
}

// Peek returns the head without removing it, or false if empty.
func (q *PBQ[V]) Peek() (V, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	h := q.header()
	if h.size == 0 {
		var zero V
		return zero, false
	}
	return q.node(qnz(h.head)).value, true
}

// ===================== blocking API =====================

// Take blocks until an element is available, then removes and returns it.
// Panics KindClosed if the handle is closed while waiting. The deferred
// unlock also covers panics (Java's finally) — a panic while holding mu
// must not strand every other waiter on the handle.
func (q *PBQ[V]) Take() V {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		h := q.header()
		if h.size != 0 {
			return q.removeHeadLocked()
		}
		q.notEmpty.wait(&q.mu)
	}
}

// PollTimeout is Take with a deadline; false if it elapses.
func (q *PBQ[V]) PollTimeout(d time.Duration) (V, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	deadline := time.Now().Add(d)
	for {
		h := q.header()
		if h.size != 0 {
			v := q.removeHeadLocked()
			return v, true
		}
		if rem := time.Until(deadline); rem > 0 {
			q.notEmpty.waitTimeout(&q.mu, rem)
		} else {
			var zero V
			return zero, false
		}
	}
}

// Put blocks until there is room, then inserts value. Circular queues never
// block. Panics KindClosed if closed while waiting.
func (q *PBQ[V]) Put(value V) {
	q.mu.Lock()
	defer q.mu.Unlock()
	h := q.header()
	for m, _ := modeFromI32(h.mode); q.full(h) && m != ModeCircular; m, _ = modeFromI32(h.mode) {
		q.notFull.wait(&q.mu)
		h = q.header()
	}
	q.enqueue(h, value)
	q.notEmpty.signal()
}

// OfferTimeout is Put with a deadline; false if it elapses.
func (q *PBQ[V]) OfferTimeout(value V, d time.Duration) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	deadline := time.Now().Add(d)
	h := q.header()
	for m, _ := modeFromI32(h.mode); q.full(h) && m != ModeCircular; m, _ = modeFromI32(h.mode) {
		if rem := time.Until(deadline); rem > 0 {
			q.notFull.waitTimeout(&q.mu, rem)
			h = q.header()
		} else {
			return false
		}
	}
	q.enqueue(h, value)
	q.notEmpty.signal()
	return true
}

// ===================== collection helpers =====================

// Len returns the current element count.
func (q *PBQ[V]) Len() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.header().size
}

// IsEmpty reports whether the queue is empty.
func (q *PBQ[V]) IsEmpty() bool { return q.Len() == 0 }

// RemainingCapacity returns capacity - size.
func (q *PBQ[V]) RemainingCapacity() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	h := q.header()
	if h.size > h.capacity {
		panic(mapdb5.NewCorruption("blocking queue: size exceeds capacity"))
	}
	return h.capacity - h.size
}

// Contains reports whether value is present (by serializer equality).
func (q *PBQ[V]) Contains(value V) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for recid := q.header().head; recid != 0; {
		n := q.node(qnz(recid))
		if q.elemSer.Equals(n.value, value) {
			return true
		}
		recid = n.next
	}
	return false
}

// RemoveValue removes the first node equal to value; true if removed.
func (q *PBQ[V]) RemoveValue(value V) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	h := q.header()
	var prevRecid uint64
	for recid := h.head; recid != 0; {
		cur := qnz(recid)
		n := q.node(cur)
		if q.elemSer.Equals(n.value, value) {
			if prevRecid == 0 {
				tail := h.tail
				if h.size == 1 {
					tail = 0
				}
				store.Update(q.st, q.headerRecid, qHeader{
					mode: h.mode, head: n.next, tail: tail,
					size: h.size - 1, capacity: h.capacity}, qHeaderSerializer)
			} else {
				previous := q.node(qnz(prevRecid))
				store.Update(q.st, qnz(prevRecid), qNode[V]{next: n.next, value: previous.value}, q.nodeSer())
				tail := h.tail
				if h.tail == recid {
					tail = prevRecid
				}
				store.Update(q.st, q.headerRecid, qHeader{
					mode: h.mode, head: h.head, tail: tail,
					size: h.size - 1, capacity: h.capacity}, qHeaderSerializer)
			}
			store.Delete(q.st, cur, q.nodeSer())
			q.notFull.signal()
			return true
		}
		prevRecid = recid
		recid = n.next
	}
	return false
}

// DrainTo moves up to maxElements head elements into a fresh slice.
func (q *PBQ[V]) DrainTo(maxElements int) []V {
	if maxElements <= 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []V
	for len(out) < maxElements && q.header().size != 0 {
		out = append(out, q.removeHeadLocked())
	}
	return out
}

// Clear removes all elements.
func (q *PBQ[V]) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	h := q.header()
	for h.size != 0 {
		h = q.dequeue(h, nil)
	}
	q.notFull.broadcast()
}

// PurgeRecords frees every node record AND the header record (teardown).
// Reads the header directly so it works even after the handle is closed.
func (q *PBQ[V]) PurgeRecords() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if h, ok := store.Get(q.st, q.headerRecid, qHeaderSerializer); ok {
		for recid := h.head; recid != 0; {
			cur := qnz(recid)
			next := q.node(cur).next
			store.Delete(q.st, cur, q.nodeSer())
			recid = next
		}
	}
	store.Delete(q.st, q.headerRecid, qHeaderSerializer)
}

// Slice snapshots the elements head-first.
func (q *PBQ[V]) Slice() []V {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []V
	for recid := q.header().head; recid != 0; {
		n := q.node(qnz(recid))
		out = append(out, n.value)
		recid = n.next
	}
	return out
}

// CloseHandle wakes blocked operations and marks the handle closed (without
// touching the shared store). Subsequent operations panic KindClosed.
func (q *PBQ[V]) CloseHandle() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed.Store(true)
	q.notEmpty.broadcast()
	q.notFull.broadcast()
}

func (q *PBQ[V]) HeaderRecid() store.Recid      { return q.headerRecid }
func (q *PBQ[V]) Serializer() ser.Serializer[V] { return q.elemSer }

// Mode returns the queue's mode (reads the header).
func (q *PBQ[V]) Mode() Mode {
	q.mu.Lock()
	defer q.mu.Unlock()
	m, _ := modeFromI32(q.header().mode)
	return m
}

// Verify checks queue structure; panics KindVerifyFailed on inconsistency.
func (q *PBQ[V]) Verify() {
	q.mu.Lock()
	defer q.mu.Unlock()
	fail := func(msg string) { panic(mapdb5.New(mapdb5.KindVerifyFailed, msg)) }
	h := q.header()
	var count uint64
	var last uint64
	for recid := h.head; recid != 0; {
		count++
		if count > h.size {
			fail("queue cycle/size mismatch")
		}
		last = recid
		recid = q.node(qnz(recid)).next
	}
	tailOK := h.tail == 0
	if count != 0 {
		tailOK = h.tail == last
	}
	if count != h.size || !tailOK {
		fail("queue header/link mismatch")
	}
}
