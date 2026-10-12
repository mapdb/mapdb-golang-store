// Package queue ports the persistent queue family (spec 04): QueueLong —
// a FIFO of (timestamp, value) u64 pairs over store.Store, byte-for-byte
// with Java org.mapdb.QueueLong — and PersistentBlockingQueue.
package queue

import (
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// LNode is a QueueLong node: two link recids plus the (Timestamp, Value)
// payload. Prev is 0 for the oldest (tail) node; Next always points to the
// following node or the head sentinel.
type LNode struct {
	Prev      uint64
	Next      uint64
	Timestamp uint64
	Value     uint64
}

// lNodeSer is the byte-for-byte Node codec: four packed longs in order
// prev, next, timestamp, value. Java writes prev/next via LONG_PACKED and
// timestamp/value via packLong; for non-negative fields both encoders emit
// identical bytes, so all four are plain packed longs. Decode uses the
// strict signed-domain UnpackLong, matching Java's Node constructor which
// rejects negative fields.
type lNodeSer struct{}

// LNodeSer is the shared instance handed to the store's erased API.
var LNodeSer = ser.NewAnyOf(lNodeSer{})

func (lNodeSer) Serialize(w *bin.Writer, n LNode) {
	w.PackUint64(n.Prev)
	w.PackUint64(n.Next)
	w.PackUint64(n.Timestamp)
	w.PackUint64(n.Value)
}

func (lNodeSer) Deserialize(r *bin.Reader, _ int) LNode {
	return LNode{
		Prev:      uint64(r.UnpackLong()),
		Next:      uint64(r.UnpackLong()),
		Timestamp: uint64(r.UnpackLong()),
		Value:     uint64(r.UnpackLong()),
	}
}

func (lNodeSer) FixedSize() int { return -1 }
func (lNodeSer) SizeHint() int  { return 40 }
func (lNodeSer) Compare(a, b LNode) int {
	if a.Prev != b.Prev {
		return cmpUint64(a.Prev, b.Prev)
	}
	if a.Next != b.Next {
		return cmpUint64(a.Next, b.Next)
	}
	if a.Timestamp != b.Timestamp {
		return cmpUint64(a.Timestamp, b.Timestamp)
	}
	return cmpUint64(a.Value, b.Value)
}
func (lNodeSer) Equals(a, b LNode) bool        { return a == b }
func (lNodeSer) NaturalOrder() bool            { return false }
func (lNodeSer) EqualsBySerializedBytes() bool { return true }

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// maxField is the largest field value that round-trips through Java, whose
// packed longs decode as signed longs. A uint64 in (MaxInt64, MaxUint64]
// would serialize a record Java's QueueLong.Node rejects (negative field).
const maxField = uint64(1<<63 - 1)

func checkField(v uint64) {
	if v > maxField {
		panic(mapdb5.NewCorruption("QueueLong: timestamp/value exceeds int64 max (would break Java interop)"))
	}
}

func qnz(v uint64) store.Recid {
	if v == 0 {
		panic(mapdb5.NewCorruption("QueueLong: zero recid where a node was expected"))
	}
	return store.Recid(v)
}

// QueueLong is a persistent FIFO of (timestamp, value) u64 pairs with O(1)
// removal and bump by node recid. A queue is reopenable from just its three
// pointer recids — there is no separate header object.
//
// Store ops panic *mapdb5.DBError; Make/Open return errors. A per-handle
// mutex serializes operations so each multi-record mutation is atomic
// against other operations on the same handle (Java `synchronized`). Do not
// open two writable handles over the same pointer recids concurrently.
type QueueLong struct {
	st            store.Store
	tailRecid     store.Recid
	headRecid     store.Recid
	headPrevRecid store.Recid
	// ptrSer encodes the three pointer records: ser.LongPacked for Java
	// QueueLong, fixed-width ser.Long for Java htree ExpireQueue.
	ptrSer ser.Serializer[int64]

	mu sync.Mutex
	// cbOwner is the goroutine id currently running a TakeUntil/ForEach
	// callback under mu (0 = none). Java's synchronized monitor is
	// reentrant — a callback re-entering the same handle works there;
	// Go's Mutex is not, so enter() fails such same-goroutine re-entry
	// loudly instead of deadlocking. Atomic because enter() reads it
	// before acquiring mu (a mutex-guarded read/write pair here would be
	// an unsynchronized race).
	cbOwner atomic.Uint64
}

// OpenQueueLong reopens a queue from its three pointer recids. Fails if
// tailRecid == headRecid.
func OpenQueueLong(s store.Store, tailRecid, headRecid, headPrevRecid store.Recid) (*QueueLong, error) {
	if tailRecid == headRecid {
		return nil, mapdb5.NewCorruption("QueueLong: tailRecid == headRecid")
	}
	return &QueueLong{st: s, tailRecid: tailRecid, headRecid: headRecid, headPrevRecid: headPrevRecid,
		ptrSer: ser.LongPacked}, nil
}

// OpenExpireQueue reopens an HTreeCache expire queue: the QueueLong
// algorithm and node codec, but Java ExpireQueue's fixed-width 8-byte
// pointer records (Serializers.LONG).
func OpenExpireQueue(s store.Store, tailRecid, headRecid, headPrevRecid store.Recid) (*QueueLong, error) {
	q, err := OpenQueueLong(s, tailRecid, headRecid, headPrevRecid)
	if err != nil {
		return nil, err
	}
	q.ptrSer = ser.Long
	return q, nil
}

// NewQueueLong allocates a fresh empty queue (sentinel + three pointer
// records) in s. Mirrors Java QueueLong.make(store).
func NewQueueLong(s store.Store) (q *QueueLong, err error) {
	return newQueue(s, ser.LongPacked)
}

// NewExpireQueue allocates a fresh empty HTreeCache expire queue with
// fixed-width pointer records. Mirrors Java ExpireQueue.create(store).
func NewExpireQueue(s store.Store) (q *QueueLong, err error) {
	return newQueue(s, ser.Long)
}

func newQueue(s store.Store, ptrSer ser.Serializer[int64]) (q *QueueLong, err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				err = de
				return
			}
			panic(r)
		}
	}()
	sentinel := s.Preallocate()
	tail := store.Put(s, int64(sentinel), ptrSer)
	head := store.Put(s, int64(sentinel), ptrSer)
	headPrev := store.Put(s, int64(0), ptrSer)
	return &QueueLong{st: s, tailRecid: tail, headRecid: head, headPrevRecid: headPrev, ptrSer: ptrSer}, nil
}

func (q *QueueLong) Store() store.Store         { return q.st }
func (q *QueueLong) TailRecid() store.Recid     { return q.tailRecid }
func (q *QueueLong) HeadRecid() store.Recid     { return q.headRecid }
func (q *QueueLong) HeadPrevRecid() store.Recid { return q.headPrevRecid }

// enter acquires the handle lock, first rejecting a same-goroutine re-entry
// from a running TakeUntil/ForEach callback.
func (q *QueueLong) enter() {
	if id := qgoid(); id != 0 && q.cbOwner.Load() == id {
		panic(mapdb5.NewCorruption("QueueLong: reentrant call into the same handle from a take_until/for_each callback"))
	}
	q.mu.Lock()
}

// startCallback marks this goroutine as the callback owner while mu is
// held; the returned func clears it (defer it).
func (q *QueueLong) startCallback() func() {
	id := qgoid()
	if id == 0 {
		panic(mapdb5.NewMisuse("QueueLong: cannot identify callback goroutine"))
	}
	q.cbOwner.Store(id)
	return func() { q.cbOwner.Store(0) }
}

func (q *QueueLong) readPtr(recid store.Recid) uint64 {
	v, ok := store.Get(q.st, recid, q.ptrSer)
	if !ok {
		panic(mapdb5.NewCorruption("QueueLong: missing queue pointer"))
	}
	return uint64(v)
}

func (q *QueueLong) writePtr(recid store.Recid, value uint64) {
	store.Update(q.st, recid, int64(value), q.ptrSer)
}

func (q *QueueLong) Tail() uint64 { q.enter(); defer q.mu.Unlock(); return q.readPtr(q.tailRecid) }
func (q *QueueLong) Head() uint64 { q.enter(); defer q.mu.Unlock(); return q.readPtr(q.headRecid) }
func (q *QueueLong) HeadPrev() uint64 {
	q.enter()
	defer q.mu.Unlock()
	return q.readPtr(q.headPrevRecid)
}

func (q *QueueLong) getNode(recid store.Recid) (LNode, bool) {
	return store.Get(q.st, recid, LNodeSer)
}

// Put appends (timestamp, value) at the head and returns the new node's
// recid. timestamp/value must be <= MaxInt64 (Java interop).
func (q *QueueLong) Put(timestamp, value uint64) store.Recid {
	checkField(timestamp)
	checkField(value)
	q.enter()
	defer q.mu.Unlock()
	next := q.st.Preallocate()
	oldHead := qnz(q.readPtr(q.headRecid))
	oldPrev := q.readPtr(q.headPrevRecid)
	store.Update(q.st, oldHead, LNode{Prev: oldPrev, Next: uint64(next), Timestamp: timestamp, Value: value}, LNodeSer)
	q.writePtr(q.headRecid, uint64(next))
	q.writePtr(q.headPrevRecid, uint64(oldHead))
	return oldHead
}

// PutPreallocated inserts a caller-preallocated node at the queue head.
func (q *QueueLong) PutPreallocated(timestamp, value uint64, nodeRecid store.Recid) {
	checkField(timestamp)
	checkField(value)
	q.enter()
	defer q.mu.Unlock()
	q.putPreallocatedLocked(timestamp, value, nodeRecid)
}

// locked body shared with Bump (already holds the lock).
func (q *QueueLong) putPreallocatedLocked(timestamp, value uint64, nodeRecid store.Recid) {
	prev := q.readPtr(q.headPrevRecid)
	sentinel := q.readPtr(q.headRecid)
	store.Update(q.st, nodeRecid, LNode{Prev: prev, Next: sentinel, Timestamp: timestamp, Value: value}, LNodeSer)
	q.writePtr(q.headPrevRecid, uint64(nodeRecid))
	if prev != 0 {
		previous, ok := q.getNode(qnz(prev))
		if !ok {
			panic(mapdb5.NewCorruption("QueueLong: previous node not found"))
		}
		previous.Next = uint64(nodeRecid)
		store.Update(q.st, qnz(prev), previous, LNodeSer)
	}
	if q.readPtr(q.tailRecid) == sentinel {
		q.writePtr(q.tailRecid, uint64(nodeRecid))
	}
}

// Take removes and returns the oldest node, or false when empty.
func (q *QueueLong) Take() (LNode, bool) {
	q.enter()
	defer q.mu.Unlock()
	return q.takeLocked()
}

func (q *QueueLong) takeLocked() (LNode, bool) {
	oldTail := qnz(q.readPtr(q.tailRecid))
	node, ok := q.getNode(oldTail)
	if !ok {
		q.writePtr(q.headPrevRecid, 0)
		return LNode{}, false
	}
	store.Delete(q.st, oldTail, LNodeSer)
	q.writePtr(q.tailRecid, node.Next)
	// Reset headPrev to 0 iff it still points at the node we just removed
	// (the single-element case). The CAS result is ignored, as in Java.
	store.CompareAndSwap(q.st, q.headPrevRecid, int64(oldTail), int64(0), q.ptrSer)
	nextRecid := qnz(node.Next)
	if next, ok := q.getNode(nextRecid); ok {
		next.Prev = 0
		store.Update(q.st, nextRecid, next, LNodeSer)
	}
	return node, true
}

// TakeUntil consumes oldest nodes while callback returns true. The callback
// runs while the handle lock is held and must NOT call back into this same
// handle — unlike Java's reentrant synchronized that panics DataCorruption.
func (q *QueueLong) TakeUntil(callback func(nodeRecid store.Recid, node LNode) bool) {
	q.enter()
	defer q.mu.Unlock()
	done := q.startCallback()
	defer done()
	for {
		recid := qnz(q.readPtr(q.tailRecid))
		node, ok := q.getNode(recid)
		if !ok || !callback(recid, node) {
			return
		}
		q.takeLocked()
	}
}

// Remove unlinks a node. When removeNode is false its record remains
// available to the caller (used by Bump).
func (q *QueueLong) Remove(nodeRecid store.Recid, removeNode bool) LNode {
	q.enter()
	defer q.mu.Unlock()
	return q.removeLocked(nodeRecid, removeNode)
}

func (q *QueueLong) removeLocked(nodeRecid store.Recid, removeNode bool) LNode {
	node, ok := q.getNode(nodeRecid)
	if !ok {
		panic(mapdb5.NewCorruption("QueueLong: node not found"))
	}
	if removeNode {
		store.Delete(q.st, nodeRecid, LNodeSer)
	}
	nextRecid := qnz(node.Next)
	if next, ok := q.getNode(nextRecid); ok {
		if next.Prev != uint64(nodeRecid) {
			panic(mapdb5.NewCorruption("QueueLong: next-node backlink mismatch"))
		}
		next.Prev = node.Prev
		store.Update(q.st, nextRecid, next, LNodeSer)
	} else {
		if q.readPtr(q.headPrevRecid) != uint64(nodeRecid) {
			panic(mapdb5.NewCorruption("QueueLong: headPrev mismatch"))
		}
		q.writePtr(q.headPrevRecid, node.Prev)
	}
	if node.Prev != 0 {
		prevRecid := qnz(node.Prev)
		previous, ok := q.getNode(prevRecid)
		if !ok || previous.Next != uint64(nodeRecid) {
			panic(mapdb5.NewCorruption("QueueLong: previous-node link mismatch"))
		}
		previous.Next = node.Next
		store.Update(q.st, prevRecid, previous, LNodeSer)
	} else {
		if q.readPtr(q.tailRecid) != uint64(nodeRecid) {
			panic(mapdb5.NewCorruption("QueueLong: tail mismatch"))
		}
		q.writePtr(q.tailRecid, node.Next)
	}
	return node
}

// Bump moves a node to the newest position and replaces its timestamp.
func (q *QueueLong) Bump(nodeRecid store.Recid, newTimestamp uint64) {
	checkField(newTimestamp)
	q.enter()
	defer q.mu.Unlock()
	newest := q.readPtr(q.headPrevRecid)
	node, ok := q.getNode(nodeRecid)
	if !ok {
		panic(mapdb5.NewCorruption("QueueLong: node not found"))
	}
	if newest == uint64(nodeRecid) {
		node.Timestamp = newTimestamp
		store.Update(q.st, nodeRecid, node, LNodeSer)
		return
	}
	q.removeLocked(nodeRecid, false)
	q.putPreallocatedLocked(newTimestamp, node.Value, nodeRecid)
}

// Clear removes every node.
func (q *QueueLong) Clear() {
	q.TakeUntil(func(store.Recid, LNode) bool { return true })
}

// Len returns the number of nodes (O(n) walk, like Java size()).
func (q *QueueLong) Len() uint64 {
	q.enter()
	defer q.mu.Unlock()
	sentinel := q.readPtr(q.headRecid)
	recid := q.readPtr(q.tailRecid)
	var count uint64
	for recid != sentinel {
		node, ok := q.getNode(qnz(recid))
		if !ok {
			panic(mapdb5.NewCorruption("QueueLong: linked node not found"))
		}
		recid = node.Next
		count++
	}
	return count
}

// Values returns the queue values oldest-first.
func (q *QueueLong) Values() []uint64 {
	q.enter()
	defer q.mu.Unlock()
	var out []uint64
	for recid := q.readPtr(q.tailRecid); ; {
		node, ok := q.getNode(qnz(recid))
		if !ok {
			return out
		}
		out = append(out, node.Value)
		recid = node.Next
	}
}

// ForEach visits every node oldest-first: f(nodeRecid, value, timestamp).
// f runs under the handle lock and must not call back into this handle
// (see TakeUntil for the re-entry contract).
func (q *QueueLong) ForEach(f func(nodeRecid store.Recid, value, timestamp uint64)) {
	q.enter()
	defer q.mu.Unlock()
	done := q.startCallback()
	defer done()
	for recid := q.readPtr(q.tailRecid); ; {
		r := qnz(recid)
		node, ok := q.getNode(r)
		if !ok {
			return
		}
		f(r, node.Value, node.Timestamp)
		recid = node.Next
	}
}

// Verify checks queue structure; panics KindVerifyFailed on inconsistency.
func (q *QueueLong) Verify() {
	q.enter()
	defer q.mu.Unlock()
	fail := func(msg string) { panic(mapdb5.New(mapdb5.KindVerifyFailed, msg)) }
	sentinel := q.readPtr(q.headRecid)
	first := q.readPtr(q.tailRecid)
	newest := q.readPtr(q.headPrevRecid)
	if sentinel == first {
		if newest != 0 {
			fail("empty QueueLong has headPrev")
		}
		return
	}
	var previous uint64
	recid := first
	for recid != sentinel {
		node, ok := q.getNode(qnz(recid))
		if !ok {
			fail("QueueLong node missing")
		}
		if node.Prev != previous {
			fail("QueueLong backlink mismatch")
		}
		previous = recid
		recid = node.Next
	}
	if _, ok := q.getNode(qnz(sentinel)); ok {
		fail("QueueLong sentinel is not preallocated")
	}
	if previous != newest {
		fail("QueueLong headPrev mismatch")
	}
}
