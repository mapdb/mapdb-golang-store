// Atomic scalar cells over a single store record (Java org.mapdb.db.Atomic).
// Each atomic reads/writes its record through the record's logical CAS.
// All are cheap to share (they hold a store reference + recid), so the DB
// instance cache can hand back the same handle (Java same-instance parity).
//
// AtomicLong/AtomicInteger/AtomicBoolean hold a non-null primitive record;
// AtomicString/AtomicVar are nullable (a null record decodes to absent).
// Catalog rows: type=AtomicLong|AtomicInteger|AtomicBoolean|AtomicString|
// AtomicVar, recid=<decimal>, and for AtomicVar additionally
// serializer=<descriptor>.
package db

import (
	"sync"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func atomicMissing() *mapdb5.DBError {
	return mapdb5.NewCorruption("atomic record missing")
}

// atomicHandle is Java Atomic's handle read/write lock: operations hold the
// read side across the open check and the store call, so closeHandle (the
// write side) both invalidates the handle and waits for in-flight
// operations to finish before the DB frees or rewinds the record — without
// it a stale handle could write a recid the store has since handed to
// another collection.
type atomicHandle struct {
	mu     sync.RWMutex
	closed bool
}

// closeHandle invalidates the handle and drains in-flight operations; the
// owning DB calls it on delete, rollback and close (Java closeHandle).
func (h *atomicHandle) closeHandle() {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
}

// guard takes the read side and returns its release; panics KindClosed on
// an invalidated handle (Java StoreClosed "atomic handle was invalidated").
func (h *atomicHandle) guard() func() {
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		panic(mapdb5.New(mapdb5.KindClosed,
			"atomic handle was invalidated by DB delete, rollback or close"))
	}
	return h.mu.RUnlock
}

// ============================ AtomicLong ============================

// AtomicLong is a 64-bit atomic cell (Java Atomic.Long).
type AtomicLong struct {
	st    store.Store
	recid uint64
	atomicHandle
}

func newAtomicLong(s store.Store, recid uint64) *AtomicLong {
	return &AtomicLong{st: s, recid: recid}
}

// Recid is the backing record.
func (a *AtomicLong) Recid() uint64 { return a.recid }

// Get returns the current value.
func (a *AtomicLong) Get() int64 {
	defer a.guard()()
	v, ok := store.Get(a.st, a.recid, ser.Long)
	if !ok {
		panic(atomicMissing())
	}
	return v
}

// Set stores value.
func (a *AtomicLong) Set(value int64) {
	defer a.guard()()
	store.Update(a.st, a.recid, value, ser.Long)
}

// CompareAndSet is the per-record logical CAS.
func (a *AtomicLong) CompareAndSet(expect, update int64) bool {
	defer a.guard()()
	return store.CompareAndSwap(a.st, a.recid, expect, update, ser.Long)
}

// GetAndSet atomically replaces and returns the prior value.
func (a *AtomicLong) GetAndSet(update int64) int64 {
	for {
		cur := a.Get()
		if a.CompareAndSet(cur, update) {
			return cur
		}
	}
}

// AddAndGet atomically adds delta and returns the result (wrapping, Java
// long parity).
func (a *AtomicLong) AddAndGet(delta int64) int64 {
	for {
		cur := a.Get()
		next := cur + delta
		if a.CompareAndSet(cur, next) {
			return next
		}
	}
}

// GetAndAdd atomically adds delta and returns the prior value.
func (a *AtomicLong) GetAndAdd(delta int64) int64 {
	for {
		cur := a.Get()
		if a.CompareAndSet(cur, cur+delta) {
			return cur
		}
	}
}

// IncrementAndGet is AddAndGet(1).
func (a *AtomicLong) IncrementAndGet() int64 { return a.AddAndGet(1) }

// GetAndIncrement is GetAndAdd(1).
func (a *AtomicLong) GetAndIncrement() int64 { return a.GetAndAdd(1) }

// DecrementAndGet is AddAndGet(-1).
func (a *AtomicLong) DecrementAndGet() int64 { return a.AddAndGet(-1) }

// GetAndDecrement is GetAndAdd(-1).
func (a *AtomicLong) GetAndDecrement() int64 { return a.GetAndAdd(-1) }

// IntValue narrows to int32 (Java Number.intValue()).
func (a *AtomicLong) IntValue() int32 { return int32(a.Get()) }

// LongValue returns the current value (Java Number.longValue()).
func (a *AtomicLong) LongValue() int64 { return a.Get() }

// ============================ AtomicInteger ============================

// AtomicInteger is a 32-bit atomic cell (Java Atomic.Integer).
type AtomicInteger struct {
	st    store.Store
	recid uint64
	atomicHandle
}

func newAtomicInteger(s store.Store, recid uint64) *AtomicInteger {
	return &AtomicInteger{st: s, recid: recid}
}

// Recid is the backing record.
func (a *AtomicInteger) Recid() uint64 { return a.recid }

// Get returns the current value.
func (a *AtomicInteger) Get() int32 {
	defer a.guard()()
	v, ok := store.Get(a.st, a.recid, ser.Int32)
	if !ok {
		panic(atomicMissing())
	}
	return v
}

// Set stores value.
func (a *AtomicInteger) Set(value int32) {
	defer a.guard()()
	store.Update(a.st, a.recid, value, ser.Int32)
}

// CompareAndSet is the per-record logical CAS.
func (a *AtomicInteger) CompareAndSet(expect, update int32) bool {
	defer a.guard()()
	return store.CompareAndSwap(a.st, a.recid, expect, update, ser.Int32)
}

// GetAndSet atomically replaces and returns the prior value.
func (a *AtomicInteger) GetAndSet(update int32) int32 {
	for {
		cur := a.Get()
		if a.CompareAndSet(cur, update) {
			return cur
		}
	}
}

// AddAndGet atomically adds delta and returns the result.
func (a *AtomicInteger) AddAndGet(delta int32) int32 {
	for {
		cur := a.Get()
		next := cur + delta
		if a.CompareAndSet(cur, next) {
			return next
		}
	}
}

// GetAndAdd atomically adds delta and returns the prior value.
func (a *AtomicInteger) GetAndAdd(delta int32) int32 {
	for {
		cur := a.Get()
		if a.CompareAndSet(cur, cur+delta) {
			return cur
		}
	}
}

// IncrementAndGet is AddAndGet(1).
func (a *AtomicInteger) IncrementAndGet() int32 { return a.AddAndGet(1) }

// GetAndIncrement is GetAndAdd(1).
func (a *AtomicInteger) GetAndIncrement() int32 { return a.GetAndAdd(1) }

// IntValue returns the current value (Java Number.intValue()).
func (a *AtomicInteger) IntValue() int32 { return a.Get() }

// LongValue widens to int64 (Java Number.longValue()).
func (a *AtomicInteger) LongValue() int64 { return int64(a.Get()) }

// ============================ AtomicBoolean ============================

// AtomicBoolean is a boolean atomic cell (Java Atomic.Boolean).
type AtomicBoolean struct {
	st    store.Store
	recid uint64
	atomicHandle
}

func newAtomicBoolean(s store.Store, recid uint64) *AtomicBoolean {
	return &AtomicBoolean{st: s, recid: recid}
}

// Recid is the backing record.
func (a *AtomicBoolean) Recid() uint64 { return a.recid }

// Get returns the current value.
func (a *AtomicBoolean) Get() bool {
	defer a.guard()()
	v, ok := store.Get(a.st, a.recid, ser.Boolean)
	if !ok {
		panic(atomicMissing())
	}
	return v
}

// Set stores value.
func (a *AtomicBoolean) Set(value bool) {
	defer a.guard()()
	store.Update(a.st, a.recid, value, ser.Boolean)
}

// CompareAndSet is the per-record logical CAS.
func (a *AtomicBoolean) CompareAndSet(expect, update bool) bool {
	defer a.guard()()
	return store.CompareAndSwap(a.st, a.recid, expect, update, ser.Boolean)
}

// GetAndSet atomically replaces and returns the prior value.
func (a *AtomicBoolean) GetAndSet(update bool) bool {
	for {
		cur := a.Get()
		if a.CompareAndSet(cur, update) {
			return cur
		}
	}
}

// ============================ AtomicString ============================

// nullableStringSer is the presence-byte codec for AtomicString (Java
// Serializers.STRING_NULLABLE): 0x00 = null, else 0x01 then the ordinary
// STRING encoding. AtomicString always writes a (non-null) record whose
// CONTENT encodes null-ness — never a store-level null record.
type nullableStringSer struct{}

func (nullableStringSer) Serialize(w *bin.Writer, v *string) {
	if v == nil {
		w.WriteU8(0)
		return
	}
	w.WriteU8(1)
	ser.String.Serialize(w, *v)
}

func (nullableStringSer) Deserialize(r *bin.Reader, _ int) *string {
	if r.ReadU8() == 0 {
		return nil
	}
	s := ser.String.Deserialize(r, -1)
	return &s
}

func (nullableStringSer) FixedSize() int { return -1 }
func (nullableStringSer) SizeHint() int  { return 32 }
func (nullableStringSer) Compare(a, b *string) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	return ser.String.Compare(*a, *b)
}
func (nullableStringSer) Equals(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
func (nullableStringSer) NaturalOrder() bool            { return true }
func (nullableStringSer) EqualsBySerializedBytes() bool { return true }

// AtomicString is a nullable string cell (Java Atomic.String). The record
// is ALWAYS present; its content's presence byte encodes null-ness, so
// create with no initial value writes a present record whose first byte is
// 0x00 and Get returns ("", false).
type AtomicString struct {
	st    store.Store
	recid uint64
	atomicHandle
}

func newAtomicString(s store.Store, recid uint64) *AtomicString {
	return &AtomicString{st: s, recid: recid}
}

// Recid is the backing record.
func (a *AtomicString) Recid() uint64 { return a.recid }

// Get returns the current value; ok=false when the stored value is null
// (Java returns null).
func (a *AtomicString) Get() (string, bool) {
	defer a.guard()()
	v, ok := store.Get(a.st, a.recid, nullableStringSer{})
	if !ok || v == nil {
		return "", false
	}
	return *v, true
}

// Set stores value; nil clears to null.
func (a *AtomicString) Set(value *string) {
	defer a.guard()()
	store.Update(a.st, a.recid, value, nullableStringSer{})
}

// SetValue stores a non-null string.
func (a *AtomicString) SetValue(value string) { a.Set(&value) }

// CompareAndSet is the per-record logical CAS over possibly-null operands.
func (a *AtomicString) CompareAndSet(expect, update *string) bool {
	defer a.guard()()
	return store.CompareAndSwap(a.st, a.recid, expect, update, nullableStringSer{})
}

// ============================ AtomicVar ============================

// AtomicVar is a nullable atomic cell over an arbitrary element serializer
// (Java Atomic.Var<E>).
type AtomicVar[V any] struct {
	st    store.Store
	recid uint64
	sr    ser.Serializer[V]
	atomicHandle
}

func newAtomicVar[V any](s store.Store, recid uint64, sr ser.Serializer[V]) *AtomicVar[V] {
	return &AtomicVar[V]{st: s, recid: recid, sr: sr}
}

// Recid is the backing record.
func (a *AtomicVar[V]) Recid() uint64 { return a.recid }

// Serializer is the element codec.
func (a *AtomicVar[V]) Serializer() ser.Serializer[V] { return a.sr }

// Get returns the current value; ok=false for a null record.
func (a *AtomicVar[V]) Get() (V, bool) {
	defer a.guard()()
	return store.Get(a.st, a.recid, a.sr)
}

// Set stores a possibly-null value.
func (a *AtomicVar[V]) Set(value *V) {
	defer a.guard()()
	store.UpdateRV(a.st, a.recid, rvOf(value), a.sr)
}

// SetValue stores a non-null value.
func (a *AtomicVar[V]) SetValue(value V) {
	defer a.guard()()
	store.Update(a.st, a.recid, value, a.sr)
}

// CompareAndSet is the per-record logical CAS over possibly-null operands.
func (a *AtomicVar[V]) CompareAndSet(expect, update *V) bool {
	defer a.guard()()
	return store.CompareAndSwapRV(a.st, a.recid, rvOf(expect), rvOf(update), a.sr)
}

// GetAndSet atomically replaces and returns the prior value (possibly null).
func (a *AtomicVar[V]) GetAndSet(update *V) (V, bool) {
	for {
		cur, ok := a.Get()
		if a.CompareAndSet(optPtr(cur, ok), update) {
			return cur, ok
		}
	}
}

// rvOf lifts a nullable pointer into the RecordValue boundary form.
func rvOf[V any](v *V) store.RecordValue[V] {
	if v == nil {
		return store.RecordValue[V]{}
	}
	return store.RecordValue[V]{Value: *v, Present: true}
}

// optPtr maps a (value, ok) pair to a nullable pointer.
func optPtr[V any](v V, ok bool) *V {
	if !ok {
		return nil
	}
	return &v
}
