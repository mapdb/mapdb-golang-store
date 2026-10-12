// Package store is the recid→record layer (spec 02): the Store interface,
// the typed wrappers collections actually call, and the in-memory reference
// implementations (StoreOnHeap, StoreByteArray, ReadOnlyWrapper).
//
// All store methods PANIC with *mapdb5.DBError — corruption, misuse, closed,
// read-only, GetVoid — mirroring Java's unchecked DBException (GD5). Only
// constructors and Close return error. Null records are untyped nil at the
// erased boundary (GD1).
package store

import (
	"iter"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// Recid is a record identifier. Recid 0 is never allocated — it is the
// universal "no link" sentinel (DirTree/htree use it as in-band absent).
type Recid = uint64

// AppendRefused is Append's "cannot grow in place" result (Java REFUSED=-1).
const AppendRefused int64 = -1

// RecordRead is the push-down read action (Java Store.RecordRead). The store
// resolves the recid under its own locks and dispatches exactly one method;
// the return value is opaque to the store.
//
// Read is permanently locked for ALL callers (spec 02 §RecordRead): user
// actions may retain state, panic on garbage, or re-enter the store — none of
// which a seqlock could survive. Any future optimistic path is restricted to
// sealed built-in actions, so public RecordRead has NO re-invocation
// contract: it is invoked exactly once with the committed bytes.
type RecordRead interface {
	// OnBytes: record is byte-resident. r is positioned at content start;
	// size is the content length.
	OnBytes(r *bin.Reader, size int) int64
	// OnObject: record is object-resident (StoreOnHeap). The base helper
	// panics Unsupported — byte-oriented actions need not implement it.
	OnObject(obj any) int64
	// OnNull: record exists but is null (preallocated or explicit null).
	OnNull() int64
}

// RecordReadBase provides the default method bodies so an action can embed it
// and implement only the dialect it needs (Java's default methods). OnNull
// returns 0; OnBytes/OnObject panic KindUnsupported.
type RecordReadBase struct{}

// OnBytes panics KindUnsupported — override for byte stores.
func (RecordReadBase) OnBytes(r *bin.Reader, size int) int64 {
	panic(mapdb5.New(mapdb5.KindUnsupported, "action does not support byte records"))
}

// OnObject panics KindUnsupported — override for object stores.
func (RecordReadBase) OnObject(obj any) int64 {
	panic(mapdb5.New(mapdb5.KindUnsupported, "action does not support object records"))
}

// OnNull returns 0 — a null record is a normal outcome, not an error.
func (RecordReadBase) OnNull() int64 { return 0 }

// Store is the core recid→record interface (spec 02). Go cannot express
// Java's generic methods, so records and serializers are ERASED (any +
// ser.AnySerializer); the generic package-level wrappers (Put/Get/Update/…)
// restore compile-time safety. Locking stays inside store methods so CAS
// remains atomic.
//
// Every erased entry point validates each NON-NIL record/operand via
// ser.ValidateAny and converts a mismatch into DBError{Serialization};
// untyped-nil (null record) bypasses validation (GD1).
type Store interface {
	// Preallocate reserves a recid with null content (Preallocated state).
	// Get returns nil; Update fills it.
	Preallocate() Recid
	// PreallocateN fills `into` with fresh preallocated recids (bulk build).
	PreallocateN(into []Recid)
	// Put stores a record (nil = explicit null content), returning its recid.
	// Serialization for byte stores happens outside store locks.
	Put(record any, sr ser.AnySerializer) Recid
	// Get reads a record: the deserialized value, or nil for
	// null/preallocated content. Panics KindGetVoid for void/deleted recids.
	Get(recid Recid, sr ser.AnySerializer) any
	// Read is the locked push-down read; returns the action's value.
	Read(recid Recid, action RecordRead) int64
	// Update replaces a recid's content; nil writes null content.
	Update(recid Recid, record any, sr ser.AnySerializer)
	// CompareAndSwap is the per-recid atomic logical CAS. Equality is
	// serializer-defined (EqualsAny), NOT byte equality (unless
	// EqualsBySerializedBytes licenses the fast path). nil matches/writes
	// null content.
	CompareAndSwap(recid Recid, expectedOld, newRecord any, sr ser.AnySerializer) bool
	// Delete removes the record; its recid may be reused.
	Delete(recid Recid, sr ser.AnySerializer)

	// Commit makes preceding mutations durable (no-op for non-durable).
	Commit()
	// Compact reclaims obsolete storage where supported.
	Compact()
	// Close releases the store; the final flush may fail, so it returns
	// error. Idempotent; the store is closed after Close even on error.
	Close() error
	IsClosed() bool
	// Verify checks store invariants; panics KindVerifyFailed on
	// inconsistency. The TCK calls it after every mutation.
	Verify()
	// AllRecids yields live recids sorted ascending, excluding preallocated
	// records (Go iterator replaces PrimitiveIterator.OfLong).
	AllRecids() iter.Seq[Recid]
	IsThreadSafe() bool
	// IsReadOnly reports whether mutators are rejected at the API surface.
	IsReadOnly() bool
	// IsTx reports whether this is a transactional store (StoreTx-backed;
	// rollback can void recids — disables collection root caches).
	IsTx() bool
	// StructuralGeneration increments on every Rollback (never on commit).
	// Collections compare a cached value to invalidate derived structure.
	// Non-tx stores return a constant.
	StructuralGeneration() uint64
	// CurrentSize is the approximate byte footprint (HTreeCache budget
	// hook). Must decrease on delete. 0 = unsupported, and after Close.
	CurrentSize() int64
}

// StoreDelta is the delta capability: in-place record growth with capacity
// refusal. Implemented by byte-backed stores.
type StoreDelta interface {
	Store
	// Append extends record content. Returns the new content size, or
	// AppendRefused when capacity is insufficient (the caller then splits).
	// Appending to a preallocated/null record establishes it.
	Append(recid Recid, data []byte) int64
	// CapacityRemaining is a capacity hint; may be stale — Append is
	// authoritative.
	CapacityRemaining(recid Recid) int64
	// UpdateWithHeadroom is Update provisioning ≥ headroom appendable bytes.
	UpdateWithHeadroom(recid Recid, record any, sr ser.AnySerializer, headroom int)
}

// StoreTx is the transactional capability.
type StoreTx interface {
	Store
	// Rollback discards all uncommitted mutations, including appends.
	Rollback()
}

// validateRecord is the erased-boundary check (GD1): a non-nil record must
// satisfy the serializer's type. nil (null record) bypasses.
func validateRecord(sr ser.AnySerializer, rec any) {
	if rec != nil && !sr.ValidateAny(rec) {
		panic(mapdb5.New(mapdb5.KindSerialization, "record type does not match serializer"))
	}
}

// validateOperand validates a stored object against the read serializer —
// heap-store records carry a type; a mismatch is a Serialization panic.
func validateOperand(sr ser.AnySerializer, obj any) {
	if obj == nil || !sr.ValidateAny(obj) {
		panic(mapdb5.New(mapdb5.KindSerialization, "stored record type does not match serializer"))
	}
}

// ---- typed wrappers (the API collections actually use) ----

// Put stores a non-null record and returns its recid.
func Put[R any](s Store, record R, sr ser.Serializer[R]) Recid {
	return s.Put(record, ser.Erase(sr))
}

// Get reads a record: (value, true) for live content, (zero, false) for
// null/preallocated. "false" deliberately does not distinguish the two null
// states — Java reads both as null too.
func Get[R any](s Store, recid Recid, sr ser.Serializer[R]) (R, bool) {
	var zero R
	v := s.Get(recid, ser.Erase(sr))
	if v == nil {
		return zero, false
	}
	r, ok := v.(R)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "deserialized record type mismatch"))
	}
	return r, true
}

// Update replaces a recid's content with a non-null record.
func Update[R any](s Store, recid Recid, record R, sr ser.Serializer[R]) {
	s.Update(recid, record, ser.Erase(sr))
}

// CompareAndSwap is the typed logical CAS.
func CompareAndSwap[R any](s Store, recid Recid, oldV, newV R, sr ser.Serializer[R]) bool {
	return s.CompareAndSwap(recid, oldV, newV, ser.Erase(sr))
}

// Delete removes a record.
func Delete[R any](s Store, recid Recid, sr ser.Serializer[R]) {
	s.Delete(recid, ser.Erase(sr))
}

// RecordValue is the typed-boundary option type: Present=false ⇔ null
// record. The erased interface expresses null as nil, which a non-pointer R
// cannot; RV is the explicit equivalent.
type RecordValue[R any] struct {
	Value   R
	Present bool
}

// rvOf lifts a typed value into RecordValue form.
func rvOf[R any](v R) RecordValue[R] { return RecordValue[R]{Value: v, Present: true} }

// rvErase maps the option to the erased operand (nil when !Present).
func rvErase[R any](rv RecordValue[R]) any {
	if !rv.Present {
		return nil
	}
	return rv.Value
}

// PutRV stores a possibly-null record.
func PutRV[R any](s Store, rv RecordValue[R], sr ser.Serializer[R]) Recid {
	return s.Put(rvErase(rv), ser.Erase(sr))
}

// UpdateRV stores possibly-null content at an existing recid.
func UpdateRV[R any](s Store, recid Recid, rv RecordValue[R], sr ser.Serializer[R]) {
	s.Update(recid, rvErase(rv), ser.Erase(sr))
}

// CompareAndSwapRV is the CAS over possibly-null operands.
func CompareAndSwapRV[R any](s Store, recid Recid, oldV, newV RecordValue[R], sr ser.Serializer[R]) bool {
	return s.CompareAndSwap(recid, rvErase(oldV), rvErase(newV), ser.Erase(sr))
}

// GetRV reads into RecordValue form — identical to Get; provided for
// call-site symmetry with the RV mutators.
func GetRV[R any](s Store, recid Recid, sr ser.Serializer[R]) RecordValue[R] {
	if v, ok := Get(s, recid, sr); ok {
		return rvOf(v)
	}
	return RecordValue[R]{}
}
