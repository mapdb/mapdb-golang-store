package store

import (
	"iter"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

// ReadOnlyWrapper — a Store decorator that rejects every mutating operation
// (Java StoreReadOnlyWrapper). Reads/inspection pass through; mutators panic
// KindReadOnly. Commit is a harmless no-op; Close closes the delegate.
//
// Logical guard, not an OS-level mode: it rejects mutations at the Store API
// only. It is NOT a StoreTx — a read-only view has nothing to roll back.
type ReadOnlyWrapper struct {
	delegate Store
}

// NewReadOnly wraps s as read-only.
func NewReadOnly(s Store) *ReadOnlyWrapper { return &ReadOnlyWrapper{delegate: s} }

// Delegate returns the wrapped store (mutating through it bypasses the
// guard — callers must not).
func (w *ReadOnlyWrapper) Delegate() Store { return w.delegate }

func roFail() {
	panic(mapdb5.New(mapdb5.KindReadOnly, "store is read-only"))
}

func (w *ReadOnlyWrapper) Preallocate() Recid   { roFail(); return 0 }
func (w *ReadOnlyWrapper) PreallocateN([]Recid) { roFail() }
func (w *ReadOnlyWrapper) Put(any, ser.AnySerializer) Recid {
	roFail()
	return 0
}
func (w *ReadOnlyWrapper) Update(Recid, any, ser.AnySerializer) { roFail() }
func (w *ReadOnlyWrapper) CompareAndSwap(Recid, any, any, ser.AnySerializer) bool {
	roFail()
	return false
}
func (w *ReadOnlyWrapper) Delete(Recid, ser.AnySerializer) { roFail() }
func (w *ReadOnlyWrapper) Compact()                        { roFail() }

// Commit is a no-op — a read-only view has nothing to make durable.
func (w *ReadOnlyWrapper) Commit() {}

func (w *ReadOnlyWrapper) Close() error { return w.delegate.Close() }

func (w *ReadOnlyWrapper) Get(recid Recid, sr ser.AnySerializer) any {
	return w.delegate.Get(recid, sr)
}
func (w *ReadOnlyWrapper) Read(recid Recid, action RecordRead) int64 {
	return w.delegate.Read(recid, action)
}
func (w *ReadOnlyWrapper) Verify()                    { w.delegate.Verify() }
func (w *ReadOnlyWrapper) AllRecids() iter.Seq[Recid] { return w.delegate.AllRecids() }
func (w *ReadOnlyWrapper) IsClosed() bool             { return w.delegate.IsClosed() }
func (w *ReadOnlyWrapper) IsThreadSafe() bool         { return w.delegate.IsThreadSafe() }
func (w *ReadOnlyWrapper) IsReadOnly() bool           { return true }
func (w *ReadOnlyWrapper) IsTx() bool                 { return w.delegate.IsTx() }
func (w *ReadOnlyWrapper) StructuralGeneration() uint64 {
	return w.delegate.StructuralGeneration()
}
func (w *ReadOnlyWrapper) CurrentSize() int64 { return w.delegate.CurrentSize() }

// LeaseTable delegates so read-only collections can still open over the
// wrapper (Rust StoreReadOnlyWrapper's StoreLease impl, D12). A delegate
// without the capability reports nil rather than panicking, so callers go
// through AcquireLease, which surfaces it as the documented unsupported
// error return.
func (w *ReadOnlyWrapper) LeaseTable() *LeaseTable {
	sl, ok := w.delegate.(StoreLease)
	if !ok {
		return nil
	}
	return sl.LeaseTable()
}

// ReadOnlyDeltaWrapper additionally rejects the StoreDelta growth surface.
// Use it when callers hold the store as StoreDelta.
type ReadOnlyDeltaWrapper struct {
	ReadOnlyWrapper
	delta StoreDelta
}

// NewReadOnlyDelta wraps a StoreDelta as read-only, rejecting Append and
// UpdateWithHeadroom while passing CapacityRemaining through.
func NewReadOnlyDelta(s StoreDelta) *ReadOnlyDeltaWrapper {
	return &ReadOnlyDeltaWrapper{ReadOnlyWrapper: ReadOnlyWrapper{delegate: s}, delta: s}
}

func (w *ReadOnlyDeltaWrapper) Append(Recid, []byte) int64 { roFail(); return AppendRefused }
func (w *ReadOnlyDeltaWrapper) CapacityRemaining(recid Recid) int64 {
	return w.delta.CapacityRemaining(recid)
}
func (w *ReadOnlyDeltaWrapper) UpdateWithHeadroom(Recid, any, ser.AnySerializer, int) {
	roFail()
}
