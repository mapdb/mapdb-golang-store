// Package ser is the element-serializer and group-format layer (spec 01).
//
// Two interfaces:
//   - Serializer[A] — typed element codec + ordering + logical equality.
//   - GroupFormat[K] — the packed key/value array of one node, owning both
//     representation (an opaque Group = any) and access algorithm, with an
//     object side (materialized, copy-on-write) and a byte side (in-place
//     binary search over serialized bytes).
//
// The store layer consumes the ERASED AnySerializer form (GD1); typed
// collections hold Serializer[A] and erase once at construction.
//
// Everything here panics *mapdb5.DBError on corrupt persisted bytes or
// precondition violations (GD5) — deserialization has no error channel.
package ser

import (
	"github.com/mapdb/mapdb5-go/bin"
)

// SelfDelimit is the Deserialize size argument meaning "framed inside a
// larger stream; self-delimit" (Java's -1 sentinel).
const SelfDelimit = -1

// Serializer is the typed, user-facing element codec (spec 01 §ser).
type Serializer[A any] interface {
	// Serialize writes value to w.
	Serialize(w *bin.Writer, value A)
	// Deserialize reads a value. size is the total bytes available, or
	// SelfDelimit (-1) when the value is framed inside a larger record and
	// must self-delimit (Java semantics). r's bytes are only valid during
	// the call (see bin.Reader): copy anything the result keeps.
	Deserialize(r *bin.Reader, size int) A
	// FixedSize is the serialized size in bytes, or -1 if variable.
	FixedSize() int
	// SizeHint is the initial Writer capacity estimate.
	SizeHint() int
	// Compare is the total order over values (must match Java order exactly).
	Compare(a, b A) int
	// Equals is logical equality — CAS uses this; byte equality is wrong for
	// non-canonical encodings.
	Equals(a, b A) bool
	// NaturalOrder reports whether Compare is exactly the key type's natural
	// ordering (JDK null-comparator convention).
	NaturalOrder() bool
	// EqualsBySerializedBytes reports whether serialization is canonical
	// (equal values ⇔ byte-identical encodings), enabling byte-compare CAS.
	EqualsBySerializedBytes() bool
}

// AnySerializer is the erased store-facing form (GD1). The erased boundary is
// DYNAMICALLY checked, not compile-time safe: every erased entry point calls
// ValidateAny on every NON-NIL record/operand and converts a mismatch into
// DBError{Serialization}. The distinguished null state — untyped nil —
// bypasses ValidateAny entirely.
type AnySerializer interface {
	SerializeAny(w *bin.Writer, value any)
	DeserializeAny(r *bin.Reader, size int) any
	FixedSize() int
	SizeHint() int
	CompareAny(a, b any) int
	EqualsAny(a, b any) bool
	EqualsBySerializedBytes() bool
	// ValidateAny reports whether value is of this serializer's type. A
	// serializer's ValidateAny(nil) is false; null Put/Update/CAS operands are
	// legal and bypass it.
	ValidateAny(value any) bool
}

// AnyOf adapts a typed Serializer[A] to the erased AnySerializer surface.
// Embed it in a built-in type to get BOTH interfaces from one value — the
// embedded typed interface promotes the Serializer[A] methods and AnyOf adds
// the Any* wrappers (this is why ser.Long can be passed to either surface
// without an allocation, spec 01).
type AnyOf[A any] struct {
	Serializer[A]
}

// NewAnyOf wraps s in the erased adapter.
func NewAnyOf[A any](s Serializer[A]) AnyOf[A] { return AnyOf[A]{s} }

// SerializeAny dispatches to the typed serializer; a wrong dynamic type is a
// misuse panic — callers gate on ValidateAny first.
func (a AnyOf[A]) SerializeAny(w *bin.Writer, v any) { a.Serialize(w, v.(A)) }

// DeserializeAny dispatches to the typed deserializer.
func (a AnyOf[A]) DeserializeAny(r *bin.Reader, size int) any {
	return a.Deserialize(r, size)
}

// CompareAny dispatches to the typed comparator.
func (a AnyOf[A]) CompareAny(x, y any) int { return a.Compare(x.(A), y.(A)) }

// EqualsAny dispatches to the typed equality.
func (a AnyOf[A]) EqualsAny(x, y any) bool { return a.Equals(x.(A), y.(A)) }

// ValidateAny reports whether v's dynamic type is A. nil → false (null is a
// Store state, not a value of A — spec 01).
func (a AnyOf[A]) ValidateAny(v any) bool {
	if v == nil {
		return false
	}
	_, ok := v.(A)
	return ok
}

// Erase adapts a typed serializer to the erased store-facing form. A value
// that already implements AnySerializer (built-ins via embedded AnyOf) is
// returned as-is — no allocation.
func Erase[A any](s Serializer[A]) AnySerializer {
	if a, ok := s.(AnySerializer); ok {
		return a
	}
	return NewAnyOf(s)
}

// SerializeToBytes serializes v into a fresh exact-length buffer (pooled
// writer inside).
func SerializeToBytes[A any](s Serializer[A], v A) []byte {
	w := bin.GetWriter()
	defer bin.PutWriter(w)
	s.Serialize(w, v)
	return w.CopyBytes()
}

// DeserializeFromBytes reads a value back from b.
func DeserializeFromBytes[A any](s Serializer[A], b []byte) A {
	return s.Deserialize(bin.NewReader(b), len(b))
}
