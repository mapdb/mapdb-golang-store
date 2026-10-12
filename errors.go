package mapdb5

import "errors"

// Kind enumerates the DBError categories, mirroring the Java DBException
// hierarchy (spec 06, GD5). Kind identity is part of the contract — TCK ports
// assert exact kinds.
type Kind int

const (
	// KindGetVoid is a read of a Void or Deleted recid (Java GetVoid).
	KindGetVoid Kind = iota
	// KindRecordTooLarge: record content exceeds the maximum single-record
	// capacity.
	KindRecordTooLarge
	// KindDataCorruption: on-disk bytes failed a structural/parity/checksum
	// invariant.
	KindDataCorruption
	// KindStoreFull: a capacity ceiling was reached with nothing damaged
	// (44-bit volume ceiling, WAL sequence exhaustion, recovery budget).
	KindStoreFull
	// KindClosed: operation attempted on a closed handle.
	KindClosed
	// KindVerifyFailed: Verify found the on-disk tiling inconsistent.
	KindVerifyFailed
	// KindNotSorted: pump input was not strictly ascending.
	KindNotSorted
	// KindAlreadyOpen: a conflicting handle holds the open lease.
	KindAlreadyOpen
	// KindReadOnly: a mutating operation on a read-only store.
	KindReadOnly
	// KindUnsupported: a deliberately-unsupported API on a HEALTHY store.
	KindUnsupported
	// KindWrongConfiguration: DB-facade configuration/usage error.
	KindWrongConfiguration
	// KindCachedTypeMismatch: per-name instance-cache type collision.
	KindCachedTypeMismatch
	// KindLocked: another handle or process holds the store lock.
	KindLocked
	// KindVolumeIO: underlying I/O failure (Err carries the cause).
	KindVolumeIO
	// KindSerialization: a serializer rejected a value, or an erased-boundary
	// ValidateAny check failed (GD1).
	KindSerialization
	// KindMisuse: programmer error — violated precondition (negative PackLong
	// domain, bad index into a group format, ...).
	KindMisuse
	// KindPoisoned: operation attempted on a handle poisoned by a failed Close.
	KindPoisoned
)

// DBError is the engine's panic type — a proper error implementation carrying
// a Kind plus context (spec 06, GD5).
type DBError struct {
	Kind  Kind
	Msg   string // human-readable detail naming the failed check / reason
	Recid uint64 // valid for KindGetVoid / KindAlreadyOpen
	Err   error  // wrapped cause (KindVolumeIO)
}

func (e *DBError) Error() string {
	s := "mapdb: " + e.Kind.String()
	if e.Kind == KindGetVoid || e.Kind == KindAlreadyOpen {
		s += " (recid=" + uitoa(e.Recid) + ")"
	}
	if e.Msg != "" {
		s += ": " + e.Msg
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

// Unwrap returns the wrapped cause (KindVolumeIO), else nil.
func (e *DBError) Unwrap() error { return e.Err }

// New builds a *DBError with a message.
func New(kind Kind, msg string) *DBError {
	return &DBError{Kind: kind, Msg: msg}
}

// NewCorruption is the hot-path constructor for a named corruption check.
func NewCorruption(reason string) *DBError {
	return &DBError{Kind: KindDataCorruption, Msg: reason}
}

// NewIO wraps an I/O failure.
func NewIO(err error) *DBError {
	return &DBError{Kind: KindVolumeIO, Err: err}
}

// NewGetVoid builds the read-of-void/deleted error for a recid.
func NewGetVoid(recid uint64) *DBError {
	return &DBError{Kind: KindGetVoid, Recid: recid}
}

// NewMisuse builds a programmer-error panic value.
func NewMisuse(msg string) *DBError {
	return &DBError{Kind: KindMisuse, Msg: msg}
}

// AsDBError unwraps a recovered panic value into a *DBError.
// Usage: `if de, ok := mapdb5.AsDBError(recover()); ok { ... }`.
func AsDBError(v any) (*DBError, bool) {
	if e, ok := v.(*DBError); ok {
		return e, true
	}
	if err, ok := v.(error); ok {
		var de *DBError
		if errors.As(err, &de) {
			return de, true
		}
	}
	return nil, false
}

func (k Kind) String() string {
	switch k {
	case KindGetVoid:
		return "get void"
	case KindRecordTooLarge:
		return "record too large"
	case KindDataCorruption:
		return "data corruption"
	case KindStoreFull:
		return "store full"
	case KindClosed:
		return "closed"
	case KindVerifyFailed:
		return "verify failed"
	case KindNotSorted:
		return "input not sorted"
	case KindAlreadyOpen:
		return "already open"
	case KindReadOnly:
		return "store is read-only"
	case KindUnsupported:
		return "unsupported operation"
	case KindWrongConfiguration:
		return "wrong configuration"
	case KindCachedTypeMismatch:
		return "cached type mismatch"
	case KindLocked:
		return "locked"
	case KindVolumeIO:
		return "io error"
	case KindSerialization:
		return "serialization"
	case KindMisuse:
		return "misuse"
	case KindPoisoned:
		return "poisoned"
	default:
		return "unknown error kind"
	}
}

func uitoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
