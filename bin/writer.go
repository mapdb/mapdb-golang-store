// Package bin is the wire-primitives layer — the DataOutput2/DataInput2
// counterparts (spec 01 §bin). Multi-byte integers are big-endian; packed
// longs use the mapdb lineage varint: 7 bits per byte, most-significant group
// first, the terminating byte has bit 0x80 set (the inverse of LEB128).
//
// Reads panic with *mapdb5.DBError{Kind: DataCorruption} on overrun or
// malformed input — never a bare index panic (spec 06, GD5).
package bin

import (
	"encoding/binary"
	"math/bits"
	"sync"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// Writer is a growable byte sink with an explicit position, mirroring
// DataOutput2. Buf may be larger than Pos (capacity); the written region is
// Buf[:Pos].
type Writer struct {
	Buf []byte // exported for zero-copy handoff, like Java's direct buf access
	Pos int

	// pooled marks a Writer currently inside writerPool. It enforces
	// single-ownership: GetWriter clears it, PutWriter sets it, and a second
	// PutWriter (or any write into a pooled writer) panics KindMisuse rather
	// than letting two owners alias the same mutable buffer.
	pooled bool
}

// NewWriter returns a Writer with Java's default 128-byte hint.
func NewWriter() *Writer { return NewWriterHint(128) }

// NewWriterHint returns a Writer pre-sized; floored at 16 like Java.
func NewWriterHint(sizeHint int) *Writer {
	if sizeHint < 16 {
		sizeHint = 16
	}
	return &Writer{Buf: make([]byte, sizeHint)}
}

var writerPool = sync.Pool{New: func() any { return NewWriter() }}

// GetWriter returns a pooled, reset Writer. Callers must not retain Buf after
// PutWriter — use CopyBytes or write-through.
func GetWriter() *Writer {
	w := writerPool.Get().(*Writer)
	w.Pos = 0
	w.pooled = false
	return w
}

// PutWriter releases a Writer obtained from GetWriter back to the pool.
// Putting the same Writer twice is a misuse panic — it would hand the same
// mutable buffer to two future GetWriter callers.
func PutWriter(w *Writer) {
	if w == nil || w.pooled {
		panic(mapdb5.NewMisuse("PutWriter of nil or already-pooled writer"))
	}
	// Drop pathologically large buffers so one huge record does not pin
	// memory in the pool (Java relies on GC; sync.Pool would too, but the
	// bound keeps steady-state pools small).
	if cap(w.Buf) > 1<<20 {
		return
	}
	w.pooled = true
	writerPool.Put(w)
}

// checkState validates the public-field invariant before any mutation.
// Buf/Pos are exported (Java parity); callers can corrupt them, and the
// writer must report that as misuse, not a runtime bounds panic.
func (w *Writer) checkState() {
	if w.pooled {
		panic(mapdb5.NewMisuse("write into pooled writer"))
	}
	if w.Pos < 0 || w.Pos > len(w.Buf) {
		panic(mapdb5.NewMisuse("writer position out of range"))
	}
}

func (w *Writer) ensure(n int) {
	w.checkState()
	if n < 0 || w.Pos+n < w.Pos {
		panic(mapdb5.NewMisuse("writer length overflow"))
	}
	need := w.Pos + n
	if need <= len(w.Buf) {
		return
	}
	grow := len(w.Buf) * 2
	if grow < need {
		grow = need
	}
	nb := make([]byte, grow)
	copy(nb, w.Buf[:w.Pos])
	w.Buf = nb
}

// WriteU8 writes the low 8 bits of b (Java writeByte semantics). Named U8,
// not Byte, so vet's io.ByteWriter signature check stays quiet.
func (w *Writer) WriteU8(b byte) {
	w.ensure(1)
	w.Buf[w.Pos] = b
	w.Pos++
}

// Write appends p in full; error is always nil (satisfies io.Writer).
func (w *Writer) Write(p []byte) (int, error) {
	w.WriteAll(p)
	return len(p), nil
}

// WriteAll appends b.
func (w *Writer) WriteAll(b []byte) {
	w.ensure(len(b))
	copy(w.Buf[w.Pos:], b)
	w.Pos += len(b)
}

// WriteShort writes a 2-byte big-endian signed value.
func (w *Writer) WriteShort(v int16) {
	w.ensure(2)
	binary.BigEndian.PutUint16(w.Buf[w.Pos:], uint16(v))
	w.Pos += 2
}

// WriteChar writes a 2-byte big-endian unsigned value.
func (w *Writer) WriteChar(v uint16) {
	w.ensure(2)
	binary.BigEndian.PutUint16(w.Buf[w.Pos:], v)
	w.Pos += 2
}

// WriteInt writes a 4-byte big-endian signed value.
func (w *Writer) WriteInt(v int32) {
	w.ensure(4)
	binary.BigEndian.PutUint32(w.Buf[w.Pos:], uint32(v))
	w.Pos += 4
}

// WriteUint32 writes a 4-byte big-endian unsigned value.
func (w *Writer) WriteUint32(v uint32) {
	w.ensure(4)
	binary.BigEndian.PutUint32(w.Buf[w.Pos:], v)
	w.Pos += 4
}

// WriteLong writes an 8-byte big-endian signed value.
func (w *Writer) WriteLong(v int64) {
	w.ensure(8)
	binary.BigEndian.PutUint64(w.Buf[w.Pos:], uint64(v))
	w.Pos += 8
}

// WriteUint64 writes an 8-byte big-endian unsigned value.
func (w *Writer) WriteUint64(v uint64) {
	w.ensure(8)
	binary.BigEndian.PutUint64(w.Buf[w.Pos:], v)
	w.Pos += 8
}

// Maximum bytes a valid packed value occupies: ceil(64/7)==10 for 64-bit,
// ceil(32/7)==5 for 32-bit. Over-long runs are corruption, not silent wrap.
const (
	MaxPackedLongBytes = 10
	MaxPackedIntBytes  = 5
)

// packU64 is the canonical encoder, shared by PackLong and PackInt.
// Faithful transcription of Java DataOutput2.packLong.
func (w *Writer) packU64(value uint64) {
	// shift = 63 - leadingZeros(value); shift -= shift % 7.
	// For value==0: LeadingZeros64==64 → shift==-1, and Go % truncates toward
	// zero like Java, so -1 - (-1%7) == 0 → loop skipped → single terminator
	// byte 0x80.
	shift := 63 - bits.LeadingZeros64(value)
	shift -= shift % 7
	for shift != 0 {
		w.WriteU8(byte((value >> uint(shift)) & 0x7F))
		shift -= 7
	}
	w.WriteU8(byte(value&0x7F) | 0x80)
}

// PackLong writes v in packed form. Domain is non-negative (Java contract):
// a negative argument is a programmer error and panics with KindMisuse.
// Signed deltas use zigzag at the call sites that need them.
func (w *Writer) PackLong(v int64) {
	if v < 0 {
		panic(mapdb5.NewMisuse("PackLong: negative value"))
	}
	w.packU64(uint64(v))
}

// PackUint64 writes the full unsigned 64-bit domain. Store-internal; the
// public packed-long contract stays non-negative (spec 01).
func (w *Writer) PackUint64(v uint64) { w.packU64(v) }

// PackInt reinterprets v as unsigned 32-bit, matching Java
// packInt(v) == packLong(v & 0xFFFFFFFF).
func (w *Writer) PackInt(v int32) { w.packU64(uint64(uint32(v))) }

// CopyBytes returns an exact-length copy of the written region.
func (w *Writer) CopyBytes() []byte {
	w.checkState()
	out := make([]byte, w.Pos)
	copy(out, w.Buf[:w.Pos])
	return out
}

// Bytes returns the written region without copying. The slice aliases Buf —
// callers must not retain it past PutWriter or further writes.
func (w *Writer) Bytes() []byte {
	w.checkState()
	return w.Buf[:w.Pos]
}
