package bin

import (
	"encoding/binary"
	"math"
	"math/bits"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// Reader is a positioned, seekable read cursor over a byte slice. The slice
// may be a zero-copy view into a volume slice; Readers never copy.
//
// SetPos is unchecked (matching Java's pos(int)); the following read is what
// bounds-checks. All read methods panic *mapdb5.DBError{Kind: DataCorruption}
// on overrun or malformed input (spec 06, GD5).
type Reader struct {
	buf []byte
	pos int
}

// NewReader returns a cursor over buf positioned at 0.
func NewReader(buf []byte) *Reader { return &Reader{buf: buf} }

// NewReaderAt returns a cursor over buf positioned at pos (unchecked).
func NewReaderAt(buf []byte, pos int) *Reader { return &Reader{buf: buf, pos: pos} }

// Len is the total length of the underlying byte range.
func (r *Reader) Len() int { return len(r.buf) }

// Pos is the current read position (may exceed Len after a raw SetPos; the
// next read then panics).
func (r *Reader) Pos() int { return r.pos }

// SetPos repositions without validation. Reads still bounds-check.
func (r *Reader) SetPos(pos int) { r.pos = pos }

// Seek repositions with validation — panics if pos is out of [0, Len].
func (r *Reader) Seek(pos int) {
	if pos < 0 || pos > len(r.buf) {
		panic(mapdb5.NewCorruption("seek out of range"))
	}
	r.pos = pos
}

// Remaining is the number of bytes left from the current position (saturating).
func (r *Reader) Remaining() int {
	if n := len(r.buf) - r.pos; n > 0 {
		return n
	}
	return 0
}

// Slice exposes the underlying buffer (byte-side in-place compares).
func (r *Reader) Slice() []byte { return r.buf }

// Subslice returns buf[start:start+length] if fully in range; panics
// DataCorruption otherwise. Used by byte-side group formats reading blobs in
// place.
func (r *Reader) Subslice(start, length int) []byte {
	if start < 0 || length < 0 || start+length < start || start+length > len(r.buf) {
		panic(mapdb5.NewCorruption("subslice out of range"))
	}
	return r.buf[start : start+length]
}

// require validates that n bytes can be read from the current position and
// returns the absolute end offset. Covers every invalid state reachable via
// the unchecked SetPos/NewReaderAt: negative pos, pos > Len, negative n,
// and pos+n overflow — so no read can ever reach a bare index panic.
func (r *Reader) require(n int, reason string) int {
	if n < 0 || r.pos < 0 || n > len(r.buf)-r.pos {
		panic(mapdb5.NewCorruption(reason))
	}
	return r.pos + n
}

// ReadU8 reads one byte, advancing (Java readUnsignedByte). Named U8, not
// Byte, so vet's io.ByteReader signature check stays quiet.
func (r *Reader) ReadU8() byte {
	r.require(1, "read past end")
	b := r.buf[r.pos]
	r.pos++
	return b
}

// ReadI8 reads a signed byte.
func (r *Reader) ReadI8() int8 { return int8(r.ReadU8()) }

// ReadUnsignedByte mirrors Java readUnsignedByte.
func (r *Reader) ReadUnsignedByte() int32 { return int32(r.ReadU8()) }

// ReadFully fills dst completely, advancing by len(dst). Panics on overrun.
func (r *Reader) ReadFully(dst []byte) {
	end := r.require(len(dst), "read_fully past end")
	copy(dst, r.buf[r.pos:end])
	r.pos = end
}

// ReadBytes returns a COPY of the next n bytes. The range is validated
// BEFORE allocating — a corrupt length cannot trigger a huge allocation.
func (r *Reader) ReadBytes(n int) []byte {
	end := r.require(n, "read_bytes out of range")
	out := make([]byte, n)
	copy(out, r.buf[r.pos:end])
	r.pos = end
	return out
}

// SkipBytes advances n bytes, checked.
func (r *Reader) SkipBytes(n int) {
	end := r.require(n, "skip out of range")
	r.pos = end
}

// ReadShort reads a 2-byte big-endian signed value.
func (r *Reader) ReadShort() int16 { return int16(r.ReadU16()) }

// ReadChar reads a 2-byte big-endian unsigned value.
func (r *Reader) ReadU16() uint16 {
	end := r.require(2, "read_u16 past end")
	v := binary.BigEndian.Uint16(r.buf[r.pos:])
	r.pos = end
	return v
}

// ReadInt reads a 4-byte big-endian signed value (Java readInt).
func (r *Reader) ReadInt() int32 { return int32(r.ReadUint32()) }

// ReadUint32 reads a 4-byte big-endian unsigned value.
func (r *Reader) ReadUint32() uint32 {
	end := r.require(4, "read_i32 past end")
	v := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos = end
	return v
}

// ReadLong reads an 8-byte big-endian signed value (Java readLong).
func (r *Reader) ReadLong() int64 { return int64(r.ReadUint64()) }

// ReadUint64 reads a raw 8-byte big-endian unsigned value (DirTree bitmap
// words, long-stack chunk headers).
func (r *Reader) ReadUint64() uint64 {
	end := r.require(8, "read_i64 past end")
	v := binary.BigEndian.Uint64(r.buf[r.pos:])
	r.pos = end
	return v
}

// unpackCapped decodes a packed varint, panicking if the run exceeds maxBytes
// or the input is truncated. The encoding is most-significant-group FIRST, so
// a maxBytes-length run carries the domain's top bits in its FIRST byte:
// payload above topMask there means the value overflows the target domain and
// is rejected rather than silently wrapped — a 10-byte run whose first byte
// exceeds 0x01 exceeds u64; a 5-byte run whose first byte exceeds 0x0F
// exceeds u32.
func (r *Reader) unpackCapped(maxBytes int, topMask byte) uint64 {
	var ret uint64
	var first byte
	for i := 0; i < maxBytes; i++ {
		v := r.ReadU8()
		if i == 0 {
			first = v & 0x7F
		}
		if i == maxBytes-1 && first&^topMask != 0 {
			panic(mapdb5.NewCorruption("packed varint overflows domain"))
		}
		ret = (ret << 7) | uint64(v&0x7F)
		if v&0x80 != 0 {
			return ret
		}
	}
	panic(mapdb5.NewCorruption("packed varint too long"))
}

// packedTopMask is the legal payload mask of a packed varint's FIRST byte in
// a maxBytes-length run: u64 (10 bytes) leaves 64-63=1 bit; u32 (5 bytes)
// leaves 32-28=4 bits.
func packedTopMask(maxBytes int) byte {
	switch maxBytes {
	case MaxPackedLongBytes:
		return 0x01
	case MaxPackedIntBytes:
		return 0x0F
	default:
		panic(mapdb5.NewMisuse("packedTopMask: unknown cap"))
	}
}

// canonicalPackedLen is the number of bytes PackUint64 emits for v:
// max(1, ceil(bitlen/7)).
func canonicalPackedLen(v uint64) int {
	n := (64 - bits.LeadingZeros64(v) + 6) / 7
	if n == 0 {
		return 1
	}
	return n
}

// UnpackLong decodes a packed long (domain: non-negative int64). Panics
// DataCorruption on: run longer than 10 bytes, decoded value exceeding
// MaxInt64, non-canonical (padded/over-length) encodings, and truncation —
// matching the spec 01 decoder contract.
func (r *Reader) UnpackLong() int64 {
	start := r.pos
	ret := r.unpackCapped(MaxPackedLongBytes, 0x01)
	if ret > math.MaxInt64 {
		panic(mapdb5.NewCorruption("packed long overflows int64"))
	}
	if r.pos-start != canonicalPackedLen(ret) {
		panic(mapdb5.NewCorruption("non-canonical packed long"))
	}
	return int64(ret)
}

// UnpackUint64 decodes a packed value across the full unsigned domain
// (store-internal: LSNs, bitmap words). Same 10-byte cap and top-bit domain
// check; no canonical check — the canonical form is what the engine writes,
// but callers here consume raw u64 fields where only bounded length, domain,
// and truncation are safety-relevant.
func (r *Reader) UnpackUint64() uint64 {
	return r.unpackCapped(MaxPackedLongBytes, 0x01)
}

// UnpackInt decodes a packed int, rejecting values exceeding 32 bits. Capped
// at 5 bytes, consistent with UnpackLongSkip on over-long runs.
func (r *Reader) UnpackInt() int32 {
	ret := r.unpackCapped(MaxPackedIntBytes, 0x0F)
	if ret > math.MaxUint32 {
		panic(mapdb5.NewCorruption("packed int overflows uint32"))
	}
	return int32(uint32(ret))
}

// UnpackLongSkip skips count packed longs. Skipped values are validated
// exactly as UnpackLong would — run length, signed domain, and canonical
// encoding — so a skip never accepts bytes a decode would reject.
func (r *Reader) UnpackLongSkip(count int) {
	for ; count > 0; count-- {
		start := r.pos
		ret := r.unpackCapped(MaxPackedLongBytes, 0x01)
		if ret > math.MaxInt64 {
			panic(mapdb5.NewCorruption("packed long overflows int64"))
		}
		if r.pos-start != canonicalPackedLen(ret) {
			panic(mapdb5.NewCorruption("non-canonical packed long"))
		}
	}
}

// UnpackUint64Skip skips count packed values across the full unsigned
// domain, matching UnpackUint64's rules: bounded run length only (maxBytes —
// pass MaxPackedLongBytes for u64 or MaxPackedIntBytes for u32 payloads),
// plus truncation. Delta formats use this for zigzag values, whose encoding
// legitimately covers the whole u64 domain.
func (r *Reader) UnpackUint64Skip(count, maxBytes int) {
	mask := packedTopMask(maxBytes)
	for ; count > 0; count-- {
		r.unpackCapped(maxBytes, mask)
	}
}

// MatchBytes compares the next len(expected) bytes against expected. The
// position ALWAYS advances by len(expected), match or not (Java semantics).
// Panics on overrun.
func (r *Reader) MatchBytes(expected []byte) bool {
	end := r.require(len(expected), "match_bytes out of range")
	m := true
	for i, e := range expected {
		if r.buf[r.pos+i] != e {
			m = false
		}
	}
	r.pos = end
	return m
}
