package ser

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
)

// Value is the closed sum for heterogeneous tuple/columnar cells (spec 01 —
// mirror of Rust's enum Value, decision D3). Java's Object[] of boxed
// primitives / String / byte[] maps to a kind tag plus fields.
type Value struct {
	Kind  ValueKind
	Num   int64  // Long/Int/Short/Byte payloads (widened)
	Str   string // String payload
	Bytes []byte // Bytes payload
}

// ValueKind tags a Value variant.
type ValueKind int

const (
	ValueLong ValueKind = iota
	ValueInt
	ValueShort
	ValueByte
	ValueString
	ValueBytes
)

// LongValue builds a Long cell.
func LongValue(v int64) Value { return Value{Kind: ValueLong, Num: v} }

// IntValue builds an Int cell.
func IntValue(v int32) Value { return Value{Kind: ValueInt, Num: int64(v)} }

// ShortValue builds a Short cell.
func ShortValue(v int16) Value { return Value{Kind: ValueShort, Num: int64(v)} }

// ByteValue builds a Byte cell.
func ByteValue(v int8) Value { return Value{Kind: ValueByte, Num: int64(v)} }

// StringValue builds a String cell.
func StringValue(v string) Value { return Value{Kind: ValueString, Str: v} }

// BytesValue builds a Bytes cell.
func BytesValue(v []byte) Value { return Value{Kind: ValueBytes, Bytes: v} }

// AsLong returns the payload iff Kind == ValueLong.
func (v Value) AsLong() (int64, bool) { return v.Num, v.Kind == ValueLong }

// AsInt returns the payload iff Kind == ValueInt.
func (v Value) AsInt() (int32, bool) { return int32(v.Num), v.Kind == ValueInt }

// AsStr returns the payload iff Kind == ValueString.
func (v Value) AsStr() (string, bool) { return v.Str, v.Kind == ValueString }

// AsBytes returns the payload iff Kind == ValueBytes.
func (v Value) AsBytes() ([]byte, bool) { return v.Bytes, v.Kind == ValueBytes }

// numI64 widens a numeric cell to int64. A non-numeric cell is a caller
// schema error: Java casts every cell to Number and fails fast with
// ClassCastException; we panic rather than silently coerce to 0.
func (v Value) numI64() int64 {
	switch v.Kind {
	case ValueLong, ValueInt, ValueShort, ValueByte:
		return v.Num
	default:
		panic(mapdb5.NewMisuse("non-numeric cell in numeric context"))
	}
}

// valueEquals is logical value equality (kind + payload).
func valueEquals(a, b Value) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case ValueString:
		return a.Str == b.Str
	case ValueBytes:
		return eqBytes(a.Bytes, b.Bytes)
	default:
		return a.Num == b.Num
	}
}
