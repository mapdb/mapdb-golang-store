package store

import (
	"math/bits"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// Bit-parity encodings for on-volume pointers/counters (Java Parity). The low
// N bits of the stored long carry a checksum of the payload bits: parity1 for
// 2-aligned payloads, parity4 for 16-aligned, parity16 for 1 MiB-aligned.
//
// A raw stored value of 0 always FAILS its parity check, so "never written /
// lost update" is distinguishable from every legitimately-stored value
// (including the encoded 0 used for empty links — Parity1Set(0) == 1).

// Parity1Set encodes v (bit 0 must be clear) with an odd total bit count.
func Parity1Set(v uint64) uint64 {
	return v | uint64((bits.OnesCount64(v)+1)&1)
}

// Parity1Get validates and strips parity1. Panics DataCorruption on failure.
func Parity1Get(v uint64) uint64 {
	if bits.OnesCount64(v)&1 != 1 {
		panic(mapdb5.NewCorruption("parity1 broken"))
	}
	return v &^ 1
}

// Parity4Set encodes v (low 4 bits must be clear).
func Parity4Set(v uint64) uint64 {
	return v | uint64((bits.OnesCount64(v)+1)&0xF)
}

// Parity4Get validates and strips parity4.
func Parity4Get(v uint64) uint64 {
	x := v &^ 0xF
	if v&0xF != uint64((bits.OnesCount64(x)+1)&0xF) {
		panic(mapdb5.NewCorruption("parity4 broken"))
	}
	return x
}

// Parity16Set encodes v (low 16 bits must be clear).
func Parity16Set(v uint64) uint64 {
	return v | uint64((bits.OnesCount64(v)+1)&0xFFFF)
}

// Parity16Get validates and strips parity16.
func Parity16Get(v uint64) uint64 {
	x := v &^ 0xFFFF
	if v&0xFFFF != uint64((bits.OnesCount64(x)+1)&0xFFFF) {
		panic(mapdb5.NewCorruption("parity16 broken"))
	}
	return x
}
