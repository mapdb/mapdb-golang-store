package store

import "math/bits"

// Capacity-based index-value encoding (Java IndexVal, spec 02 §5):
//
//	bit 63..48  capacityUnits (16 bits) — capacity = capacityUnits * 16 bytes
//	bit 47..4   offset (44 bits, 16-aligned)
//	bit 3       linked   (oversize records as chunk chains)
//	bit 2       prealloc (P state)
//	bit 1       archive  (reserved)
//	bit 0       parity   (parity1 over the whole slot once stored)
//
// Record data layout at offset: 4-byte used-length header, then content.

const (
	ivMOffset      uint64 = 0x0000_FFFF_FFFF_FFF0
	ivFlagLinked   uint64 = 8
	ivFlagPrealloc uint64 = 4
	ivFlagArchive  uint64 = 2

	ivCapNull    uint32 = 0xFFFF // null content (P state iff prealloc flag)
	ivCapDeleted uint32 = 0xFFFE // tombstone
	ivCapMax     uint32 = 0xFFFD
	// ivMaxCapacity is the max plain-record capacity incl. the 4-byte header.
	ivMaxCapacity = int64(ivCapMax) * 16
)

func ivCompose(capUnits uint32, offset uint64, flags uint64) uint64 {
	return uint64(capUnits)<<48 | offset | flags
}
func ivCapUnits(iv uint64) uint32 { return uint32(iv >> 48) }
func ivOffset(iv uint64) uint64   { return iv & ivMOffset }
func ivIsPrealloc(iv uint64) bool { return iv&ivFlagPrealloc != 0 }
func ivIsLinked(iv uint64) bool   { return iv&ivFlagLinked != 0 }
func ivParityOK(iv uint64) bool {
	return bits.OnesCount64(iv)&1 == 1
}
