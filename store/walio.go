package store

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
)

// The durability-relevant file operations the WAL reports to its event seam.
// SEC_HEADER and SEC_BODY are separate deliberately: a failure between them is
// a *partial section write*, the state W9 exists to forbid appending after.
type walOpKind int

const (
	walOpCreate walOpKind = iota
	walOpSegHeader
	walOpSecHeader
	walOpSecBody
	walOpForceData
	walOpForceFull
	walOpTruncate
	walOpUnlink
	walOpDirSync
)

// walIoEvent is one reported operation. seq is the segment's sequence number
// (0 for DirSync), off the byte offset it starts at — for a force, the file
// length it makes durable — len the bytes it writes, tag the section tag.
type walIoEvent struct {
	kind     walOpKind
	seq, off int64
	length   int64
	tag      byte
}

// walIO is the writer fault-injection/trace seam, called immediately BEFORE
// each operation; panicking *DBError fails that operation exactly as the
// platform would. Per store (a field handed in at open), not global — Go
// tests run in parallel.
type walIO interface {
	Before(e walIoEvent) error
}

// walIOEvent reports one operation if a seam is installed.
func walIOReport(io walIO, kind walOpKind, seq, off, length int64, tag byte) {
	if io == nil {
		return
	}
	if err := io.Before(walIoEvent{kind: kind, seq: seq, off: off, length: length, tag: tag}); err != nil {
		if de, ok := mapdb5.AsDBError(err); ok {
			panic(de)
		}
		panic(mapdb5.NewIO(err))
	}
}
