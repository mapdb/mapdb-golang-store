// Package mapdb5 is a Go port of the MapDB5 storage engine: a record store
// with write-ahead logging and crash recovery, persistent ordered maps, and
// the MapDB serializer family.
//
// The on-disk format is not stabilised. There is no compatibility guarantee —
// not between the Java, Rust, Zig and Go implementations, and not between
// versions of any one of them. Do not put data you care about in it.
//
// Error model (spec 06, GD5): constructors/openers/bulk loaders return error;
// every operation after a successful open panics with *DBError on corruption,
// misuse or I/O failure — mirroring Java's unchecked DBException. Recover with
// AsDBError.
package mapdb5
