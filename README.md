# mapdb-golang-store — MapDB5 store engine, Go port

**Nothing is implemented.** There is no Go source, no `go.mod`, no `go.work`.

The port specification — 7 codex-reviewed draft documents (2026-07-12) covering
overview, io/serialization, store, btree, htree, testing and decisions — lives in
the private `mapdb/todo` repo under `store/golang-store/spec/`, together with the
rest of the store engines' design record.

Start there. The implementation must match the `mapdb-golang` collection
interfaces, and is byte-format-compatible with the Java, Rust and Zig ports
(`MDB5.SD1`, `MDB5.WAL`, all group-format encodings).

Reference implementation: `mapdb-java-store`. Completed ports to read alongside
the spec: `mapdb-rust-store`, `mapdb-zig-store`.
