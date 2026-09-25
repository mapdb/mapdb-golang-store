# mapdb-golang-store — MapDB5 store engine, Go port

**Nothing is implemented.** There is no Go source, no `go.mod`, no `go.work`.

The port specification — 7 codex-reviewed draft documents (2026-07-12) covering
overview, io/serialization, store, btree, htree, testing and decisions — lives in
the private `mapdb/todo` repo under `store-history/golang-store/spec/`, together with the
rest of the store engines' design record.

Start there for the draft plan. A future implementation would need to define
how it fits the `mapdb-golang` collection interfaces. The current Java
reference uses `MDBS.SD1` for StoreDirect and segmented WAL v3 with
`MDBS.WAL` segment headers. Rust and Zig also implement WAL v3, but the
on-disk format is not frozen and no Go file-format compatibility claim can be
made before an implementation exists.

Reference implementation: `mapdb-java-store`. Completed ports to read alongside
the spec: `mapdb-rust-store`, `mapdb-zig-store`.
