# PORTING-GAPS — mapdb5 Java to Go

Deliberate deviations from the Java reference implementation
([mapdb-java-store](https://github.com/mapdb/mapdb-java-store)). These are
intentional choices, not defects: each preserves correctness. The MapDB5 file
format is not frozen, so no cross-version or cross-language compatibility is
guaranteed (see `README.md`). Read this before depending on a behaviour that
matters to you.

## Concurrency model (all collections)

- **Locked reads instead of optimistic reads.** Java's StampedLock /
  seqlock-style optimistic reads are a data race under the Go memory model,
  so every read takes its lock (StoreDirect segment read lock, HTree segment
  read lock, per-record reads in BufferTreeMap). `go test -race` is a merge
  gate. The optimistic HTree leaf read (Java: enabled when both serializers
  are fixed-size) is not ported.
- **Handle retirement drains in-flight operations.** `Close`/`CloseHandle`
  waits for running operations of that handle before releasing its open
  lease; an operation started on a retired handle panics `KindClosed`.
  Iterators never hold a lock or admission across a yield, so a suspended
  `iter.Pull` cannot block `Close` or writers.
- **Re-entrancy panics instead of deadlocking.** Go mutexes are not
  reentrant. Callbacks that re-enter the same handle where Java's reentrant
  monitors would allow it (QueueLong `TakeUntil`/`ForEach` callbacks, BTree
  external-value barrier from a sync listener, a `Close` issued from inside
  the map's own operation) panic with a descriptive `DBError` instead.
- **Bulk-load iterators may not re-enter the DB.** `CreateFrom` /
  `CreateFromSortedByHash` consume the caller's iterator under the catalog
  lock (as Java does, with its reentrant lock); a DB call from inside that
  iterator panics `Misuse` and the create fails, instead of deadlocking.
- **Store reads decode outside store locks.** `StoreDirect.Get`/`Read` copy
  the record bytes under the lock and run the serializer / `RecordRead`
  action after releasing it, so a nested store read never deadlocks behind
  a queued `Commit` (Java's reentrant lock admits the nested read).
- **No thread interruption.** Blocking queue `Take`/`Put` use timed variants
  and close-handle wakeup; there is no `InterruptedException` equivalent.

## Serialization

- **Skipped serializers:** `STRING_INTERN`, `CLASS` and `JAVA` (JVM object
  serialization) have no cross-language meaning and are not ported.
- **Invalid UTF-8 strings.** A Go string holding malformed UTF-8 is written
  with each malformed byte replaced by U+FFFD, so persisted bytes are always
  well-formed. On decode, Go replaces per byte where the JDK uses maximal-
  subpart replacement (a truncated sequence becomes one U+FFFD). This only
  differs on bytes no conforming writer produces.
- **Descriptors are checked, never used to rebuild a codec.** Typed opens
  always supply the concrete serializer; the catalog check compares
  descriptor strings. Custom codecs are recorded as `CUSTOM` and must be
  re-supplied on reopen.

## Stores

- **StoreDirect / StoreWAL file I/O is read/write, not mmap.**
  `FileMmapEnable`, `FileMmapEnableIfSupported`, `FileChannelEnable`,
  `ExecutorEnable`, `CleanerHackEnable` and `ChecksumStoreEnable` are
  accepted as no-ops.
- **StoreAppendOnly serializes writers with a mutex.** Java only asserts
  single-writer use (under `-ea`); in Go an overlap would be a data race.
  It is in-memory only, as in Java. `MemoryAppendOnlyDirectDB` is an alias
  of `MemoryAppendOnlyDB` (Go has no direct buffers). `CurrentSize()`
  returns 0 (the Java default), so an HTreeCache store-size budget is
  inactive on this store. `Verify` additionally checks that every free
  recid is in the deleted state.

## BTreeMap

- The legacy `BTreeMap.ModificationListener` (pre-spec2) is not ported; the
  spec2 sync + deferred `MapModificationListener` lists are.
- Listener panics are recovered and reported as errors; a secondary error
  masked by a primary one is joined (`errors.Join`), not attached as a
  Java "suppressed" exception.

## BufferTreeMap / BufferedPageFormat

- Like Java: no modification listeners, no external values, no O(1)
  counter — `LenLong` is a full traversal.
- LSN stamping is off (Java `STAMP_LSN=false`). LSN frames are decoded but
  no LSN golden bytes are pinned.
- Stricter decoding than Java: every element must consume exactly its
  framed length; unknown opcodes, contradictory flags or a base-length
  mismatch are `DataCorruption`.
- Port-only safety: poisoning after a failed structural update, a
  closed-handle lease, a structural version (seqlock-style) so iterators
  restart after a concurrent split and stay exactly-once, and a `Verify`
  (coverage bands, frame CRCs, sibling chains).
- `LeafHeadroom` 0 means the default (128); use `LeafHeadroomNone` for
  Java's literal 0. The DB catalog stores Java's raw value.

## HTree family

- **Explicit hashers.** Go has no `hashCode()`. Keys are hashed by a
  `mdhash.Hasher`; only the mapdb5-defined hashers are wire-stable across
  languages.
- **HTreeCache eviction listener may not mutate the cache.** Re-entrant
  mutation from the eviction callback panics `Misuse` (Java permits it).
- **HTreeCache clock is monotone.** Expiry reads a high-water clock (also
  restored on reopen from the newest persisted deadline), so a backward
  clock step lengthens lifetimes instead of breaking queue order.

## Queues

- `QueueLong` timestamp/value are `uint64` restricted to `0..=MaxInt64`
  (Java reads them back as signed longs); larger values panic.

## DB facade

- **Default hasher by key type.** Java's DEFAULT hasher
  (`fmix32(hashCode ^ seed)`) is reproduced for `int32`, `int64` and
  `string` keys. For other key types a map created without a hasher
  records a Go-specific catalog marker and hashes the serialized key bytes;
  Java cannot open such a map without a matching custom hasher.
- **Supplied hashers are recorded as `CUSTOM`.** Go cannot record a class
  name, so supplying a different hasher on reopen is not detected.
- **No background expiry.** `expireExecutor` / `expireCompactThreshold` are
  not ported; schedule `HTreeCache.ExpireEvict` yourself.
- **`Delete` frees only what an open handle can reach.** Java's delete
  opens the object to clear it; Go opens are typed, so `Delete` frees a
  collection's entries only when it was opened in this session (atomics
  always free their record). Open it before `Delete` to reclaim space.
- **No untyped `db.get(name)`.** Opens are typed (`TreeMap[K,V](d, name, …)`).
- `IndexTree*` and `SortedTableMap` handles have no close/retire hook (as
  in Java).
- **Overlapping `Rollback` calls fail fast** with `KindWrongConfiguration`
  instead of blocking (Java serializes them): a blocked rollback could
  deadlock with a listener re-entering the DB during handle retirement.
