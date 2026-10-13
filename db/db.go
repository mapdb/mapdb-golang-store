// Package db is the DB facade (Java org.mapdb.db.DB): the name catalog at
// recid 1, typed collection makers, a DB-owned per-name instance cache, and
// the close lifecycle. The reference semantics are Java's
// mapdb-java-store org.mapdb.db.DB + DBMaker, checked against the Rust and
// Zig ports (mapdb-rust-store/src/db, mapdb-zig-store/src/db).
//
// Error discipline (module convention, GD5): constructors and makers return
// error (a *mapdb5.DBError); all runtime operations PANIC with *mapdb5.DBError
// — corruption, closed, wrong configuration — mirroring Java's unchecked
// DBException. dbErrorToErr converts at constructor boundaries.
package db

import (
	"errors"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/internal/handle"
	"github.com/mapdb/mapdb5-go/store"
)

// DB lifecycle states.
const (
	dbOpen    int32 = 0
	dbClosing int32 = 1
	dbClosed  int32 = 2
)

// cacheEntry is one cached open instance: the handle plus an optional
// retire hook (queues wake blocked waiters, atomics invalidate, maps
// invalidate + release their open lease) and a best-effort data-record
// teardown for delete (maps/sets clear entry/node records — the structural
// root and counter records LEAK, per Java: freeing them would let the
// store reuse a recid a still-live handle keeps writing; queues free node
// AND header records since all clones share one globally-closable handle).
// retireLast inverts delete ordering for maps/sets: entries are cleared
// while the handle is still live, then the handle is retired — the
// opposite of queues/atomics, which must invalidate before their records
// are freed.
type cacheEntry struct {
	handle     any
	retire     func()
	teardown   func()
	retireLast bool
}

// DB is the named-object facade over a store.Store. Not safe to copy.
// Collections hold their own store reference, so they outlive the DB but
// fail KindClosed once it closes the store.
type DB struct {
	st store.Store

	adminMu sync.Mutex // serializes catalog + cache + lifecycle mutations
	// userCodeGoid is the goroutine running caller code (a bulk-load
	// iterator) while holding adminMu; 0 otherwise. See withUserCode.
	userCodeGoid atomic.Uint64
	catalog      NameCatalog
	cache        map[string]*cacheEntry
	// draining is set while rollback detaches the cache and retires its
	// handles OUTSIDE the admin lock: a catalog mutation or handle
	// adoption landing in that window would be rewound while its handle
	// stayed cached. Guarded by adminMu; checked at publish/insert.
	draining bool

	state  atomic.Int32 // dbOpen → dbClosing → dbClosed
	closed chan struct{}

	// cleanupPaths are files removed after the store closes (temp DB /
	// delete-after-close). NON-TRANSACTIONAL backends only: a WAL store owns
	// a segment namespace and deletes it itself inside Close while it still
	// holds the store lock.
	cleanupPaths []string
}

// ============================ construction ============================

// NewDB wraps a store, initializing or validating the name catalog at
// recid 1.
//
// Fresh (empty) store: put(empty catalog) must land on recid 1 (else a
// foreign writer already occupies it → error, best-effort undo), then the
// initial empty catalog is committed — the sole hidden facade commit.
//
// Non-empty store: recid 1 must already hold a valid catalog; a missing
// record is a wrong store, malformed bytes are corruption — never repaired.
func NewDB(s store.Store) (d *DB, err error) {
	defer dbErrorToErr(&err)
	return newDB(s, nil), nil
}

func newDB(s store.Store, cleanup []string) *DB {
	var catalog NameCatalog
	empty := true
	for range s.AllRecids() {
		empty = false
		break
	}
	if empty {
		if s.IsReadOnly() {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"cannot open an empty store read-only: the catalog cannot be written"))
		}
		catalog = NameCatalog{}
		recid := store.Put(s, catalog, catalogSer{})
		if recid != RecidCatalog {
			// A foreign writer already occupies recid 1: best-effort undo.
			func() { defer func() { _ = recover() }(); store.Delete(s, recid, catalogSer{}) }()
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"store is not a fresh MapDB store: recid 1 is already taken"))
		}
		// The sole hidden facade commit: persist the fresh empty catalog.
		s.Commit()
	} else {
		c, ok := store.Get(s, RecidCatalog, catalogSer{})
		if !ok {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"store has data but no name catalog at recid 1 (wrong store?)"))
		}
		catalog = c
	}
	// Reject a hostile/malformed catalog at open, before any collection can
	// be built over it.
	validateCatalog(catalog)
	return &DB{
		st:           s,
		catalog:      catalog,
		cache:        make(map[string]*cacheEntry),
		closed:       make(chan struct{}),
		cleanupPaths: cleanup,
	}
}

// Store returns the backing store.
func (d *DB) Store() store.Store { return d.st }

// ============================ admin helpers ============================

// lockAdmin acquires the administrative lock THEN rechecks lifecycle: an
// operation that queued behind an in-flight close/rollback must observe
// KindClosed once it wins the lock, so it cannot mutate into a closing DB.
// All catalog/cache mutations funnel through here. Callers defer unlock.
func (d *DB) lockAdmin() {
	d.checkNotInUserCode()
	d.adminMu.Lock()
	if d.state.Load() != dbOpen {
		d.adminMu.Unlock()
		panic(mapdb5.New(mapdb5.KindClosed, "db is closed"))
	}
}

// withUserCode runs caller-supplied code (a CreateFrom iterator) while the
// caller holds adminMu. Java's reentrant write lock lets such code re-enter
// the DB; Go's mutex would deadlock, so a re-entry from the same goroutine
// panics Misuse instead (PORTING-GAPS: re-entrancy panics, never hangs).
// The lock is kept — building outside it would race a concurrent Rollback
// discarding the half-built records.
func (d *DB) withUserCode(f func()) {
	d.userCodeGoid.Store(handle.Goid())
	defer d.userCodeGoid.Store(0)
	f()
}

// checkNotInUserCode is the withUserCode tripwire; Goid is only computed
// while a bulk load is running.
func (d *DB) checkNotInUserCode() {
	if g := d.userCodeGoid.Load(); g != 0 && g == handle.Goid() {
		panic(mapdb5.New(mapdb5.KindMisuse,
			"the DB cannot be re-entered from a CreateFrom iterator (it runs under the catalog lock)"))
	}
}

// saveCatalog persists a candidate catalog to recid 1. The DB does NOT
// auto-commit named-object catalog edits (Java parity); callers install it
// in d.catalog only after this returns, so a save failure leaves the
// in-memory catalog untouched.
func (d *DB) saveCatalog(catalog NameCatalog) {
	store.Update(d.st, RecidCatalog, catalog, catalogSer{})
}

// checkNotDraining fails named-object work fast while a rollback/close
// drain window is open. Caller holds the admin lock.
func (d *DB) checkNotDraining() {
	if d.draining {
		d.checkNotDrainingPanic()
	}
}

// checkNotDrainingPanic raises the drain-window rejection.
func (d *DB) checkNotDrainingPanic() {
	panic(mapdb5.New(mapdb5.KindWrongConfiguration,
		"DB is rolling back — named-object ops are rejected during handle retirement; retry after Rollback returns"))
}

// publishCatalog stages a catalog mutation on a COPY, persists it FIRST,
// and installs it in d.catalog only after the save succeeds.
func (d *DB) publishCatalog(mutate func(NameCatalog)) {
	d.checkNotDraining()
	staged := maps.Clone(d.catalog)
	mutate(staged)
	d.saveCatalog(staged)
	d.catalog = staged
}

// detachCache returns the cached entries and resets the cache. The caller
// retires them WITHOUT the admin lock: retirement now DRAINS in-flight map
// ops, and a drained op's listener may itself need the admin lock — a
// circular wait if the lock stayed held.
func (d *DB) detachCache() map[string]*cacheEntry {
	detached := d.cache
	d.cache = make(map[string]*cacheEntry)
	return detached
}

// retireEntries runs each entry's retire hook — must NOT hold the admin
// lock (see detachCache).
func retireEntries(entries map[string]*cacheEntry) {
	for _, e := range entries {
		if e.retire != nil {
			e.retire()
		}
	}
}

// ============================ catalog queries ============================

// GetNameCatalog returns a snapshot copy of the whole name catalog (Java
// getNameCatalog).
func (d *DB) GetNameCatalog() NameCatalog {
	d.lockAdmin()
	defer d.adminMu.Unlock()
	return maps.Clone(d.catalog)
}

// Exists reports whether a named object exists (Java exists).
func (d *DB) Exists(name string) bool {
	d.lockAdmin()
	defer d.adminMu.Unlock()
	_, ok := d.catalog[name+"#type"]
	return ok
}

// GetType returns the stored #type of name, or false when absent (Java
// getType).
func (d *DB) GetType(name string) (string, bool) {
	d.lockAdmin()
	defer d.adminMu.Unlock()
	t, ok := d.catalog[name+"#type"]
	return t, ok
}

// GetAllNames returns all named objects, ascending (Java getAllNames).
func (d *DB) GetAllNames() []string {
	d.lockAdmin()
	defer d.adminMu.Unlock()
	var names []string
	for k := range d.catalog {
		if n, ok := strings.CutSuffix(k, "#type"); ok {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	return names
}

// ============================ store passthrough ============================

// Commit makes preceding mutations durable (Java commit). No-op on a
// read-only store. Serialized with catalog mutation/close via the admin
// lock.
func (d *DB) Commit() {
	d.lockAdmin()
	defer d.adminMu.Unlock()
	if d.st.IsReadOnly() {
		return
	}
	d.st.Commit()
}

// Rollback discards uncommitted mutations and retires catalog-derived
// handles (Java rollback). Panics KindUnsupported on a non-transactional
// store. Before the rewind, every cached instance's retire hook runs
// (waking blocked queue waiters, invalidating atomics, releasing map/set
// open leases), the cache is cleared, and the catalog is reloaded +
// revalidated from recid 1: externally held old handles panic KindClosed
// on their next operation (Java "old handles are unsafe").
func (d *DB) Rollback() {
	d.lockAdmin()
	tx, ok := d.st.(store.StoreTx)
	if !ok {
		d.adminMu.Unlock()
		panic(mapdb5.New(mapdb5.KindUnsupported, "rollback on a non-transactional store"))
	}
	// One rollback owns the drain window at a time: an overlapping call
	// must not clear another's draining flag and rewind while that call's
	// retirement is still in flight (handles published in between would
	// survive the rewind). It fails fast like every other named-object op
	// during the window — blocking could deadlock a listener re-entry.
	if d.draining {
		d.adminMu.Unlock()
		d.checkNotDrainingPanic()
	}
	// Retire + drop all cached handles BEFORE rewinding (Java
	// closeRuntimeHandle on every instance, run before rollback): a stale
	// atomic could otherwise write a rewound/reused recid, a queue's
	// external reference holds no lease, and a retired map's open lease
	// must be released or a reopen reports the tree still locked.
	// Detach under the lock; retire WITHOUT it — a map's retire drains
	// in-flight ops whose listeners may re-enter the admin lock.
	d.draining = true
	detached := d.detachCache()
	d.adminMu.Unlock()
	retireEntries(detached)
	d.lockAdmin()
	d.draining = false
	defer d.adminMu.Unlock()
	tx.Rollback()
	// Reload the catalog from the reverted store and revalidate it.
	reloaded := NameCatalog{}
	if c, ok := store.Get(d.st, RecidCatalog, catalogSer{}); ok {
		reloaded = c
	}
	validateCatalog(reloaded)
	d.catalog = reloaded
}

// Compact reclaims obsolete storage where supported (Java compact).
// Serialized via the admin lock.
func (d *DB) Compact() {
	d.lockAdmin()
	defer d.adminMu.Unlock()
	d.st.Compact()
}

// IsClosed reports whether the DB has begun/finished closing.
func (d *DB) IsClosed() bool { return d.state.Load() != dbOpen }

// ============================ rename / delete ============================

// Rename rewrites every `old#...` catalog key to `new#...` without touching
// data recids, saves once, and moves the cache entry (Java rename). old
// must exist and new#type must not. The DB does NOT commit.
func (d *DB) Rename(old, newName string) {
	validateName(old)
	validateName(newName)
	d.lockAdmin()
	defer d.adminMu.Unlock()
	if _, ok := d.catalog[old+"#type"]; !ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "no such name: "+old))
	}
	if _, ok := d.catalog[newName+"#type"]; ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"cannot rename onto existing name: "+newName))
	}
	// Stage the rewrite on a COPY; install only after the save succeeds so a
	// save failure cannot leave a half-renamed in-memory catalog.
	oldPrefix := old + "#"
	staged := maps.Clone(d.catalog)
	var moved [][2]string
	for k, v := range staged {
		if strings.HasPrefix(k, oldPrefix) {
			moved = append(moved, [2]string{k, v})
		}
	}
	for _, kv := range moved {
		suffix := kv[0][len(oldPrefix):]
		delete(staged, kv[0])
		staged[newName+"#"+suffix] = kv[1]
	}
	d.saveCatalog(staged)
	d.catalog = staged
	// Cache move only after the catalog is durably restated.
	if e, ok := d.cache[old]; ok {
		delete(d.cache, old)
		d.cache[newName] = e
	}
}

// Delete unlinks all `name#...` catalog keys (saved FIRST), drops the
// cache/lease association, then best-effort frees the object's data (Java
// delete). Returns false when the name did not exist. The DB does NOT
// commit. A teardown failure leaves an unlinked leak, never a catalog
// pointer to destroyed data.
//
// Data is freed only through a handle opened in this DB session (atomics
// excepted: their single record is always freed). Java opens the object
// first; Go cannot, since opens are typed and need the caller's codecs. To
// reclaim a collection's records, open it with its maker before Delete —
// otherwise its entries leak (unreachable, never corrupting).
func (d *DB) Delete(name string) bool {
	found, atomicRecid, detached := d.deleteUnlink(name)
	if !found {
		return false
	}
	d.deleteTeardown(detached, atomicRecid)
	return true
}

// deleteUnlink is Delete's locked phase: the deferred unlock releases the
// admin lock on every exit, a catalog-save panic included.
func (d *DB) deleteUnlink(name string) (found bool, atomicRecid uint64, detached *cacheEntry) {
	d.lockAdmin()
	defer d.adminMu.Unlock()
	storedType, ok := d.catalog[name+"#type"]
	if !ok {
		return false, 0, nil
	}
	// Capture an atomic's single record recid BEFORE unlinking (params
	// vanish on strip). ONLY atomics free a record by recid here — a
	// collection's structural root/counter/header records are NEVER freed
	// off the catalog: a still-live handle keeps using them and the store
	// would hand the recid to a new object. Collections free their
	// ENTRY/NODE records through the cached handle's teardown instead.
	if isAtomicType(storedType) {
		atomicRecid = parseRecidOr0(d.catalog[name+"#recid"])
	}
	// Stage the unlink on a COPY; install only after the save succeeds.
	prefix := name + "#"
	staged := maps.Clone(d.catalog)
	for k := range staged {
		if strings.HasPrefix(k, prefix) {
			delete(staged, k)
		}
	}
	d.saveCatalog(staged)
	d.catalog = staged
	// Unlinked and durable. Drop the cached handle: queues/atomics retire
	// FIRST (a queue marks itself closed and wakes blocked waiters, an
	// atomic is invalidated, so no live op can land on a record teardown is
	// about to free), THEN the data-record teardown. Maps/sets invert
	// (retireLast): entries are cleared while the handle is still live —
	// retiring first would make Clear itself panic — then the handle is
	// retired. Both are best-effort: a failure now only leaks records —
	// it can never leave a catalog pointer to freed data.
	// The retire runs WITHOUT the admin lock: a map's CloseHandle drains
	// in-flight ops whose listeners may re-enter the lock — a circular
	// wait if it stayed held. The name is already unlinked from the
	// catalog, so concurrent opens of it cannot observe the window.
	if e, ok := d.cache[name]; ok {
		delete(d.cache, name)
		detached = e
	}
	return true, atomicRecid, detached
}

// deleteTeardown is Delete's unlocked phase: retire + free the detached
// handle's records, then (re-locked) free an atomic's single record.
func (d *DB) deleteTeardown(detached *cacheEntry, atomicRecid uint64) {
	if detached != nil {
		run := func(f func()) {
			if f != nil {
				func() { defer func() { _ = recover() }(); f() }()
			}
		}
		if detached.retireLast {
			run(detached.teardown)
			run(detached.retire)
		} else {
			run(detached.retire)
			run(detached.teardown)
		}
	}
	d.lockAdmin()
	defer d.adminMu.Unlock()
	// An atomic's single record is safe to free: it has no shared
	// structural root a clone would keep mutating (Java store.delete).
	if atomicRecid != 0 {
		func() { defer func() { _ = recover() }(); store.Delete(d.st, atomicRecid, catalogSer{}) }()
	}
}

// ============================ cache primitives ============================

// cacheLookup returns the cached handle for name, or a CachedTypeMismatch
// panic when the live entry is a different concrete type. Caller holds the
// admin lock.
func cacheLookup[T any](d *DB, name string) (T, bool) {
	var zero T
	e, ok := d.cache[name]
	if !ok {
		return zero, false
	}
	h, ok := e.handle.(T)
	if !ok {
		panic(mapdb5.New(mapdb5.KindCachedTypeMismatch,
			"'"+name+"' is already open as a different concrete type"))
	}
	return h, true
}

// cacheInsert caches a handle with optional retire hook and delete
// teardown. retireLast requests teardown-then-retire ordering on delete.
// Caller holds the admin lock.
func cacheInsert[T any](d *DB, name string, handle T, retire, teardown func(), retireLast bool) {
	d.checkNotDraining()
	d.cache[name] = &cacheEntry{handle: handle, retire: retire, teardown: teardown, retireLast: retireLast}
}

// ============================ close / lifecycle ============================

// Close releases the DB (Java close): idempotent. Wakes blocked queue
// waiters, closes the store, then runs delete-after-close cleanup — even if
// the store close errored; both errors are preserved with the store error
// primary. NO auto-commit — a WAL's uncommitted changes are intentionally
// discarded.
func (d *DB) Close() error {
	d.checkNotInUserCode()
	if !d.state.CompareAndSwap(dbOpen, dbClosing) {
		// Another caller already won the transition; wait until the store
		// is actually closed before returning (a returning Close must
		// guarantee it), then report idempotent success.
		<-d.closed
		return nil
	}
	// 1. Wake blocked queue waiters, then drop cached handles. Detach
	// under the lock; retire WITHOUT it — a map's retire drains in-flight
	// ops whose listeners may re-enter the admin lock.
	d.adminMu.Lock()
	detached := d.detachCache()
	d.adminMu.Unlock()
	retireEntries(detached)
	// 2. Close the store (capture the error but keep going).
	storeErr := errorToDBError(d.st.Close())
	// 3. Delete-after-close cleanup — runs even if the store close errored.
	cleanupErr := d.runCleanup()
	d.state.Store(dbClosed)
	close(d.closed)
	// Neither error may be dropped: store-close error primary, cleanup
	// error secondary; combine both when both failed.
	switch {
	case storeErr != nil && cleanupErr != nil:
		return mapdb5.New(mapdb5.KindDataCorruption,
			"close failed on two counts: store close ["+storeErr.Error()+
				"]; cleanup ["+cleanupErr.Error()+"]")
	case storeErr != nil:
		return storeErr
	default:
		return cleanupErr
	}
}

// errorToDBError normalizes a close error into a *DBError (the store's
// Close already returns *DBError on volume failure).
func errorToDBError(err error) error {
	if err == nil {
		return nil
	}
	if de, ok := mapdb5.AsDBError(err); ok {
		return de
	}
	return err
}

// runCleanup deletes the registered after-close files, delete-if-present.
func (d *DB) runCleanup() error {
	for _, p := range d.cleanupPaths {
		if err := removeFileIfExists(p); err != nil {
			return err
		}
	}
	return nil
}

// dbErrorToErr converts a *mapdb5.DBError panic into the named error return
// (constructor boundary, GD5); other panics propagate.
func dbErrorToErr(err *error) {
	if r := recover(); r != nil {
		if de, ok := mapdb5.AsDBError(r); ok {
			*err = de
			return
		}
		panic(r)
	}
}

// ============================ name validation ============================

// validateName enforces Java's checkName: non-empty and only
// [A-Za-z0-9._-] ('#' is forbidden — it separates a name from a parameter).
// Kept byte-wise so ordering and legality stay identical to Java.
func validateName(name string) {
	if name == "" {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration, "collection name must be non-empty"))
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == '-'
		if !ok {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"illegal collection name '"+name+"': only [A-Za-z0-9._-] allowed"))
		}
	}
}

// isAtomicType names the catalog #type values of a single-record atomic
// (Java DB.isAtomicType). Only these have their record freed by delete.
func isAtomicType(t string) bool {
	switch t {
	case "AtomicLong", "AtomicInteger", "AtomicBoolean", "AtomicString", "AtomicVar":
		return true
	}
	return false
}

// parseRecidOr0 parses a decimal recid param, treating absence/garbage as 0
// (best-effort teardown context only — strict paths use parseDecimal).
func parseRecidOr0(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// parseDecimal parses a required ASCII-decimal field (Java Long.toString).
func parseDecimal(s, what string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(mapdb5.NewCorruption("catalog " + what + " is not a decimal recid"))
	}
	return n
}

func parseRecid(cat NameCatalog, key string) uint64 {
	s, ok := cat[key]
	if !ok {
		panic(mapdb5.NewCorruption("catalog missing " + key))
	}
	return parseDecimal(s, key)
}

// parseRecidDefault0 parses an optional decimal recid param; absent → 0
// (legacy default: absent counterRecid = disabled).
func parseRecidDefault0(cat NameCatalog, key string) uint64 {
	s, ok := cat[key]
	if !ok {
		return 0
	}
	return parseDecimal(s, key)
}

// ============================ catalog validation ============================

// validateCatalog fully validates a decoded name catalog beyond MDBC
// syntax: group by the first '#', require a legal name and a known #type,
// require the EXACT required/allowed field set per type (reject unknown
// fields), validate codec descriptors, parse recids (>= 1; counterRecid==0
// allowed as disabled), booleans, and the maxNodeSize range. Called at open
// and after a rollback reload — before any collection is built over the
// catalog.
func validateCatalog(cat NameCatalog) {
	// Group `name#param -> value` by name.
	type fieldSet map[string]string
	groups := make(map[string]fieldSet)
	for k, v := range cat {
		name, param, ok := strings.Cut(k, "#")
		if !ok {
			panic(mapdb5.NewCorruption("catalog key '" + k + "' has no '#'"))
		}
		if strings.Contains(param, "#") {
			panic(mapdb5.NewCorruption("catalog key '" + k + "' has more than one '#'"))
		}
		fs := groups[name]
		if fs == nil {
			fs = fieldSet{}
			groups[name] = fs
		}
		fs[param] = v
	}
	for name, fields := range groups {
		if !legalCatalogName(name) {
			panic(mapdb5.NewCorruption("catalog has illegal object name '" + name + "'"))
		}
		ty, ok := fields["type"]
		if !ok {
			panic(mapdb5.NewCorruption("catalog object '" + name + "' has no #type"))
		}
		check := func(required, optional []string, descFields map[string]bool) {
			for _, r := range required {
				if _, ok := fields[r]; !ok {
					panic(mapdb5.NewCorruption(
						"catalog '" + name + "' (" + ty + ") missing required field '" + r + "'"))
				}
			}
			for f := range fields {
				if f == "type" {
					continue
				}
				if !slices.Contains(required, f) && !slices.Contains(optional, f) {
					panic(mapdb5.NewCorruption(
						"catalog '" + name + "' (" + ty + ") has unknown field '" + f + "'"))
				}
			}
			for f, isGroup := range descFields {
				v, ok := fields[f]
				if !ok {
					continue
				}
				valid := isValidSerDescriptor(v)
				if isGroup {
					valid = isValidGroupDescriptor(v)
				}
				if !valid {
					panic(mapdb5.NewCorruption(
						"catalog '" + name + "' field '" + f + "' has an invalid codec descriptor '" + v + "'"))
				}
			}
		}
		recidGE1 := func(field string) {
			if v, ok := fields[field]; ok {
				if parseDecimal(v, name+" "+field) < 1 {
					panic(mapdb5.NewCorruption(
						"catalog '" + name + "' " + field + " must be a decimal recid >= 1"))
				}
			}
		}
		counterOK := func(field string) {
			if v, ok := fields[field]; ok {
				parseDecimal(v, name+" "+field) // 0 allowed (disabled)
			}
		}
		maxNodeOK := func() {
			if v, ok := fields["maxNodeSize"]; ok {
				m := parseDecimal(v, name+" maxNodeSize")
				if m < 4 || m > 1_048_576 {
					panic(mapdb5.NewCorruption(
						"catalog '" + name + "' maxNodeSize '" + v + "' out of range [4, 1048576]"))
				}
			}
		}
		booleanOK := func(field string) {
			if v, ok := fields[field]; ok && v != "true" && v != "false" {
				panic(mapdb5.NewCorruption(
					"catalog '" + name + "' " + field + " is not a boolean"))
			}
		}
		switch ty {
		case "TreeMap":
			check(
				[]string{"keySerializer", "valueSerializer", "rootRecidRecid", "maxNodeSize"},
				[]string{"counterRecid", "valueInline"},
				map[string]bool{"keySerializer": true, "valueSerializer": true})
			recidGE1("rootRecidRecid")
			counterOK("counterRecid")
			maxNodeOK()
			booleanOK("valueInline")
		case "TreeSet":
			check(
				[]string{"serializer", "rootRecidRecid", "maxNodeSize"},
				[]string{"counterRecid"},
				map[string]bool{"serializer": true})
			recidGE1("rootRecidRecid")
			counterOK("counterRecid")
			maxNodeOK()
		case "BufferTreeMap":
			check(
				[]string{"keySerializer", "valueSerializer", "rootRecidRecid", "maxNodeSize", "bufferBytes", "leafHeadroom"},
				nil,
				map[string]bool{"keySerializer": true, "valueSerializer": true})
			recidGE1("rootRecidRecid")
			maxNodeOK()
			if parseDecimal(fields["bufferBytes"], name+" bufferBytes") < 32 {
				panic(mapdb5.NewCorruption("catalog '" + name + "' bufferBytes must be >= 32"))
			}
			if parseDecimal(fields["leafHeadroom"], name+" leafHeadroom") > 1<<31-1 {
				panic(mapdb5.NewCorruption("catalog '" + name + "' leafHeadroom out of range"))
			}
		case "HashMap", "HashSet", "HashMap48", "HashSet48":
			wide := ty == "HashMap48" || ty == "HashSet48"
			hasherField := "hasher"
			optional := []string{"concShift", "dirShift", "levels"}
			if wide {
				hasherField = "hasher64"
			} else {
				optional = append(optional, "kind", "counterRecid")
			}
			required := []string{"headerRecid", hasherField}
			desc := map[string]bool{"serializer": false}
			if ty == "HashMap" || ty == "HashMap48" {
				required = append(required, "keySerializer", "valueSerializer")
				desc = map[string]bool{"keySerializer": false, "valueSerializer": false}
			} else {
				required = append(required, "serializer")
			}
			check(required, optional, desc)
			recidGE1("headerRecid")
			counterOK("counterRecid")
			for _, f := range []string{"concShift", "dirShift", "levels"} {
				if v, ok := fields[f]; ok && parseDecimal(v, name+" "+f) > 64 {
					panic(mapdb5.NewCorruption("catalog '" + name + "' " + f + " out of range"))
				}
			}
			valid := validHasherMarker
			if wide {
				valid = validHasher64Marker
			}
			if !valid(fields[hasherField]) {
				panic(mapdb5.NewCorruption("catalog '" + name + "' has an unknown hasher marker '" + fields[hasherField] + "'"))
			}
			if k, ok := fields["kind"]; ok {
				allowed := k == "PLAIN" || k == "CACHE" || (ty == "HashMap" && k == "EXTERNAL")
				if !allowed {
					panic(mapdb5.NewCorruption("catalog '" + name + "' has an unknown kind '" + k + "'"))
				}
			}
		case "SortedTableMap":
			check(
				[]string{"keySerializer", "valueSerializer", "headerRecid", "entriesPerPage"},
				nil,
				map[string]bool{"keySerializer": true, "valueSerializer": true})
			recidGE1("headerRecid")
			if e := parseDecimal(fields["entriesPerPage"], name+" entriesPerPage"); e < 1 || e > 1<<31-1 {
				panic(mapdb5.NewCorruption("catalog '" + name + "' entriesPerPage out of range"))
			}
		case "IndexTreeList":
			check([]string{"headerRecid", "serializer"}, nil, map[string]bool{"serializer": false})
			recidGE1("headerRecid")
		case "IndexTreeLongLongMap":
			check([]string{"headerRecid"}, nil, nil)
			recidGE1("headerRecid")
		case "AtomicLong", "AtomicInteger", "AtomicBoolean", "AtomicString":
			check([]string{"recid"}, nil, nil)
			recidGE1("recid")
		case "AtomicVar":
			check([]string{"recid", "serializer"}, nil, map[string]bool{"serializer": false})
			recidGE1("recid")
		case "Queue", "Stack", "CircularQueue":
			check([]string{"headerRecid", "serializer"}, nil, map[string]bool{"serializer": false})
			recidGE1("headerRecid")
		default:
			panic(mapdb5.NewCorruption(
				"catalog object '" + name + "' has unknown #type '" + ty + "'"))
		}
	}
}

// legalCatalogName is validateName for the corruption path: a catalog's
// stored names are data, so an illegal one is corruption, not misuse.
func legalCatalogName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// removeFileIfExists deletes p; a missing file is not an error.
func removeFileIfExists(p string) error {
	err := os.Remove(p)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return mapdb5.NewIO(err)
}
