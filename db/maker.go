// DBMaker — the runtime DB builder (Java org.mapdb.DBMaker / Rust
// db::maker::DBMaker). Selects the backend (heap / byte-array / in-memory
// direct / file), applies option flags, validates illegal combinations, and
// constructs the DB over a ConfiguredStore.
package db

import (
	"os"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/store"
)

// backend selects which store implementation the DB opens.
type backend int

const (
	backendHeap         backend = iota // StoreOnHeap
	backendByteArray                   // StoreByteArray
	backendMemoryDirect                // in-memory StoreDirect
	backendAppendOnly                  // in-memory StoreAppendOnly
	backendFile                        // file StoreDirect or StoreWAL
)

// DBMaker is a runtime DB builder (Java DBMaker.Maker). Option methods
// return the receiver for chaining; each mutates the maker (Java parity —
// a maker is single-use).
type DBMaker struct {
	makeErr error // deferred builder failure (TempFileDB), returned by Make
	backend backend
	path    string // backendFile only

	transactionEnable     bool
	readOnly              bool
	deleteAfterClose      bool
	deleteAfterOpen       bool
	walRecoveryIndexBytes int64 // <=0 means "default"
	hasWALLimit           bool
}

// MemoryDB builds an in-memory StoreDirect DB (Java DBMaker.memoryDB()).
func MemoryDB() *DBMaker { return &DBMaker{backend: backendMemoryDirect} }

// MemoryDirectDB is memoryDB's direct-store alias (Java memoryDirectDB()).
func MemoryDirectDB() *DBMaker { return MemoryDB() }

// HeapDB builds an in-memory object-heap DB (Java DBMaker.heapDB()).
func HeapDB() *DBMaker { return &DBMaker{backend: backendHeap} }

// MemoryByteArrayDB builds an in-memory byte-array DB (Java
// DBMaker.memoryByteArrayDB()).
func MemoryByteArrayDB() *DBMaker { return &DBMaker{backend: backendByteArray} }

// MemoryAppendOnlyDB builds an in-memory append-only DB (Java
// DBMaker.memoryAppendOnlyDB()): single-writer store with lock-free readers,
// built for BufferTreeMap.
func MemoryAppendOnlyDB() *DBMaker { return &DBMaker{backend: backendAppendOnly} }

// MemoryAppendOnlyDirectDB is the off-heap alias (Java
// memoryAppendOnlyDirectDB()); Go has no direct buffers, so it is identical.
func MemoryAppendOnlyDirectDB() *DBMaker { return MemoryAppendOnlyDB() }

// FileDB builds a file-backed DB (Java DBMaker.fileDB(file)).
func FileDB(path string) *DBMaker { return &DBMaker{backend: backendFile, path: path} }

// TempFileDB creates a fresh temporary-file DB deleted after close (Java
// DBMaker.tempFileDB()). The file is placed in the system temp dir.
//
// A failure to create the file is deferred to Make, which returns it:
// builders never panic, constructors return error.
func TempFileDB() *DBMaker {
	f, err := os.CreateTemp("", "mapdb5-*.db")
	if err != nil {
		return &DBMaker{backend: backendFile, makeErr: mapdb5.NewIO(err)}
	}
	path := f.Name()
	_ = f.Close()
	m := FileDB(path)
	m.deleteAfterClose = true
	return m
}

// TransactionEnable selects the WAL store — required for file DBs that
// need commit/rollback (Java transactionEnable()).
func (m *DBMaker) TransactionEnable() *DBMaker {
	m.transactionEnable = true
	return m
}

// WALRecoveryIndexLimit sets the dense in-memory WAL recovery index limit
// in bytes. The default is 64 MiB; a valid store with a high recid may
// need a larger limit to reopen. Requires a file DB with transactions
// enabled.
func (m *DBMaker) WALRecoveryIndexLimit(bytes int64) *DBMaker {
	m.walRecoveryIndexBytes = bytes
	m.hasWALLimit = true
	return m
}

// ReadOnly opens the DB read-only (Java readOnly()).
func (m *DBMaker) ReadOnly() *DBMaker {
	m.readOnly = true
	return m
}

// FileDeleteAfterClose deletes the backing file(s) after close (Java
// fileDeleteAfterClose()).
func (m *DBMaker) FileDeleteAfterClose() *DBMaker {
	m.deleteAfterClose = true
	return m
}

// FileDeleteAfterOpen deletes the backing file immediately after the DB
// is constructed and validated (Java fileDeleteAfterOpen()).
func (m *DBMaker) FileDeleteAfterOpen() *DBMaker {
	m.deleteAfterOpen = true
	return m
}

// ---- accepted no-op options (documented deviations from Java) ----

// FileMmapEnable is accepted as a no-op: this port's file volume is
// read/write I/O, not mmap.
func (m *DBMaker) FileMmapEnable() *DBMaker { return m }

// FileMmapEnableIfSupported is accepted as a no-op (see FileMmapEnable).
func (m *DBMaker) FileMmapEnableIfSupported() *DBMaker { return m }

// ExecutorEnable is accepted as a no-op: Go callers manage their own
// goroutines.
func (m *DBMaker) ExecutorEnable() *DBMaker { return m }

// CleanerHackEnable is accepted as a no-op.
func (m *DBMaker) CleanerHackEnable() *DBMaker { return m }

// FileChannelEnable is accepted as a no-op.
func (m *DBMaker) FileChannelEnable() *DBMaker { return m }

// ChecksumStoreEnable is accepted as a no-op.
func (m *DBMaker) ChecksumStoreEnable() *DBMaker { return m }

// CloseOnJVMShutdown is accepted as a no-op: Go has no JVM shutdown hooks.
// Callers should close the DB on signal themselves.
func (m *DBMaker) CloseOnJVMShutdown() *DBMaker { return m }

// CloseOnJVMShutdownWeakReference is accepted as a no-op.
func (m *DBMaker) CloseOnJVMShutdownWeakReference() *DBMaker { return m }

// Make builds the DB (Java make()).
func (m *DBMaker) Make() (*DB, error) {
	if m.makeErr != nil {
		return nil, m.makeErr
	}
	// Reject illegal option combinations (Java DBMaker.make guards).
	wrongConfig := func(msg string) (*DB, error) {
		return nil, mapdb5.New(mapdb5.KindWrongConfiguration, msg)
	}
	isFile := m.backend == backendFile
	switch {
	case m.readOnly && m.transactionEnable:
		return wrongConfig("readOnly and transactionEnable are mutually exclusive")
	case m.transactionEnable && !isFile:
		return wrongConfig("transactionEnable requires a file DB")
	case m.hasWALLimit && !m.transactionEnable:
		return wrongConfig("walRecoveryIndexLimit requires transactionEnable")
	case m.deleteAfterOpen && !isFile:
		return wrongConfig("fileDeleteAfterOpen requires a file DB")
	case m.deleteAfterClose && !isFile:
		// Java rejects fileDeleteAfterClose on a non-file backend rather
		// than silently ignoring it.
		return wrongConfig("fileDeleteAfterClose requires a file DB")
	case m.deleteAfterOpen && m.transactionEnable:
		// WAL checkpointing recreates the path, so delete-after-open is
		// unsafe.
		return wrongConfig("fileDeleteAfterOpen is incompatible with transactionEnable (WAL)")
	}

	var cleanup []string
	// A file to unlink AFTER the DB is constructed + validated
	// (delete-after-open). Deferring the unlink means a failing Make over
	// a pre-existing non-MapDB file never destroys the user's data.
	var deleteAfterOpenPath string
	// A WAL whose delete-on-close flag is armed only AFTER the catalog
	// validates: arming it earlier means a rejected catalog's failure-path
	// store.Close() deletes a namespace this maker never owned.
	var walToArm *store.StoreWAL
	var st store.Store
	var err error

	switch m.backend {
	case backendHeap:
		s := store.NewStoreOnHeap(true)
		if m.readOnly {
			st = readOnlyStore(s)
		} else {
			st = s
		}
	case backendByteArray:
		s := store.NewStoreByteArray(true)
		if m.readOnly {
			st = readOnlyStore(s)
		} else {
			st = s
		}
	case backendMemoryDirect:
		s := store.NewStoreDirectHeap(true)
		if m.readOnly {
			st = readOnlyStore(s)
		} else {
			st = s
		}
	case backendAppendOnly:
		s := store.NewStoreAppendOnly()
		if m.readOnly {
			st = readOnlyStore(s)
		} else {
			st = s
		}
	case backendFile:
		if m.transactionEnable {
			// A WAL store owns a NAMESPACE, not a file, and deleting it is
			// the store's own job — inside Close, while it still holds the
			// store lock. Handing the path to the DB-layer cleanup instead
			// would delete after the lock was released, where a second
			// opener can already have acquired the namespace, and would
			// miss every segment besides.
			var wal *store.StoreWAL
			if m.hasWALLimit {
				wal, err = store.OpenStoreWALRecoveryLimit(m.path, true, m.walRecoveryIndexBytes)
			} else {
				wal, err = store.OpenStoreWAL(m.path, true)
			}
			if err != nil {
				return nil, err
			}
			if m.deleteAfterClose {
				walToArm = wal
			}
			st = wal
		} else {
			if m.deleteAfterClose {
				cleanup = append(cleanup, m.path)
			}
			var direct *store.StoreDirect
			direct, err = store.NewStoreDirectFile(m.path, true)
			if err != nil {
				return nil, err
			}
			if m.deleteAfterOpen {
				// Defer: unlink only once the DB has validated recid 1 as a
				// real MapDB catalog. The open StoreDirect keeps working
				// from its handle after the unlink (POSIX unlink-open).
				deleteAfterOpenPath = m.path
			}
			if m.readOnly {
				st = readOnlyStore(direct)
			} else {
				st = direct
			}
		}
	}

	// Construct + validate the DB FIRST. If this fails (a pre-existing
	// non-MapDB file), we return the error WITHOUT unlinking anything: a
	// path this maker did not itself create is never destroyed.
	db, err := newDBErr(st, cleanup)
	if err != nil {
		// The DB was never handed back — close the just-opened store so a
		// file handle/lock is not leaked. The WAL's delete-on-close was
		// never armed, so the existing namespace survives.
		_ = st.Close()
		return nil, err
	}
	// Validated as a real MapDB store: now it is safe to arm WAL
	// delete-on-close (Java arms the after-close deletion only on a
	// successfully constructed DB).
	if walToArm != nil {
		walToArm.SetDeleteOnClose(true)
	}
	// Validated as a real MapDB store: now it is safe to unlink for
	// delete-after-open (the open store handle keeps working until close).
	if deleteAfterOpenPath != "" {
		if delErr := removeFile(deleteAfterOpenPath); delErr != nil {
			// Java propagates a real deletion error; tear down the
			// just-built DB (never leak an open handle) and surface the
			// error, preserving any close error.
			if closeErr := db.Close(); closeErr != nil {
				return nil, mapdb5.New(mapdb5.KindDataCorruption,
					"fileDeleteAfterOpen failed ["+delErr.Error()+
						"]; and closing the DB also failed ["+closeErr.Error()+"]")
			}
			return nil, delErr
		}
	}
	return db, nil
}

// newDBErr is NewDB's error-returning twin (the maker's boundary).
func newDBErr(s store.Store, cleanup []string) (d *DB, err error) {
	defer dbErrorToErr(&err)
	return newDB(s, cleanup), nil
}

// removeFile is delete-after-open's unlink (Java propagates non-NotFound
// failures — removeFileIfExists preserves them).
func removeFile(p string) error {
	return removeFileIfExists(p)
}

// readOnlyStore wraps s as read-only, keeping the StoreDelta capability
// when s has it (Java StoreReadOnlyWrapper implements StoreDelta), so
// delta-backed collections such as BufferTreeMap reopen read-only.
func readOnlyStore(s store.Store) store.Store {
	if ds, ok := s.(store.StoreDelta); ok {
		return store.NewReadOnlyDelta(ds)
	}
	return store.NewReadOnly(s)
}
