package store

import (
	"sync"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// One-live-handle lease (rust decision D12, spec 02). Each store instance
// embeds a *LeaseTable; collections acquire a *LeaseGuard at open and release
// it at close. Hard exclusion in all build modes:
//   - ReadWrite: excludes every other lease on the header recid;
//   - ReadOnly: multiple RO allowed; excluded while an RW lease is live.
//
// Go has no RAII drop: Release must be called (typically via the collection's
// Close). Acquire returns error — it runs at constructor time, outside the
// panic discipline (GD5).

// LeaseKind is the requested access mode.
type LeaseKind int

const (
	// LeaseReadWrite — exclusive: excludes every other lease.
	LeaseReadWrite LeaseKind = iota
	// LeaseReadOnly — shared read: RO+RO allowed; excluded while RW is live.
	LeaseReadOnly
)

type leaseEntry struct {
	rw bool
	ro int // count of live RO leases (when !rw)
}

// StoreLease is the one-live-handle lease capability (Rust StoreLease, spec
// 02 D12). Collections assert it at open and acquire a LeaseGuard on their
// header recid; decorators delegate so wrapped stores keep the capability.
type StoreLease interface {
	Store
	// LeaseTable is this store's lease registry.
	LeaseTable() *LeaseTable
}

// AcquireLease takes a lease on headerRecid through s. Returns
// *DBError{KindUnsupported} as an error when s lacks the capability, and
// *DBError{KindAlreadyOpen} on a conflicting live handle.
func AcquireLease(s Store, headerRecid uint64, kind LeaseKind) (*LeaseGuard, error) {
	sl, ok := s.(StoreLease)
	if !ok {
		return nil, mapdb5.New(mapdb5.KindUnsupported, "store does not support handle leases")
	}
	lt := sl.LeaseTable()
	if lt == nil {
		// e.g. a wrapper whose delegate lacks the capability
		return nil, mapdb5.New(mapdb5.KindUnsupported, "store does not support handle leases")
	}
	return lt.Acquire(headerRecid, kind)
}

// LeaseTable is a per-store-instance registry keyed by header/root recid.
// One map op per collection open, zero per access.
type LeaseTable struct {
	mu sync.Mutex
	m  map[uint64]leaseEntry
}

// NewLeaseTable returns an empty table.
func NewLeaseTable() *LeaseTable {
	return &LeaseTable{m: make(map[uint64]leaseEntry)}
}

// Acquire takes a lease; *DBError{KindAlreadyOpen} as an error on conflict.
func (t *LeaseTable) Acquire(headerRecid uint64, kind LeaseKind) (*LeaseGuard, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.m[headerRecid]
	switch {
	case !ok:
		if kind == LeaseReadWrite {
			t.m[headerRecid] = leaseEntry{rw: true}
		} else {
			t.m[headerRecid] = leaseEntry{ro: 1}
		}
	case !e.rw && kind == LeaseReadOnly:
		e.ro++
		t.m[headerRecid] = e
	default:
		// RW-while-anything, RO-while-RW → conflict
		return nil, mapdb5.New(mapdb5.KindAlreadyOpen, "conflicting live handle")
	}
	return &LeaseGuard{table: t, recid: headerRecid, kind: kind}, nil
}

// LeaseGuard is a held lease; Release returns it. Not safe for concurrent
// use; call Release exactly once per Acquire.
type LeaseGuard struct {
	table    *LeaseTable
	recid    uint64
	kind     LeaseKind
	released bool
}

// Kind is the acquired access mode.
func (g *LeaseGuard) Kind() LeaseKind { return g.kind }

// HeaderRecid is the leased recid.
func (g *LeaseGuard) HeaderRecid() uint64 { return g.recid }

// Release returns the lease. Idempotent — a double Release is a no-op rather
// than a silent corruption of the count.
func (g *LeaseGuard) Release() {
	if g.released {
		return
	}
	g.released = true
	t := g.table
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.m[g.recid]
	if !ok {
		return
	}
	if !e.rw && e.ro > 1 {
		e.ro--
		t.m[g.recid] = e
		return
	}
	delete(t.m, g.recid)
}
