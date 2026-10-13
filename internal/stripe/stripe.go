// Package stripe provides a reader-scalable RWMutex: the read side is
// spread over cache-line-padded stripes chosen by goroutine identity, so
// concurrent readers stop contending on one shared reader count (Java
// SegmentLocks' padded-holder fix, applied to a lock every read takes).
// Writers take every stripe, in order. Semantics are sync.RWMutex's: no
// recursive read locking, no upgrade.
package stripe

import (
	"sync"
	"unsafe"

	"github.com/mapdb/mapdb5-go/internal/handle"
)

// Stripes is the number of reader stripes (a power of two).
const Stripes = 64

type padded struct {
	mu sync.RWMutex
	_  [64 - unsafe.Sizeof(sync.RWMutex{})%64]byte // each on its own 64-byte line
}

// RWMutex is a striped reader/writer lock. The zero value is unlocked.
type RWMutex struct {
	s [Stripes]padded
}

// Index is the calling goroutine's stripe. A goroutine's identity is
// stable while it runs, so RLock and RUnlock on the same goroutine always
// pick the same stripe.
func Index() int {
	g := handle.Goid()
	// g structs are allocated a few hundred bytes apart; a Fibonacci hash
	// of the address spreads neighbours across stripes.
	return int((g * 0x9E3779B97F4A7C15) >> (64 - 6))
}

// RLock locks the caller's stripe for reading.
func (m *RWMutex) RLock() { m.s[Index()].mu.RLock() }

// RUnlock undoes RLock; it must run on the goroutine that called RLock.
func (m *RWMutex) RUnlock() { m.s[Index()].mu.RUnlock() }

// Lock takes every stripe for writing (stripe order, so two writers cannot
// deadlock).
func (m *RWMutex) Lock() {
	for i := range m.s {
		m.s[i].mu.Lock()
	}
}

// Unlock releases every stripe.
func (m *RWMutex) Unlock() {
	for i := len(m.s) - 1; i >= 0; i-- {
		m.s[i].mu.Unlock()
	}
}

// PaddedRWMutex is a sync.RWMutex alone on its 64-byte cache line, for
// arrays of independent locks (StoreDirect segment locks) that would
// otherwise false-share.
type PaddedRWMutex struct {
	sync.RWMutex
	_ [64 - unsafe.Sizeof(sync.RWMutex{})%64]byte
}
