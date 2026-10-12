package btree

// Regression test for the M7 HTree Astra review, finding 2
// (todo/reviews/m7_htree_review.md): an external-value Get must hold its
// operation admission across the value-record expansion, not just the
// tree descent — otherwise Close can retire the handle and let a
// reopened handle delete/reuse the recid mid-read.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// gatedGetStore blocks the first erased Get after arming — precisely the
// post-descent external value read (the descent itself uses Read).
type gatedGetStore struct {
	store.Store
	armed   atomic.Bool
	once    atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *gatedGetStore) Get(recid store.Recid, sr ser.AnySerializer) any {
	if g.armed.Load() && g.once.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
	}
	return g.Store.Get(recid, sr)
}

// LeaseTable forwards the lease capability (decorators must delegate).
func (g *gatedGetStore) LeaseTable() *store.LeaseTable {
	return g.Store.(store.StoreLease).LeaseTable()
}

func TestExternalGetHoldsAdmission(t *testing.T) {
	gs := &gatedGetStore{
		Store:   store.NewStoreOnHeap(true),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	cfg := cfgI64()
	cfg.MaxNodeSize = 4
	cfg.ExternalValues = true
	m := NewBTreeMap[int64, int64](gs, cfg)
	m.Put(1, 10)

	gs.armed.Store(true)
	getDone := make(chan struct{})
	go func() { defer close(getDone); m.Get(1) }()

	select {
	case <-gs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Get never reached the external value read")
	}

	// Close must drain: it cannot return while Get's admission is live.
	closeDone := make(chan struct{})
	go func() { defer close(closeDone); _ = m.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close returned while an admitted Get was still in flight")
	case <-time.After(300 * time.Millisecond):
	}
	close(gs.release)
	<-getDone
	<-closeDone
}
