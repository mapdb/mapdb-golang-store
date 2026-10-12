package db

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/indextree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
	"path/filepath"
	"testing"
	"time"
)

func releasePanic(f func()) (p any) { defer func() { p = recover() }(); f(); return }

func TestReleaseDeleteReadOnlyUnlock(t *testing.T) {
	d, err := MemoryDB().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err = d.AtomicLongInit("x", 1).Create(); err != nil {
		t.Fatal(err)
	}
	ro, err := NewDB(store.NewReadOnly(d.Store()))
	if err != nil {
		t.Fatal(err)
	}
	p := releasePanic(func() { ro.Delete("x") })
	de, ok := mapdb5.AsDBError(p)
	if !ok || de.Kind != mapdb5.KindReadOnly {
		t.Fatalf("panic %v", p)
	}
	if !ro.adminMu.TryLock() {
		t.Fatal("Delete(ReadOnly) recovered, but adminMu remains locked: Exists/Close will hang")
	}
	ro.adminMu.Unlock()
}

func TestReleaseIndexTreeCatalog(t *testing.T) {
	s := store.NewStoreByteArray(true)
	defer s.Close()
	d, err := NewDB(s)
	if err != nil {
		t.Fatal(err)
	}
	m := indextree.NewIndexTreeLongLongMap(s)
	m.Put(1, 42)
	cat := d.GetNameCatalog()
	cat["idx#type"] = "IndexTreeLongLongMap"
	cat["idx#headerRecid"] = itoa(m.HeaderRecid())
	store.Update(s, RecidCatalog, cat, catalogSer{})
	_, err = NewDB(s)
	if err != nil {
		t.Fatalf("valid Java IndexTreeLongLongMap catalog rejected: %v", err)
	}
}

func TestReleaseBufferTreeReadOnlyReopen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "db")
	d, err := FileDB(p).Make()
	if err != nil {
		t.Fatal(err)
	}
	m, err := BufferTreeMap(d, "b", ser.LongFormat, ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(1, 42)
	d.Commit()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := FileDB(p).ReadOnly().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	got, err := BufferTreeMap(ro, "b", ser.LongFormat, ser.LongFormat).Open()
	if err != nil {
		t.Fatalf("read-only reopen failed: %v", err)
	}
	if v, ok := got.Get(1); !ok || v != 42 {
		t.Fatalf("got %v,%v", v, ok)
	}
}

// A second Rollback overlapping the first one's drain window must not
// clear its draining state and rewind early: it must fail fast, and after
// the first completes, new handles get distinct records.
func TestReleaseConcurrentRollbackDrain(t *testing.T) {
	d, err := FileDB(filepath.Join(t.TempDir(), "db")).TransactionEnable().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	m, err := TreeMap[int64, int64](d, "m", ser.LongFormat, ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(1, 1)
	d.Commit()
	entered, release := make(chan struct{}), make(chan struct{})
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error { close(entered); <-release; return nil }))
	putDone := make(chan any, 1)
	go func() { putDone <- releasePanic(func() { m.Put(1, 99) }) }()
	<-entered
	rb1 := make(chan any, 1)
	go func() { rb1 <- releasePanic(d.Rollback) }()
	deadline := time.Now().Add(2 * time.Second)
	for !m.IsClosed() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !m.IsClosed() {
		close(release)
		t.Fatal("first rollback did not begin retirement")
	}
	p2 := releasePanic(d.Rollback)
	close(release)
	<-putDone
	if p := <-rb1; p != nil {
		t.Fatalf("first rollback failed: %v", p)
	}
	if de, ok := mapdb5.AsDBError(p2); !ok || de.Kind != mapdb5.KindWrongConfiguration {
		t.Fatalf("overlapping Rollback: want KindWrongConfiguration panic, got %v", p2)
	}
	a, err := d.AtomicLongInit("a", 10).Create()
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.AtomicLongInit("b", 20).Create()
	if err != nil {
		t.Fatal(err)
	}
	if a.Recid() == b.Recid() {
		t.Fatalf("atomics share recid %d after rollback", a.Recid())
	}
	a.Set(99)
	if b.Get() != 20 {
		t.Fatalf("b=%d, want 20", b.Get())
	}
}
