package store

import (
	"testing"
	"time"

	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// reentrantSer is ser.String whose Deserialize runs a hook first.
type reentrantSer struct {
	ser.AnySerializer
	hook func()
}

func (h reentrantSer) DeserializeAny(r *bin.Reader, size int) any {
	h.hook()
	return h.AnySerializer.DeserializeAny(r, size)
}

// Regression (fable release review #6): a deserializer re-entering
// StoreDirect.Get while a Commit is queued deadlocked on the plain RWMutex
// (Java's ReentrantReadWriteLock admits the nested read). Get now decodes
// after releasing every store lock.
func TestDirectReentrantGetWithQueuedCommit(t *testing.T) {
	s := NewStoreDirectHeap(true)
	r1 := Put(s, "one", ser.String)
	r2 := Put(s, "two", ser.String)
	inDeser := make(chan struct{})
	proceed := make(chan struct{})
	var nested any
	first := true
	reent := reentrantSer{AnySerializer: ser.String, hook: func() {
		if !first {
			return
		}
		first = false
		close(inDeser)
		<-proceed
		nested = s.Get(r2, ser.String)
	}}
	getDone := make(chan any, 1)
	go func() { getDone <- s.Get(r1, reent) }()
	<-inDeser
	commitDone := make(chan struct{})
	go func() { s.Commit(); close(commitDone) }()
	time.Sleep(50 * time.Millisecond)
	close(proceed)
	select {
	case v := <-getDone:
		if v != "one" || nested != "two" {
			t.Fatalf("Get=%v nested=%v", v, nested)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("re-entrant Get deadlocked behind a queued Commit")
	}
	<-commitDone
}

// Regression (fable release review #5): Append of zero bytes is a no-op
// returning the current length for every record shape (Java appendInner);
// StoreDirect used to establish a live empty record on a null slot,
// diverging from StoreWAL.
func TestAppendEmptyIsNoOpOnEveryStore(t *testing.T) {
	try := func(name string, s StoreDelta, commit func()) {
		p := s.Preallocate()
		n := s.Put(nil, ser.String)
		v := Put(s, "abc", ser.String)
		commit()
		if got := s.Append(p, nil); got != 0 {
			t.Errorf("%s: Append(prealloc, nil)=%d, want 0", name, got)
		}
		if got := s.Append(n, []byte{}); got != 0 {
			t.Errorf("%s: Append(null, [])=%d, want 0", name, got)
		}
		if got := s.Append(v, nil); got != 4 {
			t.Errorf("%s: Append(live, nil)=%d, want 4", name, got)
		}
		commit()
		if g := s.Get(p, ser.String); g != nil {
			t.Errorf("%s: prealloc became %v", name, g)
		}
		if g := s.Get(n, ser.String); g != nil {
			t.Errorf("%s: null became %v", name, g)
		}
		if g := s.Get(v, ser.String); g != "abc" {
			t.Errorf("%s: live became %v", name, g)
		}
	}
	d := NewStoreDirectHeap(true)
	try("direct", d, d.Commit)
	w, err := OpenStoreWAL(walBase(t), true)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	try("wal", w, w.Commit)
}

// Regression (fable release review #7): UpdateWithHeadroom(nil) stores null
// on every store (Java accepts null); StoreDirect used to panic Misuse.
func TestUpdateWithHeadroomNilStoresNull(t *testing.T) {
	for name, s := range map[string]StoreDelta{
		"direct":    NewStoreDirectHeap(true),
		"bytearray": NewStoreByteArray(true),
	} {
		r := Put(s, "v", ser.String)
		s.UpdateWithHeadroom(r, nil, ser.String, 16)
		if g := s.Get(r, ser.String); g != nil {
			t.Errorf("%s: Get=%v, want nil", name, g)
		}
	}
}

// Regression (fable release review #8): Rollback on a read-only StoreWAL is
// a no-op (Java rollback has no checkWritable).
func TestWALReadOnlyRollbackIsNoOp(t *testing.T) {
	base := walBase(t)
	s, err := OpenStoreWAL(base, false)
	if err != nil {
		t.Fatal(err)
	}
	r := Put(s, "x", ser.String)
	s.Commit()
	s.Close()
	ro, err := OpenStoreWALRO(base, false)
	if err != nil {
		t.Fatal(err)
	}
	ro.Rollback()
	if g := ro.Get(r, ser.String); g != "x" {
		t.Fatalf("Get after RO rollback = %v", g)
	}
	ro.Close()
}
