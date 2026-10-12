package indextree

import (
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
	"iter"
	"testing"
)

func TestReleaseIteratorYieldsUnlocked(t *testing.T) {
	t.Run("list", func(t *testing.T) {
		s := store.NewStoreOnHeap(true)
		defer s.Close()
		l := NewIndexTreeList(s, ser.Long)
		l.Add(1)
		next, stop := iter.Pull(l.All())
		defer stop()
		next()
		if !l.lock.TryLock() {
			t.Fatal("All holds read lock across yield; Add/Set/Clear from consumer will deadlock")
		}
		l.lock.Unlock()
	})
	t.Run("map", func(t *testing.T) {
		s := store.NewStoreOnHeap(true)
		defer s.Close()
		m := NewIndexTreeLongLongMap(s)
		m.Put(1, 1)
		next, stop := iter.Pull2(m.All())
		defer stop()
		next()
		if !m.lock.TryLock() {
			t.Fatal("All holds read lock across yield; Put/Remove/Clear from consumer will deadlock")
		}
		m.lock.Unlock()
	})
}
