package store

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/mapdb/mapdb5-go/ser"
)

// Put grows the file tail while Get validates extents against it; both on
// heap and file volumes (lazy page loads race readers on a reopened file).
func TestDirectPutGetConcurrentNoRace(t *testing.T) {
	for _, file := range []bool{false, true} {
		var s *StoreDirect
		path := filepath.Join(t.TempDir(), "d.db")
		if file {
			w, err := NewStoreDirectFile(path, true)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3000; i++ {
				Put(w, make([]byte, 1000), ser.ByteArray)
			}
			w.Commit()
			w.Close()
			s, err = NewStoreDirectFile(path, true) // pages now load lazily
			if err != nil {
				t.Fatal(err)
			}
		} else {
			s = NewStoreDirectHeap(true)
		}
		seed := Put(s, []byte("seed"), ser.ByteArray)
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if _, ok := Get(s, seed, ser.ByteArray); !ok {
						t.Error("seed vanished")
						return
					}
					if file {
						Get(s, Recid(2+(i*7+r)%2999), ser.ByteArray)
					}
				}
			}(r)
		}
		for i := 0; i < 3000; i++ {
			Put(s, make([]byte, 2000), ser.ByteArray)
		}
		close(stop)
		wg.Wait()
		s.Verify()
		s.Close()
	}
}
