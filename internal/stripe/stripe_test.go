package stripe

import (
	"sync"
	"testing"
	"unsafe"
)

func TestPaddedIsOneCacheLine(t *testing.T) {
	if sz := unsafe.Sizeof(padded{}); sz != 64 {
		t.Fatalf("padded stripe is %d bytes, want 64", sz)
	}
}

// Writers exclude readers and each other; readers on many goroutines see
// a consistent pair.
func TestExclusion(t *testing.T) {
	var m RWMutex
	var a, b int
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 16; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.RLock()
				if a != b {
					m.RUnlock()
					t.Error("reader saw a torn write")
					return
				}
				m.RUnlock()
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		m.Lock()
		a++
		b++
		m.Unlock()
	}
	close(stop)
	wg.Wait()
	if a != 2000 || b != 2000 {
		t.Fatal("lost writes")
	}
}

func TestIndexStableAndSpread(t *testing.T) {
	if Index() != Index() {
		t.Fatal("index not stable on one goroutine")
	}
	seen := map[int]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			seen[Index()] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) < Stripes/4 {
		t.Fatalf("256 goroutines hit only %d stripes", len(seen))
	}
}
