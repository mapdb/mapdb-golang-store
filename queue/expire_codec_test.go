package queue

import (
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
	"testing"
)

type r2PointerSize struct{ store.RecordReadBase }

func (r2PointerSize) OnBytes(_ *bin.Reader, n int) int64 { return int64(n) }

func TestQueueReviewR2PointerCodecEveryOperation(t *testing.T) {
	for _, expire := range []bool{false, true} {
		name := "standalone_packed"
		if expire {
			name = "expire_fixed"
		}
		t.Run(name, func(t *testing.T) {
			s := store.NewStoreByteArray(true)
			// Ensure pointers reach multi-byte packed-long territory too.
			for i := 0; i < 150; i++ {
				store.Put(s, int64(i), ser.Long)
			}
			maker, opener, codec := NewQueueLong, OpenQueueLong, ser.LongPacked
			if expire {
				maker, opener, codec = NewExpireQueue, OpenExpireQueue, ser.Long
			}
			q, err := maker(s)
			if err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				q.Verify()
				for r, want := range map[uint64]uint64{q.TailRecid(): q.Tail(), q.HeadRecid(): q.Head(), q.HeadPrevRecid(): q.HeadPrev()} {
					got, ok := store.Get(s, r, codec)
					if !ok || uint64(got) != want {
						t.Fatalf("ptr %d decoded %d want %d", r, got, want)
					}
					size := s.Read(r, r2PointerSize{})
					expected := len(ser.SerializeToBytes(codec, int64(want)))
					if size != int64(expected) {
						t.Fatalf("ptr %d width=%d want %d", r, size, expected)
					}
				}
				var err error
				q, err = opener(s, q.TailRecid(), q.HeadRecid(), q.HeadPrevRecid())
				if err != nil {
					t.Fatal(err)
				}
			}
			check()
			a := q.Put(1, 1)
			check()
			b := q.Put(2, 2)
			check()
			c := q.Put(3, 3)
			check()
			q.Bump(a, 4)
			check()
			q.Bump(a, 5)
			check()
			q.Remove(c, true)
			check()
			q.Remove(b, true)
			check()
			q.Take()
			check() // single-element Take exercises headPrev CAS success
			q.Take()
			check() // empty Take writes headPrev through codec
			d := s.Preallocate()
			q.PutPreallocated(6, 6, d)
			check()
			q.Put(7, 7)
			check()
			q.Take()
			check() // non-final Take exercises CAS mismatch
			q.Clear()
			check()
			e := q.Put(8, 8)
			check()
			q.Remove(e, true)
			check()
			q.Put(9, 9)
			q.Put(10, 10)
			q.TakeUntil(func(_ store.Recid, n LNode) bool { return n.Timestamp < 10 })
			check()
			q.Clear()
			check()
		})
	}
}
