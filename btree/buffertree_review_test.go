package btree

import (
	"iter"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/format"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// Regression tests for the M6b codex review (m6_buffertree_review.md).
// The hooked store pauses BETWEEN completed store operations, preserving
// the locked per-record read contract — the interleavings are legal.

type hookStore struct {
	*store.StoreByteArray
	beforeRead   func(uint64)
	afterGet     func(uint64)
	beforeUpdate func(uint64)
	afterRewrite func(uint64)
}

func (s *hookStore) Read(r uint64, a store.RecordRead) int64 {
	if s.beforeRead != nil {
		s.beforeRead(r)
	}
	return s.StoreByteArray.Read(r, a)
}

func (s *hookStore) Get(r uint64, a ser.AnySerializer) any {
	v := s.StoreByteArray.Get(r, a)
	if s.afterGet != nil {
		s.afterGet(r)
	}
	return v
}

func (s *hookStore) Update(r uint64, v any, a ser.AnySerializer) {
	if s.beforeUpdate != nil {
		s.beforeUpdate(r)
	}
	s.StoreByteArray.Update(r, v, a)
}

func (s *hookStore) UpdateWithHeadroom(r uint64, v any, a ser.AnySerializer, h int) {
	s.StoreByteArray.UpdateWithHeadroom(r, v, a, h)
	if s.afterRewrite != nil {
		s.afterRewrite(r)
	}
}

func hookTree(t *testing.T, s *hookStore, n int64) *BufferTreeMap[int64, int64] {
	t.Helper()
	m, err := CreateBufferTreeFromSortedFill(s, btCfg(4, 32, LeafHeadroomNone), 4, func(y func(int64, int64) bool) {
		for i := int64(1); i <= n; i++ {
			if !y(i, i*10) {
				return
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// F2: a fingerprint miss on the rewritten left half of a split must still
// move right to the sibling holding the key.
func TestBufferTreeReviewFingerprintSplit(t *testing.T) {
	s := &hookStore{StoreByteArray: store.NewStoreByteArray(true)}
	m := hookTree(t, s, 4)
	root := m.rootRecid()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	s.beforeRead = func(r uint64) {
		if r == root && once.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	}
	type result struct {
		v  int64
		ok bool
	}
	done := make(chan result)
	go func() { v, ok := m.Get(4); done <- result{v, ok} }()
	<-entered
	m.PutOnly(5, 50) // splits the root leaf
	close(resume)
	if got := <-done; !got.ok || got.v != 40 {
		t.Fatalf("Get(4) across a concurrent split = %d,%v; want 40,true", got.v, got.ok)
	}
}

// F1: a reader's delayed cache initialization must not overwrite the root
// a concurrent root-grow published.
func TestBufferTreeReviewRootCacheRace(t *testing.T) {
	s := &hookStore{StoreByteArray: store.NewStoreByteArray(true)}
	m := hookTree(t, s, 4)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	s.afterGet = func(r uint64) {
		if r == m.RootRecidRecid() && once.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	}
	done := make(chan struct{})
	go func() { m.Get(1); close(done) }()
	<-entered
	m.PutOnly(5, 50) // root-grow
	actual := m.loadRootRecid()
	close(resume)
	<-done
	if c := m.inner.cachedRoot.Load(); c != actual {
		t.Fatalf("cached root %d, published root %d", c, actual)
	}
	m.PutOnly(100, 1000)
	m.Close()
	re, err := OpenBufferTreeMap(s, m.RootRecidRecid(), btCfg(4, 32, LeafHeadroomNone))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := re.Get(100); !ok || v != 1000 {
		t.Fatalf("write lost after reopen: %d,%v", v, ok)
	}
	re.Verify()
}

// F3: Close must not abort an admitted writer mid structural update.
func TestBufferTreeReviewCloseDuringFlush(t *testing.T) {
	s := &hookStore{StoreByteArray: store.NewStoreByteArray(true)}
	cfg := btCfg(4, 128, LeafHeadroomNone)
	m, err := CreateBufferTreeFromSortedFill(s, cfg, 4, func(y func(int64, int64) bool) {
		for i := int64(1); i <= 8; i++ {
			if !y(i, i*10) {
				return
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	m.PutOnly(0, 0)
	m.PutOnly(9, 90)
	child := m.load(m.rootRecid()).children[0]
	entered, resume := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	s.afterRewrite = func(r uint64) {
		if r == child && once.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	}
	flushed := make(chan any)
	go func() {
		var p any
		func() { defer func() { p = recover() }(); m.FlushAll() }()
		flushed <- p
	}()
	<-entered
	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()
	for !m.IsClosed() {
		runtime.Gosched()
	}
	close(resume)
	if p := <-flushed; p != nil {
		t.Fatalf("admitted FlushAll aborted by Close: %v", p)
	}
	<-closed
	re, err := OpenBufferTreeMap(s, m.RootRecidRecid(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var keys []int64
	for k := range re.All() {
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("reopened keys %v", keys)
	}
	re.Verify()
}

// F4: a structural failure part-way through a split poisons the map.
func TestBufferTreeReviewStructuralFailurePoisons(t *testing.T) {
	s := &hookStore{StoreByteArray: store.NewStoreByteArray(true)}
	m := hookTree(t, s, 4)
	s.beforeUpdate = func(r uint64) {
		if r == m.RootRecidRecid() {
			panic(mapdb5.New(mapdb5.KindVolumeIO, "injected root publish failure"))
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("fault not exercised")
			}
		}()
		m.PutOnly(5, 50)
	}()
	for name, f := range map[string]func(){
		"get": func() { m.Get(4) },
		"put": func() { m.PutOnly(6, 60) },
		"all": func() {
			for range m.All() {
			}
		},
	} {
		func() {
			defer func() {
				de, ok := mapdb5.AsDBError(recover())
				if !ok || de.Kind != mapdb5.KindPoisoned {
					t.Fatalf("%s after interrupted split: want KindPoisoned, got %v", name, de)
				}
			}()
			f()
		}()
	}
}

// Port finding (beyond the review): the DFS iterator must yield every
// pre-existing entry exactly once even when a child it has not yet visited
// splits after its parent was snapshot (the split-off right part is absent
// from the stale parent's child list).
func TestBufferTreeIteratorAcrossConcurrentSplit(t *testing.T) {
	for _, desc := range []bool{false, true} {
		s := store.NewStoreByteArray(true)
		var orig []int64
		m, err := CreateBufferTreeFromSortedFill(s, btCfg(4, 64, LeafHeadroomNone), 4, func(y func(int64, int64) bool) {
			for i := int64(1); i <= 16; i++ {
				orig = append(orig, i*10)
				if !y(i*10, i) {
					return
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		seq := m.All()
		if desc {
			seq = m.DescendingMap()
		}
		next, stop := iter.Pull2(seq)
		k0, _, _ := next()
		seen := map[int64]int{k0: 1}
		// split every not-yet-visited leaf
		for k := int64(51); k < 160; k += 10 {
			for j := int64(0); j < 4; j++ {
				m.PutOnly(k+j, -1)
			}
		}
		m.FlushAll()
		prev := k0
		for {
			k, _, ok := next()
			if !ok {
				break
			}
			if (!desc && k <= prev) || (desc && k >= prev) {
				t.Fatalf("desc=%v: order broken %d after %d", desc, k, prev)
			}
			prev = k
			seen[k]++
		}
		stop()
		for _, k := range orig {
			if seen[k] != 1 {
				t.Fatalf("desc=%v: pre-existing key %d seen %d times", desc, k, seen[k])
			}
		}
		m.Verify()
	}
}

// F5: Rank / SelectKey / SelectEntry (spec 03 NavigableMap surface).
func TestBufferTreeRankSelect(t *testing.T) {
	m := NewBufferTreeMap(store.NewStoreByteArray(true), btCfg(4, 64, 0))
	for i := int64(0); i < 50; i++ {
		m.PutOnly(i*2, i)
	}
	if r := m.Rank(10); r != 5 {
		t.Fatalf("Rank(10)=%d", r)
	}
	if r := m.Rank(11); r != 6 {
		t.Fatalf("Rank(11)=%d", r)
	}
	if k, v, ok := m.SelectEntry(7); !ok || k != 14 || v != 7 {
		t.Fatalf("SelectEntry(7)=%d,%d,%v", k, v, ok)
	}
	if _, ok := m.SelectKey(50); ok {
		t.Fatal("SelectKey past end")
	}
	if _, ok := m.SelectKey(-1); ok {
		t.Fatal("SelectKey(-1)")
	}
}

var _ interface {
	Rank(int64) int
	SelectKey(int) (int64, bool)
	SelectEntry(int) (int64, int64, bool)
} = (*BufferTreeMap[int64, int64])(nil)

var _ = format.FpNormal

// ---- r2 (m6_buffertree_review_r2.md) ----

type reentrantAppendStore struct {
	*store.StoreByteArray
	onAppend func()
}

func (s *reentrantAppendStore) Append(r store.Recid, b []byte) int64 {
	if f := s.onAppend; f != nil {
		s.onAppend = nil
		f()
	}
	return s.StoreByteArray.Append(r, b)
}

// R2-N2: Close re-entering from inside an admitted write must not release
// the lease under the running writer (that would admit a second writer).
func TestBufferTreeReviewReentrantCloseRejected(t *testing.T) {
	s := &reentrantAppendStore{StoreByteArray: store.NewStoreByteArray(true)}
	m := NewBufferTreeMap(s, btCfg(4, 64, LeafHeadroomNone))
	s.onAppend = func() { m.Close() }
	func() {
		defer func() {
			de, ok := mapdb5.AsDBError(recover())
			if !ok || de.Kind != mapdb5.KindMisuse {
				t.Fatalf("re-entrant Close: want Misuse, got %v", de)
			}
		}()
		m.PutOnly(1, 10)
	}()
	if m.IsClosed() {
		t.Fatal("re-entrant Close retired the handle")
	}
	if _, err := OpenBufferTreeMap(s, m.RootRecidRecid(), btCfg(4, 64, LeafHeadroomNone)); err == nil {
		t.Fatal("lease released by a rejected re-entrant Close")
	}
	m.PutOnly(2, 20)
	m.Close()
	re, err := OpenBufferTreeMap(s, m.RootRecidRecid(), btCfg(4, 64, LeafHeadroomNone))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := re.Get(2); !ok || v != 20 {
		t.Fatal("write lost")
	}
}

// R2-N1: an iterator started while a child split is in flight (rewritten
// child, parent separators not yet published) must still yield every
// pre-existing key — it waits for the split to publish.
func TestBufferTreeReviewIteratorDuringChildSplit(t *testing.T) {
	s := &hookStore{StoreByteArray: store.NewStoreByteArray(true)}
	cfg := btCfg(4, 128, LeafHeadroomNone)
	m, err := CreateBufferTreeFromSortedFill(s, cfg, 4, func(y func(int64, int64) bool) {
		for i := int64(1); i <= 8; i++ {
			if !y(i, i*10) {
				return
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	m.PutOnly(0, 0)
	m.PutOnly(9, 90)
	child := m.load(m.rootRecid()).children[0]
	entered, resume := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	s.afterRewrite = func(r uint64) {
		if r == child && once.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	}
	done := make(chan any)
	go func() {
		var p any
		func() { defer func() { p = recover() }(); m.FlushAll() }()
		done <- p
	}()
	<-entered
	collect := func(seq iter.Seq2[int64, int64]) chan []int64 {
		c := make(chan []int64, 1)
		go func() {
			var ks []int64
			for k := range seq {
				ks = append(ks, k)
			}
			c <- ks
		}()
		return c
	}
	asc := collect(m.All())
	desc := collect(m.DescendingMap())
	bAsc := collect(m.RangeEntries(1, true, false, 6, true, true, false))
	bDesc := collect(m.RangeEntries(1, true, false, 6, true, true, true))
	for i := 0; i < 50; i++ { // let the iterators reach the in-flight split
		runtime.Gosched()
	}
	close(resume)
	if p := <-done; p != nil {
		t.Fatalf("FlushAll: %v", p)
	}
	want := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if got := <-asc; !slices.Equal(got, want) {
		t.Errorf("ascending %v", got)
	}
	slices.Reverse(want)
	if got := <-desc; !slices.Equal(got, want) {
		t.Errorf("descending %v", got)
	}
	if got := <-bAsc; !slices.Equal(got, []int64{2, 3, 4, 5, 6}) {
		t.Errorf("bounded ascending %v", got)
	}
	if got := <-bDesc; !slices.Equal(got, []int64{6, 5, 4, 3, 2}) {
		t.Errorf("bounded descending %v", got)
	}
}
