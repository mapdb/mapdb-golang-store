package indextree

import (
	"iter"
	"sync"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/htree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// listHeader is the persisted list metadata (Java IndexTreeList.Header:
// packInt dirShift, packInt levels, packLong rootRecid, packLong
// counterRecid — no defaultValue field).
type listHeader struct {
	dirShift, levels        int32
	rootRecid, counterRecid uint64
}

type listHeaderSer struct{}

var listHeaderSerializer = ser.NewAnyOf[listHeader](listHeaderSer{})

func (listHeaderSer) Serialize(w *bin.Writer, h listHeader) {
	w.PackInt(h.dirShift)
	w.PackInt(h.levels)
	w.PackLong(int64(h.rootRecid))
	w.PackLong(int64(h.counterRecid))
}

func (listHeaderSer) Deserialize(r *bin.Reader, size int) listHeader {
	h := listHeader{
		dirShift:     r.UnpackInt(),
		levels:       r.UnpackInt(),
		rootRecid:    uint64(r.UnpackLong()),
		counterRecid: uint64(r.UnpackLong()),
	}
	if h.dirShift <= 0 || h.dirShift > htree.MaxDirShift || h.levels <= 0 ||
		h.rootRecid == 0 || h.counterRecid == 0 {
		panic(mapdb5.NewCorruption("indextree list header out of range"))
	}
	return h
}

func (listHeaderSer) FixedSize() int                { return -1 }
func (listHeaderSer) SizeHint() int                 { return 26 }
func (listHeaderSer) Compare(a, b listHeader) int   { panic(misuseInternal()) }
func (listHeaderSer) Equals(a, b listHeader) bool   { panic(misuseInternal()) }
func (listHeaderSer) NaturalOrder() bool            { return false }
func (listHeaderSer) EqualsBySerializedBytes() bool { return true }

// IndexTreeList is a persistent positional list over DirTree (Java
// IndexTreeList): position 0..Len()-1 maps through the tree to the
// element record's recid. Positional insert/remove shift the tail of the
// index→recid mapping so positions stay contiguous — element records
// are untouched; a removed element's record is freed.
//
// A single RWMutex guards the list (no-op when the store is not
// thread-safe). The zero value is unusable; use New/Open.
type IndexTreeList[T any] struct {
	st           store.Store
	elemSer      ser.Serializer[T]
	headerRecid  store.Recid
	rootRecid    uint64
	counterRecid uint64
	threadSafe   bool
	lock         sync.RWMutex
}

// NewIndexTreeList creates a fresh empty list.
func NewIndexTreeList[T any](s store.Store, elemSer ser.Serializer[T]) *IndexTreeList[T] {
	rootRecid := store.Put(s, htree.DirEmpty(), htree.DirSer)
	counterRecid := store.Put(s, int64(0), ser.Long)
	hdr := listHeader{
		dirShift: DirShift, levels: Levels,
		rootRecid: rootRecid, counterRecid: counterRecid,
	}
	headerRecid := store.Put(s, hdr, listHeaderSerializer)
	return &IndexTreeList[T]{
		st: s, elemSer: elemSer, headerRecid: headerRecid,
		rootRecid: rootRecid, counterRecid: counterRecid,
		threadSafe: s.IsThreadSafe(),
	}
}

// OpenIndexTreeList reattaches a list persisted under headerRecid; the
// same element serializer must be supplied. Corrupt or wrong-recid
// headers surface as errors, not panics.
func OpenIndexTreeList[T any](s store.Store, headerRecid store.Recid,
	elemSer ser.Serializer[T]) (l *IndexTreeList[T], err error) {
	defer dbErrorToErr(&err)
	if headerRecid == 0 {
		return nil, mapdb5.NewCorruption("indextree list header recid must be a valid recid")
	}
	hdr, ok := store.Get(s, uint64(headerRecid), listHeaderSerializer)
	if !ok {
		return nil, mapdb5.NewCorruption("indextree list header record is null")
	}
	if int(hdr.dirShift) != DirShift || int(hdr.levels) != Levels {
		return nil, mapdb5.NewCorruption("indextree list header geometry mismatch")
	}
	return &IndexTreeList[T]{
		st: s, elemSer: elemSer, headerRecid: headerRecid,
		rootRecid: hdr.rootRecid, counterRecid: hdr.counterRecid,
		threadSafe: s.IsThreadSafe(),
	}, nil
}

// HeaderRecid is the recid of the header record; persist to reopen.
func (l *IndexTreeList[T]) HeaderRecid() store.Recid { return l.headerRecid }

func (l *IndexTreeList[T]) lockRead() {
	if l.threadSafe {
		l.lock.RLock()
	}
}
func (l *IndexTreeList[T]) unlockRead() {
	if l.threadSafe {
		l.lock.RUnlock()
	}
}
func (l *IndexTreeList[T]) lockWrite() {
	if l.threadSafe {
		l.lock.Lock()
	}
}
func (l *IndexTreeList[T]) unlockWrite() {
	if l.threadSafe {
		l.lock.Unlock()
	}
}

func (l *IndexTreeList[T]) sizeRaw() int {
	n, _ := store.Get(l.st, l.counterRecid, ser.Long)
	return int(n)
}

func (l *IndexTreeList[T]) setSize(size int) {
	store.Update(l.st, l.counterRecid, int64(size), ser.Long)
}

func (l *IndexTreeList[T]) checkElementIndex(index, size int) {
	if index < 0 || index >= size {
		panic(mapdb5.NewMisuse("index out of bounds"))
	}
}

func (l *IndexTreeList[T]) checkPositionIndex(index, size int) {
	if index < 0 || index > size {
		panic(mapdb5.NewMisuse("index out of bounds"))
	}
}

func (l *IndexTreeList[T]) treeGet(index int64) int64 {
	return htree.TreeGet(DirShift, l.rootRecid, l.st, Levels-1, index)
}

func (l *IndexTreeList[T]) treePut(index, recid int64) {
	htree.TreePut(DirShift, l.rootRecid, l.st, Levels-1, index, recid)
}

func (l *IndexTreeList[T]) treeRemove(index int64) {
	htree.TreeRemove(DirShift, l.rootRecid, l.st, Levels-1, index)
}

// ===================== reads =====================

// Len is the element count (O(1) counter record).
func (l *IndexTreeList[T]) Len() int {
	l.lockRead()
	defer l.unlockRead()
	return l.sizeRaw()
}

// IsEmpty reports whether the list has no elements.
func (l *IndexTreeList[T]) IsEmpty() bool { return l.Len() == 0 }

// Get returns the element at index; out-of-range index panics Misuse.
// A missing record (sparse slot inside a dense list) returns the zero
// value — Java returns null.
func (l *IndexTreeList[T]) Get(index int) T {
	l.lockRead()
	defer l.unlockRead()
	l.checkElementIndex(index, l.sizeRaw())
	recid := l.treeGet(int64(index))
	var zero T
	if recid == 0 {
		return zero
	}
	v, _ := store.Get(l.st, uint64(recid), l.elemSer)
	return v
}

// IndexOf returns the first position whose element Equals value, or -1.
// Runs under a single read lock over the whole list.
func (l *IndexTreeList[T]) IndexOf(value T) int {
	l.lockRead()
	defer l.unlockRead()
	size := l.sizeRaw()
	for i := 0; i < size; i++ {
		recid := l.treeGet(int64(i))
		if recid == 0 {
			continue
		}
		v, _ := store.Get(l.st, uint64(recid), l.elemSer)
		if l.elemSer.Equals(v, value) {
			return i
		}
	}
	return -1
}

// Contains reports whether any element Equals value.
func (l *IndexTreeList[T]) Contains(value T) bool { return l.IndexOf(value) >= 0 }

// ToSlice materializes the list in positional order.
func (l *IndexTreeList[T]) ToSlice() []T {
	l.lockRead()
	defer l.unlockRead()
	size := l.sizeRaw()
	out := make([]T, 0, size)
	for i := 0; i < size; i++ {
		recid := l.treeGet(int64(i))
		var v T
		if recid != 0 {
			v, _ = store.Get(l.st, uint64(recid), l.elemSer)
		}
		out = append(out, v)
	}
	return out
}

// All iterates elements in positional order; each position resolves
// under the read lock, released before each yield, so concurrent
// structural mutation (including from the loop body) is safe but weakly
// consistent (a shifted element may be re-yielded or skipped).
func (l *IndexTreeList[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := 0; ; i++ {
			v, ok := l.elementAt(i)
			if !ok || !yield(v) {
				return
			}
		}
	}
}

// elementAt reads position i under the read lock, released before the
// caller yields (no lock is ever held across user code). ok=false past
// the current end.
func (l *IndexTreeList[T]) elementAt(i int) (v T, ok bool) {
	l.lockRead()
	defer l.unlockRead()
	if i >= l.sizeRaw() {
		return v, false
	}
	if recid := l.treeGet(int64(i)); recid != 0 {
		v, _ = store.Get(l.st, uint64(recid), l.elemSer)
	}
	return v, true
}

// ForEach visits elements in positional order.
func (l *IndexTreeList[T]) ForEach(f func(T)) {
	l.All()(func(v T) bool { f(v); return true })
}

// AnySatisfy reports whether any element satisfies predicate.
func (l *IndexTreeList[T]) AnySatisfy(p func(T) bool) bool {
	for v := range l.All() {
		if p(v) {
			return true
		}
	}
	return false
}

// AllSatisfy reports whether every element satisfies predicate.
func (l *IndexTreeList[T]) AllSatisfy(p func(T) bool) bool {
	for v := range l.All() {
		if !p(v) {
			return false
		}
	}
	return true
}

// NoneSatisfy reports whether no element satisfies predicate.
func (l *IndexTreeList[T]) NoneSatisfy(p func(T) bool) bool {
	return l.AllSatisfy(func(v T) bool { return !p(v) })
}

// ===================== writes =====================

// Add appends element at the end.
func (l *IndexTreeList[T]) Add(element T) {
	l.lockWrite()
	defer l.unlockWrite()
	index := l.sizeRaw()
	recid := store.Put(l.st, element, l.elemSer)
	l.treePut(int64(index), int64(recid))
	l.setSize(index + 1)
}

// AddAt inserts element at index, shifting the tail up by re-keying
// slots (element records untouched). store.Put runs first: it is the
// only fallible step, so a serializer failure leaves the list dense
// (Java add(int,E)).
func (l *IndexTreeList[T]) AddAt(index int, element T) {
	l.lockWrite()
	defer l.unlockWrite()
	size := l.sizeRaw()
	l.checkPositionIndex(index, size)
	newRecid := int64(store.Put(l.st, element, l.elemSer))
	for i := size - 1; i >= index; i-- {
		recid := l.treeGet(int64(i))
		if recid == 0 {
			panic(mapdb5.NewCorruption("dense list invariant broken"))
		}
		l.treeRemove(int64(i))
		l.treePut(int64(i+1), recid)
	}
	l.treePut(int64(index), newRecid)
	l.setSize(size + 1)
}

// Set replaces the element at index, returning the old value. A sparse
// gap inside the list bounds is populated (Java set).
func (l *IndexTreeList[T]) Set(index int, element T) T {
	l.lockWrite()
	defer l.unlockWrite()
	l.checkElementIndex(index, l.sizeRaw())
	recid := l.treeGet(int64(index))
	var old T
	if recid == 0 {
		l.treePut(int64(index), int64(store.Put(l.st, element, l.elemSer)))
		return old
	}
	old, _ = store.Get(l.st, uint64(recid), l.elemSer)
	store.Update(l.st, uint64(recid), element, l.elemSer)
	return old
}

// RemoveAt deletes the element at index, freeing its record and shifting
// the tail down; returns the removed value.
func (l *IndexTreeList[T]) RemoveAt(index int) T {
	l.lockWrite()
	defer l.unlockWrite()
	size := l.sizeRaw()
	l.checkElementIndex(index, size)
	recid := l.treeGet(int64(index))
	var ret T
	if recid != 0 {
		ret, _ = store.Get(l.st, uint64(recid), l.elemSer)
		store.Delete(l.st, uint64(recid), l.elemSer)
		l.treeRemove(int64(index))
	}
	for i := index + 1; i < size; i++ {
		r := l.treeGet(int64(i))
		if r == 0 {
			panic(mapdb5.NewCorruption("dense list invariant broken"))
		}
		l.treeRemove(int64(i))
		l.treePut(int64(i-1), r)
	}
	l.setSize(size - 1)
	return ret
}

// Clear removes every element, freeing all element records.
func (l *IndexTreeList[T]) Clear() {
	l.lockWrite()
	defer l.unlockWrite()
	htree.TreeClear(l.rootRecid, l.st, Levels-1,
		func(index, recid int64) {
			store.Delete(l.st, uint64(recid), l.elemSer)
		})
	l.setSize(0)
}
