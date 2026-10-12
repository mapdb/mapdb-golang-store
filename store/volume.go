package store

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

// Paged volume: 1 MiB slices, heap or file-backed (spec 02 §3). Records never
// cross a slice boundary (allocator invariant). Go needs no unsafe: []byte
// slices are GC-managed views, and the store's lock discipline serializes
// access (a range is written only under its record's segment write lock or
// the structural lock, read only under the matching read lock).
//
// File backing is a lazy page cache, not mmap: each page is ReadAt on first
// touch and flushed on Sync via WriteAt. Dirty flags are only meaningful
// under the exclusive commit barrier (all mutators hold the shared side), so
// a Sync scan never races a writer.

const (
	sliceShift = 20
	sliceSize  = int64(1) << sliceShift // 1 MiB
	sliceMask  = sliceSize - 1
)

// volumePage is one 1 MiB slice. data is allocated once and never moves.
type volumePage struct {
	data  []byte
	dirty atomic.Bool
}

type volume struct {
	pages  atomic.Pointer[[]*volumePage]
	file   *os.File // nil = anonymous heap volume
	growMu sync.Mutex
}

func newHeapVolume() *volume {
	v := &volume{}
	v.pages.Store(&[]*volumePage{})
	return v
}

func openFileVolume(path string) (*volume, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, mapdb5.NewIO(err)
	}
	v := newHeapVolume()
	v.file = f
	return v, nil
}

func (v *volume) isFileBacked() bool { return v.file != nil }

// length is the physical file length (file mode) or the addressable mapped
// length (memory mode).
func (v *volume) length() int64 {
	if v.file == nil {
		return int64(len(*v.pages.Load())) * sliceSize
	}
	st, err := v.file.Stat()
	if err != nil {
		panic(mapdb5.NewIO(err))
	}
	return st.Size()
}

// slice returns the page covering offset; nil entries are filled on demand
// (file pages are ReadAt once, heap pages allocated zeroed). Offsets past the
// grown table panic the index check — callers pre-validate via checkRange.
func (v *volume) slice(offset int64) *volumePage {
	pages := *v.pages.Load()
	p := pages[offset>>sliceShift]
	if p == nil {
		p = v.loadPage(offset >> sliceShift)
	}
	return p
}

func (v *volume) loadPage(idx int64) *volumePage {
	v.growMu.Lock()
	defer v.growMu.Unlock()
	pages := *v.pages.Load()
	if p := pages[idx]; p != nil {
		return p
	}
	data := make([]byte, sliceSize)
	if v.file != nil {
		n, err := v.file.ReadAt(data, idx*sliceSize)
		if err != nil && err != io.EOF {
			panic(mapdb5.NewIO(err))
		}
		_ = n // short read at EOF → tail stays zero
	}
	p := &volumePage{data: data}
	pages[idx] = p
	return p
}

// view returns a writable view of [offset, offset+len) inside ONE slice. The
// bounds check is unconditional: offsets derived from corrupt index values
// must yield DataCorruption, never a bare runtime panic (D4/D5). Callers use
// the view only under the appropriate lock.
func (v *volume) view(offset int64, size int64) []byte {
	if size < 0 || offset < 0 || (offset&sliceMask)+size > sliceSize {
		panic(mapdb5.NewCorruption("volume access out of slice bounds"))
	}
	return v.slice(offset).data[offset&sliceMask : (offset&sliceMask)+size]
}

// checkRange validates that [offset, offset+size) is addressable and lies in
// one slice. Every offset derived from a (possibly corrupt) index/link value
// goes through this before raw access (D4).
func (v *volume) checkRange(offset, size int64) {
	if size < 0 || offset < 0 {
		panic(mapdb5.NewCorruption("volume access with negative bounds"))
	}
	if offset > int64(1<<62)-size { // overflow guard before the add
		panic(mapdb5.NewCorruption("volume offset arithmetic overflow"))
	}
	if offset+size > int64(len(*v.pages.Load()))*sliceSize {
		panic(mapdb5.NewCorruption("volume offset past addressable end"))
	}
	if (offset&sliceMask)+size > sliceSize {
		panic(mapdb5.NewCorruption("volume access crosses a slice boundary"))
	}
}

// ensureAvailable grows so [0, end) is addressable. File mode extends the
// (sparse) file; pages themselves load lazily.
func (v *volume) ensureAvailable(end int64) {
	needed := int((end + sliceMask) >> sliceShift)
	if v.file != nil {
		want := int64(needed) * sliceSize
		if n := v.length(); n < want {
			if err := v.file.Truncate(want); err != nil {
				panic(mapdb5.NewIO(err))
			}
		}
	}
	v.growMu.Lock()
	defer v.growMu.Unlock()
	cur := *v.pages.Load()
	if len(cur) >= needed {
		return
	}
	grown := make([]*volumePage, needed)
	copy(grown, cur)
	v.pages.Store(&grown)
}

// ---- scalar accessors (big-endian, per the on-volume format) ----

func (v *volume) putI32(offset int64, x int32) {
	binary.BigEndian.PutUint32(v.view(offset, 4), uint32(x))
	v.slice(offset).dirty.Store(true)
}
func (v *volume) getI32(offset int64) int32 {
	return int32(binary.BigEndian.Uint32(v.view(offset, 4)))
}
func (v *volume) putU64(offset int64, x uint64) {
	binary.BigEndian.PutUint64(v.view(offset, 8), x)
	v.slice(offset).dirty.Store(true)
}
func (v *volume) getU64(offset int64) uint64 {
	return binary.BigEndian.Uint64(v.view(offset, 8))
}
func (v *volume) putByte(offset int64, x byte) {
	v.view(offset, 1)[0] = x
	v.slice(offset).dirty.Store(true)
}
func (v *volume) getU8(offset int64) byte {
	return v.view(offset, 1)[0]
}
func (v *volume) putData(offset int64, src []byte) {
	copy(v.view(offset, int64(len(src))), src)
	v.slice(offset).dirty.Store(true)
}
func (v *volume) getData(offset int64, dst []byte) {
	copy(dst, v.view(offset, int64(len(dst))))
}

// clear zeroes [from, to); may span slices.
func (v *volume) clear(from, to int64) {
	for p := from; p < to; {
		n := min(to-p, sliceSize-(p&sliceMask))
		clear(v.view(p, n))
		v.slice(p).dirty.Store(true)
		p += n
	}
}

// ---- durability ----

// flushPage writes one dirty page (file mode) and clears the flag.
func (v *volume) flushPage(idx int64, p *volumePage) {
	if v.file == nil || !p.dirty.Load() {
		return
	}
	if _, err := v.file.WriteAt(p.data, idx*sliceSize); err != nil {
		panic(mapdb5.NewIO(err))
	}
	p.dirty.Store(false)
}

// sync flushes every dirty page, then fsyncs. No-op in memory mode. Runs
// under the exclusive commit barrier — no mutator is active.
func (v *volume) sync() {
	if v.file == nil {
		return
	}
	for i, p := range *v.pages.Load() {
		if p != nil {
			v.flushPage(int64(i), p)
		}
	}
	if err := v.file.Sync(); err != nil {
		panic(mapdb5.NewIO(err))
	}
}

// syncHeader is the second commit phase: header page only, then fsync.
func (v *volume) syncHeader() {
	if v.file == nil {
		return
	}
	if pages := *v.pages.Load(); len(pages) > 0 && pages[0] != nil {
		v.flushPage(0, pages[0])
	}
	if err := v.file.Sync(); err != nil {
		panic(mapdb5.NewIO(err))
	}
}

// truncate shrinks the addressable volume to a slice-aligned bound.
func (v *volume) truncate(to int64) {
	if to&sliceMask != 0 {
		panic(mapdb5.NewCorruption("truncate target not page-aligned"))
	}
	v.growMu.Lock()
	defer v.growMu.Unlock()
	cur := *v.pages.Load()
	if needed := int(to >> sliceShift); len(cur) > needed {
		shrunk := make([]*volumePage, needed)
		copy(shrunk, cur)
		v.pages.Store(&shrunk)
	}
	if v.file != nil {
		if err := v.file.Truncate(to); err != nil {
			panic(mapdb5.NewIO(err))
		}
	}
}

// closeVol releases all slices. truncateTo >= 0 shrinks the file first.
func (v *volume) closeVol(truncateTo int64) error {
	v.growMu.Lock()
	v.pages.Store(&[]*volumePage{})
	f := v.file
	v.growMu.Unlock()
	if f == nil {
		return nil
	}
	var err error
	if truncateTo >= 0 {
		if st, e := f.Stat(); e == nil && st.Size() > truncateTo {
			err = f.Truncate(truncateTo)
		}
	}
	if e := f.Sync(); err == nil {
		err = e
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return mapdb5.NewIO(err)
	}
	return nil
}
