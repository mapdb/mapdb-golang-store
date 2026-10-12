package htree

// DirTree — port of Java org.mapdb.htree.DirTree: a sparse,
// bitmap-compressed directory tree mapping a bounded non-negative int64
// index to an int64 value (a recid). Shared infrastructure for HTreeMap
// (hash index → bucket recid) and IndexTree.
//
// Dir node = []int64: dir[0], dir[1] are two 64-bit bitmaps (up to 128
// slots), followed by one (value, storedIndex+1) pair per set bit, in slot
// order. A pair with storedIndex+1 != 0 is a TERMINAL entry for that exact
// index — even at a non-leaf level (path compression: a lone index is
// stored where it first becomes unique, not at full depth). A pair with
// storedIndex+1 == 0 points to a sub-directory one level down. Inserting a
// second index into a terminal slot lazily splits it into a sub-dir chain
// until the two indices diverge (treePutSub).
//
// Level convention: `levels` = the number of directory layers; callers
// pass rootLevel = levels - 1; an index occupies exactly
// levels*dirShift bits. Level L consumes index bits
// [L*dirShift, (L+1)*dirShift).
//
// Single-writer per tree is assumed (TreePut/TreeRemove/TreeClear span
// multiple records); HTreeMap guarantees it with a per-segment write lock.

import (
	"math/bits"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// MaxDirShift: the bitmap is 128 bits, so a dir level resolves at most 7
// index bits.
const MaxDirShift = 7

func dirLenInvariant(dir []int64) bool {
	return len(dir) == 2+2*(bits.OnesCount64(uint64(dir[0]))+bits.OnesCount64(uint64(dir[1])))
}

// DirEmpty is a fresh empty dir record (two zero bitmap longs).
func DirEmpty() []int64 { return make([]int64, 2) }

// dirSerializer is the dir-node wire codec: two raw bitmap longs, then the
// pairs as packed longs (values are recids, indices are stored +1 — both
// non-negative).
type dirSerializer struct{}

var DirSer = ser.NewAnyOf[[]int64](dirSerializer{})

func (dirSerializer) Serialize(w *bin.Writer, dir []int64) {
	if !dirLenInvariant(dir) {
		panic(mapdb5.NewCorruption("dir bitmap/length mismatch"))
	}
	w.WriteLong(dir[0])
	w.WriteLong(dir[1])
	for i := 2; i < len(dir); i++ {
		w.PackLong(dir[i])
	}
}

func (dirSerializer) Deserialize(r *bin.Reader, size int) []int64 {
	b1 := r.ReadLong()
	b2 := r.ReadLong()
	n := 2 + 2*(bits.OnesCount64(uint64(b1))+bits.OnesCount64(uint64(b2)))
	dir := make([]int64, n)
	dir[0] = b1
	dir[1] = b2
	for i := 2; i < n; i++ {
		dir[i] = r.UnpackLong()
	}
	return dir
}

func (dirSerializer) FixedSize() int { return -1 }
func (dirSerializer) SizeHint() int  { return 34 }
func (dirSerializer) Compare(a, b []int64) int {
	panic(mapdb5.NewMisuse("dir nodes are never compared"))
}
func (dirSerializer) Equals(a, b []int64) bool {
	panic(mapdb5.NewMisuse("dir nodes are never compared for equality"))
}
func (dirSerializer) NaturalOrder() bool {
	panic(mapdb5.NewMisuse("dir nodes have no natural order"))
}
func (dirSerializer) EqualsBySerializedBytes() bool { return true }

// treePos returns the slot in [0, 1<<dirShift) consuming level's index bits.
func treePos(dirShift, level int, index int64) int {
	return int(uint64(index) >> (dirShift * level) & ((1 << dirShift) - 1))
}

// dirOffsetFromSlot: slot → offset of its pair in the dir array, via
// bitmap popcount. Returns the NEGATED insertion offset when the slot's
// bit is unset.
func dirOffsetFromSlot(dir []int64, slot int) int {
	return dirOffsetFromBitmaps(dir[0], dir[1], slot)
}

// dirOffsetFromBitmaps — same over raw bitmaps, usable by the push-down
// byte path before (and without) materializing the array.
func dirOffsetFromBitmaps(bitmap1, bitmap2 int64, slot int) int {
	if slot < 0 || slot >= 128 {
		panic(mapdb5.NewCorruption("slot out of range"))
	}
	offset := 0
	v := bitmap1
	if slot > 63 {
		offset += bits.OnesCount64(uint64(v)) * 2
		v = bitmap2
	}
	slot &= 63
	mask := int64(1<<slot) - 1
	offset += 2 + bits.OnesCount64(uint64(v&mask))*2
	if uint64(v)>>slot&1 == 1 { // 1 if occupied, 0 if empty
		return offset
	}
	return -offset
}

// dirPut: copy-on-write insert/replace of the pair at slot; never mutates
// input.
func dirPut(dir []int64, slot int, v1, v2 int64) []int64 {
	offset := dirOffsetFromSlot(dir, slot)
	if offset < 0 { // empty slot: grow by one pair, set bitmap bit
		offset = -offset
		nd := make([]int64, len(dir)+2)
		copy(nd, dir[:offset])
		copy(nd[offset+2:], dir[offset:])
		nd[slot/64] |= 1 << (slot % 64)
		dir = nd
	} else {
		nd := make([]int64, len(dir))
		copy(nd, dir)
		dir = nd
	}
	dir[offset] = v1
	dir[offset+1] = v2
	if !dirLenInvariant(dir) {
		panic(mapdb5.NewCorruption("dirPut broke bitmap/length invariant"))
	}
	return dir
}

// dirRemove: copy-on-write removal of the pair at slot (which must be set).
func dirRemove(dir []int64, slot int) []int64 {
	offset := dirOffsetFromSlot(dir, slot)
	if offset < 0 {
		panic(mapdb5.NewCorruption("dirRemove on empty slot"))
	}
	nd := make([]int64, len(dir)-2)
	copy(nd, dir[:offset])
	copy(nd[offset:], dir[offset+2:])
	nd[slot/64] &= ^(int64(1) << (slot % 64))
	if !dirLenInvariant(nd) {
		panic(mapdb5.NewCorruption("dirRemove broke bitmap/length invariant"))
	}
	return nd
}

// ===================== push-down slot reads =====================

// slotAction decodes the two bitmap longs plus EXACTLY the queried slot's
// (value, storedIndex+1) pair — skipping earlier pairs undecoded — instead
// of materializing the whole []int64 (Java DirTree.SlotAction). Both
// dialects: OnBytes for byte stores, OnObject for StoreOnHeap.
//
// Optimistic-read discipline: keeps NO result state (the verdict rides in
// the return int64), never allocates, and clamps the bitmap-derived offset
// against the record size so torn bitmaps fail fast. "Dive to child" is
// encoded as -childRecid (< 0), terminal verdicts as >= 0.
// Go has no abstract methods — the per-action verdicts ride as function
// fields on the shared decoder.
type slotAction struct {
	dirShift int
	index    int64
	level    int // set by the descent loop before each hop
	onEmpty  func() int64
	onPair   func(value, storedIndex int64) int64
}

func (a *slotAction) OnBytes(r *bin.Reader, size int) int64 {
	b1 := r.ReadLong()
	b2 := r.ReadLong()
	dirPos := dirOffsetFromBitmaps(b1, b2, treePos(a.dirShift, a.level, a.index))
	if dirPos < 0 {
		return a.onEmpty()
	}
	// pairs are >= 2 serialized bytes each, so a consistent record holds
	// at least 16 + dirPos bytes up to and including our pair — torn
	// bitmaps fail here instead of scanning into neighboring records
	if 16+dirPos > size {
		panic(mapdb5.NewCorruption("dir bitmaps exceed record size"))
	}
	r.UnpackLongSkip(dirPos - 2) // skip earlier pairs without decoding
	value := r.UnpackLong()
	storedIndex := r.UnpackLong() - 1
	return a.onPair(value, storedIndex)
}

func (a *slotAction) OnObject(obj any) int64 {
	dir := obj.([]int64) // heap store: live array, no bytes exist
	dirPos := dirOffsetFromSlot(dir, treePos(a.dirShift, a.level, a.index))
	if dirPos < 0 {
		return a.onEmpty()
	}
	return a.onPair(dir[dirPos], dir[dirPos+1]-1)
}

func (a *slotAction) OnNull() int64 {
	panic(mapdb5.NewCorruption("dir node record is null"))
}

// dive: a zero child recid would alias a terminal verdict — reject it.
func dive(childRecid int64) int64 {
	if childRecid == 0 {
		panic(mapdb5.NewCorruption("subdir pointer recid 0"))
	}
	return -childRecid
}

// TreeGet looks up index; returns its value (recid) or 0 when absent.
// Root call passes level = levels - 1. Each hop verdict: >= 0 → final
// answer (value recid, or 0 = absent); < 0 → -childRecid.
func TreeGet(dirShift int, recid uint64, s store.Store, level int, index int64) int64 {
	if dirShift < 1 || dirShift > MaxDirShift {
		panic(mapdb5.NewCorruption("dirShift out of range"))
	}
	if index < 0 || uint64(index)>>((level+1)*dirShift) != 0 {
		panic(mapdb5.NewCorruption("index exceeds level budget"))
	}
	action := &slotAction{
		dirShift: dirShift,
		index:    index,
		onEmpty:  func() int64 { return 0 },
		onPair: func(value, storedIndex int64) int64 {
			if storedIndex != -1 {
				// terminal entry: hit only on the exact index
				if storedIndex == index {
					return value
				}
				return 0
			}
			return dive(value) // sub-directory: dive one level
		},
	}
	for ; level >= 0; level-- {
		action.level = level
		ret := s.Read(recid, action)
		if ret >= 0 {
			return ret
		}
		recid = uint64(-ret)
	}
	panic(mapdb5.NewCorruption("dir tree deeper than declared levels"))
}

// ===================== put =====================

const (
	putMutate = 0
	putNoop   = 1
)

// TreePut inserts or replaces index → value (value must be a live recid,
// != 0). Root call passes level = levels - 1. The root dir record is only
// ever updated in place, so the root recid is stable for the tree's
// lifetime. Caller must hold the tree's write lock.
//
// Descent is push-down: ancestors on the way down are probed slot-only via
// putProbeAction; only the single node that actually mutates is
// materialized and rewritten. The probe→materialize re-read is race-free
// because the tree is single-writer.
func TreePut(dirShift int, recid uint64, s store.Store, level int, index, value int64) {
	if dirShift < 1 || dirShift > MaxDirShift || index < 0 || value == 0 {
		panic(mapdb5.NewCorruption("treePut argument out of range"))
	}
	if uint64(index)>>((level+1)*dirShift) != 0 {
		panic(mapdb5.NewCorruption("index exceeds level budget"))
	}
	// putProbeAction — descent hop: < 0 → -childRecid (dive on, nothing to
	// write at this level); putNoop → mapping already present verbatim;
	// putMutate → this node must change — materialize it (and only it) and
	// apply the write.
	probe := &slotAction{
		dirShift: dirShift,
		index:    index,
		onEmpty:  func() int64 { return putMutate }, // empty slot: insert here
		onPair: func(storedValue, storedIndex int64) int64 {
			if storedIndex == -1 {
				return dive(storedValue) // sub-directory
			}
			if storedIndex == index && storedValue == value {
				return putNoop
			}
			return putMutate
		},
	}
	for ; level >= 0; level-- {
		probe.level = level
		verdict := s.Read(recid, probe)
		if verdict < 0 { // sub-directory: dive one level
			recid = uint64(-verdict)
			continue
		}
		if verdict == putNoop {
			return // mapping already present
		}
		// this node mutates: materialize just it and apply the write
		dir, ok := store.Get(s, recid, DirSer)
		if !ok {
			panic(mapdb5.NewCorruption("dir node record is null"))
		}
		slot := treePos(dirShift, level, index)
		dirPos := dirOffsetFromSlot(dir, slot)
		if dirPos < 0 { // empty slot: install terminal entry
			store.Update(s, recid, dirPut(dir, slot, value, index+1), DirSer)
			return
		}
		oldValue := dir[dirPos]
		oldIndex := dir[dirPos+1] - 1
		// single writer per tree: the node cannot have changed since the
		// probe, so the slot cannot have become a subdir pointer
		if oldIndex == -1 {
			panic(mapdb5.NewCorruption("dir node changed under the tree write lock"))
		}
		if oldIndex == index { // same index: swap the value
			if oldValue == value {
				return
			}
			dir = append([]int64(nil), dir...)
			dir[dirPos] = value
			store.Update(s, recid, dir, DirSer)
			return
		}
		// slot holds a terminal entry for a DIFFERENT index: lazy split —
		// push both entries into a fresh subdir chain until their slots
		// diverge
		subRecid := treePutSub(dirShift, s, level-1, index, value, oldIndex, oldValue)
		dir = append([]int64(nil), dir...)
		dir[dirPos] = int64(subRecid)
		dir[dirPos+1] = 0 // mark as subdir pointer
		store.Update(s, recid, dir, DirSer)
		return
	}
	panic(mapdb5.NewCorruption("dir tree deeper than declared levels"))
}

// treePutSub builds the subdir chain holding two colliding terminal
// entries; returns its recid.
func treePutSub(dirShift int, s store.Store, level int, index1, value1, index2, value2 int64) uint64 {
	// distinct indices masked to the same budget must diverge at some level >= 0
	if level < 0 {
		panic(mapdb5.NewCorruption("identical indices reached below level 0"))
	}
	if uint64(index1)>>((level+1)*dirShift) != uint64(index2)>>((level+1)*dirShift) {
		panic(mapdb5.NewCorruption("split indices disagree above current level"))
	}
	pos1 := treePos(dirShift, level, index1)
	pos2 := treePos(dirShift, level, index2)
	var dir []int64
	if pos1 == pos2 { // still colliding: chain one level deeper
		recid := treePutSub(dirShift, s, level-1, index1, value1, index2, value2)
		dir = dirPut(DirEmpty(), pos1, int64(recid), 0)
	} else {
		dir = dirPut(DirEmpty(), pos1, value1, index1+1)
		dir = dirPut(dir, pos2, value2, index2+1)
	}
	return store.Put(s, dir, DirSer)
}

// ===================== remove (collapsing) =====================

// dirRemoved is the sentinel result: entry removed, no collapse propagates
// past that level.
var dirRemoved = []int64{}

// TreeRemove removes index from the tree, collapsing on the way out (port
// of mapdb3 IndexTreeListJava.treeRemoveCollapsing). Returns true iff the
// index was present. Root call passes level = levels - 1; caller must hold
// the tree's write lock.
//
// Collapse rules (the path-compression invariant of treePutSub, in
// reverse):
//   - a non-root dir left with a SINGLE terminal occupant is deleted and
//     that occupant is pushed UP into the parent slot the dir hung from;
//   - a non-root dir whose only occupant was the just-collapsed child
//     pointer is deleted too, propagating the pushed-up occupant further;
//   - the ROOT is exempt from both (its recid must stay stable): it may
//     rest with any occupancy, including a single terminal entry or fully
//     empty.
func TreeRemove(dirShift int, recid uint64, s store.Store, level int, index int64) bool {
	if dirShift < 1 || dirShift > MaxDirShift || index < 0 {
		panic(mapdb5.NewCorruption("treeRemove argument out of range"))
	}
	if uint64(index)>>((level+1)*dirShift) != 0 {
		panic(mapdb5.NewCorruption("index exceeds level budget"))
	}
	return treeRemoveCollapsing(dirShift, recid, s, level, true, index) != nil
}

// treeRemoveCollapsing returns nil = index absent (no mutation);
// dirRemoved = removed, parents unaffected; any other array = THIS node
// was deleted and its lone surviving TERMINAL occupant is at [2],[3] —
// the caller must install it in place of its pointer to this node (or keep
// propagating the collapse).
func treeRemoveCollapsing(dirShift int, recid uint64, s store.Store, level int, topLevel bool, index int64) []int64 {
	if level < 0 {
		panic(mapdb5.NewCorruption("dir tree deeper than declared levels"))
	}
	dir, ok := store.Get(s, recid, DirSer)
	if !ok {
		panic(mapdb5.NewCorruption("dir node record is null"))
	}
	slot := treePos(dirShift, level, index)
	pos := dirOffsetFromSlot(dir, slot)
	if pos < 0 {
		return nil // slot empty: index absent
	}
	value := dir[pos]
	storedIndex := dir[pos+1] - 1

	if storedIndex == -1 { // subdir pointer: dive one level
		if value == 0 {
			panic(mapdb5.NewCorruption("subdir pointer recid 0"))
		}
		result := treeRemoveCollapsing(dirShift, uint64(value), s, level-1, false, index)
		if result == nil || len(result) == 0 { // nil or dirRemoved
			return result
		}
		// child collapsed: result[2],[3] is its lone terminal occupant,
		// pushed up
		if len(dir) == 4 && !topLevel {
			// that pointer was our only occupant: collapse this node too
			store.Delete(s, recid, DirSer)
			return result
		}
		// replace the subdir pointer with the pushed-up terminal entry
		dir = append([]int64(nil), dir...)
		dir[pos] = result[2]
		dir[pos+1] = result[3]
		store.Update(s, recid, dir, DirSer)
		return dirRemoved
	}

	if storedIndex != index {
		return nil // terminal for a DIFFERENT index: absent
	}

	dir = dirRemove(dir, slot)
	if len(dir) == 4 && dir[3] > 0 && !topLevel {
		// one occupant left and it is TERMINAL (dir[3] = storedIndex+1 > 0):
		// delete this node and push the occupant up (never push up a subdir
		// pointer — its subtree's indices don't share the parent slot's full
		// path)
		store.Delete(s, recid, DirSer)
		return dir
	}
	store.Update(s, recid, dir, DirSer)
	return dirRemoved
}

// ===================== fold / clear / isEmpty =====================

// TreeFold visits every (index, value) entry in the tree, unordered.
// Caller must hold at least the tree's read lock for the whole traversal.
//
// Deliberately NOT push-down: a full traversal decodes every pair anyway,
// and a RecordRead may be invoked on torn bytes, so it could not safely
// call the visitor (external state) mid-invocation.
func TreeFold(recid uint64, s store.Store, level int, visitor func(index, value int64)) {
	if level < 0 {
		panic(mapdb5.NewCorruption("dir tree deeper than declared levels"))
	}
	dir, ok := store.Get(s, recid, DirSer)
	if !ok {
		panic(mapdb5.NewCorruption("dir node record is null"))
	}
	for pos := 2; pos < len(dir); pos += 2 {
		value := dir[pos]
		storedIndex := dir[pos+1] - 1
		if storedIndex == -1 {
			if value == 0 {
				panic(mapdb5.NewCorruption("subdir pointer recid 0"))
			}
			TreeFold(uint64(value), s, level-1, visitor)
		} else {
			visitor(storedIndex, value)
		}
	}
}

// TreeClear deletes the whole tree's contents in one traversal (port of
// mapdb3 IndexTreeListJava.treeClear): every subdir record is freed, the
// ROOT record is reset to DirEmpty() IN PLACE (recid stays stable), and
// every terminal entry is reported to visitor so the caller can free the
// records its values point to. Caller must hold the tree's write lock.
func TreeClear(recid uint64, s store.Store, level int, visitor func(index, value int64)) {
	treeClear(recid, s, level, true, visitor)
}

func treeClear(recid uint64, s store.Store, level int, topLevel bool, visitor func(index, value int64)) {
	if level < 0 {
		panic(mapdb5.NewCorruption("dir tree deeper than declared levels"))
	}
	dir, ok := store.Get(s, recid, DirSer)
	if !ok {
		panic(mapdb5.NewCorruption("dir node record is null"))
	}
	if topLevel {
		store.Update(s, recid, DirEmpty(), DirSer)
	} else {
		store.Delete(s, recid, DirSer)
	}
	for pos := 2; pos < len(dir); pos += 2 {
		value := dir[pos]
		storedIndex := dir[pos+1] - 1
		if storedIndex == -1 {
			if value == 0 {
				panic(mapdb5.NewCorruption("subdir pointer recid 0"))
			}
			treeClear(uint64(value), s, level-1, false, visitor)
		} else {
			visitor(storedIndex, value)
		}
	}
}

// isEmptyAction is the push-down root emptiness probe: decodes only the
// two bitmap longs. An empty root implies an empty tree — every occupant
// (terminal or subdir chain) has at least one terminal entry beneath it.
// Stateless, so safe under the store's optimistic-read retry discipline.
type isEmptyAction struct{}

func (isEmptyAction) OnBytes(r *bin.Reader, size int) int64 {
	if r.ReadLong()|r.ReadLong() == 0 {
		return 1
	}
	return 0
}

func (isEmptyAction) OnObject(obj any) int64 {
	dir := obj.([]int64)
	if dir[0]|dir[1] == 0 {
		return 1
	}
	return 0
}

func (isEmptyAction) OnNull() int64 {
	panic(mapdb5.NewCorruption("dir node record is null"))
}

// TreeIsEmpty: true iff the tree holds no entries; reads only the root's
// bitmaps.
func TreeIsEmpty(recid uint64, s store.Store) bool {
	return s.Read(recid, isEmptyAction{}) == 1
}
