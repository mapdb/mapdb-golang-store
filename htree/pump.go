package htree

// HTreePump — port of Java org.mapdb.htree.HTreePump: bottom-up bulk
// builder for HTree directory trees. The builder validates hash ordering
// and bucket grouping, writes each leaf/dir/header record exactly once,
// and produces a normal live map with the same lookup performance as an
// incrementally built one.

import (
	"iter"
	"math/rand"
	"sort"

	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// CreateHTreeMapFromSortedByHash bulk-loads a map from entries sorted by
// NON-DECREASING unsigned 32-bit placement hash — the caller sorts by
// uint32(Hasher.Hash(key, HashSeed)) with the same hasher and seed as cfg.
// Misordered input and duplicate keys return pump.ErrNotSorted; store
// corruption surfaces as *mapdb5.DBError. The result is a normal live
// handle (open lease held; close with CloseHandle).
func CreateHTreeMapFromSortedByHash[K, V any](s store.Store, cfg HConfig[K, V],
	entries iter.Seq2[K, V]) (m *HTreeMap[K, V], err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				if de.Kind == mapdb5.KindNotSorted {
					err = pump.ErrNotSorted
					return
				}
				err = de
				return
			}
			panic(r)
		}
	}()
	concShift, dirShift, levels, err2 := resolveConfig(cfg)
	if err2 != nil {
		return nil, err2
	}
	if cfg.KeySer == nil || cfg.ValueSer == nil {
		return nil, mapdb5.NewMisuse("htree: KeySer and ValueSer are required")
	}
	seed := cfg.HashSeed
	if !cfg.HashSeedSet {
		seed = int32(rand.Uint32())
	}
	hasher := resolveHasher(cfg)
	leafSer := leafSerializer[K, V]{keySer: cfg.KeySer, valueSer: cfg.ValueSer}
	hashSource := func(key K) int64 {
		return int64(uint32(hasher.Hash(key, seed)))
	}
	headerRecid := htreeBuild(s, concShift, dirShift, levels, entries,
		hashSource, cfg.KeySer,
		func(keys []K, vals []V) uint64 {
			return store.Put(s, hleaf[K, V]{keys: keys, vals: vals}, leafSer)
		},
		func(roots []uint64) uint64 {
			return store.Put(s, hHeader{
				concShift: int32(concShift), dirShift: int32(dirShift), levels: int32(levels),
				hashSeed: seed, segmentRoots: roots,
			}, headerSerializer{})
		})
	return openHTreeMap(s, headerRecid, cfg)
}

// htreeBuild is the variant-agnostic core (Java HTreePump.build): group
// entries by (segment, index) — legal because the hash stream is sorted —
// write each bucket once, and build each segment's dir tree bottom-up in
// memory, materializing each dir record exactly once.
func htreeBuild[K, V any](s store.Store, concShift, dirShift, levels int,
	entries iter.Seq2[K, V],
	hashSource func(K) int64,
	keySer ser.Serializer[K],
	leafWriter func(keys []K, vals []V) uint64,
	headerWriter func(roots []uint64) uint64) (headerRecid uint64) {

	roots := make([]uint64, 1<<concShift)
	s.PreallocateN(roots)

	segmentShift := levels * dirShift
	indexMask := (int64(1) << segmentShift) - 1
	concMask := int64(1<<concShift) - 1

	state := &hBuildState{currentSegment: -1}
	var previousHash int64
	havePrevious := false

	var group *hGroup[K, V]
	next, stop := iter.Pull2(entries)
	defer stop()
	for {
		key, value, ok := next()
		if !ok {
			break
		}
		hash := hashSource(key)
		if havePrevious && previousHash > hash {
			panic(mapdb5.New(mapdb5.KindNotSorted, "bulk-load hashes not sorted unsigned"))
		}
		havePrevious = true
		previousHash = hash

		segment := int(hash>>uint(segmentShift)) & int(concMask)
		index := hash & indexMask
		if group == nil {
			group = &hGroup[K, V]{segment: segment, index: index}
		} else if group.segment != segment || group.index != index {
			if segment < group.segment {
				panic(mapdb5.New(mapdb5.KindNotSorted, "bulk-load segments not sorted"))
			}
			if group.segment == segment && group.index > index {
				panic(mapdb5.New(mapdb5.KindNotSorted, "bulk-load indices not sorted"))
			}
			flushHGroup(s, roots, dirShift, levels, leafWriter, group, state)
			group = &hGroup[K, V]{segment: segment, index: index}
		}
		group.add(keySer, key, value)
	}

	if group != nil {
		flushHGroup(s, roots, dirShift, levels, leafWriter, group, state)
	}
	if state.segmentBuilder != nil {
		store.Update(s, roots[state.currentSegment],
			state.segmentBuilder.finish(s), DirSer)
	}
	state.currentSegment++
	for seg := max(0, state.currentSegment); seg < len(roots); seg++ {
		store.Update(s, roots[seg], DirEmpty(), DirSer)
	}
	return headerWriter(roots)
}

type hBuildState struct {
	currentSegment int
	segmentBuilder *hSegmentBuilder
}

func flushHGroup[K, V any](s store.Store, roots []uint64, dirShift, levels int,
	leafWriter func(keys []K, vals []V) uint64, group *hGroup[K, V], state *hBuildState) {
	if state.currentSegment == -1 {
		for seg := 0; seg < group.segment; seg++ {
			store.Update(s, roots[seg], DirEmpty(), DirSer)
		}
		state.currentSegment = group.segment
		state.segmentBuilder = newHSegmentBuilder(dirShift, levels)
	} else if group.segment != state.currentSegment {
		store.Update(s, roots[state.currentSegment],
			state.segmentBuilder.finish(s), DirSer)
		for seg := state.currentSegment + 1; seg < group.segment; seg++ {
			store.Update(s, roots[seg], DirEmpty(), DirSer)
		}
		state.currentSegment = group.segment
		state.segmentBuilder = newHSegmentBuilder(dirShift, levels)
	}
	leafRecid := leafWriter(group.keys, group.vals)
	state.segmentBuilder.put(group.index, int64(leafRecid))
}

// hGroup is one (segment, index) bucket under accumulation.
type hGroup[K, V any] struct {
	segment int
	index   int64
	keys    []K
	vals    []V
}

func (g *hGroup[K, V]) add(keySer ser.Serializer[K], key K, value V) {
	for _, k := range g.keys {
		if keySer.Equals(k, key) {
			panic(mapdb5.New(mapdb5.KindNotSorted, "duplicate key in hash bucket"))
		}
	}
	g.keys = append(g.keys, key)
	g.vals = append(g.vals, value)
}

// hSegmentBuilder builds one segment's dir tree bottom-up in memory;
// finish materializes each dir record exactly once.
type hSegmentBuilder struct {
	dirShift  int
	rootLevel int
	root      *segNode
}

type segNode struct {
	dir      []int64
	children map[int]*segNode // slot -> child (in-memory only)
}

func newHSegmentBuilder(dirShift, levels int) *hSegmentBuilder {
	return &hSegmentBuilder{
		dirShift:  dirShift,
		rootLevel: levels - 1,
		root:      &segNode{dir: DirEmpty()},
	}
}

func (b *hSegmentBuilder) put(index, value int64) {
	b.putAt(b.root, b.rootLevel, index, value)
}

func (b *hSegmentBuilder) putAt(node *segNode, level int, index, value int64) {
	slot := treePos(b.dirShift, level, index)
	pos := dirOffsetFromSlot(node.dir, slot)
	if pos < 0 {
		node.dir = dirPut(node.dir, slot, value, index+1)
		return
	}
	oldIndex := node.dir[pos+1] - 1
	if oldIndex == -1 {
		child, ok := node.children[slot]
		if !ok {
			panic(mapdb5.NewCorruption("missing in-memory child"))
		}
		b.putAt(child, level-1, index, value)
		return
	}
	if oldIndex == index {
		panic(mapdb5.New(mapdb5.KindNotSorted, "duplicate hash index"))
	}
	oldValue := node.dir[pos]
	child := b.putSub(level-1, index, value, oldIndex, oldValue)
	if node.children == nil {
		node.children = map[int]*segNode{}
	}
	node.children[slot] = child
	// reserve the slot pair; materialize fills in the child recid
	dir := append([]int64(nil), node.dir...)
	dir[pos] = 0
	dir[pos+1] = 0
	node.dir = dir
}

// putSub is the pump's copy of DirTree.treePutSub: a fresh node chain
// holding two colliding terminal entries.
func (b *hSegmentBuilder) putSub(level int, index1, value1, index2, value2 int64) *segNode {
	if level < 0 {
		panic(mapdb5.New(mapdb5.KindNotSorted, "duplicate hash index"))
	}
	pos1 := treePos(b.dirShift, level, index1)
	pos2 := treePos(b.dirShift, level, index2)
	node := &segNode{dir: DirEmpty()}
	if pos1 == pos2 {
		child := b.putSub(level-1, index1, value1, index2, value2)
		node.children = map[int]*segNode{pos1: child}
		node.dir = dirPut(node.dir, pos1, 0, 0) // reserve the slot
	} else {
		node.dir = dirPut(node.dir, pos1, value1, index1+1)
		node.dir = dirPut(node.dir, pos2, value2, index2+1)
	}
	return node
}

// finish writes the tree bottom-up and returns the root dir content.
func (b *hSegmentBuilder) finish(s store.Store) []int64 {
	return b.materialize(b.root, s)
}

func (b *hSegmentBuilder) materialize(node *segNode, s store.Store) []int64 {
	dir := append([]int64(nil), node.dir...)
	// sorted slot order keeps record allocation deterministic (Java TreeMap)
	slots := make([]int, 0, len(node.children))
	for slot := range node.children {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	for _, slot := range slots {
		pos := dirOffsetFromSlot(dir, slot)
		childRecid := store.Put(s, b.materialize(node.children[slot], s), DirSer)
		dir[pos] = int64(childRecid)
		dir[pos+1] = 0
	}
	return dir
}
