package btree

import (
	"iter"

	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// errNotSorted is the shared bulk-load sentinel (mapdb-golang pump
// package, G4 — never mint a parallel one).
var errNotSorted = pump.ErrNotSorted

// NodeSink materializes and writes one finished node into a PREALLOCATED
// recid (Java TreePump's anonymous NodeSink). Keeps the pump itself
// serialization-free; BTreeMap (and later BufferTreeMap) provide the
// concrete sink. Methods panic *mapdb5.DBError on store failure.
type NodeSink[K, V any] interface {
	// CompareKeys is the key order (== KeyFormat.Compare); used for the
	// strictly-ascending check.
	CompareKeys(a, b K) int
	// WriteLeaf writes a leaf node: keys+values pairs; link is the
	// right-sibling recid (0 when RIGHT is set).
	WriteLeaf(recid store.Recid, flags int32, link store.Recid, keys []K, values []V)
	// WriteDir writes a directory node: separator keys + child recids.
	WriteDir(recid store.Recid, flags int32, link store.Recid, keys []K, children []store.Recid)
}

// pumpLevel is one in-progress node per tree level; index 0 = leaf.
type pumpLevel[K, V any] struct {
	keys     []K
	values   []V           // leaf level only
	children []store.Recid // dir levels only
	pending  store.Recid   // preallocated recid of this level's NEXT node; 0 = none
	first    bool          // no node flushed at this level yet (LEFT candidate)
}

// TreePump is the bottom-up single-threaded bulk builder (Java TreePump).
// Feed strictly ascending entries via Put, then Finish once; every node is
// written EXACTLY once via preallocate-next-sibling-then-write (forward
// links, no back-patching), so on a fresh store recids and data lay out
// sequentially in key order.
type TreePump[K, V any] struct {
	s        store.Store
	sink     NodeSink[K, V]
	nodeFill int
	levels   []pumpLevel[K, V]
	prevKey  K
	hasPrev  bool
	finished bool
}

// NewTreePump builds a pump writing nodes of up to maxNodeSize entries,
// flushing at nodeFill.
func NewTreePump[K, V any](s store.Store, sink NodeSink[K, V], maxNodeSize, nodeFill int) *TreePump[K, V] {
	if nodeFill < 2 || nodeFill > maxNodeSize {
		panic(mapdb5.NewMisuse("nodeFill must be in [2, maxNodeSize]"))
	}
	return &TreePump[K, V]{
		s:        s,
		sink:     sink,
		nodeFill: nodeFill,
		levels:   []pumpLevel[K, V]{{first: true}},
	}
}

// DefaultPumpFill is the default pump fill: 3/4 of maxNodeSize (mapdb1/2/3
// lineage).
func DefaultPumpFill(maxNodeSize int) int {
	f := maxNodeSize * 3 / 4
	if f < 2 {
		f = 2
	}
	return f
}

// Put feeds one entry; input must be strictly ascending or it panics
// *mapdb5.DBError{KindNotSorted}.
func (p *TreePump[K, V]) Put(key K, value V) {
	if p.finished {
		panic(mapdb5.NewMisuse("pump already finished"))
	}
	if p.hasPrev && p.sink.CompareKeys(p.prevKey, key) >= 0 {
		panic(mapdb5.New(mapdb5.KindNotSorted, "bulk-load input not in ascending key order"))
	}
	// flush BEFORE adding: interior leaves hold exactly nodeFill entries.
	if len(p.levels[0].keys) == p.nodeFill {
		p.flushLeaf()
	}
	leaf := &p.levels[0]
	leaf.keys = append(leaf.keys, key)
	leaf.values = append(leaf.values, value)
	p.prevKey = key
	p.hasPrev = true
}

// nodeRecid is the recid this level's next node lands in: reserved by the
// previous flush, or fresh for a level's first node.
func (p *TreePump[K, V]) nodeRecid(level int) store.Recid {
	if pending := p.levels[level].pending; pending != 0 {
		return pending
	}
	return p.s.Preallocate()
}

func (p *TreePump[K, V]) flushLeaf() {
	recid := p.nodeRecid(0)
	link := p.s.Preallocate()
	leaf := &p.levels[0]
	leaf.pending = link
	var flags int32
	if leaf.first {
		flags = flagLeft
	}
	leaf.first = false
	keys := leaf.keys
	values := leaf.values
	leaf.keys = nil
	leaf.values = nil
	sep := keys[len(keys)-1] // leaf's inclusive high bound
	p.sink.WriteLeaf(recid, flags, link, keys, values)
	p.pushUp(1, sep, recid)
}

// pushUp registers a flushed node with its parent level; sep = child's high
// bound.
func (p *TreePump[K, V]) pushUp(level int, sep K, child store.Recid) {
	for len(p.levels) <= level {
		p.levels = append(p.levels, pumpLevel[K, V]{first: true})
	}
	if len(p.levels[level].keys) == p.nodeFill {
		p.flushDir(level)
	}
	dir := &p.levels[level]
	dir.keys = append(dir.keys, sep)
	dir.children = append(dir.children, child)
}

func (p *TreePump[K, V]) flushDir(level int) {
	recid := p.nodeRecid(level)
	link := p.s.Preallocate()
	dir := &p.levels[level]
	dir.pending = link
	flags := flagDir
	if dir.first {
		flags |= flagLeft
	}
	dir.first = false
	keys := dir.keys
	children := dir.children
	dir.keys = nil
	dir.children = nil
	sep := keys[len(keys)-1]
	// non-rightmost dir shape: childCount == keysLen, last key = its bound.
	p.sink.WriteDir(recid, flags, link, keys, children)
	p.pushUp(level+1, sep, recid)
}

// Finish flushes the final (rightmost) node of every level, bottom-up, and
// returns the root NODE recid.
func (p *TreePump[K, V]) Finish() store.Recid {
	if p.finished {
		panic(mapdb5.NewMisuse("pump already finished"))
	}
	p.finished = true
	leaf := &p.levels[0]
	leafFlags := int32(flagRight)
	if leaf.first {
		leafFlags |= flagLeft
	}
	child0 := p.nodeRecid(0)
	p.sink.WriteLeaf(child0, leafFlags, 0, leaf.keys, leaf.values)
	child := child0
	for i := 1; i < len(p.levels); i++ {
		recid := p.nodeRecid(i)
		dir := &p.levels[i]
		flags := flagDir | flagRight
		if dir.first {
			flags |= flagLeft
		}
		children := append(dir.children, child) // rightmost extra child, no key
		p.sink.WriteDir(recid, flags, 0, dir.keys, children)
		child = recid
	}
	return child
}

// ===================== BTreeMap sink + CreateFromSorted =====================

// btreeSink materializes a node and writes it to its preallocated recid —
// the BTreeMap NodeSink (bulk builds are always inline; the Java Sink only
// builds inline maps).
type btreeSink[K, V any] struct {
	s           store.Store
	kf          ser.GroupFormat[K]
	vf          ser.GroupFormat[V]
	maxNodeSize int
}

func (s btreeSink[K, V]) CompareKeys(a, b K) int { return s.kf.Compare(a, b) }

func (s btreeSink[K, V]) WriteLeaf(recid store.Recid, flags int32, link store.Recid, keys []K, values []V) {
	// non-rightmost leaf: fence = last key (its inclusive high bound).
	var fence ser.Group
	if flags&flagRight == 0 {
		fence = s.kf.FromSlice([]K{keys[len(keys)-1]})
	}
	n := node{
		flags:  flags,
		link:   link,
		keys:   s.kf.FromSlice(keys),
		values: s.vf.FromSlice(values),
		fence:  fence,
	}
	store.Update(s.s, recid, n, newNodeSerializer(s.kf, s.vf, s.maxNodeSize, true))
}

func (s btreeSink[K, V]) WriteDir(recid store.Recid, flags int32, link store.Recid, keys []K, children []store.Recid) {
	n := node{
		flags:    flags,
		link:     link,
		keys:     s.kf.FromSlice(keys),
		children: children,
	}
	store.Update(s.s, recid, n, newNodeSerializer(s.kf, s.vf, s.maxNodeSize, true))
}

// CreateFromSorted bulk-builds a map from STRICTLY ascending entries using
// the default pump fill (3/4 of maxNodeSize). Returns pump.ErrNotSorted
// (mapdb-golang sentinel) when input is misordered or has duplicate keys;
// store corruption surfaces as a *mapdb5.DBError error. Bulk builds are
// always INLINE (Config.ExternalValues is ignored — Java's Sink only builds
// inline maps). When Config.Counter is set an O(1) size counter is
// allocated and initialized to the entry count.
func CreateFromSorted[K, V any](s store.Store, cfg Config[K, V], entries iter.Seq2[K, V]) (m *BTreeMap[K, V], err error) {
	return CreateFromSortedFill(s, cfg, DefaultPumpFill(cfg.MaxNodeSize), entries)
}

// CreateFromSortedFill is CreateFromSorted with an explicit node fill.
func CreateFromSortedFill[K, V any](s store.Store, cfg Config[K, V], nodeFill int, entries iter.Seq2[K, V]) (m *BTreeMap[K, V], err error) {
	defer func() {
		if r := recover(); r != nil {
			if de, ok := mapdb5.AsDBError(r); ok {
				if de.Kind == mapdb5.KindNotSorted {
					err = errNotSorted
					return
				}
				err = de
				return
			}
			panic(r)
		}
	}()
	// Create-time API arg (never from stored bytes): configuration error.
	if cfg.MaxNodeSize < MinMaxNodeSize || cfg.MaxNodeSize > MaxMaxNodeSize {
		return nil, mapdb5.New(mapdb5.KindWrongConfiguration, "maxNodeSize must be in 4..=1048576")
	}
	sink := btreeSink[K, V]{s: s, kf: cfg.KeyFormat, vf: cfg.ValueFormat, maxNodeSize: cfg.MaxNodeSize}
	pump := NewTreePump(s, sink, cfg.MaxNodeSize, nodeFill)
	var count int64
	for k, v := range entries {
		pump.Put(k, v)
		count++
	}
	rootRecid := pump.Finish()
	rrr := store.Put(s, int64(rootRecid), ser.Long)
	var counterRecid uint64
	if cfg.Counter {
		counterRecid = store.Put(s, count, ser.Long)
	}
	return openModeErr(s, rrr, cfg, counterRecid, true)
}

// CreateFromSink is the Sink-shaped bulk builder (mapdb-golang Sink
// pattern): Put entries, then Build returns the map. It wraps a TreePump
// against a fresh BTreeMap node sink; Build panics *mapdb5.DBError — use
// CreateFromSorted for the error-returning form.
type CreateSink[K, V any] struct {
	pump *TreePump[K, V]
	s    store.Store
	cfg  Config[K, V]
}

// NewCreateSink builds a create-sink with the default pump fill.
func NewCreateSink[K, V any](s store.Store, cfg Config[K, V]) *CreateSink[K, V] {
	sink := btreeSink[K, V]{s: s, kf: cfg.KeyFormat, vf: cfg.ValueFormat, maxNodeSize: cfg.MaxNodeSize}
	return &CreateSink[K, V]{
		pump: NewTreePump(s, sink, cfg.MaxNodeSize, DefaultPumpFill(cfg.MaxNodeSize)),
		s:    s,
		cfg:  cfg,
	}
}

// Put feeds one entry (strictly ascending required).
func (c *CreateSink[K, V]) Put(key K, value V) { c.pump.Put(key, value) }

// Build finishes the tree and opens it; panics *mapdb5.DBError on
// misordered input or store failure.
func (c *CreateSink[K, V]) Build() *BTreeMap[K, V] {
	rootRecid := c.pump.Finish()
	rrr := store.Put(c.s, int64(rootRecid), ser.Long)
	m, err := openModeErr(c.s, rrr, c.cfg, 0, true)
	if err != nil {
		panic(err)
	}
	return m
}
