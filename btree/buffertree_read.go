package btree

import (
	"iter"
	"math"
	"runtime"
	"slices"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/format"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

func sortStableBy(idx []int, cmp func(a, b int) int) { slices.SortStableFunc(idx, cmp) }

// ===================== get: push-down read action =====================

// bufferGetAction is one traversal step over [baseLen][base][ops]: scan the
// op tail newest-first (a hit is newer than anything below — terminal),
// else route/search the base image (Java BufferGetAction).
type bufferGetAction[K, V any] struct {
	store.RecordReadBase
	in    *bufferTreeInner[K, V]
	key   K
	probe []byte // canonical key bytes, serialized lazily; nil = decode+compare
	pr    format.PointResult[V]
	hdr   format.BaseHeader
	value V
	found bool
}

func (a *bufferGetAction[K, V]) probeBytes() []byte {
	if a.probe == nil {
		a.probe = a.in.codec.SerializeKey(a.key)
	}
	return a.probe
}

func (a *bufferGetAction[K, V]) OnBytes(r *bin.Reader, size int) int64 {
	in := a.in
	a.found = false
	var zero V
	a.value = zero
	start := r.Pos()
	if size < 4 {
		panic(mapdb5.NewCorruption("buffer-tree record too short for node header"))
	}
	baseLen := int(r.ReadInt())
	if baseLen < 0 || baseLen > size-4 {
		panic(mapdb5.NewCorruption("torn baseLen"))
	}
	end := start + size
	logStart := start + 4 + baseLen
	var probe []byte
	if in.canonicalKey && logStart < end {
		probe = a.probeBytes()
	}
	// op tail: TRUE newest-first reverse scan, stop at the first match.
	if in.codec.ProbeNewestFirst(r, logStart, end, a.key, probe, math.MaxInt64, &a.pr) {
		if a.pr.Put {
			a.value = a.pr.Value
			a.found = true
		}
		return 0 // PUT → value; DELETE → definitively absent
	}
	// base image
	in.codec.ParseBaseHeader(r, start+4, baseLen, &a.hdr)
	r.Seek(a.hdr.NodeBase)
	h := r.UnpackInt()
	flags := h & 0xF
	keysLen := int(uint32(h) >> 4)
	if keysLen > baseLen {
		panic(mapdb5.NewCorruption("torn keysLen"))
	}
	var link uint64
	if flags&flagRight == 0 {
		l := r.UnpackLong()
		if l == 0 {
			panic(mapdb5.NewCorruption("non-rightmost node with zero link"))
		}
		link = uint64(l)
	}
	keyGroupPos := r.Pos()
	// Fingerprint skip ONLY on a proven leaf (dirs carry fpLen=0); a
	// deleted base key was already answered by the log DELETE above.
	//
	// A fingerprint miss proves absence from THIS leaf's base only. A
	// reader holding a pre-split parent/root recid can land on the
	// rewritten left half of a split whose key moved to the right sibling,
	// so on a non-rightmost leaf the B-link move-right test (key beyond the
	// base's greatest key) must still run before answering "absent".
	if flags&flagDir == 0 && in.canonicalKey {
		if !in.codec.BaseFpMightContain(r, &a.hdr, a.probeBytes()) {
			if link == 0 {
				return 0
			}
			if keysLen == 0 {
				return int64(link)
			}
			r.Seek(keyGroupPos)
			var last K
			if in.kf.SupportsBinary() {
				last = in.kf.BytesGet(r, keysLen, keysLen-1)
			} else {
				last = in.kf.Get(in.kf.DeserializeGroup(r, keysLen), keysLen-1)
			}
			if in.kf.Compare(a.key, last) > 0 {
				return int64(link)
			}
			return 0
		}
	}
	r.Seek(keyGroupPos)
	var pos int
	var found bool
	if in.kf.SupportsBinary() {
		pos, found = in.kf.BytesSearch(r, keysLen, a.key)
	} else {
		pos, found = in.kf.Search(in.kf.DeserializeGroup(r, keysLen), a.key)
	}
	if flags&flagDir != 0 {
		childCount := keysLen
		if flags&flagRight != 0 {
			childCount++
		}
		if childCount == 0 {
			panic(mapdb5.NewCorruption("directory node with no children"))
		}
		if pos >= childCount {
			return int64(link)
		}
		r.UnpackLongSkip(pos)
		c := r.UnpackLong()
		if c == 0 {
			panic(mapdb5.NewCorruption("directory child recid is zero"))
		}
		return c
	}
	if found {
		a.found = true
		if in.vf.SupportsBinary() {
			a.value = in.vf.BytesGet(r, keysLen, pos)
		} else {
			a.value = in.vf.Get(in.vf.DeserializeGroup(r, keysLen), pos)
		}
		return 0
	}
	if pos >= keysLen && link != 0 {
		return int64(link)
	}
	return 0
}

// OnNull: a node recid resolving to a null record is a broken tree.
func (a *bufferGetAction[K, V]) OnNull() int64 {
	panic(mapdb5.NewCorruption("buffer-tree node record is null"))
}

// doGet is the push-down lookup; also the point read the CAS mutators run
// INSIDE writeMu (it takes no writer lock).
func (m *BufferTreeMap[K, V]) doGet(key K) (V, bool) {
	a := bufferGetAction[K, V]{in: m.inner, key: key}
	cyc := newCycleGuard(cycleDescentSoft)
	current := m.rootRecid()
	for current != 0 {
		cyc.visit(current)
		current = uint64(m.inner.st.Read(recidOrCorrupt(current), &a))
	}
	return a.value, a.found
}

// Get returns the value for key.
func (m *BufferTreeMap[K, V]) Get(key K) (V, bool) {
	release := m.opGuard()
	defer release()
	return m.doGet(key)
}

// ContainsKey reports whether key is present.
func (m *BufferTreeMap[K, V]) ContainsKey(key K) bool {
	_, ok := m.Get(key)
	return ok
}

// ===================== iteration: read-only pruned DFS merge =====================

type btFrame[K, V any] struct {
	children    []uint64
	batches     [][]btOp[K, V]
	idx         int
	first, last int
}

// leafInRange materializes a covering leaf's in-range live entries
// (base ∪ ops, LWW, tombstones dropped), in emit order.
func (in *bufferTreeInner[K, V]) leafInRange(n *bnode[K, V], all []btOp[K, V], lo, hi bound[K], desc bool) []kvPair[K, V] {
	keys, vals := in.applyOps(n.keys, n.values, all)
	cnt := in.kf.Size(keys)
	out := make([]kvPair[K, V], 0, cnt)
	for i := 0; i < cnt; i++ {
		k := in.kf.Get(keys, i)
		if in.inRange(k, lo, hi) {
			out = append(out, kvPair[K, V]{k: k, v: in.vf.Get(vals, i)})
		}
	}
	if desc {
		slices.Reverse(out)
	}
	return out
}

func (in *bufferTreeInner[K, V]) inRange(k K, lo, hi bound[K]) bool {
	return in.codec.InRange(k, toFmtBound(lo), toFmtBound(hi))
}

func toFmtBound[K any](b bound[K]) format.Bound[K] {
	return format.Bound[K]{Key: b.key, Set: b.set, Incl: b.incl}
}

func (in *bufferTreeInner[K, V]) filterRange(ops []btOp[K, V], lo, hi bound[K]) []btOp[K, V] {
	if (!lo.set && !hi.set) || len(ops) == 0 {
		return ops
	}
	out := make([]btOp[K, V], 0, len(ops))
	for _, op := range ops {
		if in.inRange(op.key, lo, hi) {
			out = append(out, op)
		}
	}
	return out
}

// restartSpinLimit bounds iterator re-descents that fail with NO structural
// progress (the root pointer naming a superseded root while no split is in
// flight) — only a persisted broken tree does that. Waiting on an in-flight
// split is unbounded by design: it ends in publication or in poison.
const restartSpinLimit = 1 << 20

// iterRange is the bounded pruned DFS (Java prunedRangeIterator +
// single-leaf byte-side fast path): descends ONLY children whose routing
// band covers [lo, hi], range-filters inherited ops before partitioning,
// materializes each covering leaf once. Admission covers each node load;
// it is released before entries are yielded, and every resumed yield
// re-checks the handle (a retired map stops emitting).
//
// Exactly-once for pre-existing entries (a port fix; Java's DFS shares the
// gap): the DFS does not follow sibling links, so a parent or root snapshot
// taken before a concurrent split would route past the split-off right
// parts. Every node load is bracketed by the structural version; on any
// change the walk discards its frame stack and re-descends from a fresh
// root, resuming strictly after the last emitted key.
func (m *BufferTreeMap[K, V]) iterRange(lo, hi bound[K], desc bool) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.checkOpen()
		lo, hi := lo, hi // per-run copies: advanced as the resume point
		in := m.inner
		var stack []*btFrame[K, V]
		var leaf []kvPair[K, V]
		var ver uint64
		changed := func() bool { return in.structVer.Load() != ver }

		// descend walks one path down; false = structure changed (restart).
		descend := func(recid uint64, inherited []btOp[K, V], isRoot bool) bool {
			release := m.opGuard()
			defer release()
			cyc := newCycleGuard(cycleDescentSoft)
			for {
				cyc.visit(recid)
				n := m.load(recid)
				if changed() {
					return false
				}
				if isRoot && n.flags&(flagLeft|flagRight) != flagLeft|flagRight {
					// a superseded root (left fragment of a split whose
					// root-grow is not yet published): retry
					return false
				}
				isRoot = false
				all := in.filterRange(combineOps(n.ops, inherited), lo, hi)
				if !n.isDir() {
					leaf = in.leafInRange(&n, all, lo, hi, desc)
					return true
				}
				cc := len(n.children)
				batches := in.partition(&n, all)
				first, last := 0, cc-1
				if lo.set {
					first = in.route(n.keys, lo.key, cc)
				}
				if hi.set {
					last = in.route(n.keys, hi.key, cc)
				}
				if first > last {
					leaf = nil
					return true
				}
				startCi, next := first, first+1
				if desc {
					startCi, next = last, last-1
				}
				stack = append(stack, &btFrame[K, V]{children: n.children, batches: batches, idx: next, first: first, last: last})
				recid = n.children[startCi]
				inherited = batches[startCi]
			}
		}

		// restart begins a fresh walk; false = range exhausted.
		restart := func() {
			waits := 0 // backoff counter
			stuck := 0 // failed re-descents with NO structural progress
			for {
				stack, leaf = nil, nil
				ver = in.structVer.Load()
				if ver&1 == 0 {
					if single, ok := m.byteRangeSingleLeaf(lo, hi, desc); ok && !changed() {
						leaf = single
						return
					}
					if descend(m.rootRecidGuarded(), nil, true) {
						return
					}
					// Re-descent failed. If the version did not move, the
					// root pointer names a superseded root with no split in
					// flight — only a persisted broken tree does that.
					if !changed() {
						if stuck++; stuck > restartSpinLimit {
							panic(mapdb5.NewCorruption("buffer-tree root pointer names a superseded root"))
						}
					} else {
						stuck = 0
					}
				}
				// A split is in flight (odd version) or just changed: yield
				// to the writer. It ends in publication or in poison, which
				// checkOpen reports.
				if waits++; waits > 64 {
					time.Sleep(leftEdgeSpin)
				} else {
					runtime.Gosched()
				}
				m.checkOpen()
			}
		}

		restart()
		for {
			for _, e := range leaf {
				m.checkOpen()
				if !yield(e.k, e.v) {
					return
				}
				// resume point after a restart: strictly past e.k
				if desc {
					hi = bound[K]{key: e.k, set: true}
				} else {
					lo = bound[K]{key: e.k, set: true}
				}
			}
			leaf = nil
			for leaf == nil {
				if m.rangeEmpty(lo, hi) {
					return
				}
				if changed() {
					restart()
					break
				}
				if len(stack) == 0 {
					return
				}
				f := stack[len(stack)-1]
				more := f.idx <= f.last
				if desc {
					more = f.idx >= f.first
				}
				if !more {
					stack = stack[:len(stack)-1]
					continue
				}
				ci := f.idx
				if desc {
					f.idx--
				} else {
					f.idx++
				}
				if !descend(f.children[ci], f.batches[ci], false) {
					restart()
					break
				}
				if leaf == nil {
					leaf = []kvPair[K, V]{} // empty covering leaf: keep walking
				}
			}
		}
	}
}

func (m *BufferTreeMap[K, V]) rootRecidGuarded() uint64 {
	release := m.opGuard()
	defer release()
	return m.rootRecid()
}

// rangeLeafReader produces a single leaf's in-range entries entirely from
// page bytes: a byte-side base cursor over only the in-range slice merged
// with the bounded log shadow — no base materialization (Java
// RangeLeafReader).
type rangeLeafReader[K, V any] struct {
	store.RecordReadBase
	in     *bufferTreeInner[K, V]
	lo, hi bound[K]
	isDir  bool
	out    []kvPair[K, V]
}

func (a *rangeLeafReader[K, V]) OnBytes(r *bin.Reader, size int) int64 {
	in := a.in
	start := r.Pos()
	if size < 4 {
		panic(mapdb5.NewCorruption("buffer-tree record too short for node header"))
	}
	baseLen := int(r.ReadInt())
	if baseLen < 0 || baseLen > size-4 {
		panic(mapdb5.NewCorruption("torn baseLen"))
	}
	var hdr format.BaseHeader
	in.codec.ParseBaseHeader(r, start+4, baseLen, &hdr)
	r.Seek(hdr.NodeBase)
	h := r.UnpackInt()
	flags := h & 0xF
	keysLen := int(uint32(h) >> 4)
	if flags&flagDir != 0 || flags&(flagLeft|flagRight) != flagLeft|flagRight {
		// a dir root, or a superseded root fragment: take the DFS path
		a.isDir = true
		return 0
	}
	if keysLen > baseLen {
		panic(mapdb5.NewCorruption("torn keysLen"))
	}
	if flags&flagRight == 0 {
		r.UnpackLong() // link
	}
	keyGroupStart := r.Pos()
	startIdx, endIdx, valueGroupStart := 0, 0, keyGroupStart
	if keysLen > 0 {
		if a.lo.set {
			idx, found := in.kf.BytesSearch(r, keysLen, a.lo.key)
			startIdx = idx
			if found && !a.lo.incl {
				startIdx++
			}
		} else {
			in.kf.BytesGet(r, keysLen, 0) // advance past the key group
		}
		valueGroupStart = r.Pos()
		endIdx = keysLen
		if a.hi.set {
			r.Seek(keyGroupStart)
			idx, found := in.kf.BytesSearch(r, keysLen, a.hi.key)
			endIdx = idx
			if found && a.hi.incl {
				endIdx++
			}
		}
		endIdx = max(endIdx, startIdx)
	}
	shadow := format.NewShadow[K, V](in.kf)
	in.codec.LogShadowInRange(r, start+4+baseLen, start+size, toFmtBound(a.lo), toFmtBound(a.hi), math.MaxInt64, shadow)
	cur := &leafBaseCursor[K, V]{in: in, r: r, keyGroupStart: keyGroupStart, valueGroupStart: valueGroupStart, keysLen: keysLen, idx: startIdx, endIdx: endIdx}
	in.codec.Merge(cur, shadow, func(k K, v V) bool {
		a.out = append(a.out, kvPair[K, V]{k: k, v: v})
		return true
	})
	return 0
}

func (a *rangeLeafReader[K, V]) OnNull() int64 {
	panic(mapdb5.NewCorruption("buffer-tree node record is null"))
}

// leafBaseCursor is the byte-side ascending cursor over a leaf base's
// in-range slice [idx, endIdx).
type leafBaseCursor[K, V any] struct {
	in                                      *bufferTreeInner[K, V]
	r                                       *bin.Reader
	keyGroupStart, valueGroupStart, keysLen int
	idx, endIdx                             int
	curKey                                  K
	curVal                                  V
}

func (c *leafBaseCursor[K, V]) HasNext() bool { return c.idx < c.endIdx }
func (c *leafBaseCursor[K, V]) Next() {
	c.r.Seek(c.keyGroupStart)
	c.curKey = c.in.kf.BytesGet(c.r, c.keysLen, c.idx)
	c.r.Seek(c.valueGroupStart)
	c.curVal = c.in.vf.BytesGet(c.r, c.keysLen, c.idx)
	c.idx++
}
func (c *leafBaseCursor[K, V]) Key() K   { return c.curKey }
func (c *leafBaseCursor[K, V]) Value() V { return c.curVal }

// byteRangeSingleLeaf answers the range from the root's page bytes when
// the whole map is one leaf and both formats support binary access; ok is
// false when the caller must fall back to the pruned DFS.
func (m *BufferTreeMap[K, V]) byteRangeSingleLeaf(lo, hi bound[K], desc bool) ([]kvPair[K, V], bool) {
	in := m.inner
	if !in.kf.SupportsBinary() || !in.vf.SupportsBinary() {
		return nil, false
	}
	release := m.opGuard()
	defer release()
	a := rangeLeafReader[K, V]{in: in, lo: lo, hi: hi}
	in.st.Read(recidOrCorrupt(m.rootRecid()), &a)
	if a.isDir {
		return nil, false
	}
	if desc {
		slices.Reverse(a.out)
	}
	return a.out, true
}

// ===================== public read surface =====================

// All yields every entry ascending (weakly consistent; un-flushed buffers
// at every level are merged in).
func (m *BufferTreeMap[K, V]) All() iter.Seq2[K, V] {
	return m.iterRange(bound[K]{}, bound[K]{}, false)
}

// Keys yields every key ascending.
func (m *BufferTreeMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for k := range m.All() {
			if !yield(k) {
				return
			}
		}
	}
}

// Values yields every value in key order.
func (m *BufferTreeMap[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, v := range m.All() {
			if !yield(v) {
				return
			}
		}
	}
}

// ForEach invokes f on each entry ascending.
func (m *BufferTreeMap[K, V]) ForEach(f func(K, V)) {
	for k, v := range m.All() {
		f(k, v)
	}
}

// AnySatisfy reports whether predicate holds for at least one entry.
func (m *BufferTreeMap[K, V]) AnySatisfy(predicate func(K, V) bool) bool {
	for k, v := range m.All() {
		if predicate(k, v) {
			return true
		}
	}
	return false
}

// AllSatisfy reports whether predicate holds for every entry.
func (m *BufferTreeMap[K, V]) AllSatisfy(predicate func(K, V) bool) bool {
	for k, v := range m.All() {
		if !predicate(k, v) {
			return false
		}
	}
	return true
}

// NoneSatisfy reports whether predicate holds for no entry.
func (m *BufferTreeMap[K, V]) NoneSatisfy(predicate func(K, V) bool) bool {
	return !m.AnySatisfy(predicate)
}

// LenLong counts entries (a full DFS — there is no O(1) counter).
func (m *BufferTreeMap[K, V]) LenLong() int64 {
	var n int64
	for range m.All() {
		n++
	}
	return n
}

// Len is LenLong saturated at MaxInt.
func (m *BufferTreeMap[K, V]) Len() int {
	n := m.LenLong()
	if n > math.MaxInt {
		return math.MaxInt
	}
	return int(n)
}

// IsEmpty reports whether the map has no entries.
func (m *BufferTreeMap[K, V]) IsEmpty() bool {
	for range m.All() {
		return false
	}
	return true
}

// Clear removes every entry via blind tombstones. Snapshots the key set
// first: a tombstone append can trigger a flush/split.
func (m *BufferTreeMap[K, V]) Clear() {
	var keys []K
	for k := range m.All() {
		keys = append(keys, k)
	}
	for _, k := range keys {
		m.RemoveOnly(k)
	}
}

func (m *BufferTreeMap[K, V]) rangeEmpty(lo, hi bound[K]) bool {
	if lo.set && hi.set {
		c := m.inner.kf.Compare(lo.key, hi.key)
		return c > 0 || (c == 0 && !(lo.incl && hi.incl))
	}
	return false
}

func (m *BufferTreeMap[K, V]) firstOf(lo, hi bound[K], desc bool) (K, V, bool) {
	if !m.rangeEmpty(lo, hi) {
		for k, v := range m.iterRange(lo, hi, desc) {
			return k, v, true
		}
	}
	var zk K
	var zv V
	return zk, zv, false
}

// Min returns the least entry.
func (m *BufferTreeMap[K, V]) Min() (K, V, bool) { return m.firstOf(bound[K]{}, bound[K]{}, false) }

// Max returns the greatest entry.
func (m *BufferTreeMap[K, V]) Max() (K, V, bool) { return m.firstOf(bound[K]{}, bound[K]{}, true) }

// FirstEntry is Min (Java firstEntry).
func (m *BufferTreeMap[K, V]) FirstEntry() (K, V, bool) { return m.Min() }

// LastEntry is Max (Java lastEntry).
func (m *BufferTreeMap[K, V]) LastEntry() (K, V, bool) { return m.Max() }

// Floor returns the greatest entry with key <= key.
func (m *BufferTreeMap[K, V]) Floor(key K) (K, V, bool) {
	return m.firstOf(bound[K]{}, bound[K]{key: key, set: true, incl: true}, true)
}

// Ceiling returns the least entry with key >= key.
func (m *BufferTreeMap[K, V]) Ceiling(key K) (K, V, bool) {
	return m.firstOf(bound[K]{key: key, set: true, incl: true}, bound[K]{}, false)
}

// Lower returns the greatest entry with key < key.
func (m *BufferTreeMap[K, V]) Lower(key K) (K, V, bool) {
	return m.firstOf(bound[K]{}, bound[K]{key: key, set: true}, true)
}

// Higher returns the least entry with key > key.
func (m *BufferTreeMap[K, V]) Higher(key K) (K, V, bool) {
	return m.firstOf(bound[K]{key: key, set: true}, bound[K]{}, false)
}

// HeadMap yields entries with keys strictly less than toKey.
func (m *BufferTreeMap[K, V]) HeadMap(toKey K) iter.Seq2[K, V] {
	return m.iterRange(bound[K]{}, bound[K]{key: toKey, set: true}, false)
}

// TailMap yields entries with keys >= fromKey.
func (m *BufferTreeMap[K, V]) TailMap(fromKey K) iter.Seq2[K, V] {
	return m.iterRange(bound[K]{key: fromKey, set: true, incl: true}, bound[K]{}, false)
}

// SubMap yields entries with keys in [fromKey, toKey).
func (m *BufferTreeMap[K, V]) SubMap(fromKey, toKey K) iter.Seq2[K, V] {
	lo, hi := bound[K]{key: fromKey, set: true, incl: true}, bound[K]{key: toKey, set: true}
	if m.rangeEmpty(lo, hi) {
		return func(func(K, V) bool) {}
	}
	return m.iterRange(lo, hi, false)
}

// RangeEntries yields entries within the given bounds, ascending or
// descending (Java inclusive-flag subMap / descendingMap views).
func (m *BufferTreeMap[K, V]) RangeEntries(from K, hasFrom, fromIncl bool, to K, hasTo, toIncl bool, descending bool) iter.Seq2[K, V] {
	lo := bound[K]{key: from, set: hasFrom, incl: fromIncl}
	hi := bound[K]{key: to, set: hasTo, incl: toIncl}
	if m.rangeEmpty(lo, hi) {
		return func(func(K, V) bool) {}
	}
	return m.iterRange(lo, hi, descending)
}

// DescendingMap yields every entry descending.
func (m *BufferTreeMap[K, V]) DescendingMap() iter.Seq2[K, V] {
	return m.iterRange(bound[K]{}, bound[K]{}, true)
}

// DescendingKeys yields every key descending.
func (m *BufferTreeMap[K, V]) DescendingKeys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for k := range m.DescendingMap() {
			if !yield(k) {
				return
			}
		}
	}
}

// Rank returns the number of entries with key strictly less than key —
// O(range) by bounded iteration (no order-statistic augmentation; spec 03).
func (m *BufferTreeMap[K, V]) Rank(key K) int {
	n := 0
	for range m.iterRange(bound[K]{}, bound[K]{key: key, set: true}, false) {
		if n < math.MaxInt {
			n++
		}
	}
	return n
}

// SelectKey returns the i-th key ascending (0-based), O(i).
func (m *BufferTreeMap[K, V]) SelectKey(i int) (K, bool) {
	k, _, ok := m.SelectEntry(i)
	return k, ok
}

// SelectEntry returns the i-th entry ascending (0-based), O(i).
func (m *BufferTreeMap[K, V]) SelectEntry(i int) (K, V, bool) {
	if i >= 0 {
		idx := 0
		for k, v := range m.All() {
			if idx == i {
				return k, v, true
			}
			idx++
		}
	}
	var zk K
	var zv V
	return zk, zv, false
}

// PollFirstEntry atomically removes and returns the least entry: the find
// and the private tombstone append share one writer critical section, so it
// never removes a value it did not return.
func (m *BufferTreeMap[K, V]) PollFirstEntry() (K, V, bool) { return m.poll(false) }

// PollLastEntry atomically removes and returns the greatest entry.
func (m *BufferTreeMap[K, V]) PollLastEntry() (K, V, bool) { return m.poll(true) }

func (m *BufferTreeMap[K, V]) poll(desc bool) (k K, v V, ok bool) {
	m.locked(func() {
		// The candidate iterator is fully consumed (one entry, then
		// discarded) BEFORE the tombstone append can flush/split.
		k, v, ok = m.firstOf(bound[K]{}, bound[K]{}, desc)
		if ok {
			m.appendMessage(btOp[K, V]{key: k, del: true})
		}
	})
	return
}

// ===================== verify =====================

// Verify walks the whole tree checking structural invariants (Java has no
// BufferTreeMap verify; this is the port's TCK hook): every node decodes,
// every delta frame passes its CRC32C, keys are strictly ascending, each
// node's keys AND buffered ops lie within the key band its parent routes to
// it (the coverage invariant), dir shapes match their RIGHT flag, recids are
// reached once, and the rightmost spine carries RIGHT. Panics
// KindVerifyFailed (or DataCorruption from decoding).
func (m *BufferTreeMap[K, V]) Verify() {
	release := m.opGuard()
	defer release()
	in := m.inner
	fail := func(msg string) { panic(mapdb5.New(mapdb5.KindVerifyFailed, msg)) }
	seen := map[uint64]bool{}
	// per-depth node order (left→right) with each node's link, to check
	// the B-link sibling chains.
	type lvlNode struct{ recid, link uint64 }
	var levels [][]lvlNode
	var walk func(recid uint64, lo, hi bound[K], rightmost bool, depth int)
	walk = func(recid uint64, lo, hi bound[K], rightmost bool, depth int) {
		if seen[recid] {
			fail("buffer-tree node reachable twice")
		}
		seen[recid] = true
		n := m.load(recid)
		for len(levels) <= depth {
			levels = append(levels, nil)
		}
		levels[depth] = append(levels[depth], lvlNode{recid, n.link})
		in.st.Read(recid, &crcVerifyAction[K, V]{in: in, fail: fail})
		if n.isRight() != rightmost {
			fail("buffer-tree RIGHT flag does not match the rightmost spine")
		}
		for i := 0; i < in.kf.Size(n.keys); i++ {
			if !in.inRange(in.kf.Get(n.keys, i), lo, hi) {
				fail("buffer-tree node key outside its parent's routing band")
			}
		}
		for _, op := range n.ops {
			if !in.inRange(op.key, lo, hi) {
				fail("buffer-tree buffered op outside its node's coverage")
			}
		}
		if !n.isDir() {
			return
		}
		cc := len(n.children)
		for ci, c := range n.children {
			clo, chi := lo, hi
			if ci > 0 {
				clo = bound[K]{key: in.kf.Get(n.keys, ci-1), set: true}
			}
			if ci < in.kf.Size(n.keys) {
				chi = bound[K]{key: in.kf.Get(n.keys, ci), set: true, incl: true}
			}
			walk(c, clo, chi, rightmost && ci == cc-1, depth+1)
		}
	}
	walk(m.rootRecid(), bound[K]{}, bound[K]{}, true, 0)
	for _, lv := range levels {
		for i, nd := range lv {
			want := uint64(0)
			if i+1 < len(lv) {
				want = lv[i+1].recid
			}
			if nd.link != want {
				fail("buffer-tree sibling link chain broken")
			}
		}
	}
}

// crcVerifyAction re-checks every delta frame's CRC32C in place.
type crcVerifyAction[K, V any] struct {
	store.RecordReadBase
	in   *bufferTreeInner[K, V]
	fail func(string)
}

func (a *crcVerifyAction[K, V]) OnBytes(r *bin.Reader, size int) int64 {
	start := r.Pos()
	baseLen := int(r.ReadInt())
	if baseLen < 0 || baseLen > size-4 {
		a.fail("torn baseLen")
	}
	pos, end := start+4+baseLen, start+size
	for pos < end {
		r.Seek(pos)
		tl := int(r.ReadInt())
		if tl < format.MinEntryBytes || tl > end-pos {
			a.fail("torn delta entry")
		}
		if !a.in.codec.VerifyEntry(r, pos, tl) {
			a.fail("buffer-tree delta frame CRC mismatch")
		}
		pos += tl
	}
	return 0
}

// ser.Serializer conformance for the node codec (compile-time).
var _ ser.Serializer[bnode[int64, int64]] = bnodeSerializer[int64, int64]{}
