package btree

import (
	"iter"
	"math"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// kvPair is one materialized entry held by the descending leaf batches.
type kvPair[K, V any] struct {
	k K
	v V
}

// revFrame is one retained descending-descent frame: (recid, dir-node
// snapshot, entry separator) — the subtree below holds only keys strictly
// above the separator (hasSep=false = unbounded left edge).
type revFrame[K any] struct {
	recid  uint64
	n      node
	sep    K
	hasSep bool
}

// ===================== ascending range iteration =====================

// iterRange is the bounded ascending entry iterator (weakly consistent
// leaf-link walk; no locks held across yields).
func (m *BTreeMap[K, V]) iterRange(lo, hi bound[K]) iter.Seq2[K, V] {
	m.checkOpen()
	if m.inner.valueInline {
		return m.iterRangeInline(lo, hi)
	}
	return m.iterRangeExternal(lo, hi)
}

// iterRangeInline retains and drains one decoded leaf at a time — the leaf
// snapshot is a COW-published group set, safe to hold across user yields.
// Every RESUME re-checks the handle (a retired map must not keep emitting)
// and each leaf-link hop runs inside an op admission so retirement drains
// it before the lease can be released.
func (m *BTreeMap[K, V]) iterRangeInline(lo, hi bound[K]) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		var loKey *K
		if lo.set {
			loKey = &lo.key
		}
		cyc := newCycleGuard(cycleScanSoft)
		leaf := func() node {
			release := m.opGuard()
			defer release()
			return m.firstLeafForLowerBound(loKey)
		}()
		pos := m.leafStartPos(&leaf, lo)
		loPending := lo.set
		for {
			for pos >= m.kf().Size(leaf.keys) {
				link := leaf.link
				if link == 0 {
					return
				}
				cyc.visit(link)
				func() {
					release := m.opGuard()
					defer release()
					leaf = m.load(link)
				}()
				pos = 0
			}
			k := m.kf().Get(leaf.keys, pos)
			if loPending {
				if m.belowLo(k, lo) {
					pos++
					continue
				}
				loPending = false
			}
			if m.aboveHi(k, hi) {
				return
			}
			v := m.vf().Get(leaf.values, pos)
			pos++
			if !yield(k, v) {
				return
			}
			m.checkOpen()
		}
	}
}

// iterRangeExternal takes the reclamation read barrier per ENTRY: each pull
// freshly resumes from the last emitted key and expands exactly one value
// before releasing the barrier (the Java reference algorithm — concurrent
// removes/updates between pulls are observed, and a closed store errors on
// the next pull).
func (m *BTreeMap[K, V]) iterRangeExternal(lo, hi bound[K]) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		resume := lo
		for {
			var k K
			var v V
			var ok bool
			func() {
				release := m.opGuard()
				defer release()
				m.checkExternalReentry()
				m.inner.externalLock.RLock()
				defer m.inner.externalLock.RUnlock()
				k, v, ok = m.firstExternalEntry(resume, hi)
			}()
			if !ok {
				return
			}
			if !yield(k, v) {
				return
			}
			// Resume EXCLUSIVELY from the emitted key; inserts behind it are
			// skipped, inserts ahead may be observed (weak consistency).
			resume = bound[K]{key: k, set: true, incl: false}
		}
	}
}

// firstExternalEntry returns the first external-value entry satisfying the
// bounds. The caller MUST hold externalLock.RLock across this entire call.
// Deliberately one FRESH entry per call (the Java reference algorithm).
func (m *BTreeMap[K, V]) firstExternalEntry(lo, hi bound[K]) (K, V, bool) {
	cyc := newCycleGuard(cycleScanSoft)
	var loKey *K
	if lo.set {
		loKey = &lo.key
	}
	leaf := m.firstLeafForLowerBound(loKey)
	pos := m.leafStartPos(&leaf, lo)
	loPending := lo.set
	for {
		for pos >= m.kf().Size(leaf.keys) {
			link := leaf.link
			if link == 0 {
				var zv V
				return lo.key, zv, false
			}
			cyc.visit(link)
			leaf = m.load(link)
			pos = 0
		}
		k := m.kf().Get(leaf.keys, pos)
		if loPending && m.belowLo(k, lo) {
			pos++
			continue
		}
		if m.aboveHi(k, hi) {
			var zv V
			return k, zv, false
		}
		v := m.leafValueAt(&leaf, pos)
		return k, v, true
	}
}

// ===================== descending range iteration =====================

// lastLeafBatch performs one DESCENDING scan step. It descends to the leaf
// covering the current upper bound and returns (batch, next): batch = that
// leaf's in-range entries in ASCENDING order (drain from the back), next =
// the upper bound for the following step (set=false when exhausted).
//
// Navigation left of a leaf needs no back-links: while routing down, every
// time the descent passes a dir child i > 0 (or follows a B-link right),
// the separator immediately to the LEFT of the taken branch is recorded;
// the tightest such separator is the greatest possible key strictly below
// the reached leaf's coverage.
//
// stack retains the descent path across steps; retained frames are stale
// snapshots — exactly the weak consistency the ascending iterator's
// retained leaf links already have.
//
// External-value maps: the CALLER must hold externalLock.RLock across the
// whole call — values are expanded before return, so the batch owns plain
// values and never retains recids past the barrier.
func (m *BTreeMap[K, V]) lastLeafBatch(stack *[]revFrame[K], lo, hi bound[K]) ([]kvPair[K, V], bound[K]) {
	cyc := newCycleGuard(cycleDescentSoft)
	// Drop the path frames the tightened bound has moved left of. A frame
	// covers only keys strictly ABOVE its effective lower bound — its own
	// entry separator, or for a child-0 frame the deepest ancestor
	// separator. So pop by the deepest retained Some: if the bound is not
	// strictly above it, that frame and every deeper (inheriting) frame are
	// all left of the bound.
	if !hi.set {
		*stack = (*stack)[:0]
	} else {
		for {
			deepest := -1
			for i := len(*stack) - 1; i >= 0; i-- {
				if (*stack)[i].hasSep {
					deepest = i
					break
				}
			}
			if deepest < 0 {
				break // only left-edge frames remain: they cover (-inf,..]
			}
			if m.kf().Compare(hi.key, (*stack)[deepest].sep) > 0 {
				break
			}
			*stack = (*stack)[:deepest]
		}
	}
	if len(*stack) == 0 {
		root := m.rootRecid()
		*stack = append(*stack, revFrame[K]{recid: root, n: m.load(root)})
	}
	// Tightest separator left of the (partially retained) descent path.
	var stepDown bound[K]
	for i := len(*stack) - 1; i >= 0; i-- {
		if (*stack)[i].hasSep {
			stepDown = bound[K]{key: (*stack)[i].sep, set: true, incl: true}
			break
		}
	}
	// Descend from the deepest surviving frame to the covering leaf. Dir
	// frames are pushed; the leaf itself is never retained.
	var n node
	for {
		top := &(*stack)[len(*stack)-1]
		if !top.n.isDir() {
			// Single-node tree: the root is a leaf; process it directly.
			n = top.n
			*stack = (*stack)[:len(*stack)-1]
			break
		}
		recid := top.recid
		nkeys := m.kf().Size(top.n.keys)
		var lastKey bound[K]
		if nkeys > 0 {
			lastKey = bound[K]{key: m.kf().Get(top.n.keys, nkeys-1), set: true, incl: true}
		}
		var idx int
		switch {
		case !hi.set && top.n.link != 0:
			idx = -1 // rightmost path: ride links right
		case !hi.set:
			idx = len(top.n.children) - 1
			if idx < 0 {
				panic(mapdb5.NewCorruption("dir node with no children"))
			}
		default:
			idx = m.routeChild(&top.n, &hi.key)
		}
		cyc.visit(recid)
		if idx < 0 {
			// B-link right: the sibling REPLACES this frame at the same
			// level; its subtree holds keys above this node's last key.
			if lastKey.set {
				stepDown = lastKey
			}
			link := top.n.link
			sib := m.load(link)
			*stack = (*stack)[:len(*stack)-1]
			*stack = append(*stack, revFrame[K]{recid: link, n: sib, sep: lastKey.key, hasSep: lastKey.set})
			continue
		}
		var sep bound[K]
		if idx > 0 {
			sep = bound[K]{key: m.kf().Get(top.n.keys, idx-1), set: true, incl: true}
			stepDown = sep
		}
		childRecid := top.n.children[idx]
		child := m.load(childRecid)
		if child.isDir() {
			*stack = append(*stack, revFrame[K]{recid: childRecid, n: child, sep: sep.key, hasSep: sep.set})
		} else {
			n = child
			goto leafFound
		}
	}
leafFound:
	// Leaf level: ride links right while the bound is beyond this leaf's
	// fence (concurrent split), or all the way for an open upper bound.
	for {
		follow := n.link != 0 && (!hi.set || m.beyondLeaf(&n, &hi.key))
		if !follow {
			break
		}
		sz := m.kf().Size(n.keys)
		if sz > 0 {
			stepDown = bound[K]{key: m.kf().Get(n.keys, sz-1), set: true, incl: true}
		}
		cyc.visit(n.link)
		n = m.load(n.link)
	}
	sz := m.kf().Size(n.keys)
	var batch []kvPair[K, V]
	sawBelowLo := false
	for pos := 0; pos < sz; pos++ {
		k := m.kf().Get(n.keys, pos)
		if m.belowLo(k, lo) {
			sawBelowLo = true
			continue
		}
		if m.aboveHi(k, hi) {
			break
		}
		batch = append(batch, kvPair[K, V]{k: k, v: m.leafValueAt(&n, pos)})
	}
	// A key below lo in this leaf ⇒ every leaf further left is entirely
	// below lo: the scan is complete once this batch drains.
	if sawBelowLo {
		return batch, bound[K]{}
	}
	return batch, stepDown
}

// iterRangeDesc is the bounded descending entry iterator: one root descent
// per visited leaf, O(leaf) memory — never materializes the whole range.
// The upper bound tightens strictly every step (enforced — a mangled tree
// yields corrupt, never a loop). External-value maps take the reclamation
// read barrier once per LEAF batch; it is released before entries reach
// user code.
func (m *BTreeMap[K, V]) iterRangeDesc(lo, hi bound[K]) iter.Seq2[K, V] {
	m.checkOpen()
	return func(yield func(K, V) bool) {
		var stack []revFrame[K]
		var buf []kvPair[K, V]
		upper := hi
		upperOpen := !hi.set
		done := false
		for {
			for len(buf) > 0 {
				m.checkOpen() // a retired handle must not keep emitting
				e := buf[len(buf)-1]
				buf = buf[:len(buf)-1]
				if !yield(e.k, e.v) {
					return
				}
			}
			if done {
				return
			}
			var effHi bound[K]
			if !upperOpen {
				if !upper.set {
					return // non-open scan with no bound left: exhausted
				}
				effHi = upper
			}
			var batch []kvPair[K, V]
			var next bound[K]
			func() {
				release := m.opGuard()
				defer release()
				if m.inner.valueInline {
					batch, next = m.lastLeafBatch(&stack, lo, effHi)
				} else {
					m.checkExternalReentry()
					m.inner.externalLock.RLock()
					defer m.inner.externalLock.RUnlock()
					batch, next = m.lastLeafBatch(&stack, lo, effHi)
				}
			}()
			buf = batch
			if !next.set {
				done = true
			} else {
				// The next bound must strictly decrease, or a corrupt tree
				// could loop or re-emit entries forever.
				if !upperOpen && upper.set && m.kf().Compare(next.key, upper.key) >= 0 {
					panic(mapdb5.NewCorruption("descending scan bound did not decrease"))
				}
				upper = bound[K]{key: next.key, set: true, incl: true}
				upperOpen = false
			}
			if upperOpen {
				// First step of an open scan reached the rightmost leaf and
				// recorded a real bound (or finished); never repeat the
				// open descent.
				upperOpen = false
			}
		}
	}
}

// ===================== bounded first/last primitives =====================

func (m *BTreeMap[K, V]) firstOf(lo, hi bound[K]) (K, V, bool) {
	if m.rangeEmpty(lo, hi) {
		var zk K
		var zv V
		return zk, zv, false
	}
	for k, v := range m.iterRange(lo, hi) {
		return k, v, true
	}
	var zk K
	var zv V
	return zk, zv, false
}

func (m *BTreeMap[K, V]) lastOf(lo, hi bound[K]) (K, V, bool) {
	if m.rangeEmpty(lo, hi) {
		var zk K
		var zv V
		return zk, zv, false
	}
	for k, v := range m.iterRangeDesc(lo, hi) {
		return k, v, true
	}
	var zk K
	var zv V
	return zk, zv, false
}

// rangeEmpty: true when [lo,hi] covers NO key — inverted or equal endpoints
// with either side exclusive.
func (m *BTreeMap[K, V]) rangeEmpty(lo, hi bound[K]) bool {
	if lo.set && hi.set {
		c := m.kf().Compare(lo.key, hi.key)
		if c > 0 {
			return true
		}
		if c == 0 {
			return !(lo.incl && hi.incl)
		}
	}
	return false
}

// ===================== public map surface =====================

// All yields every entry ascending (weakly consistent).
func (m *BTreeMap[K, V]) All() iter.Seq2[K, V] {
	return m.iterRange(bound[K]{}, bound[K]{})
}

// Keys yields every key ascending.
func (m *BTreeMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for k := range m.All() {
			if !yield(k) {
				return
			}
		}
	}
}

// Values yields every value in key order.
func (m *BTreeMap[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, v := range m.All() {
			if !yield(v) {
				return
			}
		}
	}
}

// ForEach invokes f on each entry ascending.
func (m *BTreeMap[K, V]) ForEach(f func(K, V)) {
	for k, v := range m.All() {
		f(k, v)
	}
}

// AnySatisfy reports whether predicate holds for at least one entry.
func (m *BTreeMap[K, V]) AnySatisfy(predicate func(K, V) bool) bool {
	for k, v := range m.All() {
		if predicate(k, v) {
			return true
		}
	}
	return false
}

// AllSatisfy reports whether predicate holds for every entry.
func (m *BTreeMap[K, V]) AllSatisfy(predicate func(K, V) bool) bool {
	for k, v := range m.All() {
		if !predicate(k, v) {
			return false
		}
	}
	return true
}

// NoneSatisfy reports whether predicate holds for no entry.
func (m *BTreeMap[K, V]) NoneSatisfy(predicate func(K, V) bool) bool {
	return !m.AnySatisfy(predicate)
}

// Min returns the least entry.
func (m *BTreeMap[K, V]) Min() (K, V, bool) {
	return m.firstOf(bound[K]{}, bound[K]{})
}

// Max returns the greatest entry.
func (m *BTreeMap[K, V]) Max() (K, V, bool) {
	return m.lastOf(bound[K]{}, bound[K]{})
}

// FirstEntry is Min (Java firstEntry).
func (m *BTreeMap[K, V]) FirstEntry() (K, V, bool) { return m.Min() }

// LastEntry is Max (Java lastEntry).
func (m *BTreeMap[K, V]) LastEntry() (K, V, bool) { return m.Max() }

// Floor returns the greatest entry with key <= key.
func (m *BTreeMap[K, V]) Floor(key K) (K, V, bool) {
	return m.lastOf(bound[K]{}, bound[K]{key: key, set: true, incl: true})
}

// Ceiling returns the least entry with key >= key.
func (m *BTreeMap[K, V]) Ceiling(key K) (K, V, bool) {
	return m.firstOf(bound[K]{key: key, set: true, incl: true}, bound[K]{})
}

// Lower returns the greatest entry with key < key.
func (m *BTreeMap[K, V]) Lower(key K) (K, V, bool) {
	return m.lastOf(bound[K]{}, bound[K]{key: key, set: true, incl: false})
}

// Higher returns the least entry with key > key.
func (m *BTreeMap[K, V]) Higher(key K) (K, V, bool) {
	return m.firstOf(bound[K]{key: key, set: true, incl: false}, bound[K]{})
}

// HeadMap yields entries with keys strictly less than toKey (TreeMap
// parity; the entries are a live weakly-consistent view, not a snapshot).
func (m *BTreeMap[K, V]) HeadMap(toKey K) iter.Seq2[K, V] {
	return m.iterRange(bound[K]{}, bound[K]{key: toKey, set: true, incl: false})
}

// TailMap yields entries with keys >= fromKey.
func (m *BTreeMap[K, V]) TailMap(fromKey K) iter.Seq2[K, V] {
	return m.iterRange(bound[K]{key: fromKey, set: true, incl: true}, bound[K]{})
}

// SubMap yields entries with keys in [fromKey, toKey).
func (m *BTreeMap[K, V]) SubMap(fromKey, toKey K) iter.Seq2[K, V] {
	return m.iterRange(bound[K]{key: fromKey, set: true, incl: true}, bound[K]{key: toKey, set: true, incl: false})
}

// DescendingMap yields every entry descending.
func (m *BTreeMap[K, V]) DescendingMap() iter.Seq2[K, V] {
	return m.iterRangeDesc(bound[K]{}, bound[K]{})
}

// DescendingKeys yields every key descending.
func (m *BTreeMap[K, V]) DescendingKeys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for k := range m.DescendingMap() {
			if !yield(k) {
				return
			}
		}
	}
}

// Rank returns the number of entries with key strictly less than key —
// O(range): the disk B-link tree carries no order-statistic augmentation
// (Java BTreeMap has none either). Implemented by a bounded key walk.
func (m *BTreeMap[K, V]) Rank(key K) int {
	n := m.sizeLongRange(bound[K]{}, bound[K]{key: key, set: true, incl: false})
	if n > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(n)
}

// SelectKey returns the i-th key ascending (0-based), O(i) by bounded key
// walk.
func (m *BTreeMap[K, V]) SelectKey(i int) (K, bool) {
	k, _, ok := m.SelectEntry(i)
	return k, ok
}

// SelectEntry returns the i-th entry ascending (0-based), O(i).
func (m *BTreeMap[K, V]) SelectEntry(i int) (K, V, bool) {
	var zk K
	var zv V
	if i < 0 {
		return zk, zv, false
	}
	idx := 0
	for k, v := range m.All() {
		if idx == i {
			return k, v, true
		}
		idx++
	}
	return zk, zv, false
}

// PollFirstEntry atomically removes and returns the least entry — a CAS
// retry loop over the first ascending candidate + conditional remove, so
// poll never removes a value it did not return.
func (m *BTreeMap[K, V]) PollFirstEntry() (K, V, bool) {
	for {
		k, v, ok := m.Min()
		if !ok {
			var zk K
			var zv V
			return zk, zv, false
		}
		if m.RemoveIfEquals(k, v) {
			return k, v, true
		}
	}
}

// PollLastEntry atomically removes and returns the greatest entry.
func (m *BTreeMap[K, V]) PollLastEntry() (K, V, bool) {
	for {
		k, v, ok := m.Max()
		if !ok {
			var zk K
			var zv V
			return zk, zv, false
		}
		if m.RemoveIfEquals(k, v) {
			return k, v, true
		}
	}
}

// ===================== verify =====================

// verify walks the whole tree from the root checking structural invariants:
// every recid reachable exactly once (cycle guard), dir child counts match
// the RIGHT-flag shape, leaf-link chains cover exactly the leaf set found
// by descent, and the counter (when enabled) equals the real entry count.
func (m *BTreeMap[K, V]) verify() {
	release := m.opGuard()
	defer release()
	fail := func(msg string) {
		panic(mapdb5.New(mapdb5.KindVerifyFailed, msg))
	}
	rootRecid := m.loadRootRecid()
	root := m.load(rootRecid)
	if root.flags&(flagLeft|flagRight) != flagLeft|flagRight {
		fail("btree root is not root-shaped")
	}
	// Full descent collecting leaf recids (chain must match) and counting
	// reachable nodes (every visit guarded — a shared/cycled recid fails).
	visited := make(map[uint64]struct{})
	var descLeaves []uint64
	var walk func(recid uint64, depth int)
	walk = func(recid uint64, depth int) {
		if recid == 0 {
			fail("btree node recid is zero")
		}
		if _, dup := visited[recid]; dup {
			fail("btree node referenced twice (shared/cycled recid)")
		}
		visited[recid] = struct{}{}
		if depth > cycleDescentSoft {
			fail("btree depth exceeds sane bound")
		}
		n := m.load(recid)
		keysLen := m.kf().Size(n.keys)
		if n.isDir() {
			want := keysLen
			if n.isRight() {
				want++
			}
			if len(n.children) != want {
				fail("dir child count does not match keysLen/RIGHT shape")
			}
			for _, c := range n.children {
				if c == 0 {
					fail("dir child recid is zero")
				}
			}
			// separator coverage: every key must route to a valid child —
			// decode already enforced sortedness; children are checked by
			// recursion.
			for _, c := range n.children {
				walk(c, depth+1)
			}
			return
		}
		// leaf
		if n.isRight() && n.link != 0 {
			fail("rightmost leaf carries a link")
		}
		if !n.isRight() {
			if n.link == 0 {
				fail("non-rightmost leaf missing link")
			}
			if n.fence == nil {
				fail("non-rightmost leaf missing fence")
			} else if keysLen > 0 {
				last := m.kf().Get(n.keys, keysLen-1)
				bound := m.kf().Get(n.fence, 0)
				if m.kf().Compare(last, bound) > 0 {
					fail("leaf fence below greatest key")
				}
			}
		} else if n.fence != nil {
			fail("rightmost leaf carries a fence")
		}
		if m.inner.valueInline {
			if m.vf().Size(n.values) != keysLen {
				fail("leaf value count != keysLen")
			}
		} else if len(n.recids) != keysLen {
			fail("leaf recid count != keysLen")
		}
		descLeaves = append(descLeaves, recid)
	}
	walk(rootRecid, 0)

	// Leaf-chain walk from the leftmost edge must visit exactly the descent
	// leaf set, in left-to-right order.
	leftmost := m.firstLeafRecidForLowerBound(nil)
	cyc := newCycleGuard(cycleScanSoft)
	var chainLeaves []uint64
	var count int64
	for recid := leftmost; recid != 0; {
		cyc.visit(recid)
		n := m.load(recid)
		if n.isDir() {
			fail("leaf chain reached a directory node")
		}
		chainLeaves = append(chainLeaves, recid)
		count += int64(m.kf().Size(n.keys))
		recid = n.link
	}
	// The descent leaf set is unordered; compare as sets with counts.
	if len(chainLeaves) != len(descLeaves) {
		fail("leaf chain length != descent leaf count")
	}
	seen := make(map[uint64]struct{}, len(chainLeaves))
	for _, r := range chainLeaves {
		seen[r] = struct{}{}
	}
	for _, r := range descLeaves {
		if _, ok := seen[r]; !ok {
			fail("descent leaf missing from link chain")
		}
	}
	// Counter consistency.
	if m.inner.counterRecid != 0 {
		v, ok := store.Get(m.inner.st, m.inner.counterRecid, ser.Long)
		if !ok {
			fail("btree size counter record missing")
		}
		if v != count {
			fail("size counter does not match entry count")
		}
	}
}
