// Package btree implements the persistent B-link tree map (spec 03): a
// Lehman-Yao B-link tree over store.Store, ported from Java
// org.mapdb.btree.BTreeMap and the Rust port.
package btree

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// Node flag bits (mapdb3 lineage).
const (
	flagDir   int32 = 8
	flagLeft  int32 = 4
	flagRight int32 = 2
)

// varElemEst is the per-element byte budget for variable-width formats when
// sizing the serialization buffer (Java VAR_ELEM_EST).
const varElemEst = 32

// node is the immutable/copy-on-write tree node. Groups are opaque
// (ser.Group); only their owning format may touch them. Exactly one of
// children/values/recids is meaningful:
//   - dir:            children (len = keysLen+1 for RIGHT, keysLen otherwise)
//   - inline leaf:    values (the real values)
//   - external leaf:  recids (value record recids, 8-byte-BE on the wire)
//
// fence is the leaf's inclusive high bound — a 1-element key group present
// iff the leaf is not rightmost (never on dirs).
type node struct {
	flags    int32
	link     uint64 // right-sibling recid; 0 ⇔ flagRight
	keys     ser.Group
	children []uint64
	values   ser.Group
	recids   []int64
	fence    ser.Group
}

func (n *node) isDir() bool   { return n.flags&flagDir != 0 }
func (n *node) isRight() bool { return n.flags&flagRight != 0 }
func (n *node) isLeft() bool  { return n.flags&flagLeft != 0 }

// nodeSerializer is the ser.Serializer[node] for node records. Format
// parameters live here (nodes hold only groups). compare/equals are
// unreachable: the store never compares nodes.
type nodeSerializer[K, V any] struct {
	kf          ser.GroupFormat[K]
	vf          ser.GroupFormat[V]
	valueInline bool
	sizeHint    int
}

func newNodeSerializer[K, V any](kf ser.GroupFormat[K], vf ser.GroupFormat[V], maxNodeSize int, valueInline bool) nodeSerializer[K, V] {
	// header (packInt + packLong <= ~11 B, round to 16) + a full node's
	// key/value bytes + 2 spare key slots (pre-split transient + fence).
	ke := kf.Element().FixedSize()
	ve := ser.LongFormat.Element().FixedSize() // external leaves store 8-byte recids
	if valueInline {
		ve = vf.Element().FixedSize()
	}
	keyBytes := varElemEst
	if ke > 0 {
		keyBytes = ke
	}
	valBytes := varElemEst
	if ve > 0 {
		valBytes = max(ve, 9) // dir children are packed longs (<=9 B)
	}
	sizeHint := 16 + (maxNodeSize+1)*(keyBytes+valBytes) + 2*keyBytes + 8
	return nodeSerializer[K, V]{kf: kf, vf: vf, valueInline: valueInline, sizeHint: sizeHint}
}

// Wire format (mapdb3 lineage, byte-compatible with Java NodeSerializer):
//
//	packInt(keysLen<<4 | flags)
//	packLong(link)          iff !RIGHT
//	key group               keysLen elements
//	dir  → keysLen(+1 iff RIGHT) packed child recids
//	leaf → value group keysLen elements (values, or 8-byte-BE recids when
//	       external) + 1-element fence key group iff !RIGHT
//
// The fence sits last so the read path never decodes it.
func (s nodeSerializer[K, V]) Serialize(w *bin.Writer, n node) {
	keysLen := s.kf.Size(n.keys)
	w.PackInt(int32(keysLen)<<4 | n.flags)
	if !n.isRight() {
		w.PackLong(int64(n.link))
	}
	s.kf.SerializeGroup(w, n.keys)
	if n.isDir() {
		for _, c := range n.children {
			w.PackLong(int64(c))
		}
		return
	}
	if s.valueInline {
		s.vf.SerializeGroup(w, n.values)
	} else {
		ser.LongFormat.SerializeGroup(w, n.recids)
	}
	if !n.isRight() {
		if n.fence == nil {
			panic(mapdb5.NewCorruption("non-rightmost leaf without fence"))
		}
		s.kf.SerializeGroup(w, n.fence)
	}
}

// checkSorted rejects a decoded key group that is not strictly increasing —
// every writer/iterator path assumes it (D5).
func (s nodeSerializer[K, V]) checkSorted(keys ser.Group) {
	n := s.kf.Size(keys)
	if n == 0 {
		return
	}
	prev := s.kf.Get(keys, 0)
	for i := 1; i < n; i++ {
		cur := s.kf.Get(keys, i)
		if s.kf.Compare(prev, cur) >= 0 {
			panic(mapdb5.NewCorruption("node keys not strictly increasing"))
		}
		prev = cur
	}
}

func (s nodeSerializer[K, V]) Deserialize(r *bin.Reader, size int) node {
	// Record start so we can require exact record consumption below: a
	// crafted, checksum-valid header that under-reports keysLen shifts the
	// key/value boundary while leaving the record locally decodable — the
	// leftover trailing bytes are the tell (D5).
	start := r.Pos()
	h := r.UnpackInt()
	flags := h & 0xF
	keysLen := int(uint32(h) >> 4)
	// Every key occupies >= 1 serialized byte, so keysLen > size is corrupt.
	if size >= 0 && keysLen > size {
		panic(mapdb5.NewCorruption("node header keysLen exceeds record size"))
	}
	// Structural validation of persisted recid invariants (D5): a
	// non-rightmost node MUST carry a nonzero right link.
	var link uint64
	if flags&flagRight == 0 {
		l := r.UnpackLong()
		if l == 0 {
			panic(mapdb5.NewCorruption("non-rightmost node with zero link"))
		}
		link = uint64(l)
	}
	keys := s.kf.DeserializeGroup(r, keysLen)
	// Search, routing, and fence math all assume strictly-increasing keys;
	// a crafted unsorted node would cause silent false-negatives or lost
	// updates — validate on the full-decode path (D5).
	s.checkSorted(keys)
	n := node{flags: flags, link: link, keys: keys}
	if flags&flagDir != 0 {
		childCount := keysLen
		if flags&flagRight != 0 {
			childCount++
		}
		// A non-rightmost dir has childCount == keysLen; a keyless one (0
		// children) can never route or be indexed — reject it.
		if childCount == 0 {
			panic(mapdb5.NewCorruption("directory node with no children"))
		}
		n.children = make([]uint64, 0, childCount)
		for i := 0; i < childCount; i++ {
			c := r.UnpackLong()
			if c == 0 {
				panic(mapdb5.NewCorruption("directory child recid is zero"))
			}
			n.children = append(n.children, uint64(c))
		}
	} else {
		if s.valueInline {
			n.values = s.vf.DeserializeGroup(r, keysLen)
		} else {
			g := ser.LongFormat.DeserializeGroup(r, keysLen)
			n.recids = g.([]int64)
		}
		if flags&flagRight == 0 {
			f := s.kf.DeserializeGroup(r, 1)
			// The fence is the leaf's inclusive high bound, so it must be
			// >= the leaf's greatest live key. A crafted fence below the
			// last key makes a writer treat an in-leaf key as "beyond the
			// leaf" and move right, losing an atomic replace (D5).
			if keysLen > 0 {
				last := s.kf.Get(keys, keysLen-1)
				bound := s.kf.Get(f, 0)
				if s.kf.Compare(last, bound) > 0 {
					panic(mapdb5.NewCorruption("leaf fence below greatest key"))
				}
			}
			n.fence = f
		}
	}
	// Require exact consumption: a well-formed node uses every content byte.
	if size >= 0 {
		if consumed := r.Pos() - start; consumed != size {
			panic(mapdb5.NewCorruption("node record has unexpected trailing/short bytes (forged keysLen)"))
		}
	}
	return n
}

func (s nodeSerializer[K, V]) FixedSize() int { return -1 }

func (s nodeSerializer[K, V]) SizeHint() int { return s.sizeHint }

func (s nodeSerializer[K, V]) Compare(a, b node) int {
	panic(mapdb5.NewMisuse("btree nodes are never compared"))
}

func (s nodeSerializer[K, V]) Equals(a, b node) bool {
	panic(mapdb5.NewMisuse("btree nodes are never compared for equality"))
}

func (s nodeSerializer[K, V]) NaturalOrder() bool {
	panic(mapdb5.NewMisuse("btree nodes have no natural order"))
}

func (s nodeSerializer[K, V]) EqualsBySerializedBytes() bool { return true }
