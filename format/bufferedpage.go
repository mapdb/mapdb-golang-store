// Package format holds page-format codecs layered above the store (Java
// org.mapdb.format). The store stays byte-blind: a format owns the framing
// of a record's content and the consumer (a tree node, an htree bucket, …)
// owns the base image inside it.
//
// BufferedPageFormat is the R1 buffered-page convention extracted from
// BufferTreeMap's op tail (Java format/BufferedPageFormat):
//
//	0          int32 baseLen                 (consumer-owned consolidated base follows)
//	4          base[baseLen]
//	4+baseLen  deltaEntry*                   (append order = oldest first)
//
// Reverse-readable delta entry:
//
//	0   int32  totalLen    bytes from this field through trailerLen inclusive (== entry size)
//	4   uint8  opType      OpPut=1, OpDelete=2 (zero is NOT a valid opcode)
//	5   uint8  flags       bit0 hasValue, bit1 hasKeyHash, bit2 hasLsn, bit3 hasCrc
//	6   [int64 lsn]        present iff hasLsn; omitted (reads as 0) while lsn==0
//	    [int32 keyHash]    present iff hasKeyHash (FNV-1a 32 over the canonical key bytes)
//	    packInt keyLen
//	    byte[] keyBytes    key element serializer encoded
//	    [packInt valueLen, byte[] valueBytes]   present iff hasValue (PUT only)
//	    [int32  crc32c]    present iff hasCrc; CRC32C over bytes [entryStart, crcPos)
//	    int32  trailerLen  == totalLen; enables the newest-first reverse scan
//
// Tail discovery uses the record SIZE handed to the read action, never the
// CRC: torn appends never bump the store's length word. The CRC is for
// bit-rot detection (VerifyEntry) only.
//
// The LSN field is an experimental seam (Java STAMP_LSN == false): every
// writer in this port passes LSNUnassigned, so the field is never present in
// frames this port produces. Its layout is NOT frozen (Java: "no golden bytes
// may be frozen for ports until R3") — the decoder accepts it for parity only.
//
// Every length and offset decoded from page bytes is tainted (GD8): framing
// violations panic *mapdb5.DBError{DataCorruption} (Java IllegalStateException).
package format

import (
	"hash/crc32"
	"math/rand/v2"
	"sort"
	"sync/atomic"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/ser"
)

// Delta-entry opcodes (Java OP_PUT / OP_DELETE).
const (
	OpPut    = 1
	OpDelete = 2
)

// Entry flag bits.
const (
	fHasValue   = 1
	fHasKeyHash = 2
	fHasLsn     = 4
	fHasCrc     = 8
)

// MinEntryBytes is the smallest well-formed entry: totalLen(4) + op(1) +
// flags(1) + keyLen varint(1) + trailer(4).
const MinEntryBytes = 11

// Base-fingerprint header constants (a Bloom filter over the BASE keys at
// the start of the base region; see WriteBaseFp).
const (
	FPVersion  = 1
	fpKMax     = 16
	fpMaxBytes = 512
)

// LSNUnassigned is the non-WAL LSN placeholder (0 = omitted from the frame).
const LSNUnassigned int64 = 0

// FpMode is the correctness-neutrality (P7) test knob. It may only ever force
// MORE base searching, never synthesize a "definitely absent".
type FpMode int32

const (
	// FpNormal honors the filter (production).
	FpNormal FpMode = iota
	// FpForceSearch never skips: always a full base search.
	FpForceSearch
	// FpRandom randomly forces a full base search.
	FpRandom
)

var fpMode atomic.Int32

// SetFpModeForTest sets the process-global fingerprint mode (a test
// instrument, not shipped API — Java testSetFpMode). FpNormal restores
// production behavior.
func SetFpModeForTest(mode FpMode) { fpMode.Store(int32(mode)) }

// BaseHeader is a parsed + validated base header (fill via ParseBaseHeader).
type BaseHeader struct {
	Version, FpLen, K, BitsPos, NodeBase, HdrLen int
}

// PointResult is a reusable point-probe result.
type PointResult[V any] struct {
	Matched bool // a visible entry (PUT or DELETE) matched the key
	Put     bool // Matched && it was a PUT (Value valid); else DELETE
	Value   V
}

// Clear resets the result.
func (p *PointResult[V]) Clear() {
	var zero V
	p.Matched, p.Put, p.Value = false, false, zero
}

// Bound is an optional inclusive/exclusive range endpoint (Set=false = open).
type Bound[K any] struct {
	Key  K
	Set  bool
	Incl bool
}

// ShadowEntry is one key's newest visible op: Tomb marks a DELETE (Java
// TOMBSTONE marker); otherwise Value is the PUT value.
type ShadowEntry[K, V any] struct {
	Key   K
	Value V
	Tomb  bool
}

// Shadow is a key-ordered net-effect map (Java NavigableMap<K,Object>),
// ascending in the format's key order.
type Shadow[K, V any] struct {
	kf      ser.GroupFormat[K]
	Entries []ShadowEntry[K, V]
}

// NewShadow returns an empty shadow ordered by kf.Compare.
func NewShadow[K, V any](kf ser.GroupFormat[K]) *Shadow[K, V] {
	return &Shadow[K, V]{kf: kf}
}

// putIfAbsent inserts e unless its key is already present (LWW: the
// newest-first scan records a key's FIRST sighting).
func (s *Shadow[K, V]) putIfAbsent(e ShadowEntry[K, V]) {
	i := sort.Search(len(s.Entries), func(i int) bool {
		return s.kf.Compare(s.Entries[i].Key, e.Key) >= 0
	})
	if i < len(s.Entries) && s.kf.Compare(s.Entries[i].Key, e.Key) == 0 {
		return
	}
	s.Entries = append(s.Entries, ShadowEntry[K, V]{})
	copy(s.Entries[i+1:], s.Entries[i:])
	s.Entries[i] = e
}

// BufferedPageFormat is the R1 buffered-page codec. Immutable after
// construction; safe for concurrent use.
type BufferedPageFormat[K, V any] struct {
	kf           ser.GroupFormat[K]
	vf           ser.GroupFormat[V]
	keyElem      ser.Serializer[K]
	valueElem    ser.Serializer[V]
	writeKeyHash bool
	writeCrc     bool
	canonicalKey bool
	writeBaseFp  bool
	bitsPerKey   int
	baseFpK      int
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// New builds a codec. baseFpBitsPerKey is the Bloom budget for the
// negative-lookup accelerator (0 disables it). keyHash and the fingerprint
// are enabled only when the key element is canonical
// (EqualsBySerializedBytes): both hash the canonical key bytes.
func New[K, V any](kf ser.GroupFormat[K], vf ser.GroupFormat[V], writeKeyHash, writeCrc bool, baseFpBitsPerKey int) *BufferedPageFormat[K, V] {
	if kf == nil || vf == nil {
		panic(mapdb5.NewMisuse("BufferedPageFormat: nil group format"))
	}
	f := &BufferedPageFormat[K, V]{
		kf:         kf,
		vf:         vf,
		keyElem:    kf.Element(),
		valueElem:  vf.Element(),
		writeCrc:   writeCrc,
		bitsPerKey: baseFpBitsPerKey,
	}
	f.canonicalKey = f.keyElem.EqualsBySerializedBytes()
	f.writeKeyHash = writeKeyHash && f.canonicalKey
	f.writeBaseFp = baseFpBitsPerKey > 0 && f.canonicalKey
	if f.writeBaseFp {
		// optimal k ≈ bitsPerKey*ln2, clamped to [1, fpKMax]; Java
		// Math.round(x) == floor(x+0.5) for positive x.
		k := int(float64(baseFpBitsPerKey)*0.6931471805599453 + 0.5)
		f.baseFpK = max(1, min(fpKMax, k))
	}
	return f
}

// Compare is the codec's key order (the key format's stored order).
func (f *BufferedPageFormat[K, V]) Compare(a, b K) int { return f.kf.Compare(a, b) }

// ================= framing =================

// ReadBaseLen reads the int32 baseLen at pageStart.
func (f *BufferedPageFormat[K, V]) ReadBaseLen(r *bin.Reader, pageStart int) int {
	r.Seek(pageStart)
	baseLen := r.ReadInt()
	if baseLen < 0 {
		panic(mapdb5.NewCorruption("torn baseLen"))
	}
	return int(baseLen)
}

// LogStart is the absolute offset of the delta log.
func LogStart(pageStart, baseLen int) int { return pageStart + 4 + baseLen }

// ================= base fingerprint =================

// BaseFingerprintEnabled reports whether this codec writes a fingerprint.
func (f *BufferedPageFormat[K, V]) BaseFingerprintEnabled() bool { return f.writeBaseFp }

func (f *BufferedPageFormat[K, V]) fpByteLen(keyCount int) int {
	if !f.writeBaseFp || keyCount <= 0 {
		return 0
	}
	bits := int64(keyCount) * int64(f.bitsPerKey)
	bytes := (bits + 7) >> 3
	return int(max(1, min(fpMaxBytes, bytes)))
}

// WriteBaseFp writes the self-describing base header (fpVersion + fpLen +
// optional k + bits) FIRST in the base region, built over exactly baseKeys.
// Pass nil (directory node / no filter) to emit an empty header.
func (f *BufferedPageFormat[K, V]) WriteBaseFp(w *bin.Writer, baseKeys ser.Group) {
	w.WriteU8(FPVersion)
	n := 0
	if baseKeys != nil {
		n = f.kf.Size(baseKeys)
	}
	fpLen := f.fpByteLen(n)
	w.PackInt(int32(fpLen))
	if fpLen == 0 {
		return
	}
	w.WriteU8(byte(f.baseFpK))
	bits := make([]byte, fpLen)
	m := uint64(fpLen) << 3
	for i := 0; i < n; i++ {
		kb := serialize(f.keyElem, f.kf.Get(baseKeys, i))
		setBits(bits, m, f.baseFpK, kb)
	}
	w.WriteAll(bits)
}

// ParseBaseHeader parses + VALIDATES the base header at headerStart
// (== pageStart+4); baseLen bounds the region. Fail-closed: an unknown
// version or any malformed/out-of-region field panics corruption.
func (f *BufferedPageFormat[K, V]) ParseBaseHeader(r *bin.Reader, headerStart, baseLen int, out *BaseHeader) {
	if baseLen < 2 {
		panic(mapdb5.NewCorruption("base region too short for header"))
	}
	regionEnd := int64(headerStart) + int64(baseLen)
	r.Seek(headerStart)
	version := int(r.ReadU8())
	if version != FPVersion {
		panic(mapdb5.NewCorruption("unknown base header version"))
	}
	fpLen := int(r.UnpackInt())
	if fpLen < 0 || int64(r.Pos()) > regionEnd {
		panic(mapdb5.NewCorruption("torn fp length"))
	}
	k, bitsPos := 0, -1
	if fpLen > 0 {
		if fpLen > fpMaxBytes {
			panic(mapdb5.NewCorruption("fp too large"))
		}
		k = int(r.ReadU8())
		if k < 1 || k > fpKMax {
			panic(mapdb5.NewCorruption("bad fp k"))
		}
		bitsPos = r.Pos()
		if int64(bitsPos)+int64(fpLen) > regionEnd {
			panic(mapdb5.NewCorruption("fp overruns base region"))
		}
	}
	nodeBase := r.Pos()
	if fpLen > 0 {
		nodeBase = bitsPos + fpLen
	}
	// The base region MUST carry a non-empty consumer node base after the
	// header: a torn/header-only base must never license an "absent" skip.
	if int64(nodeBase) >= regionEnd {
		panic(mapdb5.NewCorruption("empty/torn node base after fp header"))
	}
	*out = BaseHeader{Version: version, FpLen: fpLen, K: k, BitsPos: bitsPos, NodeBase: nodeBase, HdrLen: nodeBase - headerStart}
}

// BaseFpMightContain: true = the key MAY be in the base (must search);
// false = DEFINITELY not a base key. No false negatives. probe is the
// canonical key serialization; nil (or no filter, or a non-normal test
// mode) always answers true.
func (f *BufferedPageFormat[K, V]) BaseFpMightContain(r *bin.Reader, h *BaseHeader, probe []byte) bool {
	mode := FpMode(fpMode.Load())
	if mode == FpForceSearch {
		return true
	}
	if h.FpLen == 0 || probe == nil {
		return true
	}
	if mode == FpRandom && rand.IntN(2) == 0 {
		return true
	}
	m := uint64(h.FpLen) << 3
	hh := fnv64(probe)
	h1 := hh & 0xFFFFFFFF
	h2 := (hh>>32)&0xFFFFFFFF | 1 // odd → non-degenerate double hashing
	for i := 0; i < h.K; i++ {
		idx := (h1 + uint64(i)*h2) % m
		r.Seek(h.BitsPos + int(idx>>3))
		if r.ReadU8()&(1<<(idx&7)) == 0 {
			return false
		}
	}
	return true
}

func setBits(bits []byte, m uint64, k int, keyBytes []byte) {
	hh := fnv64(keyBytes)
	h1 := hh & 0xFFFFFFFF
	h2 := (hh>>32)&0xFFFFFFFF | 1
	for i := 0; i < k; i++ {
		idx := (h1 + uint64(i)*h2) % m
		bits[idx>>3] |= 1 << (idx & 7)
	}
}

// fnv64 is FNV-1a 64 + the murmur3 fmix64 finalizer (Java fnv64).
func fnv64(b []byte) uint64 {
	h := uint64(0xcbf29ce484222325)
	for _, v := range b {
		h ^= uint64(v)
		h *= 0x100000001b3
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// hash32 is FNV-1a 32 over the canonical key bytes (the keyHash field).
func hash32(b []byte) int32 {
	h := uint32(0x811C9DC5)
	for _, v := range b {
		h ^= uint32(v)
		h *= 0x01000193
	}
	return int32(h)
}

// ================= encode =================

// Encode returns one framed delta entry. value is ignored for OpDelete.
func (f *BufferedPageFormat[K, V]) Encode(op int, key K, value V, lsn int64) []byte {
	w := bin.GetWriter()
	defer bin.PutWriter(w)
	f.EncodeInto(w, op, key, value, lsn)
	return w.CopyBytes()
}

// EncodeInto appends one framed delta entry to w. value is ignored for
// OpDelete. lsn==0 omits the LSN field.
func (f *BufferedPageFormat[K, V]) EncodeInto(w *bin.Writer, op int, key K, value V, lsn int64) {
	if op != OpPut && op != OpDelete {
		panic(mapdb5.NewMisuse("BufferedPageFormat: invalid opType"))
	}
	hasValue := op == OpPut
	flags := 0
	if lsn != 0 {
		flags |= fHasLsn
	}
	if hasValue {
		flags |= fHasValue
	}
	if f.writeKeyHash {
		flags |= fHasKeyHash
	}
	if f.writeCrc {
		flags |= fHasCrc
	}
	keyBytes := serialize(f.keyElem, key)
	var valBytes []byte
	if hasValue {
		valBytes = serialize(f.valueElem, value)
	}
	start := w.Pos
	w.WriteInt(0) // totalLen placeholder (backpatched)
	w.WriteU8(byte(op))
	w.WriteU8(byte(flags))
	if lsn != 0 {
		w.WriteLong(lsn)
	}
	if f.writeKeyHash {
		w.WriteInt(hash32(keyBytes))
	}
	w.PackInt(int32(len(keyBytes)))
	w.WriteAll(keyBytes)
	if hasValue {
		w.PackInt(int32(len(valBytes)))
		w.WriteAll(valBytes)
	}
	crcPos := w.Pos
	totalLen := crcPos - start + 4
	if f.writeCrc {
		totalLen += 4
	}
	patchInt(w.Buf, start, int32(totalLen))
	if f.writeCrc {
		w.WriteUint32(crc32.Checksum(w.Buf[start:crcPos], castagnoli))
	}
	w.WriteInt(int32(totalLen)) // trailerLen
}

// ================= decode =================

// entryHeader is one decoded entry's framing (key/value still in place).
type entryHeader struct {
	op, flags      int
	lsn            int64
	keyHash        int32
	keyPos, keyLen int
	payloadEnd     int // first byte after key/value (crc or trailer start)
}

// readHeader decodes and bounds-checks the framing of the entry at
// [start, start+totalLen). Every field must lie inside the entry.
func (f *BufferedPageFormat[K, V]) readHeader(r *bin.Reader, start, totalLen int) entryHeader {
	end := start + totalLen
	payloadEnd := end - 4 // trailer
	r.Seek(start)
	if lead := int(r.ReadInt()); lead != totalLen {
		panic(mapdb5.NewCorruption("delta len mismatch"))
	}
	var h entryHeader
	h.op = int(r.ReadU8())
	h.flags = int(r.ReadU8())
	if h.op != OpPut && h.op != OpDelete {
		panic(mapdb5.NewCorruption("delta entry has invalid opType"))
	}
	if (h.op == OpPut) != (h.flags&fHasValue != 0) {
		panic(mapdb5.NewCorruption("delta entry value flag contradicts opType"))
	}
	if h.flags&fHasCrc != 0 {
		payloadEnd -= 4
	}
	if h.flags&fHasLsn != 0 {
		h.lsn = r.ReadLong()
	}
	if h.flags&fHasKeyHash != 0 {
		h.keyHash = r.ReadInt()
	}
	kl := int(r.UnpackInt())
	h.keyPos = r.Pos()
	if kl < 0 || h.keyPos > payloadEnd || kl > payloadEnd-h.keyPos {
		panic(mapdb5.NewCorruption("delta key overruns entry"))
	}
	h.keyLen = kl
	h.payloadEnd = payloadEnd
	return h
}

// decodeValue reads the PUT value following the key.
func (f *BufferedPageFormat[K, V]) decodeValue(r *bin.Reader, h *entryHeader) V {
	r.Seek(h.keyPos + h.keyLen)
	vl := int(r.UnpackInt())
	vp := r.Pos()
	if vl < 0 || vp > h.payloadEnd || vl > h.payloadEnd-vp {
		panic(mapdb5.NewCorruption("delta value overruns entry"))
	}
	return deserializeExact(f.valueElem, r, vp, vl)
}

// decodeKey deserializes the key in place.
func (f *BufferedPageFormat[K, V]) decodeKey(r *bin.Reader, h *entryHeader) K {
	return deserializeExact(f.keyElem, r, h.keyPos, h.keyLen)
}

// deserializeExact decodes one element from [pos, pos+n) and requires the
// serializer to consume exactly n bytes (a tainted length must not let a
// decoder wander into the next field).
func deserializeExact[A any](s ser.Serializer[A], r *bin.Reader, pos, n int) A {
	sub := bin.NewReader(r.Subslice(pos, n))
	v := s.Deserialize(sub, n)
	if sub.Pos() != n {
		panic(mapdb5.NewCorruption("delta element length mismatch"))
	}
	return v
}

// ForEachEntryOldestFirst decodes the log region [logStart, logEnd) in
// append order, calling sink per entry (value is the zero V for DELETE).
func (f *BufferedPageFormat[K, V]) ForEachEntryOldestFirst(r *bin.Reader, logStart, logEnd int, sink func(op int, key K, value V, lsn int64)) {
	pos := logStart
	for pos < logEnd {
		r.Seek(pos)
		totalLen := int(r.ReadInt())
		if totalLen < MinEntryBytes || totalLen > logEnd-pos {
			panic(mapdb5.NewCorruption("torn delta entry"))
		}
		h := f.readHeader(r, pos, totalLen)
		k := f.decodeKey(r, &h)
		var v V
		if h.op == OpPut {
			v = f.decodeValue(r, &h)
		}
		f.checkTrailer(r, pos, totalLen)
		sink(h.op, k, v, h.lsn)
		pos += totalLen
	}
}

// checkTrailer requires trailerLen == totalLen (the reverse scan relies on
// it; a forward decode must reject the same frames the reverse one does).
func (f *BufferedPageFormat[K, V]) checkTrailer(r *bin.Reader, start, totalLen int) {
	r.Seek(start + totalLen - 4)
	if int(r.ReadInt()) != totalLen {
		panic(mapdb5.NewCorruption("delta trailer mismatch"))
	}
}

// reverseStart validates the trailer ending at pos and returns the entry's
// start and length.
func reverseStart(r *bin.Reader, logStart, pos int) (start, totalLen int) {
	r.Seek(pos - 4)
	totalLen = int(r.ReadInt())
	if totalLen < MinEntryBytes || totalLen > pos-logStart {
		panic(mapdb5.NewCorruption("torn delta trailer"))
	}
	return pos - totalLen, totalLen
}

// ProbeNewestFirst scans [logStart, logEnd) newest-first and STOPS at the
// first visible match (true LWW point read). probe (canonical key bytes)
// selects the in-place byte compare; nil decodes and compares with the
// format order. snapshotLsn == math.MaxInt64 makes every entry visible.
// Returns true and fills out when a PUT or DELETE matched.
//
// keyHash is used only as a NEGATIVE filter over canonical keys — sound
// because a format whose Compare is coarser than element byte equality must
// not declare its element canonical (Java invariant).
func (f *BufferedPageFormat[K, V]) ProbeNewestFirst(r *bin.Reader, logStart, logEnd int, key K, probe []byte, snapshotLsn int64, out *PointResult[V]) bool {
	out.Clear()
	var probeHash int32
	if f.writeKeyHash && probe != nil {
		probeHash = hash32(probe)
	}
	pos := logEnd
	for pos > logStart {
		start, totalLen := reverseStart(r, logStart, pos)
		h := f.readHeader(r, start, totalLen)
		match := false
		switch {
		case h.lsn > snapshotLsn:
		case h.flags&fHasKeyHash != 0 && probe != nil && h.keyHash != probeHash:
		case probe != nil:
			r.Seek(h.keyPos)
			match = h.keyLen == len(probe) && r.MatchBytes(probe)
		default:
			match = f.kf.Compare(f.decodeKey(r, &h), key) == 0
		}
		if match {
			out.Matched = true
			if h.op == OpPut {
				out.Value = f.decodeValue(r, &h)
				out.Put = true
			}
			return true
		}
		pos = start
	}
	return false
}

// ================= range / consolidation shadows =================

// LogShadowInRange records the newest visible op per key within [lo, hi]
// into shadow (newest-first scan, first sighting wins). The base is never
// touched.
func (f *BufferedPageFormat[K, V]) LogShadowInRange(r *bin.Reader, logStart, logEnd int, lo, hi Bound[K], snapshotLsn int64, shadow *Shadow[K, V]) {
	pos := logEnd
	for pos > logStart {
		start, totalLen := reverseStart(r, logStart, pos)
		h := f.readHeader(r, start, totalLen)
		pos = start
		if h.lsn > snapshotLsn {
			continue
		}
		k := f.decodeKey(r, &h)
		if !f.InRange(k, lo, hi) {
			continue
		}
		e := ShadowEntry[K, V]{Key: k, Tomb: h.op == OpDelete}
		if h.op == OpPut {
			e.Value = f.decodeValue(r, &h)
		}
		shadow.putIfAbsent(e)
	}
}

// NetEffect is the whole-log newest-visible-op-per-key map.
func (f *BufferedPageFormat[K, V]) NetEffect(r *bin.Reader, logStart, logEnd int, snapshotLsn int64, shadow *Shadow[K, V]) {
	f.LogShadowInRange(r, logStart, logEnd, Bound[K]{}, Bound[K]{}, snapshotLsn, shadow)
}

// InRange tests key against the optional bounds in format order.
func (f *BufferedPageFormat[K, V]) InRange(key K, lo, hi Bound[K]) bool {
	if lo.Set {
		c := f.kf.Compare(key, lo.Key)
		if c < 0 || (c == 0 && !lo.Incl) {
			return false
		}
	}
	if hi.Set {
		c := f.kf.Compare(key, hi.Key)
		if c > 0 || (c == 0 && !hi.Incl) {
			return false
		}
	}
	return true
}

// KvCursor is a consumer-supplied ascending base cursor.
type KvCursor[K, V any] interface {
	HasNext() bool
	Next()
	Key() K
	Value() V
}

// Merge is the ASCENDING merge of a base cursor with a shadow: the log wins
// on a tie, tombstones drop the entry. emit returning false stops the merge.
func (f *BufferedPageFormat[K, V]) Merge(base KvCursor[K, V], shadow *Shadow[K, V], emit func(K, V) bool) {
	baseHas := base.HasNext()
	if baseHas {
		base.Next()
	}
	si := 0
	for baseHas || si < len(shadow.Entries) {
		var c int
		switch {
		case !baseHas:
			c = 1
		case si >= len(shadow.Entries):
			c = -1
		default:
			c = f.kf.Compare(base.Key(), shadow.Entries[si].Key)
		}
		if c < 0 {
			if !emit(base.Key(), base.Value()) {
				return
			}
			baseHas = base.HasNext()
			if baseHas {
				base.Next()
			}
			continue
		}
		s := shadow.Entries[si]
		si++
		if c == 0 {
			baseHas = base.HasNext()
			if baseHas {
				base.Next()
			}
		}
		if !s.Tomb && !emit(s.Key, s.Value) {
			return
		}
	}
}

// ================= consolidation primitive =================

// Consolidated is Consolidate's result: fresh net groups, or a split verdict.
type Consolidated struct {
	Keys, Values  ser.Group // nil when SplitRequired
	KeyCount      int
	SplitRequired bool
}

// Consolidate folds the newest visible op per key into fresh key/value
// groups (LEAF pages only; tombstones drop). When the result exceeds
// maxBaseKeys it returns SplitRequired — the terminal "split me" outcome.
func (f *BufferedPageFormat[K, V]) Consolidate(baseKeys, baseVals ser.Group, r *bin.Reader, logStart, logEnd int, snapshotLsn int64, maxBaseKeys int) Consolidated {
	net := NewShadow[K, V](f.kf)
	f.NetEffect(r, logStart, logEnd, snapshotLsn, net)
	n := f.kf.Size(baseKeys)
	outK := make([]K, 0, n+len(net.Entries))
	outV := make([]V, 0, n+len(net.Entries))
	i, oi := 0, 0
	for i < n || oi < len(net.Entries) {
		var c int
		switch {
		case i >= n:
			c = 1
		case oi >= len(net.Entries):
			c = -1
		default:
			c = f.kf.Compare(f.kf.Get(baseKeys, i), net.Entries[oi].Key)
		}
		if c < 0 {
			outK = append(outK, f.kf.Get(baseKeys, i))
			outV = append(outV, f.vf.Get(baseVals, i))
			i++
			continue
		}
		op := net.Entries[oi]
		oi++
		if c == 0 {
			i++
		}
		if !op.Tomb {
			outK = append(outK, op.Key)
			outV = append(outV, op.Value)
		}
	}
	if len(outK) > maxBaseKeys {
		return Consolidated{KeyCount: len(outK), SplitRequired: true}
	}
	return Consolidated{Keys: f.kf.FromSlice(outK), Values: f.vf.FromSlice(outV), KeyCount: len(outK)}
}

// ================= CRC verify =================

// VerifyEntry checks one entry's CRC32C over [entryStart, crcPos). An entry
// without a stored CRC verifies trivially. Not consulted on read paths.
func (f *BufferedPageFormat[K, V]) VerifyEntry(r *bin.Reader, entryStart, totalLen int) bool {
	if totalLen < MinEntryBytes {
		panic(mapdb5.NewCorruption("torn delta entry"))
	}
	r.Seek(entryStart + 5)
	flags := r.ReadU8()
	if flags&fHasCrc == 0 {
		return true
	}
	crcPos := entryStart + totalLen - 8
	if crcPos < entryStart+6 {
		panic(mapdb5.NewCorruption("torn delta entry"))
	}
	sum := crc32.Checksum(r.Subslice(entryStart, crcPos-entryStart), castagnoli)
	r.Seek(crcPos)
	return r.ReadUint32() == sum
}

// ================= helpers =================

func serialize[A any](s ser.Serializer[A], v A) []byte {
	w := bin.GetWriter()
	defer bin.PutWriter(w)
	s.Serialize(w, v)
	return w.CopyBytes()
}

// SerializeKey returns the canonical key bytes (the probe form).
func (f *BufferedPageFormat[K, V]) SerializeKey(key K) []byte { return serialize(f.keyElem, key) }

func patchInt(buf []byte, off int, v int32) {
	buf[off] = byte(v >> 24)
	buf[off+1] = byte(v >> 16)
	buf[off+2] = byte(v >> 8)
	buf[off+3] = byte(v)
}
