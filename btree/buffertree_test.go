package btree

import (
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mapdb/mapdb-golang/object"
	"github.com/mapdb/mapdb-golang/pump"
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/format"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

// BufferTreeMap satisfies the sibling collections MutableMap contract.
var _ object.MutableMap[int64, int64] = (*BufferTreeMap[int64, int64])(nil)

func btCfg(maxNode, buf, leaf int) BufferTreeConfig[int64, int64] {
	return BufferTreeConfig[int64, int64]{KeyFormat: ser.LongFormat, ValueFormat: ser.LongFormat,
		MaxNodeSize: maxNode, BufferBytes: buf, LeafHeadroom: leaf}
}

// rawSer reads a record's content bytes verbatim (golden comparisons).
type rawSer struct{}

func (rawSer) Serialize(w *bin.Writer, v []byte)       { w.WriteAll(v) }
func (rawSer) Deserialize(r *bin.Reader, n int) []byte { return append([]byte(nil), r.ReadBytes(n)...) }
func (rawSer) FixedSize() int                          { return -1 }
func (rawSer) SizeHint() int                           { return 64 }
func (rawSer) Compare(a, b []byte) int                 { return 0 }
func (rawSer) Equals(a, b []byte) bool                 { return false }
func (rawSer) NaturalOrder() bool                      { return false }
func (rawSer) EqualsBySerializedBytes() bool           { return true }

func rootBytes(t *testing.T, s store.Store, rrr uint64) string {
	t.Helper()
	root, ok := store.Get(s, rrr, ser.Long)
	if !ok {
		t.Fatal("no root pointer")
	}
	b, _ := store.Get[[]byte](s, uint64(root), rawSer{})
	return hex.EncodeToString(b)
}

// Golden node records produced by the compiled Java BufferTreeMap over a
// Java StoreByteArray (throwaway driver against mapdb-java-store
// target/classes) — machine cross-validated.
func TestBufferTreeJavaGoldenRecords(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := NewBufferTreeMap(s, btCfg(8, 64, 0))
	if got := rootBytes(t, s, m.RootRecidRecid()); got != "00000003018086" {
		t.Fatalf("empty root %s", got)
	}
	m.PutOnly(3, 30)
	m.PutOnly(1, 10)
	m.RemoveOnly(3)
	const buffered = "0000000301808600000024010b98e16cac88000000000000000388000000000000001e5988bfe70000002400000024010b9ae16fd288000000000000000188000000000000000aad54674d000000240000001b020a98e16cac880000000000000003fd8a1c6b0000001b"
	if got := rootBytes(t, s, m.RootRecidRecid()); got != buffered {
		t.Fatalf("buffered root\n got %s\nwant %s", got, buffered)
	}
	m.FlushAll()
	if got := rootBytes(t, s, m.RootRecidRecid()); got != "00000015018106ed960000000000000001000000000000000a" {
		t.Fatalf("flushed root %s", got)
	}

	// Deterministic workload (Java LCG): identical contents AND an
	// identical root record — same split points, same recids, same op tail.
	s2 := store.NewStoreByteArray(true)
	m2 := NewBufferTreeMap(s2, btCfg(4, 64, LeafHeadroomNone))
	x := uint64(12345)
	for i := 0; i < 300; i++ {
		x = x*6364136223846793005 + 1442695040888963407
		k := int64((x >> 33) % 97)
		if x&3 == 0 {
			m2.RemoveOnly(k)
		} else {
			m2.PutOnly(k, int64(i))
		}
	}
	var sb strings.Builder
	for k, v := range m2.All() {
		fmt.Fprintf(&sb, "%d=%d,", k, v)
	}
	const javaEntries = "0=298,1=242,2=198,3=141,4=286,5=194,6=258,7=246,10=195,11=299,12=250,13=201,14=54,15=263,16=226,17=287,18=277,19=165,20=295,21=225,22=239,26=98,28=237,30=213,31=270,33=227,34=214,35=279,36=282,39=181,42=261,44=109,45=182,46=290,47=2,48=53,49=238,50=190,51=71,52=251,53=93,54=158,55=229,56=166,57=183,59=145,60=275,61=297,62=147,64=245,65=217,66=254,68=291,69=219,70=259,72=115,73=274,74=278,75=253,77=267,78=222,79=283,80=202,81=247,82=269,84=293,85=221,86=266,87=294,88=107,91=210,92=243,93=207,94=173,96=218,"
	if sb.String() != javaEntries {
		t.Fatalf("workload entries diverge from Java:\n got %s\nwant %s", sb.String(), javaEntries)
	}
	const javaWorkloadRoot = "0000000d01809e000000000000002f8a9900000024010ba0e1794488000000000000000b88000000000000012bfc82d99c00000024"
	if got := rootBytes(t, s2, m2.RootRecidRecid()); got != javaWorkloadRoot {
		t.Fatalf("workload root\n got %s\nwant %s", got, javaWorkloadRoot)
	}
	m2.Verify()
}

type btStore struct {
	name string
	mk   func(t *testing.T) store.StoreDelta
}

func btStores() []btStore {
	return []btStore{
		{"bytearray", func(*testing.T) store.StoreDelta { return store.NewStoreByteArray(true) }},
		{"direct", func(*testing.T) store.StoreDelta { return store.NewStoreDirectHeap(true) }},
		{"wal", func(t *testing.T) store.StoreDelta {
			s, err := store.OpenStoreWAL(filepath.Join(t.TempDir(), "bt"), true)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			return s
		}},
	}
}

// oracleCheck compares every read surface against a sorted oracle.
func oracleCheck(t *testing.T, m *BufferTreeMap[int64, int64], ref map[int64]int64, keySpace int64) {
	t.Helper()
	keys := make([]int64, 0, len(ref))
	for k := range ref {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var got []int64
	for k, v := range m.All() {
		if ref[k] != v {
			t.Fatalf("All: %d=%d want %d", k, v, ref[k])
		}
		got = append(got, k)
	}
	if !slices.Equal(got, keys) {
		t.Fatalf("All keys\n got %v\nwant %v", got, keys)
	}
	var desc []int64
	for k := range m.DescendingMap() {
		desc = append(desc, k)
	}
	slices.Reverse(desc)
	if !slices.Equal(desc, keys) {
		t.Fatalf("DescendingMap keys %v", desc)
	}
	for k := int64(-1); k <= keySpace; k++ {
		v, ok := m.Get(k)
		rv, rok := ref[k]
		if ok != rok || v != rv {
			t.Fatalf("Get(%d)=%d,%v want %d,%v", k, v, ok, rv, rok)
		}
	}
	// navigation
	for _, q := range []int64{-1, 0, keySpace / 3, keySpace / 2, keySpace} {
		i := sort.Search(len(keys), func(i int) bool { return keys[i] >= q })
		chk := func(name string, gk int64, ok bool, idx int) {
			t.Helper()
			wantOK := idx >= 0 && idx < len(keys)
			if ok != wantOK || (ok && gk != keys[idx]) {
				t.Fatalf("%s(%d)=%d,%v want idx %d of %v", name, q, gk, ok, idx, keys)
			}
		}
		k, _, ok := m.Ceiling(q)
		chk("Ceiling", k, ok, i)
		hi := i
		if i < len(keys) && keys[i] == q {
			hi = i + 1
		}
		k, _, ok = m.Higher(q)
		chk("Higher", k, ok, hi)
		k, _, ok = m.Lower(q)
		chk("Lower", k, ok, i-1)
		k, _, ok = m.Floor(q)
		chk("Floor", k, ok, hi-1)
		var sub []int64
		for k := range m.SubMap(q, q+keySpace/4) {
			sub = append(sub, k)
		}
		var wantSub []int64
		for _, k := range keys {
			if k >= q && k < q+keySpace/4 {
				wantSub = append(wantSub, k)
			}
		}
		if !slices.Equal(sub, wantSub) {
			t.Fatalf("SubMap(%d) %v want %v", q, sub, wantSub)
		}
		var rdesc []int64
		for k := range m.RangeEntries(q, true, false, q+keySpace/4, true, true, true) {
			rdesc = append(rdesc, k)
		}
		var wantR []int64
		for _, k := range keys {
			if k > q && k <= q+keySpace/4 {
				wantR = append(wantR, k)
			}
		}
		slices.Reverse(wantR)
		if !slices.Equal(rdesc, wantR) {
			t.Fatalf("RangeEntries desc (%d,%d] %v want %v", q, q+keySpace/4, rdesc, wantR)
		}
	}
	if m.LenLong() != int64(len(ref)) || m.IsEmpty() != (len(ref) == 0) {
		t.Fatalf("len %d want %d", m.LenLong(), len(ref))
	}
	m.Verify()
}

func TestBufferTreeDifferential(t *testing.T) {
	for _, st := range btStores() {
		for _, shape := range []struct {
			maxNode, buf, leaf int
		}{{4, 32, LeafHeadroomNone}, {4, 64, 0}, {8, 256, 16}, {32, 4096, 0}} {
			t.Run(fmt.Sprintf("%s/%d-%d-%d", st.name, shape.maxNode, shape.buf, shape.leaf), func(t *testing.T) {
				s := st.mk(t)
				m := NewBufferTreeMap(s, btCfg(shape.maxNode, shape.buf, shape.leaf))
				ref := map[int64]int64{}
				rnd := rand.New(rand.NewSource(int64(shape.maxNode*1000 + shape.buf)))
				const keySpace = 400
				for i := 0; i < 4000; i++ {
					k := rnd.Int63n(keySpace)
					v := rnd.Int63()
					switch rnd.Intn(11) {
					case 0, 1, 2:
						m.PutOnly(k, v)
						ref[k] = v
					case 3:
						old, had := m.Put(k, v)
						if rv, rok := ref[k]; had != rok || old != rv {
							t.Fatalf("Put old %d,%v want %d,%v", old, had, rv, rok)
						}
						ref[k] = v
					case 4:
						m.RemoveOnly(k)
						delete(ref, k)
					case 5:
						old, had := m.Remove(k)
						if rv, rok := ref[k]; had != rok || old != rv {
							t.Fatalf("Remove old %d,%v want %d,%v", old, had, rv, rok)
						}
						delete(ref, k)
					case 6:
						old, had := m.PutIfAbsent(k, v)
						if rv, rok := ref[k]; had != rok || old != rv {
							t.Fatalf("PutIfAbsent %d,%v want %d,%v", old, had, rv, rok)
						}
						if !had {
							ref[k] = v
						}
					case 7:
						rv, rok := ref[k]
						exp := rv
						if rnd.Intn(2) == 0 {
							exp = v
						}
						if got := m.ReplaceIfEquals(k, exp, v); got != (rok && exp == rv) {
							t.Fatalf("ReplaceIfEquals %v", got)
						}
						if rok && exp == rv {
							ref[k] = v
						}
					case 8:
						rv, rok := ref[k]
						if got := m.RemoveIfEquals(k, rv); got != rok {
							t.Fatalf("RemoveIfEquals %v want %v", got, rok)
						}
						delete(ref, k)
					case 9:
						old, had := m.Replace(k, v)
						if rv, rok := ref[k]; had != rok || old != rv {
							t.Fatalf("Replace %d,%v", old, had)
						}
						if had {
							ref[k] = v
						}
					case 10:
						desc := rnd.Intn(2) == 0
						var pk, pv int64
						var ok bool
						if desc {
							pk, pv, ok = m.PollLastEntry()
						} else {
							pk, pv, ok = m.PollFirstEntry()
						}
						if ok != (len(ref) > 0) {
							t.Fatalf("poll ok=%v len=%d", ok, len(ref))
						}
						if ok {
							for rk := range ref {
								if (desc && rk > pk) || (!desc && rk < pk) {
									t.Fatalf("poll returned %d, %d is more extreme", pk, rk)
								}
							}
							if ref[pk] != pv {
								t.Fatalf("poll value %d want %d", pv, ref[pk])
							}
							delete(ref, pk)
						}
					}
					if i%997 == 0 {
						oracleCheck(t, m, ref, keySpace)
					}
					if i%1500 == 1499 {
						m.FlushAll()
					}
				}
				oracleCheck(t, m, ref, keySpace)
				// FP neutrality: forced / randomized base searches agree.
				for _, mode := range []format.FpMode{format.FpForceSearch, format.FpRandom} {
					format.SetFpModeForTest(mode)
					for k := int64(-1); k <= keySpace; k++ {
						v, ok := m.Get(k)
						if rv, rok := ref[k]; ok != rok || v != rv {
							format.SetFpModeForTest(format.FpNormal)
							t.Fatalf("fp mode %d Get(%d) diverges", mode, k)
						}
					}
				}
				format.SetFpModeForTest(format.FpNormal)
				m.Clear()
				oracleCheck(t, m, map[int64]int64{}, keySpace)
			})
		}
	}
}

// Variable-width keys + a non-binary value format (decode-and-compare and
// materialized-group paths; the single-leaf byte path must fall back).
func TestBufferTreeStringKeysObjectValues(t *testing.T) {
	cfg := BufferTreeConfig[string, int64]{KeyFormat: ser.StringGroupFormat,
		ValueFormat: ser.NewObjectArrayFormat[int64](ser.Long), MaxNodeSize: 4, BufferBytes: 96}
	m := NewBufferTreeMap(store.NewStoreDirectHeap(true), cfg)
	ref := map[string]int64{}
	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("k%03d-%s", rnd.Intn(300), strings.Repeat("x", rnd.Intn(20)))
		if rnd.Intn(4) == 0 {
			m.RemoveOnly(k)
			delete(ref, k)
		} else {
			m.PutOnly(k, int64(i))
			ref[k] = int64(i)
		}
	}
	m.Verify()
	var keys []string
	for k := range ref {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var got []string
	for k, v := range m.All() {
		if ref[k] != v {
			t.Fatalf("%s=%d want %d", k, v, ref[k])
		}
		got = append(got, k)
	}
	if !slices.Equal(got, keys) {
		t.Fatalf("keys diverge")
	}
	for k, v := range ref {
		if g, ok := m.Get(k); !ok || g != v {
			t.Fatalf("Get(%s)", k)
		}
	}
	if _, ok := m.Get("nope"); ok {
		t.Fatal("phantom key")
	}
	// single-leaf map with a non-binary value format (byte path declines)
	m2 := NewBufferTreeMap(store.NewStoreByteArray(true), cfg)
	m2.PutOnly("b", 2)
	m2.PutOnly("a", 1)
	var ks []string
	for k := range m2.TailMap("a") {
		ks = append(ks, k)
	}
	if !slices.Equal(ks, []string{"a", "b"}) {
		t.Fatalf("tail %v", ks)
	}
}

// An op bigger than an empty root buffer is pushed down directly.
func TestBufferTreeOversizeOp(t *testing.T) {
	cfg := BufferTreeConfig[int64, []byte]{KeyFormat: ser.LongFormat, ValueFormat: ser.ByteArrayFormat, MaxNodeSize: 4, BufferBytes: 32}
	m := NewBufferTreeMap(store.NewStoreDirectHeap(true), cfg)
	big := make([]byte, 5000)
	for i := range big {
		big[i] = byte(i)
	}
	for i := int64(0); i < 20; i++ {
		m.PutOnly(i, big[:100+i*200])
	}
	for i := int64(0); i < 20; i++ {
		v, ok := m.Get(i)
		if !ok || len(v) != int(100+i*200) || v[50] != 50 {
			t.Fatalf("Get(%d) len %d", i, len(v))
		}
	}
	m.Verify()
}

func TestBufferTreeReopenWAL(t *testing.T) {
	base := filepath.Join(t.TempDir(), "re")
	s, err := store.OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	m := NewBufferTreeMap(s, btCfg(4, 64, 0))
	ref := map[int64]int64{}
	for i := int64(0); i < 500; i++ {
		k := (i * 37) % 211
		m.PutOnly(k, i)
		ref[k] = i
		if i%5 == 0 {
			m.RemoveOnly(k)
			delete(ref, k)
		}
	}
	rrr := m.RootRecidRecid()
	s.Commit()
	m.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = store.OpenStoreWAL(base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, err = OpenBufferTreeMap(s, rrr, btCfg(4, 64, 0))
	if err != nil {
		t.Fatal(err)
	}
	oracleCheck(t, m, ref, 211)
	// rollback rewinds buffered + flushed writes (tx store: no root cache)
	for i := int64(1000); i < 1300; i++ {
		m.PutOnly(i, i)
	}
	s.Rollback()
	oracleCheck(t, m, ref, 1300)
}

func TestBufferTreeCreateFromSorted(t *testing.T) {
	for _, st := range btStores() {
		t.Run(st.name, func(t *testing.T) {
			s := st.mk(t)
			ref := map[int64]int64{}
			seq := func(yield func(int64, int64) bool) {
				for i := int64(0); i < 1000; i++ {
					ref[i*3] = -i
					if !yield(i*3, -i) {
						return
					}
				}
			}
			m, err := CreateBufferTreeFromSorted(s, btCfg(8, 128, 0), seq)
			if err != nil {
				t.Fatal(err)
			}
			oracleCheck(t, m, ref, 3000)
			for i := int64(0); i < 3000; i += 7 {
				m.PutOnly(i, i)
				ref[i] = i
			}
			oracleCheck(t, m, ref, 3000)
			bad := func(yield func(int64, int64) bool) {
				_ = yield(2, 0) && yield(1, 0)
			}
			if _, err := CreateBufferTreeFromSorted(st.mk(t), btCfg(8, 128, 0), bad); !errors.Is(err, pump.ErrNotSorted) {
				t.Fatalf("unsorted: %v", err)
			}
		})
	}
}

func TestBufferTreeConfigValidation(t *testing.T) {
	s := store.NewStoreByteArray(true)
	for _, c := range []BufferTreeConfig[int64, int64]{btCfg(3, 64, 0), btCfg(4, 31, 0), btCfg(4, 64, -2), {MaxNodeSize: 4, BufferBytes: 64}} {
		func() {
			defer func() {
				de, ok := mapdb5.AsDBError(recover())
				if !ok || de.Kind != mapdb5.KindWrongConfiguration {
					t.Fatalf("config %+v: want WrongConfiguration", c)
				}
			}()
			NewBufferTreeMap(s, c)
		}()
	}
	if _, err := OpenBufferTreeMap(s, 0, btCfg(4, 64, 0)); err == nil {
		t.Fatal("open rrr=0 succeeded")
	}
}

func TestBufferTreeLeaseAndClose(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := NewBufferTreeMap(s, btCfg(4, 64, 0))
	m.PutOnly(1, 1)
	if _, err := OpenBufferTreeMap(s, m.RootRecidRecid(), btCfg(4, 64, 0)); err == nil {
		t.Fatal("second live handle allowed")
	}
	m.Close()
	m.Close() // idempotent
	if !m.IsClosed() {
		t.Fatal("not closed")
	}
	for name, f := range map[string]func(){
		"get": func() { m.Get(1) },
		"put": func() { m.PutOnly(2, 2) },
		"all": func() {
			for range m.All() {
			}
		},
		"flush": func() { m.FlushAll() },
	} {
		func() {
			defer func() {
				de, ok := mapdb5.AsDBError(recover())
				if !ok || de.Kind != mapdb5.KindClosed {
					t.Fatalf("%s after close: want KindClosed", name)
				}
			}()
			f()
		}()
	}
	m2, err := OpenBufferTreeMap(s, m.RootRecidRecid(), btCfg(4, 64, 0))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := m2.Get(1); !ok || v != 1 {
		t.Fatal("reopen lost data")
	}
}

// A suspended pull iterator must not block Close (admission is never held
// across a yield), and resuming the retired iterator panics KindClosed.
func TestBufferTreeIteratorCloseNoDeadlock(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := NewBufferTreeMap(s, btCfg(4, 64, 0))
	for i := int64(0); i < 200; i++ {
		m.PutOnly(i, i)
	}
	next, stop := iter.Pull2(m.All())
	defer stop()
	if _, _, ok := next(); !ok {
		t.Fatal("empty")
	}
	done := make(chan struct{})
	go func() { m.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close deadlocked on a suspended iterator")
	}
	func() {
		defer func() {
			de, ok := mapdb5.AsDBError(recover())
			if !ok || de.Kind != mapdb5.KindClosed {
				t.Fatalf("resume after close: %v", de)
			}
		}()
		next()
	}()
}

// Concurrent readers against the single writer (run under -race): every
// read is one locked store.Read, so no data races; readers see each key's
// value move only forward (writer appends increasing versions).
func TestBufferTreeConcurrentReaders(t *testing.T) {
	for _, st := range btStores()[:2] {
		t.Run(st.name, func(t *testing.T) {
			m := NewBufferTreeMap(st.mk(t), btCfg(4, 64, 0))
			const keys = 64
			var wg sync.WaitGroup
			stopc := make(chan struct{})
			errc := make(chan error, 8)
			for r := 0; r < 4; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					last := make([]int64, keys)
					for {
						select {
						case <-stopc:
							return
						default:
						}
						for k := int64(0); k < keys; k++ {
							v, ok := m.Get(k)
							if ok && v < last[k] {
								errc <- fmt.Errorf("key %d went back %d -> %d", k, last[k], v)
								return
							}
							if ok {
								last[k] = v
							}
						}
						prev := int64(-1)
						for k := range m.All() {
							if k <= prev {
								errc <- fmt.Errorf("All not ascending: %d after %d", k, prev)
								return
							}
							prev = k
						}
					}
				}()
			}
			for v := int64(1); v <= 3000; v++ {
				m.PutOnly(v%keys, v)
			}
			close(stopc)
			wg.Wait()
			select {
			case err := <-errc:
				t.Fatal(err)
			default:
			}
			m.Verify()
		})
	}
}

// A flipped byte inside a buffered delta frame is caught by Verify's CRC
// pass (bit-rot detection; reads trust framing, not the CRC).
func TestBufferTreeVerifyDetectsBitRot(t *testing.T) {
	s := store.NewStoreByteArray(true)
	m := NewBufferTreeMap(s, btCfg(8, 256, 0))
	m.PutOnly(5, 50)
	root, _ := store.Get(s, m.RootRecidRecid(), ser.Long)
	b, _ := store.Get[[]byte](s, uint64(root), rawSer{})
	b[len(b)-10] ^= 0x40 // inside the value bytes of the last frame
	store.Update[[]byte](s, uint64(root), b, rawSer{})
	defer func() {
		de, ok := mapdb5.AsDBError(recover())
		if !ok || de.Kind != mapdb5.KindVerifyFailed {
			t.Fatalf("want VerifyFailed, got %v", de)
		}
	}()
	m.Verify()
}

// Torn/forged node records fail closed with DataCorruption.
func TestBufferTreeCorruptNodeRejected(t *testing.T) {
	for name, mut := range map[string]func([]byte) []byte{
		"baseLen":   func(b []byte) []byte { b[3] = 0x7f; return b },
		"truncated": func(b []byte) []byte { return b[:len(b)-1] },
		"fpVersion": func(b []byte) []byte { b[4] = 9; return b },
	} {
		t.Run(name, func(t *testing.T) {
			s := store.NewStoreByteArray(true)
			m := NewBufferTreeMap(s, btCfg(8, 256, 0))
			m.PutOnly(5, 50)
			root, _ := store.Get(s, m.RootRecidRecid(), ser.Long)
			b, _ := store.Get[[]byte](s, uint64(root), rawSer{})
			store.Update[[]byte](s, uint64(root), mut(b), rawSer{})
			for op, f := range map[string]func(){
				"get": func() { m.Get(6) }, // absent: reaches the base (a log hit answers first)
				"all": func() {
					for range m.All() {
					}
				},
			} {
				func() {
					defer func() {
						de, ok := mapdb5.AsDBError(recover())
						if !ok || de.Kind != mapdb5.KindDataCorruption {
							t.Fatalf("%s: want DataCorruption, got %v", op, de)
						}
					}()
					f()
				}()
			}
		})
	}
}

func TestBufferTreeThinSurface(t *testing.T) {
	m := NewBufferTreeMap(store.NewStoreByteArray(true), btCfg(4, 64, 0))
	if _, _, ok := m.Min(); ok {
		t.Fatal("min on empty")
	}
	for i := int64(1); i <= 20; i++ {
		m.PutOnly(i, i*10)
	}
	if m.Len() != 20 || !m.ContainsKey(7) || m.ContainsKey(0) || m.MaxNodeSize() != 4 {
		t.Fatal("len/contains/accessors")
	}
	if m.KeySerializer() == nil || m.ValueSerializer() == nil {
		t.Fatal("serializers")
	}
	if k, v, ok := m.FirstEntry(); !ok || k != 1 || v != 10 {
		t.Fatal("first")
	}
	if k, _, ok := m.LastEntry(); !ok || k != 20 {
		t.Fatal("last")
	}
	var ks, vs, dk, hk []int64
	for k := range m.Keys() {
		ks = append(ks, k)
	}
	for v := range m.Values() {
		vs = append(vs, v)
	}
	for k := range m.DescendingKeys() {
		dk = append(dk, k)
	}
	for k := range m.HeadMap(4) {
		hk = append(hk, k)
	}
	if len(ks) != 20 || vs[19] != 200 || dk[0] != 20 || !slices.Equal(hk, []int64{1, 2, 3}) {
		t.Fatalf("views %v %v %v %v", ks, vs, dk, hk)
	}
	sum := int64(0)
	m.ForEach(func(k, v int64) { sum += v })
	if sum != 2100 || !m.AnySatisfy(func(k, v int64) bool { return k == 5 }) ||
		!m.AllSatisfy(func(k, v int64) bool { return v == k*10 }) || !m.NoneSatisfy(func(k, v int64) bool { return k > 20 }) {
		t.Fatal("predicates")
	}
	var empty []int64
	for k := range m.SubMap(9, 3) {
		empty = append(empty, k)
	}
	if len(empty) != 0 {
		t.Fatal("inverted SubMap not empty")
	}
}
