package db

// DB facade tests — ported from mapdb-rust-store src/db/tests.rs
// (DBSmokeTest, DBEdgeCaseTest, DBReadOnlyMakerTest, DBQueueTest,
// DBTreeCounterListenerTest, DBTreeExternalValuesTest,
// DBParameterizedCatalogTest, BindTest, golden catalog rows).

import (
	"bytes"
	"fmt"
	"iter"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
	"github.com/mapdb/mapdb5-go/btree"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/store"
)

var tmpSeq atomic.Int64

func freshFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(os.TempDir(), fmt.Sprintf("mapdb5-w4-%d-%d.db", os.Getpid(), tmpSeq.Add(1)))
	_ = os.Remove(p)
	t.Cleanup(func() {
		_ = os.Remove(p)
		// WAL segment namespace cleanup.
		matches, _ := filepath.Glob(p + ".*")
		for _, m := range matches {
			_ = os.Remove(m)
		}
	})
	return p
}

func memDB(t *testing.T) *DB {
	t.Helper()
	d, err := MemoryDB().Make()
	if err != nil {
		t.Fatalf("MemoryDB make: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func fileDB(t *testing.T, path string) *DB {
	t.Helper()
	d, err := FileDB(path).TransactionEnable().Make()
	if err != nil {
		t.Fatalf("fileDB make: %v", err)
	}
	return d
}

func wantErr(t *testing.T, err error, kind mapdb5.Kind, what string) {
	t.Helper()
	de, ok := mapdb5.AsDBError(err)
	if !ok || de.Kind != kind {
		t.Fatalf("%s: err = %v, want %v", what, err, kind)
	}
}

// ============================ DBSmokeTest ============================

func TestCatalogAtRecid1(t *testing.T) {
	if RecidCatalog != 1 {
		t.Fatalf("RecidCatalog = %d, want 1", RecidCatalog)
	}
	d := memDB(t)
	if got := d.GetNameCatalog(); len(got) != 0 {
		t.Fatalf("fresh catalog = %v, want empty", got)
	}
}

func TestTreeMapRoundTrip(t *testing.T) {
	d := memDB(t)
	m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 100; i++ {
		m.Put(i, fmt.Sprintf("v%d", i))
	}
	if v, ok := m.Get(50); !ok || v != "v50" {
		t.Fatalf("get(50) = %v,%v", v, ok)
	}
	if k, _, ok := m.FirstEntry(); !ok || k != 0 {
		t.Fatalf("first = %v,%v", k, ok)
	}
	if k, _, ok := m.LastEntry(); !ok || k != 99 {
		t.Fatalf("last = %v,%v", k, ok)
	}
	if !d.Exists("t") {
		t.Fatal("t missing")
	}
	if ty, _ := d.GetType("t"); ty != "TreeMap" {
		t.Fatalf("type = %q", ty)
	}
}

func TestTreeSetAddThroughView(t *testing.T) {
	d := memDB(t)
	ts, err := TreeSet[int64](d, "ts", ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	if !ts.Add(3) || !ts.Add(1) || !ts.Add(2) || ts.Add(2) {
		t.Fatal("add results wrong")
	}
	if v, _ := ts.First(); v != 1 {
		t.Fatalf("first = %v", v)
	}
	if v, _ := ts.Last(); v != 3 {
		t.Fatalf("last = %v", v)
	}
	if ts.Size() != 3 || !ts.Contains(2) {
		t.Fatal("size/contains wrong")
	}
	if ty, _ := d.GetType("ts"); ty != "TreeSet" {
		t.Fatalf("type = %q", ty)
	}
}

func TestTreeSetNavigationSurface(t *testing.T) {
	d := memDB(t)
	ts, _ := TreeSet[int64](d, "ts", ser.LongFormat).Create()
	for _, k := range []int64{10, 20, 30, 40, 50} {
		ts.Add(k)
	}
	if v, _ := ts.Lower(30); v != 20 {
		t.Fatalf("lower(30) = %v", v)
	}
	if v, _ := ts.Floor(35); v != 30 {
		t.Fatalf("floor(35) = %v", v)
	}
	if v, _ := ts.Ceiling(35); v != 40 {
		t.Fatalf("ceiling(35) = %v", v)
	}
	if v, _ := ts.Higher(30); v != 40 {
		t.Fatalf("higher(30) = %v", v)
	}
	if _, ok := ts.Lower(10); ok {
		t.Fatal("lower(10) should be absent")
	}
	if got := ts.SubSet(20, true, 40, false).Slice(); fmt.Sprint(got) != "[20 30]" {
		t.Fatalf("sub = %v", got)
	}
	if got := ts.HeadSet(30, true).Slice(); fmt.Sprint(got) != "[10 20 30]" {
		t.Fatalf("head = %v", got)
	}
	if got := ts.TailSet(30, false).Slice(); fmt.Sprint(got) != "[40 50]" {
		t.Fatalf("tail = %v", got)
	}
	if got := ts.Descending().Slice(); fmt.Sprint(got) != "[50 40 30 20 10]" {
		t.Fatalf("desc = %v", got)
	}
	// Live view: backing add is visible; view remove writes through.
	tail := ts.TailSet(30, true)
	ts.Add(45)
	if got := tail.Slice(); fmt.Sprint(got) != "[30 40 45 50]" {
		t.Fatalf("live tail = %v", got)
	}
	if !tail.Remove(45) || ts.Contains(45) {
		t.Fatal("view remove write-through failed")
	}
	desc := ts.Descending()
	if v, _ := desc.First(); v != 50 {
		t.Fatalf("desc first = %v", v)
	}
	if v, _ := desc.PollFirst(); v != 50 || ts.Contains(50) {
		t.Fatal("desc pollFirst write-through failed")
	}
	if ts.HeadSet(25, false).Contains(30) {
		t.Fatal("out-of-range contains")
	}
	if v, _ := ts.PollFirst(); v != 10 {
		t.Fatalf("pollFirst = %v", v)
	}
	if v, _ := ts.PollLast(); v != 40 {
		t.Fatalf("pollLast = %v", v)
	}
	if got := ts.Slice(); fmt.Sprint(got) != "[20 30]" {
		t.Fatalf("rest = %v", got)
	}
}

func TestTreeSetNestedAndDescendingViews(t *testing.T) {
	d := memDB(t)
	ts, _ := TreeSet[int64](d, "ts", ser.LongFormat).Create()
	for _, k := range []int64{10, 20, 30, 40, 50, 60, 70} {
		ts.Add(k)
	}
	sub := ts.SubSet(20, true, 60, true)
	if got := sub.Slice(); fmt.Sprint(got) != "[20 30 40 50 60]" {
		t.Fatalf("sub = %v", got)
	}
	if got := sub.SubSet(30, true, 50, false).Slice(); fmt.Sprint(got) != "[30 40]" {
		t.Fatalf("nested sub = %v", got)
	}
	if got := sub.HeadSet(40, false).Slice(); fmt.Sprint(got) != "[20 30]" {
		t.Fatalf("nested head = %v", got)
	}
	if got := sub.TailSet(40, true).Slice(); fmt.Sprint(got) != "[40 50 60]" {
		t.Fatalf("nested tail = %v", got)
	}
	desc := ts.Descending()
	if got := desc.Slice(); fmt.Sprint(got) != "[70 60 50 40 30 20 10]" {
		t.Fatalf("desc = %v", got)
	}
	if v, _ := desc.Lower(40); v != 50 {
		t.Fatalf("desc lower(40) = %v, want 50", v)
	}
	if v, _ := desc.Higher(40); v != 30 {
		t.Fatalf("desc higher(40) = %v, want 30", v)
	}
	dsub := desc.SubSet(60, true, 20, true)
	if got := dsub.Slice(); fmt.Sprint(got) != "[60 50 40 30 20]" {
		t.Fatalf("dsub = %v", got)
	}
	if got := dsub.HeadSet(40, true).Slice(); fmt.Sprint(got) != "[60 50 40]" {
		t.Fatalf("dsub head = %v", got)
	}
	if got := dsub.TailSet(40, false).Slice(); fmt.Sprint(got) != "[30 20]" {
		t.Fatalf("dsub tail = %v", got)
	}
	if v, _ := dsub.PollFirst(); v != 60 {
		t.Fatalf("dsub pollFirst = %v, want 60", v)
	}
	if v, _ := dsub.PollLast(); v != 20 {
		t.Fatalf("dsub pollLast = %v, want 20", v)
	}
	if !ts.Contains(70) || !ts.Contains(10) {
		t.Fatal("out-of-range elements must be untouched")
	}
	// Bounded clear write-through.
	ts.SubSet(30, true, 50, true).Clear()
	if got := ts.Slice(); fmt.Sprint(got) != "[10 70]" {
		t.Fatalf("after bounded clear = %v", got)
	}
	if v, _ := desc.PollFirst(); v != 70 {
		t.Fatalf("desc pollFirst = %v", v)
	}
	if v, _ := desc.PollLast(); v != 10 {
		t.Fatalf("desc pollLast = %v", v)
	}
	if !ts.IsEmpty() {
		t.Fatal("set not empty")
	}
}

func TestAtomicsAllKinds(t *testing.T) {
	d := memDB(t)
	al, err := d.AtomicLongInit("al", 10).Create()
	if err != nil {
		t.Fatal(err)
	}
	if al.Get() != 10 {
		t.Fatalf("al = %d", al.Get())
	}
	if al.IncrementAndGet() != 11 {
		t.Fatal("increment")
	}
	if !al.CompareAndSet(11, 20) || al.CompareAndSet(11, 30) {
		t.Fatal("cas")
	}
	ai, err := d.AtomicInteger("ai").Create()
	if err != nil {
		t.Fatal(err)
	}
	if ai.Get() != 0 || ai.AddAndGet(5) != 5 {
		t.Fatal("ai")
	}
	ab, err := d.AtomicBooleanInit("ab", true).Create()
	if err != nil {
		t.Fatal(err)
	}
	if !ab.Get() {
		t.Fatal("ab")
	}
	ab.Set(false)
	if ab.Get() {
		t.Fatal("ab set")
	}
	as, err := d.AtomicString("as").Create()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := as.Get(); ok {
		t.Fatal("as should start null")
	}
	as.SetValue("hi")
	if v, ok := as.Get(); !ok || v != "hi" {
		t.Fatalf("as = %v,%v", v, ok)
	}
	init := "init"
	av, err := AtomicVarOf[string](d, "av", ser.String, &init).Create()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := av.Get(); !ok || v != "init" {
		t.Fatalf("av = %v,%v", v, ok)
	}
	av.SetValue("next")
	if v, ok := av.Get(); !ok || v != "next" {
		t.Fatalf("av = %v,%v", v, ok)
	}
}

func TestNumericAtomicsIntLongValue(t *testing.T) {
	d := memDB(t)
	v, _ := d.AtomicLongInit("long", 42).Create()
	i, _ := d.AtomicIntegerInit("int", 7).Create()
	if v.IntValue() != 42 || i.LongValue() != 7 {
		t.Fatal("numeric narrowing/widening")
	}
	d.Compact()
}

func TestPersistAcrossReopenWAL(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).Create()
		if err != nil {
			t.Fatal(err)
		}
		m.Put(7, "seven")
		if _, err := d.AtomicLongInit("counter", 99).Create(); err != nil {
			t.Fatal(err)
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		if !d.Exists("t") {
			t.Fatal("t missing after reopen")
		}
		m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := m.Get(7); !ok || v != "seven" {
			t.Fatalf("get(7) = %v,%v", v, ok)
		}
		al, err := d.AtomicLong("counter").Open()
		if err != nil {
			t.Fatal(err)
		}
		if al.Get() != 99 {
			t.Fatalf("counter = %d", al.Get())
		}
	}
}

func TestCreateOrOpenReturnsSameInstance(t *testing.T) {
	d := memDB(t)
	mk := func() *btree.BTreeMap[int64, string] {
		m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).CreateOrOpen()
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	a, b := mk(), mk()
	if !a.SharesStateWith(b) {
		t.Fatal("create_or_open did not return the cached instance")
	}
}

func TestCreateTwiceFails(t *testing.T) {
	d := memDB(t)
	if _, err := d.AtomicLong("x").Create(); err != nil {
		t.Fatal(err)
	}
	_, err := d.AtomicLong("x").Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "create twice")
}

func TestTypeMismatchFails(t *testing.T) {
	d := memDB(t)
	if _, err := d.AtomicLong("x").Create(); err != nil {
		t.Fatal(err)
	}
	_, err := TreeMap[int64, string](d, "x", ser.LongFormat, ser.StringGroupFormat).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "type mismatch")
}

func TestRenameAndDelete(t *testing.T) {
	d := memDB(t)
	m, err := TreeMap[int64, string](d, "old", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(1, "a")
	d.Rename("old", "new")
	if d.Exists("old") || !d.Exists("new") {
		t.Fatal("rename")
	}
	m2, err := TreeMap[int64, string](d, "new", ser.LongFormat, ser.StringGroupFormat).Open()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := m2.Get(1); !ok || v != "a" {
		t.Fatalf("renamed map lost data")
	}
	if !d.Delete("new") || d.Exists("new") || d.Delete("new") {
		t.Fatal("delete")
	}
}

func TestRollbackClearsCacheWAL(t *testing.T) {
	f := freshFile(t)
	d := fileDB(t, f)
	defer d.Close()
	if _, err := d.AtomicLongInit("committed", 1).Create(); err != nil {
		t.Fatal(err)
	}
	d.Commit()
	if _, err := d.AtomicLongInit("uncommitted", 2).Create(); err != nil {
		t.Fatal(err)
	}
	if !d.Exists("uncommitted") {
		t.Fatal("uncommitted not visible pre-rollback")
	}
	d.Rollback()
	if d.Exists("uncommitted") {
		t.Fatal("uncommitted visible after rollback")
	}
	if !d.Exists("committed") {
		t.Fatal("committed lost on rollback")
	}
}

func TestRollbackUnsupportedOnNonTx(t *testing.T) {
	d := memDB(t)
	defer func() {
		r := recover()
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindUnsupported {
			t.Fatalf("rollback panic = %v, want Unsupported", r)
		}
	}()
	d.Rollback()
}

func TestBadNameRejected(t *testing.T) {
	d := memDB(t)
	_, err := d.AtomicLong("bad#name").Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "bad name")
}

func TestTransactionEnableRejectedForMemory(t *testing.T) {
	_, err := MemoryDB().TransactionEnable().Make()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "tx on memory")
}

func TestGetAllNames(t *testing.T) {
	d := memDB(t)
	if _, err := d.AtomicLong("a").Create(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AtomicLong("b").Create(); err != nil {
		t.Fatal(err)
	}
	if names := d.GetAllNames(); len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("names = %v", names)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	d, err := MemoryDB().Make()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if !d.IsClosed() {
		t.Fatal("not closed")
	}
	_, err = d.AtomicLong("x").Create()
	wantErr(t, err, mapdb5.KindClosed, "create after close")
}

// ============================ DBEdgeCaseTest ============================

func TestFreshStoresInitializeWithEmptyCatalog(t *testing.T) {
	for _, mk := range []func() (*DB, error){
		MemoryDB().Make, HeapDB().Make, MemoryByteArrayDB().Make, MemoryDirectDB().Make,
		MemoryAppendOnlyDB().Make, MemoryAppendOnlyDirectDB().Make,
	} {
		d, err := mk()
		if err != nil {
			t.Fatal(err)
		}
		if len(d.GetNameCatalog()) != 0 || len(d.GetAllNames()) != 0 {
			t.Fatal("fresh catalog not empty")
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReopenStoreWithValidCatalogWorks(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		if _, err := d.AtomicLongInit("counter", 7).Create(); err != nil {
			t.Fatal(err)
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		if !d.Exists("counter") {
			t.Fatal("counter missing")
		}
		al, err := d.AtomicLong("counter").Open()
		if err != nil {
			t.Fatal(err)
		}
		if al.Get() != 7 {
			t.Fatalf("counter = %d", al.Get())
		}
	}
}

func TestDBOverPollutedStoreRecid1FailsCleanly(t *testing.T) {
	raw := store.NewStoreByteArray(true)
	recid := store.Put(raw, int64(123456789), ser.Long)
	if recid != RecidCatalog {
		t.Fatalf("first put recid = %d, want 1", recid)
	}
	_, err := NewDB(raw)
	de, ok := mapdb5.AsDBError(err)
	if !ok || (de.Kind != mapdb5.KindWrongConfiguration && de.Kind != mapdb5.KindDataCorruption) {
		t.Fatalf("err = %v, want wrong-config or corruption", err)
	}
}

func TestCreateOpenSemanticsAcrossReopen(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		if _, err := d.AtomicLong("x").Open(); err == nil {
			t.Fatal("open of absent name should fail")
		}
		if _, err := d.AtomicLongInit("x", 1).CreateOrOpen(); err != nil {
			t.Fatal(err)
		}
		if _, err := d.AtomicLong("x").Create(); err == nil {
			t.Fatal("create on existing name should fail")
		}
		al, err := d.AtomicLong("x").CreateOrOpen()
		if err != nil {
			t.Fatal(err)
		}
		if al.Get() != 1 {
			t.Fatalf("x = %d, want 1", al.Get())
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		if _, err := d.AtomicLong("x").Create(); err == nil {
			t.Fatal("create on existing name should fail")
		}
		al, err := d.AtomicLong("x").Open()
		if err != nil {
			t.Fatal(err)
		}
		if al.Get() != 1 {
			t.Fatalf("x = %d, want 1", al.Get())
		}
	}
}

func TestDeleteFreesAtomicAndAllowsRecreate(t *testing.T) {
	d := memDB(t)
	if _, err := d.AtomicLongInit("c", 5).Create(); err != nil {
		t.Fatal(err)
	}
	if !d.Delete("c") || d.Exists("c") || d.Delete("c") {
		t.Fatal("delete")
	}
	if _, ok := d.GetType("c"); ok {
		t.Fatal("type still present")
	}
	re, err := d.AtomicLongInit("c", 42).Create()
	if err != nil {
		t.Fatal(err)
	}
	if re.Get() != 42 {
		t.Fatalf("recreated c = %d", re.Get())
	}
}

func TestRenameOntoExistingAndNonexistentThrow(t *testing.T) {
	d := memDB(t)
	if _, err := d.AtomicLong("a").Create(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AtomicLong("b").Create(); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("rename onto existing should fail")
			}
		}()
		d.Rename("a", "b")
	}()
	if !d.Exists("a") || !d.Exists("b") {
		t.Fatal("failed rename must leave both names")
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("rename of nonexistent should fail")
			}
		}()
		d.Rename("nope", "x")
	}()
}

func TestManyNamedAtomicsSurviveReopen(t *testing.T) {
	f := freshFile(t)
	const n = 200
	{
		d := fileDB(t, f)
		for i := 0; i < n; i++ {
			if _, err := d.AtomicLongInit(fmt.Sprintf("al%d", i), int64(i)).Create(); err != nil {
				t.Fatal(err)
			}
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		if names := d.GetAllNames(); len(names) != n {
			t.Fatalf("names = %d, want %d", len(names), n)
		}
		for i := 0; i < n; i++ {
			al, err := d.AtomicLong(fmt.Sprintf("al%d", i)).Open()
			if err != nil {
				t.Fatal(err)
			}
			if al.Get() != int64(i) {
				t.Fatalf("al%d = %d", i, al.Get())
			}
		}
	}
}

func TestTreeMapByteArrayKeysAndValuesRoundTrip(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		m, err := TreeMap[[]byte, []byte](d, "b", ser.ByteArrayFormat, ser.ByteArrayFormat).Create()
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 200; i++ {
			m.Put([]byte{byte(i >> 8), byte(i)}, []byte{byte(i), 99})
		}
		if v, ok := m.Get([]byte{0, 50}); !ok || !bytes.Equal(v, []byte{50, 99}) {
			t.Fatalf("get = %v,%v", v, ok)
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		m, err := TreeMap[[]byte, []byte](d, "b", ser.ByteArrayFormat, ser.ByteArrayFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 200; i++ {
			if v, ok := m.Get([]byte{byte(i >> 8), byte(i)}); !ok || !bytes.Equal(v, []byte{byte(i), 99}) {
				t.Fatalf("get(%d) = %v,%v", i, v, ok)
			}
		}
	}
}

func TestTreeSetDeepSplitsSurviveReopen(t *testing.T) {
	f := freshFile(t)
	const n = int64(600)
	{
		d := fileDB(t, f)
		ts, err := TreeSet[int64](d, "ts", ser.LongFormat).MaxNodeSize(8).Create()
		if err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < n; i++ {
			ts.Add((i * 37) % n)
		}
		if ts.Size() != n {
			t.Fatalf("size = %d", ts.Size())
		}
		ts.Add(n)
		ts.Remove(n)
		if !ts.Remove(123) || ts.Contains(123) {
			t.Fatal("remove")
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		ts, err := TreeSet[int64](d, "ts", ser.LongFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if ts.Size() != n-1 {
			t.Fatalf("size = %d, want %d", ts.Size(), n-1)
		}
		prev := int64(math.MinInt64)
		for _, x := range ts.Slice() {
			if x <= prev {
				t.Fatalf("not sorted at %d", x)
			}
			prev = x
		}
	}
}

// ============================ DBReadOnlyMakerTest ============================

func TestReadOnlyReopenReadsButRejectsWrites(t *testing.T) {
	f := freshFile(t)
	{
		d, err := FileDB(f).Make()
		if err != nil {
			t.Fatal(err)
		}
		m, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).Create()
		if err != nil {
			t.Fatal(err)
		}
		m.Put(1, "one")
		m.Put(2, "two")
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		ro, err := FileDB(f).ReadOnly().Make()
		if err != nil {
			t.Fatal(err)
		}
		defer ro.Close()
		rm, err := TreeMap[int64, string](ro, "m", ser.LongFormat, ser.StringGroupFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := rm.Get(1); !ok || v != "one" {
			t.Fatalf("get(1) = %v,%v", v, ok)
		}
		func() {
			defer func() {
				r := recover()
				de, ok := mapdb5.AsDBError(r)
				if !ok || de.Kind != mapdb5.KindReadOnly {
					t.Fatalf("put panic = %v, want ReadOnly", r)
				}
			}()
			rm.Put(3, "three")
		}()
		ro.Commit() // harmless no-op
	}
}

func TestReadOnlyPlusTransactionEnableRejected(t *testing.T) {
	f := freshFile(t)
	if _, err := FileDB(f).ReadOnly().TransactionEnable().Make(); err == nil {
		t.Fatal("readOnly+tx should be rejected")
	}
	if _, err := FileDB(f).TransactionEnable().ReadOnly().Make(); err == nil {
		t.Fatal("tx+readOnly should be rejected")
	}
}

func TestReadOnlyOnEmptyStoreFailsCleanly(t *testing.T) {
	f := freshFile(t)
	_, err := FileDB(f).ReadOnly().Make()
	de, ok := mapdb5.AsDBError(err)
	if !ok || de.Kind != mapdb5.KindWrongConfiguration || !strings.Contains(strings.ToLower(de.Error()), "read") {
		t.Fatalf("err = %v", err)
	}
}

func TestReadOnlyOnEmptyMemoryStoreFailsCleanly(t *testing.T) {
	_, err := MemoryDB().ReadOnly().Make()
	de, ok := mapdb5.AsDBError(err)
	if !ok || de.Kind != mapdb5.KindWrongConfiguration || !strings.Contains(strings.ToLower(de.Error()), "read") {
		t.Fatalf("err = %v", err)
	}
}

func TestFileMmapEnableIsHarmlessNoop(t *testing.T) {
	f := freshFile(t)
	{
		d, err := FileDB(f).FileMmapEnable().FileMmapEnableIfSupported().Make()
		if err != nil {
			t.Fatal(err)
		}
		m, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).Create()
		if err != nil {
			t.Fatal(err)
		}
		m.Put(1, "x")
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d, err := FileDB(f).FileMmapEnable().Make()
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		m, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := m.Get(1); !ok || v != "x" {
			t.Fatalf("get = %v,%v", v, ok)
		}
	}
}

func TestFileDeleteAfterOpenRemovesFileButDBStillWorks(t *testing.T) {
	f := freshFile(t)
	d, err := FileDB(f).FileDeleteAfterOpen().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := os.Stat(f); !os.IsNotExist(err) {
		t.Fatalf("backing file still present after open: %v", err)
	}
	m, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(1, "alive")
	d.Commit()
	if v, ok := m.Get(1); !ok || v != "alive" {
		t.Fatalf("get = %v,%v", v, ok)
	}
}

func TestFileDeleteAfterOpenRequiresFileDB(t *testing.T) {
	_, err := MemoryDB().FileDeleteAfterOpen().Make()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "deleteAfterOpen on memory")
}

func TestFileDeleteAfterOpenRejectsWAL(t *testing.T) {
	f := freshFile(t)
	_, err := FileDB(f).TransactionEnable().FileDeleteAfterOpen().Make()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "deleteAfterOpen on WAL")
}

func TestFileDeleteAfterClose(t *testing.T) {
	f := freshFile(t)
	d, err := FileDB(f).FileDeleteAfterClose().Make()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f); err != nil {
		t.Fatalf("file missing before close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f); !os.IsNotExist(err) {
		t.Fatal("file not deleted after close")
	}
}

func TestFileDeleteAfterCloseOnMemoryRejected(t *testing.T) {
	_, err := MemoryDB().FileDeleteAfterClose().Make()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "deleteAfterClose on memory")
}

func TestLegacyMakerAliasesCompose(t *testing.T) {
	d, err := MemoryDB().
		ExecutorEnable().
		CleanerHackEnable().
		FileChannelEnable().
		ChecksumStoreEnable().
		CloseOnJVMShutdown().
		CloseOnJVMShutdownWeakReference().
		Make()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTempFileDB(t *testing.T) {
	d, err := TempFileDB().Make()
	if err != nil {
		t.Fatal(err)
	}
	m, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(1, "x")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

// ============================ DBQueueTest ============================

func TestQueueFamiliesPersistAndDispatch(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		fifo, err := Queue[string](d, "fifo", ser.String).Create()
		if err != nil {
			t.Fatal(err)
		}
		fifo.Add("first")
		st, err := Stack[string](d, "stack", ser.String).Create()
		if err != nil {
			t.Fatal(err)
		}
		st.Add("top")
		circ, err := CircularQueue[string](d, "circular", ser.String, 2).Create()
		if err != nil {
			t.Fatal(err)
		}
		circ.Add("a")
		circ.Add("b")
		circ.Add("c")
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		fifo, err := Queue[string](d, "fifo", ser.String).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := fifo.Poll(); !ok || v != "first" {
			t.Fatalf("fifo = %v,%v", v, ok)
		}
		st, err := Stack[string](d, "stack", ser.String).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := st.Poll(); !ok || v != "top" {
			t.Fatalf("stack = %v,%v", v, ok)
		}
		circ, err := CircularQueue[string](d, "circular", ser.String, 2).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := circ.Poll(); !ok || v != "b" {
			t.Fatalf("circ1 = %v,%v", v, ok)
		}
		if v, ok := circ.Poll(); !ok || v != "c" {
			t.Fatalf("circ2 = %v,%v", v, ok)
		}
	}
}

func TestDBCloseWakesBlockedConsumer(t *testing.T) {
	d, err := MemoryDB().Make()
	if err != nil {
		t.Fatal(err)
	}
	q, err := Queue[string](d, "queue", ser.String).Create()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		q.Take()
		done <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != mapdb5.KindClosed {
			t.Fatalf("take result = %v, want KindClosed", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("blocked consumer not woken by close")
	}
}

// ============================ counter + listener ============================

func TestCounterTracksInsertRemoveReplaceAndClear(t *testing.T) {
	d := memDB(t)
	m, err := TreeMap[int64, string](d, "c", ser.LongFormat, ser.StringGroupFormat).
		CounterEnable().Create()
	if err != nil {
		t.Fatal(err)
	}
	if m.CounterRecid() == 0 {
		t.Fatal("no counter")
	}
	for i := int64(0); i < 100; i++ {
		m.Put(i, fmt.Sprintf("v%d", i))
		if m.LenLong() != i+1 {
			t.Fatalf("size = %d, want %d", m.LenLong(), i+1)
		}
	}
	m.Put(0, "changed")
	if m.LenLong() != 100 {
		t.Fatalf("size after update = %d", m.LenLong())
	}
	m.Replace(0, "again")
	if _, ok := m.Replace(1000, "x"); ok {
		t.Fatal("replace of absent key")
	}
	if m.LenLong() != 100 {
		t.Fatal("replace of absent grew the counter")
	}
	for i := int64(0); i < 40; i++ {
		m.Remove(i)
		if m.LenLong() != 100-(i+1) {
			t.Fatalf("size = %d", m.LenLong())
		}
	}
	m.Clear()
	if m.LenLong() != 0 {
		t.Fatal("clear")
	}
}

func TestCounterPersistsAcrossReopen(t *testing.T) {
	f := freshFile(t)
	var stored string
	{
		d := fileDB(t, f)
		m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).
			CounterEnable().Create()
		if err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 500; i++ {
			m.Put(i, fmt.Sprintf("v%d", i))
		}
		stored = d.GetNameCatalog()["t#counterRecid"]
		if n, err := strconv.ParseUint(stored, 10, 64); err != nil || n == 0 {
			t.Fatalf("stored counterRecid = %q", stored)
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		if got := d.GetNameCatalog()["t#counterRecid"]; got != stored {
			t.Fatalf("counterRecid = %q, want %q", got, stored)
		}
		m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if strconv.FormatUint(m.CounterRecid(), 10) != stored {
			t.Fatalf("counterRecid = %d", m.CounterRecid())
		}
		if m.LenLong() != 500 {
			t.Fatalf("size = %d", m.LenLong())
		}
		for i := int64(500); i < 600; i++ {
			m.Put(i, fmt.Sprintf("v%d", i))
		}
		if m.LenLong() != 600 {
			t.Fatal("counter did not continue after reopen")
		}
	}
}

func TestPlainTreeMapReopensWithoutCounter(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		m, err := TreeMap[int64, string](d, "p", ser.LongFormat, ser.StringGroupFormat).Create()
		if err != nil {
			t.Fatal(err)
		}
		for i := int64(0); i < 300; i++ {
			m.Put(i, fmt.Sprintf("v%d", i))
		}
		if m.CounterRecid() != 0 {
			t.Fatal("unexpected counter")
		}
		if d.GetNameCatalog()["p#counterRecid"] != "0" {
			t.Fatalf("catalog counterRecid = %q", d.GetNameCatalog()["p#counterRecid"])
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		m, err := TreeMap[int64, string](d, "p", ser.LongFormat, ser.StringGroupFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if m.CounterRecid() != 0 || m.LenLong() != 300 {
			t.Fatalf("reopen: counter=%d size=%d", m.CounterRecid(), m.LenLong())
		}
		if v, ok := m.Get(42); !ok || v != "v42" {
			t.Fatal("data lost")
		}
	}
}

func TestModificationListenerCapturesInsertUpdateRemove(t *testing.T) {
	d := memDB(t)
	type ev struct {
		k    int64
		o, n *string
		trig bool
	}
	var events []ev
	listener := mapdb5.ModificationListenerFunc[int64, string](
		func(k int64, o, n *string, trig bool) error {
			var oc, nc *string
			if o != nil {
				s := *o
				oc = &s
			}
			if n != nil {
				s := *n
				nc = &s
			}
			events = append(events, ev{k, oc, nc, trig})
			return nil
		})
	m, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).
		ModificationListener(listener).Create()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(5, "fifty")
	m.Put(5, "sixty")
	m.Remove(5)
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}
	if events[0].o != nil || events[0].n == nil || *events[0].n != "fifty" {
		t.Fatalf("insert event = %+v", events[0])
	}
	if events[1].o == nil || *events[1].o != "fifty" || events[1].n == nil || *events[1].n != "sixty" {
		t.Fatalf("update event = %+v", events[1])
	}
	if events[2].o == nil || *events[2].o != "sixty" || events[2].n != nil {
		t.Fatalf("remove event = %+v", events[2])
	}
}

func TestListenerAppliedOnceToCachedHandle(t *testing.T) {
	d := memDB(t)
	created, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	listener := mapdb5.ModificationListenerFunc[int64, string](
		func(k int64, o, v *string, trig bool) error { n.Add(1); return nil })
	mk := func() *btree.BTreeMap[int64, string] {
		m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).
			ModificationListener(listener).Open()
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	a, b := mk(), mk()
	if !created.SharesStateWith(a) || !created.SharesStateWith(b) {
		t.Fatal("cache did not return same state")
	}
	created.Put(1, "one")
	if n.Load() != 1 {
		t.Fatalf("listener fired %d times, want 1 (dedup)", n.Load())
	}
}

func TestTreeMapCreateFromIteratorWithCounter(t *testing.T) {
	d := memDB(t)
	const n = 2000
	entries := func(yield func(int64, string) bool) {
		for i := int64(0); i < n; i++ {
			if !yield(i, fmt.Sprintf("v%d", i)) {
				return
			}
		}
	}
	m, err := TreeMap[int64, string](d, "bulk", ser.LongFormat, ser.StringGroupFormat).
		CounterEnable().CreateFrom(iter.Seq2[int64, string](entries))
	if err != nil {
		t.Fatal(err)
	}
	if m.CounterRecid() == 0 || m.LenLong() != n {
		t.Fatalf("counter=%d size=%d", m.CounterRecid(), m.LenLong())
	}
	if v, ok := m.Get(1999); !ok || v != "v1999" {
		t.Fatal("bulk data")
	}
	m.Put(n, "new")
	if m.LenLong() != n+1 {
		t.Fatal("counter did not grow after bulk")
	}
}

func TestCreateFromExistingNameAndUnsortedThrow(t *testing.T) {
	d := memDB(t)
	if _, err := TreeMap[int64, string](d, "dup", ser.LongFormat, ser.StringGroupFormat).Create(); err != nil {
		t.Fatal(err)
	}
	single := func(yield func(int64, string) bool) { yield(1, "a") }
	_, err := TreeMap[int64, string](d, "dup", ser.LongFormat, ser.StringGroupFormat).
		CreateFrom(iter.Seq2[int64, string](single))
	wantErr(t, err, mapdb5.KindWrongConfiguration, "createFrom on existing")
	bad := func(yield func(int64, string) bool) {
		for _, kv := range [][2]any{{int64(3), "c"}, {int64(1), "a"}, {int64(2), "b"}} {
			if !yield(kv[0].(int64), kv[1].(string)) {
				return
			}
		}
	}
	_, err = TreeMap[int64, string](d, "us", ser.LongFormat, ser.StringGroupFormat).
		CreateFrom(iter.Seq2[int64, string](bad))
	wantErr(t, err, mapdb5.KindNotSorted, "unsorted createFrom")
	if d.Exists("us") {
		t.Fatal("failed createFrom must not register the name")
	}
}

func TestTreeSetCounterAndCreateFrom(t *testing.T) {
	d := memDB(t)
	src := func(yield func(int64) bool) {
		for i := int64(0); i < 1000; i++ {
			if !yield(i * 2) {
				return
			}
		}
	}
	s, err := TreeSet[int64](d, "bulkset", ser.LongFormat).CounterEnable().
		CreateFrom(iter.Seq[int64](src))
	if err != nil {
		t.Fatal(err)
	}
	if s.Size() != 1000 {
		t.Fatalf("size = %d", s.Size())
	}
	if v, _ := s.First(); v != 0 {
		t.Fatal("first")
	}
	if v, _ := s.Last(); v != 1998 {
		t.Fatal("last")
	}
	if !s.Contains(500) || s.Contains(501) {
		t.Fatal("contains")
	}
	bad := func(yield func(int64) bool) {
		if !yield(5) {
			return
		}
		yield(2)
	}
	_, err = TreeSet[int64](d, "badset", ser.LongFormat).CreateFrom(iter.Seq[int64](bad))
	wantErr(t, err, mapdb5.KindNotSorted, "unsorted set createFrom")
	if d.Exists("badset") {
		t.Fatal("failed createFrom must not register the name")
	}
}

// ============================ external values ============================

func TestExternalValuesCatalogPersistence(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		m, err := TreeMap[int64, string](d, "tree", ser.LongFormat, ser.StringGroupFormat).
			ValuesOutsideNodesEnable().CounterEnable().Create()
		if err != nil {
			t.Fatal(err)
		}
		m.Put(1, "one")
		m.Put(2, "two")
		if m.ValueInline() {
			t.Fatal("valueInline should be false")
		}
		if d.GetNameCatalog()["tree#valueInline"] != "false" {
			t.Fatalf("valueInline = %q", d.GetNameCatalog()["tree#valueInline"])
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		m, err := TreeMap[int64, string](d, "tree", ser.LongFormat, ser.StringGroupFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if m.ValueInline() {
			t.Fatal("external map reopened as inline")
		}
		if v, ok := m.Get(1); !ok || v != "one" {
			t.Fatal("data")
		}
		if m.LenLong() != 2 {
			t.Fatal("counter")
		}
	}
}

func TestCreateFromRejectsExternalValues(t *testing.T) {
	d := memDB(t)
	entries := func(yield func(int64, string) bool) { yield(1, "a") }
	_, err := TreeMap[int64, string](d, "x", ser.LongFormat, ser.StringGroupFormat).
		ValuesOutsideNodesEnable().CreateFrom(iter.Seq2[int64, string](entries))
	wantErr(t, err, mapdb5.KindUnsupported, "external createFrom")
}

// ============================ parameterized codecs ============================

func TestParameterizedCodecsReopenWithoutResupply(t *testing.T) {
	f := freshFile(t)
	tupleFmt := ser.NewTupleFormat(ser.TupleStr, ser.TupleLong, ser.TupleInt)
	compressedFmt := ser.NewObjectArrayFormat[string](ser.NewCompressionLevel(ser.String, 6))
	columnarFmt := ser.NewColumnarValueFormat(ser.ColumnLong, ser.ColumnInt)
	{
		d := fileDB(t, f)
		tuples, err := TreeMap[[]ser.Value, string](d, "tuples", tupleFmt, ser.StringGroupFormat).Create()
		if err != nil {
			t.Fatal(err)
		}
		tuples.Put([]ser.Value{ser.StringValue("tenant"), ser.LongValue(7), ser.IntValue(1)}, "one")
		tuples.Put([]ser.Value{ser.StringValue("tenant"), ser.LongValue(7), ser.IntValue(2)}, "two")

		cv, err := TreeMap[int64, string](d, "compressed", ser.LongFormat, compressedFmt).Create()
		if err != nil {
			t.Fatal(err)
		}
		cv.Put(1, strings.Repeat("abcdefghij", 100))

		cm, err := TreeMap[int64, []ser.Value](d, "columnar", ser.LongFormat, columnarFmt).Create()
		if err != nil {
			t.Fatal(err)
		}
		cm.Put(1, []ser.Value{ser.LongValue(9), ser.IntValue(4)})
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		tuples, err := TreeMap[[]ser.Value, string](d, "tuples", tupleFmt, ser.StringGroupFormat).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := tuples.Get([]ser.Value{ser.StringValue("tenant"), ser.LongValue(7), ser.IntValue(2)}); !ok || v != "two" {
			t.Fatalf("tuple get = %v,%v", v, ok)
		}
		cv, err := TreeMap[int64, string](d, "compressed", ser.LongFormat, compressedFmt).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := cv.Get(1); !ok || v != strings.Repeat("abcdefghij", 100) {
			t.Fatal("compressed value")
		}
		cm, err := TreeMap[int64, []ser.Value](d, "columnar", ser.LongFormat, columnarFmt).Open()
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := cm.Get(1); !ok || len(v) != 2 {
			t.Fatalf("columnar = %v,%v", v, ok)
		}
	}
}

func TestWrongDescriptorOnReopenIsRejected(t *testing.T) {
	f := freshFile(t)
	{
		d := fileDB(t, f)
		if _, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).Create(); err != nil {
			t.Fatal(err)
		}
		d.Commit()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		d := fileDB(t, f)
		defer d.Close()
		// Wrong value format (LONG keys + LONG values) must be rejected.
		_, err := TreeMap[int64, int64](d, "t", ser.LongFormat, ser.LongFormat).Open()
		wantErr(t, err, mapdb5.KindWrongConfiguration, "descriptor mismatch")
	}
}

// ============================ BindTest ============================

func TestBindSecondaryIndexesHistogramAndDeleteSink(t *testing.T) {
	d := memDB(t)
	primary, err := TreeMap[int64, string](d, "p", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	lengths := NewSecondaryMap[int64, int]()
	SecondaryValue(primary, lengths, func(_ int64, v string) int { return len(v) })
	primary.Put(1, "red,round")
	if v, ok := lengths.Get(1); !ok || v != 9 {
		t.Fatalf("length = %v,%v", v, ok)
	}
	inverse := NewSecondaryMap[string, int64]()
	MapInverse(primary, inverse)
	keys := NewSecondarySet[ValuePair[string, int64]]()
	SecondaryKeys(primary, keys, func(_ int64, v string) []string {
		return strings.Split(v, ",")
	})
	hist := NewSecondaryMap[rune, int64]()
	Histogram(primary, hist, func(_ int64, v string) rune { return []rune(v)[0] })
	deleted := NewSecondaryMap[int64, string]()
	MapPutAfterDelete(primary, deleted)

	primary.Put(2, "blue,square")
	primary.Put(1, "red,square")
	if v, ok := inverse.Get("red,square"); !ok || v != 1 {
		t.Fatalf("inverse = %v,%v", v, ok)
	}
	if inverse.ContainsKey("red,round") {
		t.Fatal("stale inverse")
	}
	if !keys.Contains(ValuePair[string, int64]{"square", 1}) {
		t.Fatal("keys index missing (square,1)")
	}
	if keys.Contains(ValuePair[string, int64]{"round", 1}) {
		t.Fatal("stale keys index")
	}
	if v, _ := hist.Get('r'); v != 1 {
		t.Fatalf("hist r = %d", v)
	}
	if v, _ := hist.Get('b'); v != 1 {
		t.Fatalf("hist b = %d", v)
	}
	primary.Remove(2)
	if v, ok := deleted.Get(2); !ok || v != "blue,square" {
		t.Fatalf("deleted = %v,%v", v, ok)
	}
	if hist.ContainsKey('b') {
		t.Fatal("histogram kept a zeroed category")
	}
}

func TestBindSizeAndSelfBindRejection(t *testing.T) {
	d := memDB(t)
	primary, err := TreeMap[int64, string](d, "p", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	primary.Put(1, "one")
	counter, err := d.AtomicLong("counter").Create()
	if err != nil {
		t.Fatal(err)
	}
	SizeBinding(primary, counter)
	if counter.Get() != 1 {
		t.Fatalf("seeded counter = %d", counter.Get())
	}
	primary.Put(2, "two")
	primary.Put(2, "two updated")
	primary.Remove(1)
	if counter.Get() != 1 {
		t.Fatalf("counter = %d, want 1", counter.Get())
	}
	// Direct self-binding is rejected.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("self-bind not rejected")
			}
		}()
		RejectSelfBind(primary, primary)
	}()
	// Same check through a persistent-map secondary (identity-based).
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("self-bind via secondary not rejected")
			}
		}()
		SecondaryValue(primary, BTreeSecMap[int64, string]{M: primary},
			func(_ int64, v string) string { return v })
	}()
}

func TestBindUniqueSecondaryRejectsDuplicateDerivedKeys(t *testing.T) {
	d := memDB(t)
	primary, err := TreeMap[int64, string](d, "p", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	unique := NewSecondaryMap[int, int64]()
	SecondaryKey(primary, unique, func(_ int64, v string) int { return len(v) })
	primary.Put(1, "same")
	// "size" also has length 4 → duplicate derived key → listener error.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("duplicate derived key not rejected")
			}
		}()
		primary.Put(2, "size")
	}()
}

func TestBindConcurrentSameKeyWritersKeepSecondaryConsistent(t *testing.T) {
	d := memDB(t)
	primary, err := TreeMap[int64, string](d, "p", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	secondary := NewSecondaryMap[int64, string]()
	SecondaryValue(primary, secondary, func(_ int64, v string) string { return "d:" + v })
	const keyCount, writers, iters = 4, 8, 500
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start.Wait()
			for i := 0; i < iters; i++ {
				key := int64(i % keyCount)
				primary.Put(key, fmt.Sprintf("t%d-i%d", w, i))
			}
		}(w)
	}
	start.Done()
	wg.Wait()
	for key := int64(0); key < keyCount; key++ {
		pv, ok := primary.Get(key)
		if !ok {
			t.Fatalf("key %d missing", key)
		}
		if sv, ok := secondary.Get(key); !ok || sv != "d:"+pv {
			t.Fatalf("secondary[%d] = %q, want d:%q", key, sv, pv)
		}
	}
}

// ============================ golden catalog rows ============================

func assertCatalogRow(t *testing.T, d *DB, expected [][2]string) {
	t.Helper()
	cat := d.GetNameCatalog()
	want := make(map[string]string, len(expected))
	for _, kv := range expected {
		want[kv[0]] = kv[1]
	}
	if len(cat) != len(want) {
		t.Fatalf("catalog = %v, want %v", cat, want)
	}
	for k, v := range want {
		if cat[k] != v {
			t.Fatalf("catalog[%q] = %q, want %q (full: %v)", k, cat[k], v, cat)
		}
	}
}

func TestGoldenAtomicLongRow(t *testing.T) {
	d := memDB(t)
	a, err := d.AtomicLongInit("al", 5).Create()
	if err != nil {
		t.Fatal(err)
	}
	assertCatalogRow(t, d, [][2]string{
		{"al#type", "AtomicLong"},
		{"al#recid", strconv.FormatUint(a.Recid(), 10)},
	})
}

func TestGoldenAtomicIntegerBooleanStringRows(t *testing.T) {
	d := memDB(t)
	ai, _ := d.AtomicIntegerInit("ai", 1).Create()
	ab, _ := d.AtomicBooleanInit("ab", true).Create()
	as, _ := d.AtomicString("as").Create()
	assertCatalogRow(t, d, [][2]string{
		{"ai#type", "AtomicInteger"}, {"ai#recid", strconv.FormatUint(ai.Recid(), 10)},
		{"ab#type", "AtomicBoolean"}, {"ab#recid", strconv.FormatUint(ab.Recid(), 10)},
		{"as#type", "AtomicString"}, {"as#recid", strconv.FormatUint(as.Recid(), 10)},
	})
}

func TestGoldenAtomicVarRow(t *testing.T) {
	d := memDB(t)
	init := "x"
	av, err := AtomicVarOf[string](d, "av", ser.String, &init).Create()
	if err != nil {
		t.Fatal(err)
	}
	assertCatalogRow(t, d, [][2]string{
		{"av#type", "AtomicVar"},
		{"av#recid", strconv.FormatUint(av.Recid(), 10)},
		{"av#serializer", "STRING"},
	})
}

func TestGoldenTreeMapRow(t *testing.T) {
	d := memDB(t)
	m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).
		MaxNodeSize(32).Create()
	if err != nil {
		t.Fatal(err)
	}
	assertCatalogRow(t, d, [][2]string{
		{"t#type", "TreeMap"},
		{"t#keySerializer", "LONG"},
		{"t#valueSerializer", "STRING"},
		{"t#rootRecidRecid", strconv.FormatUint(m.RootRecidRecid(), 10)},
		{"t#maxNodeSize", "32"},
		{"t#counterRecid", "0"},
		{"t#valueInline", "true"},
	})
}

func TestGoldenTreeMapCounterAndExternalRows(t *testing.T) {
	d := memDB(t)
	m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).
		MaxNodeSize(16).ValuesOutsideNodesEnable().CounterEnable().Create()
	if err != nil {
		t.Fatal(err)
	}
	assertCatalogRow(t, d, [][2]string{
		{"t#type", "TreeMap"},
		{"t#keySerializer", "LONG"},
		{"t#valueSerializer", "STRING"},
		{"t#rootRecidRecid", strconv.FormatUint(m.RootRecidRecid(), 10)},
		{"t#maxNodeSize", "16"},
		{"t#counterRecid", strconv.FormatUint(m.CounterRecid(), 10)},
		{"t#valueInline", "false"},
	})
}

func TestGoldenTreeSetRow(t *testing.T) {
	d := memDB(t)
	s, err := TreeSet[int64](d, "s", ser.LongFormat).MaxNodeSize(32).Create()
	if err != nil {
		t.Fatal(err)
	}
	assertCatalogRow(t, d, [][2]string{
		{"s#type", "TreeSet"},
		{"s#serializer", "LONG"},
		{"s#rootRecidRecid", strconv.FormatUint(s.RootRecidRecid(), 10)},
		{"s#maxNodeSize", "32"},
		{"s#counterRecid", "0"},
	})
}

// ============================ catalog corruption ============================

func TestCatalogCorruptionRejected(t *testing.T) {
	raw := store.NewStoreByteArray(true)
	// Valid header but a broken count so entries overrun the record.
	bad := []byte{0x4D, 0x44, 0x42, 0x43, 0x00, 0x00, 0x00, 0x01, 0x00, 0x85, 'x'}
	recid := raw.Put(bad, ser.ByteArray)
	if recid != 1 {
		t.Fatalf("recid = %d", recid)
	}
	_, err := NewDB(raw)
	de, ok := mapdb5.AsDBError(err)
	if !ok || de.Kind != mapdb5.KindDataCorruption {
		t.Fatalf("err = %v, want DataCorruption", err)
	}
}

func TestCatalogUnknownTypeAndFieldsRejected(t *testing.T) {
	cases := []NameCatalog{
		{"x#type": "Bogus", "x#recid": "1"},             // unknown type
		{"x#type": "AtomicLong", "x#bogus": "1"},        // unknown field
		{"x#type": "AtomicLong"},                        // missing recid
		{"x#type": "AtomicLong", "x#recid": "0"},        // recid < 1
		{"x#type": "AtomicLong", "x#recid": "abc"},      // non-decimal
		{"x#type": "TreeMap", "x#keySerializer": "WAT"}, // bad descriptor
		{"x#type": "TreeMap", "x#keySerializer": "LONG",
			"x#valueSerializer": "STRING", "x#rootRecidRecid": "2",
			"x#maxNodeSize": "3"}, // maxNodeSize < 4
		{"x#type": "TreeMap", "x#keySerializer": "LONG",
			"x#valueSerializer": "STRING", "x#rootRecidRecid": "2",
			"x#maxNodeSize": "32", "x#valueInline": "yes"}, // bad boolean
		{"x": "1"}, // key without '#'
	}
	for i, cat := range cases {
		func() {
			defer func() {
				r := recover()
				de, ok := mapdb5.AsDBError(r)
				if !ok || de.Kind != mapdb5.KindDataCorruption {
					t.Fatalf("case %d: panic = %v, want DataCorruption", i, r)
				}
			}()
			validateCatalog(cat)
		}()
	}
	// Legal minimal catalogs validate.
	for i, cat := range []NameCatalog{
		{"x#type": "AtomicLong", "x#recid": "2"},
		{"x#type": "TreeMap", "x#keySerializer": "LONG",
			"x#valueSerializer": "STRING", "x#rootRecidRecid": "2",
			"x#maxNodeSize": "32", "x#counterRecid": "0", "x#valueInline": "true"},
		{"q#type": "Queue", "q#headerRecid": "5", "q#serializer": "STRING"},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("valid case %d panicked: %v", i, r)
				}
			}()
			validateCatalog(cat)
		}()
	}
}

func TestCatalogWireGoldenEmpty(t *testing.T) {
	// Empty catalog must serialize to exactly 10 bytes (MDBC v1).
	w := bin.NewWriter()
	catalogSer{}.Serialize(w, NameCatalog{})
	if got := w.Bytes(); !bytes.Equal(got,
		[]byte{0x4D, 0x44, 0x42, 0x43, 0x00, 0x00, 0x00, 0x01, 0x00, 0x80}) {
		t.Fatalf("empty catalog = %x", got)
	}
}

// ============================ W4 review regressions ============================

// wantPanicK asserts f panics with a *DBError of the given kind.
func wantPanicK(t *testing.T, kind mapdb5.Kind, what string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		de, ok := mapdb5.AsDBError(r)
		if !ok || de.Kind != kind {
			t.Fatalf("%s: panic = %v, want %v", what, r, kind)
		}
	}()
	f()
}

// Finding 1: a failed Make over a WAL with FileDeleteAfterClose must NOT
// delete the existing namespace — the delete-on-close flag is armed only
// after the catalog validates.
func TestWALFailedOpenPreservesNamespace(t *testing.T) {
	f := freshFile(t)
	// Pollute recid 1 with a non-catalog record, committed.
	wal, err := store.OpenStoreWAL(f, true)
	if err != nil {
		t.Fatal(err)
	}
	recid := store.Put(wal, int64(0xDEAD), ser.Long)
	wal.Commit()
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if recid != RecidCatalog {
		t.Fatalf("recid = %d, want 1", recid)
	}
	segs, err := filepath.Glob(f + ".wal.*")
	if err != nil || len(segs) == 0 {
		t.Fatalf("no wal segments before open: %v %v", segs, err)
	}
	if _, err := FileDB(f).TransactionEnable().FileDeleteAfterClose().Make(); err == nil {
		t.Fatal("expected catalog rejection")
	}
	rest, err := filepath.Glob(f + ".wal.*")
	if err != nil || len(rest) == 0 {
		t.Fatalf("wal namespace deleted by failed open: %v", rest)
	}
	// The namespace must also still be openable (lock was released).
	wal2, err := store.OpenStoreWAL(f, true)
	if err != nil {
		t.Fatalf("reopen after failed make: %v", err)
	}
	if v, ok := store.Get(wal2, recid, ser.Long); !ok || v != 0xDEAD {
		t.Fatal("record lost")
	}
	_ = wal2.Close()
}

// Finding 2: a stale atomic handle must panic KindClosed once the DB
// retires it — never write a recid the store has since reused.
func TestStaleAtomicHandleInvalidated(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		d := memDB(t)
		old, err := d.AtomicLongInit("old", 1).Create()
		if err != nil {
			t.Fatal(err)
		}
		if !d.Delete("old") {
			t.Fatal("delete")
		}
		next, err := d.AtomicLongInit("next", 2).Create()
		if err != nil {
			t.Fatal(err)
		}
		wantPanicK(t, mapdb5.KindClosed, "stale Set", func() { old.Set(99) })
		wantPanicK(t, mapdb5.KindClosed, "stale Get", func() { old.Get() })
		if v := next.Get(); v != 2 {
			t.Fatalf("stale handle overwrote reused recid: %d", v)
		}
	})
	t.Run("close", func(t *testing.T) {
		d, err := MemoryDB().Make()
		if err != nil {
			t.Fatal(err)
		}
		a, err := d.AtomicBooleanInit("b", true).Create()
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		wantPanicK(t, mapdb5.KindClosed, "Get after close", func() { a.Get() })
	})
	t.Run("rollback", func(t *testing.T) {
		d := fileDB(t, freshFile(t))
		defer d.Close()
		a, err := d.AtomicLongInit("a", 7).Create()
		if err != nil {
			t.Fatal(err)
		}
		d.Commit()
		a.Set(8)
		d.Rollback()
		wantPanicK(t, mapdb5.KindClosed, "Get after rollback", func() { a.Get() })
		wantPanicK(t, mapdb5.KindClosed, "Set after rollback", func() { a.Set(1) })
		wantPanicK(t, mapdb5.KindClosed, "CAS after rollback",
			func() { a.CompareAndSet(0, 1) })
		// Reopen by name: committed state (7) is back.
		b, err := d.AtomicLong("a").Open()
		if err != nil {
			t.Fatal(err)
		}
		if v := b.Get(); v != 7 {
			t.Fatalf("reopened a = %d, want 7", v)
		}
	})
}

// Finding 3: rollback must release DB-owned map/set leases and invalidate
// the handles, so a fresh open does not report the tree still locked.
func TestRollbackReleasesMapAndSetLeases(t *testing.T) {
	f := freshFile(t)
	d := fileDB(t, f)
	defer d.Close()
	m, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).
		CreateOrOpen()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(1, "one")
	st, err := TreeSet[int64](d, "s", ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	st.Add(9)
	d.Commit()
	d.Rollback()
	// Stale handles panic; the leases are released.
	wantPanicK(t, mapdb5.KindClosed, "stale map Get", func() { m.Get(1) })
	wantPanicK(t, mapdb5.KindClosed, "stale set Contains", func() { st.Contains(9) })
	// Reopen by name must succeed (no KindAlreadyOpen).
	m2, err := TreeMap[int64, string](d, "m", ser.LongFormat, ser.StringGroupFormat).Open()
	if err != nil {
		t.Fatalf("map reopen after rollback: %v", err)
	}
	if v, ok := m2.Get(1); !ok || v != "one" {
		t.Fatalf("reopened m[1] = %v,%v", v, ok)
	}
	s2, err := TreeSet[int64](d, "s", ser.LongFormat).Open()
	if err != nil {
		t.Fatalf("set reopen after rollback: %v", err)
	}
	if !s2.Contains(9) {
		t.Fatal("reopened set lost 9")
	}
}

// Finding 4: bounded navigation clamps the probe to the view bounds
// instead of reporting absence when the backing-map candidate is outside.
func TestSetViewBoundedNavigation(t *testing.T) {
	d := memDB(t)
	s, err := TreeSet[int64](d, "s", ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 10; i++ {
		s.Add(i)
	}
	v := s.SubSet(3, true, 7, true)
	check := func(want int64, ok bool, got int64, gok bool, what string) {
		if ok != gok || got != want {
			t.Fatalf("%s = %v,%v want %v,%v", what, got, gok, want, ok)
		}
	}
	g, ok := v.Lower(10)
	check(7, true, g, ok, "view Lower(10)")
	g, ok = v.Floor(10)
	check(7, true, g, ok, "view Floor(10)")
	g, ok = v.Ceiling(1)
	check(3, true, g, ok, "view Ceiling(1)")
	g, ok = v.Higher(1)
	check(3, true, g, ok, "view Higher(1)")
	// Exclusive endpoints.
	ve := s.SubSet(3, false, 7, false)
	g, ok = ve.Ceiling(3)
	check(4, true, g, ok, "exclusive view Ceiling(3)")
	g, ok = ve.Ceiling(1)
	check(4, true, g, ok, "exclusive view Ceiling(1)")
	g, ok = ve.Floor(7)
	check(6, true, g, ok, "exclusive view Floor(7)")
	g, ok = ve.Floor(10)
	check(6, true, g, ok, "exclusive view Floor(10)")
	// Descending orientation inherits the same clamping.
	dv := v.Descending()
	g, ok = dv.Lower(3) // strictly before 3 descending = higher than 3 ascending
	check(4, true, g, ok, "desc view Lower(3)")
	g, ok = dv.Higher(8) // strictly after 8 descending = lower than 8 ascending
	check(7, true, g, ok, "desc view Higher(8)")
	// Desc Ceiling = ascending floor; Floor = ascending ceiling.
	g, ok = dv.Ceiling(8) // greatest asc <= 8 within [3,7] = 7 (clamped)
	check(7, true, g, ok, "desc view Ceiling(8)")
	g, ok = dv.Floor(2) // smallest asc >= 2 within [3,7] = 3 (clamped)
	check(3, true, g, ok, "desc view Floor(2)")
	g, ok = dv.Ceiling(0) // greatest asc <= 0 — outside the view
	check(0, false, g, ok, "desc view Ceiling(0)")
	g, ok = dv.Floor(11) // smallest asc >= 11 — outside the view
	check(0, false, g, ok, "desc view Floor(11)")
}

// Finding 5: nested view bounds must satisfy the parent's bounds — an
// out-of-range or excluded-endpoint bound is rejected, not silently
// tightened.
func TestNestedViewBoundValidation(t *testing.T) {
	d := memDB(t)
	s, err := TreeSet[int64](d, "s", ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 10; i++ {
		s.Add(i)
	}
	v := s.SubSet(3, true, 7, true)
	wantPanicK(t, mapdb5.KindMisuse, "HeadSet(9) on [3,7]",
		func() { v.HeadSet(9, true) })
	wantPanicK(t, mapdb5.KindMisuse, "TailSet(2) on [3,7]",
		func() { v.TailSet(2, true) })
	wantPanicK(t, mapdb5.KindMisuse, "SubSet(3,8) on [3,7]",
		func() { v.SubSet(3, true, 8, true) })
	// Inclusive bound on an excluded parent endpoint is rejected.
	ve := s.SubSet(3, false, 7, false)
	wantPanicK(t, mapdb5.KindMisuse, "TailSet(3,true) on (3,7)",
		func() { ve.TailSet(3, true) })
	wantPanicK(t, mapdb5.KindMisuse, "HeadSet(7,true) on (3,7)",
		func() { ve.HeadSet(7, true) })
	// Exclusive bounds at excluded endpoints are accepted (still inside).
	_ = ve.TailSet(3, false)
	_ = ve.HeadSet(7, false)
	// Inclusive bounds at inclusive endpoints are accepted.
	_ = v.TailSet(3, true)
	_ = v.HeadSet(7, true)
	// Descending views validate in their own orientation.
	dv := v.Descending()
	wantPanicK(t, mapdb5.KindMisuse, "desc TailSet(9) on [3,7]",
		func() { dv.TailSet(9, true) })
	wantPanicK(t, mapdb5.KindMisuse, "desc HeadSet(2) on [3,7]",
		func() { dv.HeadSet(2, true) })
}

// Finding 6: an invalid-UTF-8 codec descriptor in the catalog is
// corruption, not a wrong-configuration mismatch.
func TestMalformedDescriptorUTF8Rejected(t *testing.T) {
	d := memDB(t)
	// Build a real map so the catalog row is otherwise valid, then tamper
	// the keySerializer descriptor with non-UTF-8 base64 text
	// (ARRAY:_w:TE9ORw — _w decodes to 0xff).
	if _, err := TreeMap[int64, int64](d, "t", ser.LongFormat, ser.LongFormat).Create(); err != nil {
		t.Fatal(err)
	}
	cat := d.GetNameCatalog()
	cat["t#keySerializer"] = "ARRAY:_w:TE9ORw"
	store.Update(d.Store(), RecidCatalog, cat, catalogSer{})
	// Catalog validation rejects the malformed descriptor at open as
	// data corruption (not wrong-configuration).
	_, err := NewDB(d.Store())
	wantErr(t, err, mapdb5.KindDataCorruption, "NewDB over tampered catalog")
}

// ===================== r2 review: handle lifetime =====================

// R2 finding 1: rollback retirement drains in-flight ops before rewinding
// — a Put paused inside its sync listener holds an op admission, so
// Rollback must block until it completes; the write lands before the
// rewind and never survives it.
func TestRollbackDrainsInFlightMapOp(t *testing.T) {
	f := freshFile(t)
	d := fileDB(t, f)
	defer d.Close()
	m, err := TreeMap[int64, int64](d, "m", ser.LongFormat, ser.LongFormat).CreateOrOpen()
	if err != nil {
		t.Fatal(err)
	}
	m.Put(1, 1)
	d.Commit()

	entered := make(chan struct{})
	release := make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error {
			if armed.Swap(false) {
				close(entered)
				<-release
			}
			return nil
		}))

	putDone := make(chan struct{})
	go func() { defer close(putDone); m.Put(1, 99) }()
	<-entered

	rbDone := make(chan struct{})
	go func() { defer close(rbDone); d.Rollback() }()
	select {
	case <-rbDone:
		t.Fatal("Rollback returned while an admitted op was still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-putDone
	<-rbDone

	// Reopen: the drained write was rewound — only the committed value.
	m2, err := TreeMap[int64, int64](d, "m", ser.LongFormat, ser.LongFormat).Open()
	if err != nil {
		t.Fatalf("reopen after rollback: %v", err)
	}
	if v, ok := m2.Get(1); !ok || v != 1 {
		t.Fatalf("reopened m[1] = %v,%v want 1,true", v, ok)
	}
	// The fresh handle is fully live (second lock domain healthy).
	m2.Put(2, 2)
	d.Commit()
	if v, ok := m2.Get(2); !ok || v != 2 {
		t.Fatalf("post-rollback write: %v,%v", v, ok)
	}
}

// R2 finding 1 (close path): DB.Close must also drain an admitted op
// before returning.
func TestDBCloseDrainsInFlightMapOp(t *testing.T) {
	d := memDB(t)
	m, err := TreeMap[int64, int64](d, "m", ser.LongFormat, ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	m.ModificationListenerAdd(mapdb5.SyncModificationListenerFunc[int64, int64](
		func(k int64, o, n *int64, tr bool) error {
			if armed.Swap(false) {
				close(entered)
				<-release
			}
			return nil
		}))
	putDone := make(chan struct{})
	go func() { defer close(putDone); m.Put(1, 99) }()
	<-entered
	closeDone := make(chan struct{})
	go func() { defer close(closeDone); _ = d.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close returned while an admitted op was still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-putDone
	<-closeDone
}

// failUpdateStore injects a DBError on Update of one recid while armed —
// used to fail the catalog save mid-Create.
type failUpdateStore struct {
	store.Store
	failRecid store.Recid
	armed     atomic.Bool
}

func (s *failUpdateStore) Update(recid store.Recid, record any, sr ser.AnySerializer) {
	if s.armed.Load() && recid == s.failRecid {
		panic(mapdb5.New(mapdb5.KindVolumeIO, "injected catalog write failure"))
	}
	s.Store.Update(recid, record, sr)
}

// LeaseTable forwards the delegate's lease capability — the embedded
// Store interface does not promote StoreLease.
func (s *failUpdateStore) LeaseTable() *store.LeaseTable {
	return s.Store.(store.StoreLease).LeaseTable()
}

// R2 finding 3: a map/set built but never cached (catalog publish fails)
// must release its open lease — an orphan reports the tree locked forever.
func TestCreateFailureReleasesUnpublishedLease(t *testing.T) {
	for _, what := range []string{"map", "set"} {
		t.Run(what, func(t *testing.T) {
			fs := &failUpdateStore{Store: store.NewStoreOnHeap(true), failRecid: RecidCatalog}
			d, err := NewDB(fs)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			fs.armed.Store(true)
			var cerr error
			if what == "map" {
				_, cerr = TreeMap[int64, int64](d, "x", ser.LongFormat, ser.LongFormat).Create()
			} else {
				_, cerr = TreeSet[int64](d, "x", ser.LongFormat).Create()
			}
			fs.armed.Store(false)
			if cerr == nil {
				t.Fatal("Create must fail on the injected catalog write")
			}
			// No orphaned lease survives: every record's lease must be
			// acquirable — the failed build's root-pointer lease is the
			// one a bug would leave held.
			for recid := range fs.AllRecids() {
				g, lerr := store.AcquireLease(fs, recid, store.LeaseReadWrite)
				if lerr != nil {
					t.Fatalf("orphaned lease on recid %d: %v", recid, lerr)
				}
				g.Release()
			}
		})
	}
}

// BTreeMap over StoreAppendOnly (Java memoryAppendOnlyDB): node rewrites go
// to fresh extents; enough churn that the store's own data area grows.
func TestTreeMapOnAppendOnlyDB(t *testing.T) {
	d, err := MemoryAppendOnlyDB().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	m, err := TreeMap[int64, string](d, "t", ser.LongFormat, ser.StringGroupFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 5000; i++ {
		m.Put(i, fmt.Sprintf("v%d", i))
	}
	for i := int64(0); i < 5000; i += 2 {
		m.Remove(i)
	}
	if v, ok := m.Get(4001); !ok || v != "v4001" {
		t.Fatalf("get = %v,%v", v, ok)
	}
	if _, ok := m.Get(4000); ok {
		t.Fatal("removed key present")
	}
	m.Verify()
	d.Store().Verify()
}
