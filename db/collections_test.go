package db

import (
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/htree"
	"github.com/mapdb/mapdb5-go/indextree"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
	"github.com/mapdb/mapdb5-go/sortedtable"
	"github.com/mapdb/mapdb5-go/store"
)

// javaCatalogRows are the catalog rows Java DB writes for the collections
// built by the cross-language driver (mapdb-java-store target/classes,
// 2026-10-12): every name's row with recid VALUES elided ("*" = positive
// recid, "0" = disabled counter). Go must write the same keys and values.
var javaCatalogRows = map[string]string{
	"cache#concShift": "4", "cache#counterRecid": "0", "cache#dirShift": "7",
	"cache#hasher": "DEFAULT", "cache#headerRecid": "*", "cache#keySerializer": "LONG",
	"cache#kind": "CACHE", "cache#levels": "4", "cache#type": "HashMap", "cache#valueSerializer": "LONG",
	"hm#concShift": "4", "hm#counterRecid": "0", "hm#dirShift": "7", "hm#hasher": "DEFAULT",
	"hm#headerRecid": "*", "hm#keySerializer": "LONG", "hm#kind": "PLAIN", "hm#levels": "4",
	"hm#type": "HashMap", "hm#valueSerializer": "STRING",
	"hm48#concShift": "6", "hm48#dirShift": "6", "hm48#hasher64": "LONG64", "hm48#headerRecid": "*",
	"hm48#keySerializer": "LONG", "hm48#levels": "7", "hm48#type": "HashMap48", "hm48#valueSerializer": "LONG",
	"hmc#concShift": "4", "hmc#counterRecid": "*", "hmc#dirShift": "7", "hmc#hasher": "DEFAULT",
	"hmc#headerRecid": "*", "hmc#keySerializer": "LONG", "hmc#kind": "PLAIN", "hmc#levels": "4",
	"hmc#type": "HashMap", "hmc#valueSerializer": "LONG",
	"hmx#concShift": "4", "hmx#counterRecid": "0", "hmx#dirShift": "7", "hmx#hasher": "DEFAULT",
	"hmx#headerRecid": "*", "hmx#keySerializer": "STRING", "hmx#kind": "EXTERNAL", "hmx#levels": "4",
	"hmx#type": "HashMap", "hmx#valueSerializer": "LONG",
	"hs#concShift": "4", "hs#counterRecid": "0", "hs#dirShift": "7", "hs#hasher": "DEFAULT",
	"hs#headerRecid": "*", "hs#kind": "PLAIN", "hs#levels": "4", "hs#serializer": "STRING", "hs#type": "HashSet",
	"hs48#concShift": "6", "hs48#dirShift": "6", "hs48#hasher64": "DEFAULT64", "hs48#headerRecid": "*",
	"hs48#levels": "7", "hs48#serializer": "STRING", "hs48#type": "HashSet48",
	"il#headerRecid": "*", "il#serializer": "STRING", "il#type": "IndexTreeList",
	"ll#headerRecid": "*", "ll#type": "IndexTreeLongLongMap",
	"st#entriesPerPage": "64", "st#headerRecid": "*", "st#keySerializer": "LONG",
	"st#type": "SortedTableMap", "st#valueSerializer": "STRING",
}

// buildDriverCollections creates the same collections, with the same data,
// as the Java driver.
func buildDriverCollections(t *testing.T, d *DB) {
	t.Helper()
	hm, err := HashMap[int64, string](d, "hm", ser.Long, ser.String).Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 100; i++ {
		hm.Put(i, "v"+strconv.FormatInt(i, 10))
	}
	hs, err := HashSet[string](d, "hs", ser.String).Create()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []string{"a", "b", "c"} {
		hs.Add(e)
	}
	hm48, err := HashMap48[int64, int64](d, "hm48", ser.Long, ser.Long).Hasher(mdhash.Long64).Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 50; i++ {
		hm48.Put(i, i*10)
	}
	hs48, err := HashSet48[string](d, "hs48", ser.String).Create()
	if err != nil {
		t.Fatal(err)
	}
	hs48.Add("x")
	hs48.Add("y")
	hmc, err := HashMap[int64, int64](d, "hmc", ser.Long, ser.Long).CounterEnable().Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 7; i++ {
		hmc.Put(i, i)
	}
	hmx, err := HashMap[string, int64](d, "hmx", ser.String, ser.Long).ValueInline(false).Create()
	if err != nil {
		t.Fatal(err)
	}
	hmx.Put("k1", 1)
	hmx.Put("k2", 2)
	cache, err := HashMap[int64, int64](d, "cache", ser.Long, ser.Long).ExpireAfterCreate(1_000_000_000_000).Create()
	if err != nil {
		t.Fatal(err)
	}
	cache.Put(3, 33)
	src := func(yield func(int64, string) bool) {
		for i := int64(0); i < 300; i++ {
			if !yield(i*2, "s"+strconv.FormatInt(i, 10)) {
				return
			}
		}
	}
	if _, err := SortedTableMap[int64, string](d, "st", ser.LongFormat, ser.StringGroupFormat).PageSize(64).CreateFrom(src); err != nil {
		t.Fatal(err)
	}
	il, err := IndexTreeList[string](d, "il", ser.String).Create()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []string{"p", "q", "r"} {
		il.Add(e)
	}
	ll, err := d.IndexTreeLongLongMap("ll", -1).Create()
	if err != nil {
		t.Fatal(err)
	}
	ll.Put(5, 50)
	ll.Put(1<<40, 7)
}

// verifyDriverCollections reopens every collection and checks the driver's
// data — over a Go-written or a Java-written file.
func verifyDriverCollections(t *testing.T, d *DB) {
	t.Helper()
	hm, err := HashMap[int64, string](d, "hm", ser.Long, ser.String).Open()
	if err != nil {
		t.Fatal(err)
	}
	if hm.LenLong() != 100 {
		t.Fatalf("hm len = %d", hm.LenLong())
	}
	for i := int64(1); i <= 100; i++ {
		if v, ok := hm.Get(i); !ok || v != "v"+strconv.FormatInt(i, 10) {
			t.Fatalf("hm[%d] = %q,%v", i, v, ok)
		}
	}
	hs, err := HashSet[string](d, "hs", ser.String).Open()
	if err != nil {
		t.Fatal(err)
	}
	if !hs.Contains("a") || !hs.Contains("c") || hs.Contains("z") || hs.LenLong() != 3 {
		t.Fatalf("hs content wrong: %v", slices.Collect(hs.All()))
	}
	hm48, err := HashMap48[int64, int64](d, "hm48", ser.Long, ser.Long).Open()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 50; i++ {
		if v, ok := hm48.Get(i); !ok || v != i*10 {
			t.Fatalf("hm48[%d] = %d,%v", i, v, ok)
		}
	}
	hs48, err := HashSet48[string](d, "hs48", ser.String).Open()
	if err != nil {
		t.Fatal(err)
	}
	if !hs48.Contains("x") || !hs48.Contains("y") || hs48.LenLong() != 2 {
		t.Fatal("hs48 content wrong")
	}
	hmc, err := HashMap[int64, int64](d, "hmc", ser.Long, ser.Long).Open()
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := hmc.(*CountedHashMap[int64, int64]); !ok || c.LenLong() != 7 {
		t.Fatalf("hmc = %T len %d, want counted 7", hmc, hmc.LenLong())
	}
	hmx, err := HashMap[string, int64](d, "hmx", ser.String, ser.Long).Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := hmx.(*htree.HTreeMapExternal[string, int64]); !ok {
		t.Fatalf("hmx = %T", hmx)
	}
	if v, ok := hmx.Get("k2"); !ok || v != 2 {
		t.Fatalf("hmx[k2] = %d,%v", v, ok)
	}
	cache, err := HashMap[int64, int64](d, "cache", ser.Long, ser.Long).Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.(*htree.HTreeCache[int64, int64]); !ok {
		t.Fatalf("cache = %T", cache)
	}
	if v, ok := cache.Get(3); !ok || v != 33 {
		t.Fatalf("cache[3] = %d,%v", v, ok)
	}
	st, err := SortedTableMap[int64, string](d, "st", ser.LongFormat, ser.StringGroupFormat).Open()
	if err != nil {
		t.Fatal(err)
	}
	if st.LenLong() != 300 {
		t.Fatalf("st len = %d", st.LenLong())
	}
	if v, ok := st.Get(598); !ok || v != "s299" {
		t.Fatalf("st[598] = %q,%v", v, ok)
	}
	if _, ok := st.Get(1); ok {
		t.Fatal("st[1] present")
	}
	il, err := IndexTreeList[string](d, "il", ser.String).Open()
	if err != nil {
		t.Fatal(err)
	}
	if il.Len() != 3 || il.Get(1) != "q" {
		t.Fatalf("il = %v", slices.Collect(il.All()))
	}
	ll, err := d.IndexTreeLongLongMap("ll", 0).Open()
	if err != nil {
		t.Fatal(err)
	}
	if ll.Get(5) != 50 || ll.Get(1<<40) != 7 || ll.Get(6) != -1 {
		t.Fatalf("ll = %d %d %d", ll.Get(5), ll.Get(1<<40), ll.Get(6))
	}
}

func checkJavaRows(t *testing.T, cat NameCatalog) {
	t.Helper()
	if len(cat) != len(javaCatalogRows) {
		t.Fatalf("catalog has %d rows, Java %d: %v", len(cat), len(javaCatalogRows), cat)
	}
	for k, want := range javaCatalogRows {
		got, ok := cat[k]
		switch {
		case !ok:
			t.Errorf("missing %s", k)
		case want == "*":
			if n, err := strconv.ParseUint(got, 10, 64); err != nil || n == 0 {
				t.Errorf("%s = %q, want a positive recid", k, got)
			}
		case got != want:
			t.Errorf("%s = %q, Java writes %q", k, got, want)
		}
	}
}

// Every M7 collection type: Go writes Java's catalog rows, survives
// commit/close/reopen, and reopens read-only.
func TestCollectionsCatalogRowsReopenReadOnly(t *testing.T) {
	path := freshFile(t)
	d, err := FileDB(path).Make()
	if err != nil {
		t.Fatal(err)
	}
	buildDriverCollections(t, d)
	checkJavaRows(t, d.GetNameCatalog())
	d.Commit()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = FileDB(path).Make()
	if err != nil {
		t.Fatal(err)
	}
	verifyDriverCollections(t, d)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := FileDB(path).ReadOnly().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	verifyDriverCollections(t, ro)
	hm, err := HashMap[int64, string](ro, "hm", ser.Long, ser.String).Open()
	if err != nil {
		t.Fatal(err)
	}
	wantPanicK(t, mapdb5.KindReadOnly, "read-only put", func() { hm.Put(1, "x") })
}

// The same collections over a transactional (WAL) file DB, plus rollback
// retirement of a hash map handle.
func TestCollectionsWALReopenAndRollback(t *testing.T) {
	path := freshFile(t)
	d := fileDB(t, path)
	buildDriverCollections(t, d)
	d.Commit()
	hm, err := HashMap[int64, string](d, "hm", ser.Long, ser.String).Open()
	if err != nil {
		t.Fatal(err)
	}
	hm.Put(1000, "uncommitted")
	d.Rollback()
	if !hm.IsClosed() {
		t.Fatal("rollback did not retire the hash map handle")
	}
	verifyDriverCollections(t, d)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = fileDB(t, path)
	defer d.Close()
	verifyDriverCollections(t, d)
	hm, err = HashMap[int64, string](d, "hm", ser.Long, ser.String).Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := hm.Get(1000); ok {
		t.Fatal("rolled-back entry survived")
	}
}

// Java-written file (MAPDB5_JAVA_XOPEN_DB, produced by the Java driver
// that also yields javaCatalogRows) opens in Go with identical contents.
// Skipped unless the env var names such a file.
func TestCollectionsJavaCrossOpen(t *testing.T) {
	path := os.Getenv("MAPDB5_JAVA_XOPEN_DB")
	if path == "" {
		t.Skip("MAPDB5_JAVA_XOPEN_DB not set")
	}
	d, err := FileDB(path).ReadOnly().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	checkJavaRows(t, d.GetNameCatalog())
	verifyDriverCollections(t, d)
}

func TestCollectionsTypeMismatchDeleteRename(t *testing.T) {
	d := memDB(t)
	buildDriverCollections(t, d)
	_, err := HashSet[int64](d, "hm", ser.Long).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "HashSet over HashMap")
	_, err = HashMap[int64, int64](d, "hm48", ser.Long, ser.Long).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "HashMap over HashMap48")
	_, err = IndexTreeList[string](d, "ll", ser.String).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "IndexTreeList over IndexTreeLongLongMap")
	_, err = HashMap[int64, int64](d, "hm", ser.Long, ser.Long).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "value serializer mismatch")
	_, err = HashMap[int64, string](d, "hm", ser.Long, ser.String).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "create over existing")

	hm, _ := HashMap[int64, string](d, "hm", ser.Long, ser.String).Open()
	if !d.Delete("hm") || d.Exists("hm") {
		t.Fatal("delete hm")
	}
	if !hm.IsClosed() {
		t.Fatal("deleted hash map handle not retired")
	}
	for _, n := range []string{"hs", "hs48", "hmc", "st", "il", "ll"} {
		if !d.Delete(n) {
			t.Fatalf("delete %s", n)
		}
	}
	d.Rename("hmx", "hmx2")
	hmx, err := HashMap[string, int64](d, "hmx2", ser.String, ser.Long).Open()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := hmx.Get("k1"); !ok || v != 1 {
		t.Fatal("renamed map lost data")
	}
	d.Store().Verify()
}

func TestHashMapHasherMarkers(t *testing.T) {
	d := memDB(t)
	// []byte keys: no Go hashCode — serialized-bytes fallback, recorded
	// as a Go CUSTOM marker; reopen needs no hasher.
	bm, err := HashMap[[]byte, int64](d, "bytes", ser.ByteArray, ser.Long).Create()
	if err != nil {
		t.Fatal(err)
	}
	bm.Put([]byte("k"), 1)
	if got := d.GetNameCatalog()["bytes#hasher"]; got != goSerHasher32Marker {
		t.Fatalf("bytes#hasher = %q", got)
	}

	// DEFAULT rejects a supplied hasher on reopen (Java resolveHasher).
	if _, err := HashMap[int64, int64](d, "def", ser.Long, ser.Long).Create(); err != nil {
		t.Fatal(err)
	}
	// The hasher check runs before the cache lookup, so a cached handle
	// cannot mask the mismatch.
	_, err = HashMap[int64, int64](d, "def", ser.Long, ser.Long).Hasher(mdhash.Long32).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "DEFAULT + supplied hasher")

	// A supplied hasher is CUSTOM and must be re-supplied.
	if _, err := HashMap[int64, int64](d, "cus", ser.Long, ser.Long).Hasher(mdhash.Long32).Create(); err != nil {
		t.Fatal(err)
	}
	if got := d.GetNameCatalog()["cus#hasher"]; got != "CUSTOM" {
		t.Fatalf("cus#hasher = %q", got)
	}
	_, err = HashMap[int64, int64](d, "cus", ser.Long, ser.Long).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "CUSTOM without hasher")
	if _, err := HashMap[int64, int64](d, "cus", ser.Long, ser.Long).Hasher(mdhash.Long32).Open(); err != nil {
		t.Fatal(err)
	}

	// 48-bit: built-ins are recorded by name; a different supplied hasher
	// mismatches.
	if _, err := HashMap48[int64, int64](d, "w", ser.Long, ser.Long).Hasher(mdhash.Long64).Create(); err != nil {
		t.Fatal(err)
	}
	other := mdhash.Mixing64[int64](func(k int64) int64 { return k })
	_, err = HashMap48[int64, int64](d, "w", ser.Long, ser.Long).Hasher(other).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "LONG64 vs custom")
	if _, err := HashMap48[int64, int64](d, "w", ser.Long, ser.Long).Open(); err != nil {
		t.Fatalf("LONG64 reopen without hasher: %v", err)
	}
}

func TestHashMapCacheOptions(t *testing.T) {
	d := memDB(t)
	now := int64(1000)
	clock := func() int64 { return now }
	var evicted []int64
	c, err := HashMap[int64, int64](d, "c", ser.Long, ser.Long).
		ExpireAfterCreate(10).ExpireClock(clock).
		ExpireEvictionListener(func(k, _ int64, _ htree.EvictionCause) { evicted = append(evicted, k) }).
		Create()
	if err != nil {
		t.Fatal(err)
	}
	c.Put(1, 1)
	now += 11
	if _, ok := c.Get(1); ok {
		t.Fatal("expired entry visible")
	}
	c.(*htree.HTreeCache[int64, int64]).ExpireEvict()
	if !slices.Equal(evicted, []int64{1}) {
		t.Fatalf("evicted = %v", evicted)
	}

	_, err = HashMap[int64, int64](d, "x1", ser.Long, ser.Long).ExpireEvictionListener(func(int64, int64, htree.EvictionCause) {}).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "listener without expiry")
	_, err = HashMap[int64, int64](d, "x2", ser.Long, ser.Long).ValueInline(false).ExpireMaxSize(5).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "external + expiry")
	_, err = HashMap[int64, int64](d, "x3", ser.Long, ser.Long).ExpireAfterCreate(5).ExpireAfterGet(5).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "write + access TTL")
	_, err = HashMap[int64, int64](d, "x4", ser.Long, ser.Long).ExpireAfterCreate(5).ExpireAfterUpdate(6).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "create != update TTL")
	if _, err := HashMap[int64, int64](d, "p", ser.Long, ser.Long).Create(); err != nil {
		t.Fatal(err)
	}
	_, err = HashMap[int64, int64](d, "p", ser.Long, ser.Long).ExpireOverflow(c).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "overflow on a PLAIN map")
	for _, n := range []string{"x1", "x2", "x3", "x4"} {
		if d.Exists(n) {
			t.Fatalf("failed create registered %s", n)
		}
	}
}

func TestHashSetCacheAndCounter(t *testing.T) {
	path := freshFile(t)
	d := fileDB(t, path)
	s, err := HashSet[int64](d, "s", ser.Long).CounterEnable().Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 20; i++ {
		s.Add(i)
	}
	s.Remove(3)
	sc, err := HashSet[int64](d, "sc", ser.Long).ExpireMaxSize(1000).Create()
	if err != nil {
		t.Fatal(err)
	}
	sc.Add(9)
	if got := d.GetNameCatalog()["sc#kind"]; got != "CACHE" {
		t.Fatalf("sc#kind = %q", got)
	}
	d.Commit()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = fileDB(t, path)
	defer d.Close()
	s, err = HashSet[int64](d, "s", ser.Long).Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.BackingMap().(*CountedHashMap[int64, struct{}]); !ok || s.LenLong() != 19 {
		t.Fatalf("counted set = %T len %d", s.BackingMap(), s.LenLong())
	}
	s.Clear()
	if s.LenLong() != 0 || !s.IsEmpty() {
		t.Fatalf("after clear len %d", s.LenLong())
	}
	sc, err = HashSet[int64](d, "sc", ser.Long).Open()
	if err != nil {
		t.Fatal(err)
	}
	if !sc.Contains(9) {
		t.Fatal("cache set lost element")
	}
}

func TestSortedTableAndIndexTreeFacade(t *testing.T) {
	d := memDB(t)
	sink := SortedTableMap[int64, int64](d, "st", ser.LongFormat, ser.LongFormat).PageSize(4).CreateFromSink()
	for i := int64(0); i < 10; i++ {
		sink.Put(i, i*i)
	}
	st, err := sink.Create()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := st.Get(9); !ok || v != 81 {
		t.Fatal("sink-built table wrong")
	}
	if got := d.GetNameCatalog()["st#entriesPerPage"]; got != "4" {
		t.Fatalf("entriesPerPage = %q", got)
	}
	unsorted := func(yield func(int64, int64) bool) { _ = yield(2, 0) && yield(1, 0) }
	_, err = SortedTableMap[int64, int64](d, "bad", ser.LongFormat, ser.LongFormat).CreateFrom(unsorted)
	wantErr(t, err, mapdb5.KindNotSorted, "unsorted CreateFrom")
	if d.Exists("bad") {
		t.Fatal("unsorted table registered")
	}
	empty, err := SortedTableMap[int64, int64](d, "empty", ser.LongFormat, ser.LongFormat).CreateOrOpen()
	if err != nil || empty.LenLong() != 0 {
		t.Fatalf("empty table: %v", err)
	}
	same, _ := SortedTableMap[int64, int64](d, "empty", ser.LongFormat, ser.LongFormat).Open()
	if same != empty {
		t.Fatal("open did not return the cached instance")
	}
	_, err = SortedTableMap[int64, string](d, "st", ser.LongFormat, ser.StringGroupFormat).Open()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "value format mismatch")

	l, err := IndexTreeList[int64](d, "l", ser.Long).CreateOrOpen()
	if err != nil {
		t.Fatal(err)
	}
	l.Add(4)
	l2, _ := IndexTreeList[int64](d, "l", ser.Long).CreateOrOpen()
	if l2 != l {
		t.Fatal("list not cached")
	}
	ll, err := d.IndexTreeLongLongMap("ll", 9).CreateOrOpen()
	if err != nil || ll.Get(1) != 9 {
		t.Fatalf("ll default: %v", err)
	}
	var _ *indextree.IndexTreeLongLongMap = ll
	var _ *sortedtable.SortedTableMap[int64, int64] = st
}

// Catalog validation of the new types rejects corrupt rows at NewDB.
func TestCollectionsCatalogValidation(t *testing.T) {
	base := func() (store.Store, NameCatalog) {
		s := store.NewStoreByteArray(true)
		d, err := NewDB(s)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := HashMap[int64, int64](d, "m", ser.Long, ser.Long).Create(); err != nil {
			t.Fatal(err)
		}
		return s, d.GetNameCatalog()
	}
	cases := map[string]func(NameCatalog){
		"bad hasher":     func(c NameCatalog) { c["m#hasher"] = "MURMUR" },
		"bad kind":       func(c NameCatalog) { c["m#kind"] = "WEIRD" },
		"zero header":    func(c NameCatalog) { c["m#headerRecid"] = "0" },
		"unknown field":  func(c NameCatalog) { c["m#bogus"] = "1" },
		"missing hasher": func(c NameCatalog) { delete(c, "m#hasher") },
		"48 with kind":   func(c NameCatalog) { c["m#type"] = "HashMap48"; c["m#hasher64"] = "LONG64"; delete(c, "m#hasher") },
		"bad page fill": func(c NameCatalog) {
			c["t#type"] = "SortedTableMap"
			c["t#keySerializer"] = "LONG"
			c["t#valueSerializer"] = "LONG"
			c["t#headerRecid"] = "5"
			c["t#entriesPerPage"] = "0"
		},
		"list missing ser": func(c NameCatalog) { c["l#type"] = "IndexTreeList"; c["l#headerRecid"] = "5" },
		"long map extra": func(c NameCatalog) {
			c["x#type"] = "IndexTreeLongLongMap"
			c["x#headerRecid"] = "5"
			c["x#serializer"] = "LONG"
		},
		"bad 64 hasher": func(c NameCatalog) {
			c["w#type"] = "HashSet48"
			c["w#serializer"] = "LONG"
			c["w#headerRecid"] = "5"
			c["w#hasher64"] = "DEFAULT"
		},
		"bad set ser descr": func(c NameCatalog) {
			c["m#type"] = "HashSet"
			c["m#serializer"] = "NOPE"
			delete(c, "m#keySerializer")
			delete(c, "m#valueSerializer")
		},
	}
	for name, mutate := range cases {
		s, cat := base()
		mutate(cat)
		store.Update(s, RecidCatalog, cat, catalogSer{})
		_, err := NewDB(s)
		if de, ok := mapdb5.AsDBError(err); !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Errorf("%s: NewDB err = %v, want DataCorruption", name, err)
		} else if !strings.Contains(err.Error(), "catalog") {
			t.Errorf("%s: unexpected message %v", name, err)
		}
		_ = s.Close()
	}
}
