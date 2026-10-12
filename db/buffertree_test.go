package db

import (
	"errors"
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

func TestBufferTreeMapMaker(t *testing.T) {
	path := freshFile(t)
	d := fileDB(t, path)
	m, err := BufferTreeMap(d, "bt", ser.LongFormat, ser.LongFormat).MaxNodeSize(4).BufferBytes(64).LeafHeadroom(0).Create()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 500; i++ {
		m.PutOnly(i%97, i)
	}
	if ty, _ := d.GetType("bt"); ty != "BufferTreeMap" {
		t.Fatalf("type %q", ty)
	}
	if _, err := BufferTreeMap(d, "bt", ser.LongFormat, ser.LongFormat).Create(); err == nil {
		t.Fatal("duplicate create allowed")
	}
	same, err := BufferTreeMap(d, "bt", ser.LongFormat, ser.LongFormat).Open()
	if err != nil || same != m {
		t.Fatalf("open must return the cached handle: %v", err)
	}
	if _, err := BufferTreeMap(d, "bt", ser.StringGroupFormat, ser.LongFormat).Open(); err == nil {
		t.Fatal("key codec mismatch accepted")
	}
	d.Commit()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = fileDB(t, path)
	m, err = BufferTreeMap(d, "bt", ser.LongFormat, ser.LongFormat).Open()
	if err != nil {
		t.Fatal(err)
	}
	if m.MaxNodeSize() != 4 {
		t.Fatalf("stored maxNodeSize not restored: %d", m.MaxNodeSize())
	}
	for i := int64(500 - 97); i < 500; i++ {
		if v, ok := m.Get(i % 97); !ok || v != i {
			t.Fatalf("Get(%d)=%d,%v", i%97, v, ok)
		}
	}
	m.Verify()
	// bulk build + NotSorted registers nothing
	if _, err := BufferTreeMap(d, "bad", ser.LongFormat, ser.LongFormat).CreateFrom(func(y func(int64, int64) bool) {
		_ = y(2, 0) && y(1, 0)
	}); err == nil {
		t.Fatal("unsorted accepted")
	} else if de, ok := mapdb5.AsDBError(err); !ok || de.Kind != mapdb5.KindNotSorted {
		var de2 *mapdb5.DBError
		if !errors.As(err, &de2) {
			t.Fatalf("unsorted error %v", err)
		}
	}
	if _, ok := d.GetType("bad"); ok {
		t.Fatal("failed bulk build registered")
	}
	b, err := BufferTreeMap(d, "bulk", ser.LongFormat, ser.LongFormat).CreateFrom(func(y func(int64, int64) bool) {
		for i := int64(0); i < 100; i++ {
			if !y(i, -i) {
				return
			}
		}
	})
	if err != nil || b.LenLong() != 100 {
		t.Fatalf("bulk: %v", err)
	}
	if !d.Delete("bulk") || !b.IsClosed() {
		t.Fatal("delete did not retire the handle")
	}
	d.Commit()
}

func TestBufferTreeMapMakerHeapRejected(t *testing.T) {
	d, err := HeapDB().Make()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_, err = BufferTreeMap(d, "bt", ser.LongFormat, ser.LongFormat).Create()
	de, ok := mapdb5.AsDBError(err)
	if !ok || de.Kind != mapdb5.KindWrongConfiguration {
		t.Fatalf("heap DB: want WrongConfiguration, got %v", err)
	}
	if _, ok := d.GetType("bt"); ok {
		t.Fatal("rejected map registered")
	}
}

func TestBufferTreeCatalogValidation(t *testing.T) {
	base := NameCatalog{"x#type": "BufferTreeMap", "x#keySerializer": "LONG", "x#valueSerializer": "LONG",
		"x#rootRecidRecid": "5", "x#maxNodeSize": "8", "x#bufferBytes": "64", "x#leafHeadroom": "0"}
	validateCatalog(base) // well-formed
	for name, mut := range map[string]func(NameCatalog){
		"missing":   func(c NameCatalog) { delete(c, "x#bufferBytes") },
		"unknown":   func(c NameCatalog) { c["x#counterRecid"] = "3" },
		"smallbuf":  func(c NameCatalog) { c["x#bufferBytes"] = "31" },
		"badmns":    func(c NameCatalog) { c["x#maxNodeSize"] = "3" },
		"rootzero":  func(c NameCatalog) { c["x#rootRecidRecid"] = "0" },
		"headroomX": func(c NameCatalog) { c["x#leafHeadroom"] = "-1" },
	} {
		c := NameCatalog{}
		for k, v := range base {
			c[k] = v
		}
		mut(c)
		func() {
			defer func() {
				de, ok := mapdb5.AsDBError(recover())
				if !ok || de.Kind != mapdb5.KindDataCorruption {
					t.Fatalf("%s: want DataCorruption", name)
				}
			}()
			validateCatalog(c)
		}()
	}
}
