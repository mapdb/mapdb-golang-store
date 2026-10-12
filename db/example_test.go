package db_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mapdb/mapdb5-go/db"
	"github.com/mapdb/mapdb5-go/ser"
)

// A file-backed, transactional DB with one persistent sorted map.
func Example() {
	dir, _ := os.MkdirTemp("", "mapdb5-example")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "example.db")

	d, err := db.FileDB(path).TransactionEnable().Make()
	if err != nil {
		panic(err)
	}
	m, err := db.TreeMap[int64, string](d, "users", ser.LongFormat, ser.StringGroupFormat).CreateOrOpen()
	if err != nil {
		panic(err)
	}
	m.Put(1, "alice")
	m.Put(2, "bob")
	d.Commit()
	if err := d.Close(); err != nil {
		panic(err)
	}

	// Reopen: the catalog finds the map by name.
	d, err = db.FileDB(path).TransactionEnable().Make()
	if err != nil {
		panic(err)
	}
	defer d.Close()
	m, err = db.TreeMap[int64, string](d, "users", ser.LongFormat, ser.StringGroupFormat).Open()
	if err != nil {
		panic(err)
	}
	v, ok := m.Get(2)
	fmt.Println(v, ok, m.LenLong())
	// Output: bob true 2
}
