package db

import (
	"testing"
	"time"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

// badIdentityListener is a struct VALUE with a non-comparable field: the
// listener registries reject it (listen.CheckIdentity → WrongConfiguration).
type badIdentityListener[K, V any] struct{ xs []int }

func (badIdentityListener[K, V]) Modify(K, *V, *V, bool) error { return nil }

// Regression (fable release review #2): a Create whose listener wiring
// fails must register nothing (Java create2 configures before publishing);
// it used to persist the catalog row and cache a retired handle, wedging
// the name until reopen.
func TestCreateWithFailingWiringRegistersNothing(t *testing.T) {
	d := memDB(t)
	bad := badIdentityListener[int64, int64]{xs: []int{1}}

	_, err := HashMap[int64, int64](d, "hm", ser.Long, ser.Long).ModificationListener(bad).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "HashMap create")
	_, err = HashMap[int64, int64](d, "hmc", ser.Long, ser.Long).CounterEnable().ModificationListener(bad).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "counted HashMap create")
	_, err = TreeMap[int64, int64](d, "tm", ser.LongFormat, ser.LongFormat).ModificationListener(bad).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "TreeMap create")
	_, err = TreeMap[int64, int64](d, "tmo", ser.LongFormat, ser.LongFormat).ModificationListener(bad).CreateOrOpen()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "TreeMap createOrOpen")
	_, err = HashMap48[int64, int64](d, "h48", ser.Long, ser.Long).ModificationListener(bad).Create()
	wantErr(t, err, mapdb5.KindWrongConfiguration, "HashMap48 create")

	for _, n := range []string{"hm", "hmc", "tm", "tmo", "h48"} {
		if d.Exists(n) {
			t.Errorf("%s: failed create left a catalog row", n)
		}
	}
	// The names are free: a clean create works and the handle is live.
	hm, err := HashMap[int64, int64](d, "hm", ser.Long, ser.Long).Create()
	if err != nil {
		t.Fatal(err)
	}
	hm.Put(1, 1)
	tm, err := TreeMap[int64, int64](d, "tm", ser.LongFormat, ser.LongFormat).Create()
	if err != nil {
		t.Fatal(err)
	}
	tm.Put(1, 1)

}

// Regression (fable release review #4): a CreateFrom iterator re-entering
// the DB used to deadlock on the (non-reentrant) admin lock; it now panics
// Misuse, surfaced as CreateFrom's error, and the DB stays usable.
func TestCreateFromIteratorReentryFailsFast(t *testing.T) {
	d := memDB(t)
	done := make(chan error, 1)
	go func() {
		_, err := TreeMap[int64, int64](d, "tm", ser.LongFormat, ser.LongFormat).
			CreateFrom(func(yield func(int64, int64) bool) {
				d.Exists("other") // re-entry
				yield(1, 1)
			})
		done <- err
	}()
	select {
	case err := <-done:
		wantErr(t, err, mapdb5.KindMisuse, "re-entrant CreateFrom")
	case <-time.After(5 * time.Second):
		t.Fatal("CreateFrom iterator re-entry deadlocked")
	}
	if d.Exists("tm") {
		t.Fatal("failed CreateFrom registered the name")
	}
	if _, err := TreeMap[int64, int64](d, "tm", ser.LongFormat, ser.LongFormat).Create(); err != nil {
		t.Fatalf("DB unusable after failed CreateFrom: %v", err)
	}
}

// Regression (fable release review #10): TempFileDB never panics; a temp
// file failure is returned by Make.
func TestTempFileDBErrorFromMake(t *testing.T) {
	t.Setenv("TMPDIR", "/nonexistent-mapdb5-tmpdir")
	d, err := TempFileDB().Make()
	if err == nil {
		d.Close()
		t.Fatal("Make succeeded with an unusable TMPDIR")
	}
	wantErr(t, err, mapdb5.KindVolumeIO, "TempFileDB")
}
