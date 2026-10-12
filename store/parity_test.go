package store

import (
	"testing"

	mapdb5 "github.com/mapdb/mapdb5-go"
)

func TestParityRoundtripAndZeroFails(t *testing.T) {
	for _, v := range []uint64{0, 16, 0x10, 0xFF0, 1 << 20, 0x0000_FFFF_FFFF_FFF0} {
		if got := Parity1Get(Parity1Set(v)); got != v {
			t.Fatalf("p1 roundtrip %#x got %#x", v, got)
		}
		if got := Parity4Get(Parity4Set(v)); got != v {
			t.Fatalf("p4 roundtrip %#x got %#x", v, got)
		}
	}
	for _, v := range []uint64{0, 1 << 16, 1 << 20, 1 << 40} {
		if got := Parity16Get(Parity16Set(v)); got != v {
			t.Fatalf("p16 roundtrip %#x got %#x", v, got)
		}
	}
	// raw 0 fails every parity check (the "never written" guarantee)
	for _, name := range []string{"p1", "p4", "p16"} {
		func() {
			defer func() {
				if de, ok := mapdb5.AsDBError(recover()); !ok || de.Kind != mapdb5.KindDataCorruption {
					t.Fatalf("%s: raw 0 must fail parity", name)
				}
			}()
			switch name {
			case "p1":
				Parity1Get(0)
			case "p4":
				Parity4Get(0)
			case "p16":
				Parity16Get(0)
			}
		}()
	}
	// p*set(0) is a valid non-zero encoding of 0
	if Parity1Set(0) != 1 || Parity1Get(Parity1Set(0)) != 0 {
		t.Fatal("p1set(0) contract")
	}
}

func TestParityBitFlipDetected(t *testing.T) {
	good := Parity4Set(0x1230)
	defer func() {
		if de, ok := mapdb5.AsDBError(recover()); !ok || de.Kind != mapdb5.KindDataCorruption {
			t.Fatal("flipped payload bit must fail parity")
		}
	}()
	Parity4Get(good ^ 0x100)
}
