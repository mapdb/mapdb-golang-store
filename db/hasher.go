package db

// Hasher catalog markers (Java DB hashMap/hashSet `#hasher` and
// hashMap48/hashSet48 `#hasher64`).
//
// Java's DEFAULT hasher is Hashers.objectHasher — fmix32(key.hashCode() ^
// seed); DEFAULT64 is fmix64((long) key.hashCode() ^ seed). Go has no
// hashCode, so the port reproduces it only for the key types whose Java
// hashCode is defined by value: int32 (Integer), int64 (Long) and string
// (String). For those keys a nil hasher means DEFAULT and placement is
// Java-identical. For any other key type a nil hasher falls back to the
// serialized-bytes hasher (mdhash.SerHasher32/64) and the catalog records
// a CUSTOM marker naming it: a Java reader then requires a custom hasher
// (correct — it cannot reproduce that placement), and a Go reader
// rebuilds it without one.

import (
	"reflect"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/mdhash"
	"github.com/mapdb/mapdb5-go/ser"
)

const (
	hasherDefault   = "DEFAULT"
	hasherDefault64 = "DEFAULT64"
	hasherLong64    = "LONG64"
	hasherString64  = "STRING64"
	hasherBytes64   = "BYTE_ARRAY64"
	// goSerHasher32Marker / goSerHasher64Marker name the Go fallback
	// placement for key types without a Java-value hashCode.
	goSerHasher32Marker = "CUSTOM:go.mapdb5.mdhash.SerHasher32"
	goSerHasher64Marker = "CUSTOM:go.mapdb5.mdhash.SerHasher64"
)

// javaDefault32 is Java Hashers.objectHasher for K, when Go can reproduce
// K.hashCode().
func javaDefault32[K any]() (mdhash.Hasher[K], bool) {
	var h any
	switch any(*new(K)).(type) {
	case int32:
		h = mdhash.Int32
	case int64:
		h = mdhash.Long32
	case string:
		h = mdhash.String32
	default:
		return nil, false
	}
	return h.(mdhash.Hasher[K]), true
}

// javaDefault64 is Java Hashers.objectHasher64 for K: fmix64 of the
// sign-extended 32-bit hashCode xor seed.
func javaDefault64[K any]() (mdhash.Hasher64[K], bool) {
	var h any
	switch any(*new(K)).(type) {
	case int32:
		h = mdhash.Hasher64Func[int32](func(k int32, seed int64) int64 {
			return mdhash.Fmix64(int64(k) ^ seed)
		})
	case int64:
		h = mdhash.Hasher64Func[int64](func(k int64, seed int64) int64 {
			return mdhash.Fmix64(int64(mdhash.JavaLongHashCode(k)) ^ seed)
		})
	case string:
		h = mdhash.Hasher64Func[string](func(k string, seed int64) int64 {
			return mdhash.Fmix64(int64(mdhash.JavaStringHashCode(k)) ^ seed)
		})
	default:
		return nil, false
	}
	return h.(mdhash.Hasher64[K]), true
}

// createHasher32 resolves the create-time hasher and its catalog marker.
func createHasher32[K any](supplied mdhash.Hasher[K], keySer ser.Serializer[K]) (mdhash.Hasher[K], string) {
	if supplied != nil {
		return supplied, CustomDescriptor
	}
	if h, ok := javaDefault32[K](); ok {
		return h, hasherDefault
	}
	return mdhash.SerHasher32(keySer), goSerHasher32Marker
}

// openHasher32 resolves the reopen hasher from the stored marker (Java
// resolveHasher): DEFAULT rejects a supplied hasher, CUSTOM requires one.
// Go-only allowances: DEFAULT over a key type without a Go hashCode
// equivalent accepts a supplied hasher, and the Go fallback marker
// rebuilds SerHasher32 when none is supplied.
func openHasher32[K any](marker string, supplied mdhash.Hasher[K], keySer ser.Serializer[K]) mdhash.Hasher[K] {
	switch {
	case marker == hasherDefault:
		h, ok := javaDefault32[K]()
		if !ok {
			if supplied == nil {
				panic(mapdb5.New(mapdb5.KindWrongConfiguration,
					"collection uses Java's default hashCode hasher, which has no Go equivalent for this key type; supply a hasher reproducing it"))
			}
			return supplied
		}
		if supplied != nil {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"collection was created with the default hasher; do not supply a custom hasher on reopen"))
		}
		return h
	case marker == goSerHasher32Marker:
		if supplied != nil {
			return supplied
		}
		return mdhash.SerHasher32(keySer)
	case ser.IsCustomDescriptor(marker):
		if supplied == nil {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"hash collection uses a custom hasher; re-supply it on the maker before opening"))
		}
		return supplied
	default:
		panic(mapdb5.NewCorruption("unknown hasher marker in catalog: " + marker))
	}
}

// builtinHasher64ID names a supplied 64-bit hasher when it is one of the
// Java built-ins (identity of the package-level func value).
func builtinHasher64ID(h any) (string, bool) {
	v := reflect.ValueOf(h)
	if v.Kind() != reflect.Func {
		return "", false
	}
	p := v.Pointer()
	for id, b := range map[string]any{
		hasherLong64: mdhash.Long64, hasherString64: mdhash.String64, hasherBytes64: mdhash.ByteArray64,
	} {
		if reflect.ValueOf(b).Pointer() == p {
			return id, true
		}
	}
	return "", false
}

// builtinHasher64 is the Java built-in named by id, if it fits K.
func builtinHasher64[K any](id string) (mdhash.Hasher64[K], bool) {
	var b any
	switch id {
	case hasherLong64:
		b = mdhash.Long64
	case hasherString64:
		b = mdhash.String64
	case hasherBytes64:
		b = mdhash.ByteArray64
	default:
		return nil, false
	}
	h, ok := b.(mdhash.Hasher64[K])
	if !ok {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"catalog hasher "+id+" does not apply to this key type"))
	}
	return h, true
}

// hasher64ID is Java hasher64Id for a supplied (non-nil) hasher.
func hasher64ID(h any) string {
	if id, ok := builtinHasher64ID(h); ok {
		return id
	}
	return CustomDescriptor
}

// createHasher64 resolves the create-time 64-bit hasher and its marker.
func createHasher64[K any](supplied mdhash.Hasher64[K], keySer ser.Serializer[K]) (mdhash.Hasher64[K], string) {
	if supplied != nil {
		return supplied, hasher64ID(supplied)
	}
	if h, ok := javaDefault64[K](); ok {
		return h, hasherDefault64
	}
	return mdhash.SerHasher64(keySer), goSerHasher64Marker
}

// openHasher64 is Java resolveHasher64: built-in markers resolve without a
// supplied hasher; a supplied one must carry the same id.
func openHasher64[K any](marker string, supplied mdhash.Hasher64[K], keySer ser.Serializer[K]) mdhash.Hasher64[K] {
	switch {
	case marker == hasherDefault64:
		h, ok := javaDefault64[K]()
		if !ok {
			if supplied == nil {
				panic(mapdb5.New(mapdb5.KindWrongConfiguration,
					"collection uses Java's default 64-bit hashCode hasher, which has no Go equivalent for this key type; supply a hasher reproducing it"))
			}
			return supplied
		}
		if supplied != nil {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"64-bit hasher mismatch for reopen: catalog='DEFAULT64', supplied='"+hasher64ID(supplied)+"'"))
		}
		return h
	case marker == goSerHasher64Marker:
		if supplied != nil {
			return supplied
		}
		return mdhash.SerHasher64(keySer)
	case ser.IsCustomDescriptor(marker):
		if supplied == nil {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"48-bit hash collection uses a custom hasher; re-supply it before opening"))
		}
		return supplied
	}
	b, ok := builtinHasher64[K](marker)
	if !ok {
		panic(mapdb5.NewCorruption("unknown 64-bit hasher marker in catalog: " + marker))
	}
	if supplied != nil && hasher64ID(supplied) != marker {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"64-bit hasher mismatch for reopen: catalog='"+marker+"', supplied='"+hasher64ID(supplied)+"'"))
	}
	return b
}

// validHasherMarker / validHasher64Marker are the catalog-validation
// grammar for the stored markers.
func validHasherMarker(m string) bool {
	return m == hasherDefault || ser.IsCustomDescriptor(m)
}

func validHasher64Marker(m string) bool {
	switch m {
	case hasherDefault64, hasherLong64, hasherString64, hasherBytes64:
		return true
	}
	return ser.IsCustomDescriptor(m)
}
