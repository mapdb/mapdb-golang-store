// Codec-descriptor verification for the name catalog. A stored descriptor
// that parses as a known built-in / valid recursive grammar / CUSTOM marker
// is "valid": a mismatch against it is wrong configuration. A stored
// descriptor that is unknown or malformed is catalog corruption. The port
// never reconstructs a codec from text — typed opens always supply the
// concrete codec and the check is a pure string comparison.
package db

import (
	"strconv"
	"strings"

	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/ser"
)

// CustomDescriptor is the opaque marker written for a codec whose exact wire
// identity the port cannot reproduce (Java writes CUSTOM:<fqcn>; the port
// writes the bare marker and requires the codec to be re-supplied on
// reopen).
const CustomDescriptor = "CUSTOM"

var builtinGroupDescriptors = map[string]bool{
	"LONG": true, "INT": true, "SHORT": true, "CHAR": true, "UUID": true,
	"STRING": true, "STRING_PREFIX": true, "BYTE_ARRAY": true,
	"BYTE_ARRAY_PREFIX": true, "INT_DELTA": true, "LONG_DELTA": true,
}

var builtinSerDescriptors = map[string]bool{
	"LONG": true, "INTEGER": true, "SHORT": true, "CHAR": true,
	"UUID": true, "STRING": true, "BYTE_ARRAY": true,
	"BYTE_ARRAY_UNSIGNED": true, "BOOLEAN": true, "BYTE": true,
	"FLOAT": true, "DOUBLE": true, "INTEGER_PACKED": true,
	"LONG_PACKED": true, "BYTE_ARRAY_NOSIZE": true, "STRING_NOSIZE": true,
	"STRING_ASCII": true, "STRING_INTERN": true, "RECID": true,
	"RECID_ARRAY": true, "BOOLEAN_ARRAY": true, "CHAR_ARRAY": true,
	"SHORT_ARRAY": true, "INT_ARRAY": true, "LONG_ARRAY": true,
	"FLOAT_ARRAY": true, "DOUBLE_ARRAY": true, "BIG_INTEGER": true,
	"BIG_DECIMAL": true, "DATE": true, "CLASS": true, "JAVA": true,
}

var tupleComponentNames = map[string]bool{"INT": true, "LONG": true, "STRING": true, "BYTES": true}
var columnTypeNames = map[string]bool{"LONG": true, "INT": true, "SHORT": true, "BYTE": true}

// isValidGroupDescriptor reports whether s parses as a known group-format
// descriptor (built-in, recursive grammar, or the CUSTOM marker).
func isValidGroupDescriptor(s string) bool {
	if builtinGroupDescriptors[s] || ser.IsCustomDescriptor(s) {
		return true
	}
	if b64, ok := strings.CutPrefix(s, "OBJECT_ARRAY:"); ok {
		nested, ok := ser.B64URLDecode(b64)
		return ok && isValidSerDescriptor(nested)
	}
	if list, ok := strings.CutPrefix(s, "TUPLE:"); ok {
		return allComponents(list, tupleComponentNames)
	}
	if list, ok := strings.CutPrefix(s, "COLUMNAR:"); ok {
		return allComponents(list, columnTypeNames)
	}
	return false
}

func allComponents(list string, allowed map[string]bool) bool {
	if list == "" {
		return false
	}
	for _, c := range strings.Split(list, ",") {
		if !allowed[c] {
			return false
		}
	}
	return true
}

// isValidSerDescriptor reports whether s parses as a known element-serializer
// descriptor.
func isValidSerDescriptor(s string) bool {
	if builtinSerDescriptors[s] || ser.IsCustomDescriptor(s) {
		return true
	}
	if rest, ok := strings.CutPrefix(s, "DEFLATE:"); ok {
		// DEFLATE:<level>:<b64url(nested)>
		level, nestedB64, ok := strings.Cut(rest, ":")
		if !ok {
			return false
		}
		// Java CompressionSerializer (Deflater) only accepts -1..=9; any
		// other level makes the registry return null → catalog corruption.
		lvl, err := strconv.ParseInt(level, 10, 32)
		if err != nil || lvl < -1 || lvl > 9 {
			return false
		}
		nested, ok := ser.B64URLDecode(nestedB64)
		return ok && isValidSerDescriptor(nested)
	}
	if rest, ok := strings.CutPrefix(s, "ARRAY:"); ok {
		// ARRAY:<b64(component-class)>:<b64(nested descriptor)>
		comp, nestedB64, ok := strings.Cut(rest, ":")
		if !ok {
			return false
		}
		if _, ok := ser.B64URLDecode(comp); !ok {
			return false
		}
		nested, ok := ser.B64URLDecode(nestedB64)
		return ok && isValidSerDescriptor(nested)
	}
	return false
}

// verifyDescriptor checks a supplied codec's descriptor against the stored
// catalog descriptor. A mismatch against a VALID stored descriptor is wrong
// configuration; a mismatch against a MALFORMED stored descriptor is
// catalog corruption.
func verifyDescriptor(stored string, supplied any, storedValid bool) {
	if d, ok := ser.DescriptorOf(supplied); ok {
		if d == stored {
			return
		}
		if storedValid {
			panic(mapdb5.New(mapdb5.KindWrongConfiguration,
				"codec descriptor mismatch: catalog has '"+stored+"' but the supplied codec is '"+d+"'"))
		}
		panic(mapdb5.NewCorruption("unknown/malformed stored codec descriptor '" + stored + "'"))
	}
	// A custom (unreproducible) codec matches only a stored custom marker.
	if ser.IsCustomDescriptor(stored) {
		return
	}
	if storedValid {
		panic(mapdb5.New(mapdb5.KindWrongConfiguration,
			"a custom codec was supplied but the catalog stored the known descriptor '"+stored+
				"'; re-supply the exact codec used at create time"))
	}
	panic(mapdb5.NewCorruption("unknown/malformed stored codec descriptor '" + stored + "'"))
}

// verifyGroup checks a supplied group format against a stored descriptor.
func verifyGroup[K any](stored string, supplied ser.GroupFormat[K]) {
	verifyDescriptor(stored, supplied, isValidGroupDescriptor(stored))
}

// verifySer checks a supplied element serializer against a stored descriptor.
func verifySer[V any](stored string, supplied ser.Serializer[V]) {
	verifyDescriptor(stored, supplied, isValidSerDescriptor(stored))
}
