// Codec descriptors — the stable, Java-registered wire identifiers persisted
// in the DB name catalog for every codec (Java DB.SERIALIZER_REGISTRY IDs).
//
// Because the port monomorphizes collections over interface values, there is
// no runtime class to reflect on. Each built-in codec instead carries its
// descriptor on its value (funcSer.desc / format desc fields) or computes it
// from its parameters (OBJECT_ARRAY/TUPLE/COLUMNAR/DEFLATE). A codec whose
// wire identity the port cannot reproduce reports ok=false; the catalog
// stores the opaque marker "CUSTOM", which reopen can only match against
// another custom codec — never against a known descriptor.
//
// Unlike Java, the port never RECONSTRUCTS a codec from its descriptor
// (that would require erased dispatch); typed opens always supply the
// concrete codec and the catalog check is a pure string comparison.
package ser

import (
	"encoding/base64"
	"strconv"
	"unicode/utf8"
)

// Describer is the optional stable codec-descriptor capability. Built-in
// codecs implement it; custom codecs don't (catalog stores "CUSTOM" and a
// reopen must re-supply the exact codec).
type Describer interface {
	// Descriptor returns the Java-registered descriptor string; ok=false
	// marks a custom codec with no reproducible wire identity.
	Descriptor() (descriptor string, ok bool)
}

// DescriptorOf queries any codec value (serializer or group format) for its
// descriptor; ("", false) = custom codec with no stable wire identity.
func DescriptorOf(codec any) (string, bool) {
	if d, ok := codec.(Describer); ok {
		return d.Descriptor()
	}
	return "", false
}

// DescriptorOrCustom is the descriptor to persist for a codec: the Java
// identifier when known, else the opaque custom marker.
func DescriptorOrCustom(codec any) string {
	if d, ok := DescriptorOf(codec); ok {
		return d
	}
	return "CUSTOM"
}

// IsCustomDescriptor reports whether a stored descriptor is the custom
// marker ("CUSTOM" or Java's "CUSTOM:<fqcn>" form).
func IsCustomDescriptor(stored string) bool {
	return stored == "CUSTOM" || len(stored) > 7 && stored[:7] == "CUSTOM:"
}

// B64URLEncode is RFC 4648 URL-safe base64 without padding (Java
// Base64.getUrlEncoder().withoutPadding()).
func B64URLEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

// B64URLDecode decodes RFC 4648 URL-safe base64 without padding back to a
// string; ok=false on an illegal alphabet/length or decoded bytes that are
// not valid UTF-8 (descriptor components are always text — Java decodes to
// String, which rejects malformed input the same way).
func B64URLDecode(s string) (string, bool) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || !utf8.Valid(b) {
		return "", false
	}
	return string(b), true
}

// =========================== serializer descriptors ===========================

// Descriptor returns the codec's registered element-serializer identifier
// (empty/false for a funcSer with no registered identity = custom).
func (f funcSer[A]) Descriptor() (string, bool) { return f.desc, f.desc != "" }

// Descriptor forwards to the wrapped serializer (AnyOf over a built-in
// funcSer reports the built-in's descriptor).
func (a AnyOf[A]) Descriptor() (string, bool) { return DescriptorOf(a.Serializer) }

// Descriptor computes DEFLATE:<level>:<b64url(nested-descriptor)> (Java
// CompressionSerializer). A custom delegate makes the whole codec custom.
func (s CompressionSerializer[A]) Descriptor() (string, bool) {
	nested, ok := DescriptorOf(s.Delegate)
	if !ok {
		return "", false
	}
	return "DEFLATE:" + strconv.Itoa(s.Level) + ":" + B64URLEncode([]byte(nested)), true
}

// NOTE: ArraySerializer intentionally has NO Descriptor method. Java's ARRAY
// descriptor embeds the Java component class name
// (ARRAY:<b64(class)>:<b64(nested)>), which the port cannot reproduce — it
// is a custom codec that must be re-supplied on reopen (PORTING-GAPS).

// =========================== group format descriptors ===========================

func (f fixedFormat[K]) Descriptor() (string, bool)  { return f.desc, f.desc != "" }
func (f blobFormat[K]) Descriptor() (string, bool)   { return f.desc, f.desc != "" }
func (f deltaFormat[K]) Descriptor() (string, bool)  { return f.desc, f.desc != "" }
func (f prefixFormat[K]) Descriptor() (string, bool) { return f.desc, f.desc != "" }

// Descriptor computes OBJECT_ARRAY:<b64url(element-serializer-descriptor)>
// (Java ObjectArrayFormat). A custom element makes the whole format custom.
func (f ObjectArrayFormat[K]) Descriptor() (string, bool) {
	nested, ok := DescriptorOf(f.Elem)
	if !ok {
		return "", false
	}
	return "OBJECT_ARRAY:" + B64URLEncode([]byte(nested)), true
}

var tupleComponentNames = map[TupleComponent]string{
	TupleInt:   "INT",
	TupleLong:  "LONG",
	TupleStr:   "STRING",
	TupleBytes: "BYTES",
}

// Descriptor computes TUPLE:<component>[,<component>...] (Java TupleFormat;
// component = TupleComponent.name()).
func (f TupleFormat) Descriptor() (string, bool) {
	s := "TUPLE:"
	for i, c := range f.serializer.Schema {
		if i > 0 {
			s += ","
		}
		s += tupleComponentNames[c]
	}
	return s, true
}

var columnTypeNames = map[ColumnType]string{
	ColumnLong:  "LONG",
	ColumnInt:   "INT",
	ColumnShort: "SHORT",
	ColumnByte:  "BYTE",
}

// Descriptor computes COLUMNAR:<coltype>[,<coltype>...] (Java
// ColumnarValueFormat).
func (f ColumnarValueFormat) Descriptor() (string, bool) {
	s := "COLUMNAR:"
	for i, c := range f.schema {
		if i > 0 {
			s += ","
		}
		s += columnTypeNames[c]
	}
	return s, true
}
