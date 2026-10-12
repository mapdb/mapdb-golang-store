package ser

import (
	mapdb5 "github.com/mapdb/mapdb5-go"
	"github.com/mapdb/mapdb5-go/bin"
)

// ColumnarValueFormat — a column-major (Arrow-style) GroupFormat over a
// fixed schema of fixed-width integral columns (Java ColumnarValueFormat).
//
// A group of n fixed-arity rows is stored COLUMN-BY-COLUMN so a scan over
// one column reads only that column's contiguous byte run:
//
//	[ col0 : n*w0 ][ col1 : n*w1 ] ... [ col(C-1) : n*w(C-1) ]
//
// Each cell is big-endian. With cumWidth[c] = Σ_{j<c} w_j and
// rowWidth = Σ w_j: cell (row i, col c) lives at
// start + n*cumWidth[c] + i*w_c; the group ends at start + n*rowWidth.
//
// Byte-side ops DECODE each probed cell and compare with the same SIGNED
// per-column order as Compare — a raw unsigned memcmp would misorder
// negative values.

// ColumnType is a fixed-width integral column type (big-endian wire).
type ColumnType int

const (
	ColumnLong ColumnType = iota
	ColumnInt
	ColumnShort
	ColumnByte
)

// Width is the byte width of one cell.
func (t ColumnType) Width() int {
	switch t {
	case ColumnLong:
		return 8
	case ColumnInt:
		return 4
	case ColumnShort:
		return 2
	case ColumnByte:
		return 1
	default:
		panic(mapdb5.NewMisuse("unknown column type"))
	}
}

// cellAs truncates a cell to the column width (mirrors Java
// intValue()/shortValue()/byteValue() before compare), preserving signed
// order within the width.
func cellAs(t ColumnType, v Value) int64 {
	n := v.numI64()
	switch t {
	case ColumnInt:
		return int64(int32(n))
	case ColumnShort:
		return int64(int16(n))
	case ColumnByte:
		return int64(int8(n))
	default:
		return n
	}
}

// writeCell writes one cell (big-endian, low bits per column width).
func writeCell(out *bin.Writer, t ColumnType, v Value) {
	n := v.numI64()
	switch t {
	case ColumnLong:
		out.WriteLong(n)
	case ColumnInt:
		out.WriteInt(int32(n))
	case ColumnShort:
		out.WriteShort(int16(n))
	case ColumnByte:
		out.WriteU8(byte(n))
	}
}

// readCell reads one cell into its typed Value.
func readCell(r *bin.Reader, t ColumnType) Value {
	switch t {
	case ColumnLong:
		return LongValue(r.ReadLong())
	case ColumnInt:
		return IntValue(r.ReadInt())
	case ColumnShort:
		return ShortValue(r.ReadShort())
	case ColumnByte:
		return ByteValue(r.ReadI8())
	default:
		panic(mapdb5.NewMisuse("unknown column type"))
	}
}

// compareCell is the signed per-column compare (Java compareCell).
func compareCell(t ColumnType, a, b Value) int {
	return cmpInt64(cellAs(t, a), cellAs(t, b))
}

// ColumnarValueFormat is a columnar group format over a fixed schema.
// Element = a full-arity row ([]Value); Group = [][]Value.
type ColumnarValueFormat struct {
	schema   []ColumnType
	cumWidth []int // length C+1; cumWidth[c] = bytes of columns before c
	rowWidth int
	rowSer   RowSerializer
}

// NewColumnarValueFormat builds a columnar format over the given fixed-width
// columns (arity = len(columns) >= 1).
func NewColumnarValueFormat(columns ...ColumnType) ColumnarValueFormat {
	if len(columns) == 0 {
		panic(mapdb5.NewMisuse("columnar schema needs >= 1 column"))
	}
	schema := make([]ColumnType, len(columns))
	copy(schema, columns)
	cumWidth := make([]int, len(schema)+1)
	for i, c := range schema {
		cumWidth[i+1] = cumWidth[i] + c.Width()
	}
	rowWidth := cumWidth[len(schema)]
	return ColumnarValueFormat{
		schema:   schema,
		cumWidth: cumWidth,
		rowWidth: rowWidth,
		rowSer:   RowSerializer{Schema: schema, RowWidth: rowWidth},
	}
}

// ColumnCount is the row arity.
func (f ColumnarValueFormat) ColumnCount() int { return len(f.schema) }

// ColumnTypeOf returns the type of column col.
func (f ColumnarValueFormat) ColumnTypeOf(col int) ColumnType { return f.schema[col] }

// RowWidth is the row width in bytes.
func (f ColumnarValueFormat) RowWidth() int { return f.rowWidth }

func checkedMul(a, b int) int {
	if a < 0 || b < 0 {
		panic(mapdb5.NewCorruption("columnar seek overflow"))
	}
	p := a * b
	if a != 0 && p/a != b {
		panic(mapdb5.NewCorruption("columnar seek overflow"))
	}
	return p
}

func checkedAdd(a, b int) int {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		panic(mapdb5.NewCorruption("columnar seek overflow"))
	}
	return s
}

// cellOffset is the byte offset of cell (row, col) relative to a group
// starting at start, for a group of size rows. Checked (GD8).
func (f ColumnarValueFormat) cellOffset(start, size, col, row int) int {
	colsBefore := checkedMul(size, f.cumWidth[col])
	within := checkedMul(row, f.schema[col].Width())
	return checkedAdd(start, checkedAdd(colsBefore, within))
}

// groupEnd is start + size*rowWidth, checked.
func (f ColumnarValueFormat) groupEnd(start, size int) int {
	return checkedAdd(start, checkedMul(size, f.rowWidth))
}

func (f ColumnarValueFormat) checkArity(row []Value) {
	if len(row) != len(f.schema) {
		panic(mapdb5.NewMisuse("row/probe arity != schema arity"))
	}
}

// compareRows is lexicographic compare of two full-arity rows (column 0
// most significant).
func (f ColumnarValueFormat) compareRows(a, b []Value) int {
	f.checkArity(a)
	f.checkArity(b)
	for c := range f.schema {
		if cmp := compareCell(f.schema[c], a[c], b[c]); cmp != 0 {
			return cmp
		}
	}
	return 0
}

// compareRowAt compares stored row `row` against the probe column-by-column,
// early exit (returns stored.cmp(key)).
func (f ColumnarValueFormat) compareRowAt(r *bin.Reader, start, size, row int, key []Value) int {
	for c := range f.schema {
		r.Seek(f.cellOffset(start, size, c, row))
		cell := readCell(r, f.schema[c])
		if cmp := compareCell(f.schema[c], cell, key[c]); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (f ColumnarValueFormat) cast(g Group) [][]Value {
	gg, ok := g.([][]Value)
	if !ok {
		panic(mapdb5.New(mapdb5.KindSerialization, "foreign group passed to ColumnarValueFormat"))
	}
	return gg
}

func (f ColumnarValueFormat) Element() Serializer[[]Value] { return f.rowSer }
func (f ColumnarValueFormat) Compare(a, b []Value) int {
	return f.compareRows(a, b)
}
func (f ColumnarValueFormat) SupportsBinary() bool      { return true }
func (f ColumnarValueFormat) SupportsRangeCursor() bool { return true }
func (f ColumnarValueFormat) Empty() Group              { return [][]Value{} }
func (f ColumnarValueFormat) Size(g Group) int          { return len(f.cast(g)) }

// cloneRow deep-copies a row at group boundaries — []Value rows and their
// ValueBytes payloads must not alias caller or group storage across the
// immutable-COW contract.
func cloneRow(row []Value) []Value {
	out := make([]Value, len(row))
	for i, v := range row {
		if v.Kind == ValueBytes {
			b := make([]byte, len(v.Bytes))
			copy(b, v.Bytes)
			v.Bytes = b
		}
		out[i] = v
	}
	return out
}

func (f ColumnarValueFormat) Get(g Group, idx int) []Value {
	gg := f.cast(g)
	if idx < 0 || idx >= len(gg) {
		panic(mapdb5.NewMisuse("group get index out of range"))
	}
	return cloneRow(gg[idx])
}
func (f ColumnarValueFormat) Search(g Group, key []Value) (int, bool) {
	f.checkArity(key) // fail fast even when the group is empty
	return bsearch(f.cast(g), key, f.compareRows)
}
func (f ColumnarValueFormat) Insert(g Group, idx int, key []Value) Group {
	f.checkArity(key)
	return Group(sliceInsert(f.cast(g), idx, cloneRow(key)))
}
func (f ColumnarValueFormat) Set(g Group, idx int, key []Value) Group {
	f.checkArity(key)
	return Group(sliceSet(f.cast(g), idx, cloneRow(key)))
}
func (f ColumnarValueFormat) Delete(g Group, idx int) Group {
	return Group(sliceDelete(f.cast(g), idx))
}
func (f ColumnarValueFormat) CopyRange(g Group, from, to int) Group {
	return Group(sliceCopyRange(f.cast(g), from, to))
}
func (f ColumnarValueFormat) FromSlice(keys [][]Value) Group {
	out := make([][]Value, len(keys))
	for i, row := range keys {
		f.checkArity(row)
		out[i] = cloneRow(row)
	}
	return Group(out)
}
func (f ColumnarValueFormat) ToSlice(g Group) [][]Value {
	gg := f.cast(g)
	out := make([][]Value, len(gg))
	for i, row := range gg {
		out[i] = cloneRow(row)
	}
	return out
}

// SerializeGroup writes column-major cells.
func (f ColumnarValueFormat) SerializeGroup(w *bin.Writer, g Group) {
	gg := f.cast(g)
	for c, t := range f.schema {
		for _, row := range gg {
			f.checkArity(row)
			writeCell(w, t, row[c])
		}
	}
}

func (f ColumnarValueFormat) DeserializeGroup(r *bin.Reader, count int) Group {
	groupAlloc(count, f.rowWidth, r.Remaining())
	rows := make([][]Value, count)
	for i := range rows {
		rows[i] = make([]Value, len(f.schema))
	}
	for c, t := range f.schema {
		for i := 0; i < count; i++ {
			rows[i][c] = readCell(r, t)
		}
	}
	return Group(rows)
}

func (f ColumnarValueFormat) BytesGet(r *bin.Reader, count int, idx int) []Value {
	checkGroupCount(count)
	if idx < 0 || idx >= count {
		panic(mapdb5.NewCorruption("columnar index out of range"))
	}
	start := r.Pos()
	row := make([]Value, len(f.schema))
	for c, t := range f.schema {
		r.Seek(f.cellOffset(start, count, c, idx))
		row[c] = readCell(r, t)
	}
	r.Seek(f.groupEnd(start, count))
	return row
}

func (f ColumnarValueFormat) BytesSearch(r *bin.Reader, count int, key []Value) (int, bool) {
	checkGroupCount(count)
	if len(key) != len(f.schema) {
		panic(mapdb5.NewCorruption("columnar probe arity != schema"))
	}
	start := r.Pos()
	lo, hi := 0, count-1
	found, ins := -1, 0
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		c := f.compareRowAt(r, start, count, mid, key)
		switch {
		case c == 0:
			found = mid
			hi = -1
		case c < 0:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	if found < 0 {
		ins = lo
	}
	r.Seek(f.groupEnd(start, count))
	if found >= 0 {
		return found, true
	}
	return ins, false
}

func (f ColumnarValueFormat) RangeCursor(r *bin.Reader, count, from, to int) GroupCursor[[]Value] {
	checkGroupCount(count)
	rangeBounds(count, from, to)
	return &rowCursor{f: f, r: r, start: r.Pos(), count: count, to: to, idx: from}
}

// ColumnCursor opens a cursor over ONE column's values for rows [from, to):
// reads only that column's contiguous byte run, never the whole group. On
// exhaustion the reader is left at group end.
func (f ColumnarValueFormat) ColumnCursor(r *bin.Reader, size, col, from, to int) GroupCursor[Value] {
	if col < 0 || col >= len(f.schema) {
		panic(mapdb5.NewMisuse("column_cursor col out of range"))
	}
	checkGroupCount(size)
	rangeBounds(size, from, to)
	return &columnCursor{f: f, r: r, start: r.Pos(), count: size, col: col, to: to, idx: from}
}

// rowCursor: whole-row cursor, one decode per row.
type rowCursor struct {
	f         ColumnarValueFormat
	r         *bin.Reader
	start     int
	count     int
	to        int
	idx       int
	started   bool
	cur       []Value
	exhausted bool
}

func (c *rowCursor) Next() bool {
	if c.exhausted {
		return false
	}
	if c.started {
		c.idx++
	} else {
		c.started = true
	}
	if c.idx >= c.to {
		c.exhausted = true
		c.cur = nil
		c.r.Seek(c.f.groupEnd(c.start, c.count))
		return false
	}
	row := make([]Value, len(c.f.schema))
	for col, t := range c.f.schema {
		c.r.Seek(c.f.cellOffset(c.start, c.count, col, c.idx))
		row[col] = readCell(c.r, t)
	}
	c.cur = row
	return true
}

func (c *rowCursor) Index() int {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Index outside valid element"))
	}
	return c.idx
}

func (c *rowCursor) Value() []Value {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Value outside valid element"))
	}
	return c.cur
}

// columnCursor: single-column cursor, reads only column col's contiguous run.
type columnCursor struct {
	f         ColumnarValueFormat
	r         *bin.Reader
	start     int
	count     int
	col       int
	to        int
	idx       int
	started   bool
	cur       Value
	exhausted bool
}

func (c *columnCursor) Next() bool {
	if c.exhausted {
		return false
	}
	if c.started {
		c.idx++
	} else {
		c.started = true
	}
	if c.idx >= c.to {
		c.exhausted = true
		c.cur = Value{}
		c.r.Seek(c.f.groupEnd(c.start, c.count))
		return false
	}
	c.r.Seek(c.f.cellOffset(c.start, c.count, c.col, c.idx))
	c.cur = readCell(c.r, c.f.schema[c.col])
	return true
}

func (c *columnCursor) Index() int {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Index outside valid element"))
	}
	return c.idx
}

func (c *columnCursor) Value() Value {
	if !c.started || c.exhausted {
		panic(mapdb5.NewMisuse("GroupCursor.Value outside valid element"))
	}
	return c.cur
}

// RowSerializer is the standalone single-row codec (row-major fixed-width
// cells): the GroupFormat Element serializer.
type RowSerializer struct {
	Schema   []ColumnType
	RowWidth int
}

func (s RowSerializer) Serialize(w *bin.Writer, v []Value) {
	if len(v) != len(s.Schema) {
		panic(mapdb5.NewMisuse("row arity != schema arity"))
	}
	for i, t := range s.Schema {
		writeCell(w, t, v[i])
	}
}

func (s RowSerializer) Deserialize(r *bin.Reader, _ int) []Value {
	row := make([]Value, len(s.Schema))
	for i, t := range s.Schema {
		row[i] = readCell(r, t)
	}
	return row
}

func (s RowSerializer) FixedSize() int { return s.RowWidth }
func (s RowSerializer) SizeHint() int  { return s.RowWidth }
func (s RowSerializer) Compare(a, b []Value) int {
	if len(a) != len(s.Schema) || len(b) != len(s.Schema) {
		panic(mapdb5.NewMisuse("probe arity != schema arity"))
	}
	for i, t := range s.Schema {
		if c := compareCell(t, a[i], b[i]); c != 0 {
			return c
		}
	}
	return 0
}
func (s RowSerializer) Equals(a, b []Value) bool      { return s.Compare(a, b) == 0 }
func (s RowSerializer) NaturalOrder() bool            { return false }
func (s RowSerializer) EqualsBySerializedBytes() bool { return true }
