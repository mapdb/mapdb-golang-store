module github.com/mapdb/mapdb5-go

go 1.24

// Development against the sibling checkout; the interface-consistency
// requirement (G4) stands either way.
replace github.com/mapdb/mapdb-golang => ../mapdb-golang

require github.com/mapdb/mapdb-golang v0.0.0
