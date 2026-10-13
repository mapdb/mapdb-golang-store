//go:build amd64 || arm64

package handle

// getg returns the address of the calling goroutine's runtime g struct
// (assembly: the TLS slot on amd64, the g register on arm64). It is an
// identity for the goroutine's LIFETIME: the runtime recycles g structs,
// so a value must never outlive the scope that stored it — every caller
// clears what it stores before returning (defer).
func getg() uintptr

// Goid identifies the calling goroutine. Never 0 on this platform.
func Goid() uint64 { return uint64(getg()) }
