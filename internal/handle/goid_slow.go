//go:build !amd64 && !arm64

package handle

import "runtime"

// Goid identifies the calling goroutine by parsing its runtime.Stack
// header ("goroutine 123 [running]..."). Slow (~1µs, takes the runtime
// print lock); the fast path is assembly on amd64/arm64. Returns 0 when the
// format cannot be parsed ("unidentifiable": callers skip the registry
// rather than risk a false collision).
func Goid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n < 10 {
		return 0
	}
	var id uint64
	for i := 10; i < n && buf[i] >= '0' && buf[i] <= '9'; i++ {
		id = id*10 + uint64(buf[i]-'0')
	}
	return id
}
