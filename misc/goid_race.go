//go:build race

package misc

import "runtime"

// GoID returns the current goroutine ID.
// Uses runtime.Stack parsing in race builds to avoid gls checkptr crashes.
func GoID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Format: "goroutine 123 [running]:\n"
	var id int64
	for i := len("goroutine "); i < n; i++ {
		ch := buf[i]
		if ch < '0' || ch > '9' {
			break
		}
		id = id*10 + int64(ch-'0')
	}
	return id
}
