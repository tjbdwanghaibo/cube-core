//go:build !race

package misc

import "github.com/modern-go/gls"

// GoID returns the current goroutine ID.
// Uses gls for high performance in non-race builds.
func GoID() int64 {
	return gls.GoID()
}
