//go:build !darwin && !linux && !freebsd

package hotcode

import "fmt"

type Bundle interface {
	Meta() Meta
	Apply(*Registry) error
	Revert(*Registry) error
}

func LoadPlugin(path string) (Bundle, error) {
	return nil, fmt.Errorf("hotcode: Go plugins are not supported on this platform: %s", path)
}
