//go:build darwin || linux || freebsd

package hotcode

import (
	"fmt"
	"path/filepath"
	"plugin"
)

// Bundle is implemented by hot-code plugin packages.
//
// A plugin should export a symbol named PatchBundle whose dynamic type
// implements this interface.
type Bundle interface {
	Meta() Meta
	Apply(*Registry) error
	Revert(*Registry) error
}

// LoadPlugin opens a Go plugin and applies its PatchBundle. The Go runtime does
// not unload plugins, so rollback must be implemented by the bundle's Revert
// method or by reverting individual patch points.
func LoadPlugin(path string) (Bundle, error) {
	if path == "" {
		return nil, fmt.Errorf("hotcode: plugin path required")
	}
	if filepath.Ext(path) != ".so" {
		return nil, fmt.Errorf("hotcode: plugin must be a .so file: %s", path)
	}
	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("hotcode: open plugin %s: %w", path, err)
	}
	sym, err := p.Lookup("PatchBundle")
	if err != nil {
		return nil, fmt.Errorf("hotcode: lookup PatchBundle in %s: %w", path, err)
	}
	bundle, ok := sym.(Bundle)
	if !ok {
		if ptr, ok := sym.(*Bundle); ok && ptr != nil {
			bundle = *ptr
			ok = bundle != nil
		}
	}
	if !ok {
		return nil, fmt.Errorf("hotcode: PatchBundle in %s does not implement hotcode.Bundle", path)
	}
	if err := bundle.Apply(Default); err != nil {
		return nil, fmt.Errorf("hotcode: apply plugin %s: %w", path, err)
	}
	return bundle, nil
}
