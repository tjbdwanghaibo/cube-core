package app

import (
	"context"

	"github.com/spf13/viper"
)

// Mod provides infrastructure capabilities to the Registry.
// Lifecycle: Init → Provide → Start → Stop (reverse order).
type Mod interface {
	Name() ModName
	Init(cfg *viper.Viper) error
	Provide(r *Registry) error // expose capabilities to registry
	Start() error
	Stop()
}

type ModStopperWithContext interface {
	StopWithContext(context.Context) error
}

// ModDependencyProvider can be implemented by Mods that require other Mods to
// be initialized/provided/started first.
type ModDependencyProvider interface {
	DependsOn() []ModName
}
