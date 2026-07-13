package app

import (
	"fmt"
	"sync"

	"github.com/tjbdwanghaibo/cube-core/admin"
	"github.com/tjbdwanghaibo/cube-core/health"
	"github.com/tjbdwanghaibo/cube-core/lifecycle"
	"github.com/tjbdwanghaibo/cube-core/obs"

	"github.com/spf13/viper"
)

// Registry holds capabilities provided by Mods and consumed by Services.
type Registry struct {
	mu    sync.RWMutex
	store map[ModName]any
	cfg   *viper.Viper
}

func NewRegistry(cfg *viper.Viper) *Registry {
	r := &Registry{
		store: make(map[ModName]any),
		cfg:   cfg,
	}
	r.store[ModHealth] = health.NewRegistry()
	metrics := obs.NewRegistry(obs.WithMaxSeriesPerMetric(obsMaxSeriesPerMetric(cfg)))
	obs.SetDefaultRegistry(metrics)
	r.store[ModObs] = metrics
	r.store[ModAdmin] = admin.NewRegistry()
	r.store[ModAdminMetadata] = admin.NewMetadataRegistry()
	r.store[ModLifecycle] = lifecycle.NewRegistry()
	return r
}

// Register registers a capability and fails if another provider already owns it.
func (r *Registry) Register(name ModName, value any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.store[name]; ok {
		return fmt.Errorf("registry: capability %q already registered by %T", name, existing)
	}
	r.store[name] = value
	return nil
}

// Get retrieves a capability. Returns nil, false if not found.
func (r *Registry) Get(name ModName) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.store[name]
	return v, ok
}

// MustGet retrieves a capability or panics.
func (r *Registry) MustGet(name ModName) any {
	v, ok := r.Get(name)
	if !ok {
		panic(fmt.Sprintf("registry: capability %q not found", name))
	}
	return v
}

// Config returns the viper config instance.
func (r *Registry) Config() *viper.Viper {
	return r.cfg
}

// Lookup is a type-safe generic getter.
func Lookup[T any](r *Registry, name ModName) (T, bool) {
	if r == nil {
		var zero T
		return zero, false
	}
	v, ok := r.Get(name)
	if !ok {
		var zero T
		return zero, false
	}
	t, ok := v.(T)
	return t, ok
}

// MustLookup is a type-safe generic getter that panics on failure.
func MustLookup[T any](r *Registry, name ModName) T {
	v, ok := Lookup[T](r, name)
	if !ok {
		panic(fmt.Sprintf("registry: capability %q not found or wrong type", name))
	}
	return v
}

func obsMaxSeriesPerMetric(cfg *viper.Viper) int {
	if cfg == nil {
		return 0
	}
	if cfg.IsSet("obs.max_series_per_metric") {
		return cfg.GetInt("obs.max_series_per_metric")
	}
	if cfg.IsSet("metrics.max_series_per_metric") {
		return cfg.GetInt("metrics.max_series_per_metric")
	}
	return 0
}
