package hotcode

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNameRequired = errors.New("hotcode: name required")
	ErrFuncRequired = errors.New("hotcode: function required")
	ErrNotFound     = errors.New("hotcode: patch point not found")
	ErrTypeMismatch = errors.New("hotcode: function signature mismatch")
	ErrDuplicate    = errors.New("hotcode: duplicate patch point")
)

// Meta describes an active hot-code patch.
type Meta struct {
	Version  string
	Author   string
	Reason   string
	Source   string
	LoadedAt time.Time
}

// PointInfo is a read-only view of one registered patch point.
type PointInfo struct {
	Name       string
	Type       string
	Generation uint64
	Patched    bool
	Meta       Meta
}

type point struct {
	name     string
	fnType   reflect.Type
	original any
	current  atomic.Value
	meta     atomic.Value
	gen      atomic.Uint64
}

// Registry owns all hot-code patch points for one process.
type Registry struct {
	mu     sync.RWMutex
	points map[string]*point
}

// Default is the process-wide hot-code registry.
var Default = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{points: make(map[string]*point)}
}

func Register(name string, fn any) error {
	return Default.Register(name, fn)
}

func MustRegister(name string, fn any) {
	if err := Register(name, fn); err != nil {
		panic(err)
	}
}

func Replace(name string, fn any, meta Meta) error {
	return Default.Replace(name, fn, meta)
}

func Revert(name string) error {
	return Default.Revert(name)
}

func Resolve[T any](name string, fallback T) T {
	v := Default.Resolve(name, fallback)
	typed, ok := v.(T)
	if !ok {
		return fallback
	}
	return typed
}

func List() []PointInfo {
	return Default.List()
}

// Register creates a patch point. The original function remains available for
// revert and is used until Replace is called.
func (r *Registry) Register(name string, fn any) error {
	if name == "" {
		return ErrNameRequired
	}
	if fn == nil || reflect.TypeOf(fn).Kind() != reflect.Func {
		return ErrFuncRequired
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.points[name]; ok {
		return fmt.Errorf("%w: %s", ErrDuplicate, name)
	}
	p := &point{
		name:     name,
		fnType:   reflect.TypeOf(fn),
		original: fn,
	}
	p.current.Store(fn)
	r.points[name] = p
	return nil
}

func (r *Registry) Replace(name string, fn any, meta Meta) error {
	if name == "" {
		return ErrNameRequired
	}
	if fn == nil || reflect.TypeOf(fn).Kind() != reflect.Func {
		return ErrFuncRequired
	}

	p, err := r.point(name)
	if err != nil {
		return err
	}
	if got := reflect.TypeOf(fn); got != p.fnType {
		return fmt.Errorf("%w: %s want=%s got=%s", ErrTypeMismatch, name, p.fnType, got)
	}
	if meta.LoadedAt.IsZero() {
		meta.LoadedAt = time.Now()
	}
	p.current.Store(fn)
	p.meta.Store(meta)
	p.gen.Add(1)
	return nil
}

func (r *Registry) Revert(name string) error {
	p, err := r.point(name)
	if err != nil {
		return err
	}
	p.current.Store(p.original)
	p.meta.Store(Meta{})
	p.gen.Add(1)
	return nil
}

func (r *Registry) Resolve(name string, fallback any) any {
	p, err := r.point(name)
	if err != nil {
		return fallback
	}
	fn := p.current.Load()
	if fn == nil {
		return fallback
	}
	return fn
}

func (r *Registry) List() []PointInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ret := make([]PointInfo, 0, len(r.points))
	for _, p := range r.points {
		info := PointInfo{
			Name:       p.name,
			Type:       p.fnType.String(),
			Generation: p.gen.Load(),
		}
		if fn := p.current.Load(); fn != nil {
			info.Patched = reflect.ValueOf(fn).Pointer() != reflect.ValueOf(p.original).Pointer()
		}
		if meta, ok := p.meta.Load().(Meta); ok {
			info.Meta = meta
		}
		ret = append(ret, info)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Name < ret[j].Name })
	return ret
}

func (r *Registry) point(name string) (*point, error) {
	r.mu.RLock()
	p := r.points[name]
	r.mu.RUnlock()
	if p == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return p, nil
}

func (r *Registry) resetForTest() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.points = make(map[string]*point)
}

func ResetForTest() {
	Default.resetForTest()
}
