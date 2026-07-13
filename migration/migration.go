package migration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrStepInvalid = errors.New("migration: step invalid")
	ErrPathMissing = errors.New("migration: path missing")
)

type Versioned interface {
	DataVersion() int32
	SetDataVersion(int32)
}

type Step struct {
	Name  string
	From  int32
	To    int32
	Apply func(context.Context, any) error
}

type Registry struct {
	mu    sync.RWMutex
	steps map[int32]Step
}

func NewRegistry() *Registry {
	return &Registry{steps: make(map[int32]Step)}
}

func (r *Registry) Register(step Step) error {
	if r == nil {
		return fmt.Errorf("%w: registry nil", ErrStepInvalid)
	}
	if step.From < 0 || step.To <= step.From {
		return fmt.Errorf("%w: invalid version %d -> %d", ErrStepInvalid, step.From, step.To)
	}
	if step.Apply == nil {
		return fmt.Errorf("%w: apply nil %d -> %d", ErrStepInvalid, step.From, step.To)
	}
	if step.Name == "" {
		step.Name = fmt.Sprintf("%d_to_%d", step.From, step.To)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.steps[step.From]; exists {
		return fmt.Errorf("%w: duplicate from %d", ErrStepInvalid, step.From)
	}
	r.steps[step.From] = step
	return nil
}

func (r *Registry) MustRegister(step Step) {
	if err := r.Register(step); err != nil {
		panic(err)
	}
}

func (r *Registry) Run(ctx context.Context, data Versioned, target int32) error {
	if data == nil {
		return fmt.Errorf("%w: data nil", ErrStepInvalid)
	}
	return r.RunFrom(ctx, data, data.DataVersion(), target)
}

func (r *Registry) RunFrom(ctx context.Context, data Versioned, from int32, target int32) error {
	if r == nil {
		return fmt.Errorf("%w: registry nil", ErrStepInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if data == nil {
		return fmt.Errorf("%w: data nil", ErrStepInvalid)
	}
	if target < from {
		return fmt.Errorf("%w: downgrade %d -> %d", ErrStepInvalid, from, target)
	}
	cur := from
	for cur < target {
		if err := ctx.Err(); err != nil {
			return err
		}
		step, ok := r.step(cur)
		if !ok {
			return fmt.Errorf("%w: %d -> %d", ErrPathMissing, cur, target)
		}
		if step.To > target {
			return fmt.Errorf("%w: step %s overshoots target %d", ErrPathMissing, step.Name, target)
		}
		if err := step.Apply(ctx, data); err != nil {
			return fmt.Errorf("migration: apply %s: %w", step.Name, err)
		}
		cur = step.To
		data.SetDataVersion(cur)
	}
	return nil
}

func (r *Registry) Steps() []Step {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	steps := make([]Step, 0, len(r.steps))
	for _, step := range r.steps {
		steps = append(steps, step)
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i].From < steps[j].From })
	return steps
}

func (r *Registry) step(from int32) (Step, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	step, ok := r.steps[from]
	return step, ok
}

type DAOStep struct {
	Name       string
	Collection string
	From       uint32
	To         uint32
	Apply      func(context.Context, []byte) ([]byte, error)
}

type DAORegistry struct {
	mu    sync.RWMutex
	steps map[string]map[uint32]DAOStep
}

func NewDAORegistry() *DAORegistry {
	return &DAORegistry{steps: make(map[string]map[uint32]DAOStep)}
}

var defaultDAORegistry = NewDAORegistry()

func DefaultDAORegistry() *DAORegistry {
	return defaultDAORegistry
}

func RegisterDAO(step DAOStep) error {
	return defaultDAORegistry.RegisterDAO(step)
}

func MustRegisterDAO(step DAOStep) {
	defaultDAORegistry.MustRegisterDAO(step)
}

func MigrateDAO(collection string, raw []byte, from uint32, target uint32) ([]byte, error) {
	out, _, err := defaultDAORegistry.MigrateDAO(context.Background(), collection, raw, from, target)
	return out, err
}

func (r *DAORegistry) RegisterDAO(step DAOStep) error {
	if r == nil {
		return fmt.Errorf("%w: dao registry nil", ErrStepInvalid)
	}
	if step.Collection == "" {
		return fmt.Errorf("%w: dao collection empty", ErrStepInvalid)
	}
	if step.To <= step.From {
		return fmt.Errorf("%w: invalid dao version %d -> %d", ErrStepInvalid, step.From, step.To)
	}
	if step.Apply == nil {
		return fmt.Errorf("%w: dao apply nil %s %d -> %d", ErrStepInvalid, step.Collection, step.From, step.To)
	}
	if step.Name == "" {
		step.Name = fmt.Sprintf("%s_%d_to_%d", step.Collection, step.From, step.To)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	byFrom := r.steps[step.Collection]
	if byFrom == nil {
		byFrom = make(map[uint32]DAOStep)
		r.steps[step.Collection] = byFrom
	}
	if _, exists := byFrom[step.From]; exists {
		return fmt.Errorf("%w: duplicate dao step %s from %d", ErrStepInvalid, step.Collection, step.From)
	}
	byFrom[step.From] = step
	return nil
}

func (r *DAORegistry) MustRegisterDAO(step DAOStep) {
	if err := r.RegisterDAO(step); err != nil {
		panic(err)
	}
}

func (r *DAORegistry) MigrateDAO(ctx context.Context, collection string, raw []byte, from uint32, target uint32) ([]byte, uint32, error) {
	if r == nil {
		return nil, from, fmt.Errorf("%w: dao registry nil", ErrStepInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if collection == "" {
		return nil, from, fmt.Errorf("%w: dao collection empty", ErrStepInvalid)
	}
	if target < from {
		return nil, from, fmt.Errorf("%w: dao downgrade %d -> %d", ErrStepInvalid, from, target)
	}
	cur := from
	out := append([]byte(nil), raw...)
	for cur < target {
		if err := ctx.Err(); err != nil {
			return nil, cur, err
		}
		step, ok := r.daoStep(collection, cur)
		if !ok {
			return nil, cur, fmt.Errorf("%w: dao %s %d -> %d", ErrPathMissing, collection, cur, target)
		}
		if step.To > target {
			return nil, cur, fmt.Errorf("%w: dao step %s overshoots target %d", ErrPathMissing, step.Name, target)
		}
		next, err := step.Apply(ctx, append([]byte(nil), out...))
		if err != nil {
			return nil, cur, fmt.Errorf("migration: apply dao %s: %w", step.Name, err)
		}
		out = append([]byte(nil), next...)
		cur = step.To
	}
	return out, cur, nil
}

func (r *DAORegistry) DAOSteps(collection string) []DAOStep {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	byFrom := r.steps[collection]
	steps := make([]DAOStep, 0, len(byFrom))
	for _, step := range byFrom {
		steps = append(steps, step)
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i].From < steps[j].From })
	return steps
}

func (r *DAORegistry) daoStep(collection string, from uint32) (DAOStep, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	byFrom := r.steps[collection]
	step, ok := byFrom[from]
	return step, ok
}
