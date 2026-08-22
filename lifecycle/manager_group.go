package lifecycle

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrManagerGroupState = errors.New("lifecycle: invalid manager group state")
	ErrManagerNameEmpty  = errors.New("lifecycle: manager name is empty")
	ErrManagerDuplicate  = errors.New("lifecycle: duplicate manager name")
)

// Manager is an ordered runtime component. C is the initialization context
// and R is the shutdown reason used by the owning framework.
type Manager[C any, R any] interface {
	Name() string
	Init(C) error
	Start() error
	Stop(R)
}

type ManagerGroupState uint8

const (
	ManagerGroupNew ManagerGroupState = iota
	ManagerGroupInitialized
	ManagerGroupStarted
	ManagerGroupStopped
)

// ManagerGroup owns one ordered manager lifecycle. Lifecycle operations are
// serialized, Start/Init failures roll back initialized managers in reverse
// order, and Stop is idempotent.
type ManagerGroup[C any, R any] struct {
	opMu sync.Mutex
	mu   sync.RWMutex

	managers    []Manager[C, R]
	initialized int
	state       ManagerGroupState
}

func NewManagerGroup[C any, R any](managers ...Manager[C, R]) (*ManagerGroup[C, R], error) {
	copyOfManagers := append([]Manager[C, R](nil), managers...)
	names := make(map[string]struct{}, len(copyOfManagers))
	for _, manager := range copyOfManagers {
		if manager == nil || manager.Name() == "" {
			return nil, ErrManagerNameEmpty
		}
		if _, exists := names[manager.Name()]; exists {
			return nil, fmt.Errorf("%w: %s", ErrManagerDuplicate, manager.Name())
		}
		names[manager.Name()] = struct{}{}
	}
	return &ManagerGroup[C, R]{managers: copyOfManagers}, nil
}

func (g *ManagerGroup[C, R]) State() ManagerGroupState {
	if g == nil {
		return ManagerGroupStopped
	}
	g.mu.RLock()
	state := g.state
	g.mu.RUnlock()
	return state
}

func (g *ManagerGroup[C, R]) Init(ctx C, rollbackReason R) error {
	if g == nil {
		return fmt.Errorf("%w: group is nil", ErrManagerGroupState)
	}
	g.opMu.Lock()
	defer g.opMu.Unlock()
	if g.State() != ManagerGroupNew {
		return fmt.Errorf("%w: init from %d", ErrManagerGroupState, g.State())
	}
	for index, manager := range g.managers {
		if err := callManagerInit(manager, ctx); err != nil {
			cleanupErr := g.stopInitialized(rollbackReason)
			g.setState(ManagerGroupStopped)
			return errors.Join(fmt.Errorf("lifecycle: init manager %s: %w", manager.Name(), err), cleanupErr)
		}
		g.setInitialized(index + 1)
	}
	g.setState(ManagerGroupInitialized)
	return nil
}

func (g *ManagerGroup[C, R]) Start(rollbackReason R) error {
	if g == nil {
		return fmt.Errorf("%w: group is nil", ErrManagerGroupState)
	}
	g.opMu.Lock()
	defer g.opMu.Unlock()
	if g.State() != ManagerGroupInitialized {
		return fmt.Errorf("%w: start from %d", ErrManagerGroupState, g.State())
	}
	for _, manager := range g.managers {
		if err := callManagerStart(manager); err != nil {
			cleanupErr := g.stopInitialized(rollbackReason)
			g.setState(ManagerGroupStopped)
			return errors.Join(fmt.Errorf("lifecycle: start manager %s: %w", manager.Name(), err), cleanupErr)
		}
	}
	g.setState(ManagerGroupStarted)
	return nil
}

func (g *ManagerGroup[C, R]) Stop(reason R) error {
	if g == nil {
		return nil
	}
	g.opMu.Lock()
	defer g.opMu.Unlock()
	if g.State() == ManagerGroupStopped {
		return nil
	}
	err := g.stopInitialized(reason)
	g.setState(ManagerGroupStopped)
	return err
}

func (g *ManagerGroup[C, R]) stopInitialized(reason R) error {
	g.mu.RLock()
	initialized := g.initialized
	g.mu.RUnlock()
	var errs []error
	for index := initialized - 1; index >= 0; index-- {
		manager := g.managers[index]
		if err := callManagerStop(manager, reason); err != nil {
			errs = append(errs, fmt.Errorf("lifecycle: stop manager %s: %w", manager.Name(), err))
		}
	}
	g.setInitialized(0)
	return errors.Join(errs...)
}

func (g *ManagerGroup[C, R]) setInitialized(count int) {
	g.mu.Lock()
	g.initialized = count
	g.mu.Unlock()
}

func (g *ManagerGroup[C, R]) setState(state ManagerGroupState) {
	g.mu.Lock()
	g.state = state
	g.mu.Unlock()
}

func callManagerInit[C any, R any](manager Manager[C, R], ctx C) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return manager.Init(ctx)
}

func callManagerStart[C any, R any](manager Manager[C, R]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return manager.Start()
}

func callManagerStop[C any, R any](manager Manager[C, R], reason R) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	manager.Stop(reason)
	return nil
}
