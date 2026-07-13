package health

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Status string

const (
	StatusOK       Status = "ok"
	StatusFail     Status = "fail"
	StatusDegraded Status = "degraded"
)

type Result struct {
	Name      string `json:"name"`
	Status    Status `json:"status"`
	Message   string `json:"message,omitempty"`
	Error     string `json:"error,omitempty"`
	CheckedAt int64  `json:"checked_at_ms"`
	Err       error  `json:"-"`
}

type Snapshot struct {
	OK      bool     `json:"ok"`
	Results []Result `json:"results"`
}

type Checker interface {
	CheckHealth(context.Context) Result
}

type CheckerFunc func(context.Context) Result

func (f CheckerFunc) CheckHealth(ctx context.Context) Result {
	if f == nil {
		return Result{Status: StatusFail, Message: "checker is nil"}
	}
	return f(ctx)
}

type Registry struct {
	mu       sync.RWMutex
	checkers map[string]Checker
}

func NewRegistry() *Registry {
	return &Registry{checkers: make(map[string]Checker)}
}

var defaultRegistry = NewRegistry()

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func Register(name string, checker Checker) {
	defaultRegistry.Register(name, checker)
}

func Check(ctx context.Context) Snapshot {
	return defaultRegistry.Snapshot(ctx)
}

func (r *Registry) Register(name string, checker Checker) {
	if r == nil || name == "" || checker == nil {
		return
	}
	r.mu.Lock()
	r.checkers[name] = checker
	r.mu.Unlock()
}

func (r *Registry) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.checkers = make(map[string]Checker)
	r.mu.Unlock()
}

func (r *Registry) Snapshot(ctx context.Context) Snapshot {
	if r == nil {
		return Snapshot{OK: true}
	}
	r.mu.RLock()
	names := make([]string, 0, len(r.checkers))
	checkers := make(map[string]Checker, len(r.checkers))
	for name, checker := range r.checkers {
		names = append(names, name)
		checkers[name] = checker
	}
	r.mu.RUnlock()
	sort.Strings(names)

	snap := Snapshot{OK: true, Results: make([]Result, 0, len(names))}
	now := time.Now().UnixMilli()
	for _, name := range names {
		result := checkOne(ctx, name, checkers[name], now)
		if result.Status != StatusOK {
			snap.OK = false
		}
		snap.Results = append(snap.Results, result)
	}
	return snap
}

func checkOne(ctx context.Context, name string, checker Checker, now int64) (result Result) {
	defer func() {
		if r := recover(); r != nil {
			result = Result{
				Name:      name,
				Status:    StatusFail,
				Error:     fmt.Sprintf("panic: %v", r),
				CheckedAt: now,
			}
		}
	}()
	result = checker.CheckHealth(ctx)
	result.Name = name
	if result.Status == "" {
		result.Status = StatusOK
	}
	if result.Err != nil {
		result.Error = result.Err.Error()
	}
	if result.CheckedAt == 0 {
		result.CheckedAt = now
	}
	return result
}
