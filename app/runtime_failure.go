package app

import (
	"errors"
	"sync"
)

// RuntimeFailure is the process-wide fail-stop signal for infrastructure that
// can no longer safely accept writes. The first failure wins and wakes App's
// normal graceful-shutdown path; later reports are joined for diagnostics.
type RuntimeFailure struct {
	once sync.Once
	mu   sync.RWMutex
	err  error
	done chan error
}

func NewRuntimeFailure() *RuntimeFailure {
	return &RuntimeFailure{done: make(chan error, 1)}
}

func (r *RuntimeFailure) Fail(err error) {
	if r == nil || err == nil {
		return
	}
	r.mu.Lock()
	r.err = errors.Join(r.err, err)
	first := r.err
	r.mu.Unlock()
	r.once.Do(func() { r.done <- first })
}

func (r *RuntimeFailure) Done() <-chan error {
	if r == nil {
		return nil
	}
	return r.done
}

func (r *RuntimeFailure) Err() error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.err
}
