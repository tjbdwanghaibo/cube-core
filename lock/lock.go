package lock

import (
	"sync"
	"time"
)

// Mutex is the lock interface used by entity system.
// Implementations include ReentrantMutex (local) and distributed locks (app-layer).
type Mutex interface {
	TryLock() bool
	Lock()
	LockWithTimeout(timeout time.Duration) bool
	Unlock()
	LockIdGetter
}

// LockIdGetter provides lock identity.
type LockIdGetter interface {
	LockId() int64
}

// defaultMutex wraps sync.Mutex to satisfy the Mutex interface.
var _ Mutex = (*defaultMutex)(nil)

type defaultMutex struct {
	sync.Mutex
	id int64
}

func (d *defaultMutex) LockWithTimeout(timeout time.Duration) bool {
	d.Lock()
	return true
}

func (d *defaultMutex) LockId() int64 {
	return d.id
}

// NewDefaultMutex creates a simple non-reentrant mutex satisfying the Mutex interface.
func NewDefaultMutex(id int64) Mutex {
	return &defaultMutex{id: id}
}
