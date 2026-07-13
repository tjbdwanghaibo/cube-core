package lock

import (
	"github.com/tjbdwanghaibo/cube-core/misc"
)

const defaultBucketCnt = 64

// MutexFactory creates a Mutex for the given lock ID.
// Application layer can override to provide custom lock implementations (e.g., distributed locks).
type MutexFactory func(id int64) Mutex

// LockManager manages a sharded pool of Mutex instances by entity ID.
// Similar to entity.Mgr — set globally and used by entity system.
type LockManager struct {
	locks   *misc.BucketHolder[int64, Mutex]
	factory MutexFactory
}

// NewLockManager creates a lock manager with the given factory.
// If factory is nil, defaults to NewReentrantMutex.
func NewLockManager(factory MutexFactory) *LockManager {
	if factory == nil {
		factory = func(id int64) Mutex {
			return NewReentrantMutex(id)
		}
	}
	mgr := &LockManager{
		factory: factory,
	}
	mgr.locks = misc.NewBucketHolder[int64, Mutex](defaultBucketCnt, mgr.factory, true)
	return mgr
}

// GetLock returns the Mutex for the given entity ID, creating one if necessary.
func (m *LockManager) GetLock(id int64) Mutex {
	return m.locks.Get(id)
}

// ReleaseLock removes the lock for the given ID from the manager.
func (m *LockManager) ReleaseLock(id int64) {
	m.locks.Del(id)
}

// Mgr is the global lock manager instance.
// Must be set by the application layer before creating entities.
var Mgr *LockManager
