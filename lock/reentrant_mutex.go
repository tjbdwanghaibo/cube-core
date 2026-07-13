package lock

import (
	"github.com/tjbdwanghaibo/cube-core/misc"
	"runtime"
	"sync"
	"time"
)

var _ Mutex = (*ReentrantMutex)(nil)

// ReentrantMutex is an in-process reentrant mutual exclusion lock.
// It allows the same goroutine to acquire the lock multiple times without deadlock.
type ReentrantMutex struct {
	mu        sync.Mutex
	owner     int64 // goroutine ID of lock holder
	recursion int32 // reentry count
	id        int64
}

// NewReentrantMutex creates a new reentrant mutex.
func NewReentrantMutex(ids ...int64) *ReentrantMutex {
	var id int64
	if len(ids) > 0 {
		id = ids[0]
	}
	return &ReentrantMutex{id: id}
}

func (rm *ReentrantMutex) LockId() int64 {
	return rm.id
}

// Lock acquires the lock. The same goroutine can call this multiple times.
func (rm *ReentrantMutex) Lock() {
	gid := misc.GoID()

	rm.mu.Lock()
	if rm.owner == gid {
		rm.recursion++
		rm.mu.Unlock()
		return
	}

	for rm.owner != 0 {
		rm.mu.Unlock()
		runtime.Gosched()
		rm.mu.Lock()
	}

	rm.owner = gid
	rm.recursion = 1
	rm.mu.Unlock()
}

// Unlock releases the lock.
func (rm *ReentrantMutex) Unlock() {
	gid := misc.GoID()

	rm.mu.Lock()
	if rm.owner != gid {
		rm.mu.Unlock()
		panic("unlock of unowned mutex")
	}

	recursion := rm.recursion - 1
	rm.recursion = recursion
	if recursion < 0 {
		rm.mu.Unlock()
		panic("unlock of unlocked mutex")
	}

	if recursion == 0 {
		rm.owner = 0
	}
	rm.mu.Unlock()
}

// TryLock attempts to acquire the lock without blocking.
func (rm *ReentrantMutex) TryLock() bool {
	gid := misc.GoID()

	rm.mu.Lock()
	if rm.owner == gid {
		rm.recursion++
		rm.mu.Unlock()
		return true
	}

	if rm.owner != 0 {
		rm.mu.Unlock()
		return false
	}

	rm.owner = gid
	rm.recursion = 1
	rm.mu.Unlock()
	return true
}

// LockWithTimeout acquires the lock with a timeout. Returns false if timeout expires.
func (rm *ReentrantMutex) LockWithTimeout(timeout time.Duration) bool {
	if timeout <= 0 {
		return rm.TryLock()
	}

	gid := misc.GoID()
	deadline := time.Now().Add(timeout)

	rm.mu.Lock()
	if rm.owner == gid {
		rm.recursion++
		rm.mu.Unlock()
		return true
	}

	if rm.owner == 0 {
		rm.owner = gid
		rm.recursion = 1
		rm.mu.Unlock()
		return true
	}

	for rm.owner != 0 {
		if time.Now().After(deadline) {
			rm.mu.Unlock()
			return false
		}

		rm.mu.Unlock()
		remaining := time.Until(deadline)
		waitTime := 1 * time.Millisecond
		if remaining < waitTime {
			waitTime = remaining
		}
		if waitTime > 0 {
			time.Sleep(waitTime)
		} else {
			runtime.Gosched()
		}
		rm.mu.Lock()

		if rm.owner == gid {
			rm.recursion++
			rm.mu.Unlock()
			return true
		}
	}

	rm.owner = gid
	rm.recursion = 1
	rm.mu.Unlock()
	return true
}
