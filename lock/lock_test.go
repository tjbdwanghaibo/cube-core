package lock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReentrantMutex_Basic(t *testing.T) {
	mu := NewReentrantMutex(1)

	mu.Lock()
	mu.Lock() // reentrant
	mu.Unlock()
	mu.Unlock()
}

func TestReentrantMutex_TryLock(t *testing.T) {
	mu := NewReentrantMutex(2)

	if !mu.TryLock() {
		t.Fatal("TryLock should succeed")
	}
	// Reentrant TryLock
	if !mu.TryLock() {
		t.Fatal("Reentrant TryLock should succeed")
	}
	mu.Unlock()
	mu.Unlock()
}

func TestReentrantMutex_Contention(t *testing.T) {
	mu := NewReentrantMutex(3)
	var counter int64
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			atomic.AddInt64(&counter, 1)
		}()
	}
	wg.Wait()

	if counter != 100 {
		t.Fatalf("expected 100, got %d", counter)
	}
}

func TestReentrantMutex_LockWithTimeout(t *testing.T) {
	mu := NewReentrantMutex(4)
	mu.Lock()

	done := make(chan bool, 1)
	go func() {
		// Should timeout since lock is held by another goroutine
		ok := mu.LockWithTimeout(50 * time.Millisecond)
		done <- ok
	}()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("LockWithTimeout should have failed")
		}
	case <-time.After(time.Second):
		t.Fatal("LockWithTimeout did not return in time")
	}

	mu.Unlock()
}

func TestReentrantMutex_LockId(t *testing.T) {
	mu := NewReentrantMutex(42)
	if mu.LockId() != 42 {
		t.Fatalf("expected LockId 42, got %d", mu.LockId())
	}
}

func TestDefaultMutex(t *testing.T) {
	mu := NewDefaultMutex(99)
	if mu.LockId() != 99 {
		t.Fatalf("expected LockId 99, got %d", mu.LockId())
	}
	mu.Lock()
	mu.Unlock()

	if !mu.TryLock() {
		t.Fatal("TryLock should succeed")
	}
	mu.Unlock()
}

func TestLockManager_GetLock(t *testing.T) {
	mgr := NewLockManager(nil)

	mu1 := mgr.GetLock(100)
	if mu1 == nil {
		t.Fatal("GetLock should return non-nil")
	}
	if mu1.LockId() != 100 {
		t.Fatalf("expected LockId 100, got %d", mu1.LockId())
	}

	// Same ID returns same lock
	mu2 := mgr.GetLock(100)
	if mu1 != mu2 {
		t.Fatal("GetLock should return same instance for same ID")
	}

	// Different ID returns different lock
	mu3 := mgr.GetLock(200)
	if mu1 == mu3 {
		t.Fatal("different IDs should return different locks")
	}
}

func TestLockManager_ReleaseLock(t *testing.T) {
	mgr := NewLockManager(nil)

	mu1 := mgr.GetLock(300)
	mgr.ReleaseLock(300)

	// After release, a new lock instance is created
	mu2 := mgr.GetLock(300)
	if mu1 == mu2 {
		t.Fatal("after release, GetLock should return a new instance")
	}
}

func TestLockManager_CustomFactory(t *testing.T) {
	var callCount atomic.Int32
	factory := func(id int64) Mutex {
		callCount.Add(1)
		return NewDefaultMutex(id)
	}

	mgr := NewLockManager(factory)
	mgr.GetLock(1)
	mgr.GetLock(2)

	if callCount.Load() != 2 {
		t.Fatalf("expected factory called 2 times, got %d", callCount.Load())
	}
}
