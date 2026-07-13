package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type blockingPoolTask struct {
	released atomic.Bool
}

func (t *blockingPoolTask) OnRelease() {
	t.released.Store(true)
}

func TestPoolStopWithContextReturnsWhenHandlerIsBlocked(t *testing.T) {
	started := make(chan struct{})
	unblock := make(chan struct{})
	pool := NewPool[*blockingPoolTask](PoolConfig{Name: "shutdown-test", WorkerNum: 1, QueueCap: 1}, func(*blockingPoolTask) {
		close(started)
		<-unblock
	})
	pool.Start()
	task := &blockingPoolTask{}
	if err := pool.TryDispatch(1, task); err != nil {
		t.Fatalf("TryDispatch: %v", err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := pool.StopWithContext(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopWithContext err = %v, want context deadline", err)
	}
	close(unblock)
}
