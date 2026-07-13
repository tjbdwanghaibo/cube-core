package worker

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/misc"
	"sync"
)

// PoolConfig configures a worker pool.
type PoolConfig struct {
	Name      string
	WorkerNum int
	QueueCap  int
}

type PoolStats struct {
	Name      string
	WorkerNum int
	QueueCap  int
	QueueLen  int
	Started   bool
	Stopped   bool
}

// Pool is a hash-based worker pool that dispatches tasks to workers
// deterministically by key, ensuring same-key tasks execute sequentially.
type Pool[T Task] struct {
	name      string
	workers   []*Worker[T]
	workerNum int
	queueCap  int
	handler   func(T)
	wg        sync.WaitGroup
	mu        sync.Mutex
	started   bool
	stopped   bool
	stopDone  chan struct{}
}

func NewPool[T Task](cfg PoolConfig, handler func(T)) *Pool[T] {
	num := cfg.WorkerNum
	if num <= 0 {
		num = 1
	}
	cap := cfg.QueueCap
	if cap <= 0 {
		cap = 1024
	}
	return &Pool[T]{
		name:      cfg.Name,
		workerNum: num,
		queueCap:  cap,
		handler:   handler,
	}
}

// Start creates and runs all workers.
func (p *Pool[T]) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.workers = make([]*Worker[T], p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		p.workers[i] = NewWorker[T](p.name, int64(i), p.queueCap, p.handler)
	}
	for _, w := range p.workers {
		p.wg.Add(1)
		w.Run(&p.wg)
	}
	p.started = true
	p.stopped = false
	p.stopDone = nil
}

// Stop closes all workers and waits for drain.
func (p *Pool[T]) Stop() {
	_ = p.StopWithContext(context.Background())
}

func (p *Pool[T]) StopWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	if p.stopped {
		done := p.stopDone
		p.mu.Unlock()
		return waitPoolStop(ctx, done)
	}
	p.stopped = true
	workers := p.workers
	done := make(chan struct{})
	p.stopDone = done
	p.mu.Unlock()

	for _, w := range workers {
		if w != nil {
			w.Close()
		}
	}
	go func() {
		p.wg.Wait()
		close(done)
	}()
	return waitPoolStop(ctx, done)
}

func waitPoolStop(ctx context.Context, done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool[T]) pickWorker(key int64) (*Worker[T], error) {
	p.mu.Lock()
	workers := p.workers
	started := p.started
	stopped := p.stopped
	workerNum := p.workerNum
	p.mu.Unlock()

	if !started || stopped || len(workers) == 0 || workerNum <= 0 {
		return nil, ErrWorkerClosed
	}
	idx := misc.Hash64(uint64(key)) % uint64(workerNum)
	if idx >= uint64(len(workers)) {
		return nil, ErrWorkerClosed
	}
	return workers[idx], nil
}

// TryDispatch routes a task to a worker without waiting for queue space.
// Same key always goes to the same worker.
func (p *Pool[T]) TryDispatch(key int64, task T) error {
	w, err := p.pickWorker(key)
	if err != nil {
		return err
	}
	if !w.TryCast(task) {
		return ErrWorkerQueueFull
	}
	return nil
}

// Dispatch routes a task to a worker and releases it when it cannot be queued.
func (p *Pool[T]) Dispatch(key int64, task T) error {
	if err := p.TryDispatch(key, task); err != nil {
		task.OnRelease()
		return err
	}
	return nil
}

// Go executes a task in an independent goroutine (for expensive/blocking tasks).
func (p *Pool[T]) Go(task T, handler func(T)) {
	go func() {
		misc.SafeFunc(func() {
			defer task.OnRelease()
			handler(task)
		})
	}()
}

// WorkerNum returns the number of workers in the pool.
func (p *Pool[T]) WorkerNum() int {
	return p.workerNum
}

func (p *Pool[T]) Stats() PoolStats {
	if p == nil {
		return PoolStats{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := PoolStats{
		Name:      p.name,
		WorkerNum: p.workerNum,
		QueueCap:  p.queueCap,
		Started:   p.started,
		Stopped:   p.stopped,
	}
	for _, w := range p.workers {
		if w != nil {
			stats.QueueLen += w.QueueLen()
		}
	}
	return stats
}
