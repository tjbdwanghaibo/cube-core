package worker

import (
	"github.com/tjbdwanghaibo/cube-core/misc"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Task is the constraint for items processed by workers.
type Task interface {
	OnRelease()
}

var (
	ErrWorkerClosed    = errors.New("worker: closed")
	ErrWorkerQueueFull = errors.New("worker: queue full")
)

// Worker is an MPSC queue-based single-threaded task processor.
type Worker[T Task] struct {
	id      int64
	name    string
	queue   *misc.MPSCQueue[T]
	handler func(T)
	wg      *sync.WaitGroup
	closed  atomic.Bool
	notify  chan struct{}
}

func NewWorker[T Task](name string, id int64, queueCap int, handler func(T)) *Worker[T] {
	return &Worker[T]{
		id:      id,
		name:    name,
		queue:   misc.NewMPSCQueue[T](queueCap),
		handler: handler,
		notify:  make(chan struct{}, 1),
	}
}

func (w *Worker[T]) Run(wg *sync.WaitGroup) {
	w.wg = wg
	go func() {
		defer w.wg.Done()
		w.loop()
	}()
}

func (w *Worker[T]) loop() {
	for {
		for {
			task, ok := w.queue.Pop()
			if !ok {
				break
			}
			w.safeHandle(task)
		}
		if w.closed.Load() {
			for {
				task, ok := w.queue.Pop()
				if !ok {
					return
				}
				w.safeHandle(task)
			}
		}
		<-w.notify
	}
}

func (w *Worker[T]) safeHandle(task T) {
	misc.SafeFunc(func() {
		defer task.OnRelease()
		if w.handler != nil {
			w.handler(task)
		}
	})
}

// TryCast enqueues a task without waiting. It returns false when the queue is
// full or the worker has already been closed.
func (w *Worker[T]) TryCast(task T) bool {
	if w.closed.Load() {
		return false
	}
	if !w.queue.Push(task) {
		return false
	}
	select {
	case w.notify <- struct{}{}:
	default:
	}
	return true
}

// Cast enqueues a task to this worker with a bounded wait. Prefer TryCast or
// Pool.TryDispatch when the caller needs explicit backpressure handling.
func (w *Worker[T]) Cast(task T) {
	spin := 0
	for !w.TryCast(task) {
		if w.closed.Load() {
			task.OnRelease()
			return
		}
		spin++
		if spin < 64 {
			runtime.Gosched()
			continue
		}
		time.Sleep(time.Microsecond)
		if spin > 1024 {
			task.OnRelease()
			return
		}
	}
}

// Close signals the worker to drain and stop.
func (w *Worker[T]) Close() {
	w.closed.Store(true)
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *Worker[T]) QueueLen() int {
	if w == nil || w.queue == nil {
		return 0
	}
	return w.queue.Len()
}

func (w *Worker[T]) QueueCap() int {
	if w == nil || w.queue == nil {
		return 0
	}
	return w.queue.Cap()
}
