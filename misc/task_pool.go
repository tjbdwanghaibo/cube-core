package misc

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Task interface {
	GetID() int64
	Execute() error
	String() string
}

type TaskFunc struct {
	ID   int64
	Func func() error
	Desc string
}

func (t *TaskFunc) GetID() int64   { return t.ID }
func (t *TaskFunc) Execute() error { return t.Func() }
func (t *TaskFunc) String() string {
	if t.Desc != "" {
		return t.Desc
	}
	return fmt.Sprintf("Task[%d]", t.ID)
}

type TaskPoolConfig struct {
	WorkerCount     int
	MaxTaskCount    int
	ShutdownTimeout time.Duration
}

func DefaultTaskPoolConfig() *TaskPoolConfig {
	return &TaskPoolConfig{
		WorkerCount:     4,
		MaxTaskCount:    1000,
		ShutdownTimeout: 30 * time.Second,
	}
}

type TaskPool struct {
	config    *TaskPoolConfig
	workers   []*taskWorker
	running   atomic.Bool
	wg        sync.WaitGroup
	closeOnce sync.Once

	totalTasks     atomic.Int64
	completedTasks atomic.Int64
	failedTasks    atomic.Int64
}

func NewTaskPool(config *TaskPoolConfig) *TaskPool {
	if config == nil {
		config = DefaultTaskPoolConfig()
	}
	pool := &TaskPool{
		config:  config,
		workers: make([]*taskWorker, config.WorkerCount),
	}
	for i := 0; i < config.WorkerCount; i++ {
		pool.workers[i] = newTaskWorker(i, config.MaxTaskCount/config.WorkerCount)
	}
	return pool
}

func (tp *TaskPool) Start() {
	if !tp.running.CompareAndSwap(false, true) {
		return
	}
	for _, w := range tp.workers {
		tp.wg.Add(1)
		go func(worker *taskWorker) {
			defer tp.wg.Done()
			worker.run(tp)
		}(w)
	}
}

func (tp *TaskPool) Submit(task Task) error {
	if !tp.running.Load() {
		return fmt.Errorf("task pool is not running")
	}
	workerIndex := tp.hashTaskID(task.GetID())
	if err := tp.workers[workerIndex].submitTask(task); err != nil {
		return fmt.Errorf("failed to submit task[%d]: %w", task.GetID(), err)
	}
	tp.totalTasks.Add(1)
	return nil
}

func (tp *TaskPool) SubmitFunc(taskID int64, desc string, fn func() error) error {
	return tp.Submit(&TaskFunc{ID: taskID, Func: fn, Desc: desc})
}

func (tp *TaskPool) Shutdown() error {
	return tp.ShutdownWithTimeout(tp.config.ShutdownTimeout)
}

func (tp *TaskPool) ShutdownWithTimeout(timeout time.Duration) error {
	tp.closeOnce.Do(func() {
		if !tp.running.Load() {
			return
		}
		tp.running.Store(false)
		for _, w := range tp.workers {
			w.close()
		}
	})

	done := make(chan struct{})
	go func() {
		tp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("task pool shutdown timeout after %v", timeout)
	}
}

func (tp *TaskPool) GetStats() (total, completed, failed int64, running bool) {
	return tp.totalTasks.Load(), tp.completedTasks.Load(),
		tp.failedTasks.Load(), tp.running.Load()
}

func (tp *TaskPool) IsRunning() bool {
	return tp.running.Load()
}

func (tp *TaskPool) hashTaskID(taskID int64) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%d", taskID)))
	return int(h.Sum32()) % len(tp.workers)
}

type taskWorker struct {
	id       int
	taskChan chan Task
	closed   atomic.Bool
}

func newTaskWorker(id int, bufferSize int) *taskWorker {
	if bufferSize <= 0 {
		bufferSize = 100
	}
	return &taskWorker{
		id:       id,
		taskChan: make(chan Task, bufferSize),
	}
}

func (w *taskWorker) submitTask(task Task) error {
	if w.closed.Load() {
		return fmt.Errorf("worker[%d] is closed", w.id)
	}
	select {
	case w.taskChan <- task:
		return nil
	default:
		return fmt.Errorf("worker[%d] task queue is full", w.id)
	}
}

func (w *taskWorker) close() {
	if w.closed.CompareAndSwap(false, true) {
		close(w.taskChan)
	}
}

func (w *taskWorker) run(pool *TaskPool) {
	for task := range w.taskChan {
		w.executeTask(task, pool)
	}
}

func (w *taskWorker) executeTask(task Task, pool *TaskPool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("TaskPool worker panic", "worker", w.id, "task", task.GetID(), "err", r)
			pool.failedTasks.Add(1)
		}
	}()

	err := task.Execute()
	if err != nil {
		slog.Error("TaskPool task failed", "worker", w.id, "task", task.String(), "err", err)
		pool.failedTasks.Add(1)
	} else {
		pool.completedTasks.Add(1)
	}
}
