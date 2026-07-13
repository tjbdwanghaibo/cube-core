package worker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type workerTestTask struct {
	id       int
	released *atomic.Int64
}

func (t workerTestTask) OnRelease() {
	t.released.Add(1)
}

func TestWorker_ProcessesTasksAndStops(t *testing.T) {
	var released atomic.Int64
	handled := make(chan int, 2)
	w := NewWorker[workerTestTask]("test", 0, 16, func(task workerTestTask) {
		handled <- task.id
	})

	var wg sync.WaitGroup
	wg.Add(1)
	w.Run(&wg)

	w.Cast(workerTestTask{id: 1, released: &released})
	w.Cast(workerTestTask{id: 2, released: &released})

	for i := 0; i < 2; i++ {
		select {
		case <-handled:
		case <-time.After(time.Second):
			t.Fatal("worker did not process task")
		}
	}

	w.Close()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}

	if got := released.Load(); got != 2 {
		t.Fatalf("released tasks: got %d, want 2", got)
	}
}

func TestWorker_CloseWakesIdleWorker(t *testing.T) {
	w := NewWorker[workerTestTask]("test", 0, 16, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	w.Run(&wg)

	w.Close()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle worker was not woken by close")
	}
}
