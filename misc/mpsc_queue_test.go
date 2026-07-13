package misc

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestMPSCQueue_Basic(t *testing.T) {
	q := NewMPSCQueue[int](16)

	if q.Cap() != 16 {
		t.Fatalf("expected cap 16, got %d", q.Cap())
	}

	// Push and pop
	if !q.Push(1) {
		t.Fatal("push should succeed")
	}
	if !q.Push(2) {
		t.Fatal("push should succeed")
	}
	if q.Len() != 2 {
		t.Fatalf("expected len 2, got %d", q.Len())
	}

	val, ok := q.Pop()
	if !ok || val != 1 {
		t.Fatalf("expected 1, got %d, ok=%v", val, ok)
	}
	val, ok = q.Pop()
	if !ok || val != 2 {
		t.Fatalf("expected 2, got %d, ok=%v", val, ok)
	}

	// Empty pop
	_, ok = q.Pop()
	if ok {
		t.Fatal("pop from empty queue should return false")
	}
}

func TestMPSCQueue_Full(t *testing.T) {
	q := NewMPSCQueue[int](16)

	for i := 0; i < 16; i++ {
		if !q.Push(i) {
			t.Fatalf("push %d should succeed", i)
		}
	}

	// Queue is full
	if q.Push(99) {
		t.Fatal("push to full queue should fail")
	}

	// Pop one and push again
	val, ok := q.Pop()
	if !ok || val != 0 {
		t.Fatalf("expected 0, got %d", val)
	}
	if !q.Push(99) {
		t.Fatal("push after pop should succeed")
	}
}

func TestMPSCQueue_MPSC(t *testing.T) {
	q := NewMPSCQueue[int64](1024)
	const producers = 8
	const perProducer = 10000

	var wg sync.WaitGroup
	var pushFailed atomic.Int64

	// Multiple producers
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			base := int64(pid) * perProducer
			for i := int64(0); i < perProducer; i++ {
				for !q.Push(base + i) {
					pushFailed.Add(1)
				}
			}
		}(p)
	}

	// Single consumer
	consumed := make([]int64, 0, producers*perProducer)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	draining := false
	for {
		val, ok := q.Pop()
		if ok {
			consumed = append(consumed, val)
			if len(consumed) == producers*perProducer {
				break
			}
		} else {
			if draining {
				break
			}
			select {
			case <-done:
				draining = true
			default:
			}
		}
	}

	// Drain remaining
	for {
		val, ok := q.Pop()
		if !ok {
			break
		}
		consumed = append(consumed, val)
	}

	if len(consumed) != producers*perProducer {
		t.Fatalf("expected %d items, got %d", producers*perProducer, len(consumed))
	}

	// Verify all values present
	seen := make(map[int64]bool, len(consumed))
	for _, v := range consumed {
		if seen[v] {
			t.Fatalf("duplicate value %d", v)
		}
		seen[v] = true
	}
}

func TestMPSCQueue_PowerOfTwo(t *testing.T) {
	q := NewMPSCQueue[int](10)
	if q.Cap() != 16 {
		t.Fatalf("expected 16, got %d", q.Cap())
	}

	q2 := NewMPSCQueue[int](1)
	if q2.Cap() != 16 {
		t.Fatalf("expected min 16, got %d", q2.Cap())
	}
}

func BenchmarkMPSCQueue_Push(b *testing.B) {
	q := NewMPSCQueue[int64](65536)

	// Drain in background
	go func() {
		for {
			q.Pop()
		}
	}()

	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			for !q.Push(i) {
			}
			i++
		}
	})
}
