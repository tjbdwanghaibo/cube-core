package misc

import (
	"runtime"
	"sync/atomic"
)

// MPSCQueue is a lock-free multi-producer single-consumer bounded ring buffer.
// Uses a power-of-two sized buffer with atomic sequence numbers per slot
// to coordinate multiple producers and a single consumer without locks.
type MPSCQueue[T any] struct {
	buf  []mpscSlot[T]
	mask uint64
	_pad [48]byte // cache line padding

	head atomic.Uint64
	_ph  [56]byte // cache line padding

	tail atomic.Uint64
	_pt  [56]byte // cache line padding
}

type mpscSlot[T any] struct {
	seq atomic.Uint64
	val T
}

// NewMPSCQueue creates a queue with the given capacity (rounded up to power of 2).
// Minimum capacity is 16.
func NewMPSCQueue[T any](capacity int) *MPSCQueue[T] {
	cap2 := nextPow2(capacity)
	if cap2 < 16 {
		cap2 = 16
	}
	q := &MPSCQueue[T]{
		buf:  make([]mpscSlot[T], cap2),
		mask: uint64(cap2 - 1),
	}
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
	}
	return q
}

// Push enqueues an item. Returns false if the queue is full.
// Safe to call from multiple goroutines concurrently.
func (q *MPSCQueue[T]) Push(val T) bool {
	for spin := 0; ; spin++ {
		head := q.head.Load()
		slot := &q.buf[head&q.mask]
		seq := slot.seq.Load()
		diff := int64(seq) - int64(head)

		if diff == 0 {
			// Slot is available for writing
			if q.head.CompareAndSwap(head, head+1) {
				slot.val = val
				slot.seq.Store(head + 1)
				return true
			}
		} else if diff < 0 {
			// Queue is full
			return false
		}
		// Another producer took this slot, retry
		if spin > 4 {
			runtime.Gosched()
			spin = 0
		}
	}
}

// Pop dequeues an item. Returns zero value and false if queue is empty.
// Must be called from a single goroutine only.
func (q *MPSCQueue[T]) Pop() (val T, ok bool) {
	tail := q.tail.Load()
	slot := &q.buf[tail&q.mask]
	seq := slot.seq.Load()
	diff := int64(seq) - int64(tail+1)

	if diff == 0 {
		// Item is ready
		q.tail.Store(tail + 1)
		val = slot.val
		var zero T
		slot.val = zero // clear reference for GC
		slot.seq.Store(tail + q.mask + 1)
		ok = true
		return
	}
	// Queue is empty
	return
}

// Len returns an approximate count of items in the queue.
func (q *MPSCQueue[T]) Len() int {
	head := q.head.Load()
	tail := q.tail.Load()
	n := int64(head) - int64(tail)
	if n < 0 {
		return 0
	}
	return int(n)
}

// Cap returns the queue capacity.
func (q *MPSCQueue[T]) Cap() int {
	return int(q.mask + 1)
}

func nextPow2(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}
