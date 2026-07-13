package entitysync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestAsyncSinkEnqueueBatchDoesNotWaitForSlowDownstream(t *testing.T) {
	downstream := newBlockingAsyncTestSink()
	sink := NewAsyncSink(downstream, AsyncSinkOptions{
		ShardCount:    1,
		QueueCapacity: 8,
		MaxBatch:      16,
	})
	defer closeAsyncSinkForTest(t, sink)

	ref := entity.NewPlayerSyncObserver(10)
	start := time.Now()
	sink.EnqueueBatch([]entity.SyncPacket{{
		Observer: ref,
		EntityID: 100,
		Type:     entity.SyncPacketUpdate,
		Version:  1,
		Mask:     1,
	}})
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("async enqueue blocked for %s", elapsed)
	}
	if !downstream.waitStarted(200 * time.Millisecond) {
		t.Fatal("downstream did not receive async batch")
	}
	downstream.release()
	if err := sink.Drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	stats := sink.Stats()
	if stats.DeliveredPackets != 1 || stats.DroppedPackets != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestAsyncSinkPreservesOrderPerObserver(t *testing.T) {
	downstream := &orderedAsyncTestSink{}
	sink := NewAsyncSink(downstream, AsyncSinkOptions{
		ShardCount:    4,
		QueueCapacity: 32,
		MaxBatch:      8,
	})
	defer closeAsyncSinkForTest(t, sink)

	ref := entity.NewPlayerSyncObserver(11)
	for seq := uint64(1); seq <= 20; seq++ {
		sink.Enqueue(entity.SyncPacket{
			Observer: ref,
			EntityID: int64(seq),
			Type:     entity.SyncPacketUpdate,
			Version:  seq,
			Mask:     1,
		})
	}
	if err := sink.Drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}

	got := downstream.versionsFor(ref)
	if len(got) != 20 {
		t.Fatalf("versions len = %d, got %v", len(got), got)
	}
	for i, version := range got {
		want := uint64(i + 1)
		if version != want {
			t.Fatalf("versions = %v, want ordered 1..20", got)
		}
	}
}

func TestAsyncSinkDropsUpdateWhenShardQueueIsFull(t *testing.T) {
	downstream := newBlockingAsyncTestSink()
	sink := NewAsyncSink(downstream, AsyncSinkOptions{
		ShardCount:    1,
		QueueCapacity: 1,
		MaxBatch:      1,
	})
	defer closeAsyncSinkForTest(t, sink)

	ref := entity.NewPlayerSyncObserver(12)
	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 1, Type: entity.SyncPacketUpdate, Version: 1, Mask: 1})
	if !downstream.waitStarted(200 * time.Millisecond) {
		t.Fatal("downstream did not start first batch")
	}
	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 2, Type: entity.SyncPacketUpdate, Version: 2, Mask: 1})
	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 3, Type: entity.SyncPacketUpdate, Version: 3, Mask: 1})

	stats := sink.Stats()
	if stats.QueueFull == 0 || stats.DroppedPackets == 0 {
		t.Fatalf("stats = %+v, want queue-full update drop", stats)
	}
	downstream.release()
	if err := sink.Drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
}

func closeAsyncSinkForTest(t *testing.T, sink *AsyncSink) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("close async sink: %v", err)
	}
}

type blockingAsyncTestSink struct {
	started  chan struct{}
	releaseC chan struct{}
	once     sync.Once
}

func newBlockingAsyncTestSink() *blockingAsyncTestSink {
	return &blockingAsyncTestSink{
		started:  make(chan struct{}),
		releaseC: make(chan struct{}),
	}
}

func (s *blockingAsyncTestSink) Enqueue(packet entity.SyncPacket) {
	s.EnqueueBatch([]entity.SyncPacket{packet})
}

func (s *blockingAsyncTestSink) EnqueueBatch([]entity.SyncPacket) {
	s.once.Do(func() { close(s.started) })
	<-s.releaseC
}

func (s *blockingAsyncTestSink) waitStarted(timeout time.Duration) bool {
	select {
	case <-s.started:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *blockingAsyncTestSink) release() {
	select {
	case <-s.releaseC:
	default:
		close(s.releaseC)
	}
}

type orderedAsyncTestSink struct {
	mu       sync.Mutex
	versions map[entity.SyncObserverRef][]uint64
}

func (s *orderedAsyncTestSink) Enqueue(packet entity.SyncPacket) {
	s.EnqueueBatch([]entity.SyncPacket{packet})
}

func (s *orderedAsyncTestSink) EnqueueBatch(packets []entity.SyncPacket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.versions == nil {
		s.versions = make(map[entity.SyncObserverRef][]uint64)
	}
	for _, packet := range packets {
		ref := packet.Observer.Normalize()
		s.versions[ref] = append(s.versions[ref], packet.Version)
	}
}

func (s *orderedAsyncTestSink) versionsFor(ref entity.SyncObserverRef) []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]uint64(nil), s.versions[ref.Normalize()]...)
	return out
}
