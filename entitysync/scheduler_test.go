package entitysync

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"errors"
	"testing"
	"time"
)

func TestSchedulerFlushesDirtyEntityState(t *testing.T) {
	oldScheduler := entity.GetEntitySyncScheduler()
	defer entity.SetEntitySyncScheduler(oldScheduler)

	downstream := &captureSink{}
	scheduler := NewScheduler(downstream)
	entity.SetEntitySyncScheduler(scheduler)

	base := entity.NewEntityBase(104, entity.EntityCategory(1), false, entity.EntityKind(2))
	base.EnableSync(entity.EntitySyncCreateParam{
		Enabled: true,
		Topic:   "alliance",
		Packer: entity.EntitySyncPackFunc{
			Update: func(observer entity.SyncObserverRef, mask uint64) (entity.SyncPacket, error) {
				return entity.SyncPacket{Body: "delta", Mask: mask}, nil
			},
		},
	})
	if _, ok := base.Sync().AddObserverRef(entity.NewPlayerSyncObserver(401)); !ok {
		t.Fatal("observer should be added")
	}

	base.MarkSyncDirty(0b11)
	packets := scheduler.Flush()

	if len(packets) != 1 || len(downstream.packets) != 1 {
		t.Fatalf("packets=%+v downstream=%+v", packets, downstream.packets)
	}
	if packets[0].Topic != "alliance" || packets[0].Mask != 0b11 || packets[0].Body != "delta" {
		t.Fatalf("packet mismatch: %+v", packets[0])
	}
}

func TestSchedulerAllowsNonSceneTopicEnterAndDelta(t *testing.T) {
	oldScheduler := entity.GetEntitySyncScheduler()
	defer entity.SetEntitySyncScheduler(oldScheduler)

	downstream := &captureSink{}
	scheduler := NewScheduler(downstream)
	entity.SetEntitySyncScheduler(scheduler)
	ref := entity.NewPlayerSyncObserver(402)

	base := entity.NewEntityBase(105, entity.EntityCategory(1), false, entity.EntityKind(2))
	base.EnableSync(entity.EntitySyncCreateParam{
		Enabled: true,
		Topic:   "alliance",
		Packer: entity.EntitySyncPackFunc{
			Enter: func(observer entity.SyncObserverRef) (entity.SyncPacket, error) {
				return entity.SyncPacket{Body: "alliance-full"}, nil
			},
			Update: func(observer entity.SyncObserverRef, mask uint64) (entity.SyncPacket, error) {
				return entity.SyncPacket{Body: "alliance-delta", Mask: mask}, nil
			},
		},
	})

	enter, ok := base.Sync().AddObserverRef(ref)
	if !ok {
		t.Fatal("observer enter should produce packet")
	}
	scheduler.Enqueue(enter)
	enterPackets := scheduler.Flush()
	if len(enterPackets) != 1 {
		t.Fatalf("enter packets len = %d, packets=%+v", len(enterPackets), enterPackets)
	}
	if enterPackets[0].Type != entity.SyncPacketEnter || enterPackets[0].Topic != "alliance" || enterPackets[0].Body != "alliance-full" {
		t.Fatalf("enter packet mismatch: %+v", enterPackets[0])
	}

	base.MarkSyncDirty(0b100)
	packets := scheduler.Flush()

	if len(packets) != 1 {
		t.Fatalf("packets len = %d, packets=%+v", len(packets), packets)
	}
	if packets[0].Type != entity.SyncPacketUpdate || packets[0].Mask != 0b100 || packets[0].Body != "alliance-delta" {
		t.Fatalf("delta packet mismatch: %+v", packets[0])
	}
}

func TestSchedulerKeepsDirtyStateWhenMinIntervalDefersFlush(t *testing.T) {
	oldScheduler := entity.GetEntitySyncScheduler()
	defer entity.SetEntitySyncScheduler(oldScheduler)

	downstream := &captureSink{}
	scheduler := NewScheduler(downstream)
	entity.SetEntitySyncScheduler(scheduler)

	base := entity.NewEntityBase(106, entity.EntityCategory(1), false, entity.EntityKind(2))
	base.EnableSync(entity.EntitySyncCreateParam{
		Enabled:     true,
		Topic:       "interval",
		MinInterval: time.Hour,
	})
	if _, ok := base.Sync().AddObserverRef(entity.NewPlayerSyncObserver(403)); !ok {
		t.Fatal("observer should be added")
	}

	base.MarkSyncDirty(0b001)
	if packets := scheduler.Flush(); len(packets) != 1 {
		t.Fatalf("first flush packets=%+v", packets)
	}
	downstream.packets = nil

	base.MarkSyncDirty(0b010)
	if packets := scheduler.Flush(); len(packets) != 0 {
		t.Fatalf("second flush should be deferred, packets=%+v", packets)
	}
	if !base.Sync().PendingDirty() {
		t.Fatal("dirty state should remain pending after min interval defers flush")
	}
	scheduler.mu.Lock()
	_, queued := scheduler.dirty[base.Sync()]
	scheduler.mu.Unlock()
	if !queued {
		t.Fatal("scheduler should keep deferred dirty state queued")
	}
}

func TestSchedulerStopWithContextReturnsWhenFinalFlushIsBlocked(t *testing.T) {
	downstream := newBlockingSink()
	scheduler := NewScheduler(downstream)
	scheduler.Enqueue(entity.SyncPacket{
		EntityID: 1,
		Type:     entity.SyncPacketUpdate,
		Observer: entity.NewPlayerSyncObserver(10),
		Body:     "blocked",
	})
	scheduler.Start(context.Background(), time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := scheduler.StopWithContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopWithContext err = %v, want context deadline", err)
	}

	close(downstream.release)
}

type blockingSink struct {
	release chan struct{}
}

func newBlockingSink() *blockingSink {
	return &blockingSink{release: make(chan struct{})}
}

func (s *blockingSink) Enqueue(packet entity.SyncPacket) {
	s.EnqueueBatch([]entity.SyncPacket{packet})
}

func (s *blockingSink) EnqueueBatch([]entity.SyncPacket) {
	<-s.release
}
