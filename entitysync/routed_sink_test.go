package entitysync

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/entity"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"errors"
	"testing"
)

func TestRoutedSinkRoutesRemoteBatchToTargetLocalSink(t *testing.T) {
	bus := newFakeBus()
	resolver := fakeResolver{sid: 2}
	source := NewRoutedSink(bus, resolver, 1, nil, "sync")
	targetLocal := &fakeLocalSink{}
	target := NewRoutedSink(bus, resolver, 2, targetLocal, "sync")
	if err := target.Start(); err != nil {
		t.Fatal(err)
	}
	defer target.Stop()

	source.Enqueue(entity.SyncPacket{
		Observer: entity.NewPlayerSyncObserver(100),
		EntityID: 200,
		Type:     entity.SyncPacketUpdate,
		Version:  9,
	})

	if len(targetLocal.packets) != 1 || targetLocal.packets[0].EntityID != 200 {
		t.Fatalf("target packets = %+v", targetLocal.packets)
	}
}

func TestRoutedSinkRetriesAndRecordsFailedPublish(t *testing.T) {
	bus := newFakeBus()
	bus.failPublish = 2
	store := &fakeFailedBatchStore{}
	resolver := fakeResolver{sid: 2}
	source := NewRoutedSink(bus, resolver, 1, nil, "sync")
	source.SetPublishRetries(1)
	source.SetFailedBatchStore(store)

	ok := source.RouteObserverBatch(context.Background(), entity.NewPlayerSyncObserver(100), []entity.SyncPacket{
		{Observer: entity.NewPlayerSyncObserver(100), EntityID: 200, Type: entity.SyncPacketUpdate, Version: 9},
	})
	if ok {
		t.Fatal("route should fail after retries")
	}
	stats := source.Stats()
	if stats.PublishFailures != 1 || stats.FailedBatches != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if len(source.FailedBatches()) != 1 {
		t.Fatalf("failed batches len = %d", len(source.FailedBatches()))
	}
	if len(store.batches) != 1 || store.batches[0].Observer.PlayerID() != 100 {
		t.Fatalf("stored failed batches = %+v", store.batches)
	}
}

func TestRoutedSinkFailedBatchesAreBounded(t *testing.T) {
	bus := newFakeBus()
	bus.failPublish = 4
	resolver := fakeResolver{sid: 2}
	source := NewRoutedSink(bus, resolver, 1, nil, "sync")
	source.SetMaxFailedBatches(2)

	for i := int64(1); i <= 4; i++ {
		source.RouteObserverBatch(context.Background(), entity.NewPlayerSyncObserver(100), []entity.SyncPacket{
			{Observer: entity.NewPlayerSyncObserver(100), EntityID: i, Type: entity.SyncPacketUpdate, Version: 9},
		})
	}
	failed := source.FailedBatches()
	if len(failed) != 2 {
		t.Fatalf("failed batches len = %d, want 2", len(failed))
	}
	if failed[0].Packets[0].EntityID != 3 || failed[1].Packets[0].EntityID != 4 {
		t.Fatalf("failed batches = %+v, want newest two", failed)
	}
}

type fakeResolver struct {
	sid int32
}

func (r fakeResolver) ResolveSyncRoute(_ context.Context, observer entity.SyncObserverRef) (ObserverRoute, bool, error) {
	return ObserverRoute{Observer: observer, Sid: r.sid}, true, nil
}

type fakeLocalSink struct {
	packets []entity.SyncPacket
}

func (s *fakeLocalSink) EnqueueLocalBatch(packets []entity.SyncPacket) int {
	s.packets = append(s.packets, packets...)
	return len(packets)
}

type fakeBus struct {
	handlers    map[string][]fsync.Handler
	failPublish int
}

func newFakeBus() *fakeBus {
	return &fakeBus{handlers: make(map[string][]fsync.Handler)}
}

func (b *fakeBus) Publish(msg *fsync.SyncMsg) error {
	if b.failPublish > 0 {
		b.failPublish--
		return errors.New("publish failed")
	}
	for _, h := range b.handlers[msg.Topic] {
		if err := h(msg); err != nil {
			return err
		}
	}
	return nil
}

func (b *fakeBus) Subscribe(topic string, handler fsync.Handler) (func(), error) {
	b.handlers[topic] = append(b.handlers[topic], handler)
	return func() {}, nil
}

var _ fsync.ISyncBus = (*fakeBus)(nil)

type fakeFailedBatchStore struct {
	batches []SyncBatch
}

func (s *fakeFailedBatchStore) SaveFailedSyncBatch(_ context.Context, batch SyncBatch) error {
	s.batches = append(s.batches, batch)
	return nil
}
