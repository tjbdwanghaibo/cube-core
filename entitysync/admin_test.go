package entitysync

import (
	"context"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/admin"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestFailedBatchAdminCommandsListReplayAndPurge(t *testing.T) {
	store := &failedBatchAdminStore{
		batches: []SyncBatch{{
			Observer:  entity.NewPlayerSyncObserver(1001),
			SourceSid: 2001,
			Packets:   []entity.SyncPacket{{Observer: entity.NewPlayerSyncObserver(1001), EntityID: 3001, Type: entity.SyncPacketUpdate}},
		}},
	}
	sink := &failedBatchAdminSink{}
	reg := admin.NewRegistry()
	if err := RegisterFailedBatchAdminCommands(reg, store, sink); err != nil {
		t.Fatalf("RegisterFailedBatchAdminCommands: %v", err)
	}

	list, err := reg.Execute(context.Background(), admin.Command{
		Name:    AdminCommandEntitySyncFailedList,
		Payload: admin.MustPayload(FailedBatchAdminCommand{ObserverKind: uint8(entity.SyncObserverPlayer), ObserverID: 1001}),
	})
	if err != nil {
		t.Fatalf("list command: %v", err)
	}
	if list.Data["count"].(int) != 1 {
		t.Fatalf("list result = %+v", list)
	}

	replayed, err := reg.Execute(context.Background(), admin.Command{
		Name:    AdminCommandEntitySyncFailedReplay,
		Payload: admin.MustPayload(FailedBatchAdminCommand{ObserverKind: uint8(entity.SyncObserverPlayer), ObserverID: 1001}),
	})
	if err != nil {
		t.Fatalf("replay command: %v", err)
	}
	if replayed.Data["count"].(int) != 1 || len(sink.batches) != 1 {
		t.Fatalf("replay result=%+v sink=%+v", replayed, sink.batches)
	}

	purged, err := reg.Execute(context.Background(), admin.Command{
		Name:    AdminCommandEntitySyncFailedPurge,
		Payload: admin.MustPayload(FailedBatchAdminCommand{ObserverKind: uint8(entity.SyncObserverPlayer), ObserverID: 1001}),
	})
	if err != nil {
		t.Fatalf("purge command: %v", err)
	}
	if purged.Data["count"].(int64) != 1 || store.purged != 1 {
		t.Fatalf("purge result=%+v purged=%d", purged, store.purged)
	}
}

type failedBatchAdminStore struct {
	batches []SyncBatch
	purged  int64
}

func (s *failedBatchAdminStore) SaveFailedSyncBatch(context.Context, SyncBatch) error { return nil }

func (s *failedBatchAdminStore) ListFailedSyncBatches(context.Context, entity.SyncObserverRef, int64, int64) ([]SyncBatch, error) {
	return append([]SyncBatch(nil), s.batches...), nil
}

func (s *failedBatchAdminStore) PurgeFailedSyncBatches(context.Context, entity.SyncObserverRef, int64, int64) (int64, error) {
	n := int64(len(s.batches))
	s.batches = nil
	s.purged += n
	return n, nil
}

type failedBatchAdminSink struct {
	batches [][]entity.SyncPacket
}

func (s *failedBatchAdminSink) Enqueue(entity.SyncPacket) {}

func (s *failedBatchAdminSink) EnqueueBatch(packets []entity.SyncPacket) {
	s.batches = append(s.batches, append([]entity.SyncPacket(nil), packets...))
}
