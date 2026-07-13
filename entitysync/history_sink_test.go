package entitysync

import (
	"context"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestHistorySinkAckAndResyncReplaysMissingPackets(t *testing.T) {
	ref := entity.NewPlayerSyncObserver(501)
	downstream := &captureSink{}
	sink := NewHistorySink(downstream, HistoryOptions{MaxPacketsPerStream: 8})

	sink.EnqueueBatch([]entity.SyncPacket{
		{Observer: ref, Topic: "world.troop", EntityID: 7001, Type: entity.SyncPacketUpdate, Version: 1, BaseVersion: 0, Mask: 0b001, Body: "v1"},
		{Observer: ref, Topic: "world.troop", EntityID: 7001, Type: entity.SyncPacketUpdate, Version: 2, BaseVersion: 1, Mask: 0b010, Body: "v2"},
		{Observer: ref, Topic: "world.troop", EntityID: 7001, Type: entity.SyncPacketUpdate, Version: 3, BaseVersion: 2, Mask: 0b100, Body: "v3"},
	})

	ack := sink.Ack(SyncAckRequest{Observer: ref, Topic: "world.troop", EntityID: 7001, ClientSeq: 1})
	if ack.Status != SyncAckOK || ack.AckSeq != 1 || ack.ServerSeq != 3 || ack.NeedResync {
		t.Fatalf("ack mismatch: %+v", ack)
	}

	resync := sink.Resync(SyncResyncRequest{Observer: ref, Topic: "world.troop", EntityID: 7001, ClientSeq: 1})
	if resync.Status != SyncResyncReplayed || resync.ServerSeq != 3 || resync.NeedFull {
		t.Fatalf("resync status mismatch: %+v", resync)
	}
	if len(resync.Packets) != 2 || resync.Packets[0].Body != "v2" || resync.Packets[1].Body != "v3" {
		t.Fatalf("resync packets mismatch: %+v", resync.Packets)
	}
	if len(downstream.packets) != 5 {
		t.Fatalf("downstream should receive original and replayed packets, got %d", len(downstream.packets))
	}
}

func TestHistorySinkResyncUsesTargetedFullWhenHistoryWindowMisses(t *testing.T) {
	oldMgr := entity.Mgr
	entity.Mgr = entity.NewEntityManager()
	defer func() { entity.Mgr = oldMgr }()

	ref := entity.NewPlayerSyncObserver(502)
	entityID := int64(7002)
	base := entity.NewEntityBase(entityID, entity.EntityCategory(1), false, entity.EntityKind(2))
	base.EnableSync(entity.EntitySyncCreateParam{
		Enabled: true,
		Topic:   "alliance",
		Packer: entity.EntitySyncPackFunc{
			Enter: func(observer entity.SyncObserverRef) (entity.SyncPacket, error) {
				return entity.SyncPacket{Body: "snapshot"}, nil
			},
		},
	})
	if _, ok := base.Sync().AddObserverRef(ref); !ok {
		t.Fatal("observer should be added")
	}
	entity.Mgr.Add(testSyncEntity{EntityBase: base})

	downstream := &captureSink{}
	sink := NewHistorySink(downstream, HistoryOptions{MaxPacketsPerStream: 1})
	sink.EnqueueBatch([]entity.SyncPacket{
		{Observer: ref, Topic: "alliance", EntityID: entityID, Type: entity.SyncPacketUpdate, Version: 2, BaseVersion: 1, Mask: 0b010},
		{Observer: ref, Topic: "alliance", EntityID: entityID, Type: entity.SyncPacketUpdate, Version: 3, BaseVersion: 2, Mask: 0b100},
	})

	resync := sink.Resync(SyncResyncRequest{Observer: ref, Topic: "alliance", EntityID: entityID, ClientSeq: 0})
	if resync.Status != SyncResyncFull || !resync.Snapshot || resync.NeedFull {
		t.Fatalf("resync full mismatch: %+v", resync)
	}
	if len(resync.Packets) != 1 {
		t.Fatalf("full packets len = %d", len(resync.Packets))
	}
	full := resync.Packets[0]
	if !full.Full || full.Mask != entity.SyncMaskFull || full.Reason != entity.SyncFullReasonResync ||
		full.Body != "snapshot" {
		t.Fatalf("full packet mismatch: %+v", full)
	}
}

func TestHistorySinkPrunesExpiredAndRemovedObserverStreams(t *testing.T) {
	ref := entity.NewPlayerSyncObserver(503)
	sink := NewHistorySink(nil, HistoryOptions{MaxPacketsPerStream: 8, StreamTTL: time.Nanosecond})
	sink.Enqueue(entity.SyncPacket{Observer: ref, Topic: "world", EntityID: 1, Type: entity.SyncPacketUpdate, Version: 1})
	time.Sleep(time.Millisecond)
	removed := sink.PruneExpired()
	if removed != 1 {
		t.Fatalf("removed expired = %d, want 1", removed)
	}
	ack := sink.Ack(SyncAckRequest{Observer: ref, Topic: "world", EntityID: 1, ClientSeq: 1})
	if ack.Status != SyncAckUnknown {
		t.Fatalf("ack after prune = %+v", ack)
	}

	sink.Enqueue(entity.SyncPacket{Observer: ref, Topic: "world", EntityID: 2, Type: entity.SyncPacketUpdate, Version: 1})
	if removed := sink.RemoveObserver(ref); removed != 1 {
		t.Fatalf("removed observer streams = %d, want 1", removed)
	}
}

func TestHistorySinkUsesConfiguredFullSnapshotProvider(t *testing.T) {
	ref := entity.NewPlayerSyncObserver(504)
	sink := NewHistorySink(nil, HistoryOptions{
		MaxPacketsPerStream: 1,
		FullSnapshotProvider: SyncFullSnapshotProviderFunc(func(req SyncResyncRequest) (entity.SyncPacket, bool) {
			return entity.SyncPacket{
				Observer: ref,
				Topic:    req.Topic,
				EntityID: req.EntityID,
				Type:     entity.SyncPacketUpdate,
				Version:  99,
				Full:     true,
				Mask:     entity.SyncMaskFull,
				Reason:   entity.SyncFullReasonResync,
				Body:     "remote-full",
			}, true
		}),
	})
	sink.Enqueue(entity.SyncPacket{Observer: ref, Topic: "remote", EntityID: 42, Type: entity.SyncPacketUpdate, Version: 2, BaseVersion: 1})
	sink.Enqueue(entity.SyncPacket{Observer: ref, Topic: "remote", EntityID: 42, Type: entity.SyncPacketUpdate, Version: 3, BaseVersion: 2})

	resync := sink.Resync(SyncResyncRequest{Observer: ref, Topic: "remote", EntityID: 42, ClientSeq: 0})
	if resync.Status != SyncResyncFull || len(resync.Packets) != 1 || resync.Packets[0].Body != "remote-full" {
		t.Fatalf("provider resync = %+v", resync)
	}
}

func TestHistorySinkStatsReportsStreamsAndPackets(t *testing.T) {
	sink := NewHistorySink(nil, HistoryOptions{MaxPacketsPerStream: 8})
	sink.Enqueue(entity.SyncPacket{
		Observer: entity.NewPlayerSyncObserver(1001),
		Topic:    "player",
		EntityID: 2001,
		Version:  1,
		Type:     entity.SyncPacketUpdate,
	})

	stats := sink.Stats()
	if stats.Streams != 1 || stats.Packets != 1 {
		t.Fatalf("Stats = %+v, want 1 stream and 1 packet", stats)
	}
}

func TestRuntimeFullSnapshotProviderUsesRemoteWrapperEntity(t *testing.T) {
	ref := entity.NewPlayerSyncObserver(1002)
	entityID := int64(9002)
	remote := newTestRemoteSyncEntity(entityID)
	remote.Base().EnableSync(entity.EntitySyncCreateParam{
		Enabled: true,
		Topic:   "remote",
		Packer: entity.EntitySyncPackFunc{
			Enter: func(observer entity.SyncObserverRef) (entity.SyncPacket, error) {
				return entity.SyncPacket{Body: "remote-snapshot"}, nil
			},
		},
	})
	if _, ok := remote.Base().Sync().AddObserverRef(ref); !ok {
		t.Fatal("observer should be added")
	}
	provider := NewRuntimeFullSnapshotProvider(fakeRemoteWrapperManager{
		wrapper: fakeRemoteWrapper{entity: remote},
	})

	packet, ok := provider.PackFullSync(SyncResyncRequest{Observer: ref, Topic: "remote", EntityID: entityID})
	if !ok || !packet.Full || packet.Body != "remote-snapshot" {
		t.Fatalf("remote full packet = %+v ok=%v", packet, ok)
	}
}

type testSyncEntity struct {
	*entity.EntityBase
}

func (e testSyncEntity) Base() *entity.EntityBase { return e.EntityBase }

func (e testSyncEntity) OnInitFinish(*entity.EntityCreateParam) error { return nil }

func (e testSyncEntity) OnDestroy(entity.EntityDestroyReason) {}

type testRemoteSyncEntity struct {
	entity.RemoteEntityBase
}

func newTestRemoteSyncEntity(id int64) *testRemoteSyncEntity {
	return &testRemoteSyncEntity{RemoteEntityBase: entity.RemoteEntityBase{
		EntityBase: *entity.NewEntityBase(id, entity.EntityCategory(1), false, entity.EntityKind(2)),
	}}
}

func (e *testRemoteSyncEntity) Base() *entity.EntityBase { return &e.RemoteEntityBase.EntityBase }

func (e *testRemoteSyncEntity) OnInitFinish(*entity.EntityCreateParam) error { return nil }

func (e *testRemoteSyncEntity) OnDestroy(entity.EntityDestroyReason) {}

func (e *testRemoteSyncEntity) OnDataChange([]byte, int64) {}

type fakeRemoteWrapperManager struct {
	wrapper entity.IRemoteEntityWrapper
}

func (m fakeRemoteWrapperManager) GetOrCreate(int64, entity.EntityCategory, entity.EntityKind) entity.IRemoteEntityWrapper {
	return m.wrapper
}

func (m fakeRemoteWrapperManager) Get(int64) (entity.IRemoteEntityWrapper, bool) {
	return m.wrapper, m.wrapper != nil
}

func (m fakeRemoteWrapperManager) Remove(int64) {}

func (m fakeRemoteWrapperManager) PrepareRemoteEntities([]int64) (func(), error) {
	return func() {}, nil
}

type fakeRemoteWrapper struct {
	entity entity.IThreadSafeRemoteEntity
}

func (w fakeRemoteWrapper) TryCastEntity() (func(), error) { return func() {}, nil }
func (w fakeRemoteWrapper) TryReadOnlyEntity(entity.RemoteReadOption) (entity.IThreadSafeRemoteEntity, func(), error) {
	return nil, func() {}, nil
}
func (w fakeRemoteWrapper) TryReadOnlySnapshot(entity.RemoteSnapshotRequest) (entity.RemoteSnapshot, error) {
	return entity.RemoteSnapshot{}, nil
}
func (w fakeRemoteWrapper) TryCachedSnapshot(entity.RemoteSnapshotRequest) (entity.RemoteSnapshot, error) {
	return entity.RemoteSnapshot{}, nil
}

func (w fakeRemoteWrapper) MarkRemote(context.Context) (func(), error) { return func() {}, nil }

func (w fakeRemoteWrapper) UnmarkRemote(context.Context) error { return nil }

func (w fakeRemoteWrapper) IsMarked() bool { return false }

func (w fakeRemoteWrapper) SetMarked(bool) {}

func (w fakeRemoteWrapper) EvictEntity() {}

func (w fakeRemoteWrapper) TryUpdateEntity(int64, []byte) error { return nil }

func (w fakeRemoteWrapper) TryDelEntity() error { return nil }

func (w fakeRemoteWrapper) Entity() entity.IThreadSafeRemoteEntity { return w.entity }
