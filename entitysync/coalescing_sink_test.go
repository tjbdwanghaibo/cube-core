package entitysync

import (
	"github.com/tjbdwanghaibo/cube-core/entity"
	"testing"
)

func TestCoalescingSinkMergesUpdatesPerObserverAndEntity(t *testing.T) {
	downstream := &captureSink{}
	sink := NewCoalescingSink(downstream)
	ref := entity.NewPlayerSyncObserver(100)

	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 200, Type: entity.SyncPacketUpdate, Version: 1, Mask: 0b001})
	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 200, Type: entity.SyncPacketUpdate, Version: 3, Mask: 0b100})
	packets := sink.Flush()

	if len(packets) != 1 || len(downstream.packets) != 1 {
		t.Fatalf("packets=%+v downstream=%+v", packets, downstream.packets)
	}
	if packets[0].Mask != 0b101 || packets[0].Version != 3 {
		t.Fatalf("merged packet mismatch: %+v", packets[0])
	}
}

func TestCoalescingSinkKeepsBodyDeltaUpdatesSeparate(t *testing.T) {
	sink := NewCoalescingSink(nil)
	ref := entity.NewPlayerSyncObserver(100)

	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 200, Type: entity.SyncPacketUpdate, Version: 1, Mask: 0b001, Body: "field-a"})
	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 200, Type: entity.SyncPacketUpdate, Version: 2, Mask: 0b010, Body: "field-b"})
	packets := sink.Flush()

	if len(packets) != 2 {
		t.Fatalf("body delta updates should stay separate, got %+v", packets)
	}
	if packets[0].Mask != 0b001 || packets[0].Body != "field-a" || packets[1].Mask != 0b010 || packets[1].Body != "field-b" {
		t.Fatalf("body delta order/content mismatch: %+v", packets)
	}
}

func TestCoalescingSinkEnterCancelsQueuedUpdateAndLeave(t *testing.T) {
	downstream := &captureSink{}
	sink := NewCoalescingSink(downstream)
	ref := entity.NewPlayerSyncObserver(101)

	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 201, Type: entity.SyncPacketEnter})
	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 201, Type: entity.SyncPacketUpdate, Mask: 1})
	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 201, Type: entity.SyncPacketLeave})
	packets := sink.Flush()

	if len(packets) != 0 || len(downstream.packets) != 0 {
		t.Fatalf("enter then leave should cancel, got packets=%+v downstream=%+v", packets, downstream.packets)
	}
}

func TestCoalescingSinkMergesFullUpdateMetadata(t *testing.T) {
	sink := NewCoalescingSink(nil)
	ref := entity.NewPlayerSyncObserver(102)

	sink.Enqueue(entity.SyncPacket{Observer: ref, EntityID: 202, Type: entity.SyncPacketUpdate, Version: 1, Mask: 0b001, Body: "delta"})
	sink.Enqueue(entity.SyncPacket{
		Observer:      ref,
		EntityID:      202,
		Type:          entity.SyncPacketUpdate,
		Version:       2,
		BaseVersion:   1,
		Mask:          entity.SyncMaskFull,
		Full:          true,
		SchemaVersion: 4,
		Reason:        entity.SyncFullReasonResync,
		Body:          "full",
	})
	packets := sink.Flush()

	if len(packets) != 1 {
		t.Fatalf("packets=%+v", packets)
	}
	got := packets[0]
	if !got.Full || got.Mask != entity.SyncMaskFull || got.Version != 2 || got.BaseVersion != 1 ||
		got.SchemaVersion != 4 || got.Reason != entity.SyncFullReasonResync || got.Body != "full" {
		t.Fatalf("full packet metadata mismatch: %+v", got)
	}
}

func TestCoalescingSinkKeepsDeltaAfterFullSeparate(t *testing.T) {
	sink := NewCoalescingSink(nil)
	ref := entity.NewPlayerSyncObserver(103)

	sink.Enqueue(entity.SyncPacket{
		Observer: ref,
		EntityID: 203,
		Type:     entity.SyncPacketUpdate,
		Version:  1,
		Mask:     entity.SyncMaskFull,
		Full:     true,
		Reason:   entity.SyncFullReasonResync,
		Body:     "full",
	})
	sink.Enqueue(entity.SyncPacket{
		Observer: ref,
		EntityID: 203,
		Type:     entity.SyncPacketUpdate,
		Version:  2,
		Mask:     0b010,
		Body:     "delta",
	})
	packets := sink.Flush()

	if len(packets) != 2 {
		t.Fatalf("packets=%+v", packets)
	}
	if !packets[0].Full || packets[0].Mask != entity.SyncMaskFull || packets[0].Body != "full" {
		t.Fatalf("full packet mismatch: %+v", packets[0])
	}
	if packets[1].Full || packets[1].Mask != 0b010 || packets[1].Body != "delta" {
		t.Fatalf("delta packet mismatch: %+v", packets[1])
	}
}

func TestCoalescingSinkFlushOrderIsStable(t *testing.T) {
	sink := NewCoalescingSink(nil)
	refB := entity.NewPlayerSyncObserver(200)
	refA := entity.NewPlayerSyncObserver(100)

	sink.Enqueue(entity.SyncPacket{Observer: refB, EntityID: 30, Type: entity.SyncPacketUpdate, Version: 3})
	sink.Enqueue(entity.SyncPacket{Observer: refA, EntityID: 20, Type: entity.SyncPacketUpdate, Version: 2})
	sink.Enqueue(entity.SyncPacket{Observer: refA, EntityID: 10, Type: entity.SyncPacketUpdate, Version: 1})
	packets := sink.Flush()

	if len(packets) != 3 {
		t.Fatalf("packets=%+v", packets)
	}
	got := []int64{packets[0].Observer.PlayerID(), packets[0].EntityID, packets[1].Observer.PlayerID(), packets[1].EntityID, packets[2].Observer.PlayerID(), packets[2].EntityID}
	want := []int64{100, 10, 100, 20, 200, 30}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%v, want %v", got, want)
		}
	}
}

func TestCoalescingSinkUsesObserverShardsWithoutChangingFlushSemantics(t *testing.T) {
	sink := NewCoalescingSink(nil)
	if got := sink.shardCount(); got < 2 {
		t.Fatalf("shard count = %d, want observer sharding", got)
	}
	refA, refB := differentShardObserverRefs(t, sink)
	if compareSyncObserverRef(refB, refA) < 0 {
		refA, refB = refB, refA
	}

	sink.Enqueue(entity.SyncPacket{Observer: refB, EntityID: 30, Type: entity.SyncPacketUpdate, Version: 3, Mask: 0b001})
	sink.Enqueue(entity.SyncPacket{Observer: refA, EntityID: 20, Type: entity.SyncPacketUpdate, Version: 2, Mask: 0b001})
	sink.Enqueue(entity.SyncPacket{Observer: refA, EntityID: 10, Type: entity.SyncPacketUpdate, Version: 1, Mask: 0b001})
	sink.Enqueue(entity.SyncPacket{Observer: refA, EntityID: 20, Type: entity.SyncPacketUpdate, Version: 4, Mask: 0b100})
	packets := sink.Flush()

	if len(packets) != 3 {
		t.Fatalf("packets=%+v", packets)
	}
	got := []int64{packets[0].Observer.PlayerID(), packets[0].EntityID, packets[1].Observer.PlayerID(), packets[1].EntityID, packets[2].Observer.PlayerID(), packets[2].EntityID}
	want := []int64{refA.PlayerID(), 10, refA.PlayerID(), 20, refB.PlayerID(), 30}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%v, want %v", got, want)
		}
	}
	if packets[1].Mask != 0b101 || packets[1].Version != 4 {
		t.Fatalf("merged packet mismatch: %+v", packets[1])
	}
}

func differentShardObserverRefs(t *testing.T, sink *CoalescingSink) (entity.SyncObserverRef, entity.SyncObserverRef) {
	t.Helper()
	first := entity.NewPlayerSyncObserver(1)
	firstShard := sink.shardIndexForObserver(first)
	for id := int64(2); id < 10000; id++ {
		ref := entity.NewPlayerSyncObserver(id)
		if sink.shardIndexForObserver(ref) != firstShard {
			return first, ref
		}
	}
	t.Fatalf("could not find player observers in different shards")
	return entity.SyncObserverRef{}, entity.SyncObserverRef{}
}

type captureSink struct {
	packets []entity.SyncPacket
}

func (s *captureSink) Enqueue(packet entity.SyncPacket) {
	s.packets = append(s.packets, packet)
}

func (s *captureSink) EnqueueBatch(packets []entity.SyncPacket) {
	s.packets = append(s.packets, packets...)
}
