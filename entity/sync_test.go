package entity

import (
	"sync"
	"testing"
	"time"
)

func TestEntityBaseSyncObserverAndDirtyFlush(t *testing.T) {
	id := mustBuildTestEntityID(t, 100, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	base.EnableSync(EntitySyncCreateParam{
		Enabled: true,
		Topic:   "unit",
		Packer: EntitySyncPackFunc{
			Enter: func(observer SyncObserverRef) (SyncPacket, error) {
				return SyncPacket{Body: "enter"}, nil
			},
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				return SyncPacket{Body: "update", Mask: mask}, nil
			},
			Leave: func(observer SyncObserverRef) (SyncPacket, error) {
				return SyncPacket{Body: "leave"}, nil
			},
		},
	})

	observer := NewPlayerSyncObserver(200)
	packet, ok := base.Sync().AddObserverRef(observer)
	if !ok {
		t.Fatal("AddObserverRef should produce enter packet")
	}
	if packet.Type != SyncPacketEnter || packet.Topic != "unit" || packet.EntityID != id || packet.ObserverID != 200 {
		t.Fatalf("enter packet not filled: %+v", packet)
	}

	base.MarkSyncDirty(0b101)
	packets := base.FlushSync()
	if len(packets) != 1 {
		t.Fatalf("FlushSync packets len = %d", len(packets))
	}
	if packets[0].Type != SyncPacketUpdate || packets[0].Mask != 0b101 || packets[0].Version != 1 {
		t.Fatalf("update packet not filled: %+v", packets[0])
	}
	if base.Sync().DirtyMask() != 0 {
		t.Fatalf("dirty mask should be clean: %d", base.Sync().DirtyMask())
	}

	packet, ok = base.Sync().RemoveObserverRef(observer)
	if !ok {
		t.Fatal("RemoveObserverRef should produce leave packet")
	}
	if packet.Type != SyncPacketLeave || packet.ObserverID != 200 || packet.Body != "leave" {
		t.Fatalf("leave packet not filled: %+v", packet)
	}
}

func TestEntitySyncPackerRunsUnderEntityLock(t *testing.T) {
	id := mustBuildTestEntityID(t, 109, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	mu := &observedSyncMutex{id: id}
	base.mu = mu
	base.EnableSync(EntitySyncCreateParam{
		Enabled: true,
		Topic:   "locked",
		Packer: EntitySyncPackFunc{
			Enter: func(observer SyncObserverRef) (SyncPacket, error) {
				if !mu.Held() {
					t.Fatal("enter packer should run while entity lock is held")
				}
				return SyncPacket{Body: "enter"}, nil
			},
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				if !mu.Held() {
					t.Fatal("update packer should run while entity lock is held")
				}
				return SyncPacket{Body: "update"}, nil
			},
			Leave: func(observer SyncObserverRef) (SyncPacket, error) {
				if !mu.Held() {
					t.Fatal("leave packer should run while entity lock is held")
				}
				return SyncPacket{Body: "leave"}, nil
			},
		},
	})

	observer := NewPlayerSyncObserver(209)
	if _, ok := base.Sync().AddObserverRef(observer); !ok {
		t.Fatal("AddObserverRef should produce enter packet")
	}
	base.MarkSyncDirty(1)
	if packets := base.FlushSync(); len(packets) != 1 {
		t.Fatalf("FlushSync packets len = %d", len(packets))
	}
	if _, ok := base.Sync().PackFullForObserver(observer, SyncFullReasonResync); !ok {
		t.Fatal("PackFullForObserver should produce full packet")
	}
	if _, ok := base.Sync().RemoveObserverRef(observer); !ok {
		t.Fatal("RemoveObserverRef should produce leave packet")
	}
}

type observedSyncMutex struct {
	mu       sync.Mutex
	id       int64
	held     bool
	depth    int
	maxDepth int
}

func (m *observedSyncMutex) TryLock() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.held {
		return false
	}
	m.held = true
	m.depth++
	if m.depth > m.maxDepth {
		m.maxDepth = m.depth
	}
	return true
}

func (m *observedSyncMutex) Lock() {
	m.mu.Lock()
	m.held = true
	m.depth++
	if m.depth > m.maxDepth {
		m.maxDepth = m.depth
	}
	m.mu.Unlock()
}

func (m *observedSyncMutex) LockWithTimeout(time.Duration) bool {
	m.Lock()
	return true
}

func (m *observedSyncMutex) Unlock() {
	m.mu.Lock()
	if m.depth > 0 {
		m.depth--
	}
	m.held = m.depth > 0
	m.mu.Unlock()
}

func (m *observedSyncMutex) LockId() int64 {
	return m.id
}

func (m *observedSyncMutex) Held() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held
}

func (m *observedSyncMutex) MaxDepth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxDepth
}

func TestEntitySyncFlushToSink(t *testing.T) {
	id := mustBuildTestEntityID(t, 101, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	sink := &testSyncSink{}
	base.EnableSync(EntitySyncCreateParam{Enabled: true, Topic: "sink"})
	if _, ok := base.Sync().AddObserverRef(NewPlayerSyncObserver(201)); !ok {
		t.Fatal("AddObserverRef should succeed")
	}

	base.MarkSyncDirty(1)
	packets := base.FlushSyncTo(sink)
	if len(packets) != 1 || len(sink.packets) != 1 {
		t.Fatalf("FlushSyncTo packets=%d sink=%d", len(packets), len(sink.packets))
	}
	if sink.packets[0].Topic != "sink" || sink.packets[0].EntityID != id {
		t.Fatalf("sink packet not filled: %+v", sink.packets[0])
	}
}

func TestEntitySyncObserverRef(t *testing.T) {
	id := mustBuildTestEntityID(t, 102, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	base.EnableSync(EntitySyncCreateParam{Enabled: true, Topic: "server"})

	ref := NewServerSyncObserver(7)
	packet, ok := base.Sync().AddObserverRef(ref)
	if !ok {
		t.Fatal("AddObserverRef should succeed")
	}
	if packet.Observer != ref || packet.ObserverID != 0 || packet.Type != SyncPacketEnter {
		t.Fatalf("server observer enter packet mismatch: %+v", packet)
	}
	if !base.Sync().HasObserverRef(ref) {
		t.Fatal("server observer should be tracked")
	}
	if refs := base.Sync().ObserverRefs(); len(refs) != 1 || refs[0] != ref {
		t.Fatalf("server observer refs mismatch: %+v", refs)
	}

	base.MarkSyncDirty(3)
	packets := base.FlushSync()
	if len(packets) != 1 || packets[0].Observer != ref || packets[0].Mask != 3 {
		t.Fatalf("server observer update mismatch: %+v", packets)
	}
}

func TestEntitySyncFullDirtyFlushUsesSnapshotPacker(t *testing.T) {
	id := mustBuildTestEntityID(t, 103, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	base.EnableSync(EntitySyncCreateParam{
		Enabled:       true,
		Topic:         "full",
		SchemaVersion: 3,
		Packer: EntitySyncPackFunc{
			Enter: func(observer SyncObserverRef) (SyncPacket, error) {
				return SyncPacket{Body: "full"}, nil
			},
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				return SyncPacket{Body: "delta", Mask: mask}, nil
			},
		},
	})
	if _, ok := base.Sync().AddObserverRef(NewPlayerSyncObserver(301)); !ok {
		t.Fatal("AddObserverRef should succeed")
	}

	base.MarkSyncDirty(0b10)
	base.MarkSyncFullDirty(SyncFullReasonResync)
	packets := base.FlushSync()

	if len(packets) != 1 {
		t.Fatalf("FlushSync packets len = %d", len(packets))
	}
	got := packets[0]
	if got.Type != SyncPacketUpdate || !got.Full || got.Mask != SyncMaskFull || got.Reason != SyncFullReasonResync {
		t.Fatalf("full update packet mismatch: %+v", got)
	}
	if got.Body != "full" || got.Version != 1 || got.BaseVersion != 0 || got.SchemaVersion != 3 {
		t.Fatalf("full update data/version mismatch: %+v", got)
	}
}

func TestEntitySyncFullSyncOnDirtyFlushesFullPacket(t *testing.T) {
	id := mustBuildTestEntityID(t, 113, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	updatePackCount := 0
	base.EnableSync(EntitySyncCreateParam{
		Enabled:         true,
		Topic:           "full-on-dirty",
		SchemaVersion:   5,
		FullSyncOnDirty: true,
		Packer: EntitySyncPackFunc{
			Enter: func(observer SyncObserverRef) (SyncPacket, error) {
				return SyncPacket{Body: "full-view"}, nil
			},
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				updatePackCount++
				return SyncPacket{Body: "delta-view", Mask: mask}, nil
			},
		},
	})
	ref := NewPlayerSyncObserver(313)
	if _, ok := base.Sync().AddObserverRef(ref); !ok {
		t.Fatal("AddObserverRef should succeed")
	}

	base.MarkSyncDirty(0b101)
	packets := base.FlushSync()

	if len(packets) != 1 {
		t.Fatalf("FlushSync packets len = %d", len(packets))
	}
	got := packets[0]
	if updatePackCount != 0 {
		t.Fatalf("dirty full sync should not call update packer, got %d", updatePackCount)
	}
	if got.Type != SyncPacketUpdate || !got.Full || got.Mask != SyncMaskFull ||
		got.Reason != SyncFullReasonDirty || got.Body != "full-view" {
		t.Fatalf("full-on-dirty packet mismatch: %+v", got)
	}
	if got.Version != 1 || got.BaseVersion != 0 || got.SchemaVersion != 5 || got.Observer != ref {
		t.Fatalf("full-on-dirty metadata mismatch: %+v", got)
	}
}

func TestEntitySyncPackFullForObserverUsesSnapshotWithoutBroadcastDirty(t *testing.T) {
	id := mustBuildTestEntityID(t, 104, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	base.EnableSync(EntitySyncCreateParam{
		Enabled: true,
		Topic:   "full-target",
		Packer: EntitySyncPackFunc{
			Enter: func(observer SyncObserverRef) (SyncPacket, error) {
				return SyncPacket{Body: "target-full"}, nil
			},
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				return SyncPacket{Body: "delta", Mask: mask}, nil
			},
		},
	})
	ref := NewPlayerSyncObserver(302)
	if _, ok := base.Sync().AddObserverRef(ref); !ok {
		t.Fatal("AddObserverRef should succeed")
	}
	base.MarkSyncDirty(0b100)

	packet, ok := base.Sync().PackFullForObserver(ref, SyncFullReasonResync)
	if !ok {
		t.Fatal("PackFullForObserver should produce a packet for an existing observer")
	}
	if packet.Type != SyncPacketUpdate || !packet.Full || packet.Mask != SyncMaskFull ||
		packet.Reason != SyncFullReasonResync || packet.Body != "target-full" {
		t.Fatalf("targeted full packet mismatch: %+v", packet)
	}
	if packet.Version != 1 || packet.BaseVersion != 0 {
		t.Fatalf("targeted full version mismatch: %+v", packet)
	}
	packets := base.FlushSync()
	if len(packets) != 1 || packets[0].Full || packets[0].Mask != 0b100 || packets[0].Body != "delta" {
		t.Fatalf("pending delta should remain for normal flush, got %+v", packets)
	}
}

func TestEntitySyncObserverAgnosticPackCacheReusesCleanEnterPayloadAndInvalidatesOnDirty(t *testing.T) {
	id := mustBuildTestEntityID(t, 105, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	packCount := 0
	base.EnableSync(EntitySyncCreateParam{
		Enabled: true,
		Topic:   "cached",
		PackCache: EntitySyncPackCacheConfig{
			Enabled:          true,
			ObserverAgnostic: true,
		},
		Packer: EntitySyncPackFunc{
			Enter: func(observer SyncObserverRef) (SyncPacket, error) {
				packCount++
				return SyncPacket{Body: "enter"}, nil
			},
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				return SyncPacket{Body: "update", Mask: mask}, nil
			},
		},
	})

	refA := NewPlayerSyncObserver(401)
	packetA, ok := base.Sync().AddObserverRef(refA)
	if !ok {
		t.Fatal("AddObserverRef A should produce packet")
	}
	refB := NewPlayerSyncObserver(402)
	packetB, ok := base.Sync().AddObserverRef(refB)
	if !ok {
		t.Fatal("AddObserverRef B should produce packet")
	}
	if packCount != 1 {
		t.Fatalf("clean observer-agnostic enter should be packed once, got %d", packCount)
	}
	if packetA.Observer != refA || packetB.Observer != refB || packetA.ObserverID != 401 || packetB.ObserverID != 402 {
		t.Fatalf("cached packets should keep observer headers, A=%+v B=%+v", packetA, packetB)
	}

	base.MarkSyncDirty(0b1)
	_ = base.FlushSync()
	base.Sync().RemoveObserverRef(refA)
	packetA, ok = base.Sync().AddObserverRef(refA)
	if !ok {
		t.Fatal("AddObserverRef A after dirty should produce packet")
	}
	if packCount != 2 {
		t.Fatalf("dirty should invalidate enter cache, got pack count %d", packCount)
	}
	if packetA.Observer != refA || packetA.ObserverID != 401 {
		t.Fatalf("post-dirty cached packet header mismatch: %+v", packetA)
	}
}

func TestEntitySyncTryAddObserverRefFromCachedEnter(t *testing.T) {
	id := mustBuildTestEntityID(t, 106, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	packCount := 0
	base.EnableSync(EntitySyncCreateParam{
		Enabled: true,
		Topic:   "cached-fast-enter",
		PackCache: EntitySyncPackCacheConfig{
			Enabled:          true,
			ObserverAgnostic: true,
		},
		Packer: EntitySyncPackFunc{
			Enter: func(observer SyncObserverRef) (SyncPacket, error) {
				packCount++
				return SyncPacket{Body: "enter"}, nil
			},
		},
	})

	refA := NewPlayerSyncObserver(601)
	if packet, ok := base.Sync().TryAddObserverRefFromCachedEnter(refA); ok || packet.Type != SyncPacketTypeNone {
		t.Fatalf("empty cache should not add observer: packet=%+v ok=%v", packet, ok)
	}
	if base.Sync().HasObserverRef(refA) {
		t.Fatal("empty cache path should not add observer")
	}

	if _, ok := base.Sync().AddObserverRef(refA); !ok {
		t.Fatal("AddObserverRef should build the first cached packet")
	}
	if packCount != 1 {
		t.Fatalf("first add should pack once, got %d", packCount)
	}

	refB := NewPlayerSyncObserver(602)
	packet, ok := base.Sync().TryAddObserverRefFromCachedEnter(refB)
	if !ok {
		t.Fatal("cached enter path should add a fresh observer")
	}
	if packCount != 1 {
		t.Fatalf("cached enter path should not repack, got %d", packCount)
	}
	if packet.Type != SyncPacketEnter || packet.Topic != "cached-fast-enter" || packet.EntityID != id ||
		packet.Observer != refB || packet.ObserverID != 602 || packet.Body != "enter" {
		t.Fatalf("cached enter packet mismatch: %+v", packet)
	}

	if packet, ok := base.Sync().TryAddObserverRefFromCachedEnter(refB); ok || packet.Type != SyncPacketTypeNone {
		t.Fatalf("existing observer should not produce a second packet: packet=%+v ok=%v", packet, ok)
	}
}

func TestEntitySyncObserverAgnosticPackCacheReusesUpdatePayloadWithinDirtyFlush(t *testing.T) {
	id := mustBuildTestEntityID(t, 107, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	updatePackCount := 0
	base.EnableSync(EntitySyncCreateParam{
		Enabled: true,
		Topic:   "cached-update",
		PackCache: EntitySyncPackCacheConfig{
			Enabled:          true,
			ObserverAgnostic: true,
		},
		Packer: EntitySyncPackFunc{
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				updatePackCount++
				return SyncPacket{Body: "update", Mask: mask}, nil
			},
		},
	})
	refA := NewPlayerSyncObserver(501)
	refB := NewPlayerSyncObserver(502)
	if _, ok := base.Sync().AddObserverRef(refA); !ok {
		t.Fatal("AddObserverRef A should succeed")
	}
	if _, ok := base.Sync().AddObserverRef(refB); !ok {
		t.Fatal("AddObserverRef B should succeed")
	}

	base.MarkSyncDirty(0b101)
	packets := base.FlushSync()
	if len(packets) != 2 {
		t.Fatalf("packets len = %d, want 2", len(packets))
	}
	if updatePackCount != 1 {
		t.Fatalf("observer-agnostic update should be packed once per dirty flush, got %d", updatePackCount)
	}
	if hasSyncPackCacheKind(base.Sync(), syncPackCacheKindUpdate) {
		t.Fatal("update cache should be cleared after dirty flush")
	}
	for _, packet := range packets {
		if packet.Mask != 0b101 || packet.Body != "update" {
			t.Fatalf("cached update packet mismatch: %+v", packet)
		}
		if packet.Observer != refA && packet.Observer != refB {
			t.Fatalf("cached update observer mismatch: %+v", packet)
		}
	}

	base.MarkSyncDirty(0b010)
	_ = base.FlushSync()
	if updatePackCount != 2 {
		t.Fatalf("new dirty mask should pack a fresh update, got %d", updatePackCount)
	}
}

func hasSyncPackCacheKind(state *EntitySyncState, kind syncPackCacheKind) bool {
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for key := range state.packCache {
		if key.kind == kind {
			return true
		}
	}
	return false
}

func TestEntitySyncGuardReleaseFlushesToScheduler(t *testing.T) {
	oldScheduler := GetEntitySyncScheduler()
	defer SetEntitySyncScheduler(oldScheduler)

	id := mustBuildTestEntityID(t, 104, EntityCategory(1), EntityKind(2))
	ent := &factoryTestEntity{EntityBase: NewEntityBase(id, EntityCategory(1), false, EntityKind(2))}
	mu := &observedSyncMutex{id: id}
	ent.EntityBase.mu = mu
	scheduler := &testSyncScheduler{}
	SetEntitySyncScheduler(scheduler)
	ent.EnableSync(EntitySyncCreateParam{
		Enabled: true,
		Topic:   "release",
		Packer: EntitySyncPackFunc{
			Update: func(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
				if !mu.Held() {
					t.Fatal("release flush update packer should run while entity lock is held")
				}
				return SyncPacket{Body: "release-delta", Mask: mask}, nil
			},
		},
	})
	if _, ok := ent.Sync().AddObserverRef(NewPlayerSyncObserver(302)); !ok {
		t.Fatal("AddObserverRef should succeed")
	}

	err := WithGuardScope("sync_release_flush", func(scope *GuardScope) error {
		if !scope.Guard().RequireEntity(ent) {
			t.Fatal("RequireEntity should succeed")
		}
		ent.MarkSyncDirty(0b111)
		if scheduler.marked != 0 {
			t.Fatalf("dirty state should wait for entity release while guarded, marked=%d", scheduler.marked)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithGuardScope: %v", err)
	}
	if scheduler.marked != 0 {
		t.Fatalf("guarded dirty should not be queued for async state flush, marked=%d", scheduler.marked)
	}
	if len(scheduler.packets) != 1 {
		t.Fatalf("release hook packets=%+v", scheduler.packets)
	}
	got := scheduler.packets[0]
	if got.Type != SyncPacketUpdate || got.Topic != "release" || got.Mask != 0b111 || got.Body != "release-delta" {
		t.Fatalf("release hook packet mismatch: %+v", got)
	}
	if mu.MaxDepth() != 1 {
		t.Fatalf("release flush should reuse current guard lock, max lock depth=%d", mu.MaxDepth())
	}
}

type testSyncSink struct {
	packets []SyncPacket
}

func (s *testSyncSink) Enqueue(packet SyncPacket) {
	s.packets = append(s.packets, packet)
}

func (s *testSyncSink) EnqueueBatch(packets []SyncPacket) {
	s.packets = append(s.packets, packets...)
}

type testSyncScheduler struct {
	testSyncSink
	marked int
}

func (s *testSyncScheduler) MarkDirtyState(*EntitySyncState) {
	s.marked++
}

func (s *testSyncScheduler) Flush() []SyncPacket {
	return s.packets
}
