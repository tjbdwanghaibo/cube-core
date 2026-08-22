package entity

import (
	"sync"
	"testing"
)

const (
	testRemotePlayerKind   EntityKind = 101
	testRemoteAllianceKind EntityKind = 102
)

// testRemoteEntity is a minimal remote entity for testing.
type testRemoteEntity struct {
	RemoteEntityBase
	cleared bool
}

func (t *testRemoteEntity) Base() *EntityBase              { return &t.EntityBase }
func (t *testRemoteEntity) OnDataChange(_ []byte, _ int64) {}

func mustBuildRemoteEntityID(uniqueID int64, category EntityCategory, kind EntityKind) int64 {
	if kind == EntityKindNone {
		kind = remoteTestKind(category)
	}
	MustRegisterEntityKindCategory(kind, category)
	id, err := BuildEntityID(uniqueID, kind)
	if err != nil {
		panic(err)
	}
	return id
}

func remoteTestKind(category EntityCategory) EntityKind {
	if category == testEntityCategoryAlliance {
		return testRemoteAllianceKind
	}
	return testRemotePlayerKind
}

func newTestRemoteEntity(id int64, typo EntityCategory) *testRemoteEntity {
	e := &testRemoteEntity{}
	kind := remoteTestKind(typo)
	e.EntityBase = *NewEntityBase(mustBuildRemoteEntityID(id, typo, kind), typo, false, kind)
	e.EntityBase.SetHooks(func() { e.cleared = true }, nil)
	return e
}

func newMarkedTestRemoteEntity(id int64, typo EntityCategory) *testRemoteEntity {
	e := &testRemoteEntity{}
	kind := remoteTestKind(typo)
	MustRegisterEntityKindCategory(kind, typo)
	e.EntityBase = *NewEntityBase(makeEntityID(id, typo, kind, true), typo, false, kind)
	e.EntityBase.SetHooks(func() { e.cleared = true }, nil)
	return e
}

func TestRemoteEntityBase_Interface(t *testing.T) {
	e := newTestRemoteEntity(100, testEntityCategoryPlayer)

	// Verify IThreadSafeRemoteEntity contract
	var _ IThreadSafeRemoteEntity = e

	if e.EntityVersion() != 0 {
		t.Fatal("initial entity version should be 0")
	}
	e.SetEntityVersion(42)
	if e.EntityVersion() != 42 {
		t.Fatalf("expected version 42, got %d", e.EntityVersion())
	}

	if e.ExcludeSId() != 0 {
		t.Fatal("initial excludeSId should be 0")
	}
	e.SetExcludeSId(1001)
	if e.ExcludeSId() != 1001 {
		t.Fatalf("expected excludeSId 1001, got %d", e.ExcludeSId())
	}
}

func TestRemoteEntityBase_VersionVectorIsCoherent(t *testing.T) {
	base := NewRemoteEntityBase(1, testEntityCategoryPlayer, false, testRemotePlayerKind)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for generation := uint64(2); generation < 10_000; generation++ {
			if err := base.SetRemoteVersionVector(RemoteVersionVector{
				StateVersion: generation,
				MarkerEpoch:  generation,
				LockFence:    generation,
				RouteEpoch:   generation,
			}); err != nil {
				t.Errorf("set vector: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10_000; i++ {
			version := base.RemoteVersionVector()
			if version.StateVersion == 0 {
				continue
			}
			if version.StateVersion != version.MarkerEpoch || version.StateVersion != version.LockFence || version.StateVersion != version.RouteEpoch {
				t.Errorf("torn version vector: %+v", version)
				return
			}
		}
	}()
	wg.Wait()
}

func TestRemoteEntityBase_IsRemoteCapable(t *testing.T) {
	e := newTestRemoteEntity(100, testEntityCategoryPlayer)
	if e.IsRemoteCapable() {
		t.Fatal("entity without remote bit should not be marked remote")
	}

	e2 := newMarkedTestRemoteEntity(200, testEntityCategoryPlayer)
	if !e2.IsRemoteCapable() {
		t.Fatal("entity with remote bit should be marked remote")
	}
}

func TestRemoteEntityBase_TouchUnTouch(t *testing.T) {
	e := newTestRemoteEntity(300, testEntityCategoryAlliance)

	if !e.Touch() {
		t.Fatal("Touch should succeed on fresh remote entity")
	}
	e.UnTouch()

	if e.IsClear() {
		t.Fatal("entity should not be cleared without SetRemoved")
	}
}

func TestRemoteEntityBase_GUId(t *testing.T) {
	rawID := int64(500)
	e := newTestRemoteEntity(rawID, testEntityCategoryPlayer)

	guid := e.GUId()
	meta := ResolveEntityID(guid)
	if meta.UniqueID != rawID {
		t.Fatalf("expected rawID %d, got %d", rawID, meta.UniqueID)
	}
	if meta.Category != testEntityCategoryPlayer {
		t.Fatalf("expected type %d, got %d", testEntityCategoryPlayer, meta.Category)
	}
}

func TestRemoteSnapshotAllowStaleDoesNotBypassExpiry(t *testing.T) {
	snapshot := RemoteSnapshot{Version: 1, ExpiresAt: 100}
	if snapshot.Accepts(RemoteReadOption{AllowStale: true, NowMillis: 101}) {
		t.Fatal("allow stale must not accept an expired cache entry")
	}
	if snapshot.Accepts(RemoteReadOption{AllowStale: true, NowMillis: 100}) {
		t.Fatal("snapshot must expire at its expiry boundary")
	}
}
