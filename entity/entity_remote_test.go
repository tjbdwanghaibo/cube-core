package entity

import "testing"

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

func TestRemoteCapableVsRemoteMarked(t *testing.T) {
	id := makeEntityID(700, testEntityCategoryPlayer, EntityKind(3), true)
	if !IsRemoteCapableEntityID(id) {
		t.Fatal("remote-capable id should carry capability bit")
	}
	remoteEntityHooks.mu.Lock()
	oldMarked := remoteEntityHooks.marked
	remoteEntityHooks.mu.Unlock()
	defer func() {
		remoteEntityHooks.mu.Lock()
		remoteEntityHooks.marked = oldMarked
		remoteEntityHooks.mu.Unlock()
	}()

	remoteEntityHooks.mu.Lock()
	remoteEntityHooks.marked = func(int64) bool { return false }
	remoteEntityHooks.mu.Unlock()
	if IsRemoteMarkedEntityID(id) {
		t.Fatal("remote-capable id should not be marked when marker hook says false")
	}
	remoteEntityHooks.mu.Lock()
	remoteEntityHooks.marked = func(int64) bool { return true }
	remoteEntityHooks.mu.Unlock()
	if !IsRemoteMarkedEntityID(id) {
		t.Fatal("remote-capable id should be marked when marker hook says true")
	}
	if IsRemoteMarkedEntityID(clearRemoteCapableBit(id)) {
		t.Fatal("non remote-capable id should never be remote-marked")
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

func TestRemoteEntityHooks_Nil(t *testing.T) {
	// Ensure nil hooks don't panic
	remoteEntityHooks.mu.Lock()
	remoteEntityHooks.preparer = nil
	remoteEntityHooks.marked = nil
	remoteEntityHooks.mu.Unlock()
	if _, ok, err := PrepareRemoteEntities([]int64{1}); ok || err != nil {
		t.Fatalf("PrepareRemoteEntities nil hook = ok:%v err:%v, want ok:false err:nil", ok, err)
	}
	if IsRemoteMarkedEntityID(makeEntityID(1, testEntityCategoryPlayer, EntityKindNone, true)) {
		t.Fatal("nil marker hook should not mark remote entity")
	}
}
