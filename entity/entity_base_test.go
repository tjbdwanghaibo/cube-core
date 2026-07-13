package entity

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// Test-local entity category constants (moved to game/view in production code).
const (
	testEntityCategoryPlayer   EntityCategory = 1
	testEntityCategoryAlliance EntityCategory = 2
)

var testDestroyCommon EntityDestroyReason

// testEntity is a minimal entity for testing.
type testEntity struct {
	*EntityBase
	cleared bool
}

func (t *testEntity) Base() *EntityBase { return t.EntityBase }

func newTestEntity(id int64, typo EntityCategory) *testEntity {
	e := &testEntity{}
	e.EntityBase = NewEntityBase(id, typo, false)
	e.EntityBase.SetHooks(func() { e.cleared = true }, nil)
	return e
}

func mustBuildTestEntityID(t *testing.T, uniqueID int64, category EntityCategory, kind EntityKind) int64 {
	t.Helper()
	if kind == EntityKindNone {
		return makeEntityID(uniqueID, category, kind, false)
	}
	MustRegisterEntityKindCategory(kind, category)
	id, err := BuildEntityID(uniqueID, kind)
	if err != nil {
		t.Fatalf("BuildEntityID: %v", err)
	}
	return id
}

func TestTouchUnTouch(t *testing.T) {
	e := newTestEntity(1, testEntityCategoryPlayer)

	if !e.Touch() {
		t.Fatal("Touch should succeed on fresh entity")
	}
	if !e.Touch() {
		t.Fatal("Touch should succeed again (ref count 2)")
	}
	e.UnTouch()
	e.UnTouch()

	// Entity not removed, so no cleanup should happen
	if e.IsClear() {
		t.Fatal("Entity should not be cleared without SetRemoved")
	}
}

func TestTouchAfterRemoved(t *testing.T) {
	e := newTestEntity(2, testEntityCategoryAlliance)

	e.Touch()
	e.SetRemoved()

	// Touch should fail after removal
	if e.Touch() {
		t.Fatal("Touch should fail after SetRemoved")
	}

	// UnTouch with count=1 after removed should trigger clear
	e.UnTouch()
	if !e.IsClear() {
		t.Fatal("Entity should be cleared after last UnTouch with removed flag")
	}
	if !e.cleared {
		t.Fatal("onClear hook should have been called")
	}
}

func TestConcurrentTouch(t *testing.T) {
	e := newTestEntity(3, testEntityCategoryPlayer)
	const goroutines = 100

	var wg sync.WaitGroup
	var successCount atomic.Int32

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if e.Touch() {
				successCount.Add(1)
				e.UnTouch()
			}
		}()
	}
	wg.Wait()

	if successCount.Load() != goroutines {
		t.Fatalf("Expected %d successful touches, got %d", goroutines, successCount.Load())
	}
}

func TestEntityGuardReleaseContinuesAfterHookPanic(t *testing.T) {
	e1 := newTestEntity(101, testEntityCategoryPlayer)
	e2 := newTestEntity(102, testEntityCategoryAlliance)

	unregister := RegisterOnEntityRelease(func(e IThreadSafeEntity) {
		if e.ID() == e1.ID() {
			panic("release failed")
		}
	})
	defer unregister()

	guard := GetEntityGuard()
	if !guard.RequireEntity(e1) {
		t.Fatal("lock e1")
	}
	if !guard.RequireEntity(e2) {
		t.Fatal("lock e2")
	}
	guard.ReleaseAll()
	defer EntityGuardRelease(guard)

	if len(guard.eMap) != 0 {
		t.Fatalf("all locks should be released, remaining=%d", len(guard.eMap))
	}
}

func TestEntityGuardBasic(t *testing.T) {
	e := newTestEntity(10, testEntityCategoryPlayer)

	guard := GetEntityGuard()
	if !guard.RequireEntity(e) {
		t.Fatal("RequireEntity should succeed")
	}

	// Re-require same entity should succeed (idempotent)
	if !guard.RequireEntity(e) {
		t.Fatal("RequireEntity same entity should be idempotent")
	}

	guard.ReleaseAll()
	EntityGuardRelease(guard)
}

func TestEntityGuardReleaseAllPostReleaseIdempotent(t *testing.T) {
	guard := GetEntityGuard()
	defer EntityGuardRelease(guard)

	count := 0
	guard.AppendPostRelease(func() {
		count++
	})

	guard.ReleaseAll()
	guard.ReleaseAll()

	if count != 1 {
		t.Fatalf("post-release callback should run once, got %d", count)
	}
}

func TestBuildResolveEntityID(t *testing.T) {
	rawID := int64(12345)
	entityCategory := testEntityCategoryPlayer
	kind := EntityKind(4)

	guid := mustBuildTestEntityID(t, rawID, entityCategory, kind)
	meta := ResolveEntityID(guid)

	if meta.UniqueID != rawID {
		t.Fatalf("ResolveEntityID uniqueID: got %d, want %d", meta.UniqueID, rawID)
	}
	if meta.Category != entityCategory {
		t.Fatalf("ResolveEntityID entityCategory: got %d, want %d", meta.Category, entityCategory)
	}
	if meta.Kind != kind {
		t.Fatalf("ResolveEntityID kind: got %d, want %d", meta.Kind, kind)
	}
}

func TestBuildResolveEntityIDWithKind(t *testing.T) {
	const (
		uniqueID = int64(7)
		category = EntityCategory(2)
		kind     = EntityKind(9)
	)

	id := mustBuildTestEntityID(t, uniqueID, category, kind)
	meta := ResolveEntityID(id)

	if meta.UniqueID != uniqueID {
		t.Fatalf("ResolveEntityID uniqueID: got %d, want %d", meta.UniqueID, uniqueID)
	}
	if meta.Category != category {
		t.Fatalf("ResolveEntityID category: got %d, want %d", meta.Category, category)
	}
	if gotKind := GetEntityKindFromID(id); gotKind != kind {
		t.Fatalf("GetEntityKindFromID: got %d, want %d", gotKind, kind)
	}
	if id >= 1<<20 {
		t.Fatalf("early EntityID should stay compact, got %d", id)
	}
}

func TestBuildResolveEntityIDWithRemote(t *testing.T) {
	const (
		uniqueID = int64(7)
		category = EntityCategory(2)
		kind     = EntityKind(9)
	)

	id := makeEntityID(uniqueID, category, kind, true)
	meta := ResolveEntityID(id)
	if meta.UniqueID != uniqueID || meta.Category != category {
		t.Fatalf("ResolveEntityID remote mismatch: unique=%d category=%d", meta.UniqueID, meta.Category)
	}
	if gotKind := GetEntityKindFromID(id); gotKind != kind {
		t.Fatalf("GetEntityKindFromID: got %d, want %d", gotKind, kind)
	}
	if !IsRemoteCapableEntityID(id) {
		t.Fatal("remote bit should be set")
	}
	if IsRemoteCapableEntityID(clearRemoteCapableBit(id)) {
		t.Fatal("remote bit should be clear")
	}
	if !IsRemoteCapableEntityID(setRemoteCapableBit(clearRemoteCapableBit(id))) {
		t.Fatal("remote bit should be set again")
	}
	if id <= 0 {
		t.Fatalf("remote entity id should remain positive, got %d", id)
	}
}

func TestResolveEntityID(t *testing.T) {
	const (
		uniqueID = int64(123456789)
		category = EntityCategory(2)
		kind     = EntityKind(17)
	)
	fullID := mustBuildTestEntityID(t, uniqueID, category, kind)

	meta := ResolveEntityID(fullID)
	if meta.UniqueID != uniqueID || meta.FullID != fullID || meta.Category != category || meta.Kind != kind {
		t.Fatalf("ResolveEntityID full meta mismatch: %+v", meta)
	}

	plainID := int64(123456788)
	meta = ResolveEntityID(plainID)
	if meta.FullID != plainID {
		t.Fatalf("ResolveEntityID should treat input as canonical full id: got full %d, want %d", meta.FullID, plainID)
	}
}

func TestNormalizeFullID(t *testing.T) {
	const (
		category = EntityCategory(2)
		kind     = EntityKind(17)
	)
	MustRegisterEntityKindCategory(kind, category)
	id, err := BuildEntityID(100, kind)
	if err != nil {
		t.Fatalf("BuildEntityID: %v", err)
	}
	fullID, err := NormalizeFullID(id, kind)
	if err != nil {
		t.Fatalf("NormalizeFullID: %v", err)
	}
	if fullID != id {
		t.Fatalf("NormalizeFullID = %d, want %d", fullID, id)
	}
	MustRegisterEntityKindCategory(kind+1, category)
	if _, err := NormalizeFullID(id, kind+1); err == nil {
		t.Fatal("NormalizeFullID should reject kind mismatch")
	}
	if MatchEntityID(id, kind+1) {
		t.Fatal("MatchEntityID should reject kind mismatch")
	}
}

func TestIDGen(t *testing.T) {
	var counter atomic.Uint64
	acquireBlock := func() (uint64, error) {
		return counter.Add(1), nil
	}

	gen, err := NewIDGen(1, acquireBlock)
	if err != nil {
		t.Fatalf("NewIDGen failed: %v", err)
	}

	seen := make(map[uint64]bool)
	for i := 0; i < 1000; i++ {
		id, err := gen.Generate()
		if err != nil {
			t.Fatalf("Generate failed at iteration %d: %v", i, err)
		}
		if seen[id] {
			t.Fatalf("Duplicate ID generated: %d at iteration %d", id, i)
		}
		seen[id] = true
	}
}

func TestIDGenSkipsZeroInFirstBlock(t *testing.T) {
	gen, err := NewIDGen(0, nil)
	if err != nil {
		t.Fatalf("NewIDGen failed: %v", err)
	}
	id, err := gen.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if id != 1 {
		t.Fatalf("first block first id = %d, want 1", id)
	}
}

func TestIDGenRejectsStaticReservedBlock(t *testing.T) {
	reservedBlock := (uint64(1) << (UniqueIDBits - 1)) >> IDGenOffsetBits
	if _, err := NewIDGen(reservedBlock, nil); !errors.Is(err, ErrInvalidBlockNo) {
		t.Fatalf("NewIDGen reserved block err = %v, want ErrInvalidBlockNo", err)
	}
}

func TestComponentManager(t *testing.T) {
	cm := NewComponentManager()

	comp := &mockComponent{name: "bag"}
	cm.Set(1, comp)

	got := cm.Get(1)
	if got == nil {
		t.Fatal("expected component")
	}
	if got.Name() != "bag" {
		t.Fatalf("expected 'bag', got %q", got.Name())
	}
}

func TestComponentTopologicalSortIncludesDependencies(t *testing.T) {
	sorter := newCompTopologicalSort()
	sorter.RegisterCompDependency(ComponentType(2), ComponentType(1))
	sorted, err := sorter.GetTopologicalSortedComponents()
	if err != nil {
		t.Fatalf("GetTopologicalSortedComponents: %v", err)
	}
	if len(sorted) != 2 || sorted[0] != ComponentType(1) || sorted[1] != ComponentType(2) {
		t.Fatalf("unexpected component order: %+v", sorted)
	}
}

func TestDaoManager(t *testing.T) {
	dm := NewDaoManager()

	dao := &mockDao{id: 100, coll: "players"}
	dm.Set("players", dao)

	got := dm.Get("players")
	if got == nil {
		t.Fatal("expected dao")
	}
	if got.Id() != 100 {
		t.Fatalf("expected id 100, got %d", got.Id())
	}
}

// --- mocks ---

type mockComponent struct {
	name string
}

func (m *mockComponent) Name() string                                    { return m.name }
func (m *mockComponent) OnInitFinish(_ *EntityCreateParam, _ bool) error { return nil }
func (m *mockComponent) OnDestroy(_ EntityDestroyReason)                 {}

type mockDao struct {
	id   int64
	coll string
}

func (m *mockDao) Id() int64        { return m.id }
func (m *mockDao) SetId(id int64)   { m.id = id }
func (m *mockDao) DbName() string   { return "" }
func (m *mockDao) CollName() string { return m.coll }
func (m *mockDao) Dirty() IDirty    { return nil }
func (m *mockDao) CleanDirty()      {}
