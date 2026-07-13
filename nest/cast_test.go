package nest

import (
	"errors"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

const (
	castPlayerCategory   entity.EntityCategory = 1
	castAllianceCategory entity.EntityCategory = 2
	castOtherCategory    entity.EntityCategory = 3
	castPlayerKind       entity.EntityKind     = 11
	castAllianceKind     entity.EntityKind     = 12
	castOtherKind        entity.EntityKind     = 13
)

func newMockEntityWithKind(id int64, category entity.EntityCategory, kind entity.EntityKind) *mockEntity {
	return &mockEntity{EntityBase: entity.NewEntityBase(id, category, false, kind)}
}

func mustBuildCastID(t *testing.T, uniqueID int64, category entity.EntityCategory, kind entity.EntityKind) int64 {
	t.Helper()
	if kind == entity.EntityKindNone {
		return int64(((uint64(uniqueID) & entity.UniqueIDMask) << entity.UniqueIDShift) | (uint64(category) & entity.EntityCategoryMask))
	}
	entity.MustRegisterEntityKindCategory(kind, category)
	id, err := entity.BuildEntityID(uniqueID, kind)
	if err != nil {
		t.Fatalf("BuildEntityID: %v", err)
	}
	return id
}

func withCastGroupFunc(t *testing.T) {
	t.Helper()
	old := entity.GetEntityGroupFunc
	entity.GetEntityGroupFunc = func(category entity.EntityCategory) int {
		switch category {
		case castPlayerCategory:
			return entity.EntityGroupPlayer
		case castAllianceCategory:
			return entity.EntityGroupAlliance
		default:
			return entity.EntityGroupOther
		}
	}
	t.Cleanup(func() {
		entity.GetEntityGroupFunc = old
	})
}

type castRemoteManager struct {
	prepare func(ids []int64) (func(), error)
}

func (m *castRemoteManager) PrepareRemoteEntities(ids []int64) (func(), error) {
	if m.prepare == nil {
		return nil, nil
	}
	return m.prepare(ids)
}

func (*castRemoteManager) GetOrCreate(int64, entity.EntityCategory, entity.EntityKind) entity.IRemoteEntityWrapper {
	return nil
}
func (*castRemoteManager) Get(int64) (entity.IRemoteEntityWrapper, bool)  { return nil, false }
func (*castRemoteManager) Remove(int64)                                   {}
func (*castRemoteManager) SetLoader(entity.IRemoteEntityLoader)           {}
func (*castRemoteManager) SetMarkerStore(entity.IRemoteEntityMarkerStore) {}
func (*castRemoteManager) SetSyncer(entity.IRemoteEntitySyncer)           {}
func (*castRemoteManager) Loader() entity.IRemoteEntityLoader             { return nil }
func (*castRemoteManager) MarkerStore() entity.IRemoteEntityMarkerStore   { return nil }
func (*castRemoteManager) Syncer() entity.IRemoteEntitySyncer             { return nil }
func (*castRemoteManager) ResolveRemoteSnapshot(entity.RemoteSnapshotResolveRequest) (entity.RemoteSnapshot, error) {
	return entity.RemoteSnapshot{}, nil
}

func TestCastPlayerCanCastAllianceAndOtherByCategoryOrder(t *testing.T) {
	withCastGroupFunc(t)
	getter := newMockGetter()
	InitGlobalGetter(getter)
	t.Cleanup(func() { InitGlobalGetter(nil) })

	playerID := mustBuildCastID(t, 100, castPlayerCategory, castPlayerKind)
	allianceID := mustBuildCastID(t, 200, castAllianceCategory, castAllianceKind)
	otherID := mustBuildCastID(t, 300, castOtherCategory, castOtherKind)
	player := newMockEntityWithKind(playerID, castPlayerCategory, castPlayerKind)
	alliance := newMockEntityWithKind(allianceID, castAllianceCategory, castAllianceKind)
	other := newMockEntityWithKind(otherID, castOtherCategory, castOtherKind)
	getter.Add(player)
	getter.Add(alliance)
	getter.Add(other)

	_, release := entity.NewGuardScope("cast_test")
	defer release()
	guard := entity.GetEntityGuard()
	if !guard.RequireEntity(player) {
		t.Fatal("lock player")
	}

	got, err := CastMulti(NewCastTarget(allianceID), NewCastTarget(otherID))
	if err != nil {
		t.Fatalf("CastMulti alliance/other: %v", err)
	}
	if len(got) != 2 || got[0] != alliance || got[1] != other {
		t.Fatalf("casted = %#v, want alliance/other", got)
	}
	if guard.Entities()[allianceID] == nil {
		t.Fatal("alliance should be locked in current guard")
	}
	if guard.Entities()[otherID] == nil {
		t.Fatal("other should be locked in current guard")
	}
}

func TestCastAllianceCanCastOtherByCategoryOrder(t *testing.T) {
	withCastGroupFunc(t)
	getter := newMockGetter()
	InitGlobalGetter(getter)
	t.Cleanup(func() { InitGlobalGetter(nil) })

	allianceID := mustBuildCastID(t, 201, castAllianceCategory, castAllianceKind)
	otherID := mustBuildCastID(t, 301, castOtherCategory, castOtherKind)
	alliance := newMockEntityWithKind(allianceID, castAllianceCategory, castAllianceKind)
	other := newMockEntityWithKind(otherID, castOtherCategory, castOtherKind)
	getter.Add(alliance)
	getter.Add(other)

	_, release := entity.NewGuardScope("cast_test")
	defer release()
	guard := entity.GetEntityGuard()
	if !guard.RequireEntity(alliance) {
		t.Fatal("lock alliance")
	}

	got, err := CastTargetOne[*mockEntity](NewCastTarget(otherID))
	if err != nil {
		t.Fatalf("CastTargetOne other: %v", err)
	}
	if got != other {
		t.Fatalf("casted other = %p, want %p", got, other)
	}
	if guard.Entities()[otherID] == nil {
		t.Fatal("other should be locked in current guard")
	}
}

func TestCastRejectsReverseCategoryOrder(t *testing.T) {
	withCastGroupFunc(t)
	getter := newMockGetter()
	InitGlobalGetter(getter)
	t.Cleanup(func() { InitGlobalGetter(nil) })

	playerID := mustBuildCastID(t, 101, castPlayerCategory, castPlayerKind)
	allianceID := mustBuildCastID(t, 202, castAllianceCategory, castAllianceKind)
	otherID := mustBuildCastID(t, 302, castOtherCategory, castOtherKind)
	player := newMockEntityWithKind(playerID, castPlayerCategory, castPlayerKind)
	alliance := newMockEntityWithKind(allianceID, castAllianceCategory, castAllianceKind)
	other := newMockEntityWithKind(otherID, castOtherCategory, castOtherKind)
	getter.Add(player)
	getter.Add(alliance)
	getter.Add(other)

	cases := []struct {
		name     string
		locked   *mockEntity
		targetID int64
	}{
		{name: "alliance_to_player", locked: alliance, targetID: playerID},
		{name: "other_to_player", locked: other, targetID: playerID},
		{name: "other_to_alliance", locked: other, targetID: allianceID},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, release := entity.NewGuardScope("cast_test")
			defer release()
			if !entity.GetEntityGuard().RequireEntity(tc.locked) {
				t.Fatalf("lock %s", tc.name)
			}
			_, err := CastTargetOne[*mockEntity](NewCastTarget(tc.targetID))
			if !errors.Is(err, ErrCastDeadlockRisk) {
				t.Fatalf("err = %v, want ErrCastDeadlockRisk", err)
			}
		})
	}
}

func TestCastRejectsInvalidTargetID(t *testing.T) {
	getter := newMockGetter()
	InitGlobalGetter(getter)
	t.Cleanup(func() { InitGlobalGetter(nil) })

	_, release := entity.NewGuardScope("cast_test")
	defer release()

	_, err := CastTargetOne[*mockEntity](NewCastTarget(102))
	if !errors.Is(err, ErrCastInvalidTarget) {
		t.Fatalf("err = %v, want ErrCastInvalidTarget", err)
	}
}

func TestCastPreparesRemoteManagedEntity(t *testing.T) {
	getter := newMockGetter()
	InitGlobalGetter(getter)
	t.Cleanup(func() { InitGlobalGetter(nil) })

	category := castOtherCategory
	remoteID := mustBuildCastID(t, 300, category, nestRemoteManagedKind)
	remoteEntity := newMockEntityWithKind(remoteID, category, nestRemoteManagedKind)

	var prepared []int64
	releaseCalled := false
	remoteMgr := &castRemoteManager{prepare: func(ids []int64) (func(), error) {
		prepared = append(prepared, ids...)
		getter.Add(remoteEntity)
		return func() { releaseCalled = true }, nil
	}}
	entity.BindRemoteEntityManager(remoteMgr)
	t.Cleanup(func() { entity.UnbindRemoteEntityManager(remoteMgr) })

	_, releaseGuard := entity.NewGuardScope("cast_test")
	got, err := CastTargetOne[*mockEntity](NewCastTarget(remoteID))
	if err != nil {
		releaseGuard()
		t.Fatalf("CastTargetOne remote: %v", err)
	}
	if got != remoteEntity {
		releaseGuard()
		t.Fatalf("casted remote = %p, want %p", got, remoteEntity)
	}
	if len(prepared) != 1 || prepared[0] != remoteID {
		releaseGuard()
		t.Fatalf("prepared = %v, want [%d]", prepared, remoteID)
	}
	if releaseCalled {
		releaseGuard()
		t.Fatal("remote release should wait for guard release")
	}
	releaseGuard()
	if !releaseCalled {
		t.Fatal("remote release should run when guard scope releases")
	}
}

func TestCastRequiresContext(t *testing.T) {
	getter := newMockGetter()
	InitGlobalGetter(getter)
	t.Cleanup(func() { InitGlobalGetter(nil) })

	_, err := CastTargetOne[*mockEntity](NewCastTarget(1))
	if !errors.Is(err, ErrCastNoContext) {
		t.Fatalf("err = %v, want ErrCastNoContext", err)
	}
}
