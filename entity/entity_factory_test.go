package entity

import (
	"sync/atomic"
	"testing"
)

const (
	testEntityCategory     EntityCategory = 3
	testEntityKind         EntityKind     = 3
	testRemoteBadKind      EntityKind     = 252
	testRuntimeRebuildKind EntityKind     = 253
)

// factoryTestDao implements DaoInterface for factory tests.
type factoryTestDao struct {
	id   int64
	coll string
}

func (d *factoryTestDao) Id() int64        { return d.id }
func (d *factoryTestDao) SetId(id int64)   { d.id = id }
func (d *factoryTestDao) DbName() string   { return "test" }
func (d *factoryTestDao) CollName() string { return d.coll }
func (d *factoryTestDao) Dirty() IDirty    { return nil }
func (d *factoryTestDao) CleanDirty()      {}

// factoryTestEntity implements IThreadSafeEntity.
type factoryTestEntity struct {
	*EntityBase
	ComponentManager
	DaoManager
	initCalled bool
}

func (e *factoryTestEntity) Base() *EntityBase { return e.EntityBase }

func init() {
	// Register test builder in a high type number to avoid conflicts
	RegisterEntityBuilder(&EntityBuilderParam{
		Category: testEntityCategory,
		Kind:     testEntityKind,
		Builder: func(param *EntityCreateParam) (IThreadSafeEntity, error) {
			e := &factoryTestEntity{}
			e.EntityBase = NewEntityBase(param.Id, param.Category, false, param.Kind)
			e.ComponentManager = NewComponentManager()
			e.DaoManager = NewDaoManager()
			e.initCalled = true

			// Wire DAOs
			for coll, dao := range param.Dao {
				e.DaoManager.Set(coll, dao)
			}
			return e, nil
		},
		DaoBuilders: []DaoBuilderFunc{
			func() DaoInterface { return &factoryTestDao{coll: "test_coll"} },
		},
	})
	RegisterEntityBuilder(&EntityBuilderParam{
		Category:     testEntityCategory,
		Kind:         testRemoteBadKind,
		RemotePolicy: RemotePolicyManaged,
		NoPersist:    true,
		Builder: func(param *EntityCreateParam) (IThreadSafeEntity, error) {
			return &factoryTestEntity{
				EntityBase: NewEntityBase(param.Id, param.Category, true, param.Kind),
			}, nil
		},
	})
	RegisterEntityBuilder(&EntityBuilderParam{
		Category:  testEntityCategory,
		Kind:      testRuntimeRebuildKind,
		NoPersist: true,
		Lifetime:  EntityLifetimeRuntimeRebuild,
		Builder: func(param *EntityCreateParam) (IThreadSafeEntity, error) {
			return &factoryTestEntity{
				EntityBase: NewEntityBase(param.Id, param.Category, true, param.Kind),
			}, nil
		},
	})
}

func TestNewEntity_Create(t *testing.T) {
	Mgr = NewEntityManager()
	defer func() { Mgr = nil }()

	// Set up global ID generator
	var seq atomic.Uint64
	GenerateID = func() (uint64, error) {
		return seq.Add(1), nil
	}
	defer func() { GenerateID = nil }()

	param := &EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testEntityKind,
	}

	e, err := NewEntity(param)
	if err != nil {
		t.Fatalf("NewEntity: %v", err)
	}

	if e.ID() == 0 {
		t.Fatal("entity should have generated ID")
	}
	if e.GetEntityCategory() != testEntityCategory {
		t.Fatalf("expected type %d, got %d", testEntityCategory, e.GetEntityCategory())
	}

	fe := e.(*factoryTestEntity)
	if !fe.initCalled {
		t.Fatal("builder should have been called")
	}

	// DAO should be created and wired
	dao := fe.DaoManager.Get("test_coll")
	if dao == nil {
		t.Fatal("DAO should be wired")
	}
	if dao.Id() != e.StorageID() {
		t.Fatalf("DAO id should match entity storage id: %d != %d", dao.Id(), e.StorageID())
	}

	// Should be in manager
	if Mgr.Get(e.ID()) != e {
		t.Fatal("entity should be in manager")
	}
}

func TestBuildEntity_RemoteManagedRequiresRemoteInterface(t *testing.T) {
	GenerateID = func() (uint64, error) { return 1, nil }
	defer func() { GenerateID = nil }()

	_, err := BuildEntity(&EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testRemoteBadKind,
	})
	if err == nil {
		t.Fatal("expected remote managed policy validation error")
	}
}

func TestBuildEntity_LifetimePolicy(t *testing.T) {
	GenerateID = func() (uint64, error) { return 2, nil }
	defer func() { GenerateID = nil }()

	e, err := BuildEntity(&EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testRuntimeRebuildKind,
	})
	if err != nil {
		t.Fatalf("BuildEntity: %v", err)
	}
	if e.Base().Lifetime() != EntityLifetimeRuntimeRebuild {
		t.Fatalf("lifetime = %d, want %d", e.Base().Lifetime(), EntityLifetimeRuntimeRebuild)
	}
}

func TestNewEntity_Load(t *testing.T) {
	Mgr = NewEntityManager()
	defer func() { Mgr = nil }()

	// Simulate loading with pre-existing DAO. Persistent IDs are full entity IDs.
	wantID := mustBuildTestEntityID(t, 42, testEntityCategory, testEntityKind)
	existingDao := &factoryTestDao{id: wantID, coll: "test_coll"}
	param := &EntityCreateParam{
		IsCreate: false,
		Category: testEntityCategory,
		Kind:     testEntityKind,
		Id:       wantID,
		Dao:      map[string]DaoInterface{"test_coll": existingDao},
	}

	e, err := NewEntity(param)
	if err != nil {
		t.Fatalf("NewEntity: %v", err)
	}

	if e.ID() != wantID {
		t.Fatalf("expected id %d, got %d", wantID, e.ID())
	}
	if e.StorageID() != wantID {
		t.Fatalf("expected storage id %d, got %d", wantID, e.StorageID())
	}

	fe := e.(*factoryTestEntity)
	dao := fe.DaoManager.Get("test_coll")
	if dao != existingDao {
		t.Fatal("loaded DAO should be the one provided")
	}
}

func TestNewEntity_UnregisteredType(t *testing.T) {
	Mgr = NewEntityManager()
	defer func() { Mgr = nil }()

	GenerateID = func() (uint64, error) { return 1, nil }
	defer func() { GenerateID = nil }()

	param := &EntityCreateParam{
		IsCreate: true,
		Category: EntityCategory(255), // not registered
		Kind:     EntityKind(255),
	}

	_, err := NewEntity(param)
	if err == nil {
		t.Fatal("expected error for unregistered type")
	}
}

func TestDestroyEntity(t *testing.T) {
	Mgr = NewEntityManager()
	defer func() { Mgr = nil }()

	var seq atomic.Uint64
	GenerateID = func() (uint64, error) { return seq.Add(1), nil }
	defer func() { GenerateID = nil }()

	param := &EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testEntityKind,
	}
	e, _ := NewEntity(param)

	DestroyEntity(e, testDestroyCommon, false)

	if Mgr.Get(e.ID()) != nil {
		t.Fatal("entity should be removed from manager")
	}
	if !e.IsRemoved() {
		t.Fatal("entity should be marked removed")
	}
}
