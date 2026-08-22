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
			e.EntityBase = NewEntityBaseWithMutex(param.Id, param.Category, false, param.Mutex, param.Kind)
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
				EntityBase: NewEntityBaseWithMutex(param.Id, param.Category, true, param.Mutex, param.Kind),
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
				EntityBase: NewEntityBaseWithMutex(param.Id, param.Category, true, param.Mutex, param.Kind),
			}, nil
		},
	})
}

func TestNewEntity_Create(t *testing.T) {
	var seq atomic.Uint64
	manager := NewEntityManager(WithEntityIDGenerator(func() (uint64, error) {
		return seq.Add(1), nil
	}))

	param := &EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testEntityKind,
	}

	e, err := manager.Create(param)
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
	if manager.Get(e.ID()) != e {
		t.Fatal("entity should be in manager")
	}
}

func TestEntityManagerCreateUsesInstanceDependencies(t *testing.T) {
	var sequence atomic.Uint64
	manager := NewEntityManager(WithEntityIDGenerator(func() (uint64, error) {
		return sequence.Add(1), nil
	}))
	access := NewManagerAccess(manager)
	value, err := access.Create(&EntityCreateParam{IsCreate: true, Kind: testEntityKind})
	if err != nil {
		t.Fatal(err)
	}
	if value == nil || manager.Get(value.ID()) != value || value.UniqueID() != 1 {
		t.Fatalf("instance create did not publish generated entity: %+v", value)
	}
	id := value.ID()
	if err := access.Destroy(value, EntityDestroyReason(0), false); err != nil {
		t.Fatal(err)
	}
	if manager.Get(id) != nil {
		t.Fatal("instance destroy did not remove entity")
	}
}

func TestEntityManagerCreateInScopeLocksBeforePublication(t *testing.T) {
	manager := NewEntityManager()
	id := mustBuildTestEntityID(t, 99, testEntityCategory, testEntityKind)
	err := WithGuardScope("create_publish_order", func(scope *GuardScope) error {
		value, err := manager.CreateInScope(scope, &EntityCreateParam{
			Id:       id,
			UniqueID: 99,
			Category: testEntityCategory,
			Kind:     testEntityKind,
		})
		if err != nil {
			return err
		}
		if manager.Get(id) != value {
			t.Fatal("entity was not published")
		}
		locked := make(chan bool, 1)
		go func() {
			if value.GetMutex().TryLock() {
				value.GetMutex().Unlock()
				locked <- true
				return
			}
			locked <- false
		}()
		if <-locked {
			t.Fatal("published entity mutex was not held by the creation scope")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInitEntitySyncInstallsContentState(t *testing.T) {
	e := &factoryTestEntity{
		EntityBase:       NewEntityBase(301, testEntityCategory, true, testEntityKind),
		ComponentManager: NewComponentManager(),
		DaoManager:       NewDaoManager(),
	}
	bp := &EntityBuilderParam{Sync: EntitySyncBuilderParam{
		Enabled: true,
		Topic:   "factory.subject",
		PackerFactory: func(IThreadSafeEntity) SubjectSyncPacker {
			return SubjectSyncPackFunc{
				Snapshot: func(SyncProfile) (FrozenSyncPayload, error) {
					return CopyFrozenSyncPayload(7, []byte("snapshot")), nil
				},
			}
		},
	}}

	initEntitySync(e, nil, bp)
	state := e.Sync()
	if state == nil || state.SubjectID() != e.ID() || state.Namespace() != "factory.subject" || state.SubjectKind() != uint32(testEntityKind) {
		t.Fatalf("subject sync state mismatch: %+v", state)
	}
	updates, err := state.CaptureSnapshot(nil, SyncFullReasonResync)
	if err != nil || len(updates) != 1 || updates[0].Payload.Codec() != 7 {
		t.Fatalf("subject snapshot updates=%+v err=%v", updates, err)
	}
}

func TestBuildEntity_RemoteManagedRequiresRemoteInterface(t *testing.T) {
	_, err := BuildEntity(&EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testRemoteBadKind,
		UniqueID: 1,
	})
	if err == nil {
		t.Fatal("expected remote managed policy validation error")
	}
}

func TestBuildEntity_LifetimePolicy(t *testing.T) {
	e, err := BuildEntity(&EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testRuntimeRebuildKind,
		UniqueID: 2,
	})
	if err != nil {
		t.Fatalf("BuildEntity: %v", err)
	}
	if e.Base().Lifetime() != EntityLifetimeRuntimeRebuild {
		t.Fatalf("lifetime = %d, want %d", e.Base().Lifetime(), EntityLifetimeRuntimeRebuild)
	}
}

func TestNewEntity_Load(t *testing.T) {
	manager := NewEntityManager()

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

	e, err := manager.Create(param)
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
	manager := NewEntityManager(WithEntityIDGenerator(func() (uint64, error) { return 1, nil }))

	param := &EntityCreateParam{
		IsCreate: true,
		Category: EntityCategory(255), // not registered
		Kind:     EntityKind(255),
	}

	_, err := manager.Create(param)
	if err == nil {
		t.Fatal("expected error for unregistered type")
	}
}

func TestDestroyEntity(t *testing.T) {
	var seq atomic.Uint64
	manager := NewEntityManager(WithEntityIDGenerator(func() (uint64, error) { return seq.Add(1), nil }))

	param := &EntityCreateParam{
		IsCreate: true,
		Category: testEntityCategory,
		Kind:     testEntityKind,
	}
	e, _ := manager.Create(param)

	manager.Remove(e, testDestroyCommon, false)

	if manager.Get(e.ID()) != nil {
		t.Fatal("entity should be removed from manager")
	}
	if !e.IsRemoved() {
		t.Fatal("entity should be marked removed")
	}
}
