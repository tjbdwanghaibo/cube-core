package entity

import (
	"fmt"
	"github.com/tjbdwanghaibo/cube-core/checkpoint"
	"github.com/tjbdwanghaibo/cube-core/lock"
)

// IThreadSafeEntity is the full entity interface for the nest framework.
// Business entities embed EntityBase and implement this interface.
// Component/DAO wiring is handled by generated code, NOT by this interface.
type IThreadSafeEntity interface {
	IThreadSafeEntityBase

	AutoPersist() bool

	IsRemoved() bool
	SetRemoved()
	Touch() bool
	UnTouch()

	ClearBase()
	IsClear() bool

	Base() *EntityBase

	// OnInitFinish is called after all components are initialized.
	// Override in concrete entity to perform post-initialization logic.
	OnInitFinish(param *EntityCreateParam) error

	// OnDestroy is called when the entity is removed.
	// Called after all components' OnDestroy. Override in concrete entity for cleanup.
	OnDestroy(reason EntityDestroyReason)
}

// IThreadSafeEntityBase is the minimal entity identity interface.
type IThreadSafeEntityBase interface {
	ID() int64
	UniqueID() int64
	StorageID() int64
	GUId() int64
	GetEntityCategory() EntityCategory
	GetEntityKind() EntityKind
	GetMutex() lock.Mutex
}

// Getter retrieves entities by ID.
type Getter interface {
	Get(id int64, entityCategory EntityCategory) (IThreadSafeEntity, error)
	GetMany(ids []int64, idCategories []EntityCategory) ([]IThreadSafeEntity, error)
}

// IDirty tracks modification state.
type IDirty interface {
	Dirty() bool
	SelfClean()
}

// DaoInterface is the DAO contract for entity persistence.
type DaoInterface interface {
	Id() int64
	SetId(int64)
	DbName() string
	CollName() string
	Dirty() IDirty
	CleanDirty()
}

// DatabaseScopedDao optionally declares how its logical database name is
// resolved by the storage service.
type DatabaseScopedDao interface {
	DbScope() checkpoint.DatabaseScope
}

// Guardable is implemented by entities that expose DAO instances for
// persistence, remote save, and dirty-mask tracking.
type Guardable interface {
	RangeDao(func(DaoInterface))
}

// EntityCreateParam holds parameters for entity creation.
type EntityCreateParam struct {
	IsCreate bool
	Category EntityCategory
	Kind     EntityKind
	// Id is normalized to the full EntityID before the entity is built.
	// Explicit Id values must already be full EntityIDs. Use UniqueID only at
	// entity creation/loading boundaries that own raw unique-number generation.
	Id             int64
	UniqueID       int64
	OwnerId        int64
	OwnerCategory  EntityCategory
	BelongId       int64
	BelongCategory EntityCategory
	ExclusiveId    int64
	Lifetime       EntityLifetime
	Dao            map[string]DaoInterface
	Sync           *EntitySyncCreateParam
	Param          any
}

func (param *EntityCreateParam) NormalizeID(kind EntityKind) error {
	if param == nil {
		return fmt.Errorf("entity create param is nil")
	}
	if kind == EntityKindNone {
		kind = param.Kind
	}
	if kind == EntityKindNone {
		return fmt.Errorf("entity kind must not be none")
	}
	if param.Kind != EntityKindNone && param.Kind != kind {
		return fmt.Errorf("entity kind mismatch: param=%d builder=%d", param.Kind, kind)
	}
	category, err := ResolveEntityKindCategory(kind)
	if err != nil {
		return err
	}
	if param.Category != EntityCategoryNone && param.Category != category {
		return fmt.Errorf("entity category mismatch: param=%d kind=%d category=%d", param.Category, kind, category)
	}
	param.Kind = kind
	param.Category = category

	if param.Id != 0 {
		return param.setFullID(param.Id, kind)
	}
	if param.UniqueID != 0 {
		return param.setRawID(param.UniqueID, kind)
	}
	if !param.IsCreate {
		return fmt.Errorf("entity load requires full Id or UniqueID")
	}
	if GenerateID == nil {
		return fmt.Errorf("entity.GenerateID not set")
	}
	rawID, err := GenerateID()
	if err != nil {
		return fmt.Errorf("generate entity id: %w", err)
	}
	return param.setRawID(int64(rawID), kind)
}

func (param *EntityCreateParam) setRawID(uniqueID int64, kind EntityKind) error {
	id, err := BuildEntityID(uniqueID, kind)
	if err != nil {
		return err
	}
	meta := ResolveEntityID(id)
	param.UniqueID = uniqueID
	param.Id = id
	param.Category = meta.Category
	param.Kind = meta.Kind
	return nil
}

func (param *EntityCreateParam) setFullID(id int64, kind EntityKind) error {
	fullID, err := NormalizeFullID(id, kind)
	if err != nil {
		return err
	}
	meta := ResolveEntityID(fullID)
	param.Id = fullID
	param.UniqueID = meta.UniqueID
	param.Category = meta.Category
	param.Kind = meta.Kind
	return nil
}

func (param *EntityCreateParam) StorageID() int64 {
	if param == nil {
		return 0
	}
	if param.Id != 0 {
		return ResolveEntityID(param.Id).FullID
	}
	if param.UniqueID != 0 {
		id, err := BuildEntityID(param.UniqueID, param.Kind)
		if err != nil {
			return 0
		}
		return id
	}
	return 0
}

func (param *EntityCreateParam) FullID() int64 {
	if param == nil {
		return 0
	}
	return param.Id
}

// SendMsg is a hook set by the nest package to dispatch messages from entity context.
var SendMsg func(msg any)
