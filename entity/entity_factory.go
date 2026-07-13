package entity

import (
	flog "github.com/tjbdwanghaibo/cube-core/log"
	"fmt"
	"sync"
)

// EntityBuilderFunc creates an entity from params.
// The returned entity must satisfy IThreadSafeEntity.
type EntityBuilderFunc func(param *EntityCreateParam) (IThreadSafeEntity, error)

// DaoBuilderFunc creates a new DAO instance.
type DaoBuilderFunc func() DaoInterface

// EntityBuilderParam holds the builder configuration for a concrete entity kind.
type EntityBuilderParam struct {
	Category     EntityCategory    // ownership/access category
	Kind         EntityKind        // concrete entity definition
	Builder      EntityBuilderFunc // entity constructor (generated NewXxx)
	DaoBuilders  []DaoBuilderFunc  // DAO factories (one per DAO in entity)
	LoadPriority int               // lower = load first
	NoPersist    bool              // skip auto-persist
	RemotePolicy RemotePolicy      // explicit cross-server policy
	Lifetime     EntityLifetime    // memory/persistence lifecycle policy
	Sync         EntitySyncBuilderParam
}

// --- Global entity factory registry ---

var (
	factoryMu          sync.RWMutex
	factoryByKind      = make(map[EntityKind]*EntityBuilderParam)
	kindCategoryByKind = make(map[EntityKind]EntityCategory)
	kindPolicyByKind   = make(map[EntityKind]RemotePolicy)
)

// RegisterEntityBuilder registers a builder for a concrete entity kind.
// Call from the service bootstrap path. Panics on duplicate.
func RegisterEntityBuilder(param *EntityBuilderParam) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if param.Kind == EntityKindNone {
		panic("entity builder kind must not be none")
	}
	normalizeBuilderPolicy(param)
	if err := registerEntityKindDefinitionLocked(EntityKindDef{
		Kind:         param.Kind,
		Category:     param.Category,
		RemotePolicy: param.RemotePolicy,
	}); err != nil {
		panic(err)
	}
	if _, exists := factoryByKind[param.Kind]; exists {
		panic(fmt.Sprintf("duplicate entity builder for kind %d", param.Kind))
	}
	factoryByKind[param.Kind] = param
}

func RegisterEntityKindCategory(kind EntityKind, category EntityCategory) error {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	return registerEntityKindCategoryLocked(kind, category)
}

func MustRegisterEntityKindCategory(kind EntityKind, category EntityCategory) {
	if err := RegisterEntityKindCategory(kind, category); err != nil {
		panic(err)
	}
}

func RegisterEntityKindCategories(defs ...EntityKindCategory) error {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	for _, def := range defs {
		if err := registerEntityKindCategoryLocked(def.Kind, def.Category); err != nil {
			return err
		}
	}
	return nil
}

func MustRegisterEntityKindCategories(defs ...EntityKindCategory) {
	if err := RegisterEntityKindCategories(defs...); err != nil {
		panic(err)
	}
}

func RegisterEntityKindDefs(defs ...EntityKindDef) error {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	for _, def := range defs {
		if err := registerEntityKindDefinitionLocked(def); err != nil {
			return err
		}
	}
	return nil
}

func MustRegisterEntityKindDefs(defs ...EntityKindDef) {
	if err := RegisterEntityKindDefs(defs...); err != nil {
		panic(err)
	}
}

func EntityCategoryOfKind(kind EntityKind) (EntityCategory, bool) {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	category, ok := kindCategoryByKind[kind]
	return category, ok
}

func ResolveEntityKindCategory(kind EntityKind) (EntityCategory, error) {
	if kind == EntityKindNone {
		return EntityCategoryNone, fmt.Errorf("entity kind must not be none")
	}
	if uint64(kind) > EntityKindMask {
		return EntityCategoryNone, ErrInvalidEntityKind
	}
	category, ok := EntityCategoryOfKind(kind)
	if !ok {
		return EntityCategoryNone, fmt.Errorf("%w: kind %d category is not registered", ErrInvalidEntityID, kind)
	}
	return category, nil
}

func MustEntityCategoryOfKind(kind EntityKind) EntityCategory {
	category, err := ResolveEntityKindCategory(kind)
	if err != nil {
		panic(err)
	}
	return category
}

func registerEntityKindCategoryLocked(kind EntityKind, category EntityCategory) error {
	return registerEntityKindDefinitionLocked(EntityKindDef{Kind: kind, Category: category})
}

func registerEntityKindDefinitionLocked(def EntityKindDef) error {
	kind := def.Kind
	category := def.Category
	if kind == EntityKindNone {
		return fmt.Errorf("entity kind must not be none")
	}
	if uint64(kind) > EntityKindMask {
		return ErrInvalidEntityKind
	}
	if category == EntityCategoryNone {
		return fmt.Errorf("entity category must not be none for kind %d", kind)
	}
	if uint64(category) > EntityCategoryMask {
		return ErrInvalidCategory
	}
	if existing, ok := kindCategoryByKind[kind]; ok {
		if existing != category {
			return fmt.Errorf("entity kind %d category mismatch: registered=%d new=%d", kind, existing, category)
		}
	} else {
		kindCategoryByKind[kind] = category
	}
	if existing, ok := kindPolicyByKind[kind]; ok {
		switch {
		case existing == def.RemotePolicy:
			return nil
		case existing == RemotePolicyNone && def.RemotePolicy != RemotePolicyNone:
			kindPolicyByKind[kind] = def.RemotePolicy
			return nil
		case existing != RemotePolicyNone && def.RemotePolicy == RemotePolicyNone:
			return nil
		default:
			return fmt.Errorf("entity kind %d remote policy mismatch: registered=%d new=%d", kind, existing, def.RemotePolicy)
		}
	}
	kindPolicyByKind[kind] = def.RemotePolicy
	return nil
}

// GetEntityBuilderParam retrieves the registered builder for an entity kind.
func GetEntityBuilderParam(kind EntityKind) *EntityBuilderParam {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	return factoryByKind[kind]
}

// GetAllEntityBuilders returns all registered builders.
func GetAllEntityBuilders() []*EntityBuilderParam {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	result := make([]*EntityBuilderParam, 0, len(factoryByKind))
	for _, p := range factoryByKind {
		result = append(result, p)
	}
	return result
}

func ResetEntityRegistryForTest() {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	factoryByKind = make(map[EntityKind]*EntityBuilderParam)
	kindCategoryByKind = make(map[EntityKind]EntityCategory)
	kindPolicyByKind = make(map[EntityKind]RemotePolicy)
}

func GetEntityKindRemotePolicy(kind EntityKind) RemotePolicy {
	factoryMu.RLock()
	policy, ok := kindPolicyByKind[kind]
	factoryMu.RUnlock()
	if ok {
		return policy
	}
	bp := GetEntityBuilderParam(kind)
	if bp == nil {
		return RemotePolicyNone
	}
	return bp.RemotePolicy
}

func IsEntityKindRemoteCapable(kind EntityKind) bool {
	return GetEntityKindRemotePolicy(kind).RemoteCapable()
}

func IsEntityKindRemoteManaged(kind EntityKind) bool {
	return GetEntityKindRemotePolicy(kind).RemoteManaged()
}

// --- ID generation hook ---

// GenerateID is the global ID generator function.
// Must be set by the application layer before creating entities.
var GenerateID func() (uint64, error)

// --- Global entity manager ---

// Mgr is the global entity manager instance.
// Must be set by the application layer before creating entities.
var Mgr *EntityManager

// --- Entity creation ---

func resolveEntityBuilder(param *EntityCreateParam) (*EntityBuilderParam, error) {
	if param == nil {
		return nil, fmt.Errorf("entity create param is nil")
	}
	if param.Kind == EntityKindNone {
		return nil, fmt.Errorf("entity kind must not be none")
	}
	bp := GetEntityBuilderParam(param.Kind)
	if bp == nil {
		return nil, fmt.Errorf("no entity builder registered for category %d kind %d", param.Category, param.Kind)
	}
	if param.Category == EntityCategoryNone {
		param.Category = bp.Category
	} else if bp.Category != EntityCategoryNone && param.Category != bp.Category {
		return nil, fmt.Errorf("entity builder kind %d category mismatch: param=%d builder=%d", param.Kind, param.Category, bp.Category)
	}
	return bp, nil
}

// BuildEntity creates an entity using the registered builder without adding it
// to the global manager or acquiring its guard lock. It is intended for
// loaders that need to finish construction before deciding how to publish the
// entity into memory.
func BuildEntity(param *EntityCreateParam) (IThreadSafeEntity, error) {
	bp, err := resolveEntityBuilder(param)
	if err != nil {
		return nil, err
	}

	if err := param.NormalizeID(param.Kind); err != nil {
		return nil, err
	}

	// Create DAOs for new entities
	if param.IsCreate && param.Dao == nil {
		param.Dao = make(map[string]DaoInterface)
		for _, daoBuilder := range bp.DaoBuilders {
			dao := daoBuilder()
			dao.SetId(param.StorageID())
			param.Dao[dao.CollName()] = dao
		}
	}

	// Build entity (components are created and InitAll is called inside Builder)
	e, err := bp.Builder(param)
	if err != nil {
		return nil, fmt.Errorf("build entity category %d: %w", param.Category, err)
	}
	if err := validateBuiltEntityPolicy(bp, e); err != nil {
		return nil, err
	}
	if e.Base() != nil {
		e.Base().SetLifetime(resolveEntityLifetime(param, bp))
	}
	initEntitySync(e, param, bp)

	// Entity-level post-initialization (after all components are ready)
	if err := e.OnInitFinish(param); err != nil {
		return nil, fmt.Errorf("entity OnInitFinish category %d: %w", param.Category, err)
	}
	return e, nil
}

func normalizeBuilderPolicy(param *EntityBuilderParam) {
	if param == nil {
		return
	}
	if param.Lifetime == EntityLifetimeDefault {
		param.Lifetime = DefaultEntityLifetime(param.NoPersist, param.RemotePolicy)
	}
	if err := ValidateEntityPolicy(param.Kind, param.NoPersist, param.RemotePolicy, param.Lifetime); err != nil {
		panic(err)
	}
}

func (param *EntityBuilderParam) IsRemoteCapable() bool {
	return param != nil && param.RemotePolicy.RemoteCapable()
}

func validateBuiltEntityPolicy(bp *EntityBuilderParam, e IThreadSafeEntity) error {
	if bp == nil || e == nil {
		return nil
	}
	if bp.RemotePolicy.RemoteManaged() {
		if _, ok := e.(IThreadSafeRemoteEntity); !ok {
			return fmt.Errorf("entity kind %d remote=managed but does not implement IThreadSafeRemoteEntity", bp.Kind)
		}
	}
	return nil
}

func resolveEntityLifetime(param *EntityCreateParam, bp *EntityBuilderParam) EntityLifetime {
	if param != nil && param.Lifetime != EntityLifetimeDefault {
		return param.Lifetime
	}
	if bp != nil && bp.Lifetime != EntityLifetimeDefault {
		return bp.Lifetime
	}
	noPersist := false
	remotePolicy := RemotePolicyNone
	if bp != nil {
		noPersist = bp.NoPersist
		remotePolicy = bp.RemotePolicy
	}
	return DefaultEntityLifetime(noPersist, remotePolicy)
}

// CreateEntity creates and publishes an entity in a short-lived guard scope.
// The entity lock is released before this function returns; subsequent mutation
// should go through nest/entity handlers.
func CreateEntity(param *EntityCreateParam) (IThreadSafeEntity, error) {
	var ret IThreadSafeEntity
	err := WithGuardScope("entity_create", func(scope *GuardScope) error {
		e, err := NewEntityInScope(scope, param)
		if err != nil {
			return err
		}
		ret = e
		return nil
	})
	return ret, err
}

func NewEntityInScope(scope *GuardScope, param *EntityCreateParam) (IThreadSafeEntity, error) {
	if scope == nil || scope.guard == nil {
		return nil, fmt.Errorf("entity guard scope is required")
	}
	e, err := NewEntity(param)
	if err != nil {
		return nil, err
	}
	if !scope.guard.RequireEntity(e) {
		Mgr.Remove(e, EntityDestroyReason(0), false)
		return nil, fmt.Errorf("entity guard scope lock failed: %d", e.ID())
	}
	return e, nil
}

// NewEntity creates an entity using the registered builder and publishes it to
// the global manager. For new entities (IsCreate=true), it generates a new ID
// and creates DAOs. For loaded entities (IsCreate=false), DAOs should be
// pre-populated in param.Dao.
func NewEntity(param *EntityCreateParam) (IThreadSafeEntity, error) {
	e, err := BuildEntity(param)
	if err != nil {
		return nil, err
	}

	if err := Mgr.TryAdd(e); err != nil {
		return nil, err
	}
	lifetime := EntityLifetimeDefault
	if base := e.Base(); base != nil {
		lifetime = base.Lifetime()
	}
	flog.Debug("entity: created", "id", e.ID(), "category", e.GetEntityCategory(), "kind", e.GetEntityKind(), "lifetime", lifetime)

	return e, nil
}

// DestroyEntity removes an entity from the manager and cleans up.
// If deleteFromDB is true, a delete operation is pushed to the checkpoint journal.
func DestroyEntity(e IThreadSafeEntity, reason EntityDestroyReason, deleteFromDB bool) {
	if e != nil {
		flog.Debug("entity: destroy", "id", e.ID(), "category", e.GetEntityCategory(), "kind", e.GetEntityKind(), "reason", reason, "delete_db", deleteFromDB)
	}
	Mgr.Remove(e, reason, deleteFromDB)
}

func initEntitySync(e IThreadSafeEntity, param *EntityCreateParam, bp *EntityBuilderParam) {
	if e == nil || e.Base() == nil {
		return
	}
	if param != nil && param.Sync != nil {
		if !param.Sync.Enabled {
			e.Base().SetSyncState(nil)
			return
		}
		syncParam := *param.Sync
		if syncParam.EntityID == 0 {
			syncParam.EntityID = e.ID()
		}
		if syncParam.Packer == nil && bp != nil && bp.Sync.PackerFactory != nil {
			syncParam.Packer = bp.Sync.PackerFactory(e)
		}
		e.Base().EnableSync(syncParam)
		return
	}
	if bp == nil || !bp.Sync.Enabled {
		return
	}
	syncParam := bp.Sync.toCreateParam(e)
	e.Base().EnableSync(syncParam)
}
