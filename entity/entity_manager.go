package entity

import (
	flog "github.com/tjbdwanghaibo/cube-core/log"
	"github.com/tjbdwanghaibo/cube-core/misc"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const defaultBucketCnt = 64

// EntityManager is the central registry for all entities.
// Uses sharded buckets for high-concurrency access with hundreds of thousands of entities.
type EntityManager struct {
	entities *misc.BucketHolder[int64, IThreadSafeEntity]
	addMu    sync.Mutex
	groupMu  sync.RWMutex
	groups   map[int64]map[int64]IThreadSafeEntity
}

var (
	ErrEntityNil        = errors.New("entity manager: nil entity")
	ErrEntityRemoved    = errors.New("entity manager: entity removed")
	ErrEntityExists     = errors.New("entity manager: entity already exists")
	ErrEntityNotManaged = errors.New("entity manager: entity not managed")
)

// NewEntityManager creates an EntityManager with default bucket count.
func NewEntityManager() *EntityManager {
	return NewEntityManagerWithBuckets(defaultBucketCnt)
}

// NewEntityManagerWithBuckets creates an EntityManager with specified bucket count.
func NewEntityManagerWithBuckets(bucketCnt int) *EntityManager {
	return &EntityManager{
		entities: misc.NewBucketHolder[int64, IThreadSafeEntity](bucketCnt, nil, false),
		groups:   make(map[int64]map[int64]IThreadSafeEntity),
	}
}

// Add registers an entity. Panics on duplicate ID.
func (m *EntityManager) Add(e IThreadSafeEntity) {
	if err := m.TryAdd(e); err != nil {
		panic(fmt.Sprintf("entity manager: add failed: %v", err))
	}
}

// TryAdd registers an entity and reports duplicate IDs as an error.
func (m *EntityManager) TryAdd(e IThreadSafeEntity) error {
	if e == nil {
		return ErrEntityNil
	}
	id := e.ID()
	m.addMu.Lock()
	defer m.addMu.Unlock()
	existing := m.entities.Get(id)
	if existing != nil {
		return fmt.Errorf("%w: %d", ErrEntityExists, id)
	}
	m.entities.Add(id, e)
	m.addGroupIndexLockedByManager(e)
	return nil
}

var (
	onEntityRemoveFromDBMu    sync.RWMutex
	nextRemoveFromDBHookID    atomic.Uint64
	onEntityRemoveFromDBHooks []entityRemoveFromDBHook
)

type entityRemoveFromDBHook struct {
	id uint64
	fn func(IThreadSafeEntity)
}

func RegisterOnEntityRemoveFromDB(hook func(IThreadSafeEntity)) func() {
	if hook == nil {
		return func() {}
	}
	id := nextRemoveFromDBHookID.Add(1)
	onEntityRemoveFromDBMu.Lock()
	onEntityRemoveFromDBHooks = append(onEntityRemoveFromDBHooks, entityRemoveFromDBHook{id: id, fn: hook})
	onEntityRemoveFromDBMu.Unlock()
	return func() {
		onEntityRemoveFromDBMu.Lock()
		defer onEntityRemoveFromDBMu.Unlock()
		for i, item := range onEntityRemoveFromDBHooks {
			if item.id == id {
				onEntityRemoveFromDBHooks = append(onEntityRemoveFromDBHooks[:i], onEntityRemoveFromDBHooks[i+1:]...)
				return
			}
		}
	}
}

func runOnEntityRemoveFromDB(e IThreadSafeEntity) {
	onEntityRemoveFromDBMu.RLock()
	hooks := append([]entityRemoveFromDBHook{}, onEntityRemoveFromDBHooks...)
	onEntityRemoveFromDBMu.RUnlock()
	for _, hook := range hooks {
		hook.fn(e)
	}
}

// Remove destroys, marks removed, removes from bucket, and directly cleans the entity.
// If deleteFromDB is true, triggers OnEntityRemoveFromDB to persist the deletion.
func (m *EntityManager) Remove(e IThreadSafeEntity, reason EntityDestroyReason, deleteFromDB bool) {
	_ = m.RemoveAfter(e, reason, deleteFromDB, nil)
}

// RemoveAfter runs beforeDestroy while holding the entity lock, then removes
// the entity from memory. It is used by hot/cold eviction to synchronously
// persist dirty state before the entity becomes unreachable.
func (m *EntityManager) RemoveAfter(e IThreadSafeEntity, reason EntityDestroyReason, deleteFromDB bool, beforeDestroy func(IThreadSafeEntity) error) error {
	if m == nil || e == nil || e.Base() == nil {
		return ErrEntityNil
	}
	if !e.Touch() {
		return ErrEntityRemoved
	}
	mu := e.GetMutex()
	if mu == nil {
		e.UnTouch()
		return ErrEntityNil
	}
	mu.Lock()
	if e.IsRemoved() || e.IsClear() {
		mu.Unlock()
		e.UnTouch()
		return ErrEntityRemoved
	}
	if beforeDestroy != nil {
		if err := beforeDestroy(e); err != nil {
			mu.Unlock()
			e.UnTouch()
			flog.Warn("entity manager: before destroy failed", "id", e.ID(), "category", e.GetEntityCategory(), "kind", e.GetEntityKind(), "reason", reason, "err", err)
			return err
		}
	}
	id := e.ID()
	category := e.GetEntityCategory()
	kind := e.GetEntityKind()

	// Prevent new readers and remove the entity from the global index while the
	// entity mutex is held. Lifecycle callbacks run after unlock so they can
	// safely coordinate with other entities through EntityGuard lock ordering.
	e.SetRemoved()
	m.entities.Del(e.ID())
	m.removeGroupIndex(e)
	mu.Unlock()

	e.Base().DestroyAll(reason)

	e.OnDestroy(reason)

	if deleteFromDB {
		runOnEntityRemoveFromDB(e)
	}

	e.UnTouch()
	e.ClearBase()
	flog.Debug("entity manager: removed", "id", id, "category", category, "kind", kind, "reason", reason, "delete_db", deleteFromDB)
	return nil
}

// Get returns the entity with the given ID, or nil if not found.
func (m *EntityManager) Get(id int64) IThreadSafeEntity {
	return m.entities.Get(id)
}

// GetWithCategory returns the entity with the given ID and category check.
// Returns nil if not found or type mismatch.
func (m *EntityManager) GetWithCategory(id int64, category EntityCategory) IThreadSafeEntity {
	e := m.entities.Get(id)
	if e != nil && e.GetEntityCategory() != category {
		return nil
	}
	return e
}

// GetMany returns entities matching the given IDs.
// Missing entities are skipped.
func (m *EntityManager) GetMany(ids []int64) []IThreadSafeEntity {
	result := make([]IThreadSafeEntity, 0, len(ids))
	for _, id := range ids {
		if e := m.entities.Get(id); e != nil {
			result = append(result, e)
		}
	}
	return result
}

// Exists checks if an entity with the given ID is in memory.
func (m *EntityManager) Exists(id int64) bool {
	return m.entities.Get(id) != nil
}

// Len returns the total number of managed entities.
func (m *EntityManager) Len() int {
	return m.entities.Count()
}

// Range iterates all entities across all buckets. Return false from fn to stop early.
func (m *EntityManager) Range(fn func(IThreadSafeEntity) bool) {
	m.entities.RangeAll(func(_ int64, e IThreadSafeEntity) bool {
		return fn(e)
	})
}

// RangeByCategory iterates entities of a specific category.
func (m *EntityManager) RangeByCategory(category EntityCategory, fn func(IThreadSafeEntity) bool) {
	m.entities.RangeAll(func(_ int64, e IThreadSafeEntity) bool {
		if e.GetEntityCategory() == category {
			return fn(e)
		}
		return true
	})
}

// CountByCategory returns the number of entities of a specific category.
func (m *EntityManager) CountByCategory(category EntityCategory) int {
	count := 0
	m.entities.RangeAll(func(_ int64, e IThreadSafeEntity) bool {
		if e.GetEntityCategory() == category {
			count++
		}
		return true
	})
	return count
}

func (m *EntityManager) addGroupIndexLockedByManager(e IThreadSafeEntity) {
	if m == nil || e == nil || e.Base() == nil {
		return
	}
	groupID := e.Base().GroupLockID()
	if groupID == 0 {
		return
	}
	m.groupMu.Lock()
	defer m.groupMu.Unlock()
	m.addGroupIndexLocked(groupID, e)
}

func (m *EntityManager) addGroupIndexLocked(groupID int64, e IThreadSafeEntity) {
	if groupID == 0 || e == nil {
		return
	}
	bucket := m.groups[groupID]
	if bucket == nil {
		bucket = make(map[int64]IThreadSafeEntity)
		m.groups[groupID] = bucket
	}
	bucket[e.ID()] = e
}

func (m *EntityManager) removeGroupIndex(e IThreadSafeEntity) {
	if m == nil || e == nil || e.Base() == nil {
		return
	}
	groupID := e.Base().GroupLockID()
	if groupID == 0 {
		return
	}
	m.groupMu.Lock()
	defer m.groupMu.Unlock()
	m.removeGroupIndexLocked(groupID, e.ID())
}

func (m *EntityManager) removeGroupIndexLocked(groupID int64, entityID int64) {
	if groupID == 0 {
		return
	}
	bucket := m.groups[groupID]
	if bucket == nil {
		return
	}
	delete(bucket, entityID)
	if len(bucket) == 0 {
		delete(m.groups, groupID)
	}
}

// UpdateEntityGroup updates EntityBase group state and the manager's derived
// group membership index. Callers are responsible for holding the correct
// entity/group serialization lock.
func (m *EntityManager) UpdateEntityGroup(e IThreadSafeEntity, groupID int64) error {
	if m == nil || e == nil || e.Base() == nil {
		return ErrEntityNil
	}
	if m.entities.Get(e.ID()) != e {
		return ErrEntityNotManaged
	}
	oldGroupID := e.Base().GroupLockID()
	if oldGroupID == groupID {
		return nil
	}
	m.groupMu.Lock()
	defer m.groupMu.Unlock()
	m.removeGroupIndexLocked(oldGroupID, e.ID())
	if groupID != 0 {
		m.addGroupIndexLocked(groupID, e)
	}
	e.Base().setGroupLockID(groupID)
	return nil
}

func (m *EntityManager) GetGroupEntity(groupID int64, entityID int64) IThreadSafeEntity {
	if m == nil || groupID == 0 || entityID == 0 {
		return nil
	}
	m.groupMu.RLock()
	defer m.groupMu.RUnlock()
	bucket := m.groups[groupID]
	if bucket == nil {
		return nil
	}
	return bucket[entityID]
}

func (m *EntityManager) GetGroupEntities(groupID int64) []IThreadSafeEntity {
	if m == nil || groupID == 0 {
		return nil
	}
	m.groupMu.RLock()
	defer m.groupMu.RUnlock()
	bucket := m.groups[groupID]
	if len(bucket) == 0 {
		return nil
	}
	ret := make([]IThreadSafeEntity, 0, len(bucket))
	for _, e := range bucket {
		ret = append(ret, e)
	}
	return ret
}

func (m *EntityManager) RangeGroupEntities(groupID int64, fn func(IThreadSafeEntity) bool) {
	if m == nil || groupID == 0 || fn == nil {
		return
	}
	for _, e := range m.GetGroupEntities(groupID) {
		if !fn(e) {
			return
		}
	}
}
