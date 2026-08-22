package nest

import (
	"errors"
	"sync"

	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/lock"
	"github.com/tjbdwanghaibo/cube-core/misc"
)

const entityLockGroupDispatchRetryMax = 4

type EntityLockGroupScope struct {
	groupID int64
	store   entityGroupStore
	prev    *EntityLockGroupScope
}

type entityGroupStore interface {
	GetGroupEntity(groupID, entityID int64) entity.IThreadSafeEntity
	GetGroupEntities(groupID int64) []entity.IThreadSafeEntity
	UpdateEntityGroup(entity.IThreadSafeEntity, int64) error
}

func groupStoreOf(getter entity.Getter) entityGroupStore {
	store, _ := getter.(entityGroupStore)
	return store
}

var entityLockGroupScopes sync.Map // map[int64]*EntityLockGroupScope

func CurrentEntityLockGroup() *EntityLockGroupScope {
	if value, ok := entityLockGroupScopes.Load(misc.GoID()); ok {
		if scope, ok := value.(*EntityLockGroupScope); ok {
			return scope
		}
	}
	return nil
}

func (s *EntityLockGroupScope) GroupID() int64 {
	if s == nil {
		return 0
	}
	return s.groupID
}

func (s *EntityLockGroupScope) Get(entityID int64) entity.IThreadSafeEntity {
	if s == nil || s.groupID == 0 || entityID == 0 || s.store == nil {
		return nil
	}
	ent := s.store.GetGroupEntity(s.groupID, entityID)
	if ent == nil || ent.Base() == nil || ent.Base().GroupLockID() != s.groupID {
		return nil
	}
	return ent
}

func (s *EntityLockGroupScope) Range(fn func(entity.IThreadSafeEntity) bool) {
	if s == nil || s.groupID == 0 || fn == nil || s.store == nil {
		return
	}
	for _, ent := range s.store.GetGroupEntities(s.groupID) {
		if ent == nil || ent.Base() == nil || ent.Base().GroupLockID() != s.groupID {
			continue
		}
		if !fn(ent) {
			return
		}
	}
}

func GroupEntityAs[T entity.IThreadSafeEntity](scope *EntityLockGroupScope, entityID int64) (T, bool) {
	var zero T
	ent := scope.Get(entityID)
	if ent == nil {
		return zero, false
	}
	typed, ok := ent.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}

func pushEntityLockGroupScope(groupID int64, store entityGroupStore) func() {
	if groupID == 0 {
		return func() {}
	}
	prev := CurrentEntityLockGroup()
	scope := &EntityLockGroupScope{groupID: groupID, store: store, prev: prev}
	entityLockGroupScopes.Store(misc.GoID(), scope)
	return func() {
		cur := CurrentEntityLockGroup()
		if cur != scope {
			entityLockGroupScopes.Delete(misc.GoID())
			return
		}
		if scope.prev != nil {
			entityLockGroupScopes.Store(misc.GoID(), scope.prev)
		} else {
			entityLockGroupScopes.Delete(misc.GoID())
		}
		scope.prev = nil
	}
}

type entityLockGroupLockManager struct {
	mu    sync.Mutex
	locks map[int64]*entityLockGroupLockEntry
}

type entityLockGroupLockEntry struct {
	groupID int64
	mu      lock.Mutex
	refs    int
}

func newEntityLockGroupLockManager() *entityLockGroupLockManager {
	return &entityLockGroupLockManager{locks: make(map[int64]*entityLockGroupLockEntry)}

}

func (m *entityLockGroupLockManager) acquire(groupID int64) (*entityLockGroupLockEntry, bool) {
	if m == nil || groupID == 0 {
		return nil, false
	}
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[int64]*entityLockGroupLockEntry)
	}
	entry := m.locks[groupID]
	if entry == nil {
		entry = &entityLockGroupLockEntry{groupID: groupID, mu: lock.NewReentrantMutex(-groupID)}
		m.locks[groupID] = entry
	}
	entry.refs++
	m.mu.Unlock()
	if entry.mu.TryLock() {
		return entry, true
	}
	m.releaseRef(entry)
	return nil, false
}

func (m *entityLockGroupLockManager) release(entry *entityLockGroupLockEntry) {
	if m == nil || entry == nil {
		return
	}
	entry.mu.Unlock()
	m.releaseRef(entry)
}

func (m *entityLockGroupLockManager) releaseRef(entry *entityLockGroupLockEntry) {
	m.mu.Lock()
	entry.refs--
	if entry.refs == 0 && m.locks[entry.groupID] == entry {
		delete(m.locks, entry.groupID)
	}
	m.mu.Unlock()
}

func resolveDispatchGroupID(lockEs []entity.IThreadSafeEntity) (int64, error) {
	return resolveDispatchGroupIDFromSnapshots(captureDispatchGroupSnapshots(lockEs))
}

type dispatchGroupSnapshot struct {
	ent      entity.IThreadSafeEntity
	groupID  int64
	epoch    uint64
	state    entity.EntityGroupTransitionState
	targetID int64
}

func captureDispatchGroupSnapshots(lockEs []entity.IThreadSafeEntity) []dispatchGroupSnapshot {
	ret := make([]dispatchGroupSnapshot, 0, len(lockEs))
	for _, ent := range lockEs {
		if ent == nil || ent.Base() == nil {
			continue
		}
		base := ent.Base()
		ret = append(ret, dispatchGroupSnapshot{
			ent:      ent,
			groupID:  base.GroupLockID(),
			epoch:    base.GroupEpoch(),
			state:    base.GroupTransitionState(),
			targetID: base.GroupTransitionTargetID(),
		})
	}
	return ret
}

func resolveDispatchGroupIDFromSnapshots(snapshots []dispatchGroupSnapshot) (int64, error) {
	var groupID int64
	for _, snap := range snapshots {
		next := snap.groupID
		if next == 0 {
			continue
		}
		if groupID == 0 {
			groupID = next
			continue
		}
		if groupID != next {
			return 0, ErrEntityLockGroupMix
		}
	}
	return groupID, nil
}

func validateDispatchGroupSnapshots(snapshots []dispatchGroupSnapshot) error {
	for _, snap := range snapshots {
		if snap.ent == nil || snap.ent.Base() == nil {
			return ErrEntityLockGroupChanged
		}
		base := snap.ent.Base()
		if base.GroupTransitionPending() {
			return ErrEntityGroupTransitionPending
		}
		if base.GroupLockID() != snap.groupID ||
			base.GroupEpoch() != snap.epoch ||
			base.GroupTransitionState() != snap.state ||
			base.GroupTransitionTargetID() != snap.targetID {
			return ErrEntityLockGroupChanged
		}
	}
	return nil
}

func lockDispatchEntitiesForHandler(locks *entityLockGroupLockManager, guard *entity.EntityGuard, lockEs []entity.IThreadSafeEntity) ([]entity.IThreadSafeEntity, func(), error) {
	return lockDispatchEntitiesForHandlerWithStore(locks, guard, lockEs, nil)
}

func lockDispatchEntitiesForHandlerWithStore(locks *entityLockGroupLockManager, guard *entity.EntityGuard, lockEs []entity.IThreadSafeEntity, store entityGroupStore) ([]entity.IThreadSafeEntity, func(), error) {
	var lastErr error
	for attempt := 0; attempt < entityLockGroupDispatchRetryMax; attempt++ {
		snapshots := captureDispatchGroupSnapshots(lockEs)
		groupID, err := resolveDispatchGroupIDFromSnapshots(snapshots)
		if err != nil {
			return nil, nil, err
		}
		acquired, releaseLocks, err := lockDispatchEntitiesWithGroup(locks, guard, lockEs, groupID, store)
		if err != nil {
			return nil, nil, err
		}
		if err := validateDispatchGroupSnapshots(snapshots); err != nil {
			releaseLocks()
			if errors.Is(err, ErrEntityGroupTransitionPending) {
				return nil, nil, err
			}
			lastErr = err
			continue
		}
		return acquired, releaseLocks, nil
	}
	if lastErr == nil {
		lastErr = ErrEntityLockGroupChanged
	}
	return nil, nil, lastErr
}

func lockDispatchEntitiesWithGroup(locks *entityLockGroupLockManager, guard *entity.EntityGuard, lockEs []entity.IThreadSafeEntity, groupID int64, store entityGroupStore) ([]entity.IThreadSafeEntity, func(), error) {
	if groupID == 0 {
		acquired, err := lockDispatchEntities(guard, lockEs)
		if err != nil {
			return nil, nil, err
		}
		return acquired, func() {
			releaseDispatchLocks(guard, acquired)
		}, nil
	}
	groupEntry, ok := locks.acquire(groupID)
	if !ok {
		return nil, nil, ErrLockTimeout
	}
	releaseScope := pushEntityLockGroupScope(groupID, store)
	acquired, err := tryLockDispatchEntities(guard, lockEs)
	if err != nil {
		releaseScope()
		locks.release(groupEntry)
		return nil, nil, err
	}
	return acquired, func() {
		releaseDispatchLocks(guard, acquired)
		releaseScope()
		locks.release(groupEntry)
	}, nil
}

func tryLockDispatchEntities(guard *entity.EntityGuard, lockEs []entity.IThreadSafeEntity) ([]entity.IThreadSafeEntity, error) {
	if guard == nil {
		return nil, ErrLockTimeout
	}
	acquired := make([]entity.IThreadSafeEntity, 0, len(lockEs))
	for _, ent := range lockEs {
		if ent == nil {
			continue
		}
		if _, exists := guard.Entities()[ent.GUId()]; exists {
			continue
		}
		if !tryRequireDispatchEntity(guard, ent) {
			releaseDispatchEntities(guard, acquired)
			return nil, ErrLockTimeout
		}
		acquired = append(acquired, ent)
	}
	return acquired, nil
}
