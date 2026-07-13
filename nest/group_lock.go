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
	prev    *EntityLockGroupScope
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
	if s == nil || s.groupID == 0 || entityID == 0 || entity.Mgr == nil {
		return nil
	}
	ent := entity.Mgr.GetGroupEntity(s.groupID, entityID)
	if ent == nil || ent.Base() == nil || ent.Base().GroupLockID() != s.groupID {
		return nil
	}
	return ent
}

func (s *EntityLockGroupScope) Range(fn func(entity.IThreadSafeEntity) bool) {
	if s == nil || s.groupID == 0 || fn == nil || entity.Mgr == nil {
		return
	}
	for _, ent := range entity.Mgr.GetGroupEntities(s.groupID) {
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

func pushEntityLockGroupScope(groupID int64) func() {
	if groupID == 0 {
		return func() {}
	}
	prev := CurrentEntityLockGroup()
	scope := &EntityLockGroupScope{groupID: groupID, prev: prev}
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
	locks sync.Map // map[int64]lock.Mutex
}

var defaultEntityLockGroupLocks entityLockGroupLockManager

func entityLockGroupMutex(groupID int64) lock.Mutex {
	if groupID == 0 {
		return nil
	}
	if value, ok := defaultEntityLockGroupLocks.locks.Load(groupID); ok {
		if mu, ok := value.(lock.Mutex); ok {
			return mu
		}
	}
	mu := lock.NewReentrantMutex(-groupID)
	actual, _ := defaultEntityLockGroupLocks.locks.LoadOrStore(groupID, mu)
	if ret, ok := actual.(lock.Mutex); ok {
		return ret
	}
	return mu
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

func lockDispatchEntitiesForHandler(guard *entity.EntityGuard, lockEs []entity.IThreadSafeEntity) ([]entity.IThreadSafeEntity, func(), error) {
	var lastErr error
	for attempt := 0; attempt < entityLockGroupDispatchRetryMax; attempt++ {
		snapshots := captureDispatchGroupSnapshots(lockEs)
		groupID, err := resolveDispatchGroupIDFromSnapshots(snapshots)
		if err != nil {
			return nil, nil, err
		}
		acquired, releaseLocks, err := lockDispatchEntitiesWithGroup(guard, lockEs, groupID)
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

func lockDispatchEntitiesWithGroup(guard *entity.EntityGuard, lockEs []entity.IThreadSafeEntity, groupID int64) ([]entity.IThreadSafeEntity, func(), error) {
	if groupID == 0 {
		acquired, err := lockDispatchEntities(guard, lockEs)
		if err != nil {
			return nil, nil, err
		}
		return acquired, func() {
			releaseDispatchLocks(guard, acquired)
		}, nil
	}
	groupMu := entityLockGroupMutex(groupID)
	if groupMu == nil {
		return nil, nil, ErrLockTimeout
	}
	if !groupMu.TryLock() {
		return nil, nil, ErrLockTimeout
	}
	releaseScope := pushEntityLockGroupScope(groupID)
	acquired, err := tryLockDispatchEntities(guard, lockEs)
	if err != nil {
		releaseScope()
		groupMu.Unlock()
		return nil, nil, err
	}
	return acquired, func() {
		releaseDispatchLocks(guard, acquired)
		releaseScope()
		groupMu.Unlock()
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
