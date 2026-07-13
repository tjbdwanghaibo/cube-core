package entity

// EntityGroupTransitionState describes a pending framework-level lock-group
// transition. It is a dispatch gate, not a lock.
type EntityGroupTransitionState int32

const (
	EntityGroupTransitionNone EntityGroupTransitionState = iota
	EntityGroupTransitionJoin
	EntityGroupTransitionLeave
	EntityGroupTransitionMove
)

// GroupLockID returns the entity lock group ID. Zero means the entity is
// serialized by its own entity mutex.
func (e *EntityBase) GroupLockID() int64 {
	if e == nil {
		return 0
	}
	return e.groupLockID.Load()
}

// GroupEpoch changes whenever the entity lock-group membership changes.
func (e *EntityBase) GroupEpoch() uint64 {
	if e == nil {
		return 0
	}
	return e.groupEpoch.Load()
}

// SetGroupLockIDForTest sets the lock group directly. Production code should
// use EntityManager.UpdateEntityGroup so the group index stays in sync.
func (e *EntityBase) SetGroupLockIDForTest(groupID int64) {
	e.setGroupLockID(groupID)
}

func (e *EntityBase) setGroupLockID(groupID int64) {
	if e == nil {
		return
	}
	if e.groupLockID.Swap(groupID) != groupID {
		e.groupEpoch.Add(1)
	}
}

func (e *EntityBase) GroupTransitionState() EntityGroupTransitionState {
	if e == nil {
		return EntityGroupTransitionNone
	}
	return EntityGroupTransitionState(e.groupState.Load())
}

func (e *EntityBase) GroupTransitionTargetID() int64 {
	if e == nil {
		return 0
	}
	return e.groupTargetID.Load()
}

func (e *EntityBase) BeginGroupTransition(state EntityGroupTransitionState, targetGroupID int64) bool {
	if e == nil || state == EntityGroupTransitionNone {
		return false
	}
	if !e.groupState.CompareAndSwap(int32(EntityGroupTransitionNone), int32(state)) {
		return false
	}
	e.groupTargetID.Store(targetGroupID)
	return true
}

func (e *EntityBase) ClearGroupTransition() {
	if e == nil {
		return
	}
	e.groupTargetID.Store(0)
	e.groupState.Store(int32(EntityGroupTransitionNone))
}

func (e *EntityBase) GroupTransitionPending() bool {
	return e != nil && e.GroupTransitionState() != EntityGroupTransitionNone
}
