package entity

import (
	"errors"
	"testing"
)

func TestEntityGroupBaseDefaultsAndMutation(t *testing.T) {
	e := newMgrTestEntity(2001, testEntityCategoryPlayer)

	if got := e.Base().GroupLockID(); got != 0 {
		t.Fatalf("default group lock id = %d, want 0", got)
	}
	if got := e.Base().GroupEpoch(); got != 0 {
		t.Fatalf("default group epoch = %d, want 0", got)
	}
	if got := e.Base().GroupTransitionState(); got != EntityGroupTransitionNone {
		t.Fatalf("default transition state = %v, want none", got)
	}

	e.Base().SetGroupLockIDForTest(3001)
	if got := e.Base().GroupLockID(); got != 3001 {
		t.Fatalf("group lock id = %d, want 3001", got)
	}
	if got := e.Base().GroupEpoch(); got != 1 {
		t.Fatalf("group epoch = %d, want 1", got)
	}

	if !e.Base().BeginGroupTransition(EntityGroupTransitionJoin, 4001) {
		t.Fatal("BeginGroupTransition should succeed from none state")
	}
	if got := e.Base().GroupTransitionState(); got != EntityGroupTransitionJoin {
		t.Fatalf("transition state = %v, want join", got)
	}
	if got := e.Base().GroupTransitionTargetID(); got != 4001 {
		t.Fatalf("transition target = %d, want 4001", got)
	}
	if e.Base().BeginGroupTransition(EntityGroupTransitionLeave, 0) {
		t.Fatal("BeginGroupTransition should reject while already pending")
	}
	e.Base().ClearGroupTransition()
	if got := e.Base().GroupTransitionState(); got != EntityGroupTransitionNone {
		t.Fatalf("transition state after clear = %v, want none", got)
	}
}

func TestEntityGroupManagerIndexTracksAddUpdateRemove(t *testing.T) {
	mgr := NewEntityManager()
	e1 := newMgrTestEntity(2101, testEntityCategoryPlayer)
	e2 := newMgrTestEntity(2102, testEntityCategoryPlayer)

	e1.Base().SetGroupLockIDForTest(9001)
	mgr.Add(e1)
	mgr.Add(e2)

	if got := mgr.GetGroupEntity(9001, e1.ID()); got != e1 {
		t.Fatalf("group entity lookup returned %v, want e1", got)
	}
	if got := mgr.GetGroupEntity(9001, e2.ID()); got != nil {
		t.Fatalf("ungrouped entity lookup returned %v, want nil", got)
	}

	if err := mgr.UpdateEntityGroup(e2, 9001); err != nil {
		t.Fatalf("UpdateEntityGroup join: %v", err)
	}
	gotGroup := mgr.GetGroupEntities(9001)
	if len(gotGroup) != 2 {
		t.Fatalf("group entity count = %d, want 2", len(gotGroup))
	}

	if err := mgr.UpdateEntityGroup(e1, 9002); err != nil {
		t.Fatalf("UpdateEntityGroup move: %v", err)
	}
	if got := mgr.GetGroupEntity(9001, e1.ID()); got != nil {
		t.Fatalf("old group lookup returned %v, want nil", got)
	}
	if got := mgr.GetGroupEntity(9002, e1.ID()); got != e1 {
		t.Fatalf("new group lookup returned %v, want e1", got)
	}

	mgr.Remove(e2, EntityDestroyReason(0), false)
	if got := mgr.GetGroupEntity(9001, e2.ID()); got != nil {
		t.Fatalf("removed entity lookup returned %v, want nil", got)
	}
	if got := mgr.GetGroupEntities(9001); len(got) != 0 {
		t.Fatalf("empty old group entities = %d, want 0", len(got))
	}
}

func TestEntityGroupManagerLifecycleEdges(t *testing.T) {
	mgr := NewEntityManager()
	e := newMgrTestEntity(2201, testEntityCategoryPlayer)

	if err := mgr.UpdateEntityGroup(e, 9101); !errors.Is(err, ErrEntityNotManaged) {
		t.Fatalf("UpdateEntityGroup before add err = %v, want ErrEntityNotManaged", err)
	}
	if got := e.Base().GroupLockID(); got != 0 {
		t.Fatalf("unmanaged entity group = %d, want 0", got)
	}

	e.Base().SetGroupLockIDForTest(9101)
	mgr.Add(e)
	epoch := e.Base().GroupEpoch()
	if err := mgr.UpdateEntityGroup(e, 9101); err != nil {
		t.Fatalf("UpdateEntityGroup same group: %v", err)
	}
	if got := e.Base().GroupEpoch(); got != epoch {
		t.Fatalf("same group update epoch = %d, want %d", got, epoch)
	}

	if err := mgr.UpdateEntityGroup(e, 0); err != nil {
		t.Fatalf("UpdateEntityGroup leave: %v", err)
	}
	if got := mgr.GetGroupEntity(9101, e.ID()); got != nil {
		t.Fatalf("left group lookup returned %v, want nil", got)
	}
	if got := mgr.GetGroupEntities(9101); len(got) != 0 {
		t.Fatalf("left group entity count = %d, want 0", len(got))
	}

	if err := mgr.UpdateEntityGroup(e, 9102); err != nil {
		t.Fatalf("UpdateEntityGroup rejoin: %v", err)
	}
	mgr.Remove(e, EntityDestroyReason(0), false)
	if got := mgr.GetGroupEntity(9102, e.ID()); got != nil {
		t.Fatalf("removed group lookup returned %v, want nil", got)
	}
	if got := mgr.GetGroupEntities(9102); len(got) != 0 {
		t.Fatalf("removed group entity count = %d, want 0", len(got))
	}
}
