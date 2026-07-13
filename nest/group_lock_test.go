package nest

import (
	"errors"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestEntityLockGroupScopeAvailableForGroupedDispatch(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	prevMgr := entity.Mgr
	entity.Mgr = entity.NewEntityManager()
	t.Cleanup(func() { entity.Mgr = prevMgr })

	getter := newMockGetter()
	groupID := int64(7001)
	id1 := mustBuildCastID(t, 4101, entity.EntityCategory(1), nestLocalKind)
	id2 := mustBuildCastID(t, 4102, entity.EntityCategory(1), nestLocalKind)
	idOtherGroup := mustBuildCastID(t, 4103, entity.EntityCategory(1), nestLocalKind)
	e1 := newMockEntity(id1, entity.EntityCategory(1))
	e2 := newMockEntity(id2, entity.EntityCategory(1))
	otherGroup := newMockEntity(idOtherGroup, entity.EntityCategory(1))
	e1.Base().SetGroupLockIDForTest(groupID)
	e2.Base().SetGroupLockIDForTest(groupID)
	otherGroup.Base().SetGroupLockIDForTest(groupID + 1)
	entity.Mgr.Add(e1)
	entity.Mgr.Add(e2)
	entity.Mgr.Add(otherGroup)
	getter.Add(e1)
	getter.Add(e2)
	getter.Add(otherGroup)

	name := NewHandlerName("test_entity_lock_group_scope_available")
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		scope := CurrentEntityLockGroup()
		if scope == nil {
			return nil, errors.New("missing group scope")
		}
		if scope.GroupID() != groupID {
			return nil, errors.New("wrong group scope id")
		}
		got, ok := GroupEntityAs[*mockEntity](scope, id2)
		if !ok || got != e2 {
			return nil, errors.New("same group entity lookup failed")
		}
		if got, ok := GroupEntityAs[*mockEntity](scope, idOtherGroup); ok || got != nil {
			return nil, errors.New("different group entity should not be returned")
		}
		if len(es) != 1 || es[0] != e1 {
			return nil, errors.New("dispatch target mismatch")
		}
		return "ok", nil
	})

	mgr := &NestMgr{getter: getter}
	got, err := mgr.singleDispatch(name.String(), id1, nil)
	if err != nil {
		t.Fatalf("singleDispatch: %v", err)
	}
	if got != "ok" {
		t.Fatalf("result = %v, want ok", got)
	}
	if CurrentEntityLockGroup() != nil {
		t.Fatal("group scope should be cleared after dispatch")
	}
}

func TestEntityLockGroupScopeNilForNormalDispatch(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	id := mustBuildCastID(t, 4201, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	getter.Add(e)

	name := NewHandlerName("test_entity_lock_group_scope_nil")
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		if CurrentEntityLockGroup() != nil {
			return nil, errors.New("normal dispatch should not have group scope")
		}
		if len(es) != 1 || es[0] != e {
			return nil, errors.New("dispatch target mismatch")
		}
		return "ok", nil
	})

	mgr := &NestMgr{getter: getter}
	got, err := mgr.singleDispatch(name.String(), id, nil)
	if err != nil {
		t.Fatalf("singleDispatch: %v", err)
	}
	if got != "ok" {
		t.Fatalf("result = %v, want ok", got)
	}
}

func TestEntityLockGroupSnapshotDetectsMembershipChange(t *testing.T) {
	id := mustBuildCastID(t, 4211, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	snapshots := captureDispatchGroupSnapshots([]entity.IThreadSafeEntity{e})

	e.Base().SetGroupLockIDForTest(9101)

	if err := validateDispatchGroupSnapshots(snapshots); !errors.Is(err, ErrEntityLockGroupChanged) {
		t.Fatalf("validateDispatchGroupSnapshots err = %v, want ErrEntityLockGroupChanged", err)
	}
}

func TestEntityLockGroupSnapshotDetectsPendingTransition(t *testing.T) {
	id := mustBuildCastID(t, 4212, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	snapshots := captureDispatchGroupSnapshots([]entity.IThreadSafeEntity{e})

	if !e.Base().BeginGroupTransition(entity.EntityGroupTransitionJoin, 9102) {
		t.Fatal("BeginGroupTransition should succeed")
	}

	if err := validateDispatchGroupSnapshots(snapshots); !errors.Is(err, ErrEntityGroupTransitionPending) {
		t.Fatalf("validateDispatchGroupSnapshots err = %v, want ErrEntityGroupTransitionPending", err)
	}
}

func TestLockDispatchEntitiesForHandlerRetriesEpochChangeWhileWaiting(t *testing.T) {
	id := mustBuildCastID(t, 4213, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	mu := e.GetMutex()
	mu.Lock()

	done := make(chan error, 1)
	go func() {
		guard := entity.GetEntityGuard()
		_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, []entity.IThreadSafeEntity{e})
		if err == nil {
			scope := CurrentEntityLockGroup()
			if scope == nil || scope.GroupID() != 9103 {
				err = errors.New("dispatch did not retry into the new group scope")
			}
			releaseLocks()
		}
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	e.Base().SetGroupLockIDForTest(9103)
	mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("lockDispatchEntitiesForHandler: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lockDispatchEntitiesForHandler did not return")
	}
	if CurrentEntityLockGroup() != nil {
		t.Fatal("group scope should not leak to caller goroutine")
	}
}

func TestLockDispatchEntitiesForHandlerDoesNotBlockGroupOnBusyExtraEntity(t *testing.T) {
	groupID := int64(9201)
	groupedID := mustBuildCastID(t, 4221, entity.EntityCategory(1), nestLocalKind)
	extraID := mustBuildCastID(t, 4222, entity.EntityCategory(1), nestLocalKind)
	grouped := newMockEntity(groupedID, entity.EntityCategory(1))
	extra := newMockEntity(extraID, entity.EntityCategory(1))
	grouped.Base().SetGroupLockIDForTest(groupID)

	extraLocked := make(chan struct{})
	releaseExtra := make(chan struct{})
	go func() {
		extra.GetMutex().Lock()
		close(extraLocked)
		<-releaseExtra
		extra.GetMutex().Unlock()
	}()
	<-extraLocked
	defer close(releaseExtra)

	done := make(chan error, 1)
	go func() {
		guard := entity.GetEntityGuard()
		_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, []entity.IThreadSafeEntity{grouped, extra})
		if err == nil {
			releaseLocks()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("lockDispatchEntitiesForHandler err = %v, want ErrLockTimeout", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("lockDispatchEntitiesForHandler blocked while holding group lock")
	}
}

func TestLockDispatchEntitiesForHandlerDoesNotBlockOnBusyGroupLock(t *testing.T) {
	groupID := int64(9203)
	groupedID := mustBuildCastID(t, 4241, entity.EntityCategory(1), nestLocalKind)
	grouped := newMockEntity(groupedID, entity.EntityCategory(1))
	grouped.Base().SetGroupLockIDForTest(groupID)

	groupLocked := make(chan struct{})
	releaseGroup := make(chan struct{})
	go func() {
		mu := entityLockGroupMutex(groupID)
		mu.Lock()
		close(groupLocked)
		<-releaseGroup
		mu.Unlock()
	}()
	<-groupLocked
	defer close(releaseGroup)

	done := make(chan error, 1)
	go func() {
		guard := entity.GetEntityGuard()
		_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, []entity.IThreadSafeEntity{grouped})
		if err == nil {
			releaseLocks()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("lockDispatchEntitiesForHandler err = %v, want ErrLockTimeout", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("lockDispatchEntitiesForHandler blocked on busy group lock")
	}
}

func TestEntityLockGroupDispatchRequeuesBusyGroupLock(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	groupID := int64(9204)
	groupedID := mustBuildCastID(t, 4242, entity.EntityCategory(1), nestLocalKind)
	grouped := newMockEntity(groupedID, entity.EntityCategory(1))
	grouped.Base().SetGroupLockIDForTest(groupID)
	getter.Add(grouped)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_entity_lock_group_requeue_busy_group")
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		if len(es) != 1 || es[0] != grouped {
			return nil, errors.New("dispatch entity mismatch")
		}
		scope := CurrentEntityLockGroup()
		if scope == nil || scope.GroupID() != groupID {
			return nil, errors.New("missing group scope")
		}
		return "ok", nil
	})

	groupLocked := make(chan struct{})
	releaseGroup := make(chan struct{})
	go func() {
		mu := entityLockGroupMutex(groupID)
		mu.Lock()
		close(groupLocked)
		<-releaseGroup
		mu.Unlock()
	}()
	<-groupLocked

	type syncResult struct {
		ret any
		err error
	}
	done := make(chan syncResult, 1)
	go func() {
		ret, err := Nest.Sync(name, groupedID, nil)
		done <- syncResult{ret: ret, err: err}
	}()

	select {
	case got := <-done:
		close(releaseGroup)
		t.Fatalf("Sync returned before group lock released: ret=%v err=%v", got.ret, got.err)
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseGroup)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Sync err = %v, want nil", got.err)
		}
		if got.ret != "ok" {
			t.Fatalf("Sync ret = %v, want ok", got.ret)
		}
	case <-time.After(time.Second):
		t.Fatal("Sync did not finish after group lock released")
	}
}

func TestEntityLockGroupDispatchRequeuesBusyExtraEntity(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	groupID := int64(9202)
	groupedID := mustBuildCastID(t, 4231, entity.EntityCategory(1), nestLocalKind)
	extraID := mustBuildCastID(t, 4232, entity.EntityCategory(1), nestLocalKind)
	grouped := newMockEntity(groupedID, entity.EntityCategory(1))
	extra := newMockEntity(extraID, entity.EntityCategory(1))
	grouped.Base().SetGroupLockIDForTest(groupID)
	getter.Add(grouped)
	getter.Add(extra)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_entity_lock_group_requeue_busy_extra")
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		if len(es) != 2 || es[0] != grouped || es[1] != extra {
			return nil, errors.New("dispatch entities mismatch")
		}
		scope := CurrentEntityLockGroup()
		if scope == nil || scope.GroupID() != groupID {
			return nil, errors.New("missing group scope")
		}
		return "ok", nil
	})

	extraLocked := make(chan struct{})
	releaseExtra := make(chan struct{})
	go func() {
		extra.GetMutex().Lock()
		close(extraLocked)
		<-releaseExtra
		extra.GetMutex().Unlock()
	}()
	<-extraLocked

	type syncResult struct {
		ret any
		err error
	}
	done := make(chan syncResult, 1)
	go func() {
		ret, err := Nest.MultiSync(name, []int64{groupedID, extraID}, nil)
		done <- syncResult{ret: ret, err: err}
	}()

	select {
	case got := <-done:
		close(releaseExtra)
		t.Fatalf("MultiSync returned before extra lock released: ret=%v err=%v", got.ret, got.err)
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseExtra)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("MultiSync err = %v, want nil", got.err)
		}
		if got.ret != "ok" {
			t.Fatalf("MultiSync ret = %v, want ok", got.ret)
		}
	case <-time.After(time.Second):
		t.Fatal("MultiSync did not finish after extra lock released")
	}
}
