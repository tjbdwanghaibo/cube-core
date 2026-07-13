package nest

import (
	"errors"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestEntityLockGroupTransitionJoinMoveLeaveUpdatesState(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	prevMgr := entity.Mgr
	entity.Mgr = entity.NewEntityManager()
	t.Cleanup(func() { entity.Mgr = prevMgr })

	getter := newMockGetter()
	id := mustBuildCastID(t, 4301, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	entity.Mgr.Add(e)
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	if err := Nest.RequestJoinEntityLockGroup(id, 8101); err != nil {
		t.Fatalf("RequestJoinEntityLockGroup: %v", err)
	}
	waitForNestCondition(t, func() bool {
		return e.Base().GroupLockID() == 8101 &&
			e.Base().GroupEpoch() == 1 &&
			!e.Base().GroupTransitionPending() &&
			entity.Mgr.GetGroupEntity(8101, id) == e
	})

	if err := Nest.RequestMoveEntityLockGroup(id, 8102); err != nil {
		t.Fatalf("RequestMoveEntityLockGroup: %v", err)
	}
	waitForNestCondition(t, func() bool {
		return e.Base().GroupLockID() == 8102 &&
			e.Base().GroupEpoch() == 2 &&
			!e.Base().GroupTransitionPending() &&
			entity.Mgr.GetGroupEntity(8101, id) == nil &&
			entity.Mgr.GetGroupEntity(8102, id) == e
	})

	if err := Nest.RequestLeaveEntityLockGroup(id); err != nil {
		t.Fatalf("RequestLeaveEntityLockGroup: %v", err)
	}
	waitForNestCondition(t, func() bool {
		return e.Base().GroupLockID() == 0 &&
			e.Base().GroupEpoch() == 3 &&
			!e.Base().GroupTransitionPending() &&
			entity.Mgr.GetGroupEntity(8102, id) == nil
	})
}

func TestEntityLockGroupTransitionPendingGatesNormalDispatch(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	id := mustBuildCastID(t, 4401, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	if !e.Base().BeginGroupTransition(entity.EntityGroupTransitionJoin, 8201) {
		t.Fatal("BeginGroupTransition should succeed")
	}
	getter.Add(e)

	name := NewHandlerName("test_group_transition_pending_gate")
	MustRegisterHandler(name, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		return nil, errors.New("handler should not run while transition is pending")
	})

	mgr := &NestMgr{getter: getter}
	_, err := mgr.singleDispatch(name.String(), id, nil)
	if !errors.Is(err, ErrEntityGroupTransitionPending) {
		t.Fatalf("singleDispatch err = %v, want ErrEntityGroupTransitionPending", err)
	}
}

func TestEntityLockGroupTransitionPendingRequeuesSyncDispatch(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	id := mustBuildCastID(t, 4451, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	if !e.Base().BeginGroupTransition(entity.EntityGroupTransitionJoin, 8251) {
		t.Fatal("BeginGroupTransition should succeed")
	}
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_group_transition_pending_requeue_sync")
	called := make(chan struct{}, 1)
	MustRegisterHandler(name, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		called <- struct{}{}
		return "ok", nil
	})

	time.AfterFunc(30*time.Millisecond, func() {
		e.Base().ClearGroupTransition()
	})

	got, err := Nest.Sync(name, id, nil)
	if err != nil {
		t.Fatalf("Nest.Sync err = %v, want nil", err)
	}
	if got != "ok" {
		t.Fatalf("Nest.Sync result = %v, want ok", got)
	}
	select {
	case <-called:
	default:
		t.Fatal("handler should run after pending clears")
	}
}

func TestEntityLockGroupTransitionContinuationReturnsSyncResult(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	prevMgr := entity.Mgr
	entity.Mgr = entity.NewEntityManager()
	t.Cleanup(func() { entity.Mgr = prevMgr })

	getter := newMockGetter()
	id := mustBuildCastID(t, 4501, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	entity.Mgr.Add(e)
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	startName := NewHandlerName("test_group_transition_continuation_start")
	continuationName := NewHandlerName("test_group_transition_continuation_after_join")
	MustRegisterHandler(startName, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		if err := Nest.RequestJoinEntityLockGroup(
			id,
			8301,
			GroupTransitionWithContinuation(continuationName, NewParams("ready")),
		); err != nil {
			return nil, err
		}
		return nil, ErrEntityGroupTransitionScheduled
	})
	MustRegisterHandler(continuationName, func(es []entity.IThreadSafeEntity, params []any, _ ...HandlerOption) (any, error) {
		if len(params) != 1 || params[0] != "ready" {
			return nil, errors.New("continuation params mismatch")
		}
		if len(es) != 1 || es[0] != e {
			return nil, errors.New("continuation entity mismatch")
		}
		scope := CurrentEntityLockGroup()
		if scope == nil || scope.GroupID() != 8301 {
			return nil, errors.New("continuation did not run under target group")
		}
		return "joined", nil
	})

	got, err := Nest.Sync(startName, id, nil)
	if err != nil {
		t.Fatalf("Nest.Sync: %v", err)
	}
	if got != "joined" {
		t.Fatalf("sync result = %v, want joined", got)
	}
	if e.Base().GroupLockID() != 8301 {
		t.Fatalf("group id = %d, want 8301", e.Base().GroupLockID())
	}
}

func TestEntityLockGroupTransitionRetriesWhenEntityLockBusy(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	prevMgr := entity.Mgr
	entity.Mgr = entity.NewEntityManager()
	t.Cleanup(func() { entity.Mgr = prevMgr })

	getter := newMockGetter()
	id := mustBuildCastID(t, 4601, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	entity.Mgr.Add(e)
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	mu := e.GetMutex()
	mu.Lock()
	if err := Nest.RequestJoinEntityLockGroup(id, 8401); err != nil {
		mu.Unlock()
		t.Fatalf("RequestJoinEntityLockGroup: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if !e.Base().GroupTransitionPending() {
		mu.Unlock()
		t.Fatal("transition should remain pending while entity lock is busy")
	}
	mu.Unlock()

	waitForNestCondition(t, func() bool {
		return e.Base().GroupLockID() == 8401 &&
			!e.Base().GroupTransitionPending() &&
			entity.Mgr.GetGroupEntity(8401, id) == e
	})
}

func TestEntityLockGroupTransitionTimeoutClearsPending(t *testing.T) {
	prevMgr := entity.Mgr
	entity.Mgr = entity.NewEntityManager()
	t.Cleanup(func() { entity.Mgr = prevMgr })

	getter := newMockGetter()
	id := mustBuildCastID(t, 4602, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	entity.Mgr.Add(e)
	getter.Add(e)

	if !e.Base().BeginGroupTransition(entity.EntityGroupTransitionJoin, 8402) {
		t.Fatal("BeginGroupTransition should succeed")
	}

	mgr := &NestMgr{getter: getter}
	mu := e.GetMutex()
	locked := make(chan struct{})
	release := make(chan struct{})
	go func() {
		mu.Lock()
		close(locked)
		<-release
		mu.Unlock()
	}()
	<-locked
	_, err := mgr.groupTransitionDispatch(&GroupTransitionRequest{
		EntityID:      id,
		TargetGroupID: 8402,
		State:         entity.EntityGroupTransitionJoin,
		Attempts:      entityGroupTransitionRetryMax,
		Deadline:      time.Now().Add(-time.Millisecond),
	})
	close(release)

	if !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("groupTransitionDispatch err = %v, want ErrLockTimeout", err)
	}
	if e.Base().GroupTransitionPending() {
		t.Fatal("transition pending should be cleared after timeout")
	}
}

func TestRequeueTransientDispatchHandlesGroupChanged(t *testing.T) {
	dispatcher := NewDispatcher("test_group_changed_requeue", 1, 0, 16, nil)
	dispatcher.OnInit()
	defer dispatcher.OnDestroy()

	mgr := &NestMgr{dispatcher: dispatcher}
	msg, ch := GenSyncMsg(MsgTypeSingle)
	msg.Name = "test_group_changed_requeue"
	msg.Tid = 4701

	if !requeueTransientDispatch(mgr, msg, ErrEntityLockGroupChanged) {
		t.Fatal("ErrEntityLockGroupChanged should be requeued as a transient dispatch error")
	}
	if msg.RetChan != nil {
		t.Fatal("original message RetChan should be moved to the requeued clone")
	}
	select {
	case got := <-ch:
		t.Fatalf("requeue should not complete sync channel immediately: %v", got)
	default:
	}
}

func waitForNestCondition(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
