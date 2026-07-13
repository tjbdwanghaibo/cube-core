package nest

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/misc"
	"github.com/tjbdwanghaibo/cube-core/obs"
)

const (
	entityGroupDispatchRequeueDelay = 5 * time.Millisecond
	entityGroupDispatchRequeueMax   = 400

	entityGroupTransitionRetryDelay  = 5 * time.Millisecond
	entityGroupTransitionRetryMax    = 400
	entityGroupTransitionLockTimeout = 2 * time.Second
)

type GroupTransitionRequest struct {
	EntityID      int64
	TargetGroupID int64
	State         entity.EntityGroupTransitionState
	Attempts      int
	Deadline      time.Time
	Continuation  *GroupTransitionContinuation
}

type GroupTransitionContinuation struct {
	Handler string
	Params  []any
	RetChan chan any
	Context fctx.ContextSnapshot
}

type GroupTransitionOptions struct {
	Continuation *GroupTransitionContinuation
}

type GroupTransitionOption func(*GroupTransitionOptions)

func GroupTransitionWithContinuation(name HandlerName, params Params) GroupTransitionOption {
	return func(opts *GroupTransitionOptions) {
		opts.Continuation = &GroupTransitionContinuation{
			Handler: name.String(),
			Params:  []any(params),
		}
	}
}

func (mgr *NestMgr) RequestJoinEntityLockGroup(entityID int64, groupID int64, opts ...GroupTransitionOption) error {
	if groupID == 0 {
		return ErrInvalidEntityLockGroup
	}
	return mgr.requestEntityLockGroupTransition(entityID, entity.EntityGroupTransitionJoin, groupID, opts...)
}

func (mgr *NestMgr) RequestLeaveEntityLockGroup(entityID int64, opts ...GroupTransitionOption) error {
	return mgr.requestEntityLockGroupTransition(entityID, entity.EntityGroupTransitionLeave, 0, opts...)
}

func (mgr *NestMgr) RequestMoveEntityLockGroup(entityID int64, groupID int64, opts ...GroupTransitionOption) error {
	if groupID == 0 {
		return ErrInvalidEntityLockGroup
	}
	return mgr.requestEntityLockGroupTransition(entityID, entity.EntityGroupTransitionMove, groupID, opts...)
}

func (mgr *NestMgr) requestEntityLockGroupTransition(entityID int64, state entity.EntityGroupTransitionState, targetGroupID int64, opts ...GroupTransitionOption) error {
	if mgr == nil || mgr.dispatcher == nil || mgr.getter == nil {
		return ErrNestStopped
	}
	fullID, err := entity.NormalizeFullID(entityID, entity.EntityKindNone)
	if err != nil {
		return err
	}
	meta := entity.ResolveEntityID(fullID)
	ent, err := mgr.getter.Get(meta.FullID, meta.Category)
	if err != nil {
		return err
	}
	if ent == nil || ent.Base() == nil {
		return ErrEntityNotFound
	}
	if !ent.Base().BeginGroupTransition(state, targetGroupID) {
		return ErrEntityGroupTransitionPending
	}
	opt := &GroupTransitionOptions{}
	for _, fn := range opts {
		if fn != nil {
			fn(opt)
		}
	}
	if opt.Continuation != nil {
		if err := bindGroupTransitionContinuation(opt.Continuation); err != nil {
			ent.Base().ClearGroupTransition()
			return err
		}
	}

	msg := GenMsg(MsgTypeGroupTransition)
	msg.Tid = meta.FullID
	msg.Name = "entity_lock_group_transition"
	msg.GroupTransition = &GroupTransitionRequest{
		EntityID:      meta.FullID,
		TargetGroupID: targetGroupID,
		State:         state,
		Deadline:      time.Now().Add(entityGroupTransitionLockTimeout),
		Continuation:  opt.Continuation,
	}
	bindMsgContext(msg, false)
	observeEntityGroupTransition(msg.GroupTransition, "request")
	mgr.dispatcher.SendMsg(msg)
	return nil
}

func (mgr *NestMgr) groupTransitionDispatch(req *GroupTransitionRequest) (ret any, err error) {
	defer func() {
		if err != nil && !errors.Is(err, ErrEntityGroupTransitionScheduled) {
			completeGroupTransitionContinuation(req, err)
		}
	}()
	if mgr == nil || mgr.getter == nil || req == nil {
		return nil, ErrNestStopped
	}
	fullID, err := entity.NormalizeFullID(req.EntityID, entity.EntityKindNone)
	if err != nil {
		return nil, err
	}
	meta := entity.ResolveEntityID(fullID)
	ent, err := mgr.getter.Get(meta.FullID, meta.Category)
	if err != nil {
		return nil, err
	}
	if ent == nil || ent.Base() == nil || !ent.Touch() {
		return nil, ErrEntityNotFound
	}
	defer ent.UnTouch()

	groupIDs := transitionLockGroupIDs(ent.Base().GroupLockID(), req.TargetGroupID, req.State)
	unlockGroups, ok := tryLockEntityLockGroups(groupIDs)
	if !ok {
		return nil, mgr.retryGroupTransition(req, "group_lock_busy")
	}
	defer unlockGroups()

	guard := entity.GetEntityGuard()
	acquired, err := tryLockDispatchEntities(guard, []entity.IThreadSafeEntity{ent})
	if err != nil {
		return nil, mgr.retryGroupTransition(req, "entity_lock_busy")
	}
	defer releaseDispatchLocks(guard, acquired)

	if ent.Base().GroupTransitionState() != req.State ||
		ent.Base().GroupTransitionTargetID() != req.TargetGroupID {
		return nil, fmt.Errorf("%w: stale transition", ErrEntityGroupTransitionPending)
	}
	if entity.Mgr == nil {
		ent.Base().ClearGroupTransition()
		return nil, ErrEntityNotFound
	}

	switch req.State {
	case entity.EntityGroupTransitionJoin:
		if err := entity.Mgr.UpdateEntityGroup(ent, req.TargetGroupID); err != nil {
			ent.Base().ClearGroupTransition()
			return nil, err
		}
	case entity.EntityGroupTransitionLeave:
		if err := entity.Mgr.UpdateEntityGroup(ent, 0); err != nil {
			ent.Base().ClearGroupTransition()
			return nil, err
		}
	case entity.EntityGroupTransitionMove:
		if err := entity.Mgr.UpdateEntityGroup(ent, req.TargetGroupID); err != nil {
			ent.Base().ClearGroupTransition()
			return nil, err
		}
	default:
		ent.Base().ClearGroupTransition()
		return nil, ErrInvalidEntityLockGroup
	}
	ent.Base().ClearGroupTransition()
	observeEntityGroupTransition(req, "success")
	mgr.dispatchGroupTransitionContinuation(req)
	return nil, nil
}

func (mgr *NestMgr) retryGroupTransition(req *GroupTransitionRequest, reason string) error {
	if mgr == nil || req == nil {
		return ErrNestStopped
	}
	if req.Deadline.IsZero() {
		req.Deadline = time.Now().Add(entityGroupTransitionLockTimeout)
	}
	req.Attempts++
	if req.Attempts >= entityGroupTransitionRetryMax || time.Now().After(req.Deadline) {
		err := fmt.Errorf("%w: entity_group_transition %s", ErrLockTimeout, reason)
		mgr.abortGroupTransition(req)
		observeEntityGroupTransition(req, "timeout")
		return err
	}
	if mgr.dispatcher == nil {
		return ErrNestStopped
	}
	observeEntityGroupTransition(req, "retry")
	msg := GenMsg(MsgTypeGroupTransition)
	msg.Tid = req.EntityID
	msg.Name = "entity_lock_group_transition"
	msg.GroupTransition = req
	if cur := currentNestDispatchMsg(); cur != nil {
		msg.Context = cur.Context.Clone()
	}
	mgr.dispatcher.DelaySendMsg(entityGroupTransitionRetryDelay, msg)
	return ErrEntityGroupTransitionScheduled
}

func (mgr *NestMgr) abortGroupTransition(req *GroupTransitionRequest) {
	if mgr == nil || mgr.getter == nil || req == nil {
		return
	}
	fullID, err := entity.NormalizeFullID(req.EntityID, entity.EntityKindNone)
	if err != nil {
		return
	}
	meta := entity.ResolveEntityID(fullID)
	ent, err := mgr.getter.Get(meta.FullID, meta.Category)
	if err != nil || ent == nil || ent.Base() == nil {
		return
	}
	if ent.Base().GroupTransitionState() == req.State &&
		ent.Base().GroupTransitionTargetID() == req.TargetGroupID {
		ent.Base().ClearGroupTransition()
	}
}

func bindGroupTransitionContinuation(cont *GroupTransitionContinuation) error {
	if cont == nil {
		return nil
	}
	cur := currentNestDispatchMsg()
	if cur == nil || cur.RetChan == nil {
		return ErrInvalidEntityLockGroup
	}
	cont.RetChan = cur.RetChan
	cont.Context = cur.Context.Clone()
	cur.RetChan = nil
	return nil
}

func completeGroupTransitionContinuation(req *GroupTransitionRequest, err error) {
	if req == nil || req.Continuation == nil || req.Continuation.RetChan == nil || err == nil {
		return
	}
	req.Continuation.RetChan <- err
	req.Continuation.RetChan = nil
}

func (mgr *NestMgr) dispatchGroupTransitionContinuation(req *GroupTransitionRequest) {
	if mgr == nil || mgr.dispatcher == nil || req == nil || req.Continuation == nil {
		return
	}
	cont := req.Continuation
	if cont.Handler == "" || cont.RetChan == nil {
		return
	}
	msg := GenMsg(MsgTypeSingle)
	msg.Tid = req.EntityID
	msg.Name = cont.Handler
	msg.Params = cont.Params
	msg.RetChan = cont.RetChan
	msg.Context = cont.Context.Clone()
	checkRemoteId(msg, req.EntityID)
	cont.RetChan = nil
	mgr.dispatcher.SendMsg(msg)
}

func transitionLockGroupIDs(currentGroupID int64, targetGroupID int64, state entity.EntityGroupTransitionState) []int64 {
	seen := make(map[int64]struct{}, 2)
	add := func(id int64) {
		if id == 0 {
			return
		}
		seen[id] = struct{}{}
	}
	switch state {
	case entity.EntityGroupTransitionJoin:
		add(targetGroupID)
	case entity.EntityGroupTransitionLeave:
		add(currentGroupID)
	case entity.EntityGroupTransitionMove:
		add(currentGroupID)
		add(targetGroupID)
	}
	ret := make([]int64, 0, len(seen))
	for id := range seen {
		ret = append(ret, id)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret
}

func tryLockEntityLockGroups(groupIDs []int64) (func(), bool) {
	locked := make([]int64, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		mu := entityLockGroupMutex(groupID)
		if mu == nil {
			continue
		}
		if !mu.TryLock() {
			for i := len(locked) - 1; i >= 0; i-- {
				if lockedMu := entityLockGroupMutex(locked[i]); lockedMu != nil {
					lockedMu.Unlock()
				}
			}
			return func() {}, false
		}
		locked = append(locked, groupID)
	}
	return func() {
		for i := len(locked) - 1; i >= 0; i-- {
			if mu := entityLockGroupMutex(locked[i]); mu != nil {
				mu.Unlock()
			}
		}
	}, true
}

func rejectPendingEntityGroupTransition(es []entity.IThreadSafeEntity) error {
	for _, ent := range es {
		if ent != nil && ent.Base() != nil && ent.Base().GroupTransitionPending() {
			return ErrEntityGroupTransitionPending
		}
	}
	return nil
}

func requeuePendingEntityGroupTransition(mgr *NestMgr, msg *Msg, err error) bool {
	if mgr == nil || mgr.dispatcher == nil || msg == nil || !errorsIsEntityGroupPending(err) {
		return false
	}
	if msg.Type == MsgTypeGroupTransition {
		return false
	}
	return requeueNestDispatch(mgr, msg, "group_transition_pending")
}

func requeueTransientDispatch(mgr *NestMgr, msg *Msg, err error) bool {
	reason, ok := transientDispatchRequeueReason(err)
	if mgr == nil || mgr.dispatcher == nil || msg == nil || !ok {
		return false
	}
	if msg.Type == MsgTypeGroupTransition {
		return false
	}
	return requeueNestDispatch(mgr, msg, reason)
}

func transientDispatchRequeueReason(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrLockTimeout):
		return "lock_timeout", true
	case errors.Is(err, ErrEntityLockGroupChanged):
		return "group_changed", true
	default:
		return "", false
	}
}

func requeueNestDispatch(mgr *NestMgr, msg *Msg, reason string) bool {
	if mgr == nil || mgr.dispatcher == nil || msg == nil || msg.PendingRequeues >= entityGroupDispatchRequeueMax {
		return false
	}
	next := msg.Clone()
	next.RefCount = 0
	next.RemoteRelease = nil
	next.PendingRequeues = msg.PendingRequeues + 1
	msg.RetChan = nil
	obs.IncCounter("nest.dispatch.requeue.total", obs.Labels{
		"reason": reason,
		"type":   msg.Type.String(),
	}, 1)
	mgr.dispatcher.DelaySendMsg(entityGroupDispatchRequeueDelay, next)
	return true
}

func errorsIsEntityGroupPending(err error) bool {
	return errors.Is(err, ErrEntityGroupTransitionPending)
}

func observeEntityGroupTransition(req *GroupTransitionRequest, result string) {
	if req == nil {
		return
	}
	obs.IncCounter("nest.entity_group.transition.total", obs.Labels{
		"state":  entityGroupTransitionStateName(req.State),
		"result": result,
	}, 1)
}

func entityGroupTransitionStateName(state entity.EntityGroupTransitionState) string {
	switch state {
	case entity.EntityGroupTransitionJoin:
		return "join"
	case entity.EntityGroupTransitionLeave:
		return "leave"
	case entity.EntityGroupTransitionMove:
		return "move"
	default:
		return "unknown"
	}
}

var currentNestDispatchMsgs sync.Map // map[int64]*Msg

func pushCurrentNestDispatchMsg(msg *Msg) func() {
	if msg == nil {
		return func() {}
	}
	gid := misc.GoID()
	prev, _ := currentNestDispatchMsgs.Load(gid)
	currentNestDispatchMsgs.Store(gid, msg)
	return func() {
		if prev != nil {
			currentNestDispatchMsgs.Store(gid, prev)
		} else {
			currentNestDispatchMsgs.Delete(gid)
		}
	}
}

func currentNestDispatchMsg() *Msg {
	value, ok := currentNestDispatchMsgs.Load(misc.GoID())
	if !ok {
		return nil
	}
	msg, _ := value.(*Msg)
	return msg
}
