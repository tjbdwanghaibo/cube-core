package nest

import (
	"context"
	"errors"
	"fmt"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/worker"
)

// Client is the instance-scoped command boundary used by generated senders.
// Implementations must transfer ownership of Params only for the duration of
// the dispatch; callers must treat mutable parameter values as immutable after
// a successful asynchronous admission.
type Client interface {
	Dispatch(context.Context, HandlerName, int64, Params, ...SendOpt) error
	Request(context.Context, HandlerName, int64, Params, ...SendOpt) (any, error)
	DispatchBroadcast(context.Context, HandlerName, []int64, Params, ...SendOpt) error
	DispatchMulti(context.Context, HandlerName, []int64, Params, ...SendOpt) error
	RequestMulti(context.Context, HandlerName, []int64, Params, ...SendOpt) (any, error)
	DispatchMultiGroup(context.Context, HandlerName, [][]int64, Params, ...SendOpt) error
	RequestMultiGroup(context.Context, HandlerName, [][]int64, Params, ...SendOpt) (any, error)
}

// DispatchBroadcast admits independent single-entity deliveries. If admission
// fails, deliveries admitted before the returned error may still execute;
// callers that require all-or-nothing semantics must use DispatchMulti.
func (mgr *NestMgr) DispatchBroadcast(ctx context.Context, name HandlerName, ids []int64, params Params, opts ...SendOpt) error {
	if len(ids) == 0 {
		return fmt.Errorf("%w: empty entity ids", ErrInvalidMessage)
	}
	if err := validateClientDispatch(mgr, ctx, "DispatchBroadcast", name, ErrAsyncInHandler); err != nil {
		return err
	}
	fullIDs, err := normalizeClientIDs(ids)
	if err != nil {
		return err
	}
	var admissionErr error
	mgr.dispatcher.ForEachSpliceBatch(fullIDs, func(_ int, batch []int64, _ []int) {
		if admissionErr != nil {
			return
		}
		msg := GenMsg(MsgTypeBroadcast)
		msg.Tids = append(msg.Tids[:0], batch...)
		prepareClientMessage(ctx, msg, name, params, false, opts...)
		checkRemoteIds(msg, msg.Tids)
		admissionErr = mgr.admit(msg, opts...)
	})
	return normalizeAdmissionError(admissionErr)
}

var _ Client = (*NestMgr)(nil)

func (mgr *NestMgr) Dispatch(ctx context.Context, name HandlerName, id int64, params Params, opts ...SendOpt) error {
	if err := validateClientDispatch(mgr, ctx, "Dispatch", name, ErrAsyncInHandler); err != nil {
		return err
	}
	fullID, err := normalizeClientID(id)
	if err != nil {
		return err
	}
	msg := GenMsg(MsgTypeSingle)
	msg.Tid = fullID
	prepareClientMessage(ctx, msg, name, params, false, opts...)
	checkRemoteId(msg, fullID)
	return normalizeAdmissionError(mgr.admit(msg, opts...))
}

func (mgr *NestMgr) Request(ctx context.Context, name HandlerName, id int64, params Params, opts ...SendOpt) (any, error) {
	if err := validateClientDispatch(mgr, ctx, "Request", name, ErrSyncInHandler); err != nil {
		return nil, err
	}
	fullID, err := normalizeClientID(id)
	if err != nil {
		return nil, err
	}
	msg, ch := GenSyncMsg(MsgTypeSingle)
	msg.Tid = fullID
	prepareClientMessage(ctx, msg, name, params, true, opts...)
	checkRemoteId(msg, fullID)
	requestTimeout := msg.Context.SyncWait
	if err := mgr.admit(msg, opts...); err != nil {
		return nil, normalizeAdmissionError(err)
	}
	return mgr.waitResult(ctx, ch, requestTimeout)
}

func (mgr *NestMgr) DispatchMulti(ctx context.Context, name HandlerName, ids []int64, params Params, opts ...SendOpt) error {
	if len(ids) == 0 {
		return fmt.Errorf("%w: empty entity ids", ErrInvalidMessage)
	}
	if err := validateClientDispatch(mgr, ctx, "DispatchMulti", name, ErrAsyncInHandler); err != nil {
		return err
	}
	fullIDs, err := normalizeClientIDs(ids)
	if err != nil {
		return err
	}
	msg := GenMsg(MsgTypeMulti)
	msg.Tids = append(msg.Tids[:0], fullIDs...)
	prepareClientMessage(ctx, msg, name, params, false, opts...)
	checkRemoteIds(msg, msg.Tids)
	return normalizeAdmissionError(mgr.admit(msg, opts...))
}

func (mgr *NestMgr) RequestMulti(ctx context.Context, name HandlerName, ids []int64, params Params, opts ...SendOpt) (any, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: empty entity ids", ErrInvalidMessage)
	}
	if err := validateClientDispatch(mgr, ctx, "RequestMulti", name, ErrSyncInHandler); err != nil {
		return nil, err
	}
	fullIDs, err := normalizeClientIDs(ids)
	if err != nil {
		return nil, err
	}
	msg, ch := GenSyncMsg(MsgTypeMulti)
	msg.Tids = append(msg.Tids[:0], fullIDs...)
	prepareClientMessage(ctx, msg, name, params, true, opts...)
	checkRemoteIds(msg, msg.Tids)
	requestTimeout := msg.Context.SyncWait
	if err := mgr.admit(msg, opts...); err != nil {
		return nil, normalizeAdmissionError(err)
	}
	return mgr.waitResult(ctx, ch, requestTimeout)
}

func (mgr *NestMgr) DispatchMultiGroup(ctx context.Context, name HandlerName, groups [][]int64, params Params, opts ...SendOpt) error {
	if !validGroups(groups) {
		return fmt.Errorf("%w: empty entity groups", ErrInvalidMessage)
	}
	if err := validateClientDispatch(mgr, ctx, "DispatchMultiGroup", name, ErrAsyncInHandler); err != nil {
		return err
	}
	fullGroups, err := normalizeClientGroups(groups)
	if err != nil {
		return err
	}
	msg := GenMsg(MsgTypeMultiGroup)
	msg.GroupTIds = fullGroups
	prepareClientMessage(ctx, msg, name, params, false, opts...)
	checkRemoteGroups(msg, msg.GroupTIds)
	return normalizeAdmissionError(mgr.admit(msg, opts...))
}

func (mgr *NestMgr) RequestMultiGroup(ctx context.Context, name HandlerName, groups [][]int64, params Params, opts ...SendOpt) (any, error) {
	if !validGroups(groups) {
		return nil, fmt.Errorf("%w: empty entity groups", ErrInvalidMessage)
	}
	if err := validateClientDispatch(mgr, ctx, "RequestMultiGroup", name, ErrSyncInHandler); err != nil {
		return nil, err
	}
	fullGroups, err := normalizeClientGroups(groups)
	if err != nil {
		return nil, err
	}
	msg, ch := GenSyncMsg(MsgTypeMultiGroup)
	msg.GroupTIds = fullGroups
	prepareClientMessage(ctx, msg, name, params, true, opts...)
	checkRemoteGroups(msg, msg.GroupTIds)
	requestTimeout := msg.Context.SyncWait
	if err := mgr.admit(msg, opts...); err != nil {
		return nil, normalizeAdmissionError(err)
	}
	return mgr.waitResult(ctx, ch, requestTimeout)
}

func validateClientDispatch(mgr *NestMgr, ctx context.Context, api string, name HandlerName, nestedError error) error {
	if mgr == nil {
		return ErrNestStopped
	}
	if err := mgr.FenceError(); err != nil {
		return err
	}
	if !mgr.Running() {
		return ErrNestStopped
	}
	if name.String() == "" {
		return fmt.Errorf("%w: empty handler name", ErrInvalidMessage)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidMessage)
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(ErrNestCanceled, err)
	}
	if fctx.InNestHandler() {
		current := fctx.CurrentContext()
		return fmt.Errorf("%w: api=%s caller=%s target=%s", nestedError, api, current.Meta.Handler, name.String())
	}
	return nil
}

func prepareClientMessage(ctx context.Context, msg *Msg, name HandlerName, params Params, wait bool, opts ...SendOpt) {
	msg.Name = name.String()
	msg.Params = append(msg.Params[:0], params...)
	opt := resolveSendOptions(opts)
	msg.Cost = opt.Cost
	snapshot := fctx.CaptureSnapshot()
	if !snapshot.Valid {
		snapshot.Valid = true
		snapshot.Config = fctx.RuntimeConfig()
	}
	if wait {
		snapshot.Base = ctx
	} else {
		// An accepted command outlives the request that admitted it, while still
		// carrying context values and Roost request metadata.
		snapshot.Base = context.WithoutCancel(ctx)
		snapshot.SyncWait = 0
	}
	msg.Context = snapshot
}

func resolveSendOptions(opts []SendOpt) sendOptParam {
	var ret sendOptParam
	for _, opt := range opts {
		if opt != nil {
			opt(&ret)
		}
	}
	return ret
}

func (mgr *NestMgr) admit(msg *Msg, opts ...SendOpt) error {
	opt := resolveSendOptions(opts)
	var err error
	if opt.Delay > 0 {
		err = mgr.dispatcher.TryDelaySendMsg(opt.Delay, msg)
	} else {
		err = mgr.dispatcher.TrySendMsg(msg)
	}
	return normalizeAdmissionError(err)
}

func normalizeAdmissionError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, worker.ErrWorkerQueueFull) {
		return errors.Join(ErrQueueFull, err)
	}
	if errors.Is(err, worker.ErrWorkerClosed) {
		return errors.Join(ErrNestStopped, err)
	}
	return err
}

func (mgr *NestMgr) waitResult(ctx context.Context, ch <-chan any, requestTimeout time.Duration) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := mgr.syncTimeout
	if requestTimeout > 0 {
		timeout = requestTimeout
	}
	if timeout <= 0 {
		timeout = NestSyncTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ret := <-ch:
		if err, ok := ret.(error); ok {
			return nil, err
		}
		return ret, nil
	case <-ctx.Done():
		return nil, errors.Join(ErrNestCanceled, ctx.Err())
	case <-timer.C:
		return nil, ErrNestTimeout
	}
}

func validGroups(groups [][]int64) bool {
	if len(groups) == 0 {
		return false
	}
	for _, ids := range groups {
		if len(ids) == 0 {
			return false
		}
	}
	return true
}

func normalizeClientID(id int64) (int64, error) {
	fullID, err := entity.NormalizeFullID(id, entity.EntityKindNone)
	if err != nil {
		return 0, fmt.Errorf("%w: entity id %d: %v", ErrInvalidMessage, id, err)
	}
	return fullID, nil
}

func normalizeClientIDs(ids []int64) ([]int64, error) {
	ret := make([]int64, len(ids))
	for i, id := range ids {
		fullID, err := normalizeClientID(id)
		if err != nil {
			return nil, fmt.Errorf("entity index %d: %w", i, err)
		}
		ret[i] = fullID
	}
	return ret, nil
}

func normalizeClientGroups(groups [][]int64) ([][]int64, error) {
	ret := make([][]int64, len(groups))
	for i, ids := range groups {
		fullIDs, err := normalizeClientIDs(ids)
		if err != nil {
			return nil, fmt.Errorf("entity group %d: %w", i, err)
		}
		ret[i] = fullIDs
	}
	return ret, nil
}
