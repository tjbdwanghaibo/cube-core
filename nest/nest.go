package nest

import (
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var Nest *NestMgr

const NestSyncTimeout = 5 * time.Second

var (
	ErrHandlerNotFound                = errors.New("nest: handler not found")
	ErrEntityNotFound                 = errors.New("nest: entity not found")
	ErrEntityTypeMismatch             = errors.New("nest: entity type mismatch")
	ErrLockTimeout                    = errors.New("nest: lock timeout")
	ErrNestTimeout                    = errors.New("nest: sync timeout")
	ErrNestCanceled                   = errors.New("nest: sync canceled")
	ErrNestStopped                    = errors.New("nest: stopped")
	ErrParamMismatch                  = errors.New("nest: param mismatch")
	ErrAsyncInHandler                 = errors.New("nest: async dispatch from nest handler")
	ErrSyncInHandler                  = errors.New("nest: sync dispatch from nest handler")
	ErrEntityLockGroupMix             = errors.New("nest: mixed entity lock groups")
	ErrEntityLockGroupChanged         = errors.New("nest: entity lock group changed")
	ErrEntityGroupTransitionPending   = errors.New("nest: entity group transition pending")
	ErrEntityGroupTransitionScheduled = errors.New("nest: entity group transition scheduled")
	ErrInvalidEntityLockGroup         = errors.New("nest: invalid entity lock group")
)

func NewParamCountMismatchError(handler string, got int, want int) error {
	return fmt.Errorf("%w: handler=%s got=%d want=%d", ErrParamMismatch, handler, got, want)
}

func NewParamTypeMismatchError(handler string, idx int, want string, got any) error {
	return fmt.Errorf("%w: handler=%s param=%d want=%s got=%T", ErrParamMismatch, handler, idx, want, got)
}

type NestMgr struct {
	dispatcher             *Dispatcher
	ticker                 *Ticker
	getter                 entity.Getter
	remoteSnapshotResolver RemoteSnapshotResolver
}

type HandlerName struct {
	value string
}

type Params []any

type SingleCallback func(e any, params Params) (any, error)
type MultiCallback func(es []any, params Params) (any, error)

func NewHandlerName(value string) HandlerName {
	return HandlerName{value: value}
}

func NewParams(values ...any) Params {
	return Params(values)
}

func (n HandlerName) String() string {
	return n.value
}

func (mgr *NestMgr) TickDuration() time.Duration {
	if mgr == nil || mgr.ticker == nil {
		return 100 * time.Millisecond
	}
	return mgr.ticker.Duration()
}

type NestOpts struct {
	Getter                 entity.Getter
	RemoteSnapshotResolver RemoteSnapshotResolver
	WorkerNum              int
	HbWorkerNum            int
	MsgCap                 int
	TickDuration           time.Duration
}

type NestOption func(*NestOpts)

var (
	NestOptionWithGetter = func(getter entity.Getter) NestOption {
		return func(opts *NestOpts) {
			opts.Getter = getter
		}
	}
	NestOptionWithRemoteSnapshotResolver = func(resolver RemoteSnapshotResolver) NestOption {
		return func(opts *NestOpts) {
			opts.RemoteSnapshotResolver = resolver
		}
	}
	NestOptionWithWorkerNumAndMsgCap = func(workerNum, hbWorkerNum, msgCap int) NestOption {
		return func(opts *NestOpts) {
			opts.WorkerNum = workerNum
			opts.HbWorkerNum = hbWorkerNum
			opts.MsgCap = msgCap
		}
	}
	NestOptionWithTickDuration = func(tickDuration time.Duration) NestOption {
		return func(opts *NestOpts) {
			opts.TickDuration = tickDuration
		}
	}
)

func InitNest(opts ...NestOption) {
	params := &NestOpts{}
	for _, opt := range opts {
		opt(params)
	}
	var ret *NestMgr
	dis := NewDispatcher("nest", params.WorkerNum, params.HbWorkerNum, params.MsgCap, func(msg *Msg) {
		NestDispatch(ret, msg)
	})
	tk := NewTicker(params.TickDuration)
	ret = &NestMgr{dispatcher: dis, ticker: tk, remoteSnapshotResolver: params.RemoteSnapshotResolver}
	ret.getter = params.Getter
	Nest = ret
	InitGlobalGetter(params.Getter)
	dis.OnInit()
	dis.OnRun()
	tk.Start()

	entity.SendMsg = func(msg any) {
		if m, ok := msg.(*Msg); ok {
			ret.dispatcher.SendMsg(m)
		}
	}
}

func StopNest() {
	if err := StopNestWithContext(context.Background()); err != nil {
		slog.Warn("nest stop interrupted", "err", err)
	}
}

func StopNestWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	if Nest != nil {
		Nest.ticker.Stop()
		err = Nest.dispatcher.OnDestroyWithContext(ctx)
		Nest = nil
	}
	InitGlobalGetter(nil)
	entity.SendMsg = nil
	return err
}

type sendOptParam struct {
	Delay time.Duration
	Cost  bool
}

type SendOpt func(*sendOptParam)

var (
	SendOptionWithDelay = func(delay time.Duration) SendOpt {
		return func(opt *sendOptParam) {
			opt.Delay = delay
		}
	}
	SendOptionIsCost = func() SendOpt {
		return func(opt *sendOptParam) {
			opt.Cost = true
		}
	}
)

// checkRemoteId checks whether a target ID is remote-capable and marks the
// message for remote preparation. The preparer later decides marked remote vs
// local fast path from the runtime marker store.
func checkRemoteId(msg *Msg, id int64) {
	if shouldPrepareRemoteID(entity.ResolveEntityID(id)) {
		msg.HasRemote = true
		msg.Cost = true
	}
}

// checkRemoteIds checks if any target ID in the slice is remote-capable and
// marks the message for remote preparation.
func checkRemoteIds(msg *Msg, ids []int64) {
	for _, id := range ids {
		if shouldPrepareRemoteID(entity.ResolveEntityID(id)) {
			msg.HasRemote = true
			msg.Cost = true
			return
		}
	}
}

// checkRemoteGroups checks if any target ID in grouped slices is
// remote-capable and marks the message for remote preparation.
func checkRemoteGroups(msg *Msg, groups [][]int64) {
	for _, g := range groups {
		for _, id := range g {
			if shouldPrepareRemoteID(entity.ResolveEntityID(id)) {
				msg.HasRemote = true
				msg.Cost = true
				return
			}
		}
	}
}

func shouldPrepareRemoteID(meta entity.EntityIDMeta) bool {
	if meta.Kind == entity.EntityKindNone || !meta.RemoteCapable {
		return false
	}
	if !entity.IsEntityKindRemoteCapable(meta.Kind) {
		return false
	}
	return entity.IsEntityKindRemoteManaged(meta.Kind)
}

func bindMsgContext(msg *Msg, carryBase bool) {
	if msg == nil {
		return
	}
	snapshot := fctx.CaptureSnapshot()
	if !carryBase {
		snapshot.Base = nil
		snapshot.SyncWait = 0
	}
	msg.Context = snapshot
}

func (mgr *NestMgr) AnonymousSend(name HandlerName, id int64, params Params, cb SingleCallback, opts ...SendOpt) {
	if mgr == nil || cb == nil {
		return
	}
	ensureAsyncDispatchAllowed("AnonymousSend", name)
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg := GenMsg(MsgTypeSingle)
	msg.Tid = id
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cb1 = func(es []any, params []any) (any, error) {
		if len(es) != 1 {
			return nil, errors.New("invalid msg type")
		}
		return cb(es[0], Params(params))
	}
	msg.Cost = optP.Cost
	bindMsgContext(msg, false)
	checkRemoteId(msg, id)

	if optP.Delay > 0 {
		mgr.dispatcher.DelaySendMsg(optP.Delay, msg)
	} else {
		mgr.dispatcher.SendMsg(msg)
	}
}

func (mgr *NestMgr) AnonymousMultiSend(name HandlerName, ids []int64, params Params, cb MultiCallback, opts ...SendOpt) {
	if mgr == nil || cb == nil || len(ids) == 0 {
		return
	}
	ensureAsyncDispatchAllowed("AnonymousMultiSend", name)
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg := GenMsg(MsgTypeMulti)
	msg.Tids = ids
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cb1 = func(es []any, params []any) (any, error) {
		return cb(es, Params(params))
	}
	msg.Cost = optP.Cost
	bindMsgContext(msg, false)
	checkRemoteIds(msg, ids)

	if optP.Delay > 0 {
		mgr.dispatcher.DelaySendMsg(optP.Delay, msg)
	} else {
		mgr.dispatcher.SendMsg(msg)
	}
}

func (mgr *NestMgr) AnonymousBroadcast(name HandlerName, ids []int64, params Params, cb SingleCallback, opts ...SendOpt) {
	if mgr == nil || cb == nil || len(ids) == 0 {
		return
	}
	ensureAsyncDispatchAllowed("AnonymousBroadcast", name)
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	mgr.dispatcher.ForEachSpliceBatch(ids, func(eType int, nIds []int64, origIndices []int) {
		msg := GenMsg(MsgTypeBroadcast)
		msg.Tids = nIds
		msg.Name = name.String()
		msg.Params = []any(params)
		msg.Cost = optP.Cost
		msg.Cb1 = func(es []any, params []any) (any, error) {
			if len(es) != 1 {
				return nil, errors.New("invalid msg type")
			}
			return cb(es[0], Params(params))
		}
		bindMsgContext(msg, false)
		checkRemoteIds(msg, nIds)

		if optP.Delay > 0 {
			mgr.dispatcher.DelaySendMsg(optP.Delay, msg)
		} else {
			mgr.dispatcher.SendMsg(msg)
		}
	})
}

func (mgr *NestMgr) Send(name HandlerName, id int64, params Params, opts ...SendOpt) {
	if mgr == nil {
		return
	}
	ensureAsyncDispatchAllowed("Send", name)
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg := GenMsg(MsgTypeSingle)
	msg.Tid = id
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cost = optP.Cost
	bindMsgContext(msg, false)
	checkRemoteId(msg, id)

	if optP.Delay > 0 {
		mgr.dispatcher.DelaySendMsg(optP.Delay, msg)
	} else {
		mgr.dispatcher.SendMsg(msg)
	}
}

func (mgr *NestMgr) Sync(name HandlerName, id int64, params Params, opts ...SendOpt) (any, error) {
	if mgr == nil {
		return nil, ErrNestStopped
	}
	if err := ensureSyncDispatchAllowed("Sync", name); err != nil {
		return nil, err
	}
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg, ch := GenSyncMsg(MsgTypeSingle)
	msg.Tid = id
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cost = optP.Cost
	bindMsgContext(msg, true)
	checkRemoteId(msg, id)

	mgr.dispatcher.SendMsg(msg)
	return waitSyncResult(ch)
}

func (mgr *NestMgr) MultiSend(name HandlerName, ids []int64, params Params, opts ...SendOpt) {
	if mgr == nil || len(ids) == 0 {
		return
	}
	ensureAsyncDispatchAllowed("MultiSend", name)
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg := GenMsg(MsgTypeMulti)
	msg.Tids = ids
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cost = optP.Cost
	bindMsgContext(msg, false)
	checkRemoteIds(msg, ids)

	if optP.Delay > 0 {
		mgr.dispatcher.DelaySendMsg(optP.Delay, msg)
	} else {
		mgr.dispatcher.SendMsg(msg)
	}
}

func (mgr *NestMgr) MultiSync(name HandlerName, ids []int64, params Params, opts ...SendOpt) (any, error) {
	if mgr == nil {
		return nil, ErrNestStopped
	}
	if err := ensureSyncDispatchAllowed("MultiSync", name); err != nil {
		return nil, err
	}
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg, ch := GenSyncMsg(MsgTypeMulti)
	msg.Tids = ids
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cost = optP.Cost
	bindMsgContext(msg, true)
	checkRemoteIds(msg, ids)

	mgr.dispatcher.SendMsg(msg)
	return waitSyncResult(ch)
}

func (mgr *NestMgr) MultiGroupSend(name HandlerName, groups [][]int64, params Params, opts ...SendOpt) {
	if mgr == nil || len(groups) == 0 {
		return
	}
	ensureAsyncDispatchAllowed("MultiGroupSend", name)
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg := GenMsg(MsgTypeMultiGroup)
	msg.GroupTIds = groups
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cost = optP.Cost
	bindMsgContext(msg, false)
	checkRemoteGroups(msg, groups)

	if optP.Delay > 0 {
		mgr.dispatcher.DelaySendMsg(optP.Delay, msg)
	} else {
		mgr.dispatcher.SendMsg(msg)
	}
}

func (mgr *NestMgr) MultiGroupSync(name HandlerName, groups [][]int64, params Params, opts ...SendOpt) (any, error) {
	if mgr == nil {
		return nil, ErrNestStopped
	}
	if err := ensureSyncDispatchAllowed("MultiGroupSync", name); err != nil {
		return nil, err
	}
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	msg, ch := GenSyncMsg(MsgTypeMultiGroup)
	msg.GroupTIds = groups
	msg.Name = name.String()
	msg.Params = []any(params)
	msg.Cost = optP.Cost
	bindMsgContext(msg, true)
	checkRemoteGroups(msg, groups)

	mgr.dispatcher.SendMsg(msg)
	return waitSyncResult(ch)
}

func ensureAsyncDispatchAllowed(api string, name HandlerName) {
	if !fctx.InNestHandler() {
		return
	}
	c := fctx.CurrentContext()
	err := fmt.Errorf("%w: api=%s caller=%s target=%s", ErrAsyncInHandler, api, c.Meta.Handler, name.String())
	slog.Error("nest async dispatch from nest handler rejected",
		"err", err,
		"api", api,
		"caller", c.Meta.Handler,
		"target", name.String(),
		"player", c.Meta.PlayerID,
		"frame", c.Frame,
	)
	panic(err)
}

func ensureSyncDispatchAllowed(api string, name HandlerName) error {
	if !fctx.InNestHandler() {
		return nil
	}
	c := fctx.CurrentContext()
	err := fmt.Errorf("%w: api=%s caller=%s target=%s", ErrSyncInHandler, api, c.Meta.Handler, name.String())
	slog.Error("nest sync dispatch from nest handler rejected",
		"err", err,
		"api", api,
		"caller", c.Meta.Handler,
		"target", name.String(),
		"player", c.Meta.PlayerID,
		"frame", c.Frame,
	)
	return err
}

func waitSyncResult(ch <-chan any) (any, error) {
	timeout := NestSyncTimeout
	var done <-chan struct{}
	if c := fctx.CurrentContext(); c != nil {
		if c.SyncWait > 0 {
			timeout = c.SyncWait
		}
		done = c.Done()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case ret := <-ch:
		if e, ok := ret.(error); ok {
			return nil, e
		}
		return ret, nil
	case <-done:
		return nil, ErrNestCanceled
	case <-timer.C:
		return nil, ErrNestTimeout
	}
}

func (mgr *NestMgr) Broadcast(name HandlerName, ids []int64, params Params, opts ...SendOpt) {
	if mgr == nil || len(ids) == 0 {
		return
	}
	ensureAsyncDispatchAllowed("Broadcast", name)
	optP := &sendOptParam{}
	for _, opt := range opts {
		opt(optP)
	}

	mgr.dispatcher.ForEachSpliceBatch(ids, func(eType int, nIds []int64, origIndices []int) {
		msg := GenMsg(MsgTypeBroadcast)
		msg.Tids = nIds
		msg.Name = name.String()
		msg.Params = []any(params)
		msg.Cost = optP.Cost
		bindMsgContext(msg, false)
		checkRemoteIds(msg, nIds)

		if optP.Delay > 0 {
			mgr.dispatcher.DelaySendMsg(optP.Delay, msg)
		} else {
			mgr.dispatcher.SendMsg(msg)
		}
	})
}
