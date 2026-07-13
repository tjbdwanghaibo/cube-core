package nest

import (
	"github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/hotcode"
	flog "github.com/tjbdwanghaibo/cube-core/log"
	"github.com/tjbdwanghaibo/cube-core/obs"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	handlerMu  sync.RWMutex
	handlerMap = make(map[HandlerName]handlerEntry)
)

type handlerEntry struct {
	handler BaseHandler
	meta    HandlerMeta
}

func HandlerPatchName(name HandlerName) string {
	return "nest.handler." + name.String()
}

func RegisterHandler(name HandlerName, handler BaseHandler) error {
	return RegisterHandlerWithMeta(name, handler, HandlerMeta{})
}

func RegisterHandlerWithMeta(name HandlerName, handler BaseHandler, meta HandlerMeta) error {
	handlerMu.Lock()
	defer handlerMu.Unlock()
	if _, ok := handlerMap[name]; ok {
		return fmt.Errorf("nest: duplicate handler %q", name.String())
	}
	if err := hotcode.Register(HandlerPatchName(name), handler); err != nil {
		return err
	}
	handlerMap[name] = handlerEntry{handler: handler, meta: meta}
	return nil
}

func MustRegisterHandler(name HandlerName, handler BaseHandler) {
	if err := RegisterHandler(name, handler); err != nil {
		panic(err)
	}
}

func MustRegisterHandlerWithMeta(name HandlerName, handler BaseHandler, meta HandlerMeta) {
	if err := RegisterHandlerWithMeta(name, handler, meta); err != nil {
		panic(err)
	}
}

func GetHandler(name HandlerName) BaseHandler {
	entry, ok := GetHandlerEntry(name)
	if !ok {
		return nil
	}
	return entry.handler
}

func GetHandlerEntry(name HandlerName) (handlerEntry, bool) {
	handlerMu.RLock()
	defer handlerMu.RUnlock()
	entry, ok := handlerMap[name]
	if ok && entry.handler != nil {
		entry.handler = hotcode.Resolve[BaseHandler](HandlerPatchName(name), entry.handler)
	}
	return entry, ok
}

func ResetHandlersForTest() {
	handlerMu.Lock()
	defer handlerMu.Unlock()
	handlerMap = make(map[HandlerName]handlerEntry)
	hotcode.ResetForTest()
}

type HandlerOptionParam struct {
	IsGroup  bool
	GroupLen []int
}

type HandlerOption func(opt *HandlerOptionParam)

var (
	HandlerOptionWithGroup = func(groupLen []int) HandlerOption {
		return func(opt *HandlerOptionParam) {
			opt.IsGroup = true
			opt.GroupLen = make([]int, len(groupLen))
			copy(opt.GroupLen, groupLen)
		}
	}
)

type BaseHandler func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error)

const (
	nestSlowDispatchThreshold      = 200 * time.Millisecond
	nestSlowDispatchTraceThreshold = nestSlowDispatchThreshold
	nestSlowDispatchStackLimit     = 256 * 1024
)

// NestDispatch is the core routing engine for nest messages.
func NestDispatch(mgr *NestMgr, msg *Msg) {
	if msg == nil {
		return
	}
	start := time.Now()
	traceWatch := startSlowDispatchTraceWatch(msg, start)
	releaseCtx := ensureNestContext(msg)
	_, releaseGuardScope := entity.NewGuardScope("nest:" + msg.Name)
	releaseCurrentMsg := pushCurrentNestDispatchMsg(msg)
	var err error
	var ret any
	defer func() {
		if r := recover(); r != nil {
			if recoveredErr, ok := r.(error); ok {
				err = recoveredErr
			} else {
				err = errors.New(fmt.Sprint(r))
			}
			slog.Error("nest dispatch panic", "err", err)
		}
		releaseCurrentMsg()
		releaseGuardScope()
		releaseCtx()
		// Release remote entity locks after dispatch
		if msg.RemoteRelease != nil {
			msg.RemoteRelease()
			msg.RemoteRelease = nil
		}
		if errors.Is(err, ErrEntityGroupTransitionScheduled) {
			err = nil
		}
		if requeuePendingEntityGroupTransition(mgr, msg, err) {
			err = nil
		}
		if requeueTransientDispatch(mgr, msg, err) {
			err = nil
		}
		if msg.RetChan != nil {
			if err != nil {
				msg.RetChan <- err
			} else {
				msg.RetChan <- ret
			}
		} else if err != nil {
			logAsyncDispatchError(msg, err)
		}
		cost := time.Since(start)
		mgr.recordDispatch(cost)
		emitNestTraceEvent(msg, "dispatch_done", dispatchResult(err), cost)
		traceWatch.stop(cost, dispatchResult(err))
		observeDispatch(msg, err, cost)
	}()

	emitNestTraceEvent(msg, "dispatch_start", "ok", 0)

	if err = prepareRemoteSnapshots(msg, mgr.remoteSnapshotResolver); err != nil {
		return
	}

	// Prepare remote entities (acquire distributed lock + load from DB)
	if msg.HasRemote {
		if err = prepareRemoteEntities(msg); err != nil {
			return
		}
	}

	switch msg.Type {
	case MsgTypeSingle:
		if msg.Cb1 != nil {
			ret, err = mgr.anonymousSingleDispatch(msg.Name, msg.Tid, msg.Params, msg.Cb1)
		} else {
			ret, err = mgr.singleDispatch(msg.Name, msg.Tid, msg.Params)
		}
	case MsgTypeMulti:
		if msg.Cb1 != nil {
			ret, err = mgr.anonymousMultiDispatch(msg.Name, msg.Tids, msg.Params, msg.Cb1)
		} else {
			ret, err = mgr.multiDispatch(msg.Name, msg.Tids, msg.Params)
		}
	case MsgTypeMultiGroup:
		ret, err = mgr.multiGroupDispatch(msg.Name, msg.GroupTIds, msg.Params)
	case MsgTypeBroadcast:
		if msg.Cb1 != nil {
			mgr.anonymousBroadcastDispatch(msg.Name, msg.Tids, msg.Params, msg.Cb1)
		} else {
			mgr.broadcastDispatch(msg.Name, msg.Tids, msg.Params)
		}
	case MsgTypeGroupTransition:
		ret, err = mgr.groupTransitionDispatch(msg.GroupTransition)
	}
}

func observeDispatch(msg *Msg, err error, cost time.Duration) {
	if msg == nil {
		return
	}
	labels := obs.Labels{
		"handler": msg.Name,
		"type":    msg.Type.String(),
	}
	if err != nil {
		labels["result"] = "error"
	} else {
		labels["result"] = "ok"
	}
	obs.IncCounter("nest.dispatch.total", labels, 1)
	obs.ObserveDuration("nest.dispatch.cost", labels, cost)
	if msg.HasRemote {
		obs.IncCounter("nest.dispatch.remote.total", labels, 1)
	}
	if shouldLogSlowDispatch(cost) {
		flog.NewELog().Title("nest").Warn("slow dispatch",
			"handler", msg.Name,
			"type", msg.Type.String(),
			"key", msg.Key(),
			"tid", msg.Tid,
			"tids", len(msg.Tids),
			"groups", len(msg.GroupTIds),
			"cost_ms", cost.Milliseconds(),
			"cost", cost.String(),
			"result", labels["result"],
			"cost_pool", msg.Cost,
			"remote", msg.HasRemote,
		)
	}
}

func shouldLogSlowDispatch(cost time.Duration) bool {
	return cost >= nestSlowDispatchThreshold
}

func shouldTraceSlowDispatch(cost time.Duration) bool {
	return cost > nestSlowDispatchTraceThreshold
}

func dispatchResult(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

type slowDispatchTraceWatch struct {
	timer    *time.Timer
	msg      *Msg
	start    time.Time
	done     chan struct{}
	doneOnce sync.Once
	traced   atomic.Bool
}

func startSlowDispatchTraceWatch(msg *Msg, start time.Time) *slowDispatchTraceWatch {
	if msg == nil {
		return nil
	}
	watch := &slowDispatchTraceWatch{
		msg:   msg,
		start: start,
		done:  make(chan struct{}),
	}
	watch.timer = time.AfterFunc(nestSlowDispatchTraceThreshold, func() {
		defer watch.closeDone()
		watch.trace(time.Since(watch.start), "running")
	})
	return watch
}

func (watch *slowDispatchTraceWatch) stop(cost time.Duration, result string) {
	if watch == nil || watch.timer == nil {
		return
	}
	if watch.timer.Stop() {
		watch.closeDone()
		return
	}
	if shouldTraceSlowDispatch(cost) {
		watch.trace(cost, result)
	}
	<-watch.done
}

func (watch *slowDispatchTraceWatch) trace(cost time.Duration, result string) {
	if watch.traced.CompareAndSwap(false, true) {
		logSlowDispatchTrace(newNestMsgDebugInfo(watch.msg), cost, result)
	}
}

func (watch *slowDispatchTraceWatch) closeDone() {
	watch.doneOnce.Do(func() {
		close(watch.done)
	})
}

type nestMsgDebugInfo struct {
	Handler      string
	Type         string
	Key          int64
	Tid          int64
	Tids         []int64
	Groups       [][]int64
	ParamsCount  int
	ParamTypes   []string
	ParamSamples []string
	RefCount     int
	HasRetChan   bool
	HasCallback  bool
	CostPool     bool
	Remote       bool
	ContextValid bool
	TraceID      string
	TraceEnabled bool
	TraceReason  string
	TraceTags    map[string]string
	MsgString    string
}

func newNestMsgDebugInfo(msg *Msg) nestMsgDebugInfo {
	if msg == nil {
		return nestMsgDebugInfo{}
	}
	return nestMsgDebugInfo{
		Handler:      msg.Name,
		Type:         msg.Type.String(),
		Key:          msg.Key(),
		Tid:          msg.Tid,
		Tids:         cloneInt64s(msg.Tids),
		Groups:       cloneInt64Groups(msg.GroupTIds),
		ParamsCount:  len(msg.Params),
		ParamTypes:   nestParamTypes(msg.Params),
		ParamSamples: nestParamSamples(msg.Params),
		RefCount:     msg.RefCount,
		HasRetChan:   msg.RetChan != nil,
		HasCallback:  msg.Cb1 != nil,
		CostPool:     msg.Cost,
		Remote:       msg.HasRemote,
		ContextValid: msg.Context.Valid,
		TraceID:      msg.Context.Trace.TraceID,
		TraceEnabled: msg.Context.Trace.Active(),
		TraceReason:  msg.Context.Trace.Reason,
		TraceTags:    msg.Context.Trace.Tags,
		MsgString:    msg.String(),
	}
}

func (info nestMsgDebugInfo) fields() map[string]any {
	return map[string]any{
		"handler":       info.Handler,
		"type":          info.Type,
		"key":           info.Key,
		"tid":           info.Tid,
		"tids":          info.Tids,
		"tids_count":    len(info.Tids),
		"groups":        info.Groups,
		"groups_count":  len(info.Groups),
		"params_count":  info.ParamsCount,
		"param_types":   info.ParamTypes,
		"param_samples": info.ParamSamples,
		"ref_count":     info.RefCount,
		"has_ret_chan":  info.HasRetChan,
		"has_callback":  info.HasCallback,
		"cost_pool":     info.CostPool,
		"remote":        info.Remote,
		"context_valid": info.ContextValid,
		"trace_id":      info.TraceID,
		"trace_enabled": info.TraceEnabled,
		"trace_reason":  info.TraceReason,
		"trace_tags":    info.TraceTags,
		"msg":           info.MsgString,
	}
}

func cloneInt64s(src []int64) []int64 {
	if len(src) == 0 {
		return nil
	}
	dst := make([]int64, len(src))
	copy(dst, src)
	return dst
}

func cloneInt64Groups(src [][]int64) [][]int64 {
	if len(src) == 0 {
		return nil
	}
	dst := make([][]int64, len(src))
	for i, group := range src {
		dst[i] = cloneInt64s(group)
	}
	return dst
}

func nestParamTypes(params []any) []string {
	if len(params) == 0 {
		return nil
	}
	ret := make([]string, len(params))
	for i, param := range params {
		if param == nil {
			ret[i] = "nil"
		} else {
			ret[i] = fmt.Sprintf("%T", param)
		}
	}
	return ret
}

func nestParamSamples(params []any) []string {
	if len(params) == 0 {
		return nil
	}
	ret := make([]string, len(params))
	for i, param := range params {
		ret[i] = truncateNestDebugText(fmt.Sprintf("%#v", param), 256)
	}
	return ret
}

func truncateNestDebugText(text string, limit int) string {
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit] + "...(truncated)"
}

func logSlowDispatchTrace(info nestMsgDebugInfo, cost time.Duration, result string) {
	stack, truncated := captureSlowDispatchStack()
	flog.NewELog().Title("nest").Warn("slow dispatch trace",
		"handler", info.Handler,
		"type", info.Type,
		"key", info.Key,
		"cost_ms", cost.Milliseconds(),
		"cost", cost.String(),
		"result", result,
		"msg_info", info.fields(),
		"stack_truncated", truncated,
		"stack", stack,
	)
}

type nestTraceEventInfo struct {
	Active      bool
	Handler     string
	Type        string
	Key         int64
	Tid         int64
	TidsCount   int
	GroupsCount int
	Remote      bool
	TraceID     string
	Reason      string
	Tags        map[string]string
	Source      string
	PlayerID    int64
	MsgID       uint32
	Seq         uint32
}

func newNestTraceEventInfo(msg *Msg) nestTraceEventInfo {
	if msg == nil || !msg.Context.Trace.Active() {
		return nestTraceEventInfo{}
	}
	return nestTraceEventInfo{
		Active:      true,
		Handler:     msg.Name,
		Type:        msg.Type.String(),
		Key:         msg.Key(),
		Tid:         msg.Tid,
		TidsCount:   len(msg.Tids),
		GroupsCount: len(msg.GroupTIds),
		Remote:      msg.HasRemote,
		TraceID:     msg.Context.Trace.TraceID,
		Reason:      msg.Context.Trace.Reason,
		Tags:        msg.Context.Trace.Clone().Tags,
		Source:      msg.Context.Meta.Source,
		PlayerID:    msg.Context.Meta.PlayerID,
		MsgID:       msg.Context.Meta.MsgID,
		Seq:         msg.Context.Meta.Seq,
	}
}

func emitNestTraceEvent(msg *Msg, event string, result string, cost time.Duration) {
	emitNestTraceEventInfo(newNestTraceEventInfo(msg), event, result, cost)
}

func emitNestTraceEventInfo(info nestTraceEventInfo, event string, result string, cost time.Duration) {
	if !info.Active {
		return
	}
	labels := obs.Labels{
		"handler": info.Handler,
		"type":    info.Type,
		"event":   event,
		"result":  result,
	}
	obs.IncCounter("nest.trace.events.total", labels, 1)
	if cost > 0 {
		obs.ObserveDuration("nest.trace.cost", labels, cost)
	}
	flog.NewELog().Title("nest_trace").Info("nest trace event",
		"trace_id", info.TraceID,
		"reason", info.Reason,
		"event", event,
		"result", result,
		"handler", info.Handler,
		"type", info.Type,
		"key", info.Key,
		"tid", info.Tid,
		"tids", info.TidsCount,
		"groups", info.GroupsCount,
		"cost_ms", cost.Milliseconds(),
		"cost", cost.String(),
		"remote", info.Remote,
		"source", info.Source,
		"player", info.PlayerID,
		"msg_id", info.MsgID,
		"seq", info.Seq,
		"tags", info.Tags,
	)
}

func captureSlowDispatchStack() (string, bool) {
	buf := make([]byte, nestSlowDispatchStackLimit)
	n := runtime.Stack(buf, true)
	return string(buf[:n]), n == len(buf)
}

func logAsyncDispatchError(msg *Msg, err error) {
	if msg == nil || err == nil {
		return
	}
	flog.NewELog().Title("nest").Warn("async handler failed",
		"handler", msg.Name,
		"type", msg.Type.String(),
		"key", msg.Key(),
		"tid", msg.Tid,
		"tids", len(msg.Tids),
		"groups", len(msg.GroupTIds),
		"cost", msg.Cost,
		"remote", msg.HasRemote,
		"err", err,
	)
}

func ensureNestContext(msg *Msg) func() {
	meta := ctx.RequestMeta{Source: "nest"}
	if msg != nil {
		meta.Handler = msg.Name
	}
	frame := CurTick()
	var opts []ctx.Option
	if msg != nil && msg.Context.Valid {
		opts = append(opts, ctx.WithSnapshot(msg.Context))
	}
	c, release := ctx.NewContext(opts...)
	c.MergeMeta(meta)
	c.Frame = frame
	return release
}

func (mgr *NestMgr) singleDispatch(name string, id int64, params []any) (any, error) {
	entry, ok := GetHandlerEntry(NewHandlerName(name))
	if !ok || entry.handler == nil {
		return nil, ErrHandlerNotFound
	}
	fullID, err := entity.NormalizeFullID(id, entity.EntityKindNone)
	if err != nil {
		return nil, err
	}
	meta := entity.ResolveEntityID(fullID)
	e, err := mgr.getter.Get(meta.FullID, meta.Category)
	if err != nil {
		return nil, err
	}
	if !e.Touch() {
		return nil, ErrEntityNotFound
	}
	defer e.UnTouch()
	guard := entity.GetEntityGuard()
	es := []entity.IThreadSafeEntity{e}
	if err := rejectPendingEntityGroupTransition(es); err != nil {
		return nil, err
	}
	_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, es)
	if err != nil {
		return nil, err
	}
	defer releaseLocks()
	return invokeWithRollback(entry.meta, es, func() (any, error) {
		return entry.handler(es, params)
	})
}

func (mgr *NestMgr) anonymousSingleDispatch(name string, id int64, params []any, cb func(es []any, params []any) (any, error)) (any, error) {
	if cb == nil {
		return nil, ErrHandlerNotFound
	}
	fullID, err := entity.NormalizeFullID(id, entity.EntityKindNone)
	if err != nil {
		return nil, err
	}
	meta := entity.ResolveEntityID(fullID)
	e, err := mgr.getter.Get(meta.FullID, meta.Category)
	if err != nil {
		return nil, err
	}
	if !e.Touch() {
		return nil, ErrEntityNotFound
	}
	defer e.UnTouch()
	guard := entity.GetEntityGuard()
	es := []entity.IThreadSafeEntity{e}
	if err := rejectPendingEntityGroupTransition(es); err != nil {
		return nil, err
	}
	_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, es)
	if err != nil {
		return nil, err
	}
	defer releaseLocks()
	return cb([]any{e}, params)
}

func (mgr *NestMgr) multiDispatch(name string, ids []int64, params []any) (any, error) {
	entry, ok := GetHandlerEntry(NewHandlerName(name))
	if !ok || entry.handler == nil {
		return nil, ErrHandlerNotFound
	}
	fullIDs, fullIDCategories, err := normalizeFullIDs(ids)
	if err != nil {
		return nil, err
	}
	es, err := mgr.getter.GetMany(fullIDs, fullIDCategories)
	if err != nil {
		return nil, err
	}

	var lockEs []entity.IThreadSafeEntity
	var touchedEs []entity.IThreadSafeEntity
	for i, e := range es {
		if e != nil && e.Touch() {
			lockEs = append(lockEs, e)
			touchedEs = append(touchedEs, e)
		} else {
			es[i] = nil
		}
	}
	defer func() {
		for _, te := range touchedEs {
			te.UnTouch()
		}
	}()

	if firstDispatchEntityMissing(es) {
		return nil, ErrEntityNotFound
	}
	if len(lockEs) == 0 {
		return nil, ErrEntityNotFound
	}
	if err := rejectPendingEntityGroupTransition(lockEs); err != nil {
		return nil, err
	}

	SortEntity(lockEs)
	guard := entity.GetEntityGuard()
	_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, lockEs)
	if err != nil {
		return nil, err
	}
	defer releaseLocks()
	return invokeWithRollback(entry.meta, es, func() (any, error) {
		return entry.handler(es, params)
	})
}

func (mgr *NestMgr) anonymousMultiDispatch(name string, ids []int64, params []any, cb func(es []any, params []any) (any, error)) (any, error) {
	if cb == nil {
		return nil, ErrHandlerNotFound
	}
	fullIDs, fullIDCategories, err := normalizeFullIDs(ids)
	if err != nil {
		return nil, err
	}
	es, err := mgr.getter.GetMany(fullIDs, fullIDCategories)
	if err != nil {
		return nil, err
	}

	var lockEs []entity.IThreadSafeEntity
	var touchedEs []entity.IThreadSafeEntity
	for i, e := range es {
		if e != nil && e.Touch() {
			lockEs = append(lockEs, e)
			touchedEs = append(touchedEs, e)
		} else {
			es[i] = nil
		}
	}
	defer func() {
		for _, te := range touchedEs {
			te.UnTouch()
		}
	}()

	if firstDispatchEntityMissing(es) {
		return nil, ErrEntityNotFound
	}
	if len(lockEs) == 0 {
		return nil, ErrEntityNotFound
	}
	if err := rejectPendingEntityGroupTransition(lockEs); err != nil {
		return nil, err
	}

	SortEntity(lockEs)
	guard := entity.GetEntityGuard()
	_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, lockEs)
	if err != nil {
		return nil, err
	}
	defer releaseLocks()

	anyEs := make([]any, len(es))
	for i, e := range es {
		anyEs[i] = e
	}
	return cb(anyEs, params)
}

func (mgr *NestMgr) multiGroupDispatch(name string, groups [][]int64, params []any) (any, error) {
	entry, ok := GetHandlerEntry(NewHandlerName(name))
	if !ok || entry.handler == nil {
		return nil, ErrHandlerNotFound
	}
	ids := make([]int64, 0)
	groupLen := make([]int, len(groups))
	for i, group := range groups {
		groupLen[i] = len(group)
		ids = append(ids, group...)
	}
	fullIDs, fullIDCategories, err := normalizeFullIDs(ids)
	if err != nil {
		return nil, err
	}
	es, err := mgr.getter.GetMany(fullIDs, fullIDCategories)
	if err != nil {
		return nil, err
	}

	var lockEs []entity.IThreadSafeEntity
	var touchedEs []entity.IThreadSafeEntity
	for i, e := range es {
		if e != nil && e.Touch() {
			lockEs = append(lockEs, e)
			touchedEs = append(touchedEs, e)
		} else {
			es[i] = nil
		}
	}
	defer func() {
		for _, te := range touchedEs {
			te.UnTouch()
		}
	}()

	if firstDispatchEntityMissing(es) {
		return nil, ErrEntityNotFound
	}
	if len(lockEs) == 0 {
		return nil, ErrEntityNotFound
	}
	if err := rejectPendingEntityGroupTransition(lockEs); err != nil {
		return nil, err
	}
	SortEntity(lockEs)
	guard := entity.GetEntityGuard()
	_, releaseLocks, err := lockDispatchEntitiesForHandler(guard, lockEs)
	if err != nil {
		return nil, err
	}
	defer releaseLocks()
	return invokeWithRollback(entry.meta, es, func() (any, error) {
		return entry.handler(es, params, HandlerOptionWithGroup(groupLen))
	})
}

func firstDispatchEntityMissing(es []entity.IThreadSafeEntity) bool {
	return len(es) == 0 || es[0] == nil
}

func lockDispatchEntities(guard *entity.EntityGuard, lockEs []entity.IThreadSafeEntity) ([]entity.IThreadSafeEntity, error) {
	if guard == nil {
		return nil, ErrLockTimeout
	}
	useTryLock := len(guard.Entities()) > 0 && !guard.CheckContainAllLock(lockEs)
	acquired := make([]entity.IThreadSafeEntity, 0, len(lockEs))
	for _, e := range lockEs {
		if e == nil {
			continue
		}
		if _, exists := guard.Entities()[e.GUId()]; exists {
			continue
		}
		ok := false
		if useTryLock {
			ok = tryRequireDispatchEntity(guard, e)
		} else {
			ok = guard.RequireEntity(e)
		}
		if !ok {
			releaseDispatchEntities(guard, acquired)
			return nil, ErrLockTimeout
		}
		acquired = append(acquired, e)
	}
	return acquired, nil
}

func tryRequireDispatchEntity(guard *entity.EntityGuard, ent entity.IThreadSafeEntity) bool {
	if guard == nil || ent == nil {
		return false
	}
	gID := ent.GUId()
	mu := ent.GetMutex()
	if gID == 0 || mu == nil {
		return false
	}
	if _, exists := guard.Entities()[gID]; exists {
		return true
	}
	if !mu.TryLock() {
		return false
	}
	if ent.IsClear() || ent.IsRemoved() {
		mu.Unlock()
		return false
	}
	guard.GuardEntity(ent)
	return true
}

func releaseDispatchLocks(guard *entity.EntityGuard, acquired []entity.IThreadSafeEntity) {
	if guard == nil {
		return
	}
	if entity.CurrentGuardScope() == nil {
		entity.EntityGuardRelease(guard)
		return
	}
	releaseDispatchEntities(guard, acquired)
}

func releaseDispatchEntities(guard *entity.EntityGuard, acquired []entity.IThreadSafeEntity) {
	if guard == nil {
		return
	}
	for i := len(acquired) - 1; i >= 0; i-- {
		if acquired[i] != nil {
			guard.ReleaseEntity(acquired[i].GUId())
		}
	}
}

func (mgr *NestMgr) broadcastDispatch(name string, ids []int64, params []any) {
	entry, ok := GetHandlerEntry(NewHandlerName(name))
	if !ok || entry.handler == nil {
		return
	}
	guard := entity.GetEntityGuard()
	defer entity.EntityGuardRelease(guard)

	oneEntity := make([]entity.IThreadSafeEntity, 1)
	for _, id := range ids {
		fullID, err := entity.NormalizeFullID(id, entity.EntityKindNone)
		if err != nil {
			continue
		}
		meta := entity.ResolveEntityID(fullID)
		e, err := mgr.getter.Get(meta.FullID, meta.Category)
		if err != nil {
			continue
		}
		if !e.Touch() {
			continue
		}
		if !guard.RequireEntity(e) {
			e.UnTouch()
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("nest broadcast handler panic", "id", meta.FullID, "handler", name, "err", r)
				}
				oneEntity[0] = nil
				guard.ReleaseEntity(e.GUId())
				e.UnTouch()
			}()
			oneEntity[0] = e
			if _, err := invokeWithRollback(entry.meta, oneEntity, func() (any, error) {
				return entry.handler(oneEntity, params)
			}); err != nil {
				slog.Debug("nest broadcast handler failed", "id", meta.FullID, "handler", name, "err", err)
			}
		}()
	}
}

func (mgr *NestMgr) anonymousBroadcastDispatch(name string, ids []int64, params []any, cb func(es []any, params []any) (any, error)) {
	if cb == nil {
		return
	}
	guard := entity.GetEntityGuard()
	defer entity.EntityGuardRelease(guard)

	oneEntity := make([]any, 1)
	for _, id := range ids {
		fullID, err := entity.NormalizeFullID(id, entity.EntityKindNone)
		if err != nil {
			continue
		}
		meta := entity.ResolveEntityID(fullID)
		e, err := mgr.getter.Get(meta.FullID, meta.Category)
		if err != nil {
			continue
		}
		if !e.Touch() {
			continue
		}
		if !guard.RequireEntity(e) {
			e.UnTouch()
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("nest anonymous broadcast panic", "id", meta.FullID, "handler", name, "err", r)
				}
				oneEntity[0] = nil
				guard.ReleaseEntity(e.GUId())
				e.UnTouch()
			}()
			oneEntity[0] = e
			if _, err := cb(oneEntity, params); err != nil {
				slog.Debug("nest anonymous broadcast callback failed", "id", meta.FullID, "handler", name, "err", err)
			}
		}()
	}
}

func normalizeFullIDs(ids []int64) ([]int64, []entity.EntityCategory, error) {
	fullIDs := make([]int64, len(ids))
	fullIDCategories := make([]entity.EntityCategory, len(ids))
	for i, id := range ids {
		fullID, err := entity.NormalizeFullID(id, entity.EntityKindNone)
		if err != nil {
			return nil, nil, err
		}
		meta := entity.ResolveEntityID(fullID)
		fullIDs[i] = meta.FullID
		fullIDCategories[i] = meta.Category
	}
	return fullIDs, fullIDCategories, nil
}

// SortEntity sorts entities for deadlock-free lock acquisition.
func SortEntity(es []entity.IThreadSafeEntity) {
	if len(es) == 0 {
		return
	}
	sort.Slice(es, func(i, j int) bool {
		guidI, guidJ := es[i].GUId(), es[j].GUId()
		return cmpGuidFunc(guidI, guidJ)
	})
}

func SortEntityId(guids []int64) {
	sort.Slice(guids, func(i, j int) bool {
		return cmpGuidFunc(guids[i], guids[j])
	})
}

func cmpGuidFunc(guidI, guidJ int64) bool {
	groupI, groupJ := entity.GetEntityGroup(guidI), entity.GetEntityGroup(guidJ)
	if groupI != groupJ {
		return groupI < groupJ
	}
	return guidI < guidJ
}

// prepareRemoteEntities calls the application-layer hook to acquire distributed locks
// and load remote entities before dispatch.
func prepareRemoteEntities(msg *Msg) error {
	ids := extractRemoteIds(msg)
	if len(ids) == 0 {
		return nil
	}
	release, ok, err := entity.PrepareRemoteEntities(ids)
	if !ok {
		return nil
	}
	if err != nil {
		return err
	}
	msg.RemoteRelease = release
	return nil
}

// extractRemoteIds collects entity IDs whose ID remote bit and entity kind
// indicate remote-managed lifecycle.
func extractRemoteIds(msg *Msg) []int64 {
	var ids []int64
	if msg.Tid != 0 {
		meta := entity.ResolveEntityID(msg.Tid)
		if shouldPrepareRemoteID(meta) {
			ids = append(ids, meta.FullID)
		}
	}
	for _, id := range msg.Tids {
		meta := entity.ResolveEntityID(id)
		if shouldPrepareRemoteID(meta) {
			ids = append(ids, meta.FullID)
		}
	}
	for _, group := range msg.GroupTIds {
		for _, id := range group {
			meta := entity.ResolveEntityID(id)
			if shouldPrepareRemoteID(meta) {
				ids = append(ids, meta.FullID)
			}
		}
	}
	return ids
}
