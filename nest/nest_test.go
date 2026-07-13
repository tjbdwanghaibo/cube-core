package nest

import (
	"bytes"
	"context"
	"github.com/tjbdwanghaibo/cube-core/checkpoint"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/hotcode"
	flog "github.com/tjbdwanghaibo/cube-core/log"
	"github.com/tjbdwanghaibo/cube-core/obs"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	nestLocalKind         entity.EntityKind = 248
	nestRemoteCapableKind entity.EntityKind = 250
	nestRemoteManagedKind entity.EntityKind = 251
	nestUnknownKind       entity.EntityKind = 249
)

func init() {
	entity.RegisterEntityBuilder(&entity.EntityBuilderParam{
		Category:     entity.EntityCategory(3),
		Kind:         nestRemoteCapableKind,
		RemotePolicy: entity.RemotePolicyCapable,
		NoPersist:    true,
		Builder:      func(*entity.EntityCreateParam) (entity.IThreadSafeEntity, error) { return nil, nil },
		Lifetime:     entity.EntityLifetimeRuntimeRebuild,
	})
	entity.RegisterEntityBuilder(&entity.EntityBuilderParam{
		Category:     entity.EntityCategory(3),
		Kind:         nestRemoteManagedKind,
		RemotePolicy: entity.RemotePolicyManaged,
		NoPersist:    true,
		Builder:      func(*entity.EntityCreateParam) (entity.IThreadSafeEntity, error) { return nil, nil },
		LoadPriority: 0,
		DaoBuilders:  nil,
		Lifetime:     entity.EntityLifetimeRemoteManaged,
		Sync:         entity.EntitySyncBuilderParam{},
	})
}

func TestHandlerHotcodePatch(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	name := NewHandlerName("test_hotcode_handler")
	if err := RegisterHandler(name, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		return "origin", nil
	}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	if err := hotcode.Replace(HandlerPatchName(name), BaseHandler(func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		return "patched", nil
	}), hotcode.Meta{Version: "test"}); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	handler := GetHandler(name)
	got, err := handler(nil, nil)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if got != "patched" {
		t.Fatalf("handler = %v, want patched", got)
	}
}

func TestDispatcherObserveStatsRecordsQueueGauge(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })

	dispatcher := NewDispatcher("nest", 2, 1, 64, func(*Msg) {})
	dispatcher.OnInit()
	defer dispatcher.OnDestroy()

	dispatcher.DelaySendMsg(time.Hour, GenMsg(MsgTypeSingle))
	dispatcher.observeStats()

	wantPools := map[string]bool{"main": false, "heartbeat": false, "cost": false}
	delayedSeen := false
	for _, metric := range obs.Snapshot() {
		if metric.Name == "nest.dispatch.delayed_messages" &&
			metric.Labels["dispatcher"] == "nest" &&
			metric.Value == 1 {
			delayedSeen = true
		}
		if metric.Name != "nest.dispatch.queue_len" {
			continue
		}
		if metric.Labels["dispatcher"] != "nest" {
			continue
		}
		pool := metric.Labels["pool"]
		if _, ok := wantPools[pool]; ok && metric.Value == 0 {
			wantPools[pool] = true
		}
	}
	for pool, seen := range wantPools {
		if !seen {
			t.Fatalf("missing queue gauge for pool %s", pool)
		}
	}
	if !delayedSeen {
		t.Fatalf("missing delayed gauge in metrics: %+v", obs.Snapshot())
	}
}

func TestDispatcherStatsCountsProcessedAndSlowMessages(t *testing.T) {
	dispatcher := NewDispatcher("nest", 2, 1, 64, func(*Msg) {})
	mgr := &NestMgr{dispatcher: dispatcher}

	mgr.recordDispatch(199 * time.Millisecond)
	mgr.recordDispatch(200 * time.Millisecond)
	mgr.recordDispatch(350 * time.Millisecond)

	stats := mgr.Stats()
	if stats.Work.ProcessedMessages != 3 {
		t.Fatalf("processed messages = %d, want 3", stats.Work.ProcessedMessages)
	}
	if stats.Work.Slow200msMessages != 2 {
		t.Fatalf("slow messages = %d, want 2", stats.Work.Slow200msMessages)
	}
}

func TestDispatcherDelaySendMsgDoesNotCreatePerMessageGoroutines(t *testing.T) {
	dispatcher := NewDispatcher("nest_delay_test", 1, 0, 64, func(msg *Msg) {
		msg.OnRelease()
	})
	dispatcher.OnInit()
	defer dispatcher.OnDestroy()

	before := runtime.NumGoroutine()
	for i := 0; i < 32; i++ {
		dispatcher.DelaySendMsg(time.Second, GenMsg(MsgTypeSingle))
	}
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()
	if delta := after - before; delta > 8 {
		t.Fatalf("goroutines grew by %d, want a bounded dispatcher scheduler instead of per-message goroutines", delta)
	}
	if stats := dispatcher.Stats(); stats.Delayed != 32 {
		t.Fatalf("delayed stats = %d, want 32", stats.Delayed)
	}
}

func TestShouldLogSlowDispatchUsesThreshold(t *testing.T) {
	if shouldLogSlowDispatch(nestSlowDispatchThreshold - time.Nanosecond) {
		t.Fatal("duration below threshold should not be slow")
	}
	if !shouldLogSlowDispatch(nestSlowDispatchThreshold) {
		t.Fatal("duration at threshold should be slow")
	}
}

func TestShouldTraceSlowDispatchUsesSlowThreshold(t *testing.T) {
	if shouldTraceSlowDispatch(nestSlowDispatchTraceThreshold) {
		t.Fatal("duration at threshold should not dump slow dispatch trace")
	}
	if !shouldTraceSlowDispatch(nestSlowDispatchTraceThreshold + time.Nanosecond) {
		t.Fatal("duration over threshold should dump slow dispatch trace")
	}
}

func TestNestDispatchLogsSlowTraceWithStackAndMsgInfo(t *testing.T) {
	var buf bytes.Buffer
	if err := flog.Init(flog.Options{
		Level:          slog.LevelWarn,
		Output:         &buf,
		DisableGoID:    true,
		DisableFrame:   true,
		DisableContext: true,
	}); err != nil {
		t.Fatalf("init log: %v", err)
	}
	t.Cleanup(func() {
		_ = flog.Init(flog.Options{Level: slog.LevelInfo, Output: io.Discard})
	})

	getter := newMockGetter()
	id := mustBuildCastID(t, 1001, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(id, entity.EntityCategory(1)))
	mgr := &NestMgr{getter: getter}
	msg := &Msg{
		Name:      "auto_heartbeat",
		Type:      MsgTypeSingle,
		Tid:       id,
		Tids:      []int64{id, 456},
		GroupTIds: [][]int64{{1, 2}, {3}},
		Params:    []any{"tick", 7},
		RetChan:   make(chan any, 1),
		Cb1: func([]any, []any) (any, error) {
			time.Sleep(nestSlowDispatchTraceThreshold + 20*time.Millisecond)
			return nil, nil
		},
		RefCount:  2,
		Cost:      true,
		HasRemote: false,
	}
	NestDispatch(mgr, msg)

	out := buf.String()
	for _, want := range []string{
		"slow dispatch trace",
		"handler=auto_heartbeat",
		"type=Single",
		"stack=",
		"msg_info=",
		"param_types",
		"string",
		"int",
		"has_ret_chan",
		"has_callback",
		"groups",
		"tids",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("slow dispatch trace log missing %q:\n%s", want, out)
		}
	}
}

// mockEntity for testing
type mockEntity struct {
	*entity.EntityBase
}

func (m *mockEntity) Base() *entity.EntityBase { return m.EntityBase }

func newMockEntity(id int64, typo entity.EntityCategory) *mockEntity {
	e := &mockEntity{}
	e.EntityBase = entity.NewEntityBase(id, typo, false)
	return e
}

// mockGetter implements entity.Getter
type mockGetter struct {
	mu       sync.RWMutex
	entities map[int64]entity.IThreadSafeEntity
}

type rollbackTestDao struct {
	id      int64
	Tracker checkpoint.DirtyTracker
	Value   int
}

func (d *rollbackTestDao) Id() int64            { return d.id }
func (d *rollbackTestDao) SetId(id int64)       { d.id = id }
func (d *rollbackTestDao) DbName() string       { return "test" }
func (d *rollbackTestDao) CollName() string     { return "rollback_test" }
func (d *rollbackTestDao) Dirty() entity.IDirty { return &d.Tracker }
func (d *rollbackTestDao) CleanDirty()          { d.Tracker.SelfClean() }
func (d *rollbackTestDao) DirtyTracker() *checkpoint.DirtyTracker {
	return &d.Tracker
}
func (d *rollbackTestDao) Marshal() []byte {
	raw, _ := json.Marshal(struct {
		ID    int64 `json:"id"`
		Value int   `json:"value"`
	}{ID: d.id, Value: d.Value})
	return raw
}
func (d *rollbackTestDao) Unmarshal(raw []byte) error {
	var doc struct {
		ID    int64 `json:"id"`
		Value int   `json:"value"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	d.id = doc.ID
	d.Value = doc.Value
	return nil
}

type rollbackTestEntity struct {
	*entity.EntityBase
	dao *rollbackTestDao
}

func (e *rollbackTestEntity) Base() *entity.EntityBase { return e.EntityBase }
func (e *rollbackTestEntity) RangeDao(f func(entity.DaoInterface)) {
	if f != nil {
		f(e.dao)
	}
}

func newMockGetter() *mockGetter {
	return &mockGetter{entities: make(map[int64]entity.IThreadSafeEntity)}
}

func (g *mockGetter) Add(e entity.IThreadSafeEntity) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entities[e.ID()] = e
}

func (g *mockGetter) Get(id int64, _ entity.EntityCategory) (entity.IThreadSafeEntity, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	e, ok := g.entities[id]
	if !ok {
		return nil, ErrEntityNotFound
	}
	return e, nil
}

func (g *mockGetter) GetMany(ids []int64, _ []entity.EntityCategory) ([]entity.IThreadSafeEntity, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ret := make([]entity.IThreadSafeEntity, len(ids))
	for i, id := range ids {
		ret[i] = g.entities[id]
	}
	return ret, nil
}

func TestRegisterAndDispatchHandler(t *testing.T) {
	getter := newMockGetter()
	id := mustBuildCastID(t, 1, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	called := make(chan bool, 1)
	MustRegisterHandler(NewHandlerName("test_handler"), func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error) {
		called <- true
		return "ok", nil
	})

	// Test sync dispatch
	ret, err := Nest.Sync(NewHandlerName("test_handler"), id, nil)
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if ret != "ok" {
		t.Fatalf("Expected 'ok', got %v", ret)
	}

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("Handler was not called")
	}
}

func TestMultiDispatchRequiresFirstEntity(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	missingID := mustBuildCastID(t, 7100, entity.EntityCategory(1), nestLocalKind)
	existingID := mustBuildCastID(t, 7101, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(existingID, entity.EntityCategory(1)))

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_multi_requires_first")
	called := false
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		called = true
		return len(es), nil
	})

	ret, err := Nest.MultiSync(name, []int64{missingID, existingID}, nil)
	if !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("MultiSync err = %v, want %v", err, ErrEntityNotFound)
	}
	if ret != nil {
		t.Fatalf("MultiSync ret = %v, want nil", ret)
	}
	if called {
		t.Fatal("handler should not be called when first entity is missing")
	}
}

func TestMultiDispatchAllowsMissingNonFirstEntity(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	firstID := mustBuildCastID(t, 7110, entity.EntityCategory(1), nestLocalKind)
	missingID := mustBuildCastID(t, 7111, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(firstID, entity.EntityCategory(1)))

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_multi_allows_missing_non_first")
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		if len(es) != 2 {
			return nil, fmt.Errorf("entities len=%d want 2", len(es))
		}
		if es[0] == nil || es[0].ID() != firstID {
			return nil, errors.New("first entity missing")
		}
		if es[1] != nil {
			return nil, errors.New("second entity should be nil")
		}
		return "ok", nil
	})

	ret, err := Nest.MultiSync(name, []int64{firstID, missingID}, nil)
	if err != nil {
		t.Fatalf("MultiSync err = %v", err)
	}
	if ret != "ok" {
		t.Fatalf("MultiSync ret = %v, want ok", ret)
	}
}

func TestMultiGroupDispatchRequiresFirstEntity(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	missingID := mustBuildCastID(t, 7120, entity.EntityCategory(1), nestLocalKind)
	existingID := mustBuildCastID(t, 7121, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(existingID, entity.EntityCategory(1)))

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_multi_group_requires_first")
	called := false
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		called = true
		return len(es), nil
	})

	ret, err := Nest.MultiGroupSync(name, [][]int64{{missingID}, {existingID}}, nil)
	if !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("MultiGroupSync err = %v, want %v", err, ErrEntityNotFound)
	}
	if ret != nil {
		t.Fatalf("MultiGroupSync ret = %v, want nil", ret)
	}
	if called {
		t.Fatal("handler should not be called when first grouped entity is missing")
	}
}

type testRemoteAccessRequest struct {
	Ref entity.RemoteViewRef
}

func (r testRemoteAccessRequest) RemoteAccess() []RemoteAccess {
	return []RemoteAccess{
		{
			Alias: "target_player",
			Ref:   r.Ref,
			Mode:  RemoteAcquireCache,
			Scope: 7,
		},
	}
}

type testRemoteSnapshotResolver struct {
	calls []RemoteAccess
}

func (r *testRemoteSnapshotResolver) ResolveRemoteSnapshot(access RemoteAccess) (entity.RemoteSnapshot, error) {
	r.calls = append(r.calls, access)
	version := uint64(22)
	if access.MinVersion > version {
		version = access.MinVersion
	}
	return entity.RemoteSnapshot{
		EntityID: access.Ref.EntityID,
		Kind:     access.Ref.Kind,
		Scope:    access.Scope,
		Version:  version,
		Data:     "cached-view",
	}, nil
}

func TestNestRemoteAccessPreloadsSnapshotBeforeHandler(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	id := mustBuildCastID(t, 7101, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	getter.Add(e)

	refID := mustBuildCastID(t, 7201, entity.EntityCategory(3), nestRemoteManagedKind)
	ref := entity.RemoteViewRef{EntityID: refID, Kind: nestRemoteManagedKind, Version: 20}
	resolver := &testRemoteSnapshotResolver{}

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithRemoteSnapshotResolver(resolver),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_remote_access_preload")
	key := RemoteKey[string]{Alias: "target_player"}
	MustRegisterHandler(name, func(_ []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		snapshot, ok := Remote(key)
		if !ok {
			return nil, errors.New("missing target_player snapshot")
		}
		return snapshot, nil
	})

	ret, err := Nest.Sync(name, id, NewParams(testRemoteAccessRequest{Ref: ref}))
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if ret != "cached-view" {
		t.Fatalf("remote snapshot = %v, want cached-view", ret)
	}
	if len(resolver.calls) != 1 {
		t.Fatalf("resolver calls = %d, want 1", len(resolver.calls))
	}
	if resolver.calls[0].Alias != "target_player" || resolver.calls[0].Mode != RemoteAcquireCache {
		t.Fatalf("resolver call = %+v", resolver.calls[0])
	}
}

type testRemoteAccessWithTTLRequest struct {
	Ref entity.RemoteViewRef
}

func (r testRemoteAccessWithTTLRequest) RemoteAccess() []RemoteAccess {
	return []RemoteAccess{
		{
			Alias:          "target_player",
			Ref:            r.Ref,
			Mode:           RemoteAcquireCache,
			Scope:          7,
			MinVersion:     r.Ref.Version,
			CacheTTLMillis: 30000,
			Required:       true,
		},
	}
}

func TestNestRemoteKeyAndRemoteAccessTTL(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()

	getter := newMockGetter()
	id := mustBuildCastID(t, 7301, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	getter.Add(e)

	refID := mustBuildCastID(t, 7302, entity.EntityCategory(3), nestRemoteManagedKind)
	ref := entity.RemoteViewRef{EntityID: refID, Kind: nestRemoteManagedKind, Version: 31}
	resolver := &testRemoteSnapshotResolver{}

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithRemoteSnapshotResolver(resolver),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	key := RemoteKey[string]{Alias: "target_player"}
	name := NewHandlerName("test_remote_key_ttl")
	MustRegisterHandler(name, func(_ []entity.IThreadSafeEntity, _ []any, _ ...HandlerOption) (any, error) {
		return MustRemote(key), nil
	})

	ret, err := Nest.Sync(name, id, NewParams(testRemoteAccessWithTTLRequest{Ref: ref}))
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if ret != "cached-view" {
		t.Fatalf("remote snapshot = %v, want cached-view", ret)
	}
	if len(resolver.calls) != 1 {
		t.Fatalf("resolver calls = %d, want 1", len(resolver.calls))
	}
	if resolver.calls[0].MinVersion != 31 || resolver.calls[0].CacheTTLMillis != 30000 || !resolver.calls[0].Required {
		t.Fatalf("resolver call remote policy = %+v", resolver.calls[0])
	}
}

func TestNestHandlerRejectsNestedSyncDispatch(t *testing.T) {
	cases := []struct {
		name string
		run  func(target HandlerName, id1 int64, id2 int64) (any, error)
	}{
		{
			name: "sync",
			run: func(target HandlerName, id1 int64, _ int64) (any, error) {
				return Nest.Sync(target, id1, nil)
			},
		},
		{
			name: "multi_sync",
			run: func(target HandlerName, id1 int64, id2 int64) (any, error) {
				return Nest.MultiSync(target, []int64{id1, id2}, nil)
			},
		},
		{
			name: "multi_group_sync",
			run: func(target HandlerName, id1 int64, id2 int64) (any, error) {
				return Nest.MultiGroupSync(target, [][]int64{{id1}, {id2}}, nil)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetHandlersForTest()
			t.Cleanup(ResetHandlersForTest)

			getter := newMockGetter()
			id1 := mustBuildCastID(t, 13, entity.EntityCategory(1), nestLocalKind)
			id2 := mustBuildCastID(t, 14, entity.EntityCategory(1), nestLocalKind)
			getter.Add(newMockEntity(id1, entity.EntityCategory(1)))
			getter.Add(newMockEntity(id2, entity.EntityCategory(1)))

			InitNest(
				NestOptionWithGetter(getter),
				NestOptionWithWorkerNumAndMsgCap(1, 0, 64),
				NestOptionWithTickDuration(100*time.Millisecond),
			)
			t.Cleanup(StopNest)

			called := make(chan string, 1)
			targetName := NewHandlerName("test_nested_sync_target_" + tc.name)
			outerName := NewHandlerName("test_nested_sync_outer_" + tc.name)
			MustRegisterHandler(targetName, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
				called <- "handler"
				return "inner-ok", nil
			})
			MustRegisterHandler(outerName, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
				return tc.run(targetName, id1, id2)
			})

			ret, err := Nest.Sync(outerName, id1, nil)
			if !errors.Is(err, ErrSyncInHandler) {
				t.Fatalf("outer sync err = %v, want %v", err, ErrSyncInHandler)
			}
			if ret != nil {
				t.Fatalf("outer ret = %v, want nil", ret)
			}
			select {
			case got := <-called:
				t.Fatalf("nested sync dispatch %s should be rejected, got %s", tc.name, got)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

func TestNestHandlerRejectsNestedAsyncDispatch(t *testing.T) {
	cases := []struct {
		name string
		run  func(target HandlerName, id int64, called chan<- string)
	}{
		{
			name: "send",
			run: func(target HandlerName, id int64, _ chan<- string) {
				Nest.Send(target, id, nil)
			},
		},
		{
			name: "multi_send",
			run: func(target HandlerName, id int64, _ chan<- string) {
				Nest.MultiSend(target, []int64{id}, nil)
			},
		},
		{
			name: "multi_group_send",
			run: func(target HandlerName, id int64, _ chan<- string) {
				Nest.MultiGroupSend(target, [][]int64{{id}}, nil)
			},
		},
		{
			name: "broadcast",
			run: func(target HandlerName, id int64, _ chan<- string) {
				Nest.Broadcast(target, []int64{id}, nil)
			},
		},
		{
			name: "anonymous_send",
			run: func(target HandlerName, id int64, called chan<- string) {
				Nest.AnonymousSend(target, id, nil, func(any, Params) (any, error) {
					called <- "anonymous_send"
					return nil, nil
				})
			},
		},
		{
			name: "anonymous_multi_send",
			run: func(target HandlerName, id int64, called chan<- string) {
				Nest.AnonymousMultiSend(target, []int64{id}, nil, func([]any, Params) (any, error) {
					called <- "anonymous_multi_send"
					return nil, nil
				})
			},
		},
		{
			name: "anonymous_broadcast",
			run: func(target HandlerName, id int64, called chan<- string) {
				Nest.AnonymousBroadcast(target, []int64{id}, nil, func(any, Params) (any, error) {
					called <- "anonymous_broadcast"
					return nil, nil
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetHandlersForTest()
			t.Cleanup(ResetHandlersForTest)

			getter := newMockGetter()
			id := mustBuildCastID(t, 16, entity.EntityCategory(1), nestLocalKind)
			getter.Add(newMockEntity(id, entity.EntityCategory(1)))

			InitNest(
				NestOptionWithGetter(getter),
				NestOptionWithWorkerNumAndMsgCap(1, 0, 64),
				NestOptionWithTickDuration(100*time.Millisecond),
			)
			t.Cleanup(StopNest)

			called := make(chan string, 1)
			targetName := NewHandlerName("test_nested_async_target_" + tc.name)
			outerName := NewHandlerName("test_nested_async_outer_" + tc.name)
			MustRegisterHandler(targetName, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
				called <- "handler"
				return nil, nil
			})
			MustRegisterHandler(outerName, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
				tc.run(targetName, id, called)
				return "outer-ok", nil
			})

			ret, err := Nest.Sync(outerName, id, nil)
			if !errors.Is(err, ErrAsyncInHandler) {
				t.Fatalf("outer sync err = %v, want %v", err, ErrAsyncInHandler)
			}
			if ret != nil {
				t.Fatalf("outer ret = %v, want nil", ret)
			}
			select {
			case got := <-called:
				t.Fatalf("nested async dispatch %s should be rejected, got %s", tc.name, got)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

func TestRollbackStateRestoresDaoAndDirty(t *testing.T) {
	getter := newMockGetter()
	id := mustBuildCastID(t, 301, entity.EntityCategory(1), nestLocalKind)
	dao := &rollbackTestDao{id: id, Value: 10}
	e := &rollbackTestEntity{
		EntityBase: entity.NewEntityBase(id, entity.EntityCategory(1), false, nestLocalKind),
		dao:        dao,
	}
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	committed := false
	MustRegisterHandlerWithMeta(NewHandlerName("test_rollback_state"), func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error) {
		ent := es[0].(*rollbackTestEntity)
		ent.dao.Value = 99
		ent.dao.Tracker.MarkPersist(1)
		ent.dao.Tracker.MarkSync(2)
		AfterCommit(func() { committed = true })
		return nil, errors.New("boom")
	}, HandlerMeta{Rollback: RollbackState})

	_, err := Nest.Sync(NewHandlerName("test_rollback_state"), id, nil)
	if err == nil {
		t.Fatal("expected handler error")
	}
	if dao.Value != 10 {
		t.Fatalf("dao value = %d, want rollback to 10", dao.Value)
	}
	if dao.Tracker.Dirty() {
		t.Fatal("dirty mask should be restored to clean")
	}
	if committed {
		t.Fatal("after commit callback should not run on rollback")
	}
}

func TestRollbackDirtyOnlyKeepsStateAndRestoresDirty(t *testing.T) {
	getter := newMockGetter()
	id := mustBuildCastID(t, 302, entity.EntityCategory(1), nestLocalKind)
	dao := &rollbackTestDao{id: id, Value: 10}
	e := &rollbackTestEntity{
		EntityBase: entity.NewEntityBase(id, entity.EntityCategory(1), false, nestLocalKind),
		dao:        dao,
	}
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	MustRegisterHandlerWithMeta(NewHandlerName("test_rollback_dirty"), func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error) {
		ent := es[0].(*rollbackTestEntity)
		ent.dao.Value = 99
		ent.dao.Tracker.MarkPersist(1)
		return nil, errors.New("boom")
	}, HandlerMeta{Rollback: RollbackDirty})

	_, err := Nest.Sync(NewHandlerName("test_rollback_dirty"), id, nil)
	if err == nil {
		t.Fatal("expected handler error")
	}
	if dao.Value != 99 {
		t.Fatalf("dao value = %d, dirty rollback should not restore state", dao.Value)
	}
	if dao.Tracker.Dirty() {
		t.Fatal("dirty mask should be restored to clean")
	}
}

func TestRollbackAfterCommitRunsOnSuccess(t *testing.T) {
	getter := newMockGetter()
	id := mustBuildCastID(t, 303, entity.EntityCategory(1), nestLocalKind)
	dao := &rollbackTestDao{id: id, Value: 10}
	e := &rollbackTestEntity{
		EntityBase: entity.NewEntityBase(id, entity.EntityCategory(1), false, nestLocalKind),
		dao:        dao,
	}
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	committed := make(chan struct{}, 1)
	MustRegisterHandlerWithMeta(NewHandlerName("test_rollback_commit"), func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error) {
		ent := es[0].(*rollbackTestEntity)
		ent.dao.Value = 20
		if !AfterCommit(func() { committed <- struct{}{} }) {
			return nil, errors.New("missing rollback tx")
		}
		return "ok", nil
	}, HandlerMeta{Rollback: RollbackState})

	ret, err := Nest.Sync(NewHandlerName("test_rollback_commit"), id, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if ret != "ok" || dao.Value != 20 {
		t.Fatalf("ret=%v value=%d", ret, dao.Value)
	}
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("after commit callback was not called")
	}
}

func TestSyncUsesRequestSyncWait(t *testing.T) {
	getter := newMockGetter()
	id := mustBuildCastID(t, 101, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	MustRegisterHandler(NewHandlerName("test_request_sync_wait"), func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error) {
		time.Sleep(50 * time.Millisecond)
		return "late", nil
	})

	_, release := fctx.NewContext(fctx.WithSyncWait(5 * time.Millisecond))
	defer release()

	_, err := Nest.Sync(NewHandlerName("test_request_sync_wait"), id, nil)
	if !errors.Is(err, ErrNestTimeout) {
		t.Fatalf("Sync err = %v, want %v", err, ErrNestTimeout)
	}
}

func TestSyncCarriesCurrentContextIntoHandler(t *testing.T) {
	getter := newMockGetter()
	id := mustBuildCastID(t, 102, entity.EntityCategory(1), nestLocalKind)
	e := newMockEntity(id, entity.EntityCategory(1))
	getter.Add(e)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	MustRegisterHandler(NewHandlerName("test_request_context_in_handler"), func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error) {
		c := fctx.CurrentContext()
		if c == nil {
			return nil, errors.New("handler has no request context")
		}
		if c.Base != base {
			return nil, errors.New("handler did not receive caller base context")
		}
		if c.SyncWait != 17*time.Millisecond {
			return nil, errors.New("handler did not receive caller sync wait")
		}
		if c.Meta.Source != "nest" || c.Meta.PlayerID != 777 || c.Meta.Handler != "test_request_context_in_handler" {
			return nil, errors.New("handler meta was not merged correctly")
		}
		return "ok", nil
	})

	_, release := fctx.NewContext(
		fctx.WithBase(base),
		fctx.WithSyncWait(17*time.Millisecond),
		fctx.WithPlayerProtocol(777, 10, 20),
	)
	defer release()

	ret, err := Nest.Sync(NewHandlerName("test_request_context_in_handler"), id, nil)
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if ret != "ok" {
		t.Fatalf("ret = %v, want ok", ret)
	}
}

func TestNestTracePropagatesContextAndRecordsEvents(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })

	getter := newMockGetter()
	id := mustBuildCastID(t, 103, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(id, entity.EntityCategory(1)))

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	name := NewHandlerName("test_nest_trace_context")
	MustRegisterHandler(name, func(es []entity.IThreadSafeEntity, param []any, opts ...HandlerOption) (any, error) {
		c := fctx.CurrentContext()
		if c == nil {
			return nil, errors.New("handler has no request context")
		}
		if !c.Trace.Active() {
			return nil, errors.New("handler trace is not active")
		}
		if c.Trace.TraceID != "trace-nest-test" {
			return nil, errors.New("handler got wrong trace id")
		}
		if c.Trace.Tags["player_id"] != "777" {
			return nil, errors.New("handler got wrong trace tags")
		}
		return "ok", nil
	})

	_, release := fctx.NewContext(
		fctx.WithPlayerProtocol(777, 13003, 9),
		fctx.WithTrace(fctx.TraceMeta{
			TraceID: "trace-nest-test",
			Enabled: true,
			Reason:  "test",
			Tags: map[string]string{
				"player_id": "777",
			},
		}),
	)
	defer release()

	ret, err := Nest.Sync(name, id, nil)
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if ret != "ok" {
		t.Fatalf("ret = %v, want ok", ret)
	}
	for _, event := range []string{"enqueue", "dispatch_start", "dispatch_done"} {
		if !hasNestTraceCounter(event, name.String(), "ok") {
			t.Fatalf("missing nest trace event %q in metrics: %+v", event, obs.Snapshot())
		}
	}
}

func hasNestTraceCounter(event string, handler string, result string) bool {
	for _, metric := range obs.Snapshot() {
		if metric.Name != "nest.trace.events.total" {
			continue
		}
		if metric.Labels["event"] == event &&
			metric.Labels["handler"] == handler &&
			metric.Labels["result"] == result &&
			metric.Value > 0 {
			return true
		}
	}
	return false
}

func TestTickerBasic(t *testing.T) {
	tickCount := make(chan uint64, 10)
	MustRegisterTickCallback(NewTickCallbackName("test_tick"), func(msg TickMsg) {
		tickCount <- msg.FrameNumber
	})

	tk := NewTicker(10 * time.Millisecond)
	tk.Start()
	defer tk.Stop()

	select {
	case frame := <-tickCount:
		if frame == 0 {
			t.Fatal("Frame number should be > 0")
		}
	case <-time.After(time.Second):
		t.Fatal("Tick was not received")
	}
}

func TestTickerStopIdempotentAndCallbackPanicSafe(t *testing.T) {
	MustRegisterTickCallback(NewTickCallbackName("test_tick_panic_safe"), func(msg TickMsg) {
		panic("boom")
	})

	tk := NewTicker(time.Millisecond)
	tk.Start()
	time.Sleep(5 * time.Millisecond)
	tk.Stop()
	tk.Stop()
}

func TestWorkerPool(t *testing.T) {
	results := make(chan string, 10)
	handler := func(msg *Msg) {
		results <- msg.Name
	}

	d := NewDispatcher("test", 2, 1, 32, handler)
	d.OnInit()
	d.OnRun()
	defer d.OnDestroy()

	msg := GenMsg(MsgTypeSingle)
	msg.Tid = 42
	msg.Name = "hello"
	d.SendMsg(msg)

	select {
	case name := <-results:
		if name != "hello" {
			t.Fatalf("Expected 'hello', got %s", name)
		}
	case <-time.After(time.Second):
		t.Fatal("Message was not processed")
	}
}

func TestDispatcherStoppedReturnsErrorForSyncMessage(t *testing.T) {
	d := NewDispatcher("test_stopped", 1, 0, 8, func(msg *Msg) {})
	d.OnInit()
	d.OnRun()
	d.OnDestroy()

	msg, ch := GenSyncMsg(MsgTypeSingle)
	d.SendMsg(msg)

	select {
	case ret := <-ch:
		if ret != ErrNestStopped {
			t.Fatalf("ret = %v, want %v", ret, ErrNestStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stopped error")
	}
}

func TestShouldPrepareRemoteIDUsesRemotePolicyByKind(t *testing.T) {
	category := entity.EntityCategory(3)
	remoteManagedID := mustBuildCastID(t, 1, category, nestRemoteManagedKind)
	if !shouldPrepareRemoteID(entity.ResolveEntityID(remoteManagedID)) {
		t.Fatal("remote-managed id should prepare remote entity")
	}

	localManagedID := int64(uint64(remoteManagedID) & ^(entity.EntityRemoteMask << entity.EntityRemoteShift))
	if !shouldPrepareRemoteID(entity.ResolveEntityID(localManagedID)) {
		t.Fatal("remote-managed kind should prepare even when an input id missed the remote bit")
	}

	remoteCapableID := mustBuildCastID(t, 1, category, nestRemoteCapableKind)
	if shouldPrepareRemoteID(entity.ResolveEntityID(remoteCapableID)) {
		t.Fatal("remote-capable but unmanaged kind should not use remote prepare")
	}

	remoteUnknownKindID := int64(uint64(mustBuildCastID(t, 1, category, nestUnknownKind)) | (entity.EntityRemoteMask << entity.EntityRemoteShift))
	if shouldPrepareRemoteID(entity.ResolveEntityID(remoteUnknownKindID)) {
		t.Fatal("remote bit without remote-managed kind should not prepare remote entity")
	}

	categoryOnlyID := mustBuildCastID(t, 1, category, entity.EntityKindNone)
	if shouldPrepareRemoteID(entity.ResolveEntityID(categoryOnlyID)) {
		t.Fatal("category must not imply remote entity")
	}
}

func TestEntityKindRemoteCapability(t *testing.T) {
	if !entity.IsEntityKindRemoteCapable(nestRemoteCapableKind) {
		t.Fatal("remote=capable kind should be remote-capable")
	}
	if !entity.IsEntityKindRemoteCapable(nestRemoteManagedKind) {
		t.Fatal("remote=managed kind should be remote-capable")
	}
	if entity.IsEntityKindRemoteCapable(nestUnknownKind) {
		t.Fatal("unregistered kind should not be remote-capable")
	}
}

func TestAnonymousBroadcastCallsEachEntity(t *testing.T) {
	getter := newMockGetter()
	id1 := mustBuildCastID(t, 101, entity.EntityCategory(1), nestLocalKind)
	id2 := mustBuildCastID(t, 102, entity.EntityCategory(1), nestLocalKind)
	e1 := newMockEntity(id1, entity.EntityCategory(1))
	e2 := newMockEntity(id2, entity.EntityCategory(1))
	getter.Add(e1)
	getter.Add(e2)

	InitNest(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(2, 1, 64),
		NestOptionWithTickDuration(100*time.Millisecond),
	)
	defer StopNest()

	called := make(chan int64, 2)
	Nest.AnonymousBroadcast(NewHandlerName("test_abroadcast"), []int64{id1, id2}, nil, func(e any, _ Params) (any, error) {
		called <- e.(entity.IThreadSafeEntity).ID()
		return nil, nil
	})

	got := map[int64]bool{}
	for len(got) < 2 {
		select {
		case id := <-called:
			got[id] = true
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for AnonymousBroadcast, got=%v", got)
		}
	}
	if !got[id1] || !got[id2] {
		t.Fatalf("got = %v, want both ids", got)
	}
}
