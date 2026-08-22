package nest

import (
	"context"
	"errors"
	"testing"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestClientRequestCarriesContextAndReturnsResult(t *testing.T) {
	ResetHandlersForTest()
	t.Cleanup(ResetHandlersForTest)
	getter := newMockGetter()
	id := mustBuildCastID(t, 8100, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(id, entity.EntityCategory(1)))

	engine := NewEngine(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 0, 8),
		NestOptionWithSyncTimeout(time.Second),
	)
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Shutdown(context.Background()) })

	type contextKey string
	const key contextKey = "request"
	name := NewHandlerName("client_context")
	MustRegisterMemoryHandler(name, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		if got := fctx.BaseContext().Value(key); got != "value" {
			return nil, errors.New("request context value not propagated")
		}
		return "ok", nil
	})

	ctx := context.WithValue(context.Background(), key, "value")
	ret, err := engine.Request(ctx, name, id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ret != "ok" {
		t.Fatalf("ret=%v want=ok", ret)
	}
}

func TestClientAdmissionReportsQueueFull(t *testing.T) {
	ResetHandlersForTest()
	t.Cleanup(ResetHandlersForTest)
	getter := newMockGetter()
	id := mustBuildCastID(t, 8101, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(id, entity.EntityCategory(1)))
	engine := NewEngine(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 0, 1),
	)
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	name := NewHandlerName("client_backpressure")
	MustRegisterMemoryHandler(name, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil, nil
	})
	if err := engine.Dispatch(context.Background(), name, id, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	var admissionErr error
	for i := 0; i < 64; i++ {
		admissionErr = engine.Dispatch(context.Background(), name, id, nil)
		if admissionErr != nil {
			break
		}
	}
	if !errors.Is(admissionErr, ErrQueueFull) {
		t.Fatalf("dispatch error=%v want ErrQueueFull", admissionErr)
	}
	close(release)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := engine.Dispatch(context.Background(), name, id, nil); !errors.Is(err, ErrNestStopped) {
		t.Fatalf("dispatch after shutdown=%v want ErrNestStopped", err)
	}
}

func TestClientRejectsCanceledContextBeforeAdmission(t *testing.T) {
	getter := newMockGetter()
	engine := NewEngine(NestOptionWithGetter(getter))
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Shutdown(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.Dispatch(ctx, NewHandlerName("canceled"), 1, nil); !errors.Is(err, ErrNestCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want ErrNestCanceled and context.Canceled", err)
	}
}

func TestClientFenceRejectsAdmissionWithCause(t *testing.T) {
	engine := NewEngine(NestOptionWithGetter(newMockGetter()), NestOptionWithWorkerNumAndMsgCap(1, 0, 8))
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	defer engine.Shutdown(context.Background())
	cause := errors.New("WAL fsync outcome unknown")
	engine.Fence(cause)
	err := engine.Dispatch(context.Background(), NewHandlerName("fenced"), mustBuildCastID(t, 999, entity.EntityCategory(1), nestLocalKind), nil)
	if !errors.Is(err, ErrNestFenced) || !errors.Is(err, cause) {
		t.Fatalf("fenced dispatch err=%v", err)
	}
}

func TestShutdownTimeoutDoesNotBlockIndependentEngine(t *testing.T) {
	ResetHandlersForTest()
	t.Cleanup(ResetHandlersForTest)
	getter := newMockGetter()
	id := mustBuildCastID(t, 8102, entity.EntityCategory(1), nestLocalKind)
	getter.Add(newMockEntity(id, entity.EntityCategory(1)))
	engine := NewEngine(NestOptionWithGetter(getter))
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	name := NewHandlerName("shutdown_drain")
	MustRegisterMemoryHandler(name, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		close(entered)
		<-release
		return nil, nil
	})
	if err := engine.Dispatch(context.Background(), name, id, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	err := engine.Shutdown(shutdownCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error=%v want deadline", err)
	}
	second := NewEngine(NestOptionWithGetter(getter))
	if err := second.Start(); err != nil {
		t.Fatalf("independent second start: %v", err)
	}
	close(release)
	if err := engine.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkClientRequestSingle(b *testing.B) {
	ResetHandlersForTest()
	b.Cleanup(ResetHandlersForTest)
	getter := newMockGetter()
	entity.MustRegisterEntityKindCategory(nestLocalKind, entity.EntityCategory(1))
	id, err := entity.BuildEntityID(8200, nestLocalKind)
	if err != nil {
		b.Fatal(err)
	}
	getter.Add(newMockEntity(id, entity.EntityCategory(1)))
	engine := NewEngine(
		NestOptionWithGetter(getter),
		NestOptionWithWorkerNumAndMsgCap(1, 0, 1024),
	)
	if err := engine.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Shutdown(context.Background()) })
	name := NewHandlerName("benchmark_client_request")
	MustRegisterMemoryHandler(name, func([]entity.IThreadSafeEntity, []any, ...HandlerOption) (any, error) {
		return int64(1), nil
	})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.Request(ctx, name, id, nil); err != nil {
			b.Fatal(err)
		}
	}
}
