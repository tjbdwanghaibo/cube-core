package nest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestDelayedAdmissionIsBounded(t *testing.T) {
	dispatcher := NewDispatcher("delay_limit", 1, 0, 8, func(*Msg) {})
	dispatcher.ConfigureDelayedAdmission(1, time.Second)
	dispatcher.OnInit()
	defer func() { _ = dispatcher.OnDestroyWithContext(context.Background()) }()
	if err := dispatcher.TryDelaySendMsg(500*time.Millisecond, GenMsg(MsgTypeSingle)); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.TryDelaySendMsg(500*time.Millisecond, GenMsg(MsgTypeSingle)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue overflow err=%v", err)
	}
	if err := dispatcher.TryDelaySendMsg(2*time.Second, GenMsg(MsgTypeSingle)); !errors.Is(err, ErrDelayTooLong) {
		t.Fatalf("max delay err=%v", err)
	}
}

func TestTickerStateIsInstanceScoped(t *testing.T) {
	first := NewTicker(time.Hour)
	second := NewTicker(time.Hour)
	first.doTick()
	first.doTick()
	second.doTick()
	if first.CurrentTick() != 2 || second.CurrentTick() != 1 {
		t.Fatalf("ticker state leaked: first=%d second=%d", first.CurrentTick(), second.CurrentTick())
	}
}

func TestDefaultHandlerRegistrationUsesDurableAsyncPolicy(t *testing.T) {
	ResetHandlersForTest()
	defer ResetHandlersForTest()
	name := NewHandlerName("production_default_durability")
	if err := RegisterHandler(name, func(nilEntities []entity.IThreadSafeEntity, nilParams []any, options ...HandlerOption) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	entry, ok := GetHandlerEntry(name)
	if !ok || entry.meta.Rollback != RollbackState || entry.meta.Durability != DurabilityAsync {
		t.Fatalf("unsafe default handler meta: %+v", entry.meta)
	}
}
