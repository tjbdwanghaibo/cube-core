package lifecycle

import (
	"context"
	"strings"
	"testing"
)

func TestEmitOrder(t *testing.T) {
	reg := NewRegistry()
	var got []int
	_ = reg.Register(Hook{Phase: PhaseAppInit, Order: 2, Handler: func(context.Context, Event) error {
		got = append(got, 2)
		return nil
	}})
	_ = reg.Register(Hook{Phase: PhaseAppInit, Order: 1, Handler: func(context.Context, Event) error {
		got = append(got, 1)
		return nil
	}})
	if err := reg.Emit(context.Background(), Event{Phase: PhaseAppInit}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("order = %v", got)
	}
}

func TestRegisterReplacesSamePhaseAndName(t *testing.T) {
	reg := NewRegistry()
	var got []string
	if err := reg.Register(Hook{Name: "ready", Phase: PhaseServiceStarted, Order: 1, Handler: func(context.Context, Event) error {
		got = append(got, "old")
		return nil
	}}); err != nil {
		t.Fatalf("Register old: %v", err)
	}
	if err := reg.Register(Hook{Name: "ready", Phase: PhaseServiceStarted, Order: 2, Handler: func(context.Context, Event) error {
		got = append(got, "new")
		return nil
	}}); err != nil {
		t.Fatalf("Register new: %v", err)
	}

	if err := reg.Emit(context.Background(), Event{Phase: PhaseServiceStarted}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(got) != 1 || got[0] != "new" {
		t.Fatalf("hooks executed = %v, want only replacement", got)
	}
}

func TestEmitRecoversHookPanic(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Hook{Name: "panic-hook", Phase: PhaseServiceStarted, Handler: func(context.Context, Event) error {
		panic("boom")
	}}); err != nil {
		t.Fatalf("Register panic hook: %v", err)
	}

	err := reg.Emit(context.Background(), Event{Phase: PhaseServiceStarted})
	if err == nil {
		t.Fatal("Emit error = nil, want panic error")
	}
	if !strings.Contains(err.Error(), "service.started/panic-hook") || !strings.Contains(err.Error(), "panic: boom") {
		t.Fatalf("Emit error = %v, want phase/name panic context", err)
	}
}
