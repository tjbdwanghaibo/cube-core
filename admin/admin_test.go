package admin

import (
	"context"
	"strings"
	"testing"
)

func TestRegistryExecute(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(CommandDef{
		Name: "echo",
		Handler: func(_ context.Context, cmd Command) (Result, error) {
			payload, err := DecodePayload[map[string]string](cmd)
			if err != nil {
				return Result{}, err
			}
			return Result{Data: map[string]any{"value": payload["value"]}}, nil
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	result, err := reg.Execute(context.Background(), Command{
		Name:    "echo",
		TraceID: "t1",
		Payload: MustPayload(map[string]string{"value": "ok"}),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.OK || result.TraceID != "t1" || result.Data["value"] != "ok" {
		t.Fatalf("result = %+v", result)
	}
}

func TestRegistryRegisterReplacesExistingCommand(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(CommandDef{
		Name: "echo",
		Handler: func(context.Context, Command) (Result, error) {
			return Result{Data: map[string]any{"value": "old"}}, nil
		},
	}); err != nil {
		t.Fatalf("Register old: %v", err)
	}
	if err := reg.Register(CommandDef{
		Name: "echo",
		Handler: func(context.Context, Command) (Result, error) {
			return Result{Data: map[string]any{"value": "new"}}, nil
		},
	}); err != nil {
		t.Fatalf("Register new: %v", err)
	}

	result, err := reg.Execute(context.Background(), Command{Name: "echo"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Data["value"] != "new" {
		t.Fatalf("result = %+v", result)
	}
}

func TestRegistryExecuteRecoversHandlerPanic(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(CommandDef{
		Name: "panic",
		Handler: func(context.Context, Command) (Result, error) {
			panic("boom")
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	result, err := reg.Execute(context.Background(), Command{Name: "panic", TraceID: "trace-panic"})
	if err == nil {
		t.Fatal("Execute error = nil, want panic error")
	}
	if !strings.Contains(err.Error(), "panic: boom") {
		t.Fatalf("Execute error = %v, want panic reason", err)
	}
	if result.OK || result.Name != "panic" || result.TraceID != "trace-panic" {
		t.Fatalf("result = %+v, want failed panic result with identity", result)
	}
	if result.StartedAt == 0 || result.EndedAt == 0 || result.EndedAt < result.StartedAt {
		t.Fatalf("result timing invalid: %+v", result)
	}
}

func TestMetadataRegistryRegistersAndListsSortedCommands(t *testing.T) {
	reg := NewMetadataRegistry()
	if err := reg.Register(CommandMeta{
		Name:        "feature_flag.set",
		Title:       "Set Feature Flag",
		Description: "set feature flag",
		TargetScope: []string{"game"},
		Risk:        RiskMedium,
		PayloadSchema: map[string]any{
			"type":     "object",
			"required": []string{"name", "enabled"},
		},
	}); err != nil {
		t.Fatalf("Register feature_flag.set: %v", err)
	}
	if err := reg.Register(CommandMeta{
		Name:        "config.reload",
		Title:       "Reload Config",
		TargetScope: []string{"game"},
		Risk:        RiskMedium,
	}); err != nil {
		t.Fatalf("Register config.reload: %v", err)
	}

	got, ok := reg.Get("feature_flag.set")
	if !ok {
		t.Fatal("feature_flag.set metadata not found")
	}
	if got.Title != "Set Feature Flag" || got.Risk != RiskMedium {
		t.Fatalf("metadata = %+v", got)
	}
	got.TargetScope[0] = "mutated"
	again, _ := reg.Get("feature_flag.set")
	if again.TargetScope[0] != "game" {
		t.Fatalf("metadata was not cloned: %+v", again)
	}

	list := reg.List()
	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2", len(list))
	}
	if list[0].Name != "config.reload" || list[1].Name != "feature_flag.set" {
		t.Fatalf("list not sorted: %+v", list)
	}
}
