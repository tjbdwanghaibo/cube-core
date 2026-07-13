package health

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRegistrySnapshotAggregatesDependencyHealth(t *testing.T) {
	reg := NewRegistry()
	reg.Register("mongo", CheckerFunc(func(context.Context) Result {
		return Result{Status: StatusOK, Message: "connected"}
	}))
	reg.Register("redis", CheckerFunc(func(context.Context) Result {
		return Result{Status: StatusFail, Message: "ping failed", Err: errors.New("timeout")}
	}))

	snap := reg.Snapshot(context.Background())
	if snap.OK {
		t.Fatalf("snapshot should be unhealthy: %+v", snap)
	}
	if len(snap.Results) != 2 || snap.Results[0].Name != "mongo" || snap.Results[1].Name != "redis" {
		t.Fatalf("results should be stable and sorted: %+v", snap.Results)
	}
	if snap.Results[1].Error != "timeout" {
		t.Fatalf("redis error = %q", snap.Results[1].Error)
	}
}

func TestRegistrySnapshotRecoversCheckerPanic(t *testing.T) {
	reg := NewRegistry()
	reg.Register("redis", CheckerFunc(func(context.Context) Result {
		panic("boom")
	}))

	snap := reg.Snapshot(context.Background())
	if snap.OK {
		t.Fatalf("snapshot should be unhealthy after checker panic: %+v", snap)
	}
	if len(snap.Results) != 1 {
		t.Fatalf("results = %+v, want one result", snap.Results)
	}
	got := snap.Results[0]
	if got.Name != "redis" || got.Status != StatusFail {
		t.Fatalf("panic result = %+v, want redis fail", got)
	}
	if !strings.Contains(got.Error, "panic: boom") {
		t.Fatalf("panic error = %q, want panic reason", got.Error)
	}
}
