package timer

import (
	"testing"
	"time"
)

func TestSchedulerFiresAndPersistsChanges(t *testing.T) {
	var changes []ChangeType
	s := NewScheduler(101, 0, nil, func(change ChangeType, _ Node) {
		changes = append(changes, change)
	})
	var fired int
	s.RegisterHandler(1, func(ctx Context) time.Duration {
		fired++
		if ctx.OwnerID != 101 {
			t.Fatalf("owner id = %d, want 101", ctx.OwnerID)
		}
		if ctx.Node.Param1 != 2 || ctx.Node.Param2 != 3 || string(ctx.Node.Payload) != "p" {
			t.Fatalf("unexpected node: %+v", ctx.Node)
		}
		return 0
	})

	id := s.NewTimer(time.Millisecond, 1, 2, 3, []byte("p"))
	if id == 0 {
		t.Fatal("expected timer id")
	}
	nodes := s.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes))
	}
	s.Tick(nodes[0].End.Add(time.Nanosecond))
	if fired != 1 {
		t.Fatalf("fired = %d, want 1", fired)
	}
	if len(s.Nodes()) != 0 {
		t.Fatalf("expected timer removed after one-shot fire")
	}
	if len(changes) != 2 || changes[0] != ChangeUpsert || changes[1] != ChangeDelete {
		t.Fatalf("changes = %+v, want upsert/delete", changes)
	}
}

func TestSchedulerReschedulesFromHandler(t *testing.T) {
	now := time.Unix(100, 0)
	s := NewScheduler(1, 7, []Node{{
		ID:    8,
		Type:  2,
		End:   now,
		Delay: time.Second,
	}}, nil)
	var fired int
	s.RegisterHandler(2, func(Context) time.Duration {
		fired++
		if fired == 1 {
			return 2 * time.Second
		}
		return 0
	})

	s.Tick(now)
	if fired != 1 {
		t.Fatalf("first fired = %d, want 1", fired)
	}
	next := s.NextTime()
	if !next.Equal(now.Add(2 * time.Second)) {
		t.Fatalf("next = %v, want %v", next, now.Add(2*time.Second))
	}
	s.Tick(now.Add(2 * time.Second))
	if fired != 2 {
		t.Fatalf("second fired = %d, want 2", fired)
	}
}
