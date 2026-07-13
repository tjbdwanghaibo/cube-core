package ctx

import (
	"sync"
	"testing"
)

func TestCurrentContext_Basic(t *testing.T) {
	c, release := NewContext()
	defer release()

	got := CurrentContext()
	if got != c {
		t.Fatal("CurrentContext should return stored context")
	}
}

func TestCurrentContext_NilWithoutStore(t *testing.T) {
	// Ensure no leftover from other tests
	DeleteContext()

	got := CurrentContext()
	if got != nil {
		t.Fatal("CurrentContext should return nil without store")
	}
}

func TestCurrentContext_Release(t *testing.T) {
	_, release := NewContext()
	release()

	got := CurrentContext()
	if got != nil {
		t.Fatal("CurrentContext should return nil after release")
	}
}

func TestCurrentContext_NestedReleaseRestoresParent(t *testing.T) {
	parent, releaseParent := NewContext(WithSource("parent"))
	defer releaseParent()

	child, releaseChild := NewContext(WithSource("child"))
	if got := CurrentContext(); got != child {
		t.Fatal("CurrentContext should point at child before child release")
	}
	releaseChild()

	if got := CurrentContext(); got != parent {
		t.Fatal("CurrentContext should restore parent after child release")
	}
}

func TestCurrentContext_PerGoroutine(t *testing.T) {
	c1, release1 := NewContext()
	defer release1()

	var c2 *Context
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		innerC, release := NewContext()
		defer release()
		c2 = innerC

		// Should not see c1
		got := CurrentContext()
		if got != innerC {
			t.Error("goroutine should see its own context")
		}
	}()
	wg.Wait()

	// Main goroutine still sees c1
	got := CurrentContext()
	if got != c1 {
		t.Fatal("main goroutine should still see c1")
	}
	_ = c2
}

func TestContext_KV(t *testing.T) {
	c, release := NewContext()
	defer release()

	c.Set("key1", "val1")
	v, ok := c.Get("key1")
	if !ok || v != "val1" {
		t.Fatal("Set/Get should work")
	}

	_, ok = c.Get("nonexist")
	if ok {
		t.Fatal("Get missing key should return false")
	}
}

func TestContextTraceSnapshotClonesTraceMeta(t *testing.T) {
	trace := TraceMeta{
		TraceID: "trace-test",
		Enabled: true,
		Reason:  "player_protocol",
		Sampled: true,
		Tags: map[string]string{
			"player_id": "1001",
		},
	}
	c, release := NewContext(WithTrace(trace))
	defer release()

	if !c.Trace.Active() {
		t.Fatal("trace should be active on current context")
	}
	snap := CaptureSnapshot()
	c.Trace.Tags["player_id"] = "changed"

	child, releaseChild := NewContext(WithSnapshot(snap))
	defer releaseChild()

	if child.Trace.TraceID != "trace-test" || child.Trace.Reason != "player_protocol" {
		t.Fatalf("trace meta = %+v", child.Trace)
	}
	if got := child.Trace.Tags["player_id"]; got != "1001" {
		t.Fatalf("snapshot tag player_id = %q, want 1001", got)
	}
	child.Trace.Tags["player_id"] = "child"
	if got := snap.Trace.Tags["player_id"]; got != "1001" {
		t.Fatalf("snapshot tag mutated through child context: %q", got)
	}
}

func TestGoID(t *testing.T) {
	id := GoID()
	if id <= 0 {
		t.Fatalf("goID should be positive, got %d", id)
	}

	// Different goroutines should have different IDs
	var otherId int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		otherId = GoID()
	}()
	wg.Wait()

	if otherId == id {
		t.Fatal("different goroutines should have different IDs")
	}
}
