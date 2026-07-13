package event

import (
	"sync/atomic"
	"testing"
)

const (
	testEventTypeA EventType = 1
	testEventTypeB EventType = 2
)

type testEventA struct{ Val int }

func (e *testEventA) Type() EventType { return testEventTypeA }

type testEventB struct{ Val int }

func (e *testEventB) Type() EventType { return testEventTypeB }

// testHandler implements EventHandler.
type testHandler struct {
	name     string
	received []EventData
}

func (h *testHandler) InitSub()                    {}
func (h *testHandler) SyncHandleEvent(d EventData) { h.received = append(h.received, d) }

// testAsyncHandler tracks async dispatches.
type testAsyncHandler struct {
	testHandler
	asyncCount atomic.Int32
}

func (h *testAsyncHandler) AsyncHandleEvent(d EventData) {
	h.asyncCount.Add(1)
}

func TestEventMgr_PubSub(t *testing.T) {
	mgr := NewEventMgr()

	h1 := &testHandler{name: "h1"}
	h2 := &testHandler{name: "h2"}

	group := EventGroupType("player")

	// Both subscribe to event A under "player" group
	mgr.Sub(testEventTypeA, h1, nil, []EventGroupType{group})
	mgr.Sub(testEventTypeA, h2, nil, []EventGroupType{group})

	// h1 publishes event A
	ev := &testEventA{Val: 42}
	mgr.Pub(ev, h1, []EventGroupType{group})

	// h1 should receive sync (self), h2 should receive sync (no async dispatcher)
	if len(h1.received) != 1 {
		t.Fatalf("h1 expected 1 event, got %d", len(h1.received))
	}
	if len(h2.received) != 1 {
		t.Fatalf("h2 expected 1 event, got %d", len(h2.received))
	}
	if h1.received[0].(*testEventA).Val != 42 {
		t.Fatal("wrong event value")
	}
}

func TestEventMgr_AsyncDispatch(t *testing.T) {
	mgr := NewEventMgr()

	h1 := &testHandler{name: "publisher"}
	h2 := &testAsyncHandler{testHandler: testHandler{name: "async_sub"}}

	group := EventGroupType("player")

	mgr.Sub(testEventTypeA, h1, nil, []EventGroupType{group})
	mgr.Sub(testEventTypeA, &h2.testHandler, h2, []EventGroupType{group})

	// h1 publishes
	mgr.Pub(&testEventA{Val: 1}, h1, []EventGroupType{group})

	// h1 receives sync
	if len(h1.received) != 1 {
		t.Fatalf("publisher expected 1 sync event, got %d", len(h1.received))
	}
	// h2 receives async
	if h2.asyncCount.Load() != 1 {
		t.Fatalf("async_sub expected 1 async event, got %d", h2.asyncCount.Load())
	}
	// h2's sync handler should NOT be called
	if len(h2.received) != 0 {
		t.Fatalf("async_sub sync handler should not be called, got %d", len(h2.received))
	}
}

func TestEventMgr_Unsub(t *testing.T) {
	mgr := NewEventMgr()

	h1 := &testHandler{name: "h1"}
	group := EventGroupType("player")

	mgr.Sub(testEventTypeA, h1, nil, []EventGroupType{group})
	mgr.Unsub(testEventTypeA, h1)

	mgr.Pub(&testEventA{Val: 1}, nil, []EventGroupType{group})

	if len(h1.received) != 0 {
		t.Fatalf("h1 should not receive after unsub, got %d", len(h1.received))
	}
}

func TestEventMgr_MultiGroup(t *testing.T) {
	mgr := NewEventMgr()

	h1 := &testHandler{name: "h1"}
	h2 := &testHandler{name: "h2"}

	groupA := EventGroupType("activity")
	groupB := EventGroupType("entity")

	// h1 subscribes under groupA, h2 under groupB
	mgr.Sub(testEventTypeA, h1, nil, []EventGroupType{groupA})
	mgr.Sub(testEventTypeA, h2, nil, []EventGroupType{groupB})

	// Publish to groupA only
	mgr.Pub(&testEventA{Val: 1}, nil, []EventGroupType{groupA})

	if len(h1.received) != 1 {
		t.Fatalf("h1 expected 1 event, got %d", len(h1.received))
	}
	if len(h2.received) != 0 {
		t.Fatalf("h2 should not receive (different group), got %d", len(h2.received))
	}
}

func TestEventBus(t *testing.T) {
	mgr := NewEventMgr()

	h := &testHandler{name: "bus_user"}
	bus := NewEventBus(mgr, h, nil, "player")
	bus.SubEvent(testEventTypeA)
	bus.SubEvent(testEventTypeB)

	bus.PubEvent(&testEventA{Val: 10})
	bus.PubEvent(&testEventB{Val: 20})

	if len(h.received) != 2 {
		t.Fatalf("expected 2 events, got %d", len(h.received))
	}

	bus.Destroy()

	h.received = nil
	bus.PubEvent(&testEventA{Val: 30})
	if len(h.received) != 0 {
		t.Fatalf("expected 0 after destroy, got %d", len(h.received))
	}
}

func TestEventMgr_NoDuplicate(t *testing.T) {
	mgr := NewEventMgr()

	h := &testHandler{name: "h"}
	groupA := EventGroupType("a")
	groupB := EventGroupType("b")

	// Subscribe under two groups
	mgr.Sub(testEventTypeA, h, nil, []EventGroupType{groupA, groupB})

	// Publish with both groups — handler should only receive once
	mgr.Pub(&testEventA{Val: 1}, nil, []EventGroupType{groupA, groupB})

	if len(h.received) != 1 {
		t.Fatalf("expected 1 event (dedup), got %d", len(h.received))
	}
}
