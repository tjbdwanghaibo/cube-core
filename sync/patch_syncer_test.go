package sync

import (
	"context"
	"strings"
	"testing"
)

type testPatch struct {
	PlayerID int64  `json:"player_id"`
	Name     string `json:"name,omitempty"`
}

func (p testPatch) HasData() bool {
	return p.Name != ""
}

func TestPatchSyncerPublishesAndAppliesRemotePatch(t *testing.T) {
	bus := newPatchFakeBus()
	applied := make([]testPatch, 0, 1)
	syncer := NewPatchSyncer[testPatch](bus, PatchSyncerConfig[testPatch]{
		Topic:    "player.patch",
		LocalSid: 2,
		KeyOf:    func(p testPatch) int64 { return p.PlayerID },
		WithKey: func(p testPatch, key int64) testPatch {
			p.PlayerID = key
			return p
		},
		HasData: func(p testPatch) bool { return p.HasData() },
		Apply: func(_ context.Context, p testPatch) error {
			applied = append(applied, p)
			return nil
		},
	})
	if err := syncer.Start(); err != nil {
		t.Fatal(err)
	}
	defer syncer.Stop()

	if err := syncer.Publish(context.Background(), testPatch{PlayerID: 7, Name: "hero"}); err != nil {
		t.Fatal(err)
	}

	if len(applied) != 0 {
		t.Fatalf("local publish should be skipped by same sid, applied=%+v", applied)
	}
	if err := bus.Publish(&SyncMsg{
		Topic:   "player.patch",
		Key:     8,
		Version: 9,
		Data:    []byte(`{"name":"remote"}`),
		FromSid: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if len(applied) != 1 || applied[0].PlayerID != 8 || applied[0].Name != "remote" {
		t.Fatalf("applied patches = %+v", applied)
	}
}

func TestPatchSyncerRejectsMismatchedKey(t *testing.T) {
	bus := newPatchFakeBus()
	syncer := NewPatchSyncer[testPatch](bus, PatchSyncerConfig[testPatch]{
		Topic: "player.patch",
		KeyOf: func(p testPatch) int64 { return p.PlayerID },
		Apply: func(context.Context, testPatch) error {
			t.Fatal("apply should not be called")
			return nil
		},
	})
	if err := syncer.Start(); err != nil {
		t.Fatal(err)
	}
	defer syncer.Stop()

	err := bus.Publish(&SyncMsg{
		Topic: "player.patch",
		Key:   8,
		Data:  []byte(`{"player_id":9,"name":"bad"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "key mismatch") {
		t.Fatalf("err = %v, want key mismatch", err)
	}
}

func TestPatchSyncerSkipsEmptyPatch(t *testing.T) {
	bus := newPatchFakeBus()
	applied := 0
	syncer := NewPatchSyncer[testPatch](bus, PatchSyncerConfig[testPatch]{
		Topic:   "player.patch",
		KeyOf:   func(p testPatch) int64 { return p.PlayerID },
		HasData: func(p testPatch) bool { return p.HasData() },
		Apply: func(context.Context, testPatch) error {
			applied++
			return nil
		},
	})
	if err := syncer.Start(); err != nil {
		t.Fatal(err)
	}
	defer syncer.Stop()

	if err := syncer.Publish(context.Background(), testPatch{PlayerID: 7}); err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("empty publish applied %d patches", applied)
	}
}

type patchFakeBus struct {
	handlers map[string][]Handler
}

func newPatchFakeBus() *patchFakeBus {
	return &patchFakeBus{handlers: make(map[string][]Handler)}
}

func (b *patchFakeBus) Publish(msg *SyncMsg) error {
	if msg == nil {
		return nil
	}
	for _, h := range b.handlers[msg.Topic] {
		if err := h(msg); err != nil {
			return err
		}
	}
	return nil
}

func (b *patchFakeBus) Subscribe(topic string, handler Handler) (func(), error) {
	b.handlers[topic] = append(b.handlers[topic], handler)
	idx := len(b.handlers[topic]) - 1
	return func() {
		handlers := b.handlers[topic]
		if idx < 0 || idx >= len(handlers) {
			return
		}
		b.handlers[topic] = append(handlers[:idx], handlers[idx+1:]...)
	}, nil
}

var _ ISyncBus = (*patchFakeBus)(nil)
