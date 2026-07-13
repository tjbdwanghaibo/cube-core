package replica

import (
	"context"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"testing"
)

func TestReplicatorPublishesAndAppliesEnvelope(t *testing.T) {
	bus := newFakeBus()
	store := &fakeStore{}
	rep := New(bus, "topic", store)
	if err := rep.Start(); err != nil {
		t.Fatal(err)
	}
	defer rep.Stop()

	if err := rep.Publish(context.Background(), Envelope{
		Key:     7,
		Version: 3,
		Op:      OpUpsert,
		Payload: []byte("data"),
	}); err != nil {
		t.Fatal(err)
	}
	if len(store.items) != 1 || store.items[0].Key != 7 || string(store.items[0].Payload) != "data" {
		t.Fatalf("items = %+v", store.items)
	}

	if err := rep.PublishDelete(context.Background(), 7, 4); err != nil {
		t.Fatal(err)
	}
	if len(store.items) != 2 || store.items[1].Op != OpDelete || store.items[1].Version != 4 {
		t.Fatalf("delete item = %+v", store.items)
	}
}

type fakeStore struct {
	items []Envelope
}

func (s *fakeStore) ApplyReplica(_ context.Context, env Envelope) error {
	s.items = append(s.items, env)
	return nil
}

type fakeBus struct {
	handlers map[string][]fsync.Handler
}

func newFakeBus() *fakeBus {
	return &fakeBus{handlers: make(map[string][]fsync.Handler)}
}

func (b *fakeBus) Publish(msg *fsync.SyncMsg) error {
	for _, h := range b.handlers[msg.Topic] {
		if err := h(msg); err != nil {
			return err
		}
	}
	return nil
}

func (b *fakeBus) Subscribe(topic string, handler fsync.Handler) (func(), error) {
	b.handlers[topic] = append(b.handlers[topic], handler)
	return func() {}, nil
}

var _ fsync.ISyncBus = (*fakeBus)(nil)
