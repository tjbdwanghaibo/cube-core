package bus

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/admin"
	fnats "github.com/tjbdwanghaibo/cube-core/nats"
)

func TestReliableBroadcastDedupIsPerConsumer(t *testing.T) {
	store := newReliableMemoryStore()
	msg := &fnats.NatsMsg{
		ToModule: "mail",
		MsgName:  "Changed",
		MsgID:    "broadcast-1",
	}

	var handled1 int
	b1 := New(nil, nil, JSONCodec{}, Config{Sid: 1, SvcType: "game"})
	b1.EnableReliable(store, ReliableConfig{Enabled: true})
	b1.Handle("mail", "Changed", func(*MsgContext) { handled1++ })
	b1.dispatchMsg(&incomingTask{natsMsg: msg})
	b1.dispatchMsg(&incomingTask{natsMsg: msg})

	var handled2 int
	b2 := New(nil, nil, JSONCodec{}, Config{Sid: 2, SvcType: "game"})
	b2.EnableReliable(store, ReliableConfig{Enabled: true})
	b2.Handle("mail", "Changed", func(*MsgContext) { handled2++ })
	b2.dispatchMsg(&incomingTask{natsMsg: msg})

	if handled1 != 1 {
		t.Fatalf("consumer 1 handled %d times, want 1", handled1)
	}
	if handled2 != 1 {
		t.Fatalf("consumer 2 handled %d times, want 1", handled2)
	}
	if got := store.finishedCount(); got != 2 {
		t.Fatalf("finished count = %d, want 2", got)
	}
}

func TestReliableDispatchDropGoesToDeadLetter(t *testing.T) {
	store := newReliableMemoryStore()
	b := New(nil, nil, JSONCodec{}, Config{Sid: 1, SvcType: "game"})
	b.EnableReliable(store, ReliableConfig{Enabled: true})
	b.dispatchTask(0, &incomingTask{natsMsg: &fnats.NatsMsg{
		ToModule: "map",
		MsgName:  "Move",
		MsgID:    "drop-1",
	}})

	if got := store.deadLetterCount(); got != 1 {
		t.Fatalf("dead letters = %d, want 1", got)
	}
}

func TestBusDeadLetterListAndRequeue(t *testing.T) {
	store := newReliableMemoryStore()
	client := &captureNatsClient{}
	b := New(client, nil, JSONCodec{}, Config{Sid: 1, SvcType: "game", Prefix: "cube"})
	b.EnableReliable(store, ReliableConfig{Enabled: true})
	b.deadLetter(&fnats.NatsMsg{
		ToSid:    2,
		ToModule: "mail",
		MsgName:  "Changed",
		MsgID:    "dead-1",
		Payload:  []byte(`{"x":1}`),
	}, "handler failed")

	entries, err := b.DeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed", Start: 0, Stop: 10})
	if err != nil {
		t.Fatalf("DeadLetters: %v", err)
	}
	if len(entries) != 1 || entries[0].Reason != "handler failed" || entries[0].MsgID != "dead-1" {
		t.Fatalf("entries = %+v", entries)
	}

	n, err := b.RequeueDeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed", Start: 0, Stop: 10})
	if err != nil {
		t.Fatalf("RequeueDeadLetters: %v", err)
	}
	if n != 1 || len(client.published) != 1 {
		t.Fatalf("requeue n=%d published=%+v", n, client.published)
	}
	if store.deadLetterCount() != 0 {
		t.Fatalf("dead letters after requeue = %d, want 0", store.deadLetterCount())
	}
}

func TestBusDeadLetterRequeueLimitKeepsUnselectedEntries(t *testing.T) {
	redis := newReliableFakeRedis()
	store := NewRedisReliableStore(redis, ReliableConfig{
		Enabled:       true,
		Prefix:        "bus:test",
		MaxDLQEntries: 10,
	})
	client := &captureNatsClient{}
	b := New(client, nil, JSONCodec{}, Config{Sid: 1, SvcType: "game", Prefix: "cube"})
	b.EnableReliable(store, ReliableConfig{Enabled: true})

	for _, id := range []string{"dead-1", "dead-2", "dead-3"} {
		b.deadLetter(&fnats.NatsMsg{
			ToSid:    2,
			ToModule: "mail",
			MsgName:  "Changed",
			MsgID:    id,
		}, "handler failed")
	}

	n, err := b.RequeueDeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed", Limit: 1})
	if err != nil {
		t.Fatalf("RequeueDeadLetters: %v", err)
	}
	if n != 1 || len(client.published) != 1 {
		t.Fatalf("requeue n=%d published=%+v", n, client.published)
	}
	remaining, err := b.DeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed"})
	if err != nil {
		t.Fatalf("DeadLetters: %v", err)
	}
	if len(remaining) != 2 || remaining[0].MsgID != "dead-2" || remaining[1].MsgID != "dead-3" {
		t.Fatalf("remaining dead letters = %+v, want dead-2/dead-3", remaining)
	}
}

func TestBusRpcNoHandlerRepliesWithErrorEnvelope(t *testing.T) {
	rpc := &lifecycleRpc{replies: make(chan []byte, 1)}
	b := New(&lifecycleClient{}, rpc, JSONCodec{}, Config{Sid: 5001, SvcType: "mail"})

	b.dispatchRpc(&incomingTask{
		natsMsg:      &fnats.NatsMsg{MsgName: "mail.Missing"},
		replySubject: "reply",
	})

	select {
	case raw := <-rpc.replies:
		var resp rpcErrorEnvelope
		if err := b.codec.Unmarshal(raw, &resp); err != nil {
			t.Fatalf("Unmarshal rpc error: %v", err)
		}
		if resp.Code == 0 || resp.Reason == "" {
			t.Fatalf("rpc error envelope = %+v, want non-zero code and reason", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("rpc error reply was not sent")
	}
}

func TestBusDeadLetterAdminCommandsListAndRequeue(t *testing.T) {
	store := newReliableMemoryStore()
	client := &captureNatsClient{}
	b := New(client, nil, JSONCodec{}, Config{Sid: 1, SvcType: "game", Prefix: "cube"})
	b.EnableReliable(store, ReliableConfig{Enabled: true})
	reg := admin.NewRegistry()
	if err := RegisterAdminCommands(reg, b); err != nil {
		t.Fatalf("RegisterAdminCommands: %v", err)
	}
	b.deadLetter(&fnats.NatsMsg{
		ToSid:    2,
		ToModule: "mail",
		MsgName:  "Changed",
		MsgID:    "dead-1",
		Payload:  []byte(`{"x":1}`),
	}, "handler failed")

	list, err := reg.Execute(context.Background(), admin.Command{
		Name:    AdminCommandBusDLQList,
		Payload: admin.MustPayload(DeadLetterCommand{Module: "mail", MsgName: "Changed"}),
	})
	if err != nil {
		t.Fatalf("list command: %v", err)
	}
	if list.Data["count"].(int) != 1 {
		t.Fatalf("list result = %+v", list)
	}

	requeued, err := reg.Execute(context.Background(), admin.Command{
		Name:    AdminCommandBusDLQRequeue,
		Payload: admin.MustPayload(DeadLetterCommand{Module: "mail", MsgName: "Changed"}),
	})
	if err != nil {
		t.Fatalf("requeue command: %v", err)
	}
	if requeued.Data["count"].(int64) != 1 || len(client.published) != 1 {
		t.Fatalf("requeue result=%+v published=%+v", requeued, client.published)
	}
}

type reliableMemoryStore struct {
	mu       sync.Mutex
	started  map[string]struct{}
	finished map[string]struct{}
	dead     map[string][]DeadLetterEntry
}

func newReliableMemoryStore() *reliableMemoryStore {
	return &reliableMemoryStore{
		started:  make(map[string]struct{}),
		finished: make(map[string]struct{}),
		dead:     make(map[string][]DeadLetterEntry),
	}
}

func (s *reliableMemoryStore) BeginConsume(_ context.Context, consumer ReliableConsumer, msg *fnats.NatsMsg) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := reliableTestKey(consumer, msg)
	if _, ok := s.started[key]; ok {
		return false, nil
	}
	s.started[key] = struct{}{}
	return true, nil
}

func (s *reliableMemoryStore) FinishConsume(_ context.Context, consumer ReliableConsumer, msg *fnats.NatsMsg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished[reliableTestKey(consumer, msg)] = struct{}{}
	return nil
}

func (s *reliableMemoryStore) DeadLetter(_ context.Context, consumer ReliableConsumer, msg *fnats.NatsMsg, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := DeadLetterKey(msg.ToModule, msg.MsgName)
	s.dead[key] = append(s.dead[key], DeadLetterEntry{
		MsgID:    msg.MsgID,
		ToSid:    msg.ToSid,
		ToModule: msg.ToModule,
		MsgName:  msg.MsgName,
		Consumer: consumer.Key(),
		Reason:   reason,
		Payload:  append([]byte(nil), msg.Payload...),
	})
	return nil
}

func (s *reliableMemoryStore) ListDeadLetters(_ context.Context, query DeadLetterQuery) ([]DeadLetterEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	query = query.normalize()
	items := append([]DeadLetterEntry(nil), s.dead[DeadLetterKey(query.Module, query.MsgName)]...)
	if query.Stop < 0 || query.Stop >= int64(len(items)) {
		query.Stop = int64(len(items)) - 1
	}
	if query.Start < 0 {
		query.Start = 0
	}
	if query.Start > query.Stop || query.Start >= int64(len(items)) {
		return nil, nil
	}
	return items[query.Start : query.Stop+1], nil
}

func (s *reliableMemoryStore) PurgeDeadLetters(_ context.Context, query DeadLetterQuery) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := DeadLetterKey(query.Module, query.MsgName)
	query = query.normalize()
	if !query.isWholeBucket() {
		items := append([]DeadLetterEntry(nil), s.dead[key]...)
		if query.Stop < 0 || query.Stop >= int64(len(items)) {
			query.Stop = int64(len(items)) - 1
		}
		if query.Start < 0 {
			query.Start = 0
		}
		if query.Start > query.Stop || query.Start >= int64(len(items)) {
			return 0, nil
		}
		selected := items[query.Start : query.Stop+1]
		return s.deleteDeadLettersLocked(key, selected), nil
	}
	n := int64(len(s.dead[key]))
	delete(s.dead, key)
	return n, nil
}

func (s *reliableMemoryStore) DeleteDeadLetters(_ context.Context, query DeadLetterQuery, entries []DeadLetterEntry) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteDeadLettersLocked(DeadLetterKey(query.Module, query.MsgName), entries), nil
}

func (s *reliableMemoryStore) deleteDeadLettersLocked(key string, entries []DeadLetterEntry) int64 {
	if len(entries) == 0 {
		return 0
	}
	remove := make(map[string]int, len(entries))
	for _, entry := range entries {
		remove[entry.MsgID]++
	}
	items := s.dead[key]
	kept := items[:0]
	var removed int64
	for _, item := range items {
		if count := remove[item.MsgID]; count > 0 {
			remove[item.MsgID] = count - 1
			removed++
			continue
		}
		kept = append(kept, item)
	}
	if len(kept) == 0 {
		delete(s.dead, key)
	} else {
		s.dead[key] = kept
	}
	return removed
}

func (s *reliableMemoryStore) finishedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.finished)
}

func (s *reliableMemoryStore) deadLetterCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, items := range s.dead {
		total += len(items)
	}
	return total
}

func reliableTestKey(consumer ReliableConsumer, msg *fnats.NatsMsg) string {
	if msg == nil {
		return consumer.Key() + ":"
	}
	return consumer.Key() + ":" + msg.MsgID
}

type captureNatsClient struct {
	published []string
}

func (c *captureNatsClient) Publish(subject string, data []byte) error {
	var msg fnats.NatsMsg
	if err := json.Unmarshal(data, &msg); err == nil {
		c.published = append(c.published, subject+":"+msg.MsgID)
	} else {
		c.published = append(c.published, subject)
	}
	return nil
}

func (c *captureNatsClient) Request(string, []byte, time.Duration) ([]byte, error) { return nil, nil }
func (c *captureNatsClient) Subscribe(string, fnats.MsgHandler) (fnats.ISubscription, error) {
	return nil, nil
}
func (c *captureNatsClient) QueueSubscribe(string, string, fnats.MsgHandler) (fnats.ISubscription, error) {
	return nil, nil
}
func (c *captureNatsClient) Drain() error { return nil }
func (c *captureNatsClient) Close()       {}
