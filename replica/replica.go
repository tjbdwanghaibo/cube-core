package replica

import (
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"encoding/json"
	"sync"
	"time"
)

type Op uint8

const (
	OpUpsert Op = iota + 1
	OpDelete
)

type Envelope struct {
	Topic     string `json:"topic,omitempty"`
	Key       int64  `json:"key,omitempty"`
	Version   int64  `json:"version,omitempty"`
	Op        Op     `json:"op,omitempty"`
	Payload   []byte `json:"payload,omitempty"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
}

type Store interface {
	ApplyReplica(ctx context.Context, env Envelope) error
}

type Replicator struct {
	bus     fsync.ISyncBus
	store   Store
	topic   string
	unsub   func()
	started bool
	mu      sync.Mutex
}

func New(bus fsync.ISyncBus, topic string, store Store) *Replicator {
	return &Replicator{bus: bus, topic: topic, store: store}
}

func (r *Replicator) Start() error {
	if r == nil || r.bus == nil || r.store == nil || r.topic == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	unsub, err := r.bus.Subscribe(r.topic, func(msg *fsync.SyncMsg) error {
		if msg == nil || msg.Key == 0 {
			return nil
		}
		env := Envelope{
			Topic:   r.topic,
			Key:     msg.Key,
			Version: msg.Version,
		}
		if len(msg.Data) == 0 {
			env.Op = OpDelete
			return r.store.ApplyReplica(fctx.BaseContext(), env)
		}
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			return err
		}
		if env.Topic == "" {
			env.Topic = r.topic
		}
		if env.Key == 0 {
			env.Key = msg.Key
		}
		if env.Version == 0 {
			env.Version = msg.Version
		}
		if env.Op == 0 {
			env.Op = OpUpsert
		}
		return r.store.ApplyReplica(fctx.BaseContext(), env)
	})
	if err != nil {
		return err
	}
	r.unsub = unsub
	r.started = true
	return nil
}

func (r *Replicator) Stop() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unsub != nil {
		r.unsub()
	}
	r.unsub = nil
	r.started = false
}

func (r *Replicator) Publish(ctx context.Context, env Envelope) error {
	if r == nil || r.bus == nil || r.topic == "" {
		return nil
	}
	if env.Topic == "" {
		env.Topic = r.topic
	}
	if env.Op == 0 {
		env.Op = OpUpsert
	}
	if env.UpdatedAt == 0 {
		env.UpdatedAt = time.Now().UnixMilli()
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_ = ctx
	return r.bus.Publish(&fsync.SyncMsg{
		Topic:   r.topic,
		Key:     env.Key,
		Version: env.Version,
		Data:    raw,
	})
}

func (r *Replicator) PublishDelete(ctx context.Context, key int64, version int64) error {
	if r == nil || r.bus == nil || r.topic == "" {
		return nil
	}
	_ = ctx
	return r.bus.Publish(&fsync.SyncMsg{
		Topic:   r.topic,
		Key:     key,
		Version: version,
	})
}

func MarshalPayload(v any) ([]byte, error) {
	return json.Marshal(v)
}

func UnmarshalPayload[T any](env Envelope) (T, error) {
	var ret T
	err := json.Unmarshal(env.Payload, &ret)
	return ret, err
}
