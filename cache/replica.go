package cache

import (
	"context"
	"time"

	"github.com/tjbdwanghaibo/cube-core/replica"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
)

type ReplicaConfig[K comparable, V any] struct {
	Store       Store[K, V]
	Topic       string
	KeyOf       func(V) int64
	VersionOf   func(V) int64
	UpdatedAtOf func(V) int64
	DeleteKeyOf func(replicaKey int64) K
}

type ReplicaSyncer[K comparable, V any] struct {
	cfg        ReplicaConfig[K, V]
	replicator *replica.Replicator
}

func NewReplicaSyncer[K comparable, V any](bus fsync.ISyncBus, cfg ReplicaConfig[K, V]) *ReplicaSyncer[K, V] {
	s := &ReplicaSyncer[K, V]{cfg: cfg}
	s.replicator = replica.New(bus, cfg.Topic, replicaStore[K, V]{cfg: cfg})
	return s
}

func (s *ReplicaSyncer[K, V]) Start() error {
	if s == nil || s.replicator == nil {
		return nil
	}
	return s.replicator.Start()
}

func (s *ReplicaSyncer[K, V]) Stop() {
	if s != nil && s.replicator != nil {
		s.replicator.Stop()
	}
}

func (s *ReplicaSyncer[K, V]) Publish(ctx context.Context, value V) error {
	if s == nil || s.replicator == nil || s.cfg.KeyOf == nil {
		return nil
	}
	version := int64(0)
	if s.cfg.VersionOf != nil {
		version = s.cfg.VersionOf(value)
	}
	updatedAt := int64(0)
	if s.cfg.UpdatedAtOf != nil {
		updatedAt = s.cfg.UpdatedAtOf(value)
	}
	if updatedAt == 0 {
		updatedAt = time.Now().UnixMilli()
	}
	raw, err := replica.MarshalPayload(value)
	if err != nil {
		return err
	}
	return s.replicator.Publish(ctx, replica.Envelope{
		Key:       s.cfg.KeyOf(value),
		Version:   version,
		Op:        replica.OpUpsert,
		Payload:   raw,
		UpdatedAt: updatedAt,
	})
}

func (s *ReplicaSyncer[K, V]) PublishDelete(ctx context.Context, replicaKey int64, version int64) error {
	if s == nil || s.replicator == nil {
		return nil
	}
	return s.replicator.PublishDelete(ctx, replicaKey, version)
}

type replicaStore[K comparable, V any] struct {
	cfg ReplicaConfig[K, V]
}

func (s replicaStore[K, V]) ApplyReplica(ctx context.Context, env replica.Envelope) error {
	if s.cfg.Store == nil || env.Key == 0 {
		return nil
	}
	if env.Op == replica.OpDelete {
		if s.cfg.DeleteKeyOf == nil {
			return nil
		}
		return s.cfg.Store.Delete(ctx, s.cfg.DeleteKeyOf(env.Key))
	}
	value, err := replica.UnmarshalPayload[V](env)
	if err != nil {
		return err
	}
	return s.cfg.Store.Set(ctx, value)
}
