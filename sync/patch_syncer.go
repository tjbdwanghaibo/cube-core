package sync

import (
	"context"
	"encoding/json"
	"fmt"
	stdsync "sync"
	"time"
)

// PatchSyncerConfig describes transient patch replication over ISyncBus.
// It intentionally has no store/delete/stale semantics; long-lived state belongs
// in cache replica or entity snapshot layers.
type PatchSyncerConfig[T any] struct {
	Topic    string
	LocalSid int32
	KeyOf    func(T) int64
	WithKey  func(T, int64) T
	HasData  func(T) bool
	Apply    func(context.Context, T) error
}

type PatchSyncer[T any] struct {
	bus   ISyncBus
	cfg   PatchSyncerConfig[T]
	mu    stdsync.Mutex
	unsub func()
}

func NewPatchSyncer[T any](bus ISyncBus, cfg PatchSyncerConfig[T]) *PatchSyncer[T] {
	return &PatchSyncer[T]{bus: bus, cfg: cfg}
}

func (s *PatchSyncer[T]) Start() error {
	if s == nil || s.bus == nil || s.cfg.Topic == "" || s.cfg.Apply == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unsub != nil {
		return nil
	}
	unsub, err := s.bus.Subscribe(s.cfg.Topic, s.handle)
	if err != nil {
		return err
	}
	s.unsub = unsub
	return nil
}

func (s *PatchSyncer[T]) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unsub != nil {
		s.unsub()
	}
	s.unsub = nil
}

func (s *PatchSyncer[T]) Publish(ctx context.Context, patch T) error {
	if s == nil || s.bus == nil || s.cfg.Topic == "" || s.empty(patch) {
		return nil
	}
	key := s.keyOf(patch)
	if key == 0 {
		return nil
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_ = ctx
	return s.bus.Publish(&SyncMsg{
		Topic:   s.cfg.Topic,
		Key:     key,
		Version: time.Now().UnixMilli(),
		Data:    data,
		FromSid: s.cfg.LocalSid,
	})
}

func (s *PatchSyncer[T]) handle(msg *SyncMsg) error {
	if s == nil || msg == nil || msg.Key == 0 || len(msg.Data) == 0 {
		return nil
	}
	if s.cfg.LocalSid != 0 && msg.FromSid == s.cfg.LocalSid {
		return nil
	}
	var patch T
	if err := json.Unmarshal(msg.Data, &patch); err != nil {
		return err
	}
	key := s.keyOf(patch)
	if key == 0 && s.cfg.WithKey != nil {
		patch = s.cfg.WithKey(patch, msg.Key)
		key = s.keyOf(patch)
	}
	if key != msg.Key {
		return fmt.Errorf("sync patch: key mismatch key=%d patch=%d", msg.Key, key)
	}
	if s.empty(patch) {
		return nil
	}
	return s.cfg.Apply(context.Background(), patch)
}

func (s *PatchSyncer[T]) keyOf(patch T) int64 {
	if s == nil || s.cfg.KeyOf == nil {
		return 0
	}
	return s.cfg.KeyOf(patch)
}

func (s *PatchSyncer[T]) empty(patch T) bool {
	return s != nil && s.cfg.HasData != nil && !s.cfg.HasData(patch)
}
