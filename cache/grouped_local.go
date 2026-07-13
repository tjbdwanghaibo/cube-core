package cache

import (
	"context"
	"sync"
)

type GroupedLocalStore[G comparable, K comparable, V any] struct {
	cfg     StoreConfig[K, V]
	groupOf func(K) G
	mu      sync.RWMutex
	groups  map[G]map[K]V
}

func NewGroupedLocalStore[G comparable, K comparable, V any](groupOf func(K) G, cfg StoreConfig[K, V]) *GroupedLocalStore[G, K, V] {
	return &GroupedLocalStore[G, K, V]{
		cfg:     cfg,
		groupOf: groupOf,
		groups:  make(map[G]map[K]V),
	}
}

func (s *GroupedLocalStore[G, K, V]) Get(_ context.Context, key K) (V, bool, error) {
	var zero V
	if s == nil || s.groupOf == nil || !s.cfg.validKey(key) {
		return zero, false, nil
	}
	groupKey := s.groupOf(key)
	s.mu.RLock()
	group := s.groups[groupKey]
	value, ok := group[key]
	s.mu.RUnlock()
	return value, ok, nil
}

func (s *GroupedLocalStore[G, K, V]) Set(_ context.Context, value V) error {
	if s == nil || s.groupOf == nil {
		return nil
	}
	if s.cfg.ValidateValue != nil {
		if err := s.cfg.ValidateValue(value); err != nil {
			return err
		}
	}
	key, err := s.cfg.keyOf(value)
	if err != nil {
		return err
	}
	groupKey := s.groupOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	group := s.groups[groupKey]
	if group == nil {
		group = make(map[K]V)
		s.groups[groupKey] = group
	}
	old, ok := group[key]
	if ok && s.cfg.Stale != nil && s.cfg.Stale(old, value) {
		return nil
	}
	group[key] = value
	return nil
}

func (s *GroupedLocalStore[G, K, V]) Delete(_ context.Context, key K) error {
	if s == nil || s.groupOf == nil || !s.cfg.validKey(key) {
		return nil
	}
	groupKey := s.groupOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	group := s.groups[groupKey]
	if group == nil {
		return nil
	}
	delete(group, key)
	if len(group) == 0 {
		delete(s.groups, groupKey)
	}
	return nil
}

func (s *GroupedLocalStore[G, K, V]) DeleteGroup(_ context.Context, groupKey G) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	delete(s.groups, groupKey)
	s.mu.Unlock()
	return nil
}
