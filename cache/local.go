package cache

import (
	"container/list"
	"context"
	"sync"
)

type LocalStore[K comparable, V any] struct {
	cfg        StoreConfig[K, V]
	maxEntries int
	mu         sync.RWMutex
	items      map[K]V
	order      *list.List
	entries    map[K]*list.Element
}

type LocalStoreOption[K comparable, V any] func(*LocalStore[K, V])

func WithLocalMaxEntries[K comparable, V any](maxEntries int) LocalStoreOption[K, V] {
	return func(s *LocalStore[K, V]) {
		if maxEntries > 0 {
			s.maxEntries = maxEntries
			s.order = list.New()
			s.entries = make(map[K]*list.Element)
		}
	}
}

func NewLocalStore[K comparable, V any](cfg StoreConfig[K, V], opts ...LocalStoreOption[K, V]) *LocalStore[K, V] {
	store := &LocalStore[K, V]{
		cfg:   cfg,
		items: make(map[K]V),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	return store
}

func (s *LocalStore[K, V]) Get(_ context.Context, key K) (V, bool, error) {
	var zero V
	if s == nil || !s.cfg.validKey(key) {
		return zero, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.items[key]
	if ok {
		s.touchLocked(key)
	}
	return value, ok, nil
}

func (s *LocalStore[K, V]) Set(_ context.Context, value V) error {
	if s == nil {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.items[key]
	if ok && s.cfg.Stale != nil && s.cfg.Stale(old, value) {
		return nil
	}
	s.items[key] = value
	s.touchLocked(key)
	s.evictLocked()
	return nil
}

func (s *LocalStore[K, V]) Delete(_ context.Context, key K) error {
	if s == nil || !s.cfg.validKey(key) {
		return nil
	}
	s.mu.Lock()
	delete(s.items, key)
	s.removeOrderLocked(key)
	s.mu.Unlock()
	return nil
}

func (s *LocalStore[K, V]) touchLocked(key K) {
	if s.order == nil || s.entries == nil {
		return
	}
	if ele := s.entries[key]; ele != nil {
		s.order.MoveToFront(ele)
		return
	}
	s.entries[key] = s.order.PushFront(key)
}

func (s *LocalStore[K, V]) evictLocked() {
	if s.maxEntries <= 0 || s.order == nil {
		return
	}
	for len(s.items) > s.maxEntries {
		ele := s.order.Back()
		if ele == nil {
			return
		}
		key, ok := ele.Value.(K)
		s.order.Remove(ele)
		if ok {
			delete(s.entries, key)
			delete(s.items, key)
		}
	}
}

func (s *LocalStore[K, V]) removeOrderLocked(key K) {
	if s.order == nil || s.entries == nil {
		return
	}
	ele := s.entries[key]
	if ele == nil {
		return
	}
	s.order.Remove(ele)
	delete(s.entries, key)
}
