package cache

import (
	"context"
	"sync"
	"time"
)

type LayeredStore[K comparable, V any] struct {
	cfg    StoreConfig[K, V]
	local  Store[K, V]
	remote Store[K, V]
	ttl    time.Duration

	mu     sync.Mutex
	expiry map[K]time.Time
}

func NewLayeredStore[K comparable, V any](local Store[K, V], remote Store[K, V], ttl time.Duration, cfg StoreConfig[K, V]) *LayeredStore[K, V] {
	return &LayeredStore[K, V]{
		cfg:    cfg,
		local:  local,
		remote: remote,
		ttl:    ttl,
		expiry: make(map[K]time.Time),
	}
}

func (s *LayeredStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if s == nil || !s.cfg.validKey(key) {
		return zero, false, nil
	}
	if s.local != nil {
		if s.remote == nil || s.localValid(key, time.Now()) {
			value, ok, err := s.local.Get(ctx, key)
			if err != nil {
				return zero, false, err
			}
			if ok {
				return value, true, nil
			}
		}
	}
	if s.remote == nil {
		return zero, false, nil
	}
	value, ok, err := s.remote.Get(ctx, key)
	if err != nil || !ok {
		return value, ok, err
	}
	if s.local != nil {
		_ = s.local.Set(ctx, value)
		s.setLocalExpiry(key, time.Now())
	}
	return value, true, nil
}

func (s *LayeredStore[K, V]) Set(ctx context.Context, value V) error {
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
	if s.remote != nil {
		if err := s.remote.Set(ctx, value); err != nil {
			return err
		}
		current, ok, err := s.remote.Get(ctx, key)
		if err != nil {
			return err
		}
		if ok {
			value = current
		} else {
			if s.local != nil {
				_ = s.local.Delete(ctx, key)
			}
			s.clearLocalExpiry(key)
			return nil
		}
	}
	if s.local != nil {
		if err := s.local.Set(ctx, value); err != nil {
			return err
		}
		s.setLocalExpiry(key, time.Now())
	}
	return nil
}

func (s *LayeredStore[K, V]) Delete(ctx context.Context, key K) error {
	if s == nil || !s.cfg.validKey(key) {
		return nil
	}
	if s.remote != nil {
		if err := s.remote.Delete(ctx, key); err != nil {
			return err
		}
	}
	if s.local != nil {
		if err := s.local.Delete(ctx, key); err != nil {
			return err
		}
	}
	s.clearLocalExpiry(key)
	return nil
}

func (s *LayeredStore[K, V]) localValid(key K, now time.Time) bool {
	if s == nil || s.ttl <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.expiry[key]
	if !ok || now.After(exp) {
		delete(s.expiry, key)
		return false
	}
	return true
}

func (s *LayeredStore[K, V]) setLocalExpiry(key K, now time.Time) {
	if s == nil || s.ttl <= 0 {
		return
	}
	s.mu.Lock()
	s.expiry[key] = now.Add(s.ttl)
	s.mu.Unlock()
}

func (s *LayeredStore[K, V]) clearLocalExpiry(key K) {
	if s == nil || s.ttl <= 0 {
		return
	}
	s.mu.Lock()
	delete(s.expiry, key)
	s.mu.Unlock()
}
