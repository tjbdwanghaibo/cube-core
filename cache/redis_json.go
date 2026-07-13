package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

type RedisKeyFunc[K comparable] func(K) string

type RedisJSONStore[K comparable, V any] struct {
	redis fredis.IRedis
	ttl   time.Duration
	cfg   StoreConfig[K, V]
	key   RedisKeyFunc[K]
}

func NewRedisJSONStore[K comparable, V any](redis fredis.IRedis, ttl time.Duration, key RedisKeyFunc[K], cfg StoreConfig[K, V]) *RedisJSONStore[K, V] {
	return &RedisJSONStore[K, V]{
		redis: redis,
		ttl:   ttl,
		cfg:   cfg,
		key:   key,
	}
}

func (s *RedisJSONStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if s == nil || s.redis == nil || s.key == nil || !s.cfg.validKey(key) {
		return zero, false, nil
	}
	raw, err := s.redis.Get(ctx, s.key(key))
	if err != nil {
		if errors.Is(err, fredis.ErrNil) {
			return zero, false, nil
		}
		return zero, false, err
	}
	var value V
	if err := json.Unmarshal(raw, &value); err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func (s *RedisJSONStore[K, V]) Set(ctx context.Context, value V) error {
	if s == nil || s.redis == nil {
		return nil
	}
	if s.key == nil {
		return fmt.Errorf("cache: redis json key func is nil")
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
	old, ok, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	if ok && s.cfg.Stale != nil && s.cfg.Stale(old, value) {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.redis.Set(ctx, s.key(key), raw, s.ttl)
}

func (s *RedisJSONStore[K, V]) Delete(ctx context.Context, key K) error {
	if s == nil || s.redis == nil || s.key == nil || !s.cfg.validKey(key) {
		return nil
	}
	_, err := s.redis.Del(ctx, s.key(key))
	return err
}
