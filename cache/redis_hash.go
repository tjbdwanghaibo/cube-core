package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

type RedisHashKey struct {
	Key   string
	Field string
}

type RedisHashKeyFunc[K comparable] func(K) RedisHashKey

type RedisJSONHashStore[K comparable, V any] struct {
	redis fredis.IRedis
	ttl   time.Duration
	cfg   StoreConfig[K, V]
	key   RedisHashKeyFunc[K]
}

func NewRedisJSONHashStore[K comparable, V any](redis fredis.IRedis, ttl time.Duration, key RedisHashKeyFunc[K], cfg StoreConfig[K, V]) *RedisJSONHashStore[K, V] {
	return &RedisJSONHashStore[K, V]{
		redis: redis,
		ttl:   ttl,
		cfg:   cfg,
		key:   key,
	}
}

func (s *RedisJSONHashStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if s == nil || s.redis == nil || s.key == nil || !s.cfg.validKey(key) {
		return zero, false, nil
	}
	hashKey := s.key(key)
	if hashKey.Key == "" || hashKey.Field == "" {
		return zero, false, nil
	}
	raw, err := s.redis.HGet(ctx, hashKey.Key, hashKey.Field)
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

func (s *RedisJSONHashStore[K, V]) Set(ctx context.Context, value V) error {
	if s == nil || s.redis == nil {
		return nil
	}
	if s.key == nil {
		return fmt.Errorf("cache: redis hash key func is nil")
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
	hashKey := s.key(key)
	if hashKey.Key == "" || hashKey.Field == "" {
		return ErrInvalidKey
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
	if err := s.redis.HSet(ctx, hashKey.Key, hashKey.Field, raw); err != nil {
		return err
	}
	if s.ttl > 0 {
		_, err = s.redis.Expire(ctx, hashKey.Key, s.ttl)
	}
	return err
}

func (s *RedisJSONHashStore[K, V]) Delete(ctx context.Context, key K) error {
	if s == nil || s.redis == nil || s.key == nil || !s.cfg.validKey(key) {
		return nil
	}
	hashKey := s.key(key)
	if hashKey.Key == "" {
		return nil
	}
	if hashKey.Field == "" {
		_, err := s.redis.Del(ctx, hashKey.Key)
		return err
	}
	_, err := s.redis.HDel(ctx, hashKey.Key, hashKey.Field)
	return err
}
