package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

type RedisRawJSONStore[K comparable, V any] struct {
	redis fredis.IRedis
	ttl   time.Duration
	key   RedisKeyFunc[K]
	cfg   StoreConfig[K, V]
}

func NewRedisRawJSONStore[K comparable, V any](redis fredis.IRedis, ttl time.Duration, key RedisKeyFunc[K], cfg StoreConfig[K, V]) *RedisRawJSONStore[K, V] {
	return &RedisRawJSONStore[K, V]{redis: redis, ttl: ttl, key: key, cfg: cfg}
}

func (s *RedisRawJSONStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
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

func (s *RedisRawJSONStore[K, V]) Set(ctx context.Context, value V) error {
	if s == nil || s.redis == nil {
		return nil
	}
	if s.key == nil {
		return fmt.Errorf("cache: redis raw key func is nil")
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

func (s *RedisRawJSONStore[K, V]) Delete(ctx context.Context, key K) error {
	if s == nil || s.redis == nil || s.key == nil || !s.cfg.validKey(key) {
		return nil
	}
	_, err := s.redis.Del(ctx, s.key(key))
	return err
}

type RedisRawSortedSetStore[M ~int64 | ~int | ~string] struct {
	redis fredis.IRedis
	key   string
	ttl   time.Duration
}

func NewRedisRawSortedSetStore[M ~int64 | ~int | ~string](redis fredis.IRedis, key string, ttl time.Duration) *RedisRawSortedSetStore[M] {
	return &RedisRawSortedSetStore[M]{redis: redis, key: key, ttl: ttl}
}

func (s *RedisRawSortedSetStore[M]) SetScore(ctx context.Context, member M, score float64) error {
	if s == nil || s.redis == nil || s.key == "" {
		return nil
	}
	if _, err := s.redis.ZAdd(ctx, s.key, fredis.Z{Score: score, Member: rawSortedSetMember(member)}); err != nil {
		return err
	}
	if s.ttl > 0 {
		_, err := s.redis.Expire(ctx, s.key, s.ttl)
		return err
	}
	return nil
}

func (s *RedisRawSortedSetStore[M]) Score(ctx context.Context, member M) (float64, bool, error) {
	if s == nil || s.redis == nil || s.key == "" {
		return 0, false, nil
	}
	score, err := s.redis.ZScore(ctx, s.key, rawSortedSetMember(member))
	if err != nil {
		if errors.Is(err, fredis.ErrNil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return score, true, nil
}

func (s *RedisRawSortedSetStore[M]) Delete(ctx context.Context, member M) error {
	if s == nil || s.redis == nil || s.key == "" {
		return nil
	}
	_, err := s.redis.ZRem(ctx, s.key, rawSortedSetMember(member))
	return err
}

func rawSortedSetMember[M ~int64 | ~int | ~string](member M) string {
	switch v := any(member).(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprint(v)
	}
}
