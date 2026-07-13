package redis

import (
	"context"
	"time"
)

// IRedis is the abstraction for Redis operations.
type IRedis interface {
	// --- String/KV ---
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value any, expiration time.Duration) error
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error)
	Del(ctx context.Context, keys ...string) (int64, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)
	TTL(ctx context.Context, key string) (time.Duration, error)
	Incr(ctx context.Context, key string) (int64, error)
	IncrBy(ctx context.Context, key string, value int64) (int64, error)

	// --- Hash ---
	HGet(ctx context.Context, key, field string) ([]byte, error)
	HSet(ctx context.Context, key string, values ...any) error
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, fields ...string) (int64, error)
	HExists(ctx context.Context, key, field string) (bool, error)

	// --- List ---
	LPush(ctx context.Context, key string, values ...any) (int64, error)
	RPush(ctx context.Context, key string, values ...any) (int64, error)
	LPop(ctx context.Context, key string) ([]byte, error)
	RPop(ctx context.Context, key string) ([]byte, error)
	LLen(ctx context.Context, key string) (int64, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)

	// --- Sorted Set ---
	ZAdd(ctx context.Context, key string, members ...Z) (int64, error)
	ZRem(ctx context.Context, key string, members ...any) (int64, error)
	ZScore(ctx context.Context, key string, member string) (float64, error)
	ZRank(ctx context.Context, key string, member string) (int64, error)
	ZRevRank(ctx context.Context, key string, member string) (int64, error)
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error)
	ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error)
	ZCard(ctx context.Context, key string) (int64, error)

	// --- Set ---
	SAdd(ctx context.Context, key string, members ...any) (int64, error)
	SRem(ctx context.Context, key string, members ...any) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	SIsMember(ctx context.Context, key string, member any) (bool, error)

	// --- Pipeline / Script ---
	Pipeline() IPipeline
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
	EvalSha(ctx context.Context, sha string, keys []string, args ...any) (any, error)

	// --- PubSub ---
	Publish(ctx context.Context, channel string, message any) error
	Subscribe(ctx context.Context, channels ...string) IPubSub

	// --- Connection ---
	Ping(ctx context.Context) error
	Close() error
}

// Z represents a sorted set member with score.
type Z struct {
	Score  float64
	Member string
}
