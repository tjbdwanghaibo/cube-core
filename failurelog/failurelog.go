package failurelog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/tjbdwanghaibo/cube-core/obs"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

const (
	defaultMaxEntries int64 = 10000
	appendTrimScript        = `
local len = redis.call("RPUSH", KEYS[1], ARGV[1])
local max = tonumber(ARGV[2])
if max and max > 0 then
	redis.call("LTRIM", KEYS[1], -max, -1)
end
local ttl = tonumber(ARGV[3])
if ttl and ttl > 0 then
	redis.call("PEXPIRE", KEYS[1], ttl)
end
return len
	`
	deleteRawScript = `
local key = KEYS[1]
local items = redis.call("LRANGE", key, 0, -1)
if #items == 0 then
	return 0
end
local ttl = redis.call("PTTL", key)
local remove = {}
for i = 1, #ARGV do
	local raw = ARGV[i]
	remove[raw] = (remove[raw] or 0) + 1
end
local kept = {}
local removed = 0
for i = 1, #items do
	local raw = items[i]
	local count = remove[raw]
	if count and count > 0 then
		remove[raw] = count - 1
		removed = removed + 1
	else
		kept[#kept + 1] = raw
	end
end
if removed > 0 then
	redis.call("DEL", key)
	if #kept > 0 then
		redis.call("RPUSH", key, unpack(kept))
		if ttl > 0 then
			redis.call("PEXPIRE", key, ttl)
		end
	end
end
return removed
	`
	purgeRawScript = `
local key = KEYS[1]
local count = redis.call("LLEN", key)
if count > 0 then
	redis.call("DEL", key)
end
return count
	`
)

var ErrKeyEmpty = errors.New("failurelog: key is empty")

type Config struct {
	Namespace  string
	TTL        time.Duration
	MaxEntries int64
}

func (c Config) normalize() Config {
	if c.MaxEntries == 0 {
		c.MaxEntries = defaultMaxEntries
	}
	return c
}

type RedisList struct {
	redis fredis.IRedis
	cfg   Config
}

func NewRedisList(redis fredis.IRedis, cfg Config) *RedisList {
	return &RedisList{redis: redis, cfg: cfg.normalize()}
}

func (l *RedisList) AppendRaw(ctx context.Context, key string, raw []byte) error {
	if l == nil || l.redis == nil {
		return nil
	}
	if key == "" {
		return ErrKeyEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if l.tryAppendWithScript(ctx, key, raw) {
		obs.IncCounter("failurelog_append_total", l.labels("ok"), 1)
		return nil
	}
	if _, err := l.redis.RPush(ctx, key, raw); err != nil {
		obs.IncCounter("failurelog_append_total", l.labels("error"), 1)
		return err
	}
	if err := l.trim(ctx, key); err != nil {
		obs.IncCounter("failurelog_append_total", l.labels("error"), 1)
		return err
	}
	if l.cfg.TTL > 0 {
		_, _ = l.redis.Expire(ctx, key, l.cfg.TTL)
	}
	obs.IncCounter("failurelog_append_total", l.labels("ok"), 1)
	return nil
}

func (l *RedisList) ListRaw(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if l == nil || l.redis == nil {
		return nil, nil
	}
	if key == "" {
		return nil, ErrKeyEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return l.redis.LRange(ctx, key, start, stop)
}

func (l *RedisList) Purge(ctx context.Context, key string) (int64, error) {
	if l == nil || l.redis == nil {
		return 0, nil
	}
	if key == "" {
		return 0, ErrKeyEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	n, err := l.purgeRaw(ctx, key)
	if err == nil {
		obs.IncCounter("failurelog_purge_total", l.labels("ok"), n)
	}
	if err != nil {
		obs.IncCounter("failurelog_purge_total", l.labels("error"), 1)
	}
	return n, err
}

func (l *RedisList) CountRaw(ctx context.Context, key string) (int64, error) {
	if l == nil || l.redis == nil {
		return 0, nil
	}
	if key == "" {
		return 0, ErrKeyEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return l.redis.LLen(ctx, key)
}

func (l *RedisList) DeleteRaw(ctx context.Context, key string, raws [][]byte) (int64, error) {
	if l == nil || l.redis == nil || len(raws) == 0 {
		return 0, nil
	}
	if key == "" {
		return 0, ErrKeyEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	args := make([]any, 0, len(raws))
	for _, raw := range raws {
		if len(raw) == 0 {
			continue
		}
		args = append(args, string(raw))
	}
	if len(args) == 0 {
		return 0, nil
	}
	if n, ok := l.tryDeleteWithScript(ctx, key, args); ok {
		obs.IncCounter("failurelog_delete_total", l.labels("ok"), n)
		return n, nil
	}
	n, err := l.deleteRawFallback(ctx, key, args)
	if err != nil {
		obs.IncCounter("failurelog_delete_total", l.labels("error"), 1)
		return 0, err
	}
	obs.IncCounter("failurelog_delete_total", l.labels("ok"), n)
	return n, nil
}

func (l *RedisList) labels(result string) obs.Labels {
	labels := obs.Labels{}
	if l != nil && l.cfg.Namespace != "" {
		labels["namespace"] = l.cfg.Namespace
	}
	if result != "" {
		labels["result"] = result
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}

func (l *RedisList) tryAppendWithScript(ctx context.Context, key string, raw []byte) bool {
	if l == nil || l.redis == nil {
		return false
	}
	ttlMillis := int64(0)
	if l.cfg.TTL > 0 {
		ttlMillis = l.cfg.TTL.Milliseconds()
		if ttlMillis <= 0 {
			ttlMillis = 1
		}
	}
	ret, err := l.redis.Eval(ctx, appendTrimScript, []string{key}, string(raw), l.cfg.MaxEntries, ttlMillis)
	return err == nil && ret != nil
}

func (l *RedisList) tryDeleteWithScript(ctx context.Context, key string, args []any) (int64, bool) {
	if l == nil || l.redis == nil {
		return 0, false
	}
	ret, err := l.redis.Eval(ctx, deleteRawScript, []string{key}, args...)
	if err != nil || ret == nil {
		return 0, false
	}
	n, err := redisInt64(ret)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (l *RedisList) purgeRaw(ctx context.Context, key string) (int64, error) {
	if n, ok := l.tryPurgeWithScript(ctx, key); ok {
		return n, nil
	}
	count, err := l.redis.LLen(ctx, key)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}
	if _, err := l.redis.Del(ctx, key); err != nil {
		return 0, err
	}
	return count, nil
}

func (l *RedisList) tryPurgeWithScript(ctx context.Context, key string) (int64, bool) {
	if l == nil || l.redis == nil {
		return 0, false
	}
	ret, err := l.redis.Eval(ctx, purgeRawScript, []string{key})
	if err != nil || ret == nil {
		return 0, false
	}
	n, err := redisInt64(ret)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (l *RedisList) deleteRawFallback(ctx context.Context, key string, args []any) (int64, error) {
	items, err := l.redis.LRange(ctx, key, 0, -1)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	ttl, _ := l.redis.TTL(ctx, key)
	remove := make(map[string]int, len(args))
	for _, arg := range args {
		remove[fmt.Sprint(arg)]++
	}
	kept := make([]any, 0, len(items))
	var removed int64
	for _, item := range items {
		if count := remove[item]; count > 0 {
			remove[item] = count - 1
			removed++
			continue
		}
		kept = append(kept, item)
	}
	if removed == 0 {
		return 0, nil
	}
	if _, err := l.redis.Del(ctx, key); err != nil {
		return 0, err
	}
	if len(kept) > 0 {
		if _, err := l.redis.RPush(ctx, key, kept...); err != nil {
			return 0, err
		}
		if ttl > 0 {
			_, _ = l.redis.Expire(ctx, key, ttl)
		}
	}
	return removed, nil
}

func (l *RedisList) trim(ctx context.Context, key string) error {
	if l == nil || l.redis == nil || l.cfg.MaxEntries <= 0 {
		return nil
	}
	count, err := l.redis.LLen(ctx, key)
	if err != nil {
		return err
	}
	if count <= l.cfg.MaxEntries {
		return nil
	}
	obs.IncCounter("failurelog_trim_total", nil, 1)
	items, err := l.redis.LRange(ctx, key, count-l.cfg.MaxEntries, -1)
	if err != nil {
		return err
	}
	if _, err := l.redis.Del(ctx, key); err != nil {
		return err
	}
	if len(items) > 0 {
		values := make([]any, 0, len(items))
		for _, item := range items {
			values = append(values, item)
		}
		if _, err := l.redis.RPush(ctx, key, values...); err != nil {
			return err
		}
	}
	return nil
}

func redisInt64(v any) (int64, error) {
	switch typed := v.(type) {
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case uint64:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("failurelog: unexpected redis integer %T", v)
	}
}
