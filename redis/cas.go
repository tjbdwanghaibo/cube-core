package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var ErrCASInvalidCommand = errors.New("redis cas: invalid command")

type CompareAndSetCommand struct {
	Key      string
	Expected []byte
	Next     []byte
	TTL      time.Duration
}

type CompareAndSetResult struct {
	Applied bool
	Current []byte
}

const compareAndSetScript = `
local current = redis.call("GET", KEYS[1])
local expect_missing = ARGV[4]
if expect_missing == "1" then
  if current ~= false then
    return {0, current}
  end
else
  if current == false or current ~= ARGV[1] then
    return {0, current}
  end
end
if tonumber(ARGV[3]) > 0 then
  redis.call("PSETEX", KEYS[1], ARGV[3], ARGV[2])
else
  redis.call("SET", KEYS[1], ARGV[2])
end
return {1, ARGV[2]}
`

func CompareAndSet(ctx context.Context, client IRedis, cmd CompareAndSetCommand) (CompareAndSetResult, error) {
	if client == nil || cmd.Key == "" || cmd.Next == nil {
		return CompareAndSetResult{}, ErrCASInvalidCommand
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ttlMillis := int64(0)
	if cmd.TTL > 0 {
		ttlMillis = int64(cmd.TTL / time.Millisecond)
		if ttlMillis <= 0 {
			ttlMillis = 1
		}
	}
	expectMissing := "0"
	if cmd.Expected == nil {
		expectMissing = "1"
	}
	ret, err := client.Eval(ctx, compareAndSetScript, []string{cmd.Key}, string(cmd.Expected), string(cmd.Next), ttlMillis, expectMissing)
	if err != nil {
		return CompareAndSetResult{}, err
	}
	return parseCompareAndSetResult(ret)
}

func parseCompareAndSetResult(ret any) (CompareAndSetResult, error) {
	items, ok := ret.([]any)
	if !ok {
		if typed, ok := ret.([]interface{}); ok {
			items = typed
		} else {
			return CompareAndSetResult{}, fmt.Errorf("redis cas: unexpected result %T", ret)
		}
	}
	if len(items) != 2 {
		return CompareAndSetResult{}, fmt.Errorf("redis cas: invalid result length %d", len(items))
	}
	applied, err := parseCASApplied(items[0])
	if err != nil {
		return CompareAndSetResult{}, err
	}
	current, err := parseCASBytes(items[1])
	if err != nil {
		return CompareAndSetResult{}, err
	}
	return CompareAndSetResult{Applied: applied, Current: current}, nil
}

func parseCASApplied(raw any) (bool, error) {
	switch v := raw.(type) {
	case int64:
		return v != 0, nil
	case int:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false, fmt.Errorf("redis cas: invalid applied flag %q", v)
		}
		return n != 0, nil
	default:
		return false, fmt.Errorf("redis cas: invalid applied flag %T", raw)
	}
}

func parseCASBytes(raw any) ([]byte, error) {
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case []byte:
		return append([]byte(nil), v...), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("redis cas: invalid current value %T", raw)
	}
}
