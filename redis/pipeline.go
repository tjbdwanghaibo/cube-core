package redis

import (
	"context"
	"time"
)

// IPipeline batches multiple commands into a single round-trip.
type IPipeline interface {
	Get(ctx context.Context, key string) *FutureBytes
	Set(ctx context.Context, key string, value any, expiration time.Duration)
	Del(ctx context.Context, keys ...string)
	HSet(ctx context.Context, key string, values ...any)
	HGet(ctx context.Context, key, field string) *FutureBytes
	HGetAll(ctx context.Context, key string) *FutureStringMap
	Incr(ctx context.Context, key string) *FutureInt64
	Expire(ctx context.Context, key string, expiration time.Duration)
	ZAdd(ctx context.Context, key string, members ...Z)
	RPush(ctx context.Context, key string, values ...any)
	LPop(ctx context.Context, key string) *FutureBytes

	// Exec sends all buffered commands and returns results.
	Exec(ctx context.Context) error

	// Discard clears buffered commands without executing.
	Discard()
}

// FutureBytes holds a deferred []byte result from pipeline.
type FutureBytes struct {
	val []byte
	err error
}

func NewFutureBytes(val []byte, err error) *FutureBytes {
	return &FutureBytes{val: val, err: err}
}

func (f *FutureBytes) Result() ([]byte, error) { return f.val, f.err }

func (f *FutureBytes) SetResult(val []byte, err error) {
	f.val = val
	f.err = err
}

// FutureStringMap holds a deferred map[string]string result from pipeline.
type FutureStringMap struct {
	val map[string]string
	err error
}

func NewFutureStringMap(val map[string]string, err error) *FutureStringMap {
	return &FutureStringMap{val: val, err: err}
}

func (f *FutureStringMap) Result() (map[string]string, error) { return f.val, f.err }

func (f *FutureStringMap) SetResult(val map[string]string, err error) {
	f.val = val
	f.err = err
}

// FutureInt64 holds a deferred int64 result from pipeline.
type FutureInt64 struct {
	val int64
	err error
}

func NewFutureInt64(val int64, err error) *FutureInt64 {
	return &FutureInt64{val: val, err: err}
}

func (f *FutureInt64) Result() (int64, error) { return f.val, f.err }

func (f *FutureInt64) SetResult(val int64, err error) {
	f.val = val
	f.err = err
}
