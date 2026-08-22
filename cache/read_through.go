package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrLoadWaitersExceeded = errors.New("cache: load waiter limit exceeded")

type Loader[K comparable, V any] func(context.Context, K) (V, bool, error)

type ReadThroughOptions struct {
	LocalTTL          time.Duration
	LoadTimeout       time.Duration
	MaxWaitersPerKey  int
	IgnoreRemoteError bool
}

type ReadThroughStats struct {
	Loads       uint64
	Coalesced   uint64
	LoadErrors  uint64
	RemoteError uint64
}

type readThroughCall[V any] struct {
	done    chan struct{}
	value   V
	ok      bool
	err     error
	waiters int
}

// ReadThroughStore composes L1, optional L2, and an authoritative loader. It
// coalesces concurrent misses per key without spawning a goroutine per load.
type ReadThroughStore[K comparable, V any] struct {
	cfg    StoreConfig[K, V]
	local  Store[K, V]
	remote Store[K, V]
	loader Loader[K, V]
	opts   ReadThroughOptions

	mu    sync.Mutex
	calls map[K]*readThroughCall[V]

	loads       atomic.Uint64
	coalesced   atomic.Uint64
	loadErrors  atomic.Uint64
	remoteError atomic.Uint64
}

func NewReadThroughStore[K comparable, V any](local Store[K, V], remote Store[K, V], loader Loader[K, V], cfg StoreConfig[K, V], opts ReadThroughOptions) *ReadThroughStore[K, V] {
	if opts.MaxWaitersPerKey <= 0 {
		opts.MaxWaitersPerKey = 256
	}
	return &ReadThroughStore[K, V]{cfg: cfg, local: local, remote: remote, loader: loader, opts: opts, calls: make(map[K]*readThroughCall[V])}
}

func (s *ReadThroughStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if s == nil || !s.cfg.validKey(key) {
		return zero, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.local != nil {
		value, ok, err := s.local.Get(ctx, key)
		if err != nil || ok {
			return value, ok, err
		}
	}
	return s.load(ctx, key)
}

func (s *ReadThroughStore[K, V]) load(ctx context.Context, key K) (V, bool, error) {
	s.mu.Lock()
	if call := s.calls[key]; call != nil {
		if call.waiters >= s.opts.MaxWaitersPerKey {
			s.mu.Unlock()
			var zero V
			return zero, false, ErrLoadWaitersExceeded
		}
		call.waiters++
		s.coalesced.Add(1)
		s.mu.Unlock()
		select {
		case <-call.done:
			return call.value, call.ok, call.err
		case <-ctx.Done():
			var zero V
			return zero, false, ctx.Err()
		}
	}
	call := &readThroughCall[V]{done: make(chan struct{})}
	s.calls[key] = call
	s.mu.Unlock()

	loadCtx := ctx
	var cancel context.CancelFunc
	if s.opts.LoadTimeout > 0 {
		loadCtx, cancel = context.WithTimeout(ctx, s.opts.LoadTimeout)
		defer cancel()
	}
	call.value, call.ok, call.err = s.loadOne(loadCtx, key)
	if call.err != nil {
		s.loadErrors.Add(1)
	}
	s.mu.Lock()
	delete(s.calls, key)
	close(call.done)
	s.mu.Unlock()
	return call.value, call.ok, call.err
}

func (s *ReadThroughStore[K, V]) loadOne(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if s.remote != nil {
		value, ok, err := s.remote.Get(ctx, key)
		if err != nil {
			s.remoteError.Add(1)
			if !s.opts.IgnoreRemoteError {
				return zero, false, err
			}
		} else if ok {
			s.setLocal(ctx, value)
			return value, true, nil
		}
	}
	if s.loader == nil {
		return zero, false, nil
	}
	s.loads.Add(1)
	value, ok, err := s.loader(ctx, key)
	if err != nil || !ok {
		return value, ok, err
	}
	if s.remote != nil {
		if err := s.remote.Set(ctx, value); err != nil && !s.opts.IgnoreRemoteError {
			return zero, false, err
		}
	}
	if err := s.setLocal(ctx, value); err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func (s *ReadThroughStore[K, V]) Set(ctx context.Context, value V) error {
	if s == nil {
		return nil
	}
	if s.cfg.ValidateValue != nil {
		if err := s.cfg.ValidateValue(value); err != nil {
			return err
		}
	}
	if _, err := s.cfg.keyOf(value); err != nil {
		return err
	}
	if s.remote != nil {
		if err := s.remote.Set(ctx, value); err != nil && !s.opts.IgnoreRemoteError {
			return err
		}
	}
	return s.setLocal(ctx, value)
}

func (s *ReadThroughStore[K, V]) Delete(ctx context.Context, key K) error {
	if s == nil || !s.cfg.validKey(key) {
		return nil
	}
	var joined error
	if s.remote != nil {
		joined = errors.Join(joined, s.remote.Delete(ctx, key))
	}
	if s.local != nil {
		joined = errors.Join(joined, s.local.Delete(ctx, key))
	}
	return joined
}

func (s *ReadThroughStore[K, V]) Stats() ReadThroughStats {
	if s == nil {
		return ReadThroughStats{}
	}
	return ReadThroughStats{Loads: s.loads.Load(), Coalesced: s.coalesced.Load(), LoadErrors: s.loadErrors.Load(), RemoteError: s.remoteError.Load()}
}

func (s *ReadThroughStore[K, V]) setLocal(ctx context.Context, value V) error {
	if s.local == nil {
		return nil
	}
	if expiring, ok := s.local.(ExpiringStore[K, V]); ok {
		return expiring.SetWithTTL(ctx, value, s.opts.LocalTTL)
	}
	return s.local.Set(ctx, value)
}

var _ Store[int, int] = (*ReadThroughStore[int, int])(nil)
