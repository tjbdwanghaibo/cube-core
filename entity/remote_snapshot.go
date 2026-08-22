package entity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tjbdwanghaibo/cube-core/cache"
)

var (
	ErrRemoteSnapshotGap            = errors.New("remote snapshot: delta gap")
	ErrRemoteSnapshotEpochMismatch  = errors.New("remote snapshot: epoch mismatch")
	ErrRemoteSnapshotSchemaMismatch = errors.New("remote snapshot: schema mismatch")
)

type RemoteReadConsistency uint8

const (
	RemoteReadMonotonic RemoteReadConsistency = iota + 1
	RemoteReadCached
	RemoteReadLinearizable
)

type RemoteSnapshotKey struct {
	Tenant   uint32
	EntityID int64
	Kind     EntityKind
	Scope    uint32
	Policy   uint32
}

func (k RemoteSnapshotKey) Valid() bool {
	meta := ResolveEntityID(k.EntityID)
	return meta.FullID == k.EntityID && meta.Kind == k.Kind && k.Kind != EntityKindNone
}

type ImmutableRemoteSnapshot interface {
	RemoteSnapshotSchema() uint32
	RemoteSnapshotSize() int
}

// FrozenRemoteSnapshotPayload owns immutable snapshot bytes. It makes L1 reads
// allocation-free without exposing the cache's backing slice to business code.
type FrozenRemoteSnapshotPayload struct{ data []byte }

func CopyFrozenRemoteSnapshotPayload(data []byte) FrozenRemoteSnapshotPayload {
	return FrozenRemoteSnapshotPayload{data: append([]byte(nil), data...)}
}

// TakeFrozenRemoteSnapshotPayload transfers ownership. The caller must not
// mutate data after this call.
func TakeFrozenRemoteSnapshotPayload(data []byte) FrozenRemoteSnapshotPayload {
	return FrozenRemoteSnapshotPayload{data: data}
}

func (p FrozenRemoteSnapshotPayload) Len() int                   { return len(p.data) }
func (p FrozenRemoteSnapshotPayload) BytesCopy() []byte          { return append([]byte(nil), p.data...) }
func (p FrozenRemoteSnapshotPayload) AppendTo(dst []byte) []byte { return append(dst, p.data...) }

type RemoteSnapshotEnvelope struct {
	Key          RemoteSnapshotKey
	StateVersion uint64
	BaseVersion  uint64
	MarkerEpoch  uint64
	RouteEpoch   uint64
	Schema       uint32
	Codec        uint16
	Checksum     uint64
	Full         bool
	PublishedAt  int64
	ExpiresAt    int64
	Payload      FrozenRemoteSnapshotPayload
}

func (s RemoteSnapshotEnvelope) Clone() RemoteSnapshotEnvelope {
	return s
}

func (s RemoteSnapshotEnvelope) Valid() error {
	if !s.Key.Valid() || s.StateVersion == 0 || s.MarkerEpoch == 0 || s.RouteEpoch == 0 || s.Schema == 0 || s.Payload.Len() == 0 {
		return fmt.Errorf("remote snapshot: invalid envelope")
	}
	if s.Checksum != RemoteSnapshotChecksum(s.Payload.data) {
		return fmt.Errorf("remote snapshot: checksum mismatch")
	}
	if !s.Full && s.BaseVersion == 0 {
		return fmt.Errorf("remote snapshot: delta missing base version")
	}
	return nil
}

func (s RemoteSnapshotEnvelope) Expired(now time.Time) bool {
	return s.ExpiresAt > 0 && now.UnixNano() >= s.ExpiresAt
}

type RemoteSnapshotLoader func(context.Context, RemoteSnapshotKey, RemoteReadConsistency, uint64) (RemoteSnapshotEnvelope, bool, error)

type RemoteSnapshotCacheConfig struct {
	Shards      int
	MaxEntries  int
	MaxBytes    int64
	TTL         time.Duration
	LoadTimeout time.Duration
	MaxWaiters  int
}

// RemoteSnapshotCache is the entity-specific adapter over core/cache. Epoch,
// scope, and minimum-version rules stay here instead of polluting cache.Store.
type RemoteSnapshotCache struct {
	local   *cache.AtomicLocalStore[RemoteSnapshotKey, RemoteSnapshotEnvelope]
	layered *cache.ReadThroughStore[RemoteSnapshotKey, RemoteSnapshotEnvelope]

	waitMu      sync.Mutex
	waiters     map[RemoteSnapshotKey][]remoteVersionWaiter
	waiterCount int
	maxWaiters  int
	loader      RemoteSnapshotLoader
	publishMu   [64]sync.Mutex
}

type remoteVersionWaiter struct {
	min  uint64
	done chan struct{}
}

func NewRemoteSnapshotCache(cfg RemoteSnapshotCacheConfig, l2 cache.Store[RemoteSnapshotKey, RemoteSnapshotEnvelope], loader RemoteSnapshotLoader) *RemoteSnapshotCache {
	storeCfg := cache.StoreConfig[RemoteSnapshotKey, RemoteSnapshotEnvelope]{
		KeyOf: func(value RemoteSnapshotEnvelope) RemoteSnapshotKey { return value.Key },
		Stale: func(old, next RemoteSnapshotEnvelope) bool {
			if old.MarkerEpoch != next.MarkerEpoch || old.RouteEpoch != next.RouteEpoch {
				return old.MarkerEpoch > next.MarkerEpoch || old.RouteEpoch > next.RouteEpoch
			}
			return old.StateVersion > next.StateVersion
		},
		ValidateKey:   func(key RemoteSnapshotKey) bool { return key.Valid() },
		ValidateValue: func(value RemoteSnapshotEnvelope) error { return value.Valid() },
	}
	local := cache.NewAtomicLocalStore(cache.AtomicLocalConfig[RemoteSnapshotKey, RemoteSnapshotEnvelope]{
		StoreConfig: storeCfg, Shards: cfg.Shards, MaxEntries: cfg.MaxEntries,
		MaxBytes: cfg.MaxBytes, DefaultTTL: cfg.TTL,
		SizeOf: func(value RemoteSnapshotEnvelope) int64 { return int64(value.Payload.Len() + 96) },
	})
	loadFn := cache.Loader[RemoteSnapshotKey, RemoteSnapshotEnvelope](nil)
	if loader != nil {
		loadFn = func(ctx context.Context, key RemoteSnapshotKey) (RemoteSnapshotEnvelope, bool, error) {
			return loader(ctx, key, RemoteReadMonotonic, 0)
		}
	}
	return &RemoteSnapshotCache{
		local: local,
		layered: cache.NewReadThroughStore(local, l2, loadFn, storeCfg, cache.ReadThroughOptions{
			LocalTTL: cfg.TTL, LoadTimeout: cfg.LoadTimeout, MaxWaitersPerKey: cfg.MaxWaiters, IgnoreRemoteError: true,
		}),
		waiters:    make(map[RemoteSnapshotKey][]remoteVersionWaiter),
		maxWaiters: cfg.MaxWaiters,
		loader:     loader,
	}
}

func (c *RemoteSnapshotCache) LoadAuthoritative(ctx context.Context, key RemoteSnapshotKey, consistency RemoteReadConsistency, minVersion uint64) (RemoteSnapshotEnvelope, bool, error) {
	if c == nil || c.loader == nil {
		return RemoteSnapshotEnvelope{}, false, nil
	}
	snapshot, ok, err := c.loader(ctx, key, consistency, minVersion)
	if err != nil || !ok {
		return snapshot, ok, err
	}
	if snapshot.StateVersion < minVersion {
		return RemoteSnapshotEnvelope{}, false, ErrRemoteSnapshotStale
	}
	if err := c.Publish(ctx, snapshot); err != nil {
		return RemoteSnapshotEnvelope{}, false, err
	}
	stored, found, err := c.local.Get(ctx, key)
	return stored.Clone(), found, err
}

func (c *RemoteSnapshotCache) Get(ctx context.Context, key RemoteSnapshotKey, consistency RemoteReadConsistency, minVersion uint64) (RemoteSnapshotEnvelope, bool, error) {
	if c == nil || c.layered == nil {
		return RemoteSnapshotEnvelope{}, false, nil
	}
	snapshot, ok, err := c.layered.Get(ctx, key)
	if err != nil || !ok {
		return snapshot, ok, err
	}
	if consistency == RemoteReadCached || snapshot.StateVersion >= minVersion {
		return snapshot, true, nil
	}
	return RemoteSnapshotEnvelope{}, false, ErrRemoteSnapshotStale
}

func (c *RemoteSnapshotCache) Publish(ctx context.Context, snapshot RemoteSnapshotEnvelope) error {
	if c == nil || c.layered == nil {
		return nil
	}
	snapshot.Checksum = RemoteSnapshotChecksum(snapshot.Payload.data)
	lock := &c.publishMu[remoteSnapshotPublishShard(snapshot.Key)]
	lock.Lock()
	defer lock.Unlock()
	if current, ok, err := c.local.Get(ctx, snapshot.Key); err != nil {
		return err
	} else if ok && current.MarkerEpoch == snapshot.MarkerEpoch && current.RouteEpoch == snapshot.RouteEpoch && current.StateVersion == snapshot.StateVersion {
		if current.Schema != snapshot.Schema || current.Codec != snapshot.Codec || current.Checksum != snapshot.Checksum {
			return fmt.Errorf("%w: same snapshot version has different content", ErrRemoteVersionConflict)
		}
		return nil
	}
	if err := c.layered.Set(ctx, snapshot); err != nil {
		return err
	}
	c.notify(snapshot.Key, snapshot.StateVersion)
	return nil
}

func remoteSnapshotPublishShard(key RemoteSnapshotKey) uint64 {
	h := uint64(key.EntityID) ^ uint64(key.Tenant)<<32 ^ uint64(key.Kind)<<48
	h ^= uint64(key.Scope)*0x9e3779b185ebca87 ^ uint64(key.Policy)*0xc2b2ae3d27d4eb4f
	return h & 63
}

func (c *RemoteSnapshotCache) ApplyUpdate(ctx context.Context, update RemoteSnapshotRecord) error {
	if c == nil {
		return nil
	}
	if update.Checksum != 0 && RemoteSnapshotChecksum(update.Data) != update.Checksum {
		return fmt.Errorf("remote snapshot: checksum mismatch")
	}
	if update.Full {
		return c.Publish(ctx, RemoteSnapshotEnvelope{
			Key: update.Key, BaseVersion: update.BaseVersion, StateVersion: update.StateVersion,
			MarkerEpoch: update.MarkerEpoch, RouteEpoch: update.RouteEpoch,
			Schema: update.Schema, Codec: update.Codec, Full: true,
			PublishedAt: time.Now().UnixNano(), Payload: CopyFrozenRemoteSnapshotPayload(update.Data),
		})
	}
	current, ok, err := c.local.Get(ctx, update.Key)
	if err != nil {
		return err
	}
	if !ok || current.StateVersion != update.BaseVersion {
		return ErrRemoteSnapshotGap
	}
	if current.MarkerEpoch != update.MarkerEpoch || current.RouteEpoch != update.RouteEpoch {
		return ErrRemoteSnapshotEpochMismatch
	}
	if current.Schema != update.Schema || current.Codec != update.Codec {
		return ErrRemoteSnapshotSchemaMismatch
	}
	data, err := applyRemoteSnapshotDelta(update.Schema, current.Payload.data, update.Data)
	if err != nil {
		return err
	}
	return c.Publish(ctx, RemoteSnapshotEnvelope{
		Key: update.Key, BaseVersion: update.BaseVersion, StateVersion: update.StateVersion,
		MarkerEpoch: update.MarkerEpoch, RouteEpoch: update.RouteEpoch,
		Schema: update.Schema, Codec: update.Codec, Full: false,
		PublishedAt: time.Now().UnixNano(), Payload: TakeFrozenRemoteSnapshotPayload(data),
	})
}

func (c *RemoteSnapshotCache) Delete(ctx context.Context, key RemoteSnapshotKey) error {
	if c == nil || c.layered == nil {
		return nil
	}
	return c.layered.Delete(ctx, key)
}

func (c *RemoteSnapshotCache) WaitForVersion(ctx context.Context, key RemoteSnapshotKey, minVersion uint64) error {
	if minVersion == 0 {
		return nil
	}
	if snapshot, ok, _ := c.local.Get(ctx, key); ok && snapshot.StateVersion >= minVersion {
		return nil
	}
	waiter := remoteVersionWaiter{min: minVersion, done: make(chan struct{})}
	c.waitMu.Lock()
	if c.maxWaiters > 0 && c.waiterCount >= c.maxWaiters {
		c.waitMu.Unlock()
		return ErrRemoteOverloaded
	}
	c.waiters[key] = append(c.waiters[key], waiter)
	c.waiterCount++
	c.waitMu.Unlock()
	if snapshot, ok, _ := c.local.Get(ctx, key); ok && snapshot.StateVersion >= minVersion {
		c.notify(key, snapshot.StateVersion)
	}
	select {
	case <-waiter.done:
		return nil
	case <-ctx.Done():
		c.removeWaiter(key, waiter.done)
		return ctx.Err()
	}
}

func (c *RemoteSnapshotCache) Stats() (cache.AtomicLocalStats, cache.ReadThroughStats) {
	if c == nil {
		return cache.AtomicLocalStats{}, cache.ReadThroughStats{}
	}
	return c.local.Stats(), c.layered.Stats()
}

func (c *RemoteSnapshotCache) notify(key RemoteSnapshotKey, version uint64) {
	c.waitMu.Lock()
	waiters := c.waiters[key]
	remaining := waiters[:0]
	for _, waiter := range waiters {
		if version >= waiter.min {
			close(waiter.done)
			c.waiterCount--
		} else {
			remaining = append(remaining, waiter)
		}
	}
	if len(remaining) == 0 {
		delete(c.waiters, key)
	} else {
		c.waiters[key] = remaining
	}
	c.waitMu.Unlock()
}

func (c *RemoteSnapshotCache) removeWaiter(key RemoteSnapshotKey, done chan struct{}) {
	c.waitMu.Lock()
	waiters := c.waiters[key]
	for i := range waiters {
		if waiters[i].done == done {
			waiters = append(waiters[:i], waiters[i+1:]...)
			c.waiterCount--
			break
		}
	}
	if len(waiters) == 0 {
		delete(c.waiters, key)
	} else {
		c.waiters[key] = waiters
	}
	c.waitMu.Unlock()
}
