package cache

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

var ErrEntryTooLarge = errors.New("cache: entry exceeds byte limit")

// ExpiringStore extends Store with per-entry TTL. A zero TTL means that the
// store default is used; a negative TTL disables expiration for that write.
type ExpiringStore[K comparable, V any] interface {
	Store[K, V]
	SetWithTTL(ctx context.Context, value V, ttl time.Duration) error
}

// AtomicLocalConfig configures the production in-process cache. Reads only
// take a shard RLock and never mutate a global LRU list.
type AtomicLocalConfig[K comparable, V any] struct {
	StoreConfig[K, V]
	Shards     int
	MaxEntries int
	MaxBytes   int64
	DefaultTTL time.Duration
	SizeOf     func(V) int64
	Now        func() time.Time
}

type AtomicLocalStats struct {
	Hits      uint64
	Misses    uint64
	Expired   uint64
	Evictions uint64
	Entries   int64
	Bytes     int64
}

type atomicLocalCounters struct {
	hits      atomic.Uint64
	misses    atomic.Uint64
	expired   atomic.Uint64
	evictions atomic.Uint64
	entries   atomic.Int64
	bytes     atomic.Int64
}

type atomicLocalEntry[V any] struct {
	value      V
	expiresAt  int64
	size       int64
	generation uint64
}

type atomicLocalOrder[K comparable] struct {
	key        K
	generation uint64
}

type atomicLocalShard[K comparable, V any] struct {
	mu         sync.RWMutex
	items      map[K]atomicLocalEntry[V]
	order      []atomicLocalOrder[K]
	hand       int
	entries    int
	bytes      int64
	generation uint64
}

// AtomicLocalStore is a bounded, sharded cache intended for immutable values.
// Eviction is insertion-clock based rather than exact LRU so cache hits do not
// contend on an exclusive lock.
type AtomicLocalStore[K comparable, V any] struct {
	cfg           AtomicLocalConfig[K, V]
	seed          maphash.Seed
	shards        []atomicLocalShard[K, V]
	maxPerShard   int
	bytesPerShard int64
	stats         atomicLocalCounters
}

func NewAtomicLocalStore[K comparable, V any](cfg AtomicLocalConfig[K, V]) *AtomicLocalStore[K, V] {
	shardCount := cfg.Shards
	if shardCount <= 0 {
		shardCount = 64
	}
	// Power-of-two shards make the hot-path selection a mask operation.
	shards := 1
	for shards < shardCount {
		shards <<= 1
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s := &AtomicLocalStore[K, V]{
		cfg:    cfg,
		seed:   maphash.MakeSeed(),
		shards: make([]atomicLocalShard[K, V], shards),
	}
	if cfg.MaxEntries > 0 {
		s.maxPerShard = (cfg.MaxEntries + shards - 1) / shards
	}
	if cfg.MaxBytes > 0 {
		s.bytesPerShard = (cfg.MaxBytes + int64(shards) - 1) / int64(shards)
	}
	for i := range s.shards {
		s.shards[i].items = make(map[K]atomicLocalEntry[V])
	}
	return s
}

func (s *AtomicLocalStore[K, V]) Get(_ context.Context, key K) (V, bool, error) {
	var zero V
	if s == nil || !s.cfg.validKey(key) {
		return zero, false, nil
	}
	shard := s.shard(key)
	shard.mu.RLock()
	entry, ok := shard.items[key]
	shard.mu.RUnlock()
	if !ok {
		s.stats.misses.Add(1)
		return zero, false, nil
	}
	if entry.expiresAt > 0 && s.cfg.Now().UnixNano() >= entry.expiresAt {
		s.expire(shard, key, entry.generation)
		s.stats.expired.Add(1)
		s.stats.misses.Add(1)
		return zero, false, nil
	}
	s.stats.hits.Add(1)
	return entry.value, true, nil
}

func (s *AtomicLocalStore[K, V]) Set(ctx context.Context, value V) error {
	return s.SetWithTTL(ctx, value, 0)
}

func (s *AtomicLocalStore[K, V]) SetWithTTL(_ context.Context, value V, ttl time.Duration) error {
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
	size := int64(0)
	if s.cfg.SizeOf != nil {
		size = s.cfg.SizeOf(value)
		if size < 0 {
			size = 0
		}
	}
	if s.cfg.MaxBytes > 0 && size > s.bytesPerShard {
		return ErrEntryTooLarge
	}
	if ttl == 0 {
		ttl = s.cfg.DefaultTTL
	}
	expiresAt := int64(0)
	if ttl > 0 {
		expiresAt = s.cfg.Now().Add(ttl).UnixNano()
	}
	shard := s.shard(key)
	shard.mu.Lock()
	old, exists := shard.items[key]
	if exists && s.cfg.Stale != nil && s.cfg.Stale(old.value, value) {
		shard.mu.Unlock()
		return nil
	}
	shard.generation++
	entry := atomicLocalEntry[V]{value: value, expiresAt: expiresAt, size: size, generation: shard.generation}
	shard.items[key] = entry
	shard.order = append(shard.order, atomicLocalOrder[K]{key: key, generation: entry.generation})
	if exists {
		shard.bytes += size - old.size
		s.stats.bytes.Add(size - old.size)
	} else {
		shard.entries++
		shard.bytes += size
		s.stats.entries.Add(1)
		s.stats.bytes.Add(size)
	}
	s.evictLocked(shard)
	shard.mu.Unlock()
	return nil
}

func (s *AtomicLocalStore[K, V]) Delete(_ context.Context, key K) error {
	if s == nil || !s.cfg.validKey(key) {
		return nil
	}
	shard := s.shard(key)
	shard.mu.Lock()
	s.deleteLocked(shard, key)
	shard.mu.Unlock()
	return nil
}

func (s *AtomicLocalStore[K, V]) Stats() AtomicLocalStats {
	if s == nil {
		return AtomicLocalStats{}
	}
	return AtomicLocalStats{
		Hits: s.stats.hits.Load(), Misses: s.stats.misses.Load(),
		Expired: s.stats.expired.Load(), Evictions: s.stats.evictions.Load(),
		Entries: s.stats.entries.Load(), Bytes: s.stats.bytes.Load(),
	}
}

func (s *AtomicLocalStore[K, V]) shard(key K) *atomicLocalShard[K, V] {
	hash := maphash.Comparable(s.seed, key)
	return &s.shards[int(hash&uint64(len(s.shards)-1))]
}

func (s *AtomicLocalStore[K, V]) expire(shard *atomicLocalShard[K, V], key K, generation uint64) {
	shard.mu.Lock()
	if current, ok := shard.items[key]; ok && current.generation == generation && current.expiresAt > 0 && s.cfg.Now().UnixNano() >= current.expiresAt {
		s.deleteLocked(shard, key)
	}
	shard.mu.Unlock()
}

func (s *AtomicLocalStore[K, V]) deleteLocked(shard *atomicLocalShard[K, V], key K) bool {
	entry, ok := shard.items[key]
	if !ok {
		return false
	}
	delete(shard.items, key)
	shard.entries--
	shard.bytes -= entry.size
	s.stats.entries.Add(-1)
	s.stats.bytes.Add(-entry.size)
	return true
}

func (s *AtomicLocalStore[K, V]) evictLocked(shard *atomicLocalShard[K, V]) {
	for (s.maxPerShard > 0 && shard.entries > s.maxPerShard) || (s.bytesPerShard > 0 && shard.bytes > s.bytesPerShard) {
		if shard.hand >= len(shard.order) {
			shard.order = shard.order[:0]
			shard.hand = 0
			for key, entry := range shard.items {
				shard.order = append(shard.order, atomicLocalOrder[K]{key: key, generation: entry.generation})
			}
			if len(shard.order) == 0 {
				return
			}
		}
		candidate := shard.order[shard.hand]
		shard.hand++
		entry, ok := shard.items[candidate.key]
		if !ok || entry.generation != candidate.generation {
			continue
		}
		if s.deleteLocked(shard, candidate.key) {
			s.stats.evictions.Add(1)
		}
	}
	// Periodically compact superseded clock records.
	if shard.hand > 1024 && shard.hand*2 > len(shard.order) {
		shard.order = append([]atomicLocalOrder[K](nil), shard.order[shard.hand:]...)
		shard.hand = 0
	}
}

var _ ExpiringStore[int, int] = (*AtomicLocalStore[int, int])(nil)
