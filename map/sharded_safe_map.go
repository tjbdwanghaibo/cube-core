package fmap

import (
	"sync"
	"sync/atomic"
)

// ShardedSafeMap is a thread-safe map for large object counts and concurrent access.
type ShardedSafeMap[K comparable, V any] struct {
	hash   HashFunc[K]
	mask   uint64
	size   atomic.Int64
	shards []safeShard[K, V]
}

type safeShard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func NewShardedSafeMap[K comparable, V any](shardCount int, hash HashFunc[K]) *ShardedSafeMap[K, V] {
	if hash == nil {
		panic("fmap: nil hash func")
	}
	shardCount = normalizeShardCount(shardCount)
	return &ShardedSafeMap[K, V]{
		hash:   hash,
		mask:   uint64(shardCount - 1),
		shards: make([]safeShard[K, V], shardCount),
	}
}

func NewIntegerShardedSafeMap[K Integer, V any](shardCount int) *ShardedSafeMap[K, V] {
	return NewShardedSafeMap[K, V](shardCount, HashInteger[K])
}

func NewInt64ShardedSafeMap[V any](shardCount int) *ShardedSafeMap[int64, V] {
	return NewIntegerShardedSafeMap[int64, V](shardCount)
}

func NewUint64ShardedSafeMap[V any](shardCount int) *ShardedSafeMap[uint64, V] {
	return NewIntegerShardedSafeMap[uint64, V](shardCount)
}

func NewStringShardedSafeMap[V any](shardCount int) *ShardedSafeMap[string, V] {
	return NewShardedSafeMap[string, V](shardCount, HashString)
}

func (m *ShardedSafeMap[K, V]) Set(key K, value V) {
	shard := m.shard(key)
	shard.mu.Lock()
	if shard.m == nil {
		shard.m = make(map[K]V)
	}
	if _, exists := shard.m[key]; !exists {
		m.size.Add(1)
	}
	shard.m[key] = value
	shard.mu.Unlock()
}

func (m *ShardedSafeMap[K, V]) Get(key K) (V, bool) {
	shard := m.shard(key)
	shard.mu.RLock()
	value, ok := shard.m[key]
	shard.mu.RUnlock()
	return value, ok
}

func (m *ShardedSafeMap[K, V]) Read(key K, f func(value V, exists bool)) bool {
	if f == nil {
		_, ok := m.Get(key)
		return ok
	}
	shard := m.shard(key)
	shard.mu.RLock()
	value, ok := shard.m[key]
	f(value, ok)
	shard.mu.RUnlock()
	return ok
}

func (m *ShardedSafeMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	shard := m.shard(key)
	shard.mu.Lock()
	if shard.m == nil {
		shard.m = make(map[K]V)
	}
	actual, loaded = shard.m[key]
	if !loaded {
		shard.m[key] = value
		actual = value
		m.size.Add(1)
	}
	shard.mu.Unlock()
	return actual, loaded
}

func (m *ShardedSafeMap[K, V]) Compute(key K, f ComputeFunc[V]) (V, bool) {
	var zero V
	if f == nil {
		return zero, false
	}
	shard := m.shard(key)
	shard.mu.Lock()
	old, exists := shard.m[key]
	next, keep := f(old, exists)
	if keep {
		if shard.m == nil {
			shard.m = make(map[K]V)
		}
		if !exists {
			m.size.Add(1)
		}
		shard.m[key] = next
		shard.mu.Unlock()
		return next, true
	}
	if exists {
		delete(shard.m, key)
		m.size.Add(-1)
	}
	shard.mu.Unlock()
	return zero, false
}

func (m *ShardedSafeMap[K, V]) Delete(key K) bool {
	shard := m.shard(key)
	shard.mu.Lock()
	_, ok := shard.m[key]
	if ok {
		delete(shard.m, key)
		m.size.Add(-1)
	}
	shard.mu.Unlock()
	return ok
}

func (m *ShardedSafeMap[K, V]) Len() int {
	return int(m.size.Load())
}

func (m *ShardedSafeMap[K, V]) ShardCount() int {
	if m == nil {
		return 0
	}
	return len(m.shards)
}

func (m *ShardedSafeMap[K, V]) ShardIndex(key K) int {
	if m == nil || len(m.shards) == 0 {
		return -1
	}
	return int(m.hash(key) & m.mask)
}

func (m *ShardedSafeMap[K, V]) Clear() {
	for i := range m.shards {
		m.shards[i].mu.Lock()
	}
	for i := range m.shards {
		m.shards[i].m = nil
	}
	m.size.Store(0)
	for i := len(m.shards) - 1; i >= 0; i-- {
		m.shards[i].mu.Unlock()
	}
}

func (m *ShardedSafeMap[K, V]) Snapshot() []Entry[K, V] {
	if m == nil {
		return nil
	}
	entries := make([]Entry[K, V], 0, m.Len())
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		for key, value := range shard.m {
			entries = append(entries, Entry[K, V]{Key: key, Value: value})
		}
		shard.mu.RUnlock()
	}
	return entries
}

func (m *ShardedSafeMap[K, V]) Range(f func(key K, value V) bool) {
	if f == nil {
		return
	}
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		entries := make([]Entry[K, V], 0, len(shard.m))
		for key, value := range shard.m {
			entries = append(entries, Entry[K, V]{Key: key, Value: value})
		}
		shard.mu.RUnlock()

		for j := range entries {
			if !f(entries[j].Key, entries[j].Value) {
				return
			}
		}
	}
}

func (m *ShardedSafeMap[K, V]) shard(key K) *safeShard[K, V] {
	return &m.shards[m.hash(key)&m.mask]
}

var _ IMap[int64, int64] = (*ShardedSafeMap[int64, int64])(nil)
