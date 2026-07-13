package fmap

import (
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// SmallSafeMap is a compact, thread-safe map for small object counts.
// It stores entries in a slice to avoid the allocation overhead of a hash table.
type SmallSafeMap[K comparable, V any] struct {
	mu      sync.RWMutex
	entries []Entry[K, V]
}

func NewSmallSafeMap[K comparable, V any](capHint int) *SmallSafeMap[K, V] {
	if capHint < 0 {
		capHint = 0
	}
	return &SmallSafeMap[K, V]{
		entries: make([]Entry[K, V], 0, capHint),
	}
}

func (m *SmallSafeMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.entries {
		if m.entries[i].Key == key {
			m.entries[i].Value = value
			return
		}
	}
	m.entries = append(m.entries, Entry[K, V]{Key: key, Value: value})
}

func (m *SmallSafeMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for i := range m.entries {
		if m.entries[i].Key == key {
			return m.entries[i].Value, true
		}
	}
	var zero V
	return zero, false
}

func (m *SmallSafeMap[K, V]) Delete(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.entries {
		if m.entries[i].Key != key {
			continue
		}
		last := len(m.entries) - 1
		m.entries[i] = m.entries[last]
		var zero Entry[K, V]
		m.entries[last] = zero
		m.entries = m.entries[:last]
		return true
	}
	return false
}

func (m *SmallSafeMap[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

func (m *SmallSafeMap[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.entries)
	m.entries = nil
}

func (m *SmallSafeMap[K, V]) Range(f func(key K, value V) bool) {
	if f == nil {
		return
	}
	m.mu.RLock()
	entries := append([]Entry[K, V](nil), m.entries...)
	m.mu.RUnlock()

	for i := range entries {
		if !f(entries[i].Key, entries[i].Value) {
			return
		}
	}
}

func (m *SmallSafeMap[K, V]) RawMap() map[K]V {
	if m == nil {
		return nil
	}
	ret := make(map[K]V, m.Len())
	m.Range(func(key K, value V) bool {
		ret[key] = value
		return true
	})
	return ret
}

func (m *SmallSafeMap[K, V]) SetRawMap(src map[K]V) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make([]Entry[K, V], 0, len(src))
	for key, value := range src {
		m.entries = append(m.entries, Entry[K, V]{Key: key, Value: value})
	}
}

func (m *SmallSafeMap[K, V]) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bson.MarshalValue(m.RawMap())
}

func (m *SmallSafeMap[K, V]) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
	var raw map[K]V
	if err := bson.UnmarshalValue(t, data, &raw); err != nil {
		return err
	}
	m.SetRawMap(raw)
	return nil
}

var _ IMap[int64, int64] = (*SmallSafeMap[int64, int64])(nil)
