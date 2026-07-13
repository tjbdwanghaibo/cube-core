package misc

// Key constraint for KeyMap keys.
type Key interface {
	int32 | int64 | uint64 | uint32
}

type kmEntry[K Key, V any] struct {
	key   K
	value V
}

// KeyMap is a high-performance array-based hash map for integer keys.
type KeyMap[K Key, V any] struct {
	capacity uint64
	buckets  [][]kmEntry[K, V]
	size     int
}

func NewKeyMap[K Key, V any](capacity uint64) *KeyMap[K, V] {
	if capacity == 0 {
		capacity = 1000
	}
	return &KeyMap[K, V]{
		capacity: capacity,
		buckets:  make([][]kmEntry[K, V], capacity),
		size:     0,
	}
}

func (m *KeyMap[K, V]) hashKey(key K) uint64 {
	x := uint64(key)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x = x ^ (x >> 31)
	return x
}

func (m *KeyMap[K, V]) getBucketIndex(key K) uint64 {
	return m.hashKey(key) % m.capacity
}

func (m *KeyMap[K, V]) Set(key K, value V) {
	index := m.getBucketIndex(key)
	for i, e := range m.buckets[index] {
		if e.key == key {
			m.buckets[index][i].value = value
			return
		}
	}
	m.buckets[index] = append(m.buckets[index], kmEntry[K, V]{key: key, value: value})
	m.size++
}

func (m *KeyMap[K, V]) Get(key K) (V, bool) {
	index := m.getBucketIndex(key)
	for _, e := range m.buckets[index] {
		if e.key == key {
			return e.value, true
		}
	}
	var zeroV V
	return zeroV, false
}

func (m *KeyMap[K, V]) Remove(key K) {
	index := m.getBucketIndex(key)
	bucket := m.buckets[index]
	n := len(bucket)
	for i := 0; i < n; i++ {
		if bucket[i].key == key {
			bucket[i] = bucket[n-1]
			var zero kmEntry[K, V]
			bucket[n-1] = zero
			m.buckets[index] = bucket[:n-1]
			m.size--
			return
		}
	}
}

func (m *KeyMap[K, V]) Len() int {
	return m.size
}

func (m *KeyMap[K, V]) Clear() {
	m.buckets = make([][]kmEntry[K, V], m.capacity)
	m.size = 0
}

func (m *KeyMap[K, V]) Range(f func(key K, value V) bool) {
	for _, bucket := range m.buckets {
		for _, e := range bucket {
			if !f(e.key, e.value) {
				return
			}
		}
	}
}
