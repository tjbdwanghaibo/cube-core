package fmap

import "testing"

func BenchmarkSmallSafeMapSetGet(b *testing.B) {
	m := NewSmallSafeMap[int64, int64](32)
	for i := 0; i < b.N; i++ {
		key := int64(i % 32)
		m.Set(key, int64(i))
		_, _ = m.Get(key)
	}
}

func BenchmarkShardedSafeMapSetGet(b *testing.B) {
	m := NewInt64ShardedSafeMap[int64](64)
	for i := 0; i < b.N; i++ {
		key := int64(i)
		m.Set(key, int64(i))
		_, _ = m.Get(key)
	}
}

func BenchmarkFastMapSetGet(b *testing.B) {
	m := NewInt64FastMap[int64](b.N)
	for i := 0; i < b.N; i++ {
		key := int64(i)
		m.Set(key, int64(i))
		_, _ = m.Get(key)
	}
}
