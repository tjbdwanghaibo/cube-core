package fmap

type IMap[K comparable, V any] interface {
	Set(key K, value V)
	Get(key K) (V, bool)
	Delete(key K) bool
	Len() int
	Clear()
	Range(func(key K, value V) bool)
}

type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

type HashFunc[K comparable] func(K) uint64

type ComputeFunc[V any] func(old V, exists bool) (next V, keep bool)
