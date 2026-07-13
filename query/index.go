package query

import (
	"fmt"
	"sort"
	"sync"
)

type Index[K comparable, V comparable] struct {
	mu      sync.RWMutex
	primary map[K]V
	reverse map[V]map[K]struct{}
}

func NewIndex[K comparable, V comparable]() *Index[K, V] {
	return &Index[K, V]{
		primary: make(map[K]V),
		reverse: make(map[V]map[K]struct{}),
	}
}

func (i *Index[K, V]) Upsert(key K, value V) {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.ensureLocked()
	if old, ok := i.primary[key]; ok {
		if old == value {
			return
		}
		delete(i.reverse[old], key)
		if len(i.reverse[old]) == 0 {
			delete(i.reverse, old)
		}
	}
	i.primary[key] = value
	if i.reverse[value] == nil {
		i.reverse[value] = make(map[K]struct{})
	}
	i.reverse[value][key] = struct{}{}
}

func (i *Index[K, V]) Delete(key K) {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.primary == nil {
		return
	}
	value, ok := i.primary[key]
	if !ok {
		return
	}
	delete(i.primary, key)
	delete(i.reverse[value], key)
	if len(i.reverse[value]) == 0 {
		delete(i.reverse, value)
	}
}

func (i *Index[K, V]) Get(key K) (V, bool) {
	var zero V
	if i == nil {
		return zero, false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	value, ok := i.primary[key]
	return value, ok
}

func (i *Index[K, V]) Query(value V) []K {
	if i == nil {
		return nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	keys := i.reverse[value]
	if len(keys) == 0 {
		return nil
	}
	out := make([]K, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	return out
}

func (i *Index[K, V]) Len() int {
	if i == nil {
		return 0
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.primary)
}

func (i *Index[K, V]) Clear() {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.primary = make(map[K]V)
	i.reverse = make(map[V]map[K]struct{})
}

func (i *Index[K, V]) ensureLocked() {
	if i.primary == nil {
		i.primary = make(map[K]V)
	}
	if i.reverse == nil {
		i.reverse = make(map[V]map[K]struct{})
	}
}

type OrderedIndex[K comparable, V comparable] struct {
	index *Index[K, V]
	less  func(K, K) bool
}

func NewOrderedIndex[K comparable, V comparable](less func(K, K) bool) *OrderedIndex[K, V] {
	return &OrderedIndex[K, V]{index: NewIndex[K, V](), less: less}
}

func (i *OrderedIndex[K, V]) Upsert(key K, value V) { i.index.Upsert(key, value) }
func (i *OrderedIndex[K, V]) Delete(key K)          { i.index.Delete(key) }
func (i *OrderedIndex[K, V]) Get(key K) (V, bool)   { return i.index.Get(key) }
func (i *OrderedIndex[K, V]) Len() int              { return i.index.Len() }
func (i *OrderedIndex[K, V]) Clear()                { i.index.Clear() }

func (i *OrderedIndex[K, V]) Query(value V) []K {
	if i == nil || i.index == nil {
		return nil
	}
	out := i.index.Query(value)
	if i.less != nil {
		sort.Slice(out, func(a, b int) bool { return i.less(out[a], out[b]) })
		return out
	}
	sort.Slice(out, func(a, b int) bool {
		return fmt.Sprint(out[a]) < fmt.Sprint(out[b])
	})
	return out
}
