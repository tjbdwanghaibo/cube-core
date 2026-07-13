package cache

import (
	"context"
	"errors"
)

var (
	ErrKeyFuncNil = errors.New("cache: key func is nil")
	ErrInvalidKey = errors.New("cache: invalid key")
)

type Store[K comparable, V any] interface {
	Get(ctx context.Context, key K) (V, bool, error)
	Set(ctx context.Context, value V) error
	Delete(ctx context.Context, key K) error
}

type KeyFunc[K comparable, V any] func(V) K
type StaleFunc[V any] func(old V, next V) bool
type ValidateKeyFunc[K comparable] func(K) bool
type ValidateValueFunc[V any] func(V) error

type StoreConfig[K comparable, V any] struct {
	KeyOf         KeyFunc[K, V]
	Stale         StaleFunc[V]
	ValidateKey   ValidateKeyFunc[K]
	ValidateValue ValidateValueFunc[V]
}

func (c StoreConfig[K, V]) keyOf(value V) (K, error) {
	var zero K
	if c.KeyOf == nil {
		return zero, ErrKeyFuncNil
	}
	key := c.KeyOf(value)
	if c.ValidateKey != nil && !c.ValidateKey(key) {
		return zero, ErrInvalidKey
	}
	return key, nil
}

func (c StoreConfig[K, V]) validKey(key K) bool {
	return c.ValidateKey == nil || c.ValidateKey(key)
}

func VersionStale[V any](versionOf func(V) int64) StaleFunc[V] {
	return func(old V, next V) bool {
		if versionOf == nil {
			return false
		}
		return versionOf(old) > versionOf(next)
	}
}
