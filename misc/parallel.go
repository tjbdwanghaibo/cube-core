package misc

import (
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
)

type ParallelMapOption func(*parallelMapConfig)

type parallelMapConfig struct {
	maxWorkers int
}

func WithMaxWorkers(n int) ParallelMapOption {
	return func(cfg *parallelMapConfig) {
		if n > 0 {
			cfg.maxWorkers = n
		}
	}
}

func buildConfig(dataLen int, opts []ParallelMapOption) *parallelMapConfig {
	cfg := &parallelMapConfig{maxWorkers: runtime.NumCPU()}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.maxWorkers > 8 {
		cfg.maxWorkers = 8
	}
	if cfg.maxWorkers > dataLen {
		cfg.maxWorkers = dataLen
	}
	if cfg.maxWorkers < 1 {
		cfg.maxWorkers = 1
	}
	return cfg
}

func ParallelMap[K comparable, V any](data map[K]V, fn func(K, V), opts ...ParallelMapOption) {
	if len(data) == 0 {
		return
	}
	cfg := buildConfig(len(data), opts)

	type entry struct {
		k K
		v V
	}
	entries := make([]entry, 0, len(data))
	for k, v := range data {
		entries = append(entries, entry{k, v})
	}

	parallelExec(cfg.maxWorkers, len(entries), func(idx int) {
		e := entries[idx]
		safeCall(func() { fn(e.k, e.v) }, e.k)
	})
}

func ParallelMapCollect[K comparable, V any, R any](data map[K]V, fn func(K, V) R, opts ...ParallelMapOption) []R {
	if len(data) == 0 {
		return nil
	}
	cfg := buildConfig(len(data), opts)

	type entry struct {
		k K
		v V
	}
	entries := make([]entry, 0, len(data))
	for k, v := range data {
		entries = append(entries, entry{k, v})
	}

	results := make([]R, len(entries))
	parallelExec(cfg.maxWorkers, len(entries), func(idx int) {
		e := entries[idx]
		safeCall(func() { results[idx] = fn(e.k, e.v) }, e.k)
	})
	return results
}

func ParallelSlice[V any](data []V, fn func(int, V), opts ...ParallelMapOption) {
	if len(data) == 0 {
		return
	}
	cfg := buildConfig(len(data), opts)

	parallelExec(cfg.maxWorkers, len(data), func(idx int) {
		safeCall(func() { fn(idx, data[idx]) }, idx)
	})
}

func ParallelSliceCollect[V any, R any](data []V, fn func(int, V) R, opts ...ParallelMapOption) []R {
	if len(data) == 0 {
		return nil
	}
	cfg := buildConfig(len(data), opts)

	results := make([]R, len(data))
	parallelExec(cfg.maxWorkers, len(data), func(idx int) {
		safeCall(func() { results[idx] = fn(idx, data[idx]) }, idx)
	})
	return results
}

func parallelExec(workerNum, total int, work func(idx int)) {
	if workerNum <= 1 {
		for i := 0; i < total; i++ {
			work(i)
		}
		return
	}

	var cursor atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workerNum)
	for w := 0; w < workerNum; w++ {
		go func() {
			defer wg.Done()
			for {
				idx := int(cursor.Add(1) - 1)
				if idx >= total {
					return
				}
				work(idx)
			}
		}()
	}
	wg.Wait()
}

func safeCall(fn func(), key any) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("ParallelMap panic", "key", key, "err", r)
		}
	}()
	fn()
}
