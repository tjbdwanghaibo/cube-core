package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type atomicTestValue struct {
	Key     int
	Version int64
	Size    int64
}

func atomicTestConfig() AtomicLocalConfig[int, atomicTestValue] {
	return AtomicLocalConfig[int, atomicTestValue]{
		StoreConfig: StoreConfig[int, atomicTestValue]{
			KeyOf: func(v atomicTestValue) int { return v.Key },
			Stale: VersionStale(func(v atomicTestValue) int64 { return v.Version }),
		},
		Shards: 1,
		SizeOf: func(v atomicTestValue) int64 { return v.Size },
	}
}

func TestAtomicLocalStoreVersionTTLAndBounds(t *testing.T) {
	now := time.Unix(100, 0)
	cfg := atomicTestConfig()
	cfg.MaxEntries = 2
	cfg.MaxBytes = 8
	cfg.DefaultTTL = time.Second
	cfg.Now = func() time.Time { return now }
	store := NewAtomicLocalStore(cfg)
	ctx := context.Background()
	if err := store.Set(ctx, atomicTestValue{Key: 1, Version: 2, Size: 4}); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, atomicTestValue{Key: 1, Version: 1, Size: 4}); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := store.Get(ctx, 1)
	if !ok || got.Version != 2 {
		t.Fatalf("version=%d ok=%v", got.Version, ok)
	}
	if err := store.Set(ctx, atomicTestValue{Key: 2, Version: 1, Size: 4}); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, atomicTestValue{Key: 3, Version: 1, Size: 4}); err != nil {
		t.Fatal(err)
	}
	if stats := store.Stats(); stats.Entries != 2 || stats.Bytes != 8 || stats.Evictions == 0 {
		t.Fatalf("stats=%+v", stats)
	}
	now = now.Add(2 * time.Second)
	_, ok, _ = store.Get(ctx, 3)
	if ok {
		t.Fatal("expired entry returned")
	}
	if err := store.Set(ctx, atomicTestValue{Key: 4, Version: 1, Size: 9}); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("err=%v", err)
	}
}

func TestReadThroughStoreCoalescesMisses(t *testing.T) {
	cfg := atomicTestConfig()
	local := NewAtomicLocalStore(cfg)
	var loads atomic.Int32
	start := make(chan struct{})
	store := NewReadThroughStore[int, atomicTestValue](local, nil, func(context.Context, int) (atomicTestValue, bool, error) {
		loads.Add(1)
		<-start
		return atomicTestValue{Key: 7, Version: 1}, true, nil
	}, cfg.StoreConfig, ReadThroughOptions{MaxWaitersPerKey: 16})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, ok, err := store.Get(context.Background(), 7)
			if err != nil || !ok || value.Key != 7 {
				t.Errorf("value=%+v ok=%v err=%v", value, ok, err)
			}
		}()
	}
	for loads.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(start)
	wg.Wait()
	if got := loads.Load(); got != 1 {
		t.Fatalf("loads=%d", got)
	}
	if store.Stats().Coalesced == 0 {
		t.Fatal("expected coalesced reads")
	}
}
