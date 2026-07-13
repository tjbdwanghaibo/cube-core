package cache

import (
	"context"
	"strconv"
	"testing"
	"time"

	fsync "github.com/tjbdwanghaibo/cube-core/sync"
)

type testItem struct {
	ID      int64
	Version int64
	Data    string
}

func testItemConfig() StoreConfig[int64, testItem] {
	return StoreConfig[int64, testItem]{
		KeyOf:       func(item testItem) int64 { return item.ID },
		Stale:       VersionStale(func(item testItem) int64 { return item.Version }),
		ValidateKey: func(id int64) bool { return id != 0 },
	}
}

func TestLocalStoreRejectsStaleVersion(t *testing.T) {
	store := NewLocalStore[int64, testItem](testItemConfig())
	ctx := context.Background()
	if err := store.Set(ctx, testItem{ID: 1, Version: 2, Data: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, testItem{ID: 1, Version: 1, Data: "old"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Get(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.Data != "new" {
		t.Fatalf("store value = %+v ok=%v", got, ok)
	}
}

func TestLocalStoreEvictsLeastRecentlyUsedEntry(t *testing.T) {
	ctx := context.Background()
	store := NewLocalStore[int64, testItem](
		testItemConfig(),
		WithLocalMaxEntries[int64, testItem](2),
	)
	if err := store.Set(ctx, testItem{ID: 1, Version: 1, Data: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, testItem{ID: 2, Version: 1, Data: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.Get(ctx, 1); err != nil || !ok {
		t.Fatalf("Get(1) ok=%v err=%v", ok, err)
	}
	if err := store.Set(ctx, testItem{ID: 3, Version: 1, Data: "three"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.Get(ctx, 2); err != nil || ok {
		t.Fatalf("Get(2) ok=%v err=%v, want evicted", ok, err)
	}
	if got, ok, err := store.Get(ctx, 1); err != nil || !ok || got.Data != "one" {
		t.Fatalf("Get(1) = %+v ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := store.Get(ctx, 3); err != nil || !ok || got.Data != "three" {
		t.Fatalf("Get(3) = %+v ok=%v err=%v", got, ok, err)
	}
}

func TestLayeredStoreReadsThroughAfterTTL(t *testing.T) {
	ctx := context.Background()
	local := NewLocalStore[int64, testItem](testItemConfig())
	remote := NewLocalStore[int64, testItem](testItemConfig())
	store := NewLayeredStore[int64, testItem](local, remote, time.Nanosecond, testItemConfig())

	if err := remote.Set(ctx, testItem{ID: 1, Version: 1, Data: "first"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Get(ctx, 1)
	if err != nil || !ok || got.Data != "first" {
		t.Fatalf("first read = %+v ok=%v err=%v", got, ok, err)
	}
	if err := remote.Set(ctx, testItem{ID: 1, Version: 2, Data: "second"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	got, ok, err = store.Get(ctx, 1)
	if err != nil || !ok || got.Data != "second" {
		t.Fatalf("second read = %+v ok=%v err=%v", got, ok, err)
	}
}

func TestReplicaSyncerAppliesUpdate(t *testing.T) {
	bus := newFakeSyncBus()
	store := NewLocalStore[int64, testItem](testItemConfig())
	syncer := NewReplicaSyncer[int64, testItem](bus, ReplicaConfig[int64, testItem]{
		Store:       store,
		Topic:       "test",
		KeyOf:       func(item testItem) int64 { return item.ID },
		VersionOf:   func(item testItem) int64 { return item.Version },
		DeleteKeyOf: func(id int64) int64 { return id },
	})
	if err := syncer.Start(); err != nil {
		t.Fatal(err)
	}
	defer syncer.Stop()

	if err := syncer.Publish(context.Background(), testItem{ID: 7, Version: 3, Data: "payload"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Get(context.Background(), 7)
	if err != nil || !ok || got.Data != "payload" {
		t.Fatalf("replica value = %+v ok=%v err=%v", got, ok, err)
	}
}

func TestRedisRawJSONStoreUsesRedisString(t *testing.T) {
	ctx := context.Background()
	redis := newRefHMapFakeRedis()
	store := NewRedisRawJSONStore[int64, testItem](redis, time.Hour, func(id int64) string {
		return "raw:item:" + strconv.FormatInt(id, 10)
	}, testItemConfig())

	if err := store.Set(ctx, testItem{ID: 7, Version: 2, Data: "payload"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := redis.hashes["raw:item:7"]; ok {
		t.Fatalf("raw mode should not create hash data: %+v", redis.hashes["raw:item:7"])
	}
	got, ok, err := store.Get(ctx, 7)
	if err != nil || !ok || got.Data != "payload" {
		t.Fatalf("Get = %+v ok=%v err=%v", got, ok, err)
	}
	if redis.expires["raw:item:7"] != time.Hour {
		t.Fatalf("ttl = %s, want %s", redis.expires["raw:item:7"], time.Hour)
	}
}

func TestRedisRawSortedSetStoreUsesZSet(t *testing.T) {
	ctx := context.Background()
	redis := newRefHMapFakeRedis()
	store := NewRedisRawSortedSetStore[int64](redis, "raw:rank", time.Hour)

	if err := store.SetScore(ctx, 1001, 99); err != nil {
		t.Fatalf("SetScore: %v", err)
	}
	score, ok, err := store.Score(ctx, 1001)
	if err != nil || !ok || score != 99 {
		t.Fatalf("Score = %v ok=%v err=%v", score, ok, err)
	}
	if redis.expires["raw:rank"] != time.Hour {
		t.Fatalf("ttl = %s, want %s", redis.expires["raw:rank"], time.Hour)
	}
}

type fakeSyncBus struct {
	handlers map[string][]fsync.Handler
}

func newFakeSyncBus() *fakeSyncBus {
	return &fakeSyncBus{handlers: make(map[string][]fsync.Handler)}
}

func (b *fakeSyncBus) Publish(msg *fsync.SyncMsg) error {
	if msg == nil {
		return nil
	}
	for _, h := range b.handlers[msg.Topic] {
		if err := h(msg); err != nil {
			return err
		}
	}
	return nil
}

func (b *fakeSyncBus) Subscribe(topic string, handler fsync.Handler) (func(), error) {
	b.handlers[topic] = append(b.handlers[topic], handler)
	idx := len(b.handlers[topic]) - 1
	return func() {
		handlers := b.handlers[topic]
		if idx < 0 || idx >= len(handlers) {
			return
		}
		b.handlers[topic] = append(handlers[:idx], handlers[idx+1:]...)
	}, nil
}

var _ fsync.ISyncBus = (*fakeSyncBus)(nil)
