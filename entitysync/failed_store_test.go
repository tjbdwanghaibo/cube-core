package entitysync

import (
	"context"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

func TestRedisFailedBatchStoreSavesAndListsBatches(t *testing.T) {
	redis := newFailedBatchFakeRedis()
	store := NewRedisFailedBatchStore(redis, FailedBatchStoreConfig{Prefix: "sync:fail", TTL: time.Hour})
	batch := SyncBatch{
		Observer:  entity.NewPlayerSyncObserver(1001),
		SourceSid: 2001,
		Packets:   []entity.SyncPacket{{Observer: entity.NewPlayerSyncObserver(1001), Version: 7}},
	}

	if err := store.SaveFailedSyncBatch(context.Background(), batch); err != nil {
		t.Fatalf("SaveFailedSyncBatch: %v", err)
	}
	got, err := store.ListFailedSyncBatches(context.Background(), entity.NewPlayerSyncObserver(1001), 0, -1)
	if err != nil {
		t.Fatalf("ListFailedSyncBatches: %v", err)
	}
	if len(got) != 1 || got[0].SourceSid != 2001 || len(got[0].Packets) != 1 {
		t.Fatalf("batches = %+v", got)
	}
	if redis.lastKey != "sync:fail:{1:1001}" {
		t.Fatalf("key = %q, want hash-tagged observer key", redis.lastKey)
	}
}

func TestRedisFailedBatchStoreKeepsNewestMaxEntries(t *testing.T) {
	redis := newFailedBatchFakeRedis()
	store := NewRedisFailedBatchStore(redis, FailedBatchStoreConfig{Prefix: "sync:fail", TTL: time.Hour, MaxEntries: 2})

	for _, sourceSid := range []int32{1, 2, 3} {
		if err := store.SaveFailedSyncBatch(context.Background(), SyncBatch{
			Observer:  entity.NewPlayerSyncObserver(1001),
			SourceSid: sourceSid,
		}); err != nil {
			t.Fatalf("SaveFailedSyncBatch %d: %v", sourceSid, err)
		}
	}
	got, err := store.ListFailedSyncBatches(context.Background(), entity.NewPlayerSyncObserver(1001), 0, -1)
	if err != nil {
		t.Fatalf("ListFailedSyncBatches: %v", err)
	}
	if len(got) != 2 || got[0].SourceSid != 2 || got[1].SourceSid != 3 {
		t.Fatalf("batches = %+v, want newest two", got)
	}
}

func TestRedisFailedBatchStorePartialPurgeDeletesOnlySelectedRange(t *testing.T) {
	redis := newFailedBatchFakeRedis()
	store := NewRedisFailedBatchStore(redis, FailedBatchStoreConfig{Prefix: "sync:fail", TTL: time.Hour, MaxEntries: 10})
	observer := entity.NewPlayerSyncObserver(1001)
	for _, sourceSid := range []int32{1, 2, 3} {
		if err := store.SaveFailedSyncBatch(context.Background(), SyncBatch{
			Observer:  observer,
			SourceSid: sourceSid,
		}); err != nil {
			t.Fatalf("SaveFailedSyncBatch %d: %v", sourceSid, err)
		}
	}

	n, err := store.PurgeFailedSyncBatches(context.Background(), observer, 1, 1)
	if err != nil {
		t.Fatalf("PurgeFailedSyncBatches partial: %v", err)
	}
	if n != 1 {
		t.Fatalf("purged = %d, want 1", n)
	}
	got, err := store.ListFailedSyncBatches(context.Background(), observer, 0, -1)
	if err != nil {
		t.Fatalf("ListFailedSyncBatches: %v", err)
	}
	if len(got) != 2 || got[0].SourceSid != 1 || got[1].SourceSid != 3 {
		t.Fatalf("remaining batches = %+v, want source sid 1 and 3", got)
	}
}

type failedBatchFakeRedis struct {
	lists   map[string][]string
	lastKey string
}

func newFailedBatchFakeRedis() *failedBatchFakeRedis {
	return &failedBatchFakeRedis{lists: make(map[string][]string)}
}

func (r *failedBatchFakeRedis) RPush(_ context.Context, key string, values ...any) (int64, error) {
	r.lastKey = key
	for _, value := range values {
		switch typed := value.(type) {
		case []byte:
			r.lists[key] = append(r.lists[key], string(typed))
		case string:
			r.lists[key] = append(r.lists[key], typed)
		default:
			r.lists[key] = append(r.lists[key], "")
		}
	}
	return int64(len(r.lists[key])), nil
}

func (r *failedBatchFakeRedis) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
	r.lastKey = key
	items := r.lists[key]
	if stop < 0 || stop >= int64(len(items)) {
		stop = int64(len(items)) - 1
	}
	if start < 0 {
		start = 0
	}
	if start > stop || start >= int64(len(items)) {
		return nil, nil
	}
	return append([]string(nil), items[start:stop+1]...), nil
}

func (r *failedBatchFakeRedis) Expire(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (r *failedBatchFakeRedis) Get(context.Context, string) ([]byte, error) { return nil, nil }
func (r *failedBatchFakeRedis) Set(context.Context, string, any, time.Duration) error {
	return nil
}
func (r *failedBatchFakeRedis) SetNX(context.Context, string, any, time.Duration) (bool, error) {
	return false, nil
}
func (r *failedBatchFakeRedis) Del(_ context.Context, keys ...string) (int64, error) {
	var n int64
	for _, key := range keys {
		if _, ok := r.lists[key]; ok {
			delete(r.lists, key)
			n++
		}
	}
	return n, nil
}
func (r *failedBatchFakeRedis) Exists(context.Context, ...string) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) TTL(context.Context, string) (time.Duration, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) Incr(context.Context, string) (int64, error) { return 0, nil }
func (r *failedBatchFakeRedis) IncrBy(context.Context, string, int64) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) HGet(context.Context, string, string) ([]byte, error) {
	return nil, nil
}
func (r *failedBatchFakeRedis) HSet(context.Context, string, ...any) error { return nil }
func (r *failedBatchFakeRedis) HGetAll(context.Context, string) (map[string]string, error) {
	return nil, nil
}
func (r *failedBatchFakeRedis) HDel(context.Context, string, ...string) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) HExists(context.Context, string, string) (bool, error) {
	return false, nil
}
func (r *failedBatchFakeRedis) LPush(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) LPop(context.Context, string) ([]byte, error) { return nil, nil }
func (r *failedBatchFakeRedis) RPop(context.Context, string) ([]byte, error) { return nil, nil }
func (r *failedBatchFakeRedis) LLen(_ context.Context, key string) (int64, error) {
	return int64(len(r.lists[key])), nil
}
func (r *failedBatchFakeRedis) ZAdd(context.Context, string, ...fredis.Z) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) ZRem(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) ZScore(context.Context, string, string) (float64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) ZRank(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) ZRevRank(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) ZRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *failedBatchFakeRedis) ZRevRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *failedBatchFakeRedis) ZCard(context.Context, string) (int64, error) { return 0, nil }
func (r *failedBatchFakeRedis) SAdd(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) SRem(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *failedBatchFakeRedis) SMembers(context.Context, string) ([]string, error) {
	return nil, nil
}
func (r *failedBatchFakeRedis) SIsMember(context.Context, string, any) (bool, error) {
	return false, nil
}
func (r *failedBatchFakeRedis) Pipeline() fredis.IPipeline { return nil }
func (r *failedBatchFakeRedis) Eval(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *failedBatchFakeRedis) EvalSha(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *failedBatchFakeRedis) Publish(context.Context, string, any) error { return nil }
func (r *failedBatchFakeRedis) Subscribe(context.Context, ...string) fredis.IPubSub {
	return nil
}
func (r *failedBatchFakeRedis) Ping(context.Context) error { return nil }
func (r *failedBatchFakeRedis) Close() error               { return nil }

var _ fredis.IRedis = (*failedBatchFakeRedis)(nil)
