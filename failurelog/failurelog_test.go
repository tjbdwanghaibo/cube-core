package failurelog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/obs"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

func TestRedisListAppendRawKeepsNewestEntries(t *testing.T) {
	redis := newFakeRedis()
	log := NewRedisList(redis, Config{MaxEntries: 2, TTL: time.Hour})

	for _, item := range []string{"old", "middle", "new"} {
		if err := log.AppendRaw(context.Background(), "failure:{x}", []byte(item)); err != nil {
			t.Fatalf("AppendRaw %q: %v", item, err)
		}
	}

	got, err := log.ListRaw(context.Background(), "failure:{x}", 0, -1)
	if err != nil {
		t.Fatalf("ListRaw: %v", err)
	}
	if len(got) != 2 || got[0] != "middle" || got[1] != "new" {
		t.Fatalf("items = %+v, want newest two", got)
	}
	if redis.expire["failure:{x}"] != time.Hour {
		t.Fatalf("ttl = %v, want %v", redis.expire["failure:{x}"], time.Hour)
	}
}

func TestRedisListListRawSupportsSingleElementRange(t *testing.T) {
	redis := newFakeRedis()
	log := NewRedisList(redis, Config{MaxEntries: 10})
	for _, item := range []string{"one", "two", "three"} {
		if err := log.AppendRaw(context.Background(), "failure:{x}", []byte(item)); err != nil {
			t.Fatalf("AppendRaw %q: %v", item, err)
		}
	}

	got, err := log.ListRaw(context.Background(), "failure:{x}", 0, 0)
	if err != nil {
		t.Fatalf("ListRaw: %v", err)
	}
	if len(got) != 1 || got[0] != "one" {
		t.Fatalf("ListRaw(0,0) = %+v, want [one]", got)
	}
}

func TestRedisListPurgeDeletesKey(t *testing.T) {
	redis := newFakeRedis()
	log := NewRedisList(redis, Config{})
	if err := log.AppendRaw(context.Background(), "failure:{x}", []byte("one")); err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	n, err := log.Purge(context.Background(), "failure:{x}")
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 1 {
		t.Fatalf("purged = %d, want 1", n)
	}
	got, err := log.ListRaw(context.Background(), "failure:{x}", 0, -1)
	if err != nil {
		t.Fatalf("ListRaw: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("items after purge = %+v", got)
	}
}

func TestRedisListPurgeReturnsEntryCount(t *testing.T) {
	redis := newFakeRedis()
	log := NewRedisList(redis, Config{})
	for _, item := range []string{"one", "two", "three"} {
		if err := log.AppendRaw(context.Background(), "failure:{x}", []byte(item)); err != nil {
			t.Fatalf("AppendRaw %q: %v", item, err)
		}
	}

	n, err := log.Purge(context.Background(), "failure:{x}")
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 3 {
		t.Fatalf("purged = %d, want entry count 3", n)
	}
	got, err := log.ListRaw(context.Background(), "failure:{x}", 0, -1)
	if err != nil {
		t.Fatalf("ListRaw: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("items after purge = %+v", got)
	}
}

func TestRedisListCountRawReturnsListLength(t *testing.T) {
	redis := newFakeRedis()
	log := NewRedisList(redis, Config{Namespace: "dlq", MaxEntries: 10})
	for _, item := range []string{"one", "two"} {
		if err := log.AppendRaw(context.Background(), "failure:{x}", []byte(item)); err != nil {
			t.Fatalf("AppendRaw %q: %v", item, err)
		}
	}

	got, err := log.CountRaw(context.Background(), "failure:{x}")
	if err != nil {
		t.Fatalf("CountRaw: %v", err)
	}
	if got != 2 {
		t.Fatalf("CountRaw = %d, want 2", got)
	}
}

func TestRedisListMetricsIncludeNamespace(t *testing.T) {
	obs.DefaultRegistry().Reset()
	redis := newFakeRedis()
	log := NewRedisList(redis, Config{Namespace: "bus_dlq", MaxEntries: 10})

	if err := log.AppendRaw(context.Background(), "failure:{x}", []byte("one")); err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	if _, err := log.DeleteRaw(context.Background(), "failure:{x}", [][]byte{[]byte("one")}); err != nil {
		t.Fatalf("DeleteRaw: %v", err)
	}

	var appendSeen, deleteSeen bool
	for _, metric := range obs.Snapshot() {
		if metric.Labels["namespace"] != "bus_dlq" {
			continue
		}
		switch metric.Name {
		case "failurelog_append_total":
			appendSeen = metric.Labels["result"] == "ok" && metric.Value == 1
		case "failurelog_delete_total":
			deleteSeen = metric.Labels["result"] == "ok" && metric.Value == 1
		}
	}
	if !appendSeen || !deleteSeen {
		t.Fatalf("namespace metrics missing append=%v delete=%v snapshot=%+v", appendSeen, deleteSeen, obs.Snapshot())
	}
}

func TestRedisListMetricsRecordAppendErrors(t *testing.T) {
	obs.DefaultRegistry().Reset()
	redis := newFakeRedis()
	redis.rpushErr = errors.New("redis unavailable")
	log := NewRedisList(redis, Config{Namespace: "entitysync_failed", MaxEntries: 10})

	if err := log.AppendRaw(context.Background(), "failure:{x}", []byte("one")); err == nil {
		t.Fatalf("AppendRaw should return redis error")
	}

	for _, metric := range obs.Snapshot() {
		if metric.Name == "failurelog_append_total" &&
			metric.Labels["namespace"] == "entitysync_failed" &&
			metric.Labels["result"] == "error" &&
			metric.Value == 1 {
			return
		}
	}
	t.Fatalf("append error metric missing: %+v", obs.Snapshot())
}

type fakeRedis struct {
	lists    map[string][]string
	expire   map[string]time.Duration
	rpushErr error
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{
		lists:  make(map[string][]string),
		expire: make(map[string]time.Duration),
	}
}

func (r *fakeRedis) Get(context.Context, string) ([]byte, error)           { return nil, fredis.ErrNil }
func (r *fakeRedis) Set(context.Context, string, any, time.Duration) error { return nil }
func (r *fakeRedis) SetNX(context.Context, string, any, time.Duration) (bool, error) {
	return false, nil
}
func (r *fakeRedis) Del(_ context.Context, keys ...string) (int64, error) {
	var n int64
	for _, key := range keys {
		if _, ok := r.lists[key]; ok {
			delete(r.lists, key)
			n++
		}
		delete(r.expire, key)
	}
	return n, nil
}
func (r *fakeRedis) Exists(context.Context, ...string) (int64, error) { return 0, nil }
func (r *fakeRedis) Expire(_ context.Context, key string, expiration time.Duration) (bool, error) {
	r.expire[key] = expiration
	return true, nil
}
func (r *fakeRedis) TTL(context.Context, string) (time.Duration, error)         { return 0, nil }
func (r *fakeRedis) Incr(context.Context, string) (int64, error)                { return 0, nil }
func (r *fakeRedis) IncrBy(context.Context, string, int64) (int64, error)       { return 0, nil }
func (r *fakeRedis) HGet(context.Context, string, string) ([]byte, error)       { return nil, nil }
func (r *fakeRedis) HSet(context.Context, string, ...any) error                 { return nil }
func (r *fakeRedis) HGetAll(context.Context, string) (map[string]string, error) { return nil, nil }
func (r *fakeRedis) HDel(context.Context, string, ...string) (int64, error)     { return 0, nil }
func (r *fakeRedis) HExists(context.Context, string, string) (bool, error)      { return false, nil }
func (r *fakeRedis) LPush(context.Context, string, ...any) (int64, error)       { return 0, nil }
func (r *fakeRedis) RPush(_ context.Context, key string, values ...any) (int64, error) {
	if r.rpushErr != nil {
		return 0, r.rpushErr
	}
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
func (r *fakeRedis) LPop(context.Context, string) ([]byte, error) { return nil, nil }
func (r *fakeRedis) RPop(context.Context, string) ([]byte, error) { return nil, nil }
func (r *fakeRedis) LLen(_ context.Context, key string) (int64, error) {
	return int64(len(r.lists[key])), nil
}
func (r *fakeRedis) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
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
func (r *fakeRedis) ZAdd(context.Context, string, ...fredis.Z) (int64, error) { return 0, nil }
func (r *fakeRedis) ZRem(context.Context, string, ...any) (int64, error)      { return 0, nil }
func (r *fakeRedis) ZScore(context.Context, string, string) (float64, error)  { return 0, nil }
func (r *fakeRedis) ZRank(context.Context, string, string) (int64, error)     { return 0, nil }
func (r *fakeRedis) ZRevRank(context.Context, string, string) (int64, error)  { return 0, nil }
func (r *fakeRedis) ZRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *fakeRedis) ZRevRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *fakeRedis) ZCard(context.Context, string) (int64, error)        { return 0, nil }
func (r *fakeRedis) SAdd(context.Context, string, ...any) (int64, error) { return 0, nil }
func (r *fakeRedis) SRem(context.Context, string, ...any) (int64, error) { return 0, nil }
func (r *fakeRedis) SMembers(context.Context, string) ([]string, error)  { return nil, nil }
func (r *fakeRedis) SIsMember(context.Context, string, any) (bool, error) {
	return false, nil
}
func (r *fakeRedis) Pipeline() fredis.IPipeline { return nil }
func (r *fakeRedis) Eval(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *fakeRedis) EvalSha(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *fakeRedis) Publish(context.Context, string, any) error          { return nil }
func (r *fakeRedis) Subscribe(context.Context, ...string) fredis.IPubSub { return nil }
func (r *fakeRedis) Ping(context.Context) error                          { return nil }
func (r *fakeRedis) Close() error                                        { return nil }

var _ fredis.IRedis = (*fakeRedis)(nil)
