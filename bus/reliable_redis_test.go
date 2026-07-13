package bus

import (
	"context"
	"testing"
	"time"

	fnats "github.com/tjbdwanghaibo/cube-core/nats"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

func TestRedisReliableStoreDeadLettersKeepNewestMaxEntries(t *testing.T) {
	redis := newReliableFakeRedis()
	store := NewRedisReliableStore(redis, ReliableConfig{
		Enabled:       true,
		Prefix:        "bus:test",
		DLQTTL:        time.Hour,
		MaxDLQEntries: 2,
	})

	for _, id := range []string{"old", "middle", "new"} {
		if err := store.DeadLetter(context.Background(), ReliableConsumer{ServiceType: "game", Sid: 1}, &fnats.NatsMsg{
			ToModule: "mail",
			MsgName:  "Changed",
			MsgID:    id,
		}, "failed"); err != nil {
			t.Fatalf("DeadLetter %q: %v", id, err)
		}
	}

	got, err := store.ListDeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed"})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(got) != 2 || got[0].MsgID != "middle" || got[1].MsgID != "new" {
		t.Fatalf("dead letters = %+v, want newest two", got)
	}
	if redis.expire["bus:test:dlq:mail:Changed"] != time.Hour {
		t.Fatalf("ttl = %v, want %v", redis.expire["bus:test:dlq:mail:Changed"], time.Hour)
	}
}

func TestRedisReliableStorePurgeDeadLettersDeletesOnlyQueryRange(t *testing.T) {
	redis := newReliableFakeRedis()
	store := NewRedisReliableStore(redis, ReliableConfig{
		Enabled:       true,
		Prefix:        "bus:test",
		DLQTTL:        time.Hour,
		MaxDLQEntries: 10,
	})

	for _, id := range []string{"dead-1", "dead-2", "dead-3"} {
		if err := store.DeadLetter(context.Background(), ReliableConsumer{ServiceType: "game", Sid: 1}, &fnats.NatsMsg{
			ToModule: "mail",
			MsgName:  "Changed",
			MsgID:    id,
		}, "failed"); err != nil {
			t.Fatalf("DeadLetter %q: %v", id, err)
		}
	}

	n, err := store.PurgeDeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed", Start: 0, Limit: 1})
	if err != nil {
		t.Fatalf("PurgeDeadLetters: %v", err)
	}
	if n != 1 {
		t.Fatalf("purged = %d, want 1", n)
	}
	got, err := store.ListDeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed"})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(got) != 2 || got[0].MsgID != "dead-2" || got[1].MsgID != "dead-3" {
		t.Fatalf("remaining dead letters = %+v, want dead-2/dead-3", got)
	}
}

func TestRedisReliableStorePurgeWholeBucketReturnsEntryCount(t *testing.T) {
	redis := newReliableFakeRedis()
	store := NewRedisReliableStore(redis, ReliableConfig{
		Enabled:       true,
		Prefix:        "bus:test",
		DLQTTL:        time.Hour,
		MaxDLQEntries: 10,
	})

	for _, id := range []string{"dead-1", "dead-2", "dead-3"} {
		if err := store.DeadLetter(context.Background(), ReliableConsumer{ServiceType: "game", Sid: 1}, &fnats.NatsMsg{
			ToModule: "mail",
			MsgName:  "Changed",
			MsgID:    id,
		}, "failed"); err != nil {
			t.Fatalf("DeadLetter %q: %v", id, err)
		}
	}

	n, err := store.PurgeDeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed"})
	if err != nil {
		t.Fatalf("PurgeDeadLetters: %v", err)
	}
	if n != 3 {
		t.Fatalf("purged = %d, want entry count 3", n)
	}
	got, err := store.ListDeadLetters(context.Background(), DeadLetterQuery{Module: "mail", MsgName: "Changed"})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("remaining dead letters = %+v, want empty", got)
	}
}

type reliableFakeRedis struct {
	values map[string][]byte
	lists  map[string][]string
	expire map[string]time.Duration
}

func newReliableFakeRedis() *reliableFakeRedis {
	return &reliableFakeRedis{
		values: make(map[string][]byte),
		lists:  make(map[string][]string),
		expire: make(map[string]time.Duration),
	}
}

func (r *reliableFakeRedis) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := r.values[key]
	if !ok {
		return nil, fredis.ErrNil
	}
	return append([]byte(nil), value...), nil
}
func (r *reliableFakeRedis) Set(_ context.Context, key string, value any, expiration time.Duration) error {
	r.values[key] = []byte("")
	r.expire[key] = expiration
	return nil
}
func (r *reliableFakeRedis) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	if _, ok := r.values[key]; ok {
		return false, nil
	}
	return true, r.Set(ctx, key, value, expiration)
}
func (r *reliableFakeRedis) Del(_ context.Context, keys ...string) (int64, error) {
	var n int64
	for _, key := range keys {
		if _, ok := r.lists[key]; ok {
			delete(r.lists, key)
			n++
		}
		if _, ok := r.values[key]; ok {
			delete(r.values, key)
			n++
		}
		delete(r.expire, key)
	}
	return n, nil
}
func (r *reliableFakeRedis) Exists(context.Context, ...string) (int64, error) { return 0, nil }
func (r *reliableFakeRedis) Expire(_ context.Context, key string, expiration time.Duration) (bool, error) {
	r.expire[key] = expiration
	return true, nil
}
func (r *reliableFakeRedis) TTL(context.Context, string) (time.Duration, error) { return 0, nil }
func (r *reliableFakeRedis) Incr(context.Context, string) (int64, error)        { return 0, nil }
func (r *reliableFakeRedis) IncrBy(context.Context, string, int64) (int64, error) {
	return 0, nil
}
func (r *reliableFakeRedis) HGet(context.Context, string, string) ([]byte, error) { return nil, nil }
func (r *reliableFakeRedis) HSet(context.Context, string, ...any) error           { return nil }
func (r *reliableFakeRedis) HGetAll(context.Context, string) (map[string]string, error) {
	return nil, nil
}
func (r *reliableFakeRedis) HDel(context.Context, string, ...string) (int64, error) { return 0, nil }
func (r *reliableFakeRedis) HExists(context.Context, string, string) (bool, error)  { return false, nil }
func (r *reliableFakeRedis) LPush(context.Context, string, ...any) (int64, error)   { return 0, nil }
func (r *reliableFakeRedis) RPush(_ context.Context, key string, values ...any) (int64, error) {
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
func (r *reliableFakeRedis) LPop(context.Context, string) ([]byte, error) { return nil, nil }
func (r *reliableFakeRedis) RPop(context.Context, string) ([]byte, error) { return nil, nil }
func (r *reliableFakeRedis) LLen(_ context.Context, key string) (int64, error) {
	return int64(len(r.lists[key])), nil
}
func (r *reliableFakeRedis) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
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
func (r *reliableFakeRedis) ZAdd(context.Context, string, ...fredis.Z) (int64, error) { return 0, nil }
func (r *reliableFakeRedis) ZRem(context.Context, string, ...any) (int64, error)      { return 0, nil }
func (r *reliableFakeRedis) ZScore(context.Context, string, string) (float64, error)  { return 0, nil }
func (r *reliableFakeRedis) ZRank(context.Context, string, string) (int64, error)     { return 0, nil }
func (r *reliableFakeRedis) ZRevRank(context.Context, string, string) (int64, error)  { return 0, nil }
func (r *reliableFakeRedis) ZRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *reliableFakeRedis) ZRevRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *reliableFakeRedis) ZCard(context.Context, string) (int64, error)        { return 0, nil }
func (r *reliableFakeRedis) SAdd(context.Context, string, ...any) (int64, error) { return 0, nil }
func (r *reliableFakeRedis) SRem(context.Context, string, ...any) (int64, error) { return 0, nil }
func (r *reliableFakeRedis) SMembers(context.Context, string) ([]string, error)  { return nil, nil }
func (r *reliableFakeRedis) SIsMember(context.Context, string, any) (bool, error) {
	return false, nil
}
func (r *reliableFakeRedis) Pipeline() fredis.IPipeline { return nil }
func (r *reliableFakeRedis) Eval(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *reliableFakeRedis) EvalSha(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *reliableFakeRedis) Publish(context.Context, string, any) error          { return nil }
func (r *reliableFakeRedis) Subscribe(context.Context, ...string) fredis.IPubSub { return nil }
func (r *reliableFakeRedis) Ping(context.Context) error                          { return nil }
func (r *reliableFakeRedis) Close() error                                        { return nil }

var _ fredis.IRedis = (*reliableFakeRedis)(nil)
