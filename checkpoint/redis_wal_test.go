package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/health"
	"github.com/tjbdwanghaibo/cube-core/obs"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

func TestRedisSnapshotWALSubmitWritesLatestSnapshot(t *testing.T) {
	ctx := context.Background()
	redis := newSnapshotWALFakeRedis()
	wal := NewRedisSnapshotWAL(redis, RedisSnapshotWALConfig{
		Prefix:      "cube:test:wal",
		Shards:      4,
		WorkerCount: 2,
		QueueCap:    16,
		TTL:         time.Hour,
	})

	wal.Start()
	if ok := wal.Submit([]SaveItem{{Db: "game_1", Collection: "players", ID: 1001, Version: 1, Mask: 1, Data: []byte("old")}}); !ok {
		t.Fatal("Submit old snapshot returned false")
	}
	if ok := wal.Submit([]SaveItem{{Db: "game_1", Collection: "players", ID: 1001, Version: 2, Mask: 2, Data: []byte("new")}}); !ok {
		t.Fatal("Submit new snapshot returned false")
	}
	if err := wal.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	target := redisSnapshotWALTarget(SaveItem{Db: "game_1", Collection: "players", ID: 1001})
	shard := redisSnapshotWALShard(target, 4)
	raw := redis.hashes[redisSnapshotWALSnapshotKey("cube:test:wal", shard)][target]
	if raw == "" {
		t.Fatal("snapshot payload was not written")
	}
	var payload redisSnapshotWALPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload.Version != 2 || string(payload.Data) != "new" || payload.Collection != "players" || payload.ID != 1001 {
		t.Fatalf("payload = %+v", payload)
	}
	if _, ok := redis.zsets[redisSnapshotWALPendingKey("cube:test:wal", shard)][target]; !ok {
		t.Fatal("pending zset member was not written")
	}
	if redis.expires[redisSnapshotWALSnapshotKey("cube:test:wal", shard)] != time.Hour {
		t.Fatal("snapshot key ttl was not set")
	}
}

func TestRedisSnapshotWALTargetIncludesDatabase(t *testing.T) {
	game1 := redisSnapshotWALTarget(SaveItem{Db: "game_1", Collection: "players", ID: 1001})
	game2 := redisSnapshotWALTarget(SaveItem{Db: "game_2", Collection: "players", ID: 1001})
	if game1 == game2 {
		t.Fatalf("wal target must include database: game1=%q game2=%q", game1, game2)
	}
	if game1 != "game_1|players|1001" {
		t.Fatalf("target = %q, want db scoped target", game1)
	}
}

func TestRedisSnapshotWALTargetSeparatesServerScopedDatabase(t *testing.T) {
	global := redisSnapshotWALTarget(SaveItem{Db: "game", Collection: "players", ID: 1001})
	server := redisSnapshotWALTarget(SaveItem{Db: "game", DbScope: DatabaseScopeServer, Collection: "players", ID: 1001})
	if global == server {
		t.Fatalf("global and server scoped WAL targets must differ: %q", global)
	}
}

func TestRedisSnapshotWALAckRunsAfterQueuedWrite(t *testing.T) {
	ctx := context.Background()
	redis := newSnapshotWALFakeRedis()
	wal := NewRedisSnapshotWAL(redis, RedisSnapshotWALConfig{
		Prefix:      "cube:test:wal",
		Shards:      2,
		WorkerCount: 1,
		QueueCap:    16,
	})

	item := SaveItem{Db: "game_1", Collection: "players", ID: 1001, Version: 1, Data: []byte("snapshot")}
	target := redisSnapshotWALTarget(item)
	shard := redisSnapshotWALShard(target, 2)

	wal.Start()
	if ok := wal.Submit([]SaveItem{item}); !ok {
		t.Fatal("Submit returned false")
	}
	if err := wal.Ack(ctx, []SaveItem{item}); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := wal.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if _, ok := redis.hashes[redisSnapshotWALSnapshotKey("cube:test:wal", shard)][target]; ok {
		t.Fatal("ack left snapshot hash field")
	}
	if _, ok := redis.zsets[redisSnapshotWALPendingKey("cube:test:wal", shard)][target]; ok {
		t.Fatal("ack left pending zset member")
	}
}

func TestRedisSnapshotWALReplayPersistsSnapshotsAndCleans(t *testing.T) {
	ctx := context.Background()
	redis := newSnapshotWALFakeRedis()
	wal := NewRedisSnapshotWAL(redis, RedisSnapshotWALConfig{
		Prefix:          "cube:test:wal",
		Shards:          2,
		WorkerCount:     1,
		QueueCap:        16,
		ReplayBatchSize: 10,
	})
	items := []SaveItem{
		{Db: "game_1", Collection: "players", ID: 1001, Version: 3, Mask: 7, Data: []byte("player")},
		{Db: "game_1", Collection: "tasks", ID: 1001, Version: 4, Mode: SaveModePatch, Patch: PersistPatch{FullData: []byte("task-full")}},
	}
	wal.Start()
	if ok := wal.Submit(items); !ok {
		t.Fatal("Submit returned false")
	}
	if err := wal.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	backend := &mockBackend{}
	if err := wal.Replay(ctx, backend); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	saved := backend.getSaved()
	sort.Slice(saved, func(i, j int) bool { return saved[i].Collection < saved[j].Collection })
	if len(saved) != 2 {
		t.Fatalf("saved count = %d, want 2", len(saved))
	}
	if saved[0].Collection != "players" || saved[0].Version != 3 || saved[0].Mode != SaveModeFull || string(saved[0].Data) != "player" {
		t.Fatalf("player save op = %+v", saved[0])
	}
	if saved[1].Collection != "tasks" || saved[1].Version != 4 || saved[1].Mode != SaveModeFull || string(saved[1].Data) != "task-full" {
		t.Fatalf("task save op = %+v", saved[1])
	}
	for _, item := range items {
		target := redisSnapshotWALTarget(item)
		shard := redisSnapshotWALShard(target, 2)
		if _, ok := redis.hashes[redisSnapshotWALSnapshotKey("cube:test:wal", shard)][target]; ok {
			t.Fatalf("replay left snapshot for %s", target)
		}
		if _, ok := redis.zsets[redisSnapshotWALPendingKey("cube:test:wal", shard)][target]; ok {
			t.Fatalf("replay left pending member for %s", target)
		}
	}
}

func TestRedisSnapshotWALSubmitDurableWritesImmediatelyWithoutWorker(t *testing.T) {
	ctx := context.Background()
	redis := newSnapshotWALFakeRedis()
	wal := NewRedisSnapshotWAL(redis, RedisSnapshotWALConfig{
		Prefix: "cube:test:wal",
		Shards: 2,
		TTL:    time.Hour,
	})
	item := SaveItem{Db: "game_1", Collection: "players", ID: 1001, Version: 1, Data: []byte("snapshot")}

	if ok := wal.SubmitDurable(ctx, []SaveItem{item}); !ok {
		t.Fatal("SubmitDurable returned false")
	}
	target := redisSnapshotWALTarget(item)
	shard := redisSnapshotWALShard(target, 2)
	if raw := redis.hashes[redisSnapshotWALSnapshotKey("cube:test:wal", shard)][target]; raw == "" {
		t.Fatal("durable submit did not write snapshot hash")
	}
	if _, ok := redis.zsets[redisSnapshotWALPendingKey("cube:test:wal", shard)][target]; !ok {
		t.Fatal("durable submit did not write pending zset")
	}
	if stats := wal.Stats(); stats.Written != 1 || stats.Submitted != 1 {
		t.Fatalf("wal stats = %+v, want submitted=1 written=1", stats)
	}
}

func TestRedisSnapshotWALRecordsObsMetrics(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })

	ctx := context.Background()
	redis := newSnapshotWALFakeRedis()
	wal := NewRedisSnapshotWAL(redis, RedisSnapshotWALConfig{
		Prefix: "cube:test:wal",
		Shards: 2,
	})
	item := SaveItem{Db: "game_1", Collection: "players", ID: 1001, Version: 1, Data: []byte("snapshot")}

	if ok := wal.SubmitDurable(ctx, []SaveItem{item}); !ok {
		t.Fatal("SubmitDurable returned false")
	}
	if err := wal.Ack(ctx, []SaveItem{item}); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	assertMetricValue(t, "checkpoint_redis_wal_submit_total", obs.Labels{"kind": "write", "result": "ok"}, 1)
	assertMetricValue(t, "checkpoint_redis_wal_write_total", obs.Labels{"result": "ok"}, 1)
	assertMetricValue(t, "checkpoint_redis_wal_ack_total", obs.Labels{"result": "ok"}, 1)
}

func TestRedisSnapshotWALHealthFailsWhenDropsExceedLimit(t *testing.T) {
	redis := newSnapshotWALFakeRedis()
	wal := NewRedisSnapshotWAL(redis, RedisSnapshotWALConfig{
		Prefix: "cube:test:wal",
		Shards: 1,
	})

	ok := wal.Submit([]SaveItem{{Db: "game_1", Collection: "players", ID: 1001, Version: 1, Data: []byte("snapshot")}})
	if ok {
		t.Fatal("Submit should return false when async wal is not running")
	}
	result := wal.CheckHealth(RedisSnapshotWALHealthPolicy{MaxDropped: 0, MaxErrors: 0})
	if result.Status != health.StatusFail {
		t.Fatalf("health status = %s, want fail result=%+v", result.Status, result)
	}
}

func TestRedisSnapshotWALStopCancelsInFlightWorkerOperation(t *testing.T) {
	redis := newSnapshotWALFakeRedis()
	redis.blockExec = make(chan struct{})
	redis.execStarted = make(chan struct{})
	redis.execCtxDone = make(chan struct{})
	wal := NewRedisSnapshotWAL(redis, RedisSnapshotWALConfig{
		Prefix:      "cube:test:wal",
		Shards:      1,
		WorkerCount: 1,
		QueueCap:    1,
	})
	wal.Start()
	if ok := wal.Submit([]SaveItem{{Db: "game_1", Collection: "players", ID: 1001, Version: 1, Data: []byte("snapshot")}}); !ok {
		t.Fatal("Submit returned false")
	}
	<-redis.execStarted

	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := wal.Stop(stopCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop err = %v, want context canceled", err)
	}
	<-redis.execCtxDone
}

func assertMetricValue(t *testing.T, name string, labels obs.Labels, value int64) {
	t.Helper()
	for _, metric := range obs.Snapshot() {
		if metric.Name != name || metric.Value != value {
			continue
		}
		match := true
		for key, want := range labels {
			if metric.Labels[key] != want {
				match = false
				break
			}
		}
		if match {
			return
		}
	}
	t.Fatalf("metric %s labels=%+v value=%d missing in %+v", name, labels, value, obs.Snapshot())
}

type snapshotWALFakeRedis struct {
	mu      sync.Mutex
	hashes  map[string]map[string]string
	zsets   map[string]map[string]float64
	expires map[string]time.Duration

	blockExec   chan struct{}
	execStarted chan struct{}
	execCtxDone chan struct{}
}

func newSnapshotWALFakeRedis() *snapshotWALFakeRedis {
	return &snapshotWALFakeRedis{
		hashes:  make(map[string]map[string]string),
		zsets:   make(map[string]map[string]float64),
		expires: make(map[string]time.Duration),
	}
}

func (r *snapshotWALFakeRedis) Get(context.Context, string) ([]byte, error) {
	return nil, fredis.ErrNil
}
func (r *snapshotWALFakeRedis) Set(context.Context, string, any, time.Duration) error {
	return nil
}
func (r *snapshotWALFakeRedis) SetNX(context.Context, string, any, time.Duration) (bool, error) {
	return false, nil
}
func (r *snapshotWALFakeRedis) Del(_ context.Context, keys ...string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, key := range keys {
		if _, ok := r.hashes[key]; ok {
			delete(r.hashes, key)
			n++
		}
		if _, ok := r.zsets[key]; ok {
			delete(r.zsets, key)
			n++
		}
		delete(r.expires, key)
	}
	return n, nil
}
func (r *snapshotWALFakeRedis) Exists(_ context.Context, keys ...string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, key := range keys {
		if _, ok := r.hashes[key]; ok {
			n++
			continue
		}
		if _, ok := r.zsets[key]; ok {
			n++
		}
	}
	return n, nil
}
func (r *snapshotWALFakeRedis) Expire(_ context.Context, key string, expiration time.Duration) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.hashes[key]; ok {
		r.expires[key] = expiration
		return true, nil
	}
	if _, ok := r.zsets[key]; ok {
		r.expires[key] = expiration
		return true, nil
	}
	return false, nil
}
func (r *snapshotWALFakeRedis) TTL(_ context.Context, key string) (time.Duration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.expires[key], nil
}
func (r *snapshotWALFakeRedis) Incr(context.Context, string) (int64, error) { return 0, nil }
func (r *snapshotWALFakeRedis) IncrBy(context.Context, string, int64) (int64, error) {
	return 0, nil
}
func (r *snapshotWALFakeRedis) HGet(_ context.Context, key, field string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hashes[key] == nil {
		return nil, fredis.ErrNil
	}
	value, ok := r.hashes[key][field]
	if !ok {
		return nil, fredis.ErrNil
	}
	return []byte(value), nil
}
func (r *snapshotWALFakeRedis) HSet(_ context.Context, key string, values ...any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(values)%2 != 0 {
		return errors.New("odd HSet values")
	}
	fields := r.hashes[key]
	if fields == nil {
		fields = make(map[string]string)
		r.hashes[key] = fields
	}
	for i := 0; i < len(values); i += 2 {
		fields[fmt.Sprint(values[i])] = snapshotWALFakeString(values[i+1])
	}
	return nil
}
func (r *snapshotWALFakeRedis) HGetAll(_ context.Context, key string) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]string, len(r.hashes[key]))
	for k, v := range r.hashes[key] {
		out[k] = v
	}
	return out, nil
}
func (r *snapshotWALFakeRedis) HDel(_ context.Context, key string, fields ...string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, field := range fields {
		if _, ok := r.hashes[key][field]; ok {
			delete(r.hashes[key], field)
			n++
		}
	}
	return n, nil
}
func (r *snapshotWALFakeRedis) HExists(_ context.Context, key, field string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.hashes[key][field]
	return ok, nil
}
func (r *snapshotWALFakeRedis) LPush(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *snapshotWALFakeRedis) RPush(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *snapshotWALFakeRedis) LPop(context.Context, string) ([]byte, error) {
	return nil, fredis.ErrNil
}
func (r *snapshotWALFakeRedis) RPop(context.Context, string) ([]byte, error) {
	return nil, fredis.ErrNil
}
func (r *snapshotWALFakeRedis) LLen(context.Context, string) (int64, error) { return 0, nil }
func (r *snapshotWALFakeRedis) LRange(context.Context, string, int64, int64) ([]string, error) {
	return nil, nil
}
func (r *snapshotWALFakeRedis) ZAdd(_ context.Context, key string, members ...fredis.Z) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	z := r.zsets[key]
	if z == nil {
		z = make(map[string]float64)
		r.zsets[key] = z
	}
	var added int64
	for _, member := range members {
		if _, ok := z[member.Member]; !ok {
			added++
		}
		z[member.Member] = member.Score
	}
	return added, nil
}
func (r *snapshotWALFakeRedis) ZRem(_ context.Context, key string, members ...any) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, member := range members {
		m := fmt.Sprint(member)
		if _, ok := r.zsets[key][m]; ok {
			delete(r.zsets[key], m)
			n++
		}
	}
	return n, nil
}
func (r *snapshotWALFakeRedis) ZScore(context.Context, string, string) (float64, error) {
	return 0, fredis.ErrNil
}
func (r *snapshotWALFakeRedis) ZRank(context.Context, string, string) (int64, error) {
	return 0, fredis.ErrNil
}
func (r *snapshotWALFakeRedis) ZRevRank(context.Context, string, string) (int64, error) {
	return 0, fredis.ErrNil
}
func (r *snapshotWALFakeRedis) ZRangeWithScores(_ context.Context, key string, start, stop int64) ([]fredis.Z, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	members := make([]fredis.Z, 0, len(r.zsets[key]))
	for member, score := range r.zsets[key] {
		members = append(members, fredis.Z{Member: member, Score: score})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score == members[j].Score {
			return members[i].Member < members[j].Member
		}
		return members[i].Score < members[j].Score
	})
	if len(members) == 0 || start >= int64(len(members)) {
		return nil, nil
	}
	if stop < 0 || stop >= int64(len(members)) {
		stop = int64(len(members)) - 1
	}
	return append([]fredis.Z(nil), members[start:stop+1]...), nil
}
func (r *snapshotWALFakeRedis) ZRevRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *snapshotWALFakeRedis) ZCard(_ context.Context, key string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int64(len(r.zsets[key])), nil
}
func (r *snapshotWALFakeRedis) SAdd(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *snapshotWALFakeRedis) SRem(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *snapshotWALFakeRedis) SMembers(context.Context, string) ([]string, error) {
	return nil, nil
}
func (r *snapshotWALFakeRedis) SIsMember(context.Context, string, any) (bool, error) {
	return false, nil
}
func (r *snapshotWALFakeRedis) Pipeline() fredis.IPipeline {
	return &snapshotWALFakePipeline{redis: r, blockExec: r.blockExec, execStarted: r.execStarted, execCtxDone: r.execCtxDone}
}
func (r *snapshotWALFakeRedis) Eval(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *snapshotWALFakeRedis) EvalSha(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *snapshotWALFakeRedis) Publish(context.Context, string, any) error { return nil }
func (r *snapshotWALFakeRedis) Subscribe(context.Context, ...string) fredis.IPubSub {
	return nil
}
func (r *snapshotWALFakeRedis) Ping(context.Context) error { return nil }
func (r *snapshotWALFakeRedis) Close() error               { return nil }

type snapshotWALFakePipeline struct {
	redis       *snapshotWALFakeRedis
	commands    []func()
	bytes       []*snapshotWALFakeBytesFuture
	blockExec   chan struct{}
	execStarted chan struct{}
	execCtxDone chan struct{}
}

func (p *snapshotWALFakePipeline) Get(context.Context, string) *fredis.FutureBytes {
	return fredis.NewFutureBytes(nil, fredis.ErrNil)
}
func (p *snapshotWALFakePipeline) Set(context.Context, string, any, time.Duration) {}
func (p *snapshotWALFakePipeline) Del(_ context.Context, keys ...string) {
	p.commands = append(p.commands, func() { _, _ = p.redis.Del(context.Background(), keys...) })
}
func (p *snapshotWALFakePipeline) HSet(_ context.Context, key string, values ...any) {
	copied := append([]any(nil), values...)
	p.commands = append(p.commands, func() { _ = p.redis.HSet(context.Background(), key, copied...) })
}
func (p *snapshotWALFakePipeline) HGet(_ context.Context, key, field string) *fredis.FutureBytes {
	future := &snapshotWALFakeBytesFuture{key: key, field: field, future: &fredis.FutureBytes{}}
	p.bytes = append(p.bytes, future)
	return future.future
}
func (p *snapshotWALFakePipeline) HGetAll(context.Context, string) *fredis.FutureStringMap {
	return fredis.NewFutureStringMap(nil, nil)
}
func (p *snapshotWALFakePipeline) Incr(context.Context, string) *fredis.FutureInt64 {
	return fredis.NewFutureInt64(0, nil)
}
func (p *snapshotWALFakePipeline) Expire(_ context.Context, key string, expiration time.Duration) {
	p.commands = append(p.commands, func() { _, _ = p.redis.Expire(context.Background(), key, expiration) })
}
func (p *snapshotWALFakePipeline) ZAdd(_ context.Context, key string, members ...fredis.Z) {
	copied := append([]fredis.Z(nil), members...)
	p.commands = append(p.commands, func() { _, _ = p.redis.ZAdd(context.Background(), key, copied...) })
}
func (p *snapshotWALFakePipeline) RPush(context.Context, string, ...any) {}
func (p *snapshotWALFakePipeline) LPop(context.Context, string) *fredis.FutureBytes {
	return fredis.NewFutureBytes(nil, fredis.ErrNil)
}
func (p *snapshotWALFakePipeline) Exec(ctx context.Context) error {
	if p.blockExec != nil {
		if p.execStarted != nil {
			close(p.execStarted)
		}
		if p.execCtxDone != nil {
			defer close(p.execCtxDone)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.blockExec:
		}
	}
	for _, cmd := range p.commands {
		cmd()
	}
	for _, f := range p.bytes {
		raw, err := p.redis.HGet(context.Background(), f.key, f.field)
		f.future.SetResult(raw, err)
	}
	return nil
}
func (p *snapshotWALFakePipeline) Discard() {
	p.commands = nil
	p.bytes = nil
}

type snapshotWALFakeBytesFuture struct {
	key    string
	field  string
	future *fredis.FutureBytes
}

func snapshotWALFakeString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	default:
		return fmt.Sprint(x)
	}
}

var _ fredis.IRedis = (*snapshotWALFakeRedis)(nil)
