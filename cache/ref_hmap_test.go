package cache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

type refHMapSession struct {
	ID       int64           `json:"id"`
	Snapshot refHMapSnapshot `json:"snapshot"`
	Meta     *refHMapMeta    `json:"meta,omitempty"`
	Version  uint64          `json:"version"`
}

type refHMapSnapshot struct {
	State int32        `json:"state"`
	Inner refHMapInner `json:"inner"`
}

type refHMapInner struct {
	Score int64 `json:"score"`
}

type refHMapMeta struct {
	Label string `json:"label"`
}

func refHMapSessionConfig() StoreConfig[int64, refHMapSession] {
	return StoreConfig[int64, refHMapSession]{
		KeyOf:       func(v refHMapSession) int64 { return v.ID },
		Stale:       VersionStale(func(v refHMapSession) int64 { return int64(v.Version) }),
		ValidateKey: func(id int64) bool { return id > 0 },
	}
}

func TestRedisRefHMapStoreStoresNestedStructsAsSameSlotKeyRefs(t *testing.T) {
	ctx := context.Background()
	redis := newRefHMapFakeRedis()
	store := NewRedisRefHMapStore[int64, refHMapSession](redis, RefHMapConfig[int64, refHMapSession]{
		Prefix:      "cube:test",
		Name:        "session",
		TTL:         time.Hour,
		MaxDepth:    8,
		StoreConfig: refHMapSessionConfig(),
	})

	value := refHMapSession{
		ID:       1001,
		Snapshot: refHMapSnapshot{State: 3, Inner: refHMapInner{Score: 900}},
		Meta:     &refHMapMeta{Label: "open"},
		Version:  2,
	}
	if err := store.Set(ctx, value); err != nil {
		t.Fatalf("Set: %v", err)
	}

	rootKey := "cube:test:{session:1001}:root"
	snapshotKey := "cube:test:{session:1001}:snapshot"
	innerKey := "cube:test:{session:1001}:snapshot:inner"
	metaKey := "cube:test:{session:1001}:meta"
	if redis.pipelineExecs == 0 {
		t.Fatal("Set should use pipeline")
	}
	if got := redis.hashes[rootKey]["snapshot"]; got != snapshotKey {
		t.Fatalf("root snapshot ref = %q, want %q", got, snapshotKey)
	}
	if got := redis.hashes[rootKey]["meta"]; got != metaKey {
		t.Fatalf("root meta ref = %q, want %q", got, metaKey)
	}
	if got := redis.hashes[snapshotKey]["inner"]; got != innerKey {
		t.Fatalf("snapshot inner ref = %q, want %q", got, innerKey)
	}
	if got := redis.hashes[innerKey]["score"]; got != "900" {
		t.Fatalf("inner score = %q, want 900", got)
	}
	if got := redis.hashes[rootKey]["version"]; got != "2" {
		t.Fatalf("root version = %q, want 2", got)
	}

	got, ok, err := store.Get(ctx, 1001)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get returned ok=false")
	}
	if got.ID != 1001 || got.Snapshot.State != 3 || got.Snapshot.Inner.Score != 900 || got.Meta == nil || got.Meta.Label != "open" || got.Version != 2 {
		t.Fatalf("decoded value = %+v", got)
	}

	if err := store.Delete(ctx, 1001); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, key := range []string{rootKey, snapshotKey, innerKey, metaKey} {
		if _, ok := redis.hashes[key]; ok {
			t.Fatalf("Delete left key %s", key)
		}
	}
}

func TestRedisRefHMapStoreSetDeletesPreviouslyRegisteredKeys(t *testing.T) {
	ctx := context.Background()
	redis := newRefHMapFakeRedis()
	store := NewRedisRefHMapStore[int64, refHMapSession](redis, RefHMapConfig[int64, refHMapSession]{
		Prefix:      "cube:test",
		Name:        "session",
		StoreConfig: refHMapSessionConfig(),
	})

	rootKey := "cube:test:{session:1001}:root"
	oldKey := "cube:test:{session:1001}:old_schema_child"
	redis.hashes[rootKey] = map[string]string{refHMapRegistryField: rootKey + "\n" + oldKey}
	redis.hashes[oldKey] = map[string]string{"stale": "1"}

	if err := store.Set(ctx, refHMapSession{
		ID:       1001,
		Snapshot: refHMapSnapshot{State: 1},
		Version:  1,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, ok := redis.hashes[oldKey]; ok {
		t.Fatalf("Set left old registered key %s", oldKey)
	}
	if registry := redis.hashes[rootKey][refHMapRegistryField]; !strings.Contains(registry, "cube:test:{session:1001}:snapshot") {
		t.Fatalf("registry = %q, missing new snapshot key", registry)
	}
}

func TestRedisRefHMapStoreDeleteUsesRegisteredKeys(t *testing.T) {
	ctx := context.Background()
	redis := newRefHMapFakeRedis()
	store := NewRedisRefHMapStore[int64, refHMapSession](redis, RefHMapConfig[int64, refHMapSession]{
		Prefix:      "cube:test",
		Name:        "session",
		StoreConfig: refHMapSessionConfig(),
	})

	rootKey := "cube:test:{session:1001}:root"
	oldKey := "cube:test:{session:1001}:old_schema_child"
	redis.hashes[rootKey] = map[string]string{refHMapRegistryField: rootKey + "\n" + oldKey}
	redis.hashes[oldKey] = map[string]string{"stale": "1"}

	if err := store.Delete(ctx, 1001); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := redis.hashes[rootKey]; ok {
		t.Fatalf("Delete left root key %s", rootKey)
	}
	if _, ok := redis.hashes[oldKey]; ok {
		t.Fatalf("Delete left old registered key %s", oldKey)
	}
}

func TestRedisRefHMapStorePatchUpdatesOnlyTargetScalar(t *testing.T) {
	ctx := context.Background()
	redis := newRefHMapFakeRedis()
	store := NewRedisRefHMapStore[int64, refHMapSession](redis, RefHMapConfig[int64, refHMapSession]{
		Prefix:      "cube:test",
		Name:        "session",
		StoreConfig: refHMapSessionConfig(),
	})

	if err := store.Set(ctx, refHMapSession{
		ID:       1001,
		Snapshot: refHMapSnapshot{State: 3, Inner: refHMapInner{Score: 900}},
		Meta:     &refHMapMeta{Label: "open"},
		Version:  2,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	redis.resetCounters()

	if err := store.Patch(ctx, 1001, "Snapshot.Inner.Score", int64(901)); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	if redis.delCalls != 0 {
		t.Fatalf("Patch should not delete/rewrite whole object, delCalls=%d", redis.delCalls)
	}
	innerKey := "cube:test:{session:1001}:snapshot:inner"
	if got := redis.hashes[innerKey]["score"]; got != "901" {
		t.Fatalf("patched score = %q, want 901", got)
	}
	if got := redis.hashes["cube:test:{session:1001}:meta"]["label"]; got != "open" {
		t.Fatalf("patch changed unrelated meta label = %q", got)
	}
}

func TestRedisRefHMapStoreTreatsTimeAsScalar(t *testing.T) {
	type timedValue struct {
		ID int64
		At time.Time
	}
	ctx := context.Background()
	redis := newRefHMapFakeRedis()
	store := NewRedisRefHMapStore[int64, timedValue](redis, RefHMapConfig[int64, timedValue]{
		Prefix: "cube:test",
		Name:   "timed",
		StoreConfig: StoreConfig[int64, timedValue]{
			KeyOf: func(v timedValue) int64 { return v.ID },
		},
	})
	at := time.Unix(1700000000, 123456789).UTC()
	if err := store.Set(ctx, timedValue{ID: 1, At: at}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok, err := store.Get(ctx, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || !got.At.Equal(at) {
		t.Fatalf("decoded timed value = %+v ok=%v", got, ok)
	}
	if _, ok := redis.hashes["cube:test:{timed:1}:at"]; ok {
		t.Fatal("time.Time should be stored as scalar field, not child hash")
	}
}

func TestRedisRefHMapStoreRejectsCycles(t *testing.T) {
	type cyclicNode struct {
		ID   int64
		Next *cyclicNode
	}

	store := NewRedisRefHMapStore[int64, cyclicNode](newRefHMapFakeRedis(), RefHMapConfig[int64, cyclicNode]{
		Prefix: "cube:test",
		Name:   "cyclic",
		StoreConfig: StoreConfig[int64, cyclicNode]{
			KeyOf: func(v cyclicNode) int64 { return v.ID },
		},
	})
	err := store.Set(context.Background(), cyclicNode{ID: 1, Next: &cyclicNode{ID: 2}})
	if err == nil {
		t.Fatal("expected cycle error")
	}
	if !errors.Is(err, ErrRefHMapCycle) {
		t.Fatalf("error = %v, want ErrRefHMapCycle", err)
	}
}

type refHMapFakeRedis struct {
	hashes        map[string]map[string]string
	kv            map[string]string
	zsets         map[string]map[string]float64
	expires       map[string]time.Duration
	pipelineExecs int
	delCalls      int
}

func newRefHMapFakeRedis() *refHMapFakeRedis {
	return &refHMapFakeRedis{
		hashes:  make(map[string]map[string]string),
		kv:      make(map[string]string),
		zsets:   make(map[string]map[string]float64),
		expires: make(map[string]time.Duration),
	}
}

func (r *refHMapFakeRedis) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := r.kv[key]
	if !ok {
		return nil, fredis.ErrNil
	}
	return []byte(value), nil
}
func (r *refHMapFakeRedis) Set(_ context.Context, key string, value any, expiration time.Duration) error {
	r.kv[key] = toRefHMapFakeString(value)
	if expiration > 0 {
		r.expires[key] = expiration
	}
	return nil
}
func (r *refHMapFakeRedis) SetNX(context.Context, string, any, time.Duration) (bool, error) {
	return false, nil
}
func (r *refHMapFakeRedis) Del(_ context.Context, keys ...string) (int64, error) {
	r.delCalls++
	var deleted int64
	for _, key := range keys {
		if _, ok := r.kv[key]; ok {
			deleted++
			delete(r.kv, key)
		}
		if _, ok := r.hashes[key]; ok {
			deleted++
			delete(r.hashes, key)
		}
		if _, ok := r.zsets[key]; ok {
			deleted++
			delete(r.zsets, key)
		}
		delete(r.expires, key)
	}
	return deleted, nil
}
func (r *refHMapFakeRedis) Exists(_ context.Context, keys ...string) (int64, error) {
	var count int64
	for _, key := range keys {
		if _, ok := r.kv[key]; ok {
			count++
			continue
		}
		if _, ok := r.hashes[key]; ok {
			count++
			continue
		}
		if _, ok := r.zsets[key]; ok {
			count++
		}
	}
	return count, nil
}
func (r *refHMapFakeRedis) Expire(_ context.Context, key string, expiration time.Duration) (bool, error) {
	if _, ok := r.hashes[key]; !ok {
		if _, ok := r.kv[key]; !ok {
			if _, ok := r.zsets[key]; !ok {
				return false, nil
			}
		}
	}
	r.expires[key] = expiration
	return true, nil
}
func (r *refHMapFakeRedis) TTL(_ context.Context, key string) (time.Duration, error) {
	return r.expires[key], nil
}
func (r *refHMapFakeRedis) Incr(context.Context, string) (int64, error) { return 0, nil }
func (r *refHMapFakeRedis) IncrBy(context.Context, string, int64) (int64, error) {
	return 0, nil
}
func (r *refHMapFakeRedis) HGet(_ context.Context, key, field string) ([]byte, error) {
	fields := r.hashes[key]
	if fields == nil {
		return nil, fredis.ErrNil
	}
	value, ok := fields[field]
	if !ok {
		return nil, fredis.ErrNil
	}
	return []byte(value), nil
}
func (r *refHMapFakeRedis) HSet(_ context.Context, key string, values ...any) error {
	if len(values)%2 != 0 {
		return errors.New("odd HSet values")
	}
	fields := r.hashes[key]
	if fields == nil {
		fields = make(map[string]string)
		r.hashes[key] = fields
	}
	for i := 0; i < len(values); i += 2 {
		fields[toRefHMapFakeString(values[i])] = toRefHMapFakeString(values[i+1])
	}
	return nil
}
func (r *refHMapFakeRedis) HGetAll(_ context.Context, key string) (map[string]string, error) {
	return cloneRefHMapFakeHash(r.hashes[key]), nil
}
func (r *refHMapFakeRedis) HDel(_ context.Context, key string, fields ...string) (int64, error) {
	var deleted int64
	for _, field := range fields {
		if _, ok := r.hashes[key][field]; ok {
			delete(r.hashes[key], field)
			deleted++
		}
	}
	return deleted, nil
}
func (r *refHMapFakeRedis) HExists(_ context.Context, key, field string) (bool, error) {
	_, ok := r.hashes[key][field]
	return ok, nil
}
func (r *refHMapFakeRedis) LPush(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *refHMapFakeRedis) RPush(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *refHMapFakeRedis) LPop(context.Context, string) ([]byte, error) { return nil, fredis.ErrNil }
func (r *refHMapFakeRedis) RPop(context.Context, string) ([]byte, error) { return nil, fredis.ErrNil }
func (r *refHMapFakeRedis) LLen(context.Context, string) (int64, error)  { return 0, nil }
func (r *refHMapFakeRedis) LRange(context.Context, string, int64, int64) ([]string, error) {
	return nil, nil
}
func (r *refHMapFakeRedis) ZAdd(_ context.Context, key string, members ...fredis.Z) (int64, error) {
	zset := r.zsets[key]
	if zset == nil {
		zset = make(map[string]float64)
		r.zsets[key] = zset
	}
	var added int64
	for _, member := range members {
		if _, ok := zset[member.Member]; !ok {
			added++
		}
		zset[member.Member] = member.Score
	}
	return added, nil
}
func (r *refHMapFakeRedis) ZRem(_ context.Context, key string, members ...any) (int64, error) {
	zset := r.zsets[key]
	var deleted int64
	for _, member := range members {
		m := toRefHMapFakeString(member)
		if _, ok := zset[m]; ok {
			delete(zset, m)
			deleted++
		}
	}
	return deleted, nil
}
func (r *refHMapFakeRedis) ZScore(_ context.Context, key string, member string) (float64, error) {
	score, ok := r.zsets[key][member]
	if !ok {
		return 0, fredis.ErrNil
	}
	return score, nil
}
func (r *refHMapFakeRedis) ZRank(context.Context, string, string) (int64, error) {
	return 0, fredis.ErrNil
}
func (r *refHMapFakeRedis) ZRevRank(context.Context, string, string) (int64, error) {
	return 0, fredis.ErrNil
}
func (r *refHMapFakeRedis) ZRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *refHMapFakeRedis) ZRevRangeWithScores(context.Context, string, int64, int64) ([]fredis.Z, error) {
	return nil, nil
}
func (r *refHMapFakeRedis) ZCard(_ context.Context, key string) (int64, error) {
	return int64(len(r.zsets[key])), nil
}
func (r *refHMapFakeRedis) SAdd(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *refHMapFakeRedis) SRem(context.Context, string, ...any) (int64, error) {
	return 0, nil
}
func (r *refHMapFakeRedis) SMembers(context.Context, string) ([]string, error) { return nil, nil }
func (r *refHMapFakeRedis) SIsMember(context.Context, string, any) (bool, error) {
	return false, nil
}
func (r *refHMapFakeRedis) Pipeline() fredis.IPipeline {
	return &refHMapFakePipeline{redis: r}
}
func (r *refHMapFakeRedis) Eval(_ context.Context, _ string, keys []string, args ...any) (any, error) {
	if len(keys) == 0 || len(args) < 2 {
		return int64(0), nil
	}
	for _, key := range keys {
		delete(r.hashes, key)
		delete(r.expires, key)
	}
	ttl, _ := strconv.ParseInt(toRefHMapFakeString(args[0]), 10, 64)
	writeCount, _ := strconv.Atoi(toRefHMapFakeString(args[1]))
	arg := 2
	for i := 0; i < writeCount; i++ {
		keyIndex, _ := strconv.Atoi(toRefHMapFakeString(args[arg]))
		arg++
		pairCount, _ := strconv.Atoi(toRefHMapFakeString(args[arg]))
		arg++
		values := make([]any, 0, pairCount*2)
		for j := 0; j < pairCount*2; j++ {
			values = append(values, args[arg])
			arg++
		}
		if keyIndex > 0 && keyIndex <= len(keys) && len(values) > 0 {
			_ = r.HSet(context.Background(), keys[keyIndex-1], values...)
			if ttl > 0 {
				r.expires[keys[keyIndex-1]] = time.Duration(ttl) * time.Millisecond
			}
		}
	}
	return int64(1), nil
}
func (r *refHMapFakeRedis) EvalSha(context.Context, string, []string, ...any) (any, error) {
	return nil, nil
}
func (r *refHMapFakeRedis) Publish(context.Context, string, any) error { return nil }
func (r *refHMapFakeRedis) Subscribe(context.Context, ...string) fredis.IPubSub {
	return nil
}
func (r *refHMapFakeRedis) Ping(context.Context) error { return nil }
func (r *refHMapFakeRedis) Close() error               { return nil }

func (r *refHMapFakeRedis) resetCounters() {
	r.pipelineExecs = 0
	r.delCalls = 0
}

type refHMapFakePipeline struct {
	redis      *refHMapFakeRedis
	commands   []func()
	mapFutures []*refHMapFakeMapFuture
}

func (p *refHMapFakePipeline) Get(context.Context, string) *fredis.FutureBytes {
	return fredis.NewFutureBytes(nil, fredis.ErrNil)
}
func (p *refHMapFakePipeline) Set(context.Context, string, any, time.Duration) {}
func (p *refHMapFakePipeline) Del(_ context.Context, keys ...string) {
	p.commands = append(p.commands, func() { _, _ = p.redis.Del(context.Background(), keys...) })
}
func (p *refHMapFakePipeline) HSet(_ context.Context, key string, values ...any) {
	copied := append([]any(nil), values...)
	p.commands = append(p.commands, func() { _ = p.redis.HSet(context.Background(), key, copied...) })
}
func (p *refHMapFakePipeline) HGet(context.Context, string, string) *fredis.FutureBytes {
	return fredis.NewFutureBytes(nil, fredis.ErrNil)
}
func (p *refHMapFakePipeline) HGetAll(_ context.Context, key string) *fredis.FutureStringMap {
	f := &refHMapFakeMapFuture{key: key, future: &fredis.FutureStringMap{}}
	p.mapFutures = append(p.mapFutures, f)
	return f.future
}
func (p *refHMapFakePipeline) Incr(context.Context, string) *fredis.FutureInt64 {
	return fredis.NewFutureInt64(0, nil)
}
func (p *refHMapFakePipeline) Expire(_ context.Context, key string, expiration time.Duration) {
	p.commands = append(p.commands, func() { _, _ = p.redis.Expire(context.Background(), key, expiration) })
}
func (p *refHMapFakePipeline) ZAdd(_ context.Context, key string, members ...fredis.Z) {
	copied := append([]fredis.Z(nil), members...)
	p.commands = append(p.commands, func() { _, _ = p.redis.ZAdd(context.Background(), key, copied...) })
}
func (p *refHMapFakePipeline) RPush(context.Context, string, ...any) {}
func (p *refHMapFakePipeline) LPop(context.Context, string) *fredis.FutureBytes {
	return fredis.NewFutureBytes(nil, fredis.ErrNil)
}
func (p *refHMapFakePipeline) Exec(context.Context) error {
	for _, cmd := range p.commands {
		cmd()
	}
	for _, mf := range p.mapFutures {
		mf.future.SetResult(cloneRefHMapFakeHash(p.redis.hashes[mf.key]), nil)
	}
	p.redis.pipelineExecs++
	return nil
}
func (p *refHMapFakePipeline) Discard() {
	p.commands = nil
	p.mapFutures = nil
}

type refHMapFakeMapFuture struct {
	key    string
	future *fredis.FutureStringMap
}

func toRefHMapFakeString(v any) string {
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
		return strconv.FormatInt(reflectValueInt64(x), 10)
	}
}

func reflectValueInt64(v any) int64 {
	switch x := v.(type) {
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case uint:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	default:
		return 0
	}
}

func cloneRefHMapFakeHash(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ fredis.IRedis = (*refHMapFakeRedis)(nil)
