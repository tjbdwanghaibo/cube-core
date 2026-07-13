package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tjbdwanghaibo/cube-core/health"
	"github.com/tjbdwanghaibo/cube-core/obs"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

const (
	defaultRedisSnapshotWALPrefix          = "cube:checkpoint:wal"
	defaultRedisSnapshotWALShards          = 16
	defaultRedisSnapshotWALWorkerCount     = 4
	defaultRedisSnapshotWALQueueCap        = 4096
	defaultRedisSnapshotWALReplayBatchSize = 200
)

// RedisSnapshotWALConfig configures the best-effort Redis snapshot WAL.
type RedisSnapshotWALConfig struct {
	Prefix          string
	Shards          int
	WorkerCount     int
	QueueCap        int
	TTL             time.Duration
	ReplayBatchSize int
}

func (c RedisSnapshotWALConfig) normalize() RedisSnapshotWALConfig {
	c.Prefix = strings.TrimRight(strings.TrimSpace(c.Prefix), ":")
	if c.Prefix == "" {
		c.Prefix = defaultRedisSnapshotWALPrefix
	}
	if c.Shards <= 0 {
		c.Shards = defaultRedisSnapshotWALShards
	}
	if c.WorkerCount <= 0 {
		c.WorkerCount = defaultRedisSnapshotWALWorkerCount
	}
	if c.QueueCap <= 0 {
		c.QueueCap = defaultRedisSnapshotWALQueueCap
	}
	if c.ReplayBatchSize <= 0 {
		c.ReplayBatchSize = defaultRedisSnapshotWALReplayBatchSize
	}
	return c
}

type SnapshotWALStats struct {
	Submitted int64
	Written   int64
	Acked     int64
	Dropped   int64
	Errors    int64
	Replayed  int64
	Cleaned   int64
}

type redisSnapshotWALPayload struct {
	Db         string   `json:"db,omitempty"`
	Collection string   `json:"collection"`
	ID         int64    `json:"id"`
	Version    uint64   `json:"version"`
	Mask       uint64   `json:"mask,omitempty"`
	Mode       SaveMode `json:"mode"`
	Data       []byte   `json:"data"`
	CreatedAt  int64    `json:"created_at"`
}

func (p redisSnapshotWALPayload) saveOp() SaveOp {
	return SaveOp{
		Collection: p.Collection,
		ID:         p.ID,
		Version:    p.Version,
		Mask:       p.Mask,
		Mode:       SaveModeFull,
		Data:       append([]byte(nil), p.Data...),
	}
}

type redisSnapshotWALTaskKind uint8

const (
	redisSnapshotWALTaskWrite redisSnapshotWALTaskKind = iota + 1
	redisSnapshotWALTaskAck
)

func (k redisSnapshotWALTaskKind) metricValue() string {
	switch k {
	case redisSnapshotWALTaskWrite:
		return "write"
	case redisSnapshotWALTaskAck:
		return "ack"
	default:
		return "unknown"
	}
}

type redisSnapshotWALTask struct {
	kind    redisSnapshotWALTaskKind
	target  string
	shard   int
	payload redisSnapshotWALPayload
}

// RedisSnapshotWAL is a best-effort Redis-backed snapshot buffer for checkpoint.
type RedisSnapshotWAL struct {
	redis fredis.IRedis
	cfg   RedisSnapshotWALConfig

	mu      sync.RWMutex
	queues  []chan redisSnapshotWALTask
	wg      sync.WaitGroup
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc

	submitted atomic.Int64
	written   atomic.Int64
	acked     atomic.Int64
	dropped   atomic.Int64
	errs      atomic.Int64
	replayed  atomic.Int64
	cleaned   atomic.Int64
}

type RedisSnapshotWALHealthPolicy struct {
	MaxDropped int64
	MaxErrors  int64
}

func NewRedisSnapshotWAL(redis fredis.IRedis, cfg RedisSnapshotWALConfig) *RedisSnapshotWAL {
	return &RedisSnapshotWAL{
		redis: redis,
		cfg:   cfg.normalize(),
	}
}

func (w *RedisSnapshotWAL) Start() {
	if w == nil || w.redis == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running.Load() {
		return
	}
	w.queues = make([]chan redisSnapshotWALTask, w.cfg.WorkerCount)
	w.ctx, w.cancel = context.WithCancel(context.Background())
	perWorkerCap := w.cfg.QueueCap / w.cfg.WorkerCount
	if perWorkerCap <= 0 {
		perWorkerCap = 1
	}
	for i := range w.queues {
		ch := make(chan redisSnapshotWALTask, perWorkerCap)
		w.queues[i] = ch
		w.wg.Add(1)
		go w.worker(w.ctx, ch)
	}
	w.running.Store(true)
}

func (w *RedisSnapshotWAL) Stop(ctx context.Context) error {
	if w == nil || !w.running.Load() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	if !w.running.Load() {
		w.mu.Unlock()
		return nil
	}
	w.running.Store(false)
	queues := w.queues
	cancel := w.cancel
	w.queues = nil
	w.ctx = nil
	w.cancel = nil
	for _, ch := range queues {
		close(ch)
	}
	w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		if cancel != nil {
			cancel()
		}
		return err
	}

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		if cancel != nil {
			cancel()
		}
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

func (w *RedisSnapshotWAL) Submit(items []SaveItem) bool {
	if w == nil || w.redis == nil || len(items) == 0 {
		return true
	}
	ok := true
	for _, item := range items {
		task, valid := w.writeTask(item)
		if !valid {
			continue
		}
		if !w.enqueue(task) {
			ok = false
		}
	}
	return ok
}

func (w *RedisSnapshotWAL) SubmitDurable(ctx context.Context, items []SaveItem) bool {
	if w == nil || w.redis == nil || len(items) == 0 {
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, item := range items {
		task, valid := w.writeTask(item)
		if !valid {
			continue
		}
		w.submitted.Add(1)
		obs.IncCounter("checkpoint_redis_wal_submit_total", obs.Labels{"kind": task.kind.metricValue(), "result": "ok"}, 1)
		if err := w.writeSnapshot(ctx, task); err != nil {
			w.errs.Add(1)
			w.recordTaskError(task.kind)
			slog.Warn("checkpoint redis wal durable submit failed", "target", task.target, "err", err)
			return false
		}
	}
	return true
}

func (w *RedisSnapshotWAL) Ack(ctx context.Context, items []SaveItem) error {
	if w == nil || w.redis == nil || len(items) == 0 {
		return nil
	}
	tasks := make([]redisSnapshotWALTask, 0, len(items))
	for _, item := range items {
		task, valid := w.ackTask(item)
		if !valid {
			continue
		}
		tasks = append(tasks, task)
	}
	if len(tasks) == 0 {
		return nil
	}
	if w.running.Load() {
		for _, task := range tasks {
			_ = w.enqueue(task)
		}
		return nil
	}
	return w.ackTasks(ctx, tasks)
}

func (w *RedisSnapshotWAL) Replay(ctx context.Context, backend StorageBackend) error {
	if w == nil || w.redis == nil || backend == nil {
		return nil
	}
	for shard := 0; shard < w.cfg.Shards; shard++ {
		if err := w.replayShard(ctx, backend, shard); err != nil {
			return err
		}
	}
	return nil
}

func (w *RedisSnapshotWAL) Stats() SnapshotWALStats {
	if w == nil {
		return SnapshotWALStats{}
	}
	return SnapshotWALStats{
		Submitted: w.submitted.Load(),
		Written:   w.written.Load(),
		Acked:     w.acked.Load(),
		Dropped:   w.dropped.Load(),
		Errors:    w.errs.Load(),
		Replayed:  w.replayed.Load(),
		Cleaned:   w.cleaned.Load(),
	}
}

func (w *RedisSnapshotWAL) CheckHealth(policy RedisSnapshotWALHealthPolicy) health.Result {
	stats := w.Stats()
	switch {
	case stats.Errors > policy.MaxErrors:
		return health.Result{Status: health.StatusFail, Message: fmt.Sprintf("errors=%d max=%d", stats.Errors, policy.MaxErrors)}
	case stats.Dropped > policy.MaxDropped:
		return health.Result{Status: health.StatusFail, Message: fmt.Sprintf("dropped=%d max=%d", stats.Dropped, policy.MaxDropped)}
	default:
		return health.Result{Status: health.StatusOK, Message: "ok"}
	}
}

func (w *RedisSnapshotWAL) worker(ctx context.Context, ch <-chan redisSnapshotWALTask) {
	defer w.wg.Done()
	for task := range ch {
		var err error
		switch task.kind {
		case redisSnapshotWALTaskWrite:
			err = w.writeSnapshot(ctx, task)
		case redisSnapshotWALTaskAck:
			err = w.ackTasks(ctx, []redisSnapshotWALTask{task})
		}
		if err != nil {
			w.errs.Add(1)
			w.recordTaskError(task.kind)
			slog.Warn("checkpoint redis wal task failed", "kind", task.kind, "target", task.target, "err", err)
		}
	}
}

func (w *RedisSnapshotWAL) enqueue(task redisSnapshotWALTask) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.running.Load() || len(w.queues) == 0 {
		w.dropped.Add(1)
		obs.IncCounter("checkpoint_redis_wal_submit_total", obs.Labels{"kind": task.kind.metricValue(), "result": "dropped"}, 1)
		return false
	}
	idx := redisSnapshotWALWorker(task.target, len(w.queues))
	select {
	case w.queues[idx] <- task:
		w.submitted.Add(1)
		obs.IncCounter("checkpoint_redis_wal_submit_total", obs.Labels{"kind": task.kind.metricValue(), "result": "ok"}, 1)
		return true
	default:
		w.dropped.Add(1)
		obs.IncCounter("checkpoint_redis_wal_submit_total", obs.Labels{"kind": task.kind.metricValue(), "result": "dropped"}, 1)
		return false
	}
}

func (w *RedisSnapshotWAL) writeTask(item SaveItem) (redisSnapshotWALTask, bool) {
	if item.Collection == "" || item.ID == 0 || (item.Version == 0 && item.Data == nil) {
		return redisSnapshotWALTask{}, false
	}
	data := item.Data
	if item.Mode == SaveModePatch && len(item.Patch.FullData) > 0 {
		data = item.Patch.FullData
	}
	if len(data) == 0 {
		return redisSnapshotWALTask{}, false
	}
	target := redisSnapshotWALTarget(item)
	payload := redisSnapshotWALPayload{
		Db:         item.Db,
		Collection: item.Collection,
		ID:         item.ID,
		Version:    item.Version,
		Mask:       item.Mask,
		Mode:       item.Mode,
		Data:       append([]byte(nil), data...),
		CreatedAt:  time.Now().UnixNano(),
	}
	return redisSnapshotWALTask{
		kind:    redisSnapshotWALTaskWrite,
		target:  target,
		shard:   redisSnapshotWALShard(target, w.cfg.Shards),
		payload: payload,
	}, true
}

func (w *RedisSnapshotWAL) ackTask(item SaveItem) (redisSnapshotWALTask, bool) {
	if item.Collection == "" || item.ID == 0 {
		return redisSnapshotWALTask{}, false
	}
	target := redisSnapshotWALTarget(item)
	return redisSnapshotWALTask{
		kind:   redisSnapshotWALTaskAck,
		target: target,
		shard:  redisSnapshotWALShard(target, w.cfg.Shards),
	}, true
}

func (w *RedisSnapshotWAL) writeSnapshot(ctx context.Context, task redisSnapshotWALTask) error {
	raw, err := json.Marshal(task.payload)
	if err != nil {
		return err
	}
	snapshotKey := redisSnapshotWALSnapshotKey(w.cfg.Prefix, task.shard)
	pendingKey := redisSnapshotWALPendingKey(w.cfg.Prefix, task.shard)
	score := float64(task.payload.CreatedAt)
	if pipe := w.redis.Pipeline(); pipe != nil {
		pipe.HSet(ctx, snapshotKey, task.target, raw)
		pipe.ZAdd(ctx, pendingKey, fredis.Z{Score: score, Member: task.target})
		if w.cfg.TTL > 0 {
			pipe.Expire(ctx, snapshotKey, w.cfg.TTL)
			pipe.Expire(ctx, pendingKey, w.cfg.TTL)
		}
		if err := pipe.Exec(ctx); err != nil {
			return err
		}
		w.written.Add(1)
		obs.IncCounter("checkpoint_redis_wal_write_total", obs.Labels{"result": "ok"}, 1)
		return nil
	}
	if err := w.redis.HSet(ctx, snapshotKey, task.target, raw); err != nil {
		return err
	}
	if _, err := w.redis.ZAdd(ctx, pendingKey, fredis.Z{Score: score, Member: task.target}); err != nil {
		return err
	}
	if w.cfg.TTL > 0 {
		if _, err := w.redis.Expire(ctx, snapshotKey, w.cfg.TTL); err != nil {
			return err
		}
		if _, err := w.redis.Expire(ctx, pendingKey, w.cfg.TTL); err != nil {
			return err
		}
	}
	w.written.Add(1)
	obs.IncCounter("checkpoint_redis_wal_write_total", obs.Labels{"result": "ok"}, 1)
	return nil
}

func (w *RedisSnapshotWAL) ackTasks(ctx context.Context, tasks []redisSnapshotWALTask) error {
	if len(tasks) == 0 {
		return nil
	}
	grouped := make(map[int]map[string]struct{})
	for _, task := range tasks {
		if task.target == "" {
			continue
		}
		if grouped[task.shard] == nil {
			grouped[task.shard] = make(map[string]struct{})
		}
		grouped[task.shard][task.target] = struct{}{}
	}
	for shard, set := range grouped {
		targets := make([]string, 0, len(set))
		members := make([]any, 0, len(set))
		for target := range set {
			targets = append(targets, target)
			members = append(members, target)
		}
		if _, err := w.redis.HDel(ctx, redisSnapshotWALSnapshotKey(w.cfg.Prefix, shard), targets...); err != nil {
			obs.IncCounter("checkpoint_redis_wal_ack_total", obs.Labels{"result": "error"}, 1)
			return err
		}
		if _, err := w.redis.ZRem(ctx, redisSnapshotWALPendingKey(w.cfg.Prefix, shard), members...); err != nil {
			obs.IncCounter("checkpoint_redis_wal_ack_total", obs.Labels{"result": "error"}, 1)
			return err
		}
		w.acked.Add(int64(len(targets)))
		w.cleaned.Add(int64(len(targets)))
		obs.IncCounter("checkpoint_redis_wal_ack_total", obs.Labels{"result": "ok"}, int64(len(targets)))
		obs.IncCounter("checkpoint_redis_wal_clean_total", obs.Labels{"result": "ok"}, int64(len(targets)))
	}
	return nil
}

func (w *RedisSnapshotWAL) replayShard(ctx context.Context, backend StorageBackend, shard int) error {
	pendingKey := redisSnapshotWALPendingKey(w.cfg.Prefix, shard)
	snapshotKey := redisSnapshotWALSnapshotKey(w.cfg.Prefix, shard)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pending, err := w.redis.ZRangeWithScores(ctx, pendingKey, 0, int64(w.cfg.ReplayBatchSize-1))
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}
		payloads, staleTargets, err := w.loadReplayPayloads(ctx, snapshotKey, pending)
		if err != nil {
			return err
		}
		if len(staleTargets) > 0 {
			if err := w.ackTargets(ctx, shard, staleTargets); err != nil {
				return err
			}
		}
		if len(payloads) == 0 {
			continue
		}
		ops := make([]SaveOp, len(payloads))
		tasks := make([]redisSnapshotWALTask, len(payloads))
		for i, payload := range payloads {
			ops[i] = payload.saveOp()
			target := redisSnapshotWALPayloadTarget(payload)
			tasks[i] = redisSnapshotWALTask{kind: redisSnapshotWALTaskAck, shard: shard, target: target}
		}
		results, err := backend.BulkSave(ctx, ops)
		if err != nil {
			return err
		}
		if len(results) != len(ops) {
			return fmt.Errorf("checkpoint redis wal replay: result count %d, want %d", len(results), len(ops))
		}
		ackTasks := make([]redisSnapshotWALTask, 0, len(tasks))
		for i, result := range results {
			if result.OK || result.VersionConflict {
				ackTasks = append(ackTasks, tasks[i])
				w.replayed.Add(1)
				obs.IncCounter("checkpoint_redis_wal_replay_total", obs.Labels{"result": "ok"}, 1)
				continue
			}
			if result.Err != nil {
				obs.IncCounter("checkpoint_redis_wal_replay_total", obs.Labels{"result": "error"}, 1)
				return fmt.Errorf("checkpoint redis wal replay %s/%d: %w", ops[i].Collection, ops[i].ID, result.Err)
			}
			obs.IncCounter("checkpoint_redis_wal_replay_total", obs.Labels{"result": "error"}, 1)
			return fmt.Errorf("checkpoint redis wal replay %s/%d failed", ops[i].Collection, ops[i].ID)
		}
		if err := w.ackTasks(ctx, ackTasks); err != nil {
			return err
		}
	}
}

func (w *RedisSnapshotWAL) recordTaskError(kind redisSnapshotWALTaskKind) {
	switch kind {
	case redisSnapshotWALTaskWrite:
		obs.IncCounter("checkpoint_redis_wal_write_total", obs.Labels{"result": "error"}, 1)
	case redisSnapshotWALTaskAck:
		obs.IncCounter("checkpoint_redis_wal_ack_total", obs.Labels{"result": "error"}, 1)
	}
}

func (w *RedisSnapshotWAL) loadReplayPayloads(ctx context.Context, snapshotKey string, pending []fredis.Z) ([]redisSnapshotWALPayload, []string, error) {
	payloads := make([]redisSnapshotWALPayload, 0, len(pending))
	staleTargets := make([]string, 0)
	if pipe := w.redis.Pipeline(); pipe != nil {
		futures := make(map[string]*fredis.FutureBytes, len(pending))
		for _, item := range pending {
			futures[item.Member] = pipe.HGet(ctx, snapshotKey, item.Member)
		}
		if err := pipe.Exec(ctx); err != nil {
			return nil, nil, err
		}
		for _, item := range pending {
			raw, err := futures[item.Member].Result()
			if err != nil {
				if errors.Is(err, fredis.ErrNil) {
					staleTargets = append(staleTargets, item.Member)
					continue
				}
				return nil, nil, err
			}
			payload, err := decodeRedisSnapshotWALPayload(raw)
			if err != nil {
				return nil, nil, err
			}
			payloads = append(payloads, payload)
		}
		return payloads, staleTargets, nil
	}
	for _, item := range pending {
		raw, err := w.redis.HGet(ctx, snapshotKey, item.Member)
		if err != nil {
			if errors.Is(err, fredis.ErrNil) {
				staleTargets = append(staleTargets, item.Member)
				continue
			}
			return nil, nil, err
		}
		payload, err := decodeRedisSnapshotWALPayload(raw)
		if err != nil {
			return nil, nil, err
		}
		payloads = append(payloads, payload)
	}
	return payloads, staleTargets, nil
}

func (w *RedisSnapshotWAL) ackTargets(ctx context.Context, shard int, targets []string) error {
	tasks := make([]redisSnapshotWALTask, 0, len(targets))
	for _, target := range targets {
		tasks = append(tasks, redisSnapshotWALTask{kind: redisSnapshotWALTaskAck, shard: shard, target: target})
	}
	return w.ackTasks(ctx, tasks)
}

func decodeRedisSnapshotWALPayload(raw []byte) (redisSnapshotWALPayload, error) {
	var payload redisSnapshotWALPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return redisSnapshotWALPayload{}, err
	}
	if payload.Collection == "" || payload.ID == 0 || len(payload.Data) == 0 {
		return redisSnapshotWALPayload{}, fmt.Errorf("checkpoint redis wal: invalid payload collection=%q id=%d data=%d", payload.Collection, payload.ID, len(payload.Data))
	}
	return payload, nil
}

func redisSnapshotWALPayloadTarget(payload redisSnapshotWALPayload) string {
	return redisSnapshotWALTarget(SaveItem{Db: payload.Db, Collection: payload.Collection, ID: payload.ID})
}

func redisSnapshotWALTarget(item SaveItem) string {
	if item.Db != "" {
		return item.Db + "|" + item.Collection + "|" + strconv.FormatInt(item.ID, 10)
	}
	return item.Collection + "|" + strconv.FormatInt(item.ID, 10)
}

func redisSnapshotWALSnapshotKey(prefix string, shard int) string {
	return strings.TrimRight(prefix, ":") + ":{" + strconv.Itoa(shard) + "}:snapshot"
}

func redisSnapshotWALPendingKey(prefix string, shard int) string {
	return strings.TrimRight(prefix, ":") + ":{" + strconv.Itoa(shard) + "}:pending"
}

func redisSnapshotWALShard(target string, shards int) int {
	if shards <= 1 {
		return 0
	}
	return int(redisSnapshotWALHash(target) % uint32(shards))
}

func redisSnapshotWALWorker(target string, workers int) int {
	if workers <= 1 {
		return 0
	}
	return int(redisSnapshotWALHash(target) % uint32(workers))
}

func redisSnapshotWALHash(target string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(target))
	return h.Sum32()
}
