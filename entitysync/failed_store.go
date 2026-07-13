package entitysync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/failurelog"
	"github.com/tjbdwanghaibo/cube-core/obs"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

const (
	defaultFailedBatchPrefix = "cube:entitysync:failed_batch"
	defaultFailedBatchTTL    = 7 * 24 * time.Hour
	defaultFailedBatchMax    = 10000
)

type FailedBatchStoreConfig struct {
	Prefix     string
	TTL        time.Duration
	MaxEntries int64
}

type RedisFailedBatchStore struct {
	redis fredis.IRedis
	cfg   FailedBatchStoreConfig
	log   *failurelog.RedisList
}

func NewRedisFailedBatchStore(redis fredis.IRedis, cfg FailedBatchStoreConfig) *RedisFailedBatchStore {
	if cfg.Prefix == "" {
		cfg.Prefix = defaultFailedBatchPrefix
	}
	cfg.Prefix = strings.TrimRight(cfg.Prefix, ":")
	if cfg.TTL <= 0 {
		cfg.TTL = defaultFailedBatchTTL
	}
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = defaultFailedBatchMax
	}
	return &RedisFailedBatchStore{
		redis: redis,
		cfg:   cfg,
		log: failurelog.NewRedisList(redis, failurelog.Config{
			Namespace:  "entitysync_failed",
			TTL:        cfg.TTL,
			MaxEntries: cfg.MaxEntries,
		}),
	}
}

func (s *RedisFailedBatchStore) SaveFailedSyncBatch(ctx context.Context, batch SyncBatch) error {
	if s == nil || s.redis == nil {
		return nil
	}
	raw, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	key := s.key(batch.Observer)
	if err := s.log.AppendRaw(ctx, key, raw); err != nil {
		return err
	}
	obs.IncCounter("entitysync_failed_batch_store_total", obs.Labels{
		"observer_kind": fmt.Sprintf("%d", batch.Observer.Normalize().Kind),
	}, 1)
	return nil
}

func (s *RedisFailedBatchStore) ListFailedSyncBatches(ctx context.Context, observer entity.SyncObserverRef, start, stop int64) ([]SyncBatch, error) {
	if s == nil || s.redis == nil {
		return nil, nil
	}
	if stop == 0 {
		stop = -1
	}
	items, err := s.log.ListRaw(ctx, s.key(observer), start, stop)
	if err != nil {
		return nil, err
	}
	out := make([]SyncBatch, 0, len(items))
	for _, item := range items {
		var batch SyncBatch
		if err := json.Unmarshal([]byte(item), &batch); err != nil {
			return nil, err
		}
		out = append(out, batch)
	}
	return out, nil
}

func (s *RedisFailedBatchStore) PurgeFailedSyncBatches(ctx context.Context, observer entity.SyncObserverRef, start, stop int64) (int64, error) {
	if s == nil || s.redis == nil {
		return 0, nil
	}
	key := s.key(observer)
	if start == 0 && (stop == 0 || stop == -1) {
		return s.log.Purge(ctx, key)
	}
	items, err := s.log.ListRaw(ctx, key, start, stop)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	raws := make([][]byte, 0, len(items))
	for _, item := range items {
		raws = append(raws, []byte(item))
	}
	return s.log.DeleteRaw(ctx, key, raws)
}

func (s *RedisFailedBatchStore) key(observer entity.SyncObserverRef) string {
	return fmt.Sprintf("%s:{%s}", s.cfg.Prefix, failedBatchObserverKey(observer))
}

func failedBatchObserverKey(observer entity.SyncObserverRef) string {
	observer = observer.Normalize()
	switch {
	case observer.ID != 0:
		return fmt.Sprintf("%d:%d", observer.Kind, observer.ID)
	case observer.Sid != 0:
		return fmt.Sprintf("%d:%d", observer.Kind, observer.Sid)
	case observer.Key != "":
		return fmt.Sprintf("%d:%s", observer.Kind, observer.Key)
	default:
		return "0"
	}
}

var _ FailedBatchStore = (*RedisFailedBatchStore)(nil)
var _ FailedBatchAdminStore = (*RedisFailedBatchStore)(nil)
