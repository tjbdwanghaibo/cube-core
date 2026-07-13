package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tjbdwanghaibo/cube-core/failurelog"
	"github.com/tjbdwanghaibo/cube-core/nats"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

const (
	defaultReliablePrefix = "cube:bus"
	defaultInboxTTL       = 24 * time.Hour
	defaultDLQTTL         = 7 * 24 * time.Hour
	defaultDLQMaxEntries  = 10000
)

// ReliableConfig controls message idempotency and dead-letter recording for
// asynchronous bus messages. RPC is intentionally excluded because callers
// already own request retry and timeout semantics.
type ReliableConfig struct {
	Enabled       bool
	Prefix        string
	InboxTTL      time.Duration
	DLQTTL        time.Duration
	MaxDLQEntries int64
}

func (c ReliableConfig) normalize() ReliableConfig {
	if c.Prefix == "" {
		c.Prefix = defaultReliablePrefix
	}
	c.Prefix = strings.TrimRight(c.Prefix, ":")
	if c.InboxTTL <= 0 {
		c.InboxTTL = defaultInboxTTL
	}
	if c.DLQTTL <= 0 {
		c.DLQTTL = defaultDLQTTL
	}
	if c.MaxDLQEntries == 0 {
		c.MaxDLQEntries = defaultDLQMaxEntries
	}
	return c
}

// ReliableStore is the durable side channel used by Bus to deduplicate
// messages and keep failed deliveries inspectable.
type ReliableStore interface {
	BeginConsume(ctx context.Context, consumer ReliableConsumer, msg *nats.NatsMsg) (bool, error)
	FinishConsume(ctx context.Context, consumer ReliableConsumer, msg *nats.NatsMsg) error
	DeadLetter(ctx context.Context, consumer ReliableConsumer, msg *nats.NatsMsg, reason string) error
}

type ReliableDeadLetterStore interface {
	ListDeadLetters(ctx context.Context, query DeadLetterQuery) ([]DeadLetterEntry, error)
	PurgeDeadLetters(ctx context.Context, query DeadLetterQuery) (int64, error)
}

type ReliableDeadLetterEntryDeleter interface {
	DeleteDeadLetters(ctx context.Context, query DeadLetterQuery, entries []DeadLetterEntry) (int64, error)
}

type ReliableConsumer struct {
	ServiceType string
	Sid         int32
}

func (c ReliableConsumer) Key() string {
	serviceType := c.ServiceType
	if serviceType == "" {
		serviceType = "_"
	}
	return fmt.Sprintf("%s:%d", serviceType, c.Sid)
}

type RedisReliableStore struct {
	redis fredis.IRedis
	cfg   ReliableConfig
	dlq   *failurelog.RedisList
}

func NewRedisReliableStore(redis fredis.IRedis, cfg ReliableConfig) *RedisReliableStore {
	cfg = cfg.normalize()
	return &RedisReliableStore{
		redis: redis,
		cfg:   cfg,
		dlq: failurelog.NewRedisList(redis, failurelog.Config{
			Namespace:  "bus_dlq",
			TTL:        cfg.DLQTTL,
			MaxEntries: cfg.MaxDLQEntries,
		}),
	}
}

func (s *RedisReliableStore) BeginConsume(ctx context.Context, consumer ReliableConsumer, msg *nats.NatsMsg) (bool, error) {
	if s == nil || s.redis == nil || msg == nil || msg.MsgID == "" {
		return true, nil
	}
	ok, err := s.redis.SetNX(ctx, s.inboxKey(consumer, msg.MsgID), "processing", s.cfg.InboxTTL)
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (s *RedisReliableStore) FinishConsume(ctx context.Context, consumer ReliableConsumer, msg *nats.NatsMsg) error {
	if s == nil || s.redis == nil || msg == nil || msg.MsgID == "" {
		return nil
	}
	return s.redis.Set(ctx, s.inboxKey(consumer, msg.MsgID), "done", s.cfg.InboxTTL)
}

func (s *RedisReliableStore) DeadLetter(ctx context.Context, consumer ReliableConsumer, msg *nats.NatsMsg, reason string) error {
	if s == nil || s.redis == nil || msg == nil {
		return nil
	}
	raw, err := json.Marshal(DeadLetterEntry{
		MsgID:     msg.MsgID,
		FromSid:   msg.FromSid,
		ToSid:     msg.ToSid,
		ToModule:  msg.ToModule,
		MsgName:   msg.MsgName,
		Broadcast: int32(msg.Broadcast),
		Attempt:   msg.Attempt,
		Consumer:  consumer.Key(),
		CreatedAt: msg.CreatedAt,
		Reason:    reason,
		Payload:   msg.Payload,
		FailedAt:  time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	key := s.deadLetterKey(msg.ToModule, msg.MsgName)
	if err := s.dlq.AppendRaw(ctx, key, raw); err != nil {
		return err
	}
	if msg.MsgID != "" {
		_ = s.redis.Set(ctx, s.inboxKey(consumer, msg.MsgID), "failed", s.cfg.InboxTTL)
	}
	return nil
}

func (s *RedisReliableStore) ListDeadLetters(ctx context.Context, query DeadLetterQuery) ([]DeadLetterEntry, error) {
	if s == nil || s.redis == nil {
		return nil, nil
	}
	query = query.normalize()
	items, err := s.dlq.ListRaw(ctx, s.deadLetterKey(query.Module, query.MsgName), query.Start, query.Stop)
	if err != nil {
		return nil, err
	}
	ret := make([]DeadLetterEntry, 0, len(items))
	for _, item := range items {
		var entry DeadLetterEntry
		if err := json.Unmarshal([]byte(item), &entry); err != nil {
			return nil, err
		}
		ret = append(ret, entry)
	}
	return ret, nil
}

func (s *RedisReliableStore) PurgeDeadLetters(ctx context.Context, query DeadLetterQuery) (int64, error) {
	if s == nil || s.redis == nil {
		return 0, nil
	}
	query = query.normalize()
	if query.isWholeBucket() {
		return s.dlq.Purge(ctx, s.deadLetterKey(query.Module, query.MsgName))
	}
	entries, err := s.ListDeadLetters(ctx, query)
	if err != nil {
		return 0, err
	}
	return s.DeleteDeadLetters(ctx, query, entries)
}

func (s *RedisReliableStore) DeleteDeadLetters(ctx context.Context, query DeadLetterQuery, entries []DeadLetterEntry) (int64, error) {
	if s == nil || s.redis == nil || len(entries) == 0 {
		return 0, nil
	}
	query = query.normalize()
	raws := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		raw, err := json.Marshal(entry)
		if err != nil {
			return 0, err
		}
		raws = append(raws, raw)
	}
	return s.dlq.DeleteRaw(ctx, s.deadLetterKey(query.Module, query.MsgName), raws)
}

func (s *RedisReliableStore) inboxKey(consumer ReliableConsumer, msgID string) string {
	return fmt.Sprintf("%s:inbox:%s:%s", s.cfg.Prefix, consumer.Key(), msgID)
}

func (s *RedisReliableStore) deadLetterKey(module, msgName string) string {
	return fmt.Sprintf("%s:dlq:%s", s.cfg.Prefix, DeadLetterKey(module, msgName))
}

func DeadLetterKey(module, msgName string) string {
	if module == "" {
		module = "_"
	}
	if msgName == "" {
		msgName = "_"
	}
	return fmt.Sprintf("%s:%s", module, msgName)
}

type DeadLetterQuery struct {
	Module  string
	MsgName string
	Start   int64
	Stop    int64
	Limit   int64
}

func (q DeadLetterQuery) normalize() DeadLetterQuery {
	if q.Start < 0 {
		q.Start = 0
	}
	if q.Limit > 0 {
		q.Stop = q.Start + q.Limit - 1
	} else if q.Stop == 0 {
		q.Stop = -1
	}
	return q
}

func (q DeadLetterQuery) isWholeBucket() bool {
	return q.Start == 0 && q.Limit <= 0 && q.Stop == -1
}

type DeadLetterEntry struct {
	MsgID     string `json:"msg_id,omitempty"`
	FromSid   int32  `json:"from_sid"`
	ToSid     int32  `json:"to_sid"`
	ToModule  string `json:"to_module"`
	MsgName   string `json:"msg_name"`
	Broadcast int32  `json:"broadcast"`
	Attempt   int32  `json:"attempt"`
	Consumer  string `json:"consumer,omitempty"`
	CreatedAt int64  `json:"created_at"`
	Reason    string `json:"reason"`
	Payload   []byte `json:"payload,omitempty"`
	FailedAt  int64  `json:"failed_at"`
}

func (e DeadLetterEntry) toNatsMsg(newMsgID string) *nats.NatsMsg {
	attempt := e.Attempt + 1
	if attempt <= 0 {
		attempt = 1
	}
	return &nats.NatsMsg{
		FromSid:   e.FromSid,
		ToSid:     e.ToSid,
		ToModule:  e.ToModule,
		MsgName:   e.MsgName,
		Payload:   append([]byte(nil), e.Payload...),
		Broadcast: nats.BroadcastType(e.Broadcast),
		MsgID:     newMsgID,
		Attempt:   attempt,
		CreatedAt: time.Now().UnixMilli(),
	}
}
