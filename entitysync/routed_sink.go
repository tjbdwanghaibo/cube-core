package entitysync

import (
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/obs"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const DefaultTopicPrefix = "entity.sync"

const defaultRoutedFailedBatchMax = 1024

type LocalSink interface {
	EnqueueLocalBatch([]entity.SyncPacket) int
}

type ObserverRoute struct {
	Observer entity.SyncObserverRef
	Sid      int32
}

type ObserverRouteResolver interface {
	ResolveSyncRoute(ctx context.Context, observer entity.SyncObserverRef) (ObserverRoute, bool, error)
}

type SyncBatch struct {
	Observer  entity.SyncObserverRef `json:"observer"`
	SourceSid int32                  `json:"source_sid"`
	CreatedAt int64                  `json:"created_at"`
	Packets   []entity.SyncPacket    `json:"packets"`
}

type FailedBatchStore interface {
	SaveFailedSyncBatch(context.Context, SyncBatch) error
}

type RoutedSinkStats struct {
	LocalBatches    int64
	RoutedBatches   int64
	PublishFailures int64
	FailedBatches   int64
}

type RoutedSink struct {
	bus              fsync.ISyncBus
	resolver         ObserverRouteResolver
	localSid         int32
	topicPrefix      string
	localSink        LocalSink
	publishRetries   int
	stats            RoutedSinkStats
	failedBatches    []SyncBatch
	maxFailedBatches int
	failedStore      FailedBatchStore
	unsub            func()
	started          bool
	mu               sync.Mutex
}

func NewRoutedSink(bus fsync.ISyncBus, resolver ObserverRouteResolver, localSid int32, localSink LocalSink, topicPrefix string) *RoutedSink {
	if topicPrefix == "" {
		topicPrefix = DefaultTopicPrefix
	}
	return &RoutedSink{
		bus:              bus,
		resolver:         resolver,
		localSid:         localSid,
		topicPrefix:      topicPrefix,
		localSink:        localSink,
		maxFailedBatches: defaultRoutedFailedBatchMax,
	}
}

func (s *RoutedSink) SetLocalSink(sink LocalSink) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.localSink = sink
	s.mu.Unlock()
}

func (s *RoutedSink) SetPublishRetries(retries int) {
	if s == nil {
		return
	}
	if retries < 0 {
		retries = 0
	}
	s.mu.Lock()
	s.publishRetries = retries
	s.mu.Unlock()
}

func (s *RoutedSink) SetFailedBatchStore(store FailedBatchStore) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.failedStore = store
	s.mu.Unlock()
}

func (s *RoutedSink) SetMaxFailedBatches(max int) {
	if s == nil {
		return
	}
	if max < 0 {
		max = 0
	}
	s.mu.Lock()
	s.maxFailedBatches = max
	if max > 0 && len(s.failedBatches) > max {
		s.failedBatches = append([]SyncBatch(nil), s.failedBatches[len(s.failedBatches)-max:]...)
	}
	if max == 0 {
		s.failedBatches = nil
	}
	s.mu.Unlock()
}

func (s *RoutedSink) Stats() RoutedSinkStats {
	if s == nil {
		return RoutedSinkStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *RoutedSink) FailedBatches() []SyncBatch {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := make([]SyncBatch, len(s.failedBatches))
	copy(ret, s.failedBatches)
	return ret
}

func (s *RoutedSink) Start() error {
	if s == nil || s.bus == nil || s.localSid == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	unsub, err := s.bus.Subscribe(s.topicForSid(s.localSid), func(msg *fsync.SyncMsg) error {
		if msg == nil || len(msg.Data) == 0 {
			return nil
		}
		var batch SyncBatch
		if err := json.Unmarshal(msg.Data, &batch); err != nil {
			return err
		}
		s.mu.Lock()
		local := s.localSink
		s.mu.Unlock()
		if local != nil {
			local.EnqueueLocalBatch(batch.Packets)
			s.recordLocalBatch()
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.unsub = unsub
	s.started = true
	return nil
}

func (s *RoutedSink) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unsub != nil {
		s.unsub()
	}
	s.unsub = nil
	s.started = false
}

func (s *RoutedSink) Enqueue(packet entity.SyncPacket) {
	s.EnqueueBatch([]entity.SyncPacket{packet})
}

func (s *RoutedSink) EnqueueBatch(packets []entity.SyncPacket) {
	if s == nil || len(packets) == 0 {
		return
	}
	groups := make(map[entity.SyncObserverRef][]entity.SyncPacket)
	for _, packet := range packets {
		ref := packet.Observer.Normalize()
		if ref.Empty() && packet.ObserverID != 0 {
			ref = entity.NewPlayerSyncObserver(packet.ObserverID)
			packet.Observer = ref
		}
		if ref.Empty() {
			continue
		}
		groups[ref] = append(groups[ref], packet)
	}
	for ref, group := range groups {
		_ = s.RouteObserverBatch(fctx.BaseContext(), ref, group)
	}
}

func (s *RoutedSink) RouteObserverBatch(ctx context.Context, observer entity.SyncObserverRef, packets []entity.SyncPacket) bool {
	observer = observer.Normalize()
	if s == nil || observer.Empty() || len(packets) == 0 {
		return false
	}
	route, ok, err := s.resolveRoute(ctx, observer)
	if err != nil || !ok || route.Sid == 0 {
		return false
	}
	if route.Sid == s.localSid {
		s.mu.Lock()
		local := s.localSink
		s.mu.Unlock()
		if local == nil {
			return false
		}
		local.EnqueueLocalBatch(packets)
		s.recordLocalBatch()
		return true
	}
	if s.bus == nil {
		return false
	}
	batch := SyncBatch{
		Observer:  observer,
		SourceSid: s.localSid,
		CreatedAt: time.Now().UnixMilli(),
		Packets:   packets,
	}
	raw, err := json.Marshal(batch)
	if err != nil {
		return false
	}
	msg := &fsync.SyncMsg{
		Topic:   s.topicForSid(route.Sid),
		Key:     observerKey(observer),
		Version: int64(maxPacketVersion(packets)),
		Data:    raw,
	}
	if err := s.publishWithRetry(msg); err != nil {
		s.recordPublishFailure(batch)
		return false
	}
	s.recordRoutedBatch()
	return true
}

func (s *RoutedSink) publishWithRetry(msg *fsync.SyncMsg) error {
	s.mu.Lock()
	retries := s.publishRetries
	bus := s.bus
	s.mu.Unlock()
	if bus == nil {
		return fmt.Errorf("entitysync: routed sink has no bus")
	}
	var err error
	for attempt := 0; attempt <= retries; attempt++ {
		err = bus.Publish(msg)
		if err == nil {
			return nil
		}
	}
	return err
}

func (s *RoutedSink) recordLocalBatch() {
	s.mu.Lock()
	s.stats.LocalBatches++
	s.mu.Unlock()
	obs.IncCounter("entitysync_routed_batch_total", obs.Labels{"route": "local"}, 1)
}

func (s *RoutedSink) recordRoutedBatch() {
	s.mu.Lock()
	s.stats.RoutedBatches++
	s.mu.Unlock()
	obs.IncCounter("entitysync_routed_batch_total", obs.Labels{"route": "remote"}, 1)
}

func (s *RoutedSink) recordPublishFailure(batch SyncBatch) {
	batch = cloneSyncBatch(batch)
	s.mu.Lock()
	s.stats.PublishFailures++
	s.stats.FailedBatches++
	if s.maxFailedBatches > 0 {
		s.failedBatches = append(s.failedBatches, batch)
		if len(s.failedBatches) > s.maxFailedBatches {
			copy(s.failedBatches, s.failedBatches[len(s.failedBatches)-s.maxFailedBatches:])
			s.failedBatches = s.failedBatches[:s.maxFailedBatches]
		}
	}
	store := s.failedStore
	s.mu.Unlock()
	obs.IncCounter("entitysync_routed_publish_failure_total", obs.Labels{
		"observer_kind": fmt.Sprintf("%d", batch.Observer.Kind),
	}, 1)
	if store != nil {
		if err := store.SaveFailedSyncBatch(context.Background(), batch); err != nil {
			obs.IncCounter("entitysync_routed_failed_batch_store_error_total", obs.Labels{
				"observer_kind": fmt.Sprintf("%d", batch.Observer.Kind),
			}, 1)
		}
	}
}

func cloneSyncBatch(batch SyncBatch) SyncBatch {
	batch.Packets = append([]entity.SyncPacket(nil), batch.Packets...)
	return batch
}

func (s *RoutedSink) resolveRoute(ctx context.Context, observer entity.SyncObserverRef) (ObserverRoute, bool, error) {
	observer = observer.Normalize()
	if observer.Kind == entity.SyncObserverServer && observer.Sid != 0 {
		return ObserverRoute{Observer: observer, Sid: observer.Sid}, true, nil
	}
	if s.resolver == nil {
		if id := observer.PlayerID(); id != 0 && s.localSink != nil {
			return ObserverRoute{Observer: observer, Sid: s.localSid}, true, nil
		}
		return ObserverRoute{}, false, nil
	}
	return s.resolver.ResolveSyncRoute(ctx, observer)
}

func (s *RoutedSink) topicForSid(sid int32) string {
	return fmt.Sprintf("%s.%d", s.topicPrefix, sid)
}

func observerKey(observer entity.SyncObserverRef) int64 {
	observer = observer.Normalize()
	if observer.ID != 0 {
		return observer.ID
	}
	return int64(observer.Sid)
}

func maxPacketVersion(packets []entity.SyncPacket) uint64 {
	var maxVersion uint64
	for _, packet := range packets {
		if packet.Version > maxVersion {
			maxVersion = packet.Version
		}
	}
	return maxVersion
}

var _ entity.EntitySyncSink = (*RoutedSink)(nil)
