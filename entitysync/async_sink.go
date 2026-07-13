package entitysync

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/obs"
)

const (
	defaultAsyncSinkShardCount             = 16
	defaultAsyncSinkQueueCapacity          = 1024
	defaultAsyncSinkMaxBatch               = 256
	defaultAsyncSinkCriticalEnqueueTimeout = 5 * time.Millisecond
)

// AsyncSink moves downstream sync delivery out of the nest handler path.
// Packets for the same observer always go to the same worker shard, preserving
// client-visible order while allowing different observers to be delivered in
// parallel.
type AsyncSink struct {
	downstream entity.EntitySyncSink
	maxBatch   int
	criticalTO time.Duration

	mu     sync.RWMutex
	shards []asyncSinkShard
	wg     sync.WaitGroup
	closed bool
	stats  asyncSinkAtomicStats
	labels []obs.Labels
}

type AsyncSinkOptions struct {
	ShardCount             int
	QueueCapacity          int
	MaxBatch               int
	CriticalEnqueueTimeout time.Duration
}

type AsyncSinkStats struct {
	AcceptedBatches  uint64
	AcceptedPackets  uint64
	DeliveredBatches uint64
	DeliveredPackets uint64
	DroppedBatches   uint64
	DroppedPackets   uint64
	CriticalDropped  uint64
	QueueFull        uint64
}

type asyncSinkAtomicStats struct {
	acceptedBatches  atomic.Uint64
	acceptedPackets  atomic.Uint64
	deliveredBatches atomic.Uint64
	deliveredPackets atomic.Uint64
	droppedBatches   atomic.Uint64
	droppedPackets   atomic.Uint64
	criticalDropped  atomic.Uint64
	queueFull        atomic.Uint64
}

type asyncSinkShard struct {
	ch chan []entity.SyncPacket
}

func NewAsyncSink(downstream entity.EntitySyncSink, opts AsyncSinkOptions) *AsyncSink {
	shardCount := opts.ShardCount
	if shardCount <= 0 {
		shardCount = defaultAsyncSinkShardCount
	}
	queueCapacity := opts.QueueCapacity
	if queueCapacity <= 0 {
		queueCapacity = defaultAsyncSinkQueueCapacity
	}
	maxBatch := opts.MaxBatch
	if maxBatch <= 0 {
		maxBatch = defaultAsyncSinkMaxBatch
	}
	criticalTO := opts.CriticalEnqueueTimeout
	if criticalTO <= 0 {
		criticalTO = defaultAsyncSinkCriticalEnqueueTimeout
	}
	s := &AsyncSink{
		downstream: downstream,
		maxBatch:   maxBatch,
		criticalTO: criticalTO,
		shards:     make([]asyncSinkShard, shardCount),
		labels:     make([]obs.Labels, shardCount),
	}
	for i := range s.shards {
		s.shards[i].ch = make(chan []entity.SyncPacket, queueCapacity)
		s.labels[i] = obs.Labels{"shard": strconv.Itoa(i)}
		go s.runShard(i, s.shards[i].ch)
	}
	return s
}

func (s *AsyncSink) Enqueue(packet entity.SyncPacket) {
	s.EnqueueBatch([]entity.SyncPacket{packet})
}

func (s *AsyncSink) EnqueueBatch(packets []entity.SyncPacket) {
	if s == nil || len(packets) == 0 {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || len(s.shards) == 0 {
		return
	}
	byShard := make([][]entity.SyncPacket, len(s.shards))
	for _, packet := range packets {
		normalized, ref, ok := normalizeAsyncSyncPacket(packet)
		if !ok {
			continue
		}
		idx := s.shardIndexForObserver(ref)
		byShard[idx] = append(byShard[idx], normalized)
	}
	for idx, shardPackets := range byShard {
		if len(shardPackets) == 0 {
			continue
		}
		s.enqueueShardBatchesLocked(idx, shardPackets)
	}
}

func (s *AsyncSink) Drain(ctx context.Context) error {
	if s == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *AsyncSink) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		for i := range s.shards {
			close(s.shards[i].ch)
		}
	}
	s.mu.Unlock()
	return s.Drain(ctx)
}

func (s *AsyncSink) Stats() AsyncSinkStats {
	if s == nil {
		return AsyncSinkStats{}
	}
	return AsyncSinkStats{
		AcceptedBatches:  s.stats.acceptedBatches.Load(),
		AcceptedPackets:  s.stats.acceptedPackets.Load(),
		DeliveredBatches: s.stats.deliveredBatches.Load(),
		DeliveredPackets: s.stats.deliveredPackets.Load(),
		DroppedBatches:   s.stats.droppedBatches.Load(),
		DroppedPackets:   s.stats.droppedPackets.Load(),
		CriticalDropped:  s.stats.criticalDropped.Load(),
		QueueFull:        s.stats.queueFull.Load(),
	}
}

func (s *AsyncSink) enqueueShardBatchesLocked(shardIdx int, packets []entity.SyncPacket) {
	maxBatch := s.maxBatch
	if maxBatch <= 0 {
		maxBatch = len(packets)
	}
	for start := 0; start < len(packets); start += maxBatch {
		end := min(start+maxBatch, len(packets))
		s.enqueueShardBatchLocked(shardIdx, packets[start:end])
	}
}

func (s *AsyncSink) enqueueShardBatchLocked(shardIdx int, packets []entity.SyncPacket) {
	if len(packets) == 0 {
		return
	}
	batch := append([]entity.SyncPacket(nil), packets...)
	ch := s.shards[shardIdx].ch
	s.wg.Add(1)
	select {
	case ch <- batch:
		s.recordAccepted(shardIdx, batch)
		return
	default:
	}
	s.wg.Done()
	s.recordQueueFull(shardIdx)
	critical, droppable := splitAsyncOverflowPackets(batch)
	if len(droppable) > 0 {
		s.recordDropped(shardIdx, droppable, false)
	}
	if len(critical) > 0 {
		s.enqueueCriticalOverflowLocked(shardIdx, critical)
	}
}

func (s *AsyncSink) enqueueCriticalOverflowLocked(shardIdx int, packets []entity.SyncPacket) {
	batch := append([]entity.SyncPacket(nil), packets...)
	ch := s.shards[shardIdx].ch
	s.wg.Add(1)
	timer := time.NewTimer(s.criticalTO)
	defer timer.Stop()
	select {
	case ch <- batch:
		s.recordAccepted(shardIdx, batch)
	case <-timer.C:
		s.wg.Done()
		s.recordDropped(shardIdx, batch, true)
	}
}

func (s *AsyncSink) runShard(shardIdx int, ch <-chan []entity.SyncPacket) {
	for packets := range ch {
		start := time.Now()
		if s.downstream != nil && len(packets) > 0 {
			s.downstream.EnqueueBatch(packets)
		}
		s.stats.deliveredBatches.Add(1)
		s.stats.deliveredPackets.Add(uint64(len(packets)))
		obs.IncCounter("entitysync.async.delivered_batches", s.labels[shardIdx], 1)
		obs.IncCounter("entitysync.async.delivered_packets", s.labels[shardIdx], int64(len(packets)))
		obs.ObserveDuration("entitysync.async.delivery_cost", s.labels[shardIdx], time.Since(start))
		obs.SetGauge("entitysync.async.queue_len", s.labels[shardIdx], int64(len(ch)))
		s.wg.Done()
	}
}

func (s *AsyncSink) recordAccepted(shardIdx int, packets []entity.SyncPacket) {
	s.stats.acceptedBatches.Add(1)
	s.stats.acceptedPackets.Add(uint64(len(packets)))
	obs.IncCounter("entitysync.async.accepted_batches", s.labels[shardIdx], 1)
	obs.IncCounter("entitysync.async.accepted_packets", s.labels[shardIdx], int64(len(packets)))
	obs.SetGauge("entitysync.async.queue_len", s.labels[shardIdx], int64(len(s.shards[shardIdx].ch)))
}

func (s *AsyncSink) recordQueueFull(shardIdx int) {
	s.stats.queueFull.Add(1)
	obs.IncCounter("entitysync.async.queue_full_total", s.labels[shardIdx], 1)
	obs.SetGauge("entitysync.async.queue_len", s.labels[shardIdx], int64(len(s.shards[shardIdx].ch)))
}

func (s *AsyncSink) recordDropped(shardIdx int, packets []entity.SyncPacket, critical bool) {
	s.stats.droppedBatches.Add(1)
	s.stats.droppedPackets.Add(uint64(len(packets)))
	if critical {
		s.stats.criticalDropped.Add(uint64(len(packets)))
	}
	labels := obs.Labels{
		"shard":    strconv.Itoa(shardIdx),
		"critical": strconv.FormatBool(critical),
	}
	obs.IncCounter("entitysync.async.dropped_batches", labels, 1)
	obs.IncCounter("entitysync.async.dropped_packets", labels, int64(len(packets)))
}

func (s *AsyncSink) shardIndexForObserver(ref entity.SyncObserverRef) int {
	if s == nil || len(s.shards) == 0 {
		return 0
	}
	return int(hashSyncObserverRef(ref) % uint64(len(s.shards)))
}

func normalizeAsyncSyncPacket(packet entity.SyncPacket) (entity.SyncPacket, entity.SyncObserverRef, bool) {
	ref := packet.Observer.Normalize()
	if ref.Empty() && packet.ObserverID != 0 {
		ref = entity.NewPlayerSyncObserver(packet.ObserverID)
		packet.Observer = ref
	}
	if ref.Empty() {
		return entity.SyncPacket{}, entity.SyncObserverRef{}, false
	}
	if packet.Observer.Empty() {
		packet.Observer = ref
	}
	if packet.ObserverID == 0 {
		packet.ObserverID = ref.PlayerID()
	}
	return packet, ref, true
}

func splitAsyncOverflowPackets(packets []entity.SyncPacket) ([]entity.SyncPacket, []entity.SyncPacket) {
	critical := make([]entity.SyncPacket, 0)
	droppable := make([]entity.SyncPacket, 0)
	for _, packet := range packets {
		if isAsyncDroppablePacket(packet) {
			droppable = append(droppable, packet)
		} else {
			critical = append(critical, packet)
		}
	}
	return critical, droppable
}

func isAsyncDroppablePacket(packet entity.SyncPacket) bool {
	return packet.Type == entity.SyncPacketUpdate && !packet.Full && packet.Mask != entity.SyncMaskFull
}

var _ entity.EntitySyncSink = (*AsyncSink)(nil)
