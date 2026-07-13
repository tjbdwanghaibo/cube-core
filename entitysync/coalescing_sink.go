package entitysync

import (
	"sort"
	"sync"

	"github.com/tjbdwanghaibo/cube-core/entity"
	fmap "github.com/tjbdwanghaibo/cube-core/map"
)

// CoalescingSink batches entity sync packets per observer and collapses
// repeated updates for the same entity before forwarding to a downstream sink.
type CoalescingSink struct {
	opMu       sync.RWMutex
	configMu   sync.RWMutex
	downstream entity.EntitySyncSink
	maxBatch   int
	shards     []coalescingSinkShard
}

type coalescingSinkShard struct {
	mu     sync.Mutex
	queues map[entity.SyncObserverRef]*coalescedObserverQueue
}

type coalescedObserverQueue struct {
	enters           []entity.SyncPacket
	leaves           []entity.SyncPacket
	customs          []entity.SyncPacket
	updates          map[int64][]entity.SyncPacket
	afterFullUpdates map[int64][]entity.SyncPacket
}

type coalescedObserverPackets struct {
	ref     entity.SyncObserverRef
	packets []entity.SyncPacket
}

const defaultCoalescingSinkShardCount = 64

func NewCoalescingSink(downstream entity.EntitySyncSink) *CoalescingSink {
	return newCoalescingSinkWithShardCount(downstream, defaultCoalescingSinkShardCount)
}

func newCoalescingSinkWithShardCount(downstream entity.EntitySyncSink, shardCount int) *CoalescingSink {
	if shardCount <= 0 {
		shardCount = defaultCoalescingSinkShardCount
	}
	shards := make([]coalescingSinkShard, shardCount)
	for i := range shards {
		shards[i].queues = make(map[entity.SyncObserverRef]*coalescedObserverQueue)
	}
	return &CoalescingSink{
		downstream: downstream,
		maxBatch:   256,
		shards:     shards,
	}
}

func (s *CoalescingSink) SetDownstream(downstream entity.EntitySyncSink) {
	if s == nil {
		return
	}
	s.configMu.Lock()
	s.downstream = downstream
	s.configMu.Unlock()
}

func (s *CoalescingSink) Downstream() entity.EntitySyncSink {
	if s == nil {
		return nil
	}
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.downstream
}

func (s *CoalescingSink) SetMaxBatch(maxBatch int) {
	if s == nil {
		return
	}
	s.configMu.Lock()
	s.maxBatch = maxBatch
	s.configMu.Unlock()
}

func (s *CoalescingSink) Enqueue(packet entity.SyncPacket) {
	if s == nil {
		return
	}
	packet, ref, ok := normalizeCoalescedPacket(packet)
	if !ok {
		return
	}

	s.opMu.RLock()
	defer s.opMu.RUnlock()
	shard := s.shardForObserver(ref)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	enqueueCoalescedPacketLocked(shard.queueLocked(ref), packet)
}

func enqueueCoalescedPacketLocked(queue *coalescedObserverQueue, packet entity.SyncPacket) {
	switch packet.Type {
	case entity.SyncPacketEnter:
		delete(queue.updates, packet.EntityID)
		delete(queue.afterFullUpdates, packet.EntityID)
		removeCoalescedPacketByEntityID(&queue.leaves, packet.EntityID)
		if !hasCoalescedPacketByEntityID(queue.enters, packet.EntityID) {
			queue.enters = append(queue.enters, packet)
		}
	case entity.SyncPacketLeave:
		if removeCoalescedPacketByEntityID(&queue.enters, packet.EntityID) {
			delete(queue.updates, packet.EntityID)
			delete(queue.afterFullUpdates, packet.EntityID)
			return
		}
		delete(queue.updates, packet.EntityID)
		delete(queue.afterFullUpdates, packet.EntityID)
		if !hasCoalescedPacketByEntityID(queue.leaves, packet.EntityID) {
			queue.leaves = append(queue.leaves, packet)
		}
	case entity.SyncPacketUpdate:
		if hasCoalescedPacketByEntityID(queue.enters, packet.EntityID) {
			return
		}
		if packet.Full {
			queue.updates[packet.EntityID] = []entity.SyncPacket{packet}
			delete(queue.afterFullUpdates, packet.EntityID)
			return
		}
		old := queue.updates[packet.EntityID]
		if len(old) > 0 && old[len(old)-1].Full {
			queue.afterFullUpdates[packet.EntityID] = appendOrMergeCoalescedUpdate(queue.afterFullUpdates[packet.EntityID], packet)
			return
		}
		queue.updates[packet.EntityID] = appendOrMergeCoalescedUpdate(old, packet)
	case entity.SyncPacketCustom:
		queue.customs = append(queue.customs, packet)
	default:
		queue.customs = append(queue.customs, packet)
	}
}

func (s *CoalescingSink) EnqueueBatch(packets []entity.SyncPacket) {
	if s == nil || len(packets) == 0 {
		return
	}
	if len(packets) == 1 {
		s.Enqueue(packets[0])
		return
	}
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	byShard := make([][]entity.SyncPacket, len(s.shards))
	for _, packet := range packets {
		normalized, ref, ok := normalizeCoalescedPacket(packet)
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
		shard := &s.shards[idx]
		shard.mu.Lock()
		for _, packet := range shardPackets {
			enqueueCoalescedPacketLocked(shard.queueLocked(packet.Observer.Normalize()), packet)
		}
		shard.mu.Unlock()
	}
}

func (s *CoalescingSink) Flush() []entity.SyncPacket {
	if s == nil {
		return nil
	}
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	groups := make([]coalescedObserverPackets, 0)
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		groups = append(groups, shard.drainLocked()...)
		shard.mu.Unlock()
	}
	sort.Slice(groups, func(i, j int) bool {
		return compareSyncObserverRef(groups[i].ref, groups[j].ref) < 0
	})
	packets := make([]entity.SyncPacket, 0)
	for _, group := range groups {
		packets = append(packets, group.packets...)
	}

	downstream, maxBatch := s.configSnapshot()

	if downstream != nil && len(packets) > 0 {
		if maxBatch <= 0 || len(packets) <= maxBatch {
			downstream.EnqueueBatch(packets)
		} else {
			for start := 0; start < len(packets); start += maxBatch {
				end := min(start+maxBatch, len(packets))
				downstream.EnqueueBatch(packets[start:end])
			}
		}
	}
	return packets
}

func (s *CoalescingSink) Reset() {
	if s == nil {
		return
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		clear(shard.queues)
		shard.mu.Unlock()
	}
}

func (s *CoalescingSink) configSnapshot() (entity.EntitySyncSink, int) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.downstream, s.maxBatch
}

func (s *CoalescingSink) shardCount() int {
	if s == nil {
		return 0
	}
	return len(s.shards)
}

func (s *CoalescingSink) shardForObserver(ref entity.SyncObserverRef) *coalescingSinkShard {
	return &s.shards[s.shardIndexForObserver(ref)]
}

func (s *CoalescingSink) shardIndexForObserver(ref entity.SyncObserverRef) int {
	if s == nil || len(s.shards) == 0 {
		return 0
	}
	return int(hashSyncObserverRef(ref) % uint64(len(s.shards)))
}

func (shard *coalescingSinkShard) queueLocked(ref entity.SyncObserverRef) *coalescedObserverQueue {
	ref = ref.Normalize()
	queue := shard.queues[ref]
	if queue == nil {
		queue = &coalescedObserverQueue{
			updates:          make(map[int64][]entity.SyncPacket),
			afterFullUpdates: make(map[int64][]entity.SyncPacket),
		}
		shard.queues[ref] = queue
	}
	return queue
}

func (shard *coalescingSinkShard) drainLocked() []coalescedObserverPackets {
	groups := make([]coalescedObserverPackets, 0)
	refs := make([]entity.SyncObserverRef, 0, len(shard.queues))
	for ref := range shard.queues {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		return compareSyncObserverRef(refs[i], refs[j]) < 0
	})
	for _, ref := range refs {
		queue := shard.queues[ref]
		if len(queue.leaves) == 0 && len(queue.enters) == 0 && len(queue.updates) == 0 && len(queue.afterFullUpdates) == 0 && len(queue.customs) == 0 {
			delete(shard.queues, ref)
			continue
		}
		packets := make([]entity.SyncPacket, 0, len(queue.leaves)+len(queue.enters)+len(queue.updates)+len(queue.afterFullUpdates)+len(queue.customs))
		packets = append(packets, queue.leaves...)
		packets = append(packets, queue.enters...)
		packets = appendSortedCoalescedUpdates(packets, queue.updates)
		packets = appendSortedCoalescedUpdates(packets, queue.afterFullUpdates)
		packets = append(packets, queue.customs...)
		groups = append(groups, coalescedObserverPackets{ref: ref, packets: packets})
		delete(shard.queues, ref)
	}
	return groups
}

func normalizeCoalescedPacket(packet entity.SyncPacket) (entity.SyncPacket, entity.SyncObserverRef, bool) {
	ref := packet.Observer.Normalize()
	if ref.Empty() && packet.ObserverID != 0 {
		ref = entity.NewPlayerSyncObserver(packet.ObserverID)
		packet.Observer = ref
	}
	if ref.Empty() || packet.EntityID == 0 {
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

func hashSyncObserverRef(ref entity.SyncObserverRef) uint64 {
	ref = ref.Normalize()
	hash := fmap.HashUint64(uint64(ref.Kind))
	hash = mixSyncObserverHash(hash, fmap.HashUint64(uint64(ref.ID)))
	hash = mixSyncObserverHash(hash, fmap.HashUint64(uint64(uint32(ref.Sid))))
	hash = mixSyncObserverHash(hash, fmap.HashString(ref.Key))
	return hash
}

func mixSyncObserverHash(hash, value uint64) uint64 {
	return fmap.HashUint64(hash ^ (value + 0x9e3779b97f4a7c15 + (hash << 6) + (hash >> 2)))
}

func appendSortedCoalescedUpdates(dst []entity.SyncPacket, src map[int64][]entity.SyncPacket) []entity.SyncPacket {
	if len(src) == 0 {
		return dst
	}
	ids := make([]int64, 0, len(src))
	for id := range src {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		dst = append(dst, src[id]...)
	}
	return dst
}

func compareSyncObserverRef(a, b entity.SyncObserverRef) int {
	a = a.Normalize()
	b = b.Normalize()
	if a.Kind != b.Kind {
		if a.Kind < b.Kind {
			return -1
		}
		return 1
	}
	if a.ID != b.ID {
		if a.ID < b.ID {
			return -1
		}
		return 1
	}
	if a.Sid != b.Sid {
		if a.Sid < b.Sid {
			return -1
		}
		return 1
	}
	if a.Key < b.Key {
		return -1
	}
	if a.Key > b.Key {
		return 1
	}
	return 0
}

func mergeCoalescedUpdate(old, packet entity.SyncPacket) entity.SyncPacket {
	if old.EntityID == 0 {
		return packet
	}
	old.Mask |= packet.Mask
	old.Full = old.Full || packet.Full
	if packet.Version > old.Version {
		old.Version = packet.Version
	}
	if packet.BaseVersion != 0 && (old.BaseVersion == 0 || packet.BaseVersion < old.BaseVersion) {
		old.BaseVersion = packet.BaseVersion
	}
	if packet.SchemaVersion != 0 {
		old.SchemaVersion = packet.SchemaVersion
	}
	if packet.Reason != entity.SyncFullReasonNone {
		old.Reason = packet.Reason
	}
	if packet.Body != nil {
		old.Body = packet.Body
	}
	return old
}

func appendOrMergeCoalescedUpdate(list []entity.SyncPacket, packet entity.SyncPacket) []entity.SyncPacket {
	if len(list) == 0 {
		return []entity.SyncPacket{packet}
	}
	last := list[len(list)-1]
	if canMergeCoalescedUpdate(last, packet) {
		list[len(list)-1] = mergeCoalescedUpdate(last, packet)
		return list
	}
	return append(list, packet)
}

func canMergeCoalescedUpdate(old, packet entity.SyncPacket) bool {
	if old.Full || packet.Full {
		return true
	}
	return old.Body == nil && packet.Body == nil
}

func hasCoalescedPacketByEntityID(packets []entity.SyncPacket, entityID int64) bool {
	for _, packet := range packets {
		if packet.EntityID == entityID {
			return true
		}
	}
	return false
}

func removeCoalescedPacketByEntityID(packets *[]entity.SyncPacket, entityID int64) bool {
	list := *packets
	for i, packet := range list {
		if packet.EntityID != entityID {
			continue
		}
		copy(list[i:], list[i+1:])
		list[len(list)-1] = entity.SyncPacket{}
		*packets = list[:len(list)-1]
		return true
	}
	return false
}

var _ entity.EntitySyncSink = (*CoalescingSink)(nil)
