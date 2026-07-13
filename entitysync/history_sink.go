package entitysync

import (
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"sync"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

const defaultHistoryPacketsPerStream = 64

type SyncAckStatus uint8

const (
	SyncAckOK SyncAckStatus = iota + 1
	SyncAckUnknown
	SyncAckGap
	SyncAckStale
)

type SyncResyncStatus uint8

const (
	SyncResyncCurrent SyncResyncStatus = iota + 1
	SyncResyncReplayed
	SyncResyncFull
	SyncResyncNeedFull
)

type HistoryOptions struct {
	MaxPacketsPerStream  int
	StreamTTL            time.Duration
	FullSnapshotProvider SyncFullSnapshotProvider
}

type SyncFullSnapshotProvider interface {
	PackFullSync(SyncResyncRequest) (entity.SyncPacket, bool)
}

type SyncFullSnapshotProviderFunc func(SyncResyncRequest) (entity.SyncPacket, bool)

func (f SyncFullSnapshotProviderFunc) PackFullSync(req SyncResyncRequest) (entity.SyncPacket, bool) {
	if f == nil {
		return entity.SyncPacket{}, false
	}
	return f(req)
}

type SyncAckRequest struct {
	Observer      entity.SyncObserverRef
	Topic         string
	EntityID      int64
	ClientSeq     uint64
	SchemaVersion uint32
}

type SyncAckResult struct {
	Status       SyncAckStatus
	Observer     entity.SyncObserverRef
	Topic        string
	EntityID     int64
	AckSeq       uint64
	ServerSeq    uint64
	NeedResync   bool
	NeedFull     bool
	SnapshotSeen bool
}

type SyncResyncRequest struct {
	Observer      entity.SyncObserverRef
	Topic         string
	EntityID      int64
	ClientSeq     uint64
	SchemaVersion uint32
	ForceFull     bool
}

type SyncResyncResult struct {
	Status    SyncResyncStatus
	Observer  entity.SyncObserverRef
	Topic     string
	EntityID  int64
	ClientSeq uint64
	ServerSeq uint64
	NeedFull  bool
	Snapshot  bool
	Packets   []entity.SyncPacket
}

type HistoryStats struct {
	Streams int `json:"streams"`
	Packets int `json:"packets"`
}

// HistorySink records the packet stream sent to each observer before forwarding
// it. Ack/resync handlers use the retained window to replay missed deltas and
// fall back to a targeted full snapshot when the window is no longer contiguous.
type HistorySink struct {
	mu         sync.Mutex
	downstream entity.EntitySyncSink
	history    *syncHistory
	full       SyncFullSnapshotProvider
}

func NewHistorySink(downstream entity.EntitySyncSink, opts HistoryOptions) *HistorySink {
	return &HistorySink{
		downstream: downstream,
		history:    newSyncHistory(opts),
		full:       opts.FullSnapshotProvider,
	}
}

func (s *HistorySink) SetDownstream(downstream entity.EntitySyncSink) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.downstream = downstream
	s.mu.Unlock()
}

func (s *HistorySink) Downstream() entity.EntitySyncSink {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.downstream
}

func (s *HistorySink) Enqueue(packet entity.SyncPacket) {
	s.EnqueueBatch([]entity.SyncPacket{packet})
}

func (s *HistorySink) EnqueueBatch(packets []entity.SyncPacket) {
	if s == nil || len(packets) == 0 {
		return
	}
	packets = normalizeHistoryPackets(packets)
	s.history.record(packets)
	s.mu.Lock()
	downstream := s.downstream
	s.mu.Unlock()
	if downstream != nil {
		downstream.EnqueueBatch(packets)
	}
}

func (s *HistorySink) Ack(req SyncAckRequest) SyncAckResult {
	if s == nil || s.history == nil {
		return SyncAckResult{Status: SyncAckUnknown}
	}
	return s.history.ack(req)
}

func (s *HistorySink) Resync(req SyncResyncRequest) SyncResyncResult {
	if s == nil || s.history == nil {
		return SyncResyncResult{Status: SyncResyncNeedFull, NeedFull: true}
	}
	result := s.history.resync(req)
	if req.ForceFull || result.NeedFull {
		if full, ok := s.fullPacket(req); ok {
			result = SyncResyncResult{
				Status:    SyncResyncFull,
				Observer:  full.Observer,
				Topic:     full.Topic,
				EntityID:  full.EntityID,
				ClientSeq: req.ClientSeq,
				ServerSeq: full.Version,
				Snapshot:  true,
				Packets:   []entity.SyncPacket{full},
			}
			s.history.record(result.Packets)
		}
	}
	if len(result.Packets) > 0 {
		s.mu.Lock()
		downstream := s.downstream
		s.mu.Unlock()
		if downstream != nil {
			downstream.EnqueueBatch(result.Packets)
		}
	}
	return result
}

func (s *HistorySink) Stats() HistoryStats {
	if s == nil || s.history == nil {
		return HistoryStats{}
	}
	return s.history.stats()
}

func (s *HistorySink) PruneExpired() int {
	if s == nil || s.history == nil {
		return 0
	}
	return s.history.pruneExpired(fctx.Now())
}

func (s *HistorySink) RemoveObserver(observer entity.SyncObserverRef) int {
	if s == nil || s.history == nil {
		return 0
	}
	return s.history.removeObserver(observer)
}

func (s *HistorySink) fullPacket(req SyncResyncRequest) (entity.SyncPacket, bool) {
	if s != nil && s.full != nil {
		if packet, ok := s.full.PackFullSync(req); ok {
			return packet, true
		}
	}
	return targetedFullPacket(req)
}

type syncHistory struct {
	mu        sync.Mutex
	maxPacket int
	streamTTL time.Duration
	streams   map[syncHistoryKey]*syncHistoryStream
}

type syncHistoryKey struct {
	Observer entity.SyncObserverRef
	Topic    string
	EntityID int64
}

type syncHistoryStream struct {
	ackSeq        uint64
	serverSeq     uint64
	schemaVersion uint32
	snapshotSeen  bool
	updatedAt     time.Time
	packets       []entity.SyncPacket
}

func newSyncHistory(opts HistoryOptions) *syncHistory {
	maxPacket := opts.MaxPacketsPerStream
	if maxPacket <= 0 {
		maxPacket = defaultHistoryPacketsPerStream
	}
	return &syncHistory{
		maxPacket: maxPacket,
		streamTTL: opts.StreamTTL,
		streams:   make(map[syncHistoryKey]*syncHistoryStream),
	}
}

func (h *syncHistory) record(packets []entity.SyncPacket) {
	if h == nil || len(packets) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := fctx.Now()
	for _, packet := range packets {
		key, ok := historyKey(packet.Observer, packet.Topic, packet.EntityID)
		if !ok || packet.Version == 0 {
			continue
		}
		stream := h.streams[key]
		if stream == nil {
			stream = &syncHistoryStream{}
			h.streams[key] = stream
		}
		stream.updatedAt = now
		if packet.Version > stream.serverSeq {
			stream.serverSeq = packet.Version
		}
		if packet.SchemaVersion != 0 {
			stream.schemaVersion = packet.SchemaVersion
		}
		if packet.Full || packet.Type == entity.SyncPacketEnter {
			stream.snapshotSeen = true
		}
		stream.packets = appendOrReplacePacket(stream.packets, packet)
		if len(stream.packets) > h.maxPacket {
			copy(stream.packets, stream.packets[len(stream.packets)-h.maxPacket:])
			stream.packets = stream.packets[:h.maxPacket]
		}
	}
}

func (h *syncHistory) pruneExpired(now time.Time) int {
	if h == nil || h.streamTTL <= 0 {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	removed := 0
	for key, stream := range h.streams {
		if stream == nil || (!stream.updatedAt.IsZero() && now.Sub(stream.updatedAt) > h.streamTTL) {
			delete(h.streams, key)
			removed++
		}
	}
	return removed
}

func (h *syncHistory) removeObserver(observer entity.SyncObserverRef) int {
	observer = observer.Normalize()
	if h == nil || observer.Empty() {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	removed := 0
	for key := range h.streams {
		if key.Observer.Normalize() == observer {
			delete(h.streams, key)
			removed++
		}
	}
	return removed
}

func (h *syncHistory) stats() HistoryStats {
	if h == nil {
		return HistoryStats{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	stats := HistoryStats{Streams: len(h.streams)}
	for _, stream := range h.streams {
		if stream != nil {
			stats.Packets += len(stream.packets)
		}
	}
	return stats
}

func (h *syncHistory) ack(req SyncAckRequest) SyncAckResult {
	req.Observer = req.Observer.Normalize()
	result := SyncAckResult{
		Status:   SyncAckUnknown,
		Observer: req.Observer,
		Topic:    req.Topic,
		EntityID: req.EntityID,
		AckSeq:   req.ClientSeq,
	}
	key, ok := historyKey(req.Observer, req.Topic, req.EntityID)
	if h == nil || !ok {
		return result
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	stream := h.streams[key]
	if stream == nil {
		return result
	}
	result.ServerSeq = stream.serverSeq
	result.SnapshotSeen = stream.snapshotSeen
	if req.SchemaVersion != 0 && stream.schemaVersion != 0 && req.SchemaVersion != stream.schemaVersion {
		result.Status = SyncAckGap
		result.NeedResync = true
		result.NeedFull = true
		return result
	}
	if req.ClientSeq > stream.serverSeq {
		result.Status = SyncAckGap
		result.NeedResync = true
		result.NeedFull = true
		return result
	}
	if req.ClientSeq < stream.ackSeq {
		result.Status = SyncAckStale
		result.AckSeq = stream.ackSeq
		return result
	}
	stream.ackSeq = req.ClientSeq
	result.Status = SyncAckOK
	result.AckSeq = stream.ackSeq
	return result
}

func (h *syncHistory) resync(req SyncResyncRequest) SyncResyncResult {
	req.Observer = req.Observer.Normalize()
	result := SyncResyncResult{
		Status:    SyncResyncNeedFull,
		Observer:  req.Observer,
		Topic:     req.Topic,
		EntityID:  req.EntityID,
		ClientSeq: req.ClientSeq,
		NeedFull:  true,
	}
	key, ok := historyKey(req.Observer, req.Topic, req.EntityID)
	if h == nil || !ok {
		return result
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	stream := h.streams[key]
	if stream == nil {
		return result
	}
	result.ServerSeq = stream.serverSeq
	if req.SchemaVersion != 0 && stream.schemaVersion != 0 && req.SchemaVersion != stream.schemaVersion {
		return result
	}
	if req.ClientSeq >= stream.serverSeq {
		result.Status = SyncResyncCurrent
		result.NeedFull = false
		return result
	}
	missing := packetsAfter(stream.packets, req.ClientSeq)
	if len(missing) == 0 {
		return result
	}
	first := missing[0]
	if !first.Full && first.Type != entity.SyncPacketEnter && first.BaseVersion > req.ClientSeq {
		return result
	}
	result.Status = SyncResyncReplayed
	result.NeedFull = false
	result.Snapshot = first.Full || first.Type == entity.SyncPacketEnter
	result.Packets = missing
	return result
}

func historyKey(observer entity.SyncObserverRef, topic string, entityID int64) (syncHistoryKey, bool) {
	observer = observer.Normalize()
	if observer.Empty() || topic == "" || entityID == 0 {
		return syncHistoryKey{}, false
	}
	return syncHistoryKey{Observer: observer, Topic: topic, EntityID: entityID}, true
}

func normalizeHistoryPackets(packets []entity.SyncPacket) []entity.SyncPacket {
	out := make([]entity.SyncPacket, 0, len(packets))
	for _, packet := range packets {
		ref := packet.Observer.Normalize()
		if ref.Empty() && packet.ObserverID != 0 {
			ref = entity.NewPlayerSyncObserver(packet.ObserverID)
		}
		if ref.Empty() || packet.Topic == "" || packet.EntityID == 0 {
			continue
		}
		packet.Observer = ref
		if packet.ObserverID == 0 {
			packet.ObserverID = ref.PlayerID()
		}
		out = append(out, packet)
	}
	return out
}

func appendOrReplacePacket(packets []entity.SyncPacket, packet entity.SyncPacket) []entity.SyncPacket {
	for i, existing := range packets {
		if existing.Version == packet.Version {
			packets[i] = packet
			return packets
		}
	}
	return append(packets, packet)
}

func packetsAfter(packets []entity.SyncPacket, seq uint64) []entity.SyncPacket {
	if len(packets) == 0 {
		return nil
	}
	out := make([]entity.SyncPacket, 0, len(packets))
	for _, packet := range packets {
		if packet.Version > seq {
			out = append(out, packet)
		}
	}
	return out
}

func targetedFullPacket(req SyncResyncRequest) (entity.SyncPacket, bool) {
	if entity.Mgr == nil || req.EntityID == 0 {
		return entity.SyncPacket{}, false
	}
	ent := entity.Mgr.Get(req.EntityID)
	return packFullFromEntity(ent, req)
}

var _ entity.EntitySyncSink = (*HistorySink)(nil)
