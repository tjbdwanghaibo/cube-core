// Package syncstream provides domain-neutral ordered state streams. It owns
// stream sequencing, bounded replay history, acknowledgements, and the decision
// to fall back to a full snapshot. Payload encoding and transport are left to
// higher layers.
package syncstream

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrTopicRequired                = errors.New("syncstream: topic is required")
	ErrAckAhead                     = errors.New("syncstream: acknowledgement is ahead of the stream")
	ErrPayloadTooLarge              = errors.New("syncstream: payload exceeds configured limit")
	ErrStreamLimit                  = errors.New("syncstream: stream limit reached")
	ErrSchemaTransitionRequiresFull = errors.New("syncstream: schema transition requires a full packet")
	ErrSnapshotProviderRequired     = errors.New("syncstream: snapshot provider is required")
	ErrInvalidSnapshot              = errors.New("syncstream: invalid history snapshot")
	ErrHistoryStoreRequired         = errors.New("syncstream: history store is required")
)

const HistorySnapshotVersion uint32 = 1

// Observer identifies an isolated consumer view. Scope can carry a shard,
// match, room, or authorization-view identifier without coupling core to it.
type Observer struct {
	Kind    uint8
	ID      int64
	Session int32
	Scope   string
}

// Stream identifies one ordered logical record stream.
type Stream struct {
	Topic string
	Key   int64
}

// Packet is the transport-independent synchronization envelope. Delta packets
// form a chain through BaseSequence. A Full packet starts a new chain.
type Packet struct {
	Observer      Observer
	Stream        Stream
	Sequence      uint64
	BaseSequence  uint64
	SchemaVersion uint32
	Full          bool
	Critical      bool
	Payload       []byte
}

// Clone detaches packet payload storage.
func (packet Packet) Clone() Packet {
	packet.Payload = append([]byte(nil), packet.Payload...)
	return packet
}

// Sink receives ordered packets. Implementations must not retain Payload
// without copying it.
type Sink interface {
	Enqueue(Packet)
}

// BatchSink optionally accepts a batch without changing packet order.
type BatchSink interface {
	EnqueueBatch([]Packet)
}

type HistoryOptions struct {
	MaxPacketsPerStream int
	SchemaVersion       uint32
	MaxPayloadBytes     int
	MaxStreams          int
}

type streamState struct {
	latest  uint64
	acked   uint64
	dropped uint64
	schema  uint32
	items   []Packet
}

// History sequences and retains packets independently for every observer and
// stream pair.
type History struct {
	mutex   sync.RWMutex
	options HistoryOptions
	streams map[streamKey]*streamState
}

type streamKey struct {
	Observer Observer
	Stream   Stream
}

func NewHistory(options HistoryOptions) *History {
	if options.MaxPacketsPerStream <= 0 {
		options.MaxPacketsPerStream = 256
	}
	if options.SchemaVersion == 0 {
		options.SchemaVersion = 1
	}
	return &History{options: options, streams: make(map[streamKey]*streamState)}
}

// Append assigns the next per-stream sequence and retains a detached packet.
func (history *History) Append(packet Packet) (Packet, error) {
	if packet.Stream.Topic == "" {
		return Packet{}, ErrTopicRequired
	}
	if history.options.MaxPayloadBytes > 0 && len(packet.Payload) > history.options.MaxPayloadBytes {
		return Packet{}, ErrPayloadTooLarge
	}
	history.mutex.Lock()
	defer history.mutex.Unlock()
	key := streamKey{Observer: packet.Observer, Stream: packet.Stream}
	state := history.streams[key]
	if state == nil {
		if history.options.MaxStreams > 0 && len(history.streams) >= history.options.MaxStreams {
			return Packet{}, ErrStreamLimit
		}
		state = &streamState{}
		history.streams[key] = state
	}
	if packet.SchemaVersion == 0 {
		packet.SchemaVersion = history.options.SchemaVersion
	}
	if state.latest > 0 && packet.SchemaVersion != state.schema && !packet.Full {
		return Packet{}, ErrSchemaTransitionRequiresFull
	}
	packet.Sequence = state.latest + 1
	if packet.Full {
		packet.BaseSequence = 0
	} else {
		packet.BaseSequence = state.latest
	}
	packet = packet.Clone()
	state.latest = packet.Sequence
	state.schema = packet.SchemaVersion
	state.items = append(state.items, packet)
	if overflow := len(state.items) - history.options.MaxPacketsPerStream; overflow > 0 {
		copy(state.items, state.items[overflow:])
		state.items = state.items[:history.options.MaxPacketsPerStream]
		state.dropped += uint64(overflow)
	}
	return packet.Clone(), nil
}

// Acknowledge records monotonic consumer progress. History stays bounded by the
// configured limit; acknowledgements are used for diagnostics and recovery.
func (history *History) Acknowledge(observer Observer, stream Stream, sequence uint64) error {
	history.mutex.Lock()
	defer history.mutex.Unlock()
	state := history.streams[streamKey{Observer: observer, Stream: stream}]
	if state == nil {
		if sequence == 0 {
			return nil
		}
		return ErrAckAhead
	}
	if sequence > state.latest {
		return ErrAckAhead
	}
	if sequence > state.acked {
		state.acked = sequence
	}
	return nil
}

type ResyncReason string

const (
	ResyncNone           ResyncReason = ""
	ResyncHistoryMissing ResyncReason = "history_missing"
	ResyncHistoryGap     ResyncReason = "history_gap"
	ResyncSchemaMismatch ResyncReason = "schema_mismatch"
	ResyncClientAhead    ResyncReason = "client_ahead"
)

type ResyncRequest struct {
	Observer      Observer
	Stream        Stream
	AfterSequence uint64
	SchemaVersion uint32
}

// ResyncResult either contains a contiguous replay or asks the domain layer to
// append and send a Full snapshot.
type ResyncResult struct {
	Packets        []Packet
	FullRequired   bool
	Reason         ResyncReason
	LatestSequence uint64
}

// SnapshotProvider creates a domain snapshot when retained history cannot
// repair a consumer. Recover never calls the provider while holding History's
// lock, so providers may safely inspect their own synchronized state.
type SnapshotProvider interface {
	Snapshot(ResyncRequest) (Packet, error)
}

// Recover returns a replay when possible and automatically appends a new full
// snapshot otherwise.
func (history *History) Recover(request ResyncRequest, provider SnapshotProvider) (ResyncResult, error) {
	result := history.Resync(request)
	if !result.FullRequired {
		return result, nil
	}
	if provider == nil {
		return result, ErrSnapshotProviderRequired
	}
	packet, err := provider.Snapshot(request)
	if err != nil {
		return result, err
	}
	packet.Observer = request.Observer
	packet.Stream = request.Stream
	packet.Full = true
	if request.SchemaVersion != 0 {
		packet.SchemaVersion = request.SchemaVersion
	}
	packet, err = history.Append(packet)
	if err != nil {
		return result, err
	}
	return ResyncResult{Packets: []Packet{packet}, Reason: result.Reason, LatestSequence: packet.Sequence}, nil
}

func (history *History) Resync(request ResyncRequest) ResyncResult {
	history.mutex.RLock()
	defer history.mutex.RUnlock()
	state := history.streams[streamKey{Observer: request.Observer, Stream: request.Stream}]
	if state == nil {
		return ResyncResult{FullRequired: true, Reason: ResyncHistoryMissing}
	}
	result := ResyncResult{LatestSequence: state.latest}
	if request.AfterSequence > state.latest {
		result.FullRequired, result.Reason = true, ResyncClientAhead
		return result
	}
	if request.SchemaVersion != 0 && state.latest > 0 && state.schema != request.SchemaVersion {
		result.FullRequired, result.Reason = true, ResyncSchemaMismatch
		return result
	}
	if request.AfterSequence == state.latest {
		return result
	}
	first := -1
	for index := range state.items {
		if state.items[index].Sequence > request.AfterSequence {
			first = index
			break
		}
	}
	if first < 0 {
		result.FullRequired, result.Reason = true, ResyncHistoryGap
		return result
	}
	candidate := state.items[first:]
	if request.SchemaVersion != 0 && candidate[0].SchemaVersion != request.SchemaVersion {
		result.FullRequired, result.Reason = true, ResyncSchemaMismatch
		return result
	}
	if !candidate[0].Full && candidate[0].BaseSequence != request.AfterSequence {
		result.FullRequired, result.Reason = true, ResyncHistoryGap
		return result
	}
	previous := candidate[0].Sequence
	for index := 1; index < len(candidate); index++ {
		if candidate[index].SchemaVersion != candidate[0].SchemaVersion || (!candidate[index].Full && candidate[index].BaseSequence != previous) {
			result.FullRequired, result.Reason = true, ResyncHistoryGap
			return result
		}
		previous = candidate[index].Sequence
	}
	result.Packets = make([]Packet, len(candidate))
	for index := range candidate {
		result.Packets[index] = candidate[index].Clone()
	}
	return result
}

type StreamStatus struct {
	LatestSequence uint64
	AckedSequence  uint64
	OldestSequence uint64
	Pending        uint64
	Dropped        uint64
	Retained       int
}

func (history *History) Status(observer Observer, stream Stream) StreamStatus {
	history.mutex.RLock()
	defer history.mutex.RUnlock()
	state := history.streams[streamKey{Observer: observer, Stream: stream}]
	if state == nil {
		return StreamStatus{}
	}
	status := StreamStatus{
		LatestSequence: state.latest,
		AckedSequence:  state.acked,
		Pending:        state.latest - state.acked,
		Dropped:        state.dropped,
		Retained:       len(state.items),
	}
	if len(state.items) > 0 {
		status.OldestSequence = state.items[0].Sequence
	}
	return status
}

// HistoryMetrics is a lock-consistent aggregate suitable for health checks and
// metrics exporters.
type HistoryMetrics struct {
	Streams  int
	Retained uint64
	Dropped  uint64
	Pending  uint64
}

func (history *History) Metrics() HistoryMetrics {
	history.mutex.RLock()
	defer history.mutex.RUnlock()
	metrics := HistoryMetrics{Streams: len(history.streams)}
	for _, state := range history.streams {
		metrics.Retained += uint64(len(state.items))
		metrics.Dropped += state.dropped
		metrics.Pending += state.latest - state.acked
	}
	return metrics
}

// HistorySnapshot is the stable, transport-neutral persistence representation.
// Export and Import always detach packet payloads.
type HistorySnapshot struct {
	Version uint32
	Streams []HistoryStreamSnapshot
}

type HistoryStreamSnapshot struct {
	Observer Observer
	Stream   Stream
	Latest   uint64
	Acked    uint64
	Dropped  uint64
	Packets  []Packet
}

func (history *History) Export() HistorySnapshot {
	history.mutex.RLock()
	defer history.mutex.RUnlock()
	result := HistorySnapshot{Version: HistorySnapshotVersion, Streams: make([]HistoryStreamSnapshot, 0, len(history.streams))}
	for key, state := range history.streams {
		stream := HistoryStreamSnapshot{
			Observer: key.Observer,
			Stream:   key.Stream,
			Latest:   state.latest,
			Acked:    state.acked,
			Dropped:  state.dropped,
			Packets:  make([]Packet, len(state.items)),
		}
		for index := range state.items {
			stream.Packets[index] = state.items[index].Clone()
		}
		result.Streams = append(result.Streams, stream)
	}
	sort.Slice(result.Streams, func(left, right int) bool {
		a, b := result.Streams[left], result.Streams[right]
		if a.Observer.Scope != b.Observer.Scope {
			return a.Observer.Scope < b.Observer.Scope
		}
		if a.Observer.Kind != b.Observer.Kind {
			return a.Observer.Kind < b.Observer.Kind
		}
		if a.Observer.ID != b.Observer.ID {
			return a.Observer.ID < b.Observer.ID
		}
		if a.Observer.Session != b.Observer.Session {
			return a.Observer.Session < b.Observer.Session
		}
		if a.Stream.Topic != b.Stream.Topic {
			return a.Stream.Topic < b.Stream.Topic
		}
		return a.Stream.Key < b.Stream.Key
	})
	return result
}

// Import atomically replaces history after validating the complete snapshot.
func (history *History) Import(snapshot HistorySnapshot) error {
	if snapshot.Version != HistorySnapshotVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidSnapshot, snapshot.Version)
	}
	if history.options.MaxStreams > 0 && len(snapshot.Streams) > history.options.MaxStreams {
		return ErrStreamLimit
	}
	streams := make(map[streamKey]*streamState, len(snapshot.Streams))
	for index := range snapshot.Streams {
		item := snapshot.Streams[index]
		if item.Stream.Topic == "" {
			return fmt.Errorf("%w: stream %d has no topic", ErrInvalidSnapshot, index)
		}
		key := streamKey{Observer: item.Observer, Stream: item.Stream}
		if _, exists := streams[key]; exists {
			return fmt.Errorf("%w: duplicate stream", ErrInvalidSnapshot)
		}
		if item.Acked > item.Latest || (item.Latest > 0 && len(item.Packets) == 0) {
			return fmt.Errorf("%w: inconsistent sequence metadata", ErrInvalidSnapshot)
		}
		state := &streamState{latest: item.Latest, acked: item.Acked, dropped: item.Dropped}
		var previous uint64
		var previousSchema uint32
		for packetIndex := range item.Packets {
			packet := item.Packets[packetIndex]
			if packet.Observer != item.Observer || packet.Stream != item.Stream || packet.Sequence == 0 || packet.SchemaVersion == 0 {
				return fmt.Errorf("%w: packet identity mismatch", ErrInvalidSnapshot)
			}
			if history.options.MaxPayloadBytes > 0 && len(packet.Payload) > history.options.MaxPayloadBytes {
				return ErrPayloadTooLarge
			}
			if packetIndex == 0 {
				if !packet.Full && packet.BaseSequence+1 != packet.Sequence {
					return fmt.Errorf("%w: invalid first packet base", ErrInvalidSnapshot)
				}
			} else {
				if packet.Sequence != previous+1 || (!packet.Full && packet.BaseSequence != previous) {
					return fmt.Errorf("%w: broken packet chain", ErrInvalidSnapshot)
				}
				if packet.SchemaVersion != previousSchema && !packet.Full {
					return fmt.Errorf("%w: schema transition without full packet", ErrInvalidSnapshot)
				}
			}
			if packet.Full && packet.BaseSequence != 0 {
				return fmt.Errorf("%w: full packet has a base", ErrInvalidSnapshot)
			}
			state.items = append(state.items, packet.Clone())
			previous, previousSchema = packet.Sequence, packet.SchemaVersion
		}
		if len(state.items) > 0 && previous != item.Latest {
			return fmt.Errorf("%w: latest sequence mismatch", ErrInvalidSnapshot)
		}
		if overflow := len(state.items) - history.options.MaxPacketsPerStream; overflow > 0 {
			state.items = append([]Packet(nil), state.items[overflow:]...)
			state.dropped += uint64(overflow)
		}
		if len(state.items) > 0 {
			state.schema = state.items[len(state.items)-1].SchemaVersion
		}
		streams[key] = state
	}
	history.mutex.Lock()
	history.streams = streams
	history.mutex.Unlock()
	return nil
}

// HistoryStore allows callers to choose their durability mechanism.
type HistoryStore interface {
	Load() (HistorySnapshot, error)
	Save(HistorySnapshot) error
}

func (history *History) Save(store HistoryStore) error {
	if store == nil {
		return ErrHistoryStoreRequired
	}
	return store.Save(history.Export())
}

func (history *History) Restore(store HistoryStore) error {
	if store == nil {
		return ErrHistoryStoreRequired
	}
	snapshot, err := store.Load()
	if err != nil {
		return err
	}
	return history.Import(snapshot)
}
