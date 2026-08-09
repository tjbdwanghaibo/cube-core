// Package syncstream provides domain-neutral ordered state streams. It owns
// stream sequencing, bounded replay history, acknowledgements, and the decision
// to fall back to a full snapshot. Payload encoding and transport are left to
// higher layers.
package syncstream

import (
	"errors"
	"sync"
)

var (
	ErrTopicRequired = errors.New("syncstream: topic is required")
	ErrAckAhead      = errors.New("syncstream: acknowledgement is ahead of the stream")
)

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
}

type streamState struct {
	latest uint64
	acked  uint64
	items  []Packet
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
	history.mutex.Lock()
	defer history.mutex.Unlock()
	key := streamKey{Observer: packet.Observer, Stream: packet.Stream}
	state := history.streams[key]
	if state == nil {
		state = &streamState{}
		history.streams[key] = state
	}
	packet.Sequence = state.latest + 1
	if packet.SchemaVersion == 0 {
		packet.SchemaVersion = history.options.SchemaVersion
	}
	if packet.Full {
		packet.BaseSequence = 0
	} else {
		packet.BaseSequence = state.latest
	}
	packet = packet.Clone()
	state.latest = packet.Sequence
	state.items = append(state.items, packet)
	if overflow := len(state.items) - history.options.MaxPacketsPerStream; overflow > 0 {
		copy(state.items, state.items[overflow:])
		state.items = state.items[:history.options.MaxPacketsPerStream]
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
	Retained       int
}

func (history *History) Status(observer Observer, stream Stream) StreamStatus {
	history.mutex.RLock()
	defer history.mutex.RUnlock()
	state := history.streams[streamKey{Observer: observer, Stream: stream}]
	if state == nil {
		return StreamStatus{}
	}
	return StreamStatus{LatestSequence: state.latest, AckedSequence: state.acked, Retained: len(state.items)}
}
