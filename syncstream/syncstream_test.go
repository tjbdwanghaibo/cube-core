package syncstream

import (
	"errors"
	"reflect"
	"testing"
)

func TestHistoryAppendReplayAndAck(t *testing.T) {
	history := NewHistory(HistoryOptions{MaxPacketsPerStream: 4, SchemaVersion: 7})
	observer := Observer{Kind: 1, ID: 42, Scope: "match"}
	stream := Stream{Topic: "skill.presentation", Key: 9}
	first, err := history.Append(Packet{Observer: observer, Stream: stream, Full: true, Payload: []byte("full")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := history.Append(Packet{Observer: observer, Stream: stream, Payload: []byte("delta")})
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || first.BaseSequence != 0 || second.Sequence != 2 || second.BaseSequence != 1 || second.SchemaVersion != 7 {
		t.Fatalf("sequence chain = %#v %#v", first, second)
	}
	replay := history.Resync(ResyncRequest{Observer: observer, Stream: stream, AfterSequence: 1, SchemaVersion: 7})
	if replay.FullRequired || len(replay.Packets) != 1 || replay.Packets[0].Sequence != 2 {
		t.Fatalf("replay = %#v", replay)
	}
	replay.Packets[0].Payload[0] = 'X'
	if got := history.Resync(ResyncRequest{Observer: observer, Stream: stream, AfterSequence: 1}).Packets[0].Payload; !reflect.DeepEqual(got, []byte("delta")) {
		t.Fatalf("history payload aliased replay: %q", got)
	}
	if err := history.Acknowledge(observer, stream, 2); err != nil {
		t.Fatal(err)
	}
	if status := history.Status(observer, stream); status.AckedSequence != 2 || status.LatestSequence != 2 {
		t.Fatalf("status = %#v", status)
	}
	if err := history.Acknowledge(observer, stream, 3); !errors.Is(err, ErrAckAhead) {
		t.Fatalf("ack ahead error = %v", err)
	}
}

func TestHistoryDetectsGapSchemaMismatchAndClientAhead(t *testing.T) {
	history := NewHistory(HistoryOptions{MaxPacketsPerStream: 2, SchemaVersion: 3})
	observer := Observer{ID: 1}
	stream := Stream{Topic: "state", Key: 2}
	for _, payload := range []string{"one", "two", "three"} {
		if _, err := history.Append(Packet{Observer: observer, Stream: stream, Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	if result := history.Resync(ResyncRequest{Observer: observer, Stream: stream, AfterSequence: 0, SchemaVersion: 3}); !result.FullRequired || result.Reason != ResyncHistoryGap {
		t.Fatalf("gap result = %#v", result)
	}
	if result := history.Resync(ResyncRequest{Observer: observer, Stream: stream, AfterSequence: 1, SchemaVersion: 4}); !result.FullRequired || result.Reason != ResyncSchemaMismatch {
		t.Fatalf("schema result = %#v", result)
	}
	if result := history.Resync(ResyncRequest{Observer: observer, Stream: stream, AfterSequence: 9}); !result.FullRequired || result.Reason != ResyncClientAhead {
		t.Fatalf("ahead result = %#v", result)
	}
}

func TestFullPacketRepairsTruncatedHistory(t *testing.T) {
	history := NewHistory(HistoryOptions{MaxPacketsPerStream: 2})
	observer := Observer{ID: 1}
	stream := Stream{Topic: "state"}
	_, _ = history.Append(Packet{Observer: observer, Stream: stream, Payload: []byte("lost")})
	full, _ := history.Append(Packet{Observer: observer, Stream: stream, Full: true, Payload: []byte("snapshot")})
	delta, _ := history.Append(Packet{Observer: observer, Stream: stream, Payload: []byte("delta")})
	result := history.Resync(ResyncRequest{Observer: observer, Stream: stream, AfterSequence: 0})
	if result.FullRequired || len(result.Packets) != 2 || !result.Packets[0].Full || result.Packets[0].Sequence != full.Sequence || result.Packets[1].Sequence != delta.Sequence {
		t.Fatalf("repair replay = %#v", result)
	}
}

func TestObserverStreamsAreIsolated(t *testing.T) {
	history := NewHistory(HistoryOptions{})
	stream := Stream{Topic: "state", Key: 7}
	a, _ := history.Append(Packet{Observer: Observer{ID: 1}, Stream: stream})
	b, _ := history.Append(Packet{Observer: Observer{ID: 2}, Stream: stream})
	if a.Sequence != 1 || b.Sequence != 1 {
		t.Fatalf("observer sequences leaked: a=%d b=%d", a.Sequence, b.Sequence)
	}
}

type snapshotProviderFunc func(ResyncRequest) (Packet, error)

func (provider snapshotProviderFunc) Snapshot(request ResyncRequest) (Packet, error) {
	return provider(request)
}

func TestRecoverAutomaticallyAppendsFullSnapshot(t *testing.T) {
	history := NewHistory(HistoryOptions{SchemaVersion: 3})
	request := ResyncRequest{Observer: Observer{ID: 7}, Stream: Stream{Topic: "state", Key: 9}, SchemaVersion: 4}
	result, err := history.Recover(request, snapshotProviderFunc(func(got ResyncRequest) (Packet, error) {
		if got != request {
			t.Fatalf("snapshot request = %#v", got)
		}
		return Packet{Payload: []byte("snapshot"), Critical: true}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.FullRequired || result.Reason != ResyncHistoryMissing || len(result.Packets) != 1 {
		t.Fatalf("recover result = %#v", result)
	}
	packet := result.Packets[0]
	if !packet.Full || packet.Sequence != 1 || packet.SchemaVersion != 4 || packet.Observer != request.Observer || packet.Stream != request.Stream {
		t.Fatalf("recovery packet = %#v", packet)
	}
	replay, err := history.Recover(ResyncRequest{Observer: request.Observer, Stream: request.Stream, SchemaVersion: 4}, nil)
	if err != nil || replay.FullRequired || len(replay.Packets) != 1 {
		t.Fatalf("replay after recovery = %#v, %v", replay, err)
	}
}

func TestHistoryLimitsSchemaTransitionAndMetrics(t *testing.T) {
	history := NewHistory(HistoryOptions{MaxPacketsPerStream: 2, MaxPayloadBytes: 4, MaxStreams: 1, SchemaVersion: 1})
	observer := Observer{ID: 1}
	stream := Stream{Topic: "state"}
	if _, err := history.Append(Packet{Observer: observer, Stream: stream, Payload: []byte("large")}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("payload error = %v", err)
	}
	for index := 0; index < 3; index++ {
		if _, err := history.Append(Packet{Observer: observer, Stream: stream, Payload: []byte{byte(index)}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := history.Append(Packet{Observer: observer, Stream: stream, SchemaVersion: 2}); !errors.Is(err, ErrSchemaTransitionRequiresFull) {
		t.Fatalf("schema transition error = %v", err)
	}
	if _, err := history.Append(Packet{Observer: Observer{ID: 2}, Stream: stream}); !errors.Is(err, ErrStreamLimit) {
		t.Fatalf("stream limit error = %v", err)
	}
	if err := history.Acknowledge(observer, stream, 1); err != nil {
		t.Fatal(err)
	}
	status := history.Status(observer, stream)
	if status.OldestSequence != 2 || status.Dropped != 1 || status.Pending != 2 || status.Retained != 2 {
		t.Fatalf("status = %#v", status)
	}
	metrics := history.Metrics()
	if metrics.Streams != 1 || metrics.Retained != 2 || metrics.Dropped != 1 || metrics.Pending != 2 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestHistoryExportImportIsDetachedAndAtomic(t *testing.T) {
	source := NewHistory(HistoryOptions{MaxPacketsPerStream: 4, SchemaVersion: 2})
	observer := Observer{ID: 5, Scope: "room"}
	stream := Stream{Topic: "state", Key: 3}
	full, _ := source.Append(Packet{Observer: observer, Stream: stream, Full: true, Payload: []byte("full")})
	_, _ = source.Append(Packet{Observer: observer, Stream: stream, Payload: []byte("delta")})
	_ = source.Acknowledge(observer, stream, full.Sequence)

	exported := source.Export()
	target := NewHistory(HistoryOptions{MaxPacketsPerStream: 4})
	if err := target.Import(exported); err != nil {
		t.Fatal(err)
	}
	exported.Streams[0].Packets[0].Payload[0] = 'X'
	replay := target.Resync(ResyncRequest{Observer: observer, Stream: stream})
	if replay.FullRequired || string(replay.Packets[0].Payload) != "full" {
		t.Fatalf("import aliased payload: %#v", replay)
	}

	invalid := target.Export()
	invalid.Streams[0].Packets[1].BaseSequence = 99
	if err := target.Import(invalid); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("invalid import error = %v", err)
	}
	if status := target.Status(observer, stream); status.LatestSequence != 2 {
		t.Fatalf("invalid import was not atomic: %#v", status)
	}
}

type memoryHistoryStore struct {
	snapshot HistorySnapshot
}

func (store *memoryHistoryStore) Load() (HistorySnapshot, error) { return store.snapshot, nil }
func (store *memoryHistoryStore) Save(snapshot HistorySnapshot) error {
	store.snapshot = snapshot
	return nil
}

func TestHistoryStoreSaveAndRestore(t *testing.T) {
	source := NewHistory(HistoryOptions{})
	packet, _ := source.Append(Packet{Observer: Observer{ID: 1}, Stream: Stream{Topic: "state"}, Full: true, Payload: []byte("snapshot")})
	store := &memoryHistoryStore{}
	if err := source.Save(store); err != nil {
		t.Fatal(err)
	}
	target := NewHistory(HistoryOptions{})
	if err := target.Restore(store); err != nil {
		t.Fatal(err)
	}
	if status := target.Status(packet.Observer, packet.Stream); status.LatestSequence != packet.Sequence {
		t.Fatalf("restored status = %#v", status)
	}
	if err := target.Save(nil); !errors.Is(err, ErrHistoryStoreRequired) {
		t.Fatalf("nil store error = %v", err)
	}
}
