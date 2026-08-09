package syncstream

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
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

func BenchmarkHistoryAppendAcknowledge(b *testing.B) {
	history := NewHistory(HistoryOptions{MaxPacketsPerStream: 256, Epoch: 1, PruneAcknowledged: true})
	observer := Observer{ID: 1}
	stream := Stream{Topic: "state", Key: 1}
	payload := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		packet, err := history.Append(Packet{Observer: observer, Stream: stream, Payload: payload})
		if err != nil {
			b.Fatal(err)
		}
		if err := history.AcknowledgeEpoch(observer, stream, packet.Epoch, packet.Sequence); err != nil {
			b.Fatal(err)
		}
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

type memoryJournal struct {
	snapshot  HistorySnapshot
	mutations []HistoryMutation
	recordErr error
}

func (journal *memoryJournal) Load() (HistorySnapshot, error) { return journal.snapshot, nil }
func (journal *memoryJournal) Record(mutation HistoryMutation) error {
	if journal.recordErr != nil {
		return journal.recordErr
	}
	journal.mutations = append(journal.mutations, mutation)
	return nil
}
func (journal *memoryJournal) Checkpoint(snapshot HistorySnapshot) error {
	journal.snapshot = snapshot
	journal.mutations = nil
	return nil
}

func TestDurableHistoryWritesAheadAndDoesNotMutateOnJournalFailure(t *testing.T) {
	seed := NewHistory(HistoryOptions{Epoch: 77}).Export()
	journal := &memoryJournal{snapshot: seed}
	history, err := NewHistoryWithJournal(HistoryOptions{}, journal)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := history.Append(Packet{Observer: Observer{ID: 1}, Stream: Stream{Topic: "state"}, Payload: []byte("one")})
	if err != nil || packet.Epoch != 77 || len(journal.mutations) != 1 || journal.mutations[0].Kind != HistoryMutationAppend {
		t.Fatalf("append=%#v mutations=%#v err=%v", packet, journal.mutations, err)
	}
	journal.recordErr = errors.New("disk full")
	if err := history.Acknowledge(packet.Observer, packet.Stream, packet.Sequence); err == nil {
		t.Fatal("expected journal failure")
	}
	if status := history.Status(packet.Observer, packet.Stream); status.AckedSequence != 0 {
		t.Fatalf("failed journal mutated ACK: %#v", status)
	}
	other := Stream{Topic: "other"}
	if _, err := history.Append(Packet{Observer: packet.Observer, Stream: other}); err == nil {
		t.Fatal("expected journal failure")
	}
	if status := history.Status(packet.Observer, other); status.Retained != 0 {
		t.Fatalf("failed journal retained empty stream: %#v", status)
	}
}

func TestEpochLifecycleAndIdleSweep(t *testing.T) {
	history := NewHistory(HistoryOptions{Epoch: 11, IdleTTL: time.Hour})
	observer := Observer{ID: 2}
	stream := Stream{Topic: "state"}
	packet, _ := history.Append(Packet{Observer: observer, Stream: stream})
	if result := history.Resync(ResyncRequest{Observer: observer, Stream: stream, Epoch: 10}); !result.FullRequired || result.Reason != ResyncEpochMismatch {
		t.Fatalf("epoch mismatch = %#v", result)
	}
	if removed, err := history.SweepIdle(time.Now().Add(2 * time.Hour)); err != nil || removed != 1 {
		t.Fatalf("sweep removed=%d err=%v", removed, err)
	}
	_, _ = history.Append(Packet{Observer: observer, Stream: stream})
	_, _ = history.Append(Packet{Observer: observer, Stream: Stream{Topic: "presentation"}})
	if removed, err := history.DeleteObserver(observer); err != nil || removed != 2 {
		t.Fatalf("delete observer removed=%d err=%v", removed, err)
	}
	if err := history.RotateEpoch(12); err != nil || history.Epoch() != 12 {
		t.Fatalf("rotate epoch=%d err=%v", history.Epoch(), err)
	}
	if packet.Epoch != 11 {
		t.Fatalf("packet epoch = %d", packet.Epoch)
	}
}

func TestFileHistoryJournalRecoversCheckpointAndWAL(t *testing.T) {
	directory := t.TempDir()
	journal, err := NewFileHistoryJournal(directory, 91)
	if err != nil {
		t.Fatal(err)
	}
	history, err := NewHistoryWithJournal(HistoryOptions{MaxPacketsPerStream: 8}, journal)
	if err != nil {
		t.Fatal(err)
	}
	observer := Observer{ID: 4}
	stream := Stream{Topic: "state"}
	first, err := history.Append(Packet{Observer: observer, Stream: stream, Full: true, Payload: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	if err := history.AcknowledgeEpoch(observer, stream, first.Epoch, first.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := history.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	second, err := history.Append(Packet{Observer: observer, Stream: stream, Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	reopenedJournal, err := NewFileHistoryJournal(directory, 1)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewHistoryWithJournal(HistoryOptions{MaxPacketsPerStream: 8}, reopenedJournal)
	if err != nil {
		t.Fatal(err)
	}
	status := reopened.Status(observer, stream)
	if status.Epoch != 91 || status.LatestSequence != second.Sequence || status.AckedSequence != first.Sequence {
		t.Fatalf("status=%#v", status)
	}
	replay := reopened.Resync(ResyncRequest{Observer: observer, Stream: stream, Epoch: 91, AfterSequence: 1})
	if replay.FullRequired || len(replay.Packets) != 1 || string(replay.Packets[0].Payload) != "second" {
		t.Fatalf("replay=%#v", replay)
	}
}

func TestAcknowledgementPrunesAndJournalRecoversPrunedStream(t *testing.T) {
	directory := t.TempDir()
	journal, err := NewFileHistoryJournal(directory, 101)
	if err != nil {
		t.Fatal(err)
	}
	history, err := NewHistoryWithJournal(HistoryOptions{PruneAcknowledged: true}, journal)
	if err != nil {
		t.Fatal(err)
	}
	observer := Observer{ID: 5}
	stream := Stream{Topic: "state"}
	first, err := history.Append(Packet{Observer: observer, Stream: stream, Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := history.AcknowledgeEpoch(observer, stream, first.Epoch, first.Sequence); err != nil {
		t.Fatal(err)
	}
	if status := history.Status(observer, stream); status.Retained != 0 || status.Pruned != 1 {
		t.Fatalf("status after prune = %#v", status)
	}

	reopenedJournal, err := NewFileHistoryJournal(directory, 1)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewHistoryWithJournal(HistoryOptions{PruneAcknowledged: true}, reopenedJournal)
	if err != nil {
		t.Fatal(err)
	}
	status := reopened.Status(observer, stream)
	if status.LatestSequence != 1 || status.AckedSequence != 1 || status.Retained != 0 || status.Pruned != 1 {
		t.Fatalf("reopened status = %#v", status)
	}
	second, err := reopened.Append(Packet{Observer: observer, Stream: stream})
	if err != nil || second.Sequence != 2 || second.BaseSequence != 1 {
		t.Fatalf("continued packet = %#v, err=%v", second, err)
	}
}

func TestFileHistoryJournalFailsClosedOnNewestGenerationCorruption(t *testing.T) {
	journal, err := NewFileHistoryJournal(t.TempDir(), 201)
	if err != nil {
		t.Fatal(err)
	}
	history, err := NewHistoryWithJournal(HistoryOptions{}, journal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := history.Append(Packet{Stream: Stream{Topic: "state"}, Full: true}); err != nil {
		t.Fatal(err)
	}
	if err := history.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := history.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journal.checkpointPath(journal.generation), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, _ := NewFileHistoryJournal(journal.directory, 1)
	if _, err := NewHistoryWithJournal(HistoryOptions{}, reopened); err == nil {
		t.Fatal("expected newest generation corruption to stop recovery")
	}
}
