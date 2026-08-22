package replication

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestShadowStoreCaptureIsImmutableAndBounded(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxObjects = 2
	store := NewShadowStore(limits)
	ref := ObjectRef{ID: 1, Generation: 1}
	if err := store.UpsertObject(ref, 10); err != nil {
		t.Fatal(err)
	}
	payload := []byte{1, 2, 3}
	if err := store.SetComponent(ref, ComponentState{TypeID: 2, SchemaVersion: 1, Data: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 99
	snapshot, err := store.Capture(testMeta(1))
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.Objects[0].Components[0].Data[0]; got != 1 {
		t.Fatalf("shadow payload was not copied: %d", got)
	}
	if err := store.SetComponent(ref, ComponentState{TypeID: 2, SchemaVersion: 1, Data: []byte{7}}); err != nil {
		t.Fatal(err)
	}
	if got := snapshot.Objects[0].Components[0].Data[0]; got != 1 {
		t.Fatalf("captured snapshot mutated after write: %d", got)
	}
	if err := store.UpsertObject(ObjectRef{ID: 2, Generation: 1}, 10); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertObject(ObjectRef{ID: 3, Generation: 1}, 10); !errors.Is(err, ErrObjectLimit) {
		t.Fatalf("expected object limit, got %v", err)
	}
}

func TestDeltaCodecRoundTripAndApply(t *testing.T) {
	base := mustSnapshot(t, 1, []ObjectState{
		{Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 10, Components: []ComponentState{
			{TypeID: 1, SchemaVersion: 1, Data: []byte("position-a")},
			{TypeID: 2, SchemaVersion: 1, Data: []byte("health-a")},
		}},
		{Ref: ObjectRef{ID: 2, Generation: 1}, Archetype: 20, Components: []ComponentState{
			{TypeID: 1, SchemaVersion: 1, Data: []byte("old")},
		}},
	})
	current := mustSnapshot(t, 2, []ObjectState{
		{Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 10, Components: []ComponentState{
			{TypeID: 1, SchemaVersion: 1, Data: []byte("position-b")},
			{TypeID: 3, SchemaVersion: 2, Data: []byte("ability")},
		}},
		{Ref: ObjectRef{ID: 3, Generation: 1}, Archetype: 30, Components: []ComponentState{
			{TypeID: 4, SchemaVersion: 1, Data: []byte("projectile")},
		}},
	})
	frame, err := BuildDelta(&base, current)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Kind != FrameDelta || frame.BaseTick != 1 {
		t.Fatalf("unexpected frame: %+v", frame)
	}
	encoded, err := EncodeFrame(frame, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeFrame(encoded, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	applied, err := ApplyDelta(&base, decoded, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current, applied) {
		t.Fatalf("delta apply mismatch:\nwant=%+v\n got=%+v", current, applied)
	}

	full, err := BuildDelta(nil, current)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := ApplyDelta(nil, full, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current, restored) {
		t.Fatalf("full apply mismatch: %+v", restored)
	}
}

func TestDecodeFrameRejectsTruncationAndTrailingData(t *testing.T) {
	snapshot := mustSnapshot(t, 1, []ObjectState{{
		Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 1,
		Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: []byte("payload")}},
	}})
	frame, _ := BuildDelta(nil, snapshot)
	encoded, err := EncodeFrame(frame, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][]byte{encoded[:len(encoded)-1], append(append([]byte(nil), encoded...), 1)} {
		if _, err := DecodeFrame(invalid, DefaultLimits()); err == nil {
			t.Fatal("invalid frame should be rejected")
		}
	}
}

func TestDatagramFragmentReassemblyOutOfOrder(t *testing.T) {
	data := bytes.Repeat([]byte("frame-data-"), 400)
	frame := DeltaFrame{SnapshotMeta: testMeta(5), Kind: FrameFull}
	packets, err := FragmentFrame(frame, 7, data, 300, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) < 2 {
		t.Fatal("test requires fragmented payload")
	}
	reassembler := NewReassembler(DefaultLimits(), time.Second)
	var restored []byte
	for i := len(packets) - 1; i >= 0; i-- {
		payload, complete, header, pushErr := reassembler.Push(packets[i], time.Now())
		if pushErr != nil {
			t.Fatal(pushErr)
		}
		if complete {
			restored = payload
			if header.Sequence != 7 || header.Tick != 5 {
				t.Fatalf("unexpected header: %+v", header)
			}
		}
	}
	if !bytes.Equal(data, restored) {
		t.Fatal("reassembled payload mismatch")
	}
	corrupt := append([]byte(nil), packets[0]...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, _, _, err := reassembler.Push(corrupt, time.Now()); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected checksum error, got %v", err)
	}
}

func TestReplicatorUsesAtomicDatagramBatch(t *testing.T) {
	transport := &batchRecordingTransport{}
	replicator := NewReplicator(ReplicatorConfig{Transport: transport, MaxDatagram: 80})
	t.Cleanup(replicator.Close)
	if err := replicator.RegisterSession(SessionInfo{ID: 9}); err != nil {
		t.Fatal(err)
	}
	snapshot := mustSnapshot(t, 1, []ObjectState{{
		Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 1,
		Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: bytes.Repeat([]byte{1}, 256)}},
	}})
	if err := replicator.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.SendLatest(context.Background(), 9); err != nil {
		t.Fatal(err)
	}
	if transport.batches != 1 || transport.fragments < 2 || transport.singles != 0 {
		t.Fatalf("unexpected batch calls: %+v", transport)
	}
}

type batchRecordingTransport struct {
	batches   int
	fragments int
	singles   int
}

func (t *batchRecordingTransport) SendDatagram(context.Context, SessionID, []byte) error {
	t.singles++
	return nil
}

func (t *batchRecordingTransport) SendDatagramBatch(_ context.Context, _ SessionID, packets [][]byte) error {
	t.batches++
	t.fragments += len(packets)
	return nil
}

func (*batchRecordingTransport) SendReliable(context.Context, SessionID, []byte) error { return nil }

func TestReplicatorMigratesTransportSessionLifecycle(t *testing.T) {
	first := &lifecycleRecordingTransport{sessions: make(map[SessionID]bool)}
	second := &lifecycleRecordingTransport{sessions: make(map[SessionID]bool)}
	replicator := NewReplicator(ReplicatorConfig{Transport: first})
	t.Cleanup(replicator.Close)
	if err := replicator.RegisterSession(SessionInfo{ID: 17}); err != nil {
		t.Fatal(err)
	}
	if !first.sessions[17] {
		t.Fatal("initial transport did not receive session")
	}
	if err := replicator.SetTransport(first); err != nil {
		t.Fatal(err)
	}
	if first.registered != 1 {
		t.Fatalf("idempotent SetTransport registered twice: %d", first.registered)
	}
	if err := replicator.SetTransport(second); err != nil {
		t.Fatal(err)
	}
	if first.sessions[17] || !second.sessions[17] || first.removed != 1 || second.registered != 1 {
		t.Fatalf("session was not migrated: first=%+v second=%+v", first, second)
	}
	if snapshot, ok := replicator.Session(17); !ok || !snapshot.ForceFull {
		t.Fatalf("transport migration must force a recoverable full frame: %+v", snapshot)
	}
}

func TestReplicatorDoesNotExposeHalfRegisteredSession(t *testing.T) {
	transport := &blockingLifecycleTransport{started: make(chan struct{}), release: make(chan struct{})}
	replicator := NewReplicator(ReplicatorConfig{Transport: transport})
	t.Cleanup(replicator.Close)
	registered := make(chan error, 1)
	go func() { registered <- replicator.RegisterSession(SessionInfo{ID: 18}) }()
	<-transport.started
	if ids := replicator.SessionIDs(); len(ids) != 0 {
		t.Fatalf("half-registered session became visible: %v", ids)
	}
	close(transport.release)
	if err := <-registered; err != nil {
		t.Fatal(err)
	}
	if ids := replicator.SessionIDs(); len(ids) != 1 || ids[0] != 18 {
		t.Fatalf("registered session missing: %v", ids)
	}
}

type blockingLifecycleTransport struct {
	started chan struct{}
	release chan struct{}
}

func (*blockingLifecycleTransport) SendDatagram(context.Context, SessionID, []byte) error {
	return nil
}
func (*blockingLifecycleTransport) SendReliable(context.Context, SessionID, []byte) error {
	return nil
}
func (transport *blockingLifecycleTransport) RegisterSession(SessionInfo) error {
	close(transport.started)
	<-transport.release
	return nil
}
func (*blockingLifecycleTransport) RemoveSession(SessionID) bool { return true }

func TestDatagramLimitAndSharedReassemblerSessionIsolation(t *testing.T) {
	limits := DefaultLimits()
	frame := DeltaFrame{SnapshotMeta: testMeta(1), Kind: FrameFull}
	if _, err := FragmentFrame(frame, 1, []byte("payload"), limits.MaxDatagramBytes+1, limits); !errors.Is(err, ErrInvalidDatagram) {
		t.Fatalf("oversized configured datagram should fail: %v", err)
	}
	oversized := make([]byte, limits.MaxDatagramBytes+1)
	if _, _, err := DecodeDatagram(oversized, limits); !errors.Is(err, ErrInvalidDatagram) {
		t.Fatalf("oversized inbound datagram should fail: %v", err)
	}

	firstData := bytes.Repeat([]byte{1}, 240)
	secondData := bytes.Repeat([]byte{2}, 240)
	firstPackets, err := FragmentFrame(frame, 3, firstData, 100, limits)
	if err != nil {
		t.Fatal(err)
	}
	secondPackets, err := FragmentFrame(frame, 3, secondData, 100, limits)
	if err != nil {
		t.Fatal(err)
	}
	reassembler := NewReassembler(limits, time.Second)
	now := time.Now()
	for index := range firstPackets {
		first, firstComplete, _, firstErr := reassembler.PushFor(101, firstPackets[index], now)
		if firstErr != nil {
			t.Fatal(firstErr)
		}
		second, secondComplete, _, secondErr := reassembler.PushFor(202, secondPackets[index], now)
		if secondErr != nil {
			t.Fatal(secondErr)
		}
		if index == len(firstPackets)-1 {
			if !firstComplete || !secondComplete || !bytes.Equal(first, firstData) || !bytes.Equal(second, secondData) {
				t.Fatal("shared reassembler mixed authenticated sessions")
			}
		}
	}
}

type lifecycleRecordingTransport struct {
	sessions   map[SessionID]bool
	registered int
	removed    int
}

func (*lifecycleRecordingTransport) SendDatagram(context.Context, SessionID, []byte) error {
	return nil
}

func (*lifecycleRecordingTransport) SendReliable(context.Context, SessionID, []byte) error {
	return nil
}

func (transport *lifecycleRecordingTransport) RegisterSession(info SessionInfo) error {
	transport.sessions[info.ID] = true
	transport.registered++
	return nil
}

func (transport *lifecycleRecordingTransport) RemoveSession(id SessionID) bool {
	exists := transport.sessions[id]
	delete(transport.sessions, id)
	if exists {
		transport.removed++
	}
	return exists
}

func TestReassemblerExpiresAndBoundsInflightFrames(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInflightFrames = 1
	frame := DeltaFrame{SnapshotMeta: testMeta(1), Kind: FrameFull}
	packetsA, _ := FragmentFrame(frame, 1, bytes.Repeat([]byte{1}, 400), 300, limits)
	frame.Tick = 2
	packetsB, _ := FragmentFrame(frame, 2, bytes.Repeat([]byte{2}, 400), 300, limits)
	now := time.Now()
	reassembler := NewReassembler(limits, time.Second)
	if _, _, _, err := reassembler.Push(packetsA[0], now); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := reassembler.Push(packetsB[0], now); !errors.Is(err, ErrReassemblyCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
	if removed := reassembler.Expire(now.Add(time.Second)); removed != 1 {
		t.Fatalf("expired=%d", removed)
	}
}

func TestReplicatorUsesAckedProjectedBaseline(t *testing.T) {
	projector := ProjectorFunc(func(session SessionInfo, snapshot Snapshot) (Snapshot, error) {
		if session.TeamID == 1 {
			filtered := snapshot.Objects[:0]
			for _, obj := range snapshot.Objects {
				if obj.Archetype != 99 {
					filtered = append(filtered, obj)
				}
			}
			snapshot.Objects = filtered
		}
		return snapshot, nil
	})
	replicator := NewReplicator(ReplicatorConfig{Projector: projector, MaxDatagram: 300})
	defer replicator.Close()
	if err := replicator.RegisterSession(SessionInfo{ID: 9, TeamID: 1}); err != nil {
		t.Fatal(err)
	}
	first := mustSnapshot(t, 1, []ObjectState{
		{Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 1, Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: []byte("a")}}},
		{Ref: ObjectRef{ID: 2, Generation: 1}, Archetype: 99, Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: []byte("secret")}}},
	})
	if err := replicator.Publish(first); err != nil {
		t.Fatal(err)
	}
	prepared, err := replicator.PrepareLatest(9)
	if err != nil {
		t.Fatal(err)
	}
	full, packets := prepared.Frame, prepared.Datagrams
	if full.Kind != FrameFull || len(full.Objects) != 1 || len(packets) == 0 {
		t.Fatalf("unexpected projected full frame: %+v", full)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Acknowledge(9, 1); err != nil {
		t.Fatal(err)
	}
	second := mustSnapshot(t, 2, []ObjectState{
		{Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 1, Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: []byte("b")}}},
		{Ref: ObjectRef{ID: 2, Generation: 1}, Archetype: 99, Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: []byte("more-secret")}}},
	})
	if err := replicator.Publish(second); err != nil {
		t.Fatal(err)
	}
	delta, _, err := replicator.BuildLatest(9)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Kind != FrameDelta || delta.BaseTick != 1 || len(delta.Objects) != 1 || delta.Objects[0].Ref.ID != 1 {
		t.Fatalf("unexpected projected delta: %+v", delta)
	}
	if err := replicator.Acknowledge(9, 99); !errors.Is(err, ErrInvalidAck) {
		t.Fatalf("expected invalid ack, got %v", err)
	}
}

func TestReplicatorDeltaUsesExactProjectionPreviouslySent(t *testing.T) {
	showSecret := false
	projector := ProjectorFunc(func(_ SessionInfo, snapshot Snapshot) (Snapshot, error) {
		if !showSecret {
			filtered := snapshot.Objects[:0]
			for _, obj := range snapshot.Objects {
				if obj.Archetype != 99 {
					filtered = append(filtered, obj)
				}
			}
			snapshot.Objects = filtered
		}
		return snapshot, nil
	})
	replicator := NewReplicator(ReplicatorConfig{Projector: projector})
	defer replicator.Close()
	if err := replicator.RegisterSession(SessionInfo{ID: 10}); err != nil {
		t.Fatal(err)
	}
	first := mustSnapshot(t, 1, []ObjectState{
		{Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 1},
		{Ref: ObjectRef{ID: 2, Generation: 1}, Archetype: 99},
	})
	if err := replicator.Publish(first); err != nil {
		t.Fatal(err)
	}
	prepared, err := replicator.PrepareLatest(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Acknowledge(10, 1); err != nil {
		t.Fatal(err)
	}
	showSecret = true
	second := mustSnapshot(t, 2, first.Objects)
	if err := replicator.Publish(second); err != nil {
		t.Fatal(err)
	}
	delta, _, err := replicator.BuildLatest(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Objects) != 1 || delta.Objects[0].Operation != ObjectCreate || delta.Objects[0].Ref.ID != 2 {
		t.Fatalf("newly visible object must be created relative to the exact sent baseline: %+v", delta)
	}
}

func TestShadowStoreConcurrentCapture(t *testing.T) {
	store := NewShadowStore(DefaultLimits())
	ref := ObjectRef{ID: 1, Generation: 1}
	if err := store.UpsertObject(ref, 1); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = store.SetComponent(ref, ComponentState{TypeID: uint16(worker + 1), SchemaVersion: 1, Data: []byte{byte(i)}})
				_, _ = store.Capture(testMeta(uint32(i + 1)))
			}
		}(worker)
	}
	wg.Wait()
	if count := store.ObjectCount(); count != 1 {
		t.Fatalf("object count=%d", count)
	}
}

func TestSchemaRegistryRejectsDuplicates(t *testing.T) {
	registry := NewSchemaRegistry()
	schema := ComponentSchema{
		TypeID: 1, Name: "transform", Version: 1, MaxEncodedSize: 32,
		Policy: ReplicationPolicy{Lane: LaneState, Reliability: ReliabilityUnreliableLatest, Priority: PriorityHigh, MaxRateHz: 20, Visibility: VisibilityPublic, Codec: CodecGeneratedBitset},
	}
	if err := registry.Register(schema); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(schema); err == nil {
		t.Fatal("duplicate schema should fail")
	}
	if got := registry.Snapshot(); len(got) != 1 || got[0].Name != "transform" {
		t.Fatalf("schemas=%+v", got)
	}
}

func TestHundredObjectSnapshotStaysWithinExpectedRoomBudget(t *testing.T) {
	objects := make([]ObjectState, 0, 100)
	for id := uint16(1); id <= 100; id++ {
		objects = append(objects, ObjectState{
			Ref: ObjectRef{ID: id, Generation: 1}, Archetype: 1,
			Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: bytes.Repeat([]byte{byte(id)}, 24)}},
		})
	}
	snapshot := mustSnapshot(t, 1, objects)
	frame, err := BuildDelta(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeFrame(frame, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 6<<10 {
		t.Fatalf("100-object keyframe unexpectedly large: %d bytes", len(encoded))
	}
	packets, err := FragmentFrame(frame, 1, encoded, DefaultMaxDatagram, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, packet := range packets {
		if len(packet) > DefaultMaxDatagram {
			t.Fatalf("datagram exceeds MTU budget: %d", len(packet))
		}
	}
}

func TestReplicatorCloseWaitsForActiveTransport(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	transport := TransportFunc{Datagram: func(context.Context, SessionID, []byte) error {
		close(started)
		<-release
		return nil
	}}
	replicator := NewReplicator(ReplicatorConfig{Transport: transport})
	if err := replicator.RegisterSession(SessionInfo{ID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Publish(mustSnapshot(t, 1, nil)); err != nil {
		t.Fatal(err)
	}
	sendDone := make(chan error, 1)
	go func() {
		_, err := replicator.SendLatest(context.Background(), 1)
		sendDone <- err
	}()
	<-started
	closeDone := make(chan struct{})
	go func() {
		replicator.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Close returned while transport operation was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-sendDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after transport returned")
	}
}

func TestBuildLatestPreviewCannotBeAcknowledged(t *testing.T) {
	replicator := NewReplicator(ReplicatorConfig{})
	t.Cleanup(replicator.Close)
	if err := replicator.RegisterSession(SessionInfo{ID: 31}); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Publish(mustSnapshot(t, 1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, packets, err := replicator.BuildLatest(31); err != nil || len(packets) == 0 {
		t.Fatalf("preview failed: packets=%d err=%v", len(packets), err)
	}
	if err := replicator.Acknowledge(31, 1); !errors.Is(err, ErrInvalidAck) {
		t.Fatalf("unsent preview became acknowledgeable: %v", err)
	}
	state, ok := replicator.Session(31)
	if !ok || state.LastSent != 0 || !state.ForceFull {
		t.Fatalf("preview polluted session history: %+v", state)
	}
}

func TestSendLatestFailureDoesNotCommitSessionHistory(t *testing.T) {
	sendErr := errors.New("datagram rejected")
	replicator := NewReplicator(ReplicatorConfig{Transport: TransportFunc{
		Datagram: func(context.Context, SessionID, []byte) error { return sendErr },
	}})
	t.Cleanup(replicator.Close)
	if err := replicator.RegisterSession(SessionInfo{ID: 32}); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Publish(mustSnapshot(t, 1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.SendLatest(context.Background(), 32); !errors.Is(err, sendErr) {
		t.Fatalf("expected send failure, got %v", err)
	}
	if err := replicator.Acknowledge(32, 1); !errors.Is(err, ErrInvalidAck) {
		t.Fatalf("failed frame became acknowledgeable: %v", err)
	}
	state, ok := replicator.Session(32)
	if !ok || state.LastSent != 0 || !state.ForceFull {
		t.Fatalf("failed send polluted session history: %+v", state)
	}
}

func TestSetTransportWaitsForInflightSend(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	oldTransport := TransportFunc{Datagram: func(context.Context, SessionID, []byte) error {
		close(started)
		<-release
		return nil
	}}
	replicator := NewReplicator(ReplicatorConfig{Transport: oldTransport})
	t.Cleanup(replicator.Close)
	if err := replicator.RegisterSession(SessionInfo{ID: 33}); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Publish(mustSnapshot(t, 1, nil)); err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() {
		_, err := replicator.SendLatest(context.Background(), 33)
		sent <- err
	}()
	<-started
	swapped := make(chan error, 1)
	go func() {
		swapped <- replicator.SetTransport(TransportFunc{Datagram: func(context.Context, SessionID, []byte) error { return nil }})
	}()
	select {
	case err := <-swapped:
		t.Fatalf("transport changed during an in-flight send: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := <-swapped; err != nil {
		t.Fatal(err)
	}
}

func FuzzDecodeFrameDoesNotPanic(f *testing.F) {
	snapshot, _ := NewSnapshot(testMeta(1), []ObjectState{{
		Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 1,
		Components: []ComponentState{{TypeID: 1, SchemaVersion: 1, Data: []byte("seed")}},
	}}, DefaultLimits())
	frame, _ := BuildDelta(nil, snapshot)
	encoded, _ := EncodeFrame(frame, DefaultLimits())
	f.Add(encoded)
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeFrame(data, DefaultLimits())
	})
}

func testMeta(tick uint32) SnapshotMeta {
	return SnapshotMeta{RoomID: 100, Epoch: 1, Tick: tick, SchemaVersion: 1}
}

func mustSnapshot(t *testing.T, tick uint32, objects []ObjectState) Snapshot {
	t.Helper()
	snapshot, err := NewSnapshot(testMeta(tick), objects, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
