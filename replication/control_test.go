package replication

import (
	"errors"
	"testing"
)

func TestControlCodecRejectsCorruption(t *testing.T) {
	want := ControlMessage{Type: ControlAck, RoomID: 9, Epoch: 3, Tick: 7, Sequence: 11}
	raw, err := EncodeControl(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeControl(raw)
	if err != nil || got != want {
		t.Fatalf("control=%+v err=%v", got, err)
	}
	raw[12] ^= 0xff
	if _, err := DecodeControl(raw); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("corrupt control err=%v", err)
	}
}

func TestReplicatorControlAckAndResync(t *testing.T) {
	const session SessionID = 5
	r := NewReplicator(ReplicatorConfig{})
	defer r.Close()
	if err := r.RegisterSession(SessionInfo{ID: session}); err != nil {
		t.Fatal(err)
	}
	first := Snapshot{SnapshotMeta: SnapshotMeta{RoomID: 77, Epoch: 1, Tick: 1, SchemaVersion: 1}}
	if err := r.Publish(first); err != nil {
		t.Fatal(err)
	}
	prepared, err := r.PrepareLatest(session)
	if err != nil || prepared.Frame.Kind != FrameFull {
		t.Fatalf("first frame=%+v err=%v", prepared, err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	ack, _ := EncodeControl(ControlMessage{Type: ControlAck, RoomID: 77, Epoch: 1, Tick: 1, Sequence: 1})
	if err := r.HandleControl(session, ack); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Tick = 2
	if err := r.Publish(second); err != nil {
		t.Fatal(err)
	}
	prepared, err = r.PrepareLatest(session)
	if err != nil || prepared.Frame.Kind != FrameDelta || prepared.Frame.BaseTick != 1 {
		t.Fatalf("delta frame=%+v err=%v", prepared, err)
	}
	prepared.Abort()
	resync, _ := EncodeControl(ControlMessage{Type: ControlResync, Reason: ResyncBaselineMissing, RoomID: 77, Epoch: 1, Tick: 1, Sequence: 2})
	if err := r.HandleControl(session, resync); err != nil {
		t.Fatal(err)
	}
	prepared, err = r.PrepareLatest(session)
	if err != nil || prepared.Frame.Kind != FrameFull {
		t.Fatalf("resync frame=%+v err=%v", prepared, err)
	}
	if err := r.HandleControl(session, resync); !errors.Is(err, ErrInvalidControl) {
		t.Fatalf("replayed control err=%v", err)
	}
	stats := r.Stats()
	if stats.ResyncRequests != 1 || stats.InvalidControls != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}
