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
