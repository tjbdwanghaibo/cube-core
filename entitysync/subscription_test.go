package entitysync

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

type recordingEnvelopeSink struct {
	mu        sync.Mutex
	batches   [][]DeliveryEnvelope
	rejectErr error
}

type discardEnvelopeSink struct{}

func (discardEnvelopeSink) AdmitEnvelopes(context.Context, []DeliveryEnvelope) error { return nil }

func (s *recordingEnvelopeSink) AdmitEnvelopes(_ context.Context, envelopes []DeliveryEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rejectErr != nil {
		return s.rejectErr
	}
	s.batches = append(s.batches, append([]DeliveryEnvelope(nil), envelopes...))
	return nil
}

func (s *recordingEnvelopeSink) snapshot() [][]DeliveryEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]DeliveryEnvelope, len(s.batches))
	for i := range s.batches {
		out[i] = append([]DeliveryEnvelope(nil), s.batches[i]...)
	}
	return out
}

func newSubscriptionTestState(t *testing.T, subjectID int64, packCount *int) *entity.SubjectSyncState {
	t.Helper()
	return entity.NewSubjectSyncState(entity.SubjectSyncCreateParam{
		Enabled: true, SubjectID: subjectID,
		Packer: entity.SubjectSyncPackFunc{
			Snapshot: func(profile entity.SyncProfile) (entity.FrozenSyncPayload, error) {
				*packCount++
				return entity.TakeFrozenSyncPayload(1, []byte("snapshot:"+profile.Key)), nil
			},
			Delta: func(profile entity.SyncProfile, _ uint64) (entity.FrozenSyncPayload, error) {
				*packCount++
				return entity.TakeFrozenSyncPayload(1, []byte("delta:"+profile.Key)), nil
			},
		},
	})
}

func TestSubscriptionCoordinatorSharesProfilePayload(t *testing.T) {
	sink := &recordingEnvelopeSink{}
	coordinator := NewSubscriptionCoordinator(sink)
	packCount := 0
	state := newSubscriptionTestState(t, 1001, &packCount)
	profile := entity.SyncProfile{Key: "near", LOD: 1}
	for id := int64(1); id <= 20; id++ {
		if _, err := coordinator.Subscribe(context.Background(), SubscriberRef{Kind: SubscriberKindPlayer, ID: id}, state, profile); err != nil {
			t.Fatal(err)
		}
	}
	packCount = 0
	state.MarkDirty(7)
	prepared, err := state.Prepare([]entity.SyncProfile{profile})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Distribute(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if packCount != 1 {
		t.Fatalf("profile payload packed %d times for 20 subscribers", packCount)
	}
	batches := sink.snapshot()
	last := batches[len(batches)-1]
	if len(last) != 20 || last[0].Kind != EnvelopeDelta {
		t.Fatalf("unexpected distributed batch: len=%d kind=%d", len(last), last[0].Kind)
	}
	if state.Version() != 1 || state.PendingDirty() {
		t.Fatalf("content state was not committed: version=%d dirty=%v", state.Version(), state.PendingDirty())
	}
}

func TestSubscriptionAdmissionFailureRollsBack(t *testing.T) {
	wantErr := errors.New("queue full")
	sink := &recordingEnvelopeSink{rejectErr: wantErr}
	coordinator := NewSubscriptionCoordinator(sink)
	packCount := 0
	state := newSubscriptionTestState(t, 1002, &packCount)
	subscriber := SubscriberRef{Kind: SubscriberKindPlayer, ID: 11}
	if _, err := coordinator.Subscribe(context.Background(), subscriber, state, entity.SyncProfile{Key: "default"}); !errors.Is(err, wantErr) {
		t.Fatalf("Subscribe error=%v", err)
	}
	if _, ok := coordinator.Get(subscriber, state.SubjectID()); ok {
		t.Fatal("failed subscription remained active")
	}

	sink.rejectErr = nil
	if _, err := coordinator.Subscribe(context.Background(), subscriber, state, entity.SyncProfile{Key: "default"}); err != nil {
		t.Fatal(err)
	}
	state.MarkDirty(1)
	prepared, err := state.Prepare(nil)
	if err != nil {
		t.Fatal(err)
	}
	sink.rejectErr = wantErr
	if err := coordinator.Distribute(context.Background(), prepared); !errors.Is(err, wantErr) {
		t.Fatalf("Distribute error=%v", err)
	}
	if state.Version() != 0 || !state.PendingDirty() {
		t.Fatalf("failed distribution committed state: version=%d dirty=%v", state.Version(), state.PendingDirty())
	}
}

func TestSubscriptionUnsubscribeFailureRestoresActive(t *testing.T) {
	sink := &recordingEnvelopeSink{}
	coordinator := NewSubscriptionCoordinator(sink)
	packCount := 0
	state := newSubscriptionTestState(t, 1003, &packCount)
	subscriber := SubscriberRef{Kind: SubscriberKindPlayer, ID: 12}
	if _, err := coordinator.Subscribe(context.Background(), subscriber, state, entity.SyncProfile{Key: "default"}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("leave rejected")
	sink.rejectErr = wantErr
	if err := coordinator.Unsubscribe(context.Background(), subscriber, state.SubjectID()); !errors.Is(err, wantErr) {
		t.Fatalf("Unsubscribe error=%v", err)
	}
	got, ok := coordinator.Get(subscriber, state.SubjectID())
	if !ok || got.State != SubscriptionActive {
		t.Fatalf("subscription not restored: %+v ok=%v", got, ok)
	}
	sink.rejectErr = nil
	if err := coordinator.Unsubscribe(context.Background(), subscriber, state.SubjectID()); err != nil {
		t.Fatal(err)
	}
	if _, ok := coordinator.Get(subscriber, state.SubjectID()); ok {
		t.Fatal("subscription remained after admitted leave")
	}
}

func TestSubscriptionCoordinatorFlushSubjectAndContainsSinkPanic(t *testing.T) {
	packCount := 0
	state := newSubscriptionTestState(t, 1004, &packCount)
	panicking := ReliableEnvelopeSinkFunc(func(context.Context, []DeliveryEnvelope) error {
		panic("transport bug")
	})
	coordinator := NewSubscriptionCoordinator(panicking)
	subscriber := SubscriberRef{Kind: SubscriberKindPlayer, ID: 13}
	if _, err := coordinator.Subscribe(context.Background(), subscriber, state, entity.SyncProfile{Key: "default"}); !errors.Is(err, ErrEnvelopeAdmission) {
		t.Fatalf("sink panic was not contained: %v", err)
	}

	sink := &recordingEnvelopeSink{}
	coordinator.SetSink(sink)
	if _, err := coordinator.Subscribe(context.Background(), subscriber, state, entity.SyncProfile{Key: "default"}); err != nil {
		t.Fatal(err)
	}
	state.MarkDirty(1)
	if err := coordinator.FlushSubject(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if state.Version() != 1 || state.PendingDirty() {
		t.Fatalf("FlushSubject did not commit: version=%d dirty=%v", state.Version(), state.PendingDirty())
	}
}

func BenchmarkSubscriptionCoordinatorSharedPayload100Subscribers(b *testing.B) {
	coordinator := NewSubscriptionCoordinator(discardEnvelopeSink{})
	state := entity.NewSubjectSyncState(entity.SubjectSyncCreateParam{
		Enabled: true, SubjectID: 2001,
		Packer: entity.SubjectSyncPackFunc{
			Snapshot: func(entity.SyncProfile) (entity.FrozenSyncPayload, error) {
				return entity.TakeFrozenSyncPayload(1, make([]byte, 1024)), nil
			},
			Delta: func(entity.SyncProfile, uint64) (entity.FrozenSyncPayload, error) {
				return entity.TakeFrozenSyncPayload(1, make([]byte, 1024)), nil
			},
		},
	})
	profile := entity.SyncProfile{Key: "near", LOD: 1}
	for id := int64(1); id <= 100; id++ {
		if _, err := coordinator.Subscribe(context.Background(), SubscriberRef{Kind: SubscriberKindPlayer, ID: id}, state, profile); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.MarkDirty(1)
		prepared, err := state.Prepare([]entity.SyncProfile{profile})
		if err != nil {
			b.Fatal(err)
		}
		if err := coordinator.Distribute(context.Background(), prepared); err != nil {
			b.Fatal(err)
		}
	}
}
