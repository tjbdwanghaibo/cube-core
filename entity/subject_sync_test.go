package entity

import (
	"errors"
	"sync"
	"testing"
	"time"

	flock "github.com/tjbdwanghaibo/cube-core/lock"
)

func TestFrozenSyncPayloadOwnership(t *testing.T) {
	source := []byte{1, 2, 3}
	copied := CopyFrozenSyncPayload(7, source)
	source[0] = 9
	if got := copied.BytesCopy(); copied.Codec() != 7 || len(got) != 3 || got[0] != 1 {
		t.Fatalf("copied payload changed: codec=%d data=%v", copied.Codec(), got)
	}
	read := copied.BytesCopy()
	read[1] = 9
	if copied.BytesCopy()[1] != 2 {
		t.Fatal("BytesCopy exposed mutable storage")
	}
}

func TestSubjectSyncPrepareCommitProfiles(t *testing.T) {
	var packed []SyncProfile
	state := NewSubjectSyncState(SubjectSyncCreateParam{
		Enabled: true, SubjectID: 42,
		Packer: SubjectSyncPackFunc{Delta: func(profile SyncProfile, mask uint64) (FrozenSyncPayload, error) {
			packed = append(packed, profile)
			return TakeFrozenSyncPayload(1, []byte{byte(profile.LOD), byte(mask)}), nil
		}},
	})
	state.MarkDirty(3)
	prepared, err := state.Prepare([]SyncProfile{{Key: "near", LOD: 1}, {Key: "far", LOD: 2}, {Key: "near", LOD: 1}})
	if err != nil {
		t.Fatal(err)
	}
	updates := prepared.Updates()
	if len(packed) != 2 || len(updates) != 2 {
		t.Fatalf("profiles were not deduplicated: packed=%d updates=%d", len(packed), len(updates))
	}
	for _, update := range updates {
		if update.SubjectID != 42 || update.Version != 1 || update.BaseVersion != 0 || update.Mask != 3 {
			t.Fatalf("invalid prepared update: %+v", update)
		}
	}
	if state.Version() != 0 || !state.PendingDirty() {
		t.Fatal("prepare committed state before delivery")
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if state.Version() != 1 || state.PendingDirty() {
		t.Fatalf("commit failed: version=%d dirty=%v", state.Version(), state.PendingDirty())
	}
}

func TestSubjectSyncAbortAndConcurrentDirty(t *testing.T) {
	state := NewSubjectSyncState(SubjectSyncCreateParam{
		Enabled: true, SubjectID: 43,
		Packer: SubjectSyncPackFunc{Delta: func(SyncProfile, uint64) (FrozenSyncPayload, error) {
			return TakeFrozenSyncPayload(1, []byte("delta")), nil
		}},
	})
	state.MarkDirty(1)
	prepared, err := state.Prepare(nil)
	if err != nil {
		t.Fatal(err)
	}
	state.MarkDirty(2)
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if state.Version() != 1 || !state.PendingDirty() {
		t.Fatalf("new dirty was lost: version=%d dirty=%v", state.Version(), state.PendingDirty())
	}
	second, err := state.Prepare(nil)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("downstream unavailable")
	if err := second.AbortWithError(wantErr); err != nil {
		t.Fatal(err)
	}
	if state.Version() != 1 || !state.PendingDirty() || !errors.Is(state.LastError(), wantErr) {
		t.Fatalf("abort changed content state: version=%d dirty=%v err=%v", state.Version(), state.PendingDirty(), state.LastError())
	}
}

func TestSubjectSyncSnapshotDoesNotAdvanceVersion(t *testing.T) {
	state := NewSubjectSyncState(SubjectSyncCreateParam{
		Enabled: true, SubjectID: 44,
		Packer: SubjectSyncPackFunc{
			Snapshot: func(profile SyncProfile) (FrozenSyncPayload, error) {
				return TakeFrozenSyncPayload(1, []byte(profile.Key)), nil
			},
			Delta: func(SyncProfile, uint64) (FrozenSyncPayload, error) {
				return TakeFrozenSyncPayload(1, []byte("delta")), nil
			},
		},
	})
	state.MarkDirty(1)
	prepared, err := state.Prepare(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshots, err := state.CaptureSnapshot([]SyncProfile{{Key: "near"}}, SyncFullReasonResync)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Version != 1 || snapshots[0].BaseVersion != 1 || state.Version() != 1 {
		t.Fatalf("snapshot advanced shared version: snapshot=%+v state=%d", snapshots, state.Version())
	}
}

func TestSubjectSyncEntityLockOrderAvoidsGuardInversion(t *testing.T) {
	id := mustBuildTestEntityID(t, 901, EntityCategory(1), EntityKind(2))
	base := NewEntityBase(id, EntityCategory(1), false, EntityKind(2))
	mu := flock.NewDefaultMutex(id)
	base.mu = mu
	base.EnableSync(EntitySyncCreateParam{
		Enabled: true, EntityID: id,
		Packer: SubjectSyncPackFunc{Delta: func(SyncProfile, uint64) (FrozenSyncPayload, error) {
			return TakeFrozenSyncPayload(1, []byte("delta")), nil
		}},
	})
	state := base.Sync()
	state.MarkDirty(1)

	mu.Lock()
	asyncStarted := make(chan struct{})
	asyncDone := make(chan struct{})
	go func() {
		close(asyncStarted)
		prepared, err := state.Prepare(nil)
		if err == nil {
			_ = prepared.Abort()
		}
		close(asyncDone)
	}()
	<-asyncStarted

	// The async caller waits for Entity without holding prepareMu. A guarded
	// caller can therefore prepare and abort without a lock cycle.
	state.prepareMu.Lock()
	state.prepareMu.Unlock()
	mu.Unlock()
	<-asyncDone
}

func TestSubjectSyncConcurrentMarkDirtyRace(t *testing.T) {
	state := NewSubjectSyncState(SubjectSyncCreateParam{
		Enabled: true, SubjectID: 45,
		Packer: SubjectSyncPackFunc{Delta: func(SyncProfile, uint64) (FrozenSyncPayload, error) {
			return TakeFrozenSyncPayload(1, []byte("delta")), nil
		}},
	})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(bit uint) {
			defer wg.Done()
			state.MarkDirty(uint64(1) << (bit % 16))
		}(uint(i))
	}
	wg.Wait()
	prepared, err := state.Prepare(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestSubjectSyncDirtyNotifierRunsOutsideStateLock(t *testing.T) {
	state := NewSubjectSyncState(SubjectSyncCreateParam{Enabled: true, SubjectID: 810})
	notified := make(chan struct{}, 1)
	state.SetDirtyNotifier(func(current *SubjectSyncState) {
		if !current.PendingDirty() {
			t.Error("notifier observed clean state")
		}
		notified <- struct{}{}
	})
	state.MarkDirty(1)
	select {
	case <-notified:
	case <-time.After(time.Second):
		t.Fatal("dirty notifier was not called")
	}
}

func TestSubjectSyncDirtyNotifierPanicIsContained(t *testing.T) {
	state := NewSubjectSyncState(SubjectSyncCreateParam{Enabled: true, SubjectID: 811})
	state.SetDirtyNotifier(func(*SubjectSyncState) { panic("notifier panic") })
	state.MarkFullDirty(SyncFullReasonDirty)
	if !state.PendingDirty() {
		t.Fatal("notifier panic lost dirty state")
	}
}
