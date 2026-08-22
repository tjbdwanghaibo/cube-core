package checkpoint

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- DirtyTracker tests ---

func TestDirtyTracker_Basic(t *testing.T) {
	var d DirtyTracker

	if d.Dirty() {
		t.Fatal("new tracker should not be dirty")
	}
	if d.Version() != 0 {
		t.Fatal("new tracker version should be 0")
	}

	d.MarkPersist(1 << 2)
	if !d.HasPersistDirty() {
		t.Fatal("should be persist dirty after MarkPersist")
	}
	if d.HasSyncDirty() {
		t.Fatal("MarkPersist should not mark sync dirty")
	}
	d.MarkSync(1 << 3)
	if !d.HasSyncDirty() {
		t.Fatal("should be sync dirty after MarkSync")
	}

	ver := d.IncVersion()
	if ver != 1 {
		t.Fatalf("expected version 1, got %d", ver)
	}
	if d.Version() != 1 {
		t.Fatalf("expected version 1, got %d", d.Version())
	}
}

func TestDirtyTracker_FlushCycle(t *testing.T) {
	var d DirtyTracker

	const mask uint64 = 1 << 4
	d.MarkPersist(mask)
	v := d.IncVersion()

	snapMask := d.TakePersistDirty()
	if snapMask != mask {
		t.Fatalf("expected snap mask %d, got %d", mask, snapMask)
	}
	if d.Version() != v {
		t.Fatalf("expected version %d, got %d", v, d.Version())
	}
	if d.HasPersistDirty() {
		t.Fatal("should not be persist dirty after take")
	}

	d.CommitPersist(snapMask)
	if d.HasPersistDirty() {
		t.Fatal("should not be dirty after commit")
	}
}

func TestDirtyTracker_CommitDoesNotClearNewDirty(t *testing.T) {
	var d DirtyTracker

	const mask uint64 = 1 << 5
	d.MarkPersist(mask)
	snapMask := d.TakePersistDirty()

	// Entity modified again while the async write is in flight.
	d.MarkPersist(mask)

	d.CommitPersist(snapMask)

	if !d.HasPersistDirty() {
		t.Fatal("commit should not clear new dirty mask")
	}
}

func TestDirtyTracker_Rollback(t *testing.T) {
	var d DirtyTracker

	d.MarkPersist(1 << 1)
	mask := d.TakePersistDirty()

	d.RollbackPersist(mask)
	if d.PersistDirtyMask() != 1<<1 {
		t.Fatal("should restore exactly the captured dirty mask")
	}
}

func TestDirtyTracker_SyncCycle(t *testing.T) {
	var d DirtyTracker

	const mask uint64 = 1 << 3
	d.MarkSync(mask)
	snapMask := d.TakeSyncDirty()
	if snapMask != mask {
		t.Fatalf("expected sync mask %d, got %d", mask, snapMask)
	}
	if d.HasSyncDirty() {
		t.Fatal("should not be sync dirty after take")
	}

	d.RollbackSync(snapMask)
	if d.SyncDirtyMask() != mask {
		t.Fatalf("expected rollback sync mask %d, got %d", mask, d.SyncDirtyMask())
	}

	snapMask = d.TakeSyncDirty()
	d.CommitSync(snapMask)
	if d.HasSyncDirty() {
		t.Fatal("should not be sync dirty after commit")
	}
}

func TestDirtyTracker_SetVersion(t *testing.T) {
	var d DirtyTracker
	d.SetVersion(42)
	if d.Version() != 42 {
		t.Fatalf("expected 42, got %d", d.Version())
	}
	if d.Dirty() {
		t.Fatal("SetVersion should clear dirty")
	}
}

func TestDirtyTracker_Concurrent(t *testing.T) {
	var d DirtyTracker
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				d.IncVersion()
			}
		}()
	}
	wg.Wait()

	if d.Version() != 10000 {
		t.Fatalf("expected 10000, got %d", d.Version())
	}
}

// --- Journal tests ---

func TestJournal_PushPop(t *testing.T) {
	j := NewJournal(10)

	items := []SaveItem{
		{Collection: "players", ID: 1, Version: 1, Data: []byte("data1")},
		{Collection: "players", ID: 2, Version: 1, Data: []byte("data2")},
	}

	ok := j.Push(items)
	if !ok {
		t.Fatal("push should succeed")
	}
	if j.Len() != 1 {
		t.Fatalf("expected len 1, got %d", j.Len())
	}

	batch := j.PopBatch(10)
	if len(batch) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(batch))
	}
	if len(batch[0].Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(batch[0].Items))
	}
}

func TestJournal_BackPressure(t *testing.T) {
	j := NewJournal(2) // cap = 2

	// Fill journal
	j.Push([]SaveItem{{Collection: "a", ID: 1, Version: 1, Data: []byte("x")}})
	j.Push([]SaveItem{{Collection: "a", ID: 2, Version: 1, Data: []byte("x")}})

	// Third push should block
	done := make(chan bool, 1)
	go func() {
		j.Push([]SaveItem{{Collection: "a", ID: 3, Version: 1, Data: []byte("x")}})
		done <- true
	}()

	select {
	case <-done:
		t.Fatal("push should block when at capacity")
	case <-time.After(50 * time.Millisecond):
		// expected: blocked
	}

	// Pop one to free space
	j.PopBatch(1)

	select {
	case <-done:
		// unblocked
	case <-time.After(100 * time.Millisecond):
		t.Fatal("push should unblock after pop")
	}
}

func TestJournal_Close(t *testing.T) {
	j := NewJournal(10)
	j.Close()

	ok := j.Push([]SaveItem{{Collection: "a", ID: 1, Version: 1, Data: []byte("x")}})
	if ok {
		t.Fatal("push after close should return false")
	}

	batch := j.PopBatch(10)
	if batch != nil {
		t.Fatal("pop from closed empty journal should return nil")
	}
}

func TestJournalStatsExposeCapacityPressure(t *testing.T) {
	j := NewJournal(2)
	if stats := j.Stats(); stats.Cap != 2 || stats.Len != 0 || stats.Closed {
		t.Fatalf("initial stats = %+v", stats)
	}
	if !j.Push([]SaveItem{{Collection: "player", ID: 1, Version: 1}}) {
		t.Fatalf("push failed")
	}
	stats := j.Stats()
	if stats.Len != 1 || stats.Cap != 2 || stats.FillRatio != 0.5 {
		t.Fatalf("stats after push = %+v", stats)
	}
	j.Close()
	if stats := j.Stats(); !stats.Closed {
		t.Fatalf("closed stats = %+v", stats)
	}
}

func TestJournalPushWithContextTimesOutWhenFull(t *testing.T) {
	j := NewJournal(1)
	if !j.Push([]SaveItem{{Collection: "players", ID: 1, Version: 1, Data: []byte("one")}}) {
		t.Fatal("initial Push returned false")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if ok := j.PushWithContext(ctx, []SaveItem{{Collection: "players", ID: 2, Version: 1, Data: []byte("two")}}); ok {
		t.Fatal("PushWithContext returned true when journal was full until context timeout")
	}
	if j.Len() != 1 {
		t.Fatalf("journal len = %d, want 1", j.Len())
	}
}

// --- Mock Backend ---

type mockBackend struct {
	mu                 sync.Mutex
	saved              []SaveOp
	removed            []RemoveOp
	loaded             []RawDoc
	results            []SaveResult
	resultByCollection map[string]SaveResult
	saveErr            error
	saveCt             atomic.Int64
}

func (m *mockBackend) BulkSave(_ context.Context, ops []SaveOp) ([]SaveResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	m.saveCt.Add(1)
	results := make([]SaveResult, len(ops))
	for i, op := range ops {
		m.saved = append(m.saved, op)
		if m.resultByCollection != nil {
			results[i] = m.resultByCollection[op.Collection]
		} else if len(m.results) > i {
			results[i] = m.results[i]
		} else {
			results[i] = SaveResult{OK: true}
		}
	}
	return results, nil
}

func (m *mockBackend) BulkLoad(_ context.Context, _ LoadOp) ([]RawDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loaded, nil
}

func (m *mockBackend) BulkRemove(_ context.Context, op RemoveOp) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removed = append(m.removed, op)
	return nil
}

func (m *mockBackend) getSaved() []SaveOp {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]SaveOp, len(m.saved))
	copy(cp, m.saved)
	return cp
}

// --- Checkpoint integration test ---

func TestCheckpoint_SubmitAndFlush(t *testing.T) {
	backend := &mockBackend{}
	cp := New(backend,
		WithJournalCap(100),
		WithFlushWorkers(1),
		WithFlushInterval(10*time.Millisecond),
	)

	ctx := context.Background()
	cp.Start(ctx)

	var d1, d2 DirtyTracker
	d1.MarkPersist(1)
	v1 := d1.IncVersion()
	m1 := d1.TakePersistDirty()

	d2.MarkPersist(1)
	v2 := d2.IncVersion()
	m2 := d2.TakePersistDirty()

	cp.Submit([]SaveItem{
		{Collection: "players", ID: 100, Version: v1, Fence: 7, OwnerSid: 1001, Shared: true, Mask: m1, Data: []byte("p100"), Tracker: &d1},
		{Collection: "players", ID: 200, Version: v2, Mask: m2, Data: []byte("p200"), Tracker: &d2},
	})

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	_ = cp.Stop(ctx)

	saved := backend.getSaved()
	if len(saved) != 2 {
		t.Fatalf("expected 2 saved ops, got %d", len(saved))
	}
	if saved[0].ID == 100 && (saved[0].Fence != 7 || saved[0].OwnerSid != 1001 || !saved[0].Shared) {
		t.Fatalf("ownership metadata was not forwarded: %#v", saved[0])
	}
	if saved[1].ID == 100 && (saved[1].Fence != 7 || saved[1].OwnerSid != 1001 || !saved[1].Shared) {
		t.Fatalf("ownership metadata was not forwarded: %#v", saved[1])
	}

	// Trackers should be committed
	if d1.Dirty() {
		t.Fatal("d1 should not be dirty after successful flush")
	}
	if d2.Dirty() {
		t.Fatal("d2 should not be dirty after successful flush")
	}
}

func TestCheckpointActiveFlushDrainsWithoutStopping(t *testing.T) {
	backend := &mockBackend{}
	cp := New(backend, WithFlushWorkers(0))
	if err := cp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var dirty DirtyTracker
	dirty.MarkPersist(1)
	item := SaveItem{Collection: "players", ID: 301, Version: dirty.IncVersion(), Mask: dirty.TakePersistDirty(), Data: []byte("state"), Tracker: &dirty}
	if !cp.Submit([]SaveItem{item}) {
		t.Fatal("submit failed")
	}
	if err := cp.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cp.Journal().Len() != 0 || len(backend.getSaved()) != 1 || dirty.Dirty() {
		t.Fatalf("pending=%d saved=%d dirty=%v", cp.Journal().Len(), len(backend.getSaved()), dirty.Dirty())
	}
	if !cp.Running() {
		t.Fatal("active flush must not stop checkpoint")
	}
	if err := cp.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type blockingBackend struct {
	mockBackend
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingBackend) BulkSave(ctx context.Context, ops []SaveOp) ([]SaveResult, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.release:
		return b.mockBackend.BulkSave(ctx, ops)
	}
}

func TestCheckpointActiveFlushWaitsForInFlightWorker(t *testing.T) {
	backend := &blockingBackend{entered: make(chan struct{}), release: make(chan struct{})}
	cp := New(backend, WithFlushWorkers(1), WithFlushInterval(time.Millisecond))
	if err := cp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !cp.Submit([]SaveItem{{Collection: "players", ID: 302, Version: 1, Data: []byte("state")}}) {
		t.Fatal("submit failed")
	}
	<-backend.entered
	flushed := make(chan error, 1)
	go func() { flushed <- cp.Flush(context.Background()) }()
	select {
	case err := <-flushed:
		t.Fatalf("flush returned before in-flight write: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(backend.release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if err := cp.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointRequeuesPerItemBackendFailure(t *testing.T) {
	backend := &mockBackend{results: []SaveResult{{Err: errors.New("rejected")}}}
	cp := New(backend, WithFlushWorkers(0))
	if err := cp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !cp.Submit([]SaveItem{{Collection: "players", ID: 303, Version: 1, Data: []byte("state")}}) {
		t.Fatal("submit failed")
	}
	if err := cp.Flush(context.Background()); err == nil {
		t.Fatal("first flush must report the item failure")
	}
	if cp.Journal().Len() != 1 {
		t.Fatalf("failed item was not requeued: pending=%d", cp.Journal().Len())
	}
	backend.mu.Lock()
	backend.results = nil
	backend.mu.Unlock()
	if err := cp.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cp.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointSubmitForwardsToSnapshotWALAfterJournalPush(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	cp := New(&mockBackend{}, WithSnapshotWAL(wal), WithFlushWorkers(0))
	items := []SaveItem{{Collection: "players", ID: 1001, Version: 1, Data: []byte("snapshot")}}

	if ok := cp.Submit(items); !ok {
		t.Fatal("Submit returned false")
	}

	if len(wal.submitted) != 1 {
		t.Fatalf("wal submitted count = %d, want 1", len(wal.submitted))
	}
	if got := wal.submitted[0][0]; got.Collection != "players" || got.ID != 1001 || string(got.Data) != "snapshot" {
		t.Fatalf("wal submitted item = %+v", got)
	}
}

func TestCheckpointSubmitFailsWhenRequiredSnapshotWALRejects(t *testing.T) {
	wal := &fakeSnapshotWAL{rejectSubmit: true}
	cp := New(&mockBackend{}, WithSnapshotWAL(wal), WithSnapshotWALRequired(true), WithFlushWorkers(0))
	items := []SaveItem{{Collection: "players", ID: 1, Version: 1, Data: []byte("data")}}

	if ok := cp.Submit(items); ok {
		t.Fatal("Submit returned true when required snapshot WAL rejected the batch")
	}
	if cp.Journal().Len() != 0 {
		t.Fatalf("journal len = %d, want 0 when required WAL rejects", cp.Journal().Len())
	}
	if len(wal.submitted) != 1 {
		t.Fatalf("wal submitted len = %d, want 1", len(wal.submitted))
	}
}

func TestCheckpointSubmitDurableSnapshotWALWritesBeforeJournalPush(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	cp := New(&mockBackend{}, WithSnapshotWAL(wal), WithSnapshotWALMode(SnapshotWALModeDurable), WithSnapshotWALDurableTimeout(time.Second), WithFlushWorkers(0))
	items := []SaveItem{{Collection: "players", ID: 1, Version: 1, Data: []byte("data")}}

	if ok := cp.Submit(items); !ok {
		t.Fatal("Submit returned false")
	}
	if len(wal.durableSubmitted) != 1 {
		t.Fatalf("durable submitted len = %d, want 1", len(wal.durableSubmitted))
	}
	if len(wal.submitted) != 0 {
		t.Fatalf("async submitted len = %d, want 0", len(wal.submitted))
	}
	if cp.Journal().Len() != 1 {
		t.Fatalf("journal len = %d, want 1", cp.Journal().Len())
	}
}

func TestCheckpointSubmitDurableSnapshotWALFailureSkipsJournalPush(t *testing.T) {
	wal := &fakeSnapshotWAL{rejectDurable: true}
	cp := New(&mockBackend{}, WithSnapshotWAL(wal), WithSnapshotWALMode(SnapshotWALModeDurable), WithSnapshotWALDurableTimeout(time.Second), WithFlushWorkers(0))
	items := []SaveItem{{Collection: "players", ID: 1, Version: 1, Data: []byte("data")}}

	if ok := cp.Submit(items); ok {
		t.Fatal("Submit returned true when durable snapshot WAL rejected the batch")
	}
	if cp.Journal().Len() != 0 {
		t.Fatalf("journal len = %d, want 0", cp.Journal().Len())
	}
}

func TestCheckpointDurableWALRemainsAcceptedWhenLiveJournalIsFull(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	cp := New(&mockBackend{},
		WithJournalCap(1),
		WithFlushWorkers(0),
		WithSnapshotWAL(wal),
		WithSnapshotWALMode(SnapshotWALModeDurable),
		WithSnapshotWALDurableTimeout(time.Second),
		WithJournalSubmitTimeout(10*time.Millisecond),
	)
	if !cp.journal.Push([]SaveItem{{Collection: "players", ID: 1, Version: 1, Data: []byte("one")}}) {
		t.Fatal("initial journal push failed")
	}

	if ok := cp.Submit([]SaveItem{{Collection: "players", ID: 2, Version: 1, Data: []byte("two")}}); !ok {
		t.Fatal("durably admitted snapshot must remain accepted when the live journal is full")
	}
	if len(wal.durableSubmitted) != 1 {
		t.Fatalf("durable wal submit count = %d, want 1", len(wal.durableSubmitted))
	}
	if cp.Journal().Len() != 1 {
		t.Fatalf("journal len = %d, want 1", cp.Journal().Len())
	}
}

func TestCheckpointSubmitRemovePersistsDeleteTombstoneBeforeFlush(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	cp := New(&mockBackend{}, WithSnapshotWAL(wal), WithFlushWorkers(0))

	if ok := cp.SubmitRemove("players", []int64{1001, 1002}); !ok {
		t.Fatal("SubmitRemove returned false")
	}

	if len(wal.deleted) != 1 {
		t.Fatalf("wal delete batch count = %d, want 1", len(wal.deleted))
	}
	deleted := wal.deleted[0]
	if len(deleted) != 2 || deleted[0].Collection != "players" || deleted[0].ID != 1001 || deleted[1].ID != 1002 {
		t.Fatalf("wal delete tombstones = %+v", deleted)
	}
	if len(wal.acked) != 0 {
		t.Fatalf("delete WAL was acked before backend removal: %+v", wal.acked)
	}
}

func TestCheckpointSubmitRemoveItemsPreservesDbForBackendAndWAL(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	backend := &mockBackend{}
	cp := New(backend, WithSnapshotWAL(wal), WithFlushWorkers(0))

	if ok := cp.SubmitRemoveItems([]SaveItem{
		{Db: "game_1", Collection: "players", ID: 1001, Fence: 9, OwnerSid: 1001, Shared: true},
		{Db: "game_2", Collection: "players", ID: 1001},
	}); !ok {
		t.Fatal("SubmitRemoveItems returned false")
	}
	cp.journal.Close()
	if err := cp.flusher.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	if len(backend.removed) != 2 {
		t.Fatalf("removed ops = %+v, want two db-scoped remove ops", backend.removed)
	}
	if backend.removed[0].Db != "game_1" || backend.removed[1].Db != "game_2" {
		t.Fatalf("removed ops = %+v, want db game_1/game_2", backend.removed)
	}
	if op := backend.removed[0]; op.Fence != 9 || op.OwnerSid != 1001 || !op.Shared {
		t.Fatalf("fenced remove metadata was not forwarded: %+v", op)
	}
	acked := make([]SaveItem, 0, 2)
	for _, batch := range wal.acked {
		acked = append(acked, batch...)
	}
	if len(acked) != 2 || acked[0].Db != "game_1" || acked[1].Db != "game_2" {
		t.Fatalf("wal acked = %+v, want db game_1/game_2", acked)
	}
}

func TestFlusherDedupKeepsSameCollectionAndIDInDifferentDb(t *testing.T) {
	backend := &mockBackend{}
	cp := New(backend, WithFlushWorkers(0))
	items := []SaveItem{
		{Db: "game_1", Collection: "players", ID: 1001, Version: 1, Data: []byte("game1")},
		{Db: "game_2", Collection: "players", ID: 1001, Version: 1, Data: []byte("game2")},
	}
	if !cp.journal.Push(items) {
		t.Fatal("journal push failed")
	}
	cp.journal.Close()

	if err := cp.flusher.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	saved := backend.getSaved()
	if len(saved) != 2 {
		t.Fatalf("saved ops = %+v, want two db-scoped saves", saved)
	}
	if saved[0].Db != "game_1" || saved[1].Db != "game_2" {
		t.Fatalf("saved ops = %+v, want db game_1/game_2", saved)
	}
}

func TestFlusherAcksSnapshotWALAfterSuccessfulOrConflictedFlush(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	backend := &mockBackend{
		resultByCollection: map[string]SaveResult{
			"players": {OK: true},
			"tasks":   {VersionConflict: true},
			"bags":    {Err: errors.New("write failed")},
		},
	}
	cp := New(backend, WithSnapshotWAL(wal), WithFlushWorkers(0))
	items := []SaveItem{
		{Collection: "players", ID: 1001, Version: 1, Data: []byte("ok")},
		{Collection: "tasks", ID: 1001, Version: 1, Data: []byte("conflict")},
		{Collection: "bags", ID: 1001, Version: 1, Data: []byte("failed")},
	}
	if !cp.journal.Push(items) {
		t.Fatal("journal push failed")
	}
	cp.journal.Close()

	if err := cp.flusher.FlushAll(context.Background()); err == nil {
		t.Fatal("FlushAll must report the unpersisted bags item")
	}

	if len(wal.acked) != 1 {
		t.Fatalf("wal ack batch count = %d, want 1", len(wal.acked))
	}
	acked := wal.acked[0]
	if len(acked) != 2 {
		t.Fatalf("wal ack item count = %d, want 2", len(acked))
	}
	ackedCollections := map[string]bool{}
	for _, item := range acked {
		ackedCollections[item.Collection] = true
	}
	if !ackedCollections["players"] || !ackedCollections["tasks"] || ackedCollections["bags"] {
		t.Fatalf("wal acked items = %+v", acked)
	}
}

func TestCheckpoint_Dedup(t *testing.T) {
	backend := &mockBackend{}
	cp := New(backend,
		WithJournalCap(100),
		WithFlushWorkers(1),
		WithFlushInterval(10*time.Millisecond),
	)

	ctx := context.Background()
	cp.Start(ctx)

	// Submit same entity twice with different versions
	var d1, d2 DirtyTracker
	d1.MarkPersist(1)
	d1.IncVersion() // v=1
	m1 := d1.TakePersistDirty()

	d2.MarkPersist(1)
	d2.IncVersion()
	d2.IncVersion() // v=2
	m2 := d2.TakePersistDirty()

	cp.Submit([]SaveItem{
		{Collection: "players", ID: 100, Version: 1, Mask: m1, Data: []byte("old"), Tracker: &d1},
	})
	cp.Submit([]SaveItem{
		{Collection: "players", ID: 100, Version: 2, Mask: m2, Data: []byte("new"), Tracker: &d2},
	})

	time.Sleep(50 * time.Millisecond)
	_ = cp.Stop(ctx)

	saved := backend.getSaved()
	// Should have deduped to latest version
	if len(saved) != 1 {
		t.Fatalf("expected 1 saved op (deduped), got %d", len(saved))
	}
	if string(saved[0].Data) != "new" {
		t.Fatalf("expected 'new' data, got %q", saved[0].Data)
	}
	if saved[0].Version != 2 {
		t.Fatalf("expected version 2, got %d", saved[0].Version)
	}
}

func TestFlusherDedupSaveAfterRemoveRecreatesDocument(t *testing.T) {
	cp := New(&mockBackend{})
	saves, removes := cp.flusher.dedup([]JournalEntry{{Items: []SaveItem{
		{Db: "game", Collection: "players", ID: 100, Version: 2, Data: []byte("old")},
		{Db: "game", Collection: "players", ID: 100},
		{Db: "game", Collection: "players", ID: 100, Version: 3, Data: []byte("new")},
	}}})
	if len(removes) != 0 {
		t.Fatalf("save after remove must cancel tombstone, got %+v", removes)
	}
	if len(saves) != 1 || saves[0].Version != 3 || string(saves[0].Data) != "new" {
		t.Fatalf("unexpected final save: %+v", saves)
	}
}

func TestFreezeSaveItemsOwnsMutablePatchData(t *testing.T) {
	values := []int{1, 2}
	nested := map[string]any{"values": values}
	items := freezeSaveItems([]SaveItem{{
		Collection: "players", ID: 100, Version: 1, Mode: SaveModePatch,
		Data:  []byte("full"),
		Patch: PersistPatch{Set: map[string]any{"inventory": nested}, FullData: []byte("fallback")},
	}})
	values[0] = 99
	nested["late"] = true

	got := items[0].Patch.Set["inventory"].(map[string]any)
	gotValues := got["values"].([]int)
	if gotValues[0] != 1 {
		t.Fatalf("frozen patch aliases source slice: %+v", gotValues)
	}
	if _, exists := got["late"]; exists {
		t.Fatalf("frozen patch aliases source map: %+v", got)
	}
	items[0].Data[0] = 'F'
	if string(items[0].Patch.FullData) != "fallback" {
		t.Fatalf("patch full fallback aliases save data: %q", items[0].Patch.FullData)
	}
}

func TestCheckpointCannotRestartAfterStop(t *testing.T) {
	cp := New(&mockBackend{}, WithFlushWorkers(0))
	if err := cp.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := cp.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := cp.Start(context.Background()); !errors.Is(err, ErrCheckpointStopped) {
		t.Fatalf("restart error = %v, want %v", err, ErrCheckpointStopped)
	}
}

func TestCheckpoint_StopTimeoutRollsBackPending(t *testing.T) {
	backend := &mockBackend{}
	cp := New(backend, WithFlushWorkers(0))

	ctx := context.Background()
	cp.Start(ctx)

	var d DirtyTracker
	d.MarkPersist(1)
	mask := d.TakePersistDirty()
	cp.Submit([]SaveItem{
		{Collection: "players", ID: 100, Version: 1, Mask: mask, Data: []byte("p100"), Tracker: &d},
	})

	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cp.Stop(stopCtx); err == nil {
		t.Fatal("expected stop error")
	}
	if d.PersistDirtyMask() != mask {
		t.Fatalf("pending dirty should be rolled back, got %d", d.PersistDirtyMask())
	}
}

type fakeSnapshotWAL struct {
	submitted        [][]SaveItem
	durableSubmitted [][]SaveItem
	deleted          [][]SaveItem
	durableDeleted   [][]SaveItem
	acked            [][]SaveItem
	started          int
	stopped          int
	replayed         int
	rejectSubmit     bool
	rejectDurable    bool
}

func (w *fakeSnapshotWAL) Start() {
	w.started++
}

func (w *fakeSnapshotWAL) Stop(context.Context) error {
	w.stopped++
	return nil
}

func (w *fakeSnapshotWAL) Submit(items []SaveItem) bool {
	w.submitted = append(w.submitted, cloneSaveItemsForTest(items))
	return !w.rejectSubmit
}

func (w *fakeSnapshotWAL) SubmitDurable(_ context.Context, items []SaveItem) bool {
	w.durableSubmitted = append(w.durableSubmitted, cloneSaveItemsForTest(items))
	return !w.rejectDurable
}

func (w *fakeSnapshotWAL) SubmitDelete(items []SaveItem) bool {
	w.deleted = append(w.deleted, cloneSaveItemsForTest(items))
	return !w.rejectSubmit
}

func (w *fakeSnapshotWAL) SubmitDeleteDurable(_ context.Context, items []SaveItem) bool {
	w.durableDeleted = append(w.durableDeleted, cloneSaveItemsForTest(items))
	return !w.rejectDurable
}

func (w *fakeSnapshotWAL) Ack(_ context.Context, items []SaveItem) error {
	w.acked = append(w.acked, cloneSaveItemsForTest(items))
	return nil
}

func (w *fakeSnapshotWAL) Replay(context.Context, StorageBackend) error {
	w.replayed++
	return nil
}

func (w *fakeSnapshotWAL) Stats() SnapshotWALStats {
	return SnapshotWALStats{}
}

func cloneSaveItemsForTest(items []SaveItem) []SaveItem {
	out := make([]SaveItem, len(items))
	for i, item := range items {
		out[i] = item
		out[i].Data = append([]byte(nil), item.Data...)
	}
	return out
}

// --- Loader tests ---

type mockExister struct {
	ids map[int64]bool
}

func (m *mockExister) Exists(id int64) bool {
	return m.ids[id]
}

func TestLoader_Basic(t *testing.T) {
	backend := &mockBackend{
		loaded: []RawDoc{
			{ID: 1, Version: 5, Data: []byte("doc1")},
			{ID: 2, Version: 3, Data: []byte("doc2")},
			{ID: 3, Version: 1, Data: []byte("doc3")},
		},
	}

	exister := &mockExister{ids: map[int64]bool{2: true}}

	var loaded []int64
	var mu sync.Mutex

	templates := []LoadTemplate{
		{
			Collection: "players",
			OnLoad: func(doc RawDoc) error {
				mu.Lock()
				loaded = append(loaded, doc.ID)
				mu.Unlock()
				return nil
			},
		},
	}

	loader := NewLoader(backend, exister)
	err := loader.LoadAll(context.Background(), templates)
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded) != 2 {
		t.Fatalf("expected 2 loaded (skipping id=2), got %d", len(loaded))
	}
}

func TestLoader_Dependencies(t *testing.T) {
	backend := &mockBackend{
		loaded: []RawDoc{{ID: 1, Version: 1, Data: []byte("x")}},
	}

	var order []string
	var mu sync.Mutex

	templates := []LoadTemplate{
		{
			Collection: "alliances",
			DependsOn:  []string{"players"},
			OnLoad: func(doc RawDoc) error {
				mu.Lock()
				order = append(order, "alliances")
				mu.Unlock()
				return nil
			},
		},
		{
			Collection: "players",
			OnLoad: func(doc RawDoc) error {
				mu.Lock()
				order = append(order, "players")
				mu.Unlock()
				return nil
			},
		},
	}

	loader := NewLoader(backend, nil)
	err := loader.LoadAll(context.Background(), templates)
	if err != nil {
		t.Fatal(err)
	}

	if len(order) < 2 {
		t.Fatalf("expected 2 loads, got %d", len(order))
	}
	if order[0] != "players" {
		t.Fatalf("expected players first, got %s", order[0])
	}
}

func TestLoaderRejectsMissingBackendAndAcceptsNilContext(t *testing.T) {
	if err := NewLoader(nil, nil).LoadAll(nil, []LoadTemplate{{Collection: "players"}}); !errors.Is(err, ErrCheckpointBackendRequired) {
		t.Fatalf("missing backend error=%v", err)
	}
	backend := &mockBackend{}
	if err := NewLoader(backend, nil).LoadAll(nil, []LoadTemplate{{Collection: "players"}}); err != nil {
		t.Fatalf("nil context load: %v", err)
	}
}
