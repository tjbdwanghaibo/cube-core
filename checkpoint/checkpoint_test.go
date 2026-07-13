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
	if d.PersistDirtyMask() != DirtyAll {
		t.Fatal("should be dirty after rollback")
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
		{Collection: "players", ID: 100, Version: v1, Mask: m1, Data: []byte("p100"), Tracker: &d1},
		{Collection: "players", ID: 200, Version: v2, Mask: m2, Data: []byte("p200"), Tracker: &d2},
	})

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	_ = cp.Stop(ctx)

	saved := backend.getSaved()
	if len(saved) != 2 {
		t.Fatalf("expected 2 saved ops, got %d", len(saved))
	}

	// Trackers should be committed
	if d1.Dirty() {
		t.Fatal("d1 should not be dirty after successful flush")
	}
	if d2.Dirty() {
		t.Fatal("d2 should not be dirty after successful flush")
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

func TestCheckpointSubmitUsesJournalSubmitTimeoutAfterDurableWAL(t *testing.T) {
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

	if ok := cp.Submit([]SaveItem{{Collection: "players", ID: 2, Version: 1, Data: []byte("two")}}); ok {
		t.Fatal("Submit returned true when journal remained full past submit timeout")
	}
	if len(wal.durableSubmitted) != 1 {
		t.Fatalf("durable wal submit count = %d, want 1", len(wal.durableSubmitted))
	}
	if cp.Journal().Len() != 1 {
		t.Fatalf("journal len = %d, want 1", cp.Journal().Len())
	}
}

func TestCheckpointSubmitRemoveAcksSnapshotWAL(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	cp := New(&mockBackend{}, WithSnapshotWAL(wal), WithFlushWorkers(0))

	if ok := cp.SubmitRemove("players", []int64{1001, 1002}); !ok {
		t.Fatal("SubmitRemove returned false")
	}

	if len(wal.acked) != 1 {
		t.Fatalf("wal ack batch count = %d, want 1", len(wal.acked))
	}
	acked := wal.acked[0]
	if len(acked) != 2 || acked[0].Collection != "players" || acked[0].ID != 1001 || acked[1].ID != 1002 {
		t.Fatalf("wal acked removes = %+v", acked)
	}
}

func TestCheckpointSubmitRemoveItemsPreservesDbForBackendAndWAL(t *testing.T) {
	wal := &fakeSnapshotWAL{}
	backend := &mockBackend{}
	cp := New(backend, WithSnapshotWAL(wal), WithFlushWorkers(0))

	if ok := cp.SubmitRemoveItems([]SaveItem{
		{Db: "game_1", Collection: "players", ID: 1001},
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
	if len(wal.acked) == 0 || len(wal.acked[0]) != 2 {
		t.Fatalf("wal acked = %+v, want two db-scoped items", wal.acked)
	}
	if wal.acked[0][0].Db != "game_1" || wal.acked[0][1].Db != "game_2" {
		t.Fatalf("wal acked = %+v, want db game_1/game_2", wal.acked)
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

	if err := cp.flusher.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
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
	if d.PersistDirtyMask() != DirtyAll {
		t.Fatalf("pending dirty should be rolled back, got %d", d.PersistDirtyMask())
	}
}

type fakeSnapshotWAL struct {
	submitted        [][]SaveItem
	durableSubmitted [][]SaveItem
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
