package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Flusher reads from Journal and writes to StorageBackend in batches.
type Flusher struct {
	journal *Journal
	backend StorageBackend
	cfg     Config
	wal     SnapshotWAL

	wg     sync.WaitGroup
	stopMu sync.Mutex
	stopCh chan struct{}
	cancel context.CancelFunc

	workMu   sync.Mutex
	inFlight int
	progress chan struct{}
}

func newFlusher(journal *Journal, backend StorageBackend, cfg Config, wal SnapshotWAL) *Flusher {
	return &Flusher{
		journal:  journal,
		backend:  backend,
		cfg:      cfg,
		wal:      wal,
		progress: make(chan struct{}, 1),
	}
}

// Start launches flush workers.
func (f *Flusher) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	f.stopMu.Lock()
	stopCh := make(chan struct{})
	f.stopCh = stopCh
	f.cancel = cancel
	f.stopMu.Unlock()
	for i := 0; i < f.cfg.FlushWorkers; i++ {
		f.wg.Add(1)
		go f.worker(runCtx, stopCh, i)
	}
}

// Stop signals workers to finish and waits.
func (f *Flusher) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	f.stopMu.Lock()
	if f.stopCh != nil {
		close(f.stopCh)
		f.stopCh = nil
	}
	cancel := f.cancel
	f.stopMu.Unlock()

	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

// FlushAll drains the journal completely. Called during graceful shutdown.
func (f *Flusher) FlushAll(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := f.takeBatch()
		if len(batch) == 0 {
			f.workMu.Lock()
			idle := f.inFlight == 0 && f.journal.Len() == 0
			f.workMu.Unlock()
			if idle {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-f.progress:
			}
			continue
		}
		err := f.processBatch(ctx, batch)
		f.finishBatch()
		if err != nil {
			return err
		}
	}
}

func (f *Flusher) RollbackPending() {
	rollbackJournalEntries(f.journal.DrainAll())
}

func (f *Flusher) worker(ctx context.Context, stopCh <-chan struct{}, id int) {
	defer f.wg.Done()

	ticker := time.NewTicker(f.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			batch := f.takeBatch()
			if len(batch) == 0 {
				if f.journal.IsClosed() {
					return
				}
				continue
			}
			if err := f.processBatch(ctx, batch); err != nil {
				f.finishBatch()
				if ctx.Err() != nil {
					return
				}
				slog.Error("checkpoint process batch failed", "worker", id, "err", err)
				continue
			}
			f.finishBatch()
		}
	}
}

func (f *Flusher) takeBatch() []JournalEntry {
	f.workMu.Lock()
	defer f.workMu.Unlock()
	batch := f.journal.TryPopBatch(f.cfg.BatchSize)
	if len(batch) > 0 {
		f.inFlight++
	}
	return batch
}

func (f *Flusher) finishBatch() {
	f.workMu.Lock()
	if f.inFlight > 0 {
		f.inFlight--
	}
	f.workMu.Unlock()
	select {
	case f.progress <- struct{}{}:
	default:
	}
}

func (f *Flusher) processBatch(ctx context.Context, entries []JournalEntry) error {
	// Separate saves and removes, dedup saves by (collection, id) keeping latest version.
	saves, removes := f.dedup(entries)

	if len(saves) > 0 {
		if err := f.flushSaves(ctx, saves); err != nil {
			if !f.journal.PushFront(saveJournalEntries(saves)) {
				slog.Error("checkpoint requeue saves failed", "err", err)
			}
			return err
		}
	}
	if len(removes) > 0 {
		if err := f.flushRemoves(ctx, removes); err != nil {
			if !f.journal.PushFront(removeJournalEntries(removes)) {
				slog.Error("checkpoint requeue removes failed", "err", err)
			}
			return err
		}
	}
	return nil
}

// dedupEntry holds a SaveItem with its deduplication key.
type dedupEntry struct {
	item SaveItem
}

func (f *Flusher) dedup(entries []JournalEntry) (saves []SaveItem, removes map[removeKey][]int64) {
	// Dedup saves: merge patches for the same (collection, id). A later full
	// snapshot replaces earlier patches. We only commit dirty masks after the
	// merged write succeeds, so a failed flush can roll back to a full save.
	type key struct {
		db      string
		dbScope DatabaseScope
		coll    string
		id      int64
	}
	saveMap := make(map[key]SaveItem)
	removes = make(map[removeKey][]int64)
	removeIndex := make(map[key]removeKey)

	for _, entry := range entries {
		for _, item := range entry.Items {
			if item.Version == 0 && item.Data == nil {
				// Remove operation
				rk := removeKey{db: item.Db, dbScope: item.DbScope, coll: item.Collection, fence: item.Fence, ownerSid: item.OwnerSid, shared: item.Shared}
				removes[rk] = append(removes[rk], item.ID)
				// Also remove from saveMap if present
				delete(saveMap, key{item.Db, item.DbScope, item.Collection, item.ID})
				removeIndex[key{item.Db, item.DbScope, item.Collection, item.ID}] = rk
				continue
			}
			k := key{item.Db, item.DbScope, item.Collection, item.ID}
			// Last operation wins. A save after a tombstone recreates the
			// document and must cancel the earlier remove from this batch.
			if rk, ok := removeIndex[k]; ok {
				removes[rk] = removeID(removes[rk], item.ID)
				if len(removes[rk]) == 0 {
					delete(removes, rk)
				}
				delete(removeIndex, k)
			}
			if existing, ok := saveMap[k]; ok {
				saveMap[k] = mergeSaveItem(existing, item)
			} else {
				saveMap[k] = item
			}
		}
	}

	saves = make([]SaveItem, 0, len(saveMap))
	for _, item := range saveMap {
		saves = append(saves, item)
	}
	sort.Slice(saves, func(i, j int) bool {
		if saves[i].Db != saves[j].Db {
			return saves[i].Db < saves[j].Db
		}
		if saves[i].DbScope != saves[j].DbScope {
			return saves[i].DbScope < saves[j].DbScope
		}
		if saves[i].Collection != saves[j].Collection {
			return saves[i].Collection < saves[j].Collection
		}
		return saves[i].ID < saves[j].ID
	})
	return saves, removes
}

func removeID(ids []int64, target int64) []int64 {
	for i := range ids {
		if ids[i] == target {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}

type removeKey struct {
	db       string
	dbScope  DatabaseScope
	coll     string
	fence    uint64
	ownerSid int32
	shared   bool
}

func mergeSaveItem(existing SaveItem, next SaveItem) SaveItem {
	if next.Version >= existing.Version {
		next.targets = mergeSaveTargets(existing, next)
		next.Mask |= existing.Mask
		if next.Tracker == nil {
			next.Tracker = existing.Tracker
		}
		if next.Mode == SaveModePatch && existing.Mode == SaveModePatch {
			next.Patch = existing.Patch.Merge(next.Patch)
		}
		if len(next.Data) == 0 {
			next.Data = existing.Data
		}
		return next
	}

	existing.targets = mergeSaveTargets(existing, next)
	existing.Mask |= next.Mask
	if existing.Mode == SaveModePatch && next.Mode == SaveModePatch {
		existing.Patch = next.Patch.Merge(existing.Patch)
	}
	if len(existing.Data) == 0 {
		existing.Data = next.Data
	}
	return existing
}

func mergeSaveTargets(existing SaveItem, next SaveItem) []saveTarget {
	targets := make([]saveTarget, 0, len(existing.targets)+len(next.targets)+2)
	targets = appendSaveTargets(targets, existing)
	targets = appendSaveTargets(targets, next)
	return targets
}

func appendSaveTargets(targets []saveTarget, item SaveItem) []saveTarget {
	if len(item.targets) > 0 {
		return append(targets, item.targets...)
	}
	if item.Tracker != nil && item.Mask != 0 {
		return append(targets, saveTarget{Tracker: item.Tracker, Mask: item.Mask})
	}
	return targets
}

func (f *Flusher) flushSaves(ctx context.Context, items []SaveItem) error {
	// Split into batches by size
	for start := 0; start < len(items); {
		end := start
		batchBytes := 0
		for end < len(items) && (end-start) < f.cfg.BatchSize && batchBytes < f.cfg.BatchBytes {
			batchBytes += saveItemSizeHint(items[end])
			end++
		}
		if end == start {
			end = start + 1 // at least one item
		}
		if err := f.flushSaveBatch(ctx, items[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func saveItemSizeHint(item SaveItem) int {
	n := len(item.Data)
	if item.Mode == SaveModePatch {
		n += item.Patch.SizeHint()
	}
	return n
}

func (f *Flusher) flushSaveBatch(ctx context.Context, items []SaveItem) error {
	ops := make([]SaveOp, len(items))
	for i, item := range items {
		ops[i] = SaveOp{
			Db:         item.Db,
			DbScope:    item.DbScope,
			Collection: item.Collection,
			ID:         item.ID,
			Version:    item.Version,
			Fence:      item.Fence,
			OwnerSid:   item.OwnerSid,
			Shared:     item.Shared,
			Mask:       item.Mask,
			Mode:       item.Mode,
			Data:       item.Data,
			Patch:      item.Patch,
		}
	}

	backoff := f.cfg.RetryBackoff
	for {
		results, err := f.backend.BulkSave(ctx, ops)
		if err != nil {
			if ctx.Err() != nil {
				rollbackPersistItems(items)
				return ctx.Err()
			}
			slog.Error("checkpoint flush save error, retrying", "err", err, "backoff", backoff)
			if err := waitRetry(ctx, backoff); err != nil {
				rollbackPersistItems(items)
				return err
			}
			backoff = min(backoff*2, f.cfg.RetryMaxBack)
			continue
		}
		if len(results) != len(items) {
			rollbackPersistItems(items)
			return fmt.Errorf("checkpoint backend returned %d save results for %d items", len(results), len(items))
		}

		// Process results
		ackItems := make([]SaveItem, 0, len(results))
		var itemFailure error
		for i, r := range results {
			if r.OK {
				commitPersistItem(items[i])
				ackItems = append(ackItems, items[i])
			} else if r.VersionConflict {
				// Stale version, discard — newer version will be saved
				commitPersistItem(items[i])
				ackItems = append(ackItems, items[i])
				slog.Debug("checkpoint version conflict, discarded",
					"coll", items[i].Collection, "id", items[i].ID,
					"ver", items[i].Version)
			} else {
				rollbackPersistItem(items[i])
				failure := r.Err
				if failure == nil {
					failure = fmt.Errorf("backend rejected save")
				}
				itemFailure = errors.Join(itemFailure, fmt.Errorf("%s/%d: %w", items[i].Collection, items[i].ID, failure))
				slog.Warn("checkpoint save item failed",
					"coll", items[i].Collection, "id", items[i].ID,
					"err", r.Err)
			}
		}
		if f.wal != nil && len(ackItems) > 0 {
			if err := f.wal.Ack(ctx, ackItems); err != nil {
				slog.Warn("checkpoint redis wal ack failed", "err", err, "items", len(ackItems))
			}
		}
		return itemFailure
	}
}

func removeJournalEntries(removes map[removeKey][]int64) []JournalEntry {
	entries := make([]JournalEntry, 0, len(removes))
	for key, ids := range removes {
		items := make([]SaveItem, 0, len(ids))
		for _, id := range ids {
			items = append(items, SaveItem{
				Db: key.db, DbScope: key.dbScope, Collection: key.coll, ID: id,
				Fence: key.fence, OwnerSid: key.ownerSid, Shared: key.shared,
			})
		}
		entries = append(entries, JournalEntry{Items: items, PushAt: time.Now().UnixNano()})
	}
	return entries
}

func saveJournalEntries(items []SaveItem) []JournalEntry {
	entries := make([]JournalEntry, 0, len(items))
	for _, item := range items {
		entries = append(entries, JournalEntry{
			Items:  []SaveItem{item},
			PushAt: time.Now().UnixNano(),
		})
	}
	return entries
}

func (f *Flusher) flushRemoves(ctx context.Context, removes map[removeKey][]int64) error {
	keys := make([]removeKey, 0, len(removes))
	for key := range removes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].db != keys[j].db {
			return keys[i].db < keys[j].db
		}
		if keys[i].dbScope != keys[j].dbScope {
			return keys[i].dbScope < keys[j].dbScope
		}
		return keys[i].coll < keys[j].coll
	})
	for _, key := range keys {
		ids := removes[key]
		backoff := f.cfg.RetryBackoff
		for {
			err := f.backend.BulkRemove(ctx, RemoveOp{Db: key.db, DbScope: key.dbScope, Collection: key.coll, IDs: ids, Fence: key.fence, OwnerSid: key.ownerSid, Shared: key.shared})
			if err == nil {
				if f.wal != nil {
					ackItems := make([]SaveItem, 0, len(ids))
					for _, id := range ids {
						ackItems = append(ackItems, SaveItem{Db: key.db, DbScope: key.dbScope, Collection: key.coll, ID: id, Fence: key.fence, OwnerSid: key.ownerSid, Shared: key.shared})
					}
					if ackErr := f.wal.Ack(ctx, ackItems); ackErr != nil {
						err = fmt.Errorf("checkpoint delete WAL ack: %w", ackErr)
					} else {
						break
					}
				} else {
					break
				}
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("checkpoint flush remove error, retrying",
				"db", key.db, "coll", key.coll, "err", err, "backoff", backoff)
			if err := waitRetry(ctx, backoff); err != nil {
				return err
			}
			backoff = min(backoff*2, f.cfg.RetryMaxBack)
		}
	}
	return nil
}

func rollbackPersistItems(items []SaveItem) {
	for _, item := range items {
		rollbackPersistItem(item)
	}
}

func rollbackJournalEntries(entries []JournalEntry) {
	for _, entry := range entries {
		for _, item := range entry.Items {
			if item.Version == 0 && item.Data == nil {
				continue
			}
			rollbackPersistItem(item)
		}
	}
}

func commitPersistItem(item SaveItem) {
	if len(item.targets) > 0 {
		for _, target := range item.targets {
			if target.Tracker != nil {
				target.Tracker.CommitPersist(target.Mask)
			}
		}
		return
	}
	if item.Tracker != nil {
		item.Tracker.CommitPersist(item.Mask)
	}
}

func rollbackPersistItem(item SaveItem) {
	if len(item.targets) > 0 {
		for _, target := range item.targets {
			if target.Tracker != nil {
				target.Tracker.RollbackPersist(target.Mask)
			}
		}
		return
	}
	if item.Tracker != nil {
		item.Tracker.RollbackPersist(item.Mask)
	}
}

func waitRetry(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
