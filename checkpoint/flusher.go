package checkpoint

import (
	"context"
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
}

func newFlusher(journal *Journal, backend StorageBackend, cfg Config, wal SnapshotWAL) *Flusher {
	return &Flusher{
		journal: journal,
		backend: backend,
		cfg:     cfg,
		wal:     wal,
	}
}

// Start launches flush workers.
func (f *Flusher) Start(ctx context.Context) {
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
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := f.journal.PopBatch(f.cfg.BatchSize)
		if len(batch) == 0 {
			return nil
		}
		if err := f.processBatch(ctx, batch); err != nil {
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
			batch := f.journal.PopBatch(f.cfg.BatchSize)
			if len(batch) == 0 {
				if f.journal.IsClosed() {
					return
				}
				continue
			}
			if err := f.processBatch(ctx, batch); err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("checkpoint process batch failed", "worker", id, "err", err)
			}
		}
	}
}

func (f *Flusher) processBatch(ctx context.Context, entries []JournalEntry) error {
	// Separate saves and removes, dedup saves by (collection, id) keeping latest version.
	saves, removes := f.dedup(entries)

	if len(saves) > 0 {
		if err := f.flushSaves(ctx, saves); err != nil {
			return err
		}
	}
	if len(removes) > 0 {
		if err := f.flushRemoves(ctx, removes); err != nil {
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
		db   string
		coll string
		id   int64
	}
	saveMap := make(map[key]SaveItem)
	removes = make(map[removeKey][]int64)

	for _, entry := range entries {
		for _, item := range entry.Items {
			if item.Version == 0 && item.Data == nil {
				// Remove operation
				rk := removeKey{db: item.Db, coll: item.Collection}
				removes[rk] = append(removes[rk], item.ID)
				// Also remove from saveMap if present
				delete(saveMap, key{item.Db, item.Collection, item.ID})
				continue
			}
			k := key{item.Db, item.Collection, item.ID}
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
		if saves[i].Collection != saves[j].Collection {
			return saves[i].Collection < saves[j].Collection
		}
		return saves[i].ID < saves[j].ID
	})
	return saves, removes
}

type removeKey struct {
	db   string
	coll string
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
			Collection: item.Collection,
			ID:         item.ID,
			Version:    item.Version,
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

		// Process results
		ackItems := make([]SaveItem, 0, len(results))
		for i, r := range results {
			tracker := items[i].Tracker
			if tracker == nil && len(items[i].targets) == 0 {
				if r.OK || r.VersionConflict {
					ackItems = append(ackItems, items[i])
				}
				continue
			}
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
		return nil
	}
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
		return keys[i].coll < keys[j].coll
	})
	for _, key := range keys {
		ids := removes[key]
		backoff := f.cfg.RetryBackoff
		for {
			err := f.backend.BulkRemove(ctx, RemoveOp{Db: key.db, Collection: key.coll, IDs: ids})
			if err == nil {
				break
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
