package checkpoint

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/obs"
	"log/slog"
	"sync"
)

// Checkpoint is the main entry point for the save/load subsystem.
// It manages the Journal, Flusher, and Loader lifecycle.
type Checkpoint struct {
	cfg     Config
	journal *Journal
	flusher *Flusher
	loader  *Loader
	backend StorageBackend
	wal     SnapshotWAL

	mu      sync.Mutex
	running bool
}

type SnapshotWAL interface {
	Start()
	Stop(ctx context.Context) error
	Submit(items []SaveItem) bool
	Ack(ctx context.Context, items []SaveItem) error
	Replay(ctx context.Context, backend StorageBackend) error
	Stats() SnapshotWALStats
}

type DurableSnapshotWAL interface {
	SubmitDurable(ctx context.Context, items []SaveItem) bool
}

// New creates a Checkpoint instance.
func New(backend StorageBackend, opts ...Option) *Checkpoint {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	journal := NewJournal(cfg.JournalCap)

	return &Checkpoint{
		cfg:     cfg,
		journal: journal,
		backend: backend,
		wal:     cfg.SnapshotWAL,
		flusher: newFlusher(journal, backend, cfg, cfg.SnapshotWAL),
	}
}

// Start begins the flush workers.
func (c *Checkpoint) Start(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return
	}
	c.running = true
	if c.wal != nil {
		c.wal.Start()
	}
	c.flusher.Start(ctx)
	slog.Info("checkpoint started",
		"journal_cap", c.cfg.JournalCap,
		"flush_workers", c.cfg.FlushWorkers,
		"batch_size", c.cfg.BatchSize,
		"flush_interval", c.cfg.FlushInterval,
	)
}

// Stop gracefully shuts down: closes journal, flushes all pending data, waits for workers.
func (c *Checkpoint) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	c.mu.Unlock()

	slog.Info("checkpoint stopping, flushing remaining entries", "pending", c.journal.Len())

	// Close journal to prevent new pushes
	c.journal.Close()

	// Stop workers (they will exit on ctx cancel or journal close)
	if err := c.flusher.Stop(ctx); err != nil {
		c.flusher.RollbackPending()
		if c.wal != nil {
			_ = c.wal.Stop(ctx)
		}
		return err
	}

	// Drain remaining entries
	if err := c.flusher.FlushAll(ctx); err != nil {
		c.flusher.RollbackPending()
		if c.wal != nil {
			_ = c.wal.Stop(ctx)
		}
		return err
	}
	if c.wal != nil {
		if err := c.wal.Stop(ctx); err != nil {
			return err
		}
	}

	slog.Info("checkpoint stopped")
	return nil
}

// Submit pushes save items into the journal.
// Called from entity guard release (under entity lock).
// Blocks if journal is at capacity (back-pressure).
func (c *Checkpoint) Submit(items []SaveItem) bool {
	if c.wal != nil && c.cfg.SnapshotWALMode == SnapshotWALModeDurable {
		durable, ok := c.wal.(DurableSnapshotWAL)
		if !ok {
			slog.Warn("checkpoint: durable snapshot wal requested but wal does not support durable submission", "items", len(items))
			return false
		}
		ctx := context.Background()
		cancel := func() {}
		if c.cfg.SnapshotWALDurableTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, c.cfg.SnapshotWALDurableTimeout)
		}
		ok = durable.SubmitDurable(ctx, items)
		cancel()
		if !ok {
			slog.Warn("checkpoint: durable snapshot wal rejected save batch", "items", len(items))
			return false
		}
		ok = c.pushJournal(items)
		if !ok {
			slog.Warn("checkpoint: journal rejected save batch after durable wal accepted it", "items", len(items))
		}
		return ok
	}
	if c.wal != nil && c.cfg.SnapshotWALRequired {
		if !c.wal.Submit(items) {
			slog.Warn("checkpoint: required snapshot wal rejected save batch", "items", len(items))
			return false
		}
		ok := c.pushJournal(items)
		if !ok {
			slog.Warn("checkpoint: journal rejected save batch after required wal accepted it", "items", len(items))
		}
		return ok
	}
	ok := c.pushJournal(items)
	if ok && c.wal != nil {
		_ = c.wal.Submit(items)
	}
	return ok
}

// SubmitRemove queues a remove operation.
func (c *Checkpoint) SubmitRemove(collection string, ids []int64) bool {
	items := make([]SaveItem, 0, len(ids))
	for _, id := range ids {
		items = append(items, SaveItem{Collection: collection, ID: id})
	}
	return c.SubmitRemoveItems(items)
}

func (c *Checkpoint) SubmitRemoveItems(items []SaveItem) bool {
	ok := c.pushRemoveJournal(items)
	if ok && c.wal != nil && len(items) > 0 {
		_ = c.wal.Ack(context.Background(), items)
	}
	return ok
}

func (c *Checkpoint) pushJournal(items []SaveItem) bool {
	if c == nil || c.journal == nil {
		return false
	}
	if c.cfg.JournalSubmitTimeout <= 0 {
		return c.journal.Push(items)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.JournalSubmitTimeout)
	defer cancel()
	ok := c.journal.PushWithContext(ctx, items)
	if !ok && ctx.Err() != nil {
		obs.IncCounter("checkpoint_journal_submit_timeout_total", nil, 1)
	}
	return ok
}

func (c *Checkpoint) pushRemoveJournal(items []SaveItem) bool {
	if c == nil || c.journal == nil {
		return false
	}
	if c.cfg.JournalSubmitTimeout <= 0 {
		return c.journal.PushRemoveItems(items)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.JournalSubmitTimeout)
	defer cancel()
	ok := c.journal.PushWithContext(ctx, normalizeRemoveItems(items))
	if !ok && ctx.Err() != nil {
		obs.IncCounter("checkpoint_journal_submit_timeout_total", nil, 1)
	}
	return ok
}

func normalizeRemoveItems(items []SaveItem) []SaveItem {
	normalized := make([]SaveItem, 0, len(items))
	for _, item := range items {
		if item.Collection == "" || item.ID == 0 {
			continue
		}
		item.Version = 0
		item.Data = nil
		item.Patch = PersistPatch{}
		item.Mode = SaveModeFull
		normalized = append(normalized, item)
	}
	return normalized
}

// Load creates a loader and executes templates.
func (c *Checkpoint) Load(ctx context.Context, templates []LoadTemplate, exister EntityExister) error {
	loader := NewLoader(c.backend, exister)
	return loader.LoadAll(ctx, templates)
}

func (c *Checkpoint) ReplayWAL(ctx context.Context) error {
	if c == nil || c.wal == nil {
		return nil
	}
	return c.wal.Replay(ctx, c.backend)
}

// Journal returns the journal for direct access (e.g. metrics).
func (c *Checkpoint) Journal() *Journal {
	return c.journal
}

func (c *Checkpoint) JournalStats() JournalStats {
	if c == nil || c.journal == nil {
		return JournalStats{}
	}
	return c.journal.Stats()
}

// Backend returns the storage backend.
func (c *Checkpoint) Backend() StorageBackend {
	return c.backend
}

func (c *Checkpoint) SnapshotWALStats() SnapshotWALStats {
	if c == nil || c.wal == nil {
		return SnapshotWALStats{}
	}
	return c.wal.Stats()
}
