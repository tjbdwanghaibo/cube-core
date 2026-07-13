package checkpoint

import (
	"context"
	"sync"
	"time"
)

// SaveItem is a single DAO snapshot produced by entity guard release.
type SaveItem struct {
	Db         string // database name
	Collection string
	ID         int64
	Version    uint64 // version after IncVersion
	Mask       uint64 // field-level dirty mask sampled under entity lock
	Mode       SaveMode
	Data       []byte       // full serialized data
	Patch      PersistPatch // field-level persistence update
	Tracker    *DirtyTracker
	targets    []saveTarget
}

type saveTarget struct {
	Tracker *DirtyTracker
	Mask    uint64
}

// JournalEntry groups SaveItems from one guard release.
type JournalEntry struct {
	Items  []SaveItem
	PushAt int64 // UnixNano timestamp
}

// Journal is a bounded FIFO queue for save snapshots.
// When full, Push blocks (back-pressure to nest worker).
type Journal struct {
	mu       sync.Mutex
	cond     *sync.Cond
	entries  []JournalEntry
	closed   bool
	cap      int
	popReady *sync.Cond // signal for flusher
}

type JournalStats struct {
	Len       int
	Cap       int
	FillRatio float64
	Closed    bool
}

// NewJournal creates a journal with given capacity.
func NewJournal(cap int) *Journal {
	if cap <= 0 {
		cap = 10000
	}
	j := &Journal{
		entries: make([]JournalEntry, 0, cap),
		cap:     cap,
	}
	j.cond = sync.NewCond(&j.mu)
	j.popReady = sync.NewCond(&j.mu)
	return j
}

// Push adds a snapshot entry. Blocks if journal is at capacity (back-pressure).
// Returns false if journal is closed.
func (j *Journal) Push(items []SaveItem) bool {
	return j.PushWithContext(context.Background(), items)
}

func (j *Journal) PushWithContext(ctx context.Context, items []SaveItem) bool {
	if len(items) == 0 {
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	stopWake := j.wakeOnContextDoneLocked(ctx)
	if stopWake != nil {
		defer close(stopWake)
	}

	// Wait while at capacity
	for len(j.entries) >= j.cap && !j.closed {
		if ctx.Err() != nil {
			return false
		}
		j.cond.Wait()
	}
	if j.closed {
		return false
	}

	j.entries = append(j.entries, JournalEntry{
		Items:  items,
		PushAt: time.Now().UnixNano(),
	})

	// Signal flusher that data is available
	j.popReady.Signal()
	return true
}

func (j *Journal) wakeOnContextDoneLocked(ctx context.Context) chan struct{} {
	done := ctx.Done()
	if done == nil {
		return nil
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-done:
			j.mu.Lock()
			j.cond.Broadcast()
			j.mu.Unlock()
		case <-stop:
		}
	}()
	return stop
}

// PushRemove adds a remove operation as a special entry with nil Data.
func (j *Journal) PushRemove(collection string, ids []int64) bool {
	if len(ids) == 0 {
		return true
	}

	items := make([]SaveItem, len(ids))
	for i, id := range ids {
		items[i] = SaveItem{
			Collection: collection,
			ID:         id,
			Version:    0, // version 0 signals removal
		}
	}
	return j.PushRemoveItems(items)
}

func (j *Journal) PushRemoveItems(items []SaveItem) bool {
	if len(items) == 0 {
		return true
	}
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
	return j.Push(normalized)
}

// PopBatch retrieves up to max entries. Blocks until data available or closed.
// Returns nil if closed and empty.
func (j *Journal) PopBatch(max int) []JournalEntry {
	j.mu.Lock()
	defer j.mu.Unlock()

	for len(j.entries) == 0 && !j.closed {
		j.popReady.Wait()
	}

	if len(j.entries) == 0 {
		return nil
	}

	n := len(j.entries)
	if n > max {
		n = max
	}

	batch := make([]JournalEntry, n)
	copy(batch, j.entries[:n])
	j.entries = j.entries[n:]

	// Signal producers that space is available
	j.cond.Broadcast()
	return batch
}

func (j *Journal) DrainAll() []JournalEntry {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.entries) == 0 {
		return nil
	}
	entries := make([]JournalEntry, len(j.entries))
	copy(entries, j.entries)
	j.entries = nil
	j.cond.Broadcast()
	return entries
}

// Len returns current journal size.
func (j *Journal) Len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.entries)
}

func (j *Journal) Stats() JournalStats {
	j.mu.Lock()
	defer j.mu.Unlock()
	stats := JournalStats{
		Len:    len(j.entries),
		Cap:    j.cap,
		Closed: j.closed,
	}
	if j.cap > 0 {
		stats.FillRatio = float64(len(j.entries)) / float64(j.cap)
	}
	return stats
}

// Close marks the journal as closed. No more pushes allowed.
// Wakes up all waiters.
func (j *Journal) Close() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.closed = true
	j.cond.Broadcast()
	j.popReady.Broadcast()
}

// IsClosed returns whether the journal has been closed.
func (j *Journal) IsClosed() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.closed
}
