package replication

import "sync"

type SnapshotRing struct {
	mu       sync.RWMutex
	capacity int
	items    []Snapshot
	byTick   map[uint32]int
	latest   uint32
}

func NewSnapshotRing(capacity int) *SnapshotRing {
	if capacity <= 0 {
		capacity = 64
	}
	return &SnapshotRing{capacity: capacity, items: make([]Snapshot, 0, capacity), byTick: make(map[uint32]int, capacity)}
}

func (r *SnapshotRing) Add(snapshot Snapshot) error {
	if r == nil {
		return ErrInvalidFrame
	}
	if err := snapshot.SnapshotMeta.validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latest != 0 && snapshot.Tick <= r.latest {
		return ErrBaselineMismatch
	}
	if len(r.items) == r.capacity {
		delete(r.byTick, r.items[0].Tick)
		copy(r.items, r.items[1:])
		r.items = r.items[:len(r.items)-1]
		for tick, index := range r.byTick {
			r.byTick[tick] = index - 1
		}
	}
	r.items = append(r.items, snapshot.Clone())
	r.byTick[snapshot.Tick] = len(r.items) - 1
	r.latest = snapshot.Tick
	return nil
}

func (r *SnapshotRing) Get(tick uint32) (Snapshot, bool) {
	if r == nil || tick == 0 {
		return Snapshot{}, false
	}
	r.mu.RLock()
	index, ok := r.byTick[tick]
	if !ok || index < 0 || index >= len(r.items) {
		r.mu.RUnlock()
		return Snapshot{}, false
	}
	snapshot := r.items[index].Clone()
	r.mu.RUnlock()
	return snapshot, true
}

func (r *SnapshotRing) Latest() (Snapshot, bool) {
	if r == nil {
		return Snapshot{}, false
	}
	r.mu.RLock()
	if len(r.items) == 0 {
		r.mu.RUnlock()
		return Snapshot{}, false
	}
	snapshot := r.items[len(r.items)-1].Clone()
	r.mu.RUnlock()
	return snapshot, true
}

func (r *SnapshotRing) LatestTick() uint32 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.latest
}

func (r *SnapshotRing) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.items)
}

func (r *SnapshotRing) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.items = r.items[:0]
	clear(r.byTick)
	r.latest = 0
	r.mu.Unlock()
}
