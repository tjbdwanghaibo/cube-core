package checkpoint

import "sync/atomic"

// DirtyScope describes what a dirty field needs.
type DirtyScope uint8

const (
	DirtyPersist DirtyScope = 1 << iota
	DirtySync
)

const DirtyAll uint64 = ^uint64(0)

// DirtyTracker tracks field-level dirty masks separately for persistence and
// cross-server sync. Snapshotting happens under entity lock, so no flushing mask
// is needed: persist failures mark the whole DAO dirty for the next low-frequency
// flush, while sync failures restore only the sampled sync mask.
type DirtyTracker struct {
	persistDirty atomic.Uint64
	syncDirty    atomic.Uint64
	version      atomic.Uint64
	syncVersion  atomic.Uint64
}

type DirtySnapshot struct {
	PersistDirty uint64
	SyncDirty    uint64
	Version      uint64
	SyncVersion  uint64
}

func (d *DirtyTracker) MarkScope(scope DirtyScope, mask uint64) {
	if mask == 0 {
		return
	}
	if scope&DirtyPersist != 0 {
		d.MarkPersist(mask)
	}
	if scope&DirtySync != 0 {
		d.MarkSync(mask)
	}
}

func (d *DirtyTracker) MarkPersist(mask uint64) {
	if mask != 0 {
		d.persistDirty.Or(mask)
	}
}

func (d *DirtyTracker) MarkSync(mask uint64) {
	if mask != 0 {
		d.syncDirty.Or(mask)
	}
}

func (d *DirtyTracker) PersistDirtyMask() uint64 {
	return d.persistDirty.Load()
}

func (d *DirtyTracker) SyncDirtyMask() uint64 {
	return d.syncDirty.Load()
}

func (d *DirtyTracker) HasPersistDirty() bool {
	return d.PersistDirtyMask() != 0
}

func (d *DirtyTracker) HasSyncDirty() bool {
	return d.SyncDirtyMask() != 0
}

func (d *DirtyTracker) TakePersistDirty() uint64 {
	return d.persistDirty.Swap(0)
}

func (d *DirtyTracker) TakeSyncDirty() uint64 {
	return d.syncDirty.Swap(0)
}

func (d *DirtyTracker) CommitPersist(_ uint64) {}

func (d *DirtyTracker) RollbackPersist(_ uint64) {
	d.persistDirty.Or(DirtyAll)
}

func (d *DirtyTracker) CommitSync(_ uint64) {}

func (d *DirtyTracker) RollbackSync(mask uint64) {
	d.MarkSync(mask)
}

// Dirty implements entity.IDirty.
func (d *DirtyTracker) Dirty() bool {
	return d.HasPersistDirty() || d.HasSyncDirty()
}

// SelfClean implements entity.IDirty and clears both dirty masks.
func (d *DirtyTracker) SelfClean() {
	d.persistDirty.Store(0)
	d.syncDirty.Store(0)
}

func (d *DirtyTracker) Version() uint64 {
	return d.version.Load()
}

func (d *DirtyTracker) IncVersion() uint64 {
	return d.version.Add(1)
}

func (d *DirtyTracker) SyncVersion() uint64 {
	return d.syncVersion.Load()
}

func (d *DirtyTracker) IncSyncVersion() uint64 {
	return d.syncVersion.Add(1)
}

func (d *DirtyTracker) SetVersion(v uint64) {
	d.version.Store(v)
	d.persistDirty.Store(0)
	d.syncDirty.Store(0)
}

func (d *DirtyTracker) Snapshot() DirtySnapshot {
	if d == nil {
		return DirtySnapshot{}
	}
	return DirtySnapshot{
		PersistDirty: d.persistDirty.Load(),
		SyncDirty:    d.syncDirty.Load(),
		Version:      d.version.Load(),
		SyncVersion:  d.syncVersion.Load(),
	}
}

func (d *DirtyTracker) Restore(snapshot DirtySnapshot) {
	if d == nil {
		return
	}
	d.persistDirty.Store(snapshot.PersistDirty)
	d.syncDirty.Store(snapshot.SyncDirty)
	d.version.Store(snapshot.Version)
	d.syncVersion.Store(snapshot.SyncVersion)
}
