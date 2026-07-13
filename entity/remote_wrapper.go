package entity

import "context"

// IRemoteEntityWrapper manages a single remote entity's distributed lifecycle.
// Each remote entity (identified by unique ID + category + kind) has one wrapper instance.
type IRemoteEntityWrapper interface {
	// TryCastEntity acquires lock (if marked) → loads/validates version → attaches entity.
	// Returns release func for: save → sync → unlock.
	// If entity is not marked, uses local fast path (no dist lock).
	TryCastEntity() (release func(), err error)

	// TryReadOnlyEntity loads or returns the authoritative entity for a single
	// read without acquiring the distributed write lock. The returned release is
	// a no-op and never persists dirty state.
	TryReadOnlyEntity(option RemoteReadOption) (IThreadSafeRemoteEntity, func(), error)

	// TryReadOnlySnapshot loads or returns an immutable snapshot for a single
	// authoritative read without acquiring the distributed write lock.
	TryReadOnlySnapshot(req RemoteSnapshotRequest) (RemoteSnapshot, error)

	// TryCachedSnapshot returns an immutable snapshot from the local cache/
	// materialization path. It never exposes a mutable entity pointer.
	TryCachedSnapshot(req RemoteSnapshotRequest) (RemoteSnapshot, error)

	// MarkRemote marks entity as remote-owned (acquires dist lock, writes mark,
	// saves) and returns the release function that must be called to unlock.
	MarkRemote(ctx context.Context) (release func(), err error)

	// UnmarkRemote clears remote mark (entity returns to local ownership).
	UnmarkRemote(ctx context.Context) error

	// IsMarked returns cached mark state (fast path, no Redis call).
	IsMarked() bool

	// SetMarked updates local cached mark state (called by sync handler).
	SetMarked(v bool)

	// EvictEntity removes local entity from memory (after unmark by remote owner).
	EvictEntity()

	// TryUpdateEntity applies sync data (version + payload) from another server.
	TryUpdateEntity(version int64, data []byte) error

	// TryDelEntity applies remote deletion notification.
	TryDelEntity() error

	// Entity returns the currently attached entity (may be nil).
	Entity() IThreadSafeRemoteEntity
}

// IRemoteEntityWrapperManager manages all RemoteEntityWrappers.
type IRemoteEntityWrapperManager interface {
	// GetOrCreate returns existing wrapper or creates one for (id, category, kind).
	GetOrCreate(id int64, category EntityCategory, kind EntityKind) IRemoteEntityWrapper

	// Get returns wrapper if exists, nil otherwise.
	Get(id int64) (IRemoteEntityWrapper, bool)

	// Remove removes and cleans up wrapper.
	Remove(id int64)

	// PrepareRemoteEntities is the batch entry point for nest dispatch.
	// Separates marked/non-marked, acquires locks in order, loads entities.
	// Returns aggregated release func for all entities.
	PrepareRemoteEntities(ids []int64) (release func(), err error)
}
