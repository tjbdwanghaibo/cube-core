package entity

import (
	"context"
	"encoding/json"
)

// RemoteSyncItem is one sampled sync payload with the dirty-mask callbacks
// needed to finish or retry that sampled payload.
type RemoteSyncItem struct {
	Collection string
	Data       []byte
	Commit     func()
	Rollback   func()
}

// RemoteSyncSnapshot is a batch of sampled sync payloads. A single entity can
// own multiple DAOs, and each DAO may produce an independent sync payload.
type RemoteSyncSnapshot struct {
	Items []RemoteSyncItem
}

type RemoteSyncPayload struct {
	Collection string `json:"collection"`
	Data       []byte `json:"data"`
}

func EncodeRemoteSyncPayload(collection string, data []byte) ([]byte, error) {
	return json.Marshal(RemoteSyncPayload{
		Collection: collection,
		Data:       data,
	})
}

func DecodeRemoteSyncPayload(raw []byte) (RemoteSyncPayload, error) {
	var payload RemoteSyncPayload
	err := json.Unmarshal(raw, &payload)
	return payload, err
}

type RemoteSyncApplier interface {
	ApplyRemoteSync(collection string, data []byte, version int64) error
}

// IRemoteEntityLoader loads/saves remote entities from persistent storage.
// Application layer implements per entity category (Player, Alliance, etc.).
type IRemoteEntityLoader interface {
	// LoadRemoteEntity loads entity from DB, creates/merges into EntityManager.
	// Returns nil if entity does not exist.
	LoadRemoteEntity(id int64, kind EntityKind) IThreadSafeRemoteEntity
	// SaveRemoteEntity persists dirty entity state.
	SaveRemoteEntity(e IThreadSafeRemoteEntity) error
	// SnapshotRemoteEntitySync samples sync dirty fields and returns a payload
	// plus commit/rollback callbacks. It does not persist data.
	SnapshotRemoteEntitySync(e IThreadSafeRemoteEntity) RemoteSyncSnapshot
	// DelRemoteEntity deletes entity from DB.
	DelRemoteEntity(e IThreadSafeRemoteEntity) error
	// CheckEntityExist checks if entity exists in DB without loading.
	CheckEntityExist(id int64, kind EntityKind) bool
}

// IRemoteEntityMarkerStore provides remote entity mark persistence.
// A "mark" indicates entity is exclusively held by a server for remote access.
type IRemoteEntityMarkerStore interface {
	// IsMarked checks if entity is currently marked (reads from cache/Redis).
	IsMarked(ctx context.Context, id int64) (bool, error)
	// Mark stores mark for entity (called under dist lock protection).
	Mark(ctx context.Context, id int64) error
	// Unmark removes mark.
	Unmark(ctx context.Context, id int64) error
}

// IRemoteEntitySyncer broadcasts entity changes to other servers holding the entity.
type IRemoteEntitySyncer interface {
	// SyncEntity publishes entity data change to subscribers.
	SyncEntity(id int64, version int64, collection string, data []byte) error
	// SyncDelEntity publishes entity deletion to subscribers.
	SyncDelEntity(id int64, version int64) error
}

// IRemoteEntityManager is the top-level orchestrator for remote entity lifecycle.
// Composes WrapperManager + Loader + MarkerStore + Syncer.
type IRemoteEntityManager interface {
	IRemoteEntityWrapperManager

	// SetLoader sets application-layer entity loader.
	SetLoader(loader IRemoteEntityLoader)
	// SetMarkerStore sets the mark persistence backend.
	SetMarkerStore(store IRemoteEntityMarkerStore)
	// SetSyncer sets the cross-server sync backend (optional).
	SetSyncer(syncer IRemoteEntitySyncer)

	// Loader returns current loader (may be nil).
	Loader() IRemoteEntityLoader
	// MarkerStore returns current marker store (may be nil).
	MarkerStore() IRemoteEntityMarkerStore
	// Syncer returns current syncer (may be nil).
	Syncer() IRemoteEntitySyncer

	// ResolveRemoteSnapshot resolves a read-only/cache remote snapshot. It must
	// not expose mutable entity/component/DAO pointers to callers.
	ResolveRemoteSnapshot(req RemoteSnapshotResolveRequest) (RemoteSnapshot, error)
}
