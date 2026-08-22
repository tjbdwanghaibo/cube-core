package entity

import (
	"context"
	"errors"
)

var (
	// ErrRemotePersistenceIndeterminate means a remote save/delete returned an
	// error after the backend may already have accepted it. Callers must not
	// blindly replay the business command.
	ErrRemotePersistenceIndeterminate = errors.New("remote entity persistence outcome is indeterminate")
	// ErrRemoteReleaseIncomplete means state persistence completed but an
	// ownership/distributed guard could not be released normally.
	ErrRemoteReleaseIncomplete = errors.New("remote entity release is incomplete")
)

// RemoteEntityRelease closes guards acquired for one dispatch. It remains a
// small internal carrier used by Nest; persistence is owned by RemoteWriteBatch.
type RemoteEntityRelease func() error

// IRemoteEntityLoader materializes authoritative entities for the write path.
// Persistence is performed exclusively through IRemoteAtomicBatchCommitter;
// loaders must never expose an independent save/delete path.
type IRemoteEntityLoader interface {
	LoadRemoteEntity(context.Context, int64, EntityKind) (IThreadSafeRemoteEntity, error)
}

// IRemoteEntityLocalLookup is the zero-I/O fast path used while ownership or
// write gates are held. Implementations must never perform storage access.
type IRemoteEntityLocalLookup interface {
	LookupLocalRemoteEntity(int64, EntityKind) IThreadSafeRemoteEntity
}

// IRemoteEntityBackend is the complete authoritative capability set. Keeping
// it as one contract makes incomplete production wiring a compile-time error.
type IRemoteEntityBackend interface {
	IRemoteEntityLoader
	IRemoteAtomicBatchCommitter
	IRemoteSnapshotLoader
	IRemoteCommitOutbox
}

// IRemoteEntityOwnershipStore is the authoritative fenced ownership state.
// Absence is not local ownership: writers must first atomically claim it.
// Every state transition is a compare-and-swap so concurrent servers cannot
// create two owners or reuse an ownership generation.
type IRemoteEntityOwnershipStore interface {
	GetOwnership(ctx context.Context, id int64) (RemoteEntityMarkerLease, bool, error)
	ClaimOwnership(ctx context.Context, id int64, ownerSid int32) (RemoteEntityMarkerLease, error)
	EnterSharedExpected(ctx context.Context, id int64, expected RemoteEntityMarkerLease) (RemoteEntityMarkerLease, error)
	LeaveSharedExpected(ctx context.Context, id int64, expected RemoteEntityMarkerLease) (RemoteEntityMarkerLease, error)
	TransferExpected(ctx context.Context, id int64, expected RemoteEntityMarkerLease, newOwnerSid int32) (RemoteEntityMarkerLease, error)
}

// RemoteEntityMarkerLease identifies one ownership generation. Implementations
// should reject releases and writes made with an older fence.
type RemoteEntityMarkerLease struct {
	OwnerSid    int32
	MarkerEpoch uint64
	RouteEpoch  uint64
	Shared      bool
}

// IRemoteEntityManager is the single production Remote Entity contract. Writes
// are immutable transaction batches; reads are immutable scoped snapshots.
type IRemoteEntityManager interface {
	RemoteWriteBatchManager
	RemoteOwnershipManager
	RemoteSnapshotInterestManager
	RemoteSnapshotReader

	SetBackend(backend IRemoteEntityBackend)
	SetOwnershipStore(store IRemoteEntityOwnershipStore)
}
