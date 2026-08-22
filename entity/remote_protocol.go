package entity

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"time"
)

var (
	ErrRemoteRejected                = errors.New("remote entity: write rejected before commit")
	ErrRemoteOverloaded              = errors.New("remote entity: capacity exceeded")
	ErrRemoteFenced                  = errors.New("remote entity: writer fenced")
	ErrRemoteOwnerTransition         = errors.New("remote entity: owner transition in progress")
	ErrRemoteVersionConflict         = errors.New("remote entity: state version conflict")
	ErrRemoteCommitTimeout           = errors.New("remote entity: commit timed out")
	ErrRemoteAtomicBatchUnsupported  = errors.New("remote entity: atomic batch unsupported")
	ErrRemoteWriteCapabilityDisabled = errors.New("remote entity: write capability disabled")
	ErrRemoteInvalidStateTransition  = errors.New("remote entity: invalid ownership transition")
	ErrRemoteCommitNotFinalized      = errors.New("remote entity: commit not finalized")
)

type RemoteOwnershipState uint8

const (
	RemoteOwnershipUnknown RemoteOwnershipState = iota
	RemoteOwnershipLocalOwned
	RemoteOwnershipSharing
	RemoteOwnershipShared
	RemoteOwnershipDraining
	RemoteOwnershipFenced
	RemoteOwnershipRecovering
	RemoteOwnershipQuarantined
)

func (s RemoteOwnershipState) String() string {
	switch s {
	case RemoteOwnershipLocalOwned:
		return "local_owned"
	case RemoteOwnershipSharing:
		return "sharing"
	case RemoteOwnershipShared:
		return "shared"
	case RemoteOwnershipDraining:
		return "draining"
	case RemoteOwnershipFenced:
		return "fenced"
	case RemoteOwnershipRecovering:
		return "recovering"
	case RemoteOwnershipQuarantined:
		return "quarantined"
	default:
		return "unknown"
	}
}

func ValidRemoteOwnershipTransition(from, to RemoteOwnershipState) bool {
	if from == to {
		return true
	}
	switch from {
	case RemoteOwnershipUnknown:
		return to == RemoteOwnershipRecovering || to == RemoteOwnershipLocalOwned || to == RemoteOwnershipShared
	case RemoteOwnershipLocalOwned:
		return to == RemoteOwnershipSharing || to == RemoteOwnershipDraining || to == RemoteOwnershipFenced
	case RemoteOwnershipSharing:
		return to == RemoteOwnershipShared || to == RemoteOwnershipLocalOwned || to == RemoteOwnershipFenced
	case RemoteOwnershipShared:
		return to == RemoteOwnershipDraining || to == RemoteOwnershipFenced
	case RemoteOwnershipDraining:
		return to == RemoteOwnershipLocalOwned || to == RemoteOwnershipShared || to == RemoteOwnershipFenced || to == RemoteOwnershipRecovering
	case RemoteOwnershipFenced:
		return to == RemoteOwnershipRecovering
	case RemoteOwnershipRecovering:
		return to == RemoteOwnershipLocalOwned || to == RemoteOwnershipShared || to == RemoteOwnershipQuarantined || to == RemoteOwnershipFenced
	case RemoteOwnershipQuarantined:
		return to == RemoteOwnershipRecovering
	default:
		return false
	}
}

type RemoteVersionVector struct {
	StateVersion uint64
	MarkerEpoch  uint64
	LockFence    uint64
	RouteEpoch   uint64
}

func (v RemoteVersionVector) ValidateWrite(base RemoteVersionVector) error {
	if v.MarkerEpoch != base.MarkerEpoch || v.RouteEpoch != base.RouteEpoch || v.LockFence < base.LockFence {
		return ErrRemoteFenced
	}
	if v.StateVersion != base.StateVersion {
		return ErrRemoteVersionConflict
	}
	return nil
}

type RemoteWriteMode uint8

const (
	RemoteWriteOwnerRouted RemoteWriteMode = iota + 1
	RemoteWriteSharedLock
)

type RemoteWriteLease struct {
	EntityID    int64
	OwnerSID    int32
	Mode        RemoteWriteMode
	State       RemoteOwnershipState
	BaseVersion uint64
	MarkerEpoch uint64
	LockFence   uint64
	RouteEpoch  uint64
	AcquiredAt  int64
	ExpiresAt   int64
}

func (l RemoteWriteLease) Valid() bool {
	if l.EntityID == 0 || l.MarkerEpoch == 0 || l.RouteEpoch == 0 {
		return false
	}
	if l.Mode == RemoteWriteSharedLock && l.LockFence == 0 {
		return false
	}
	return l.State == RemoteOwnershipLocalOwned || l.State == RemoteOwnershipShared
}

type RemoteTransactionID [16]byte

func (id RemoteTransactionID) IsZero() bool   { return id == RemoteTransactionID{} }
func (id RemoteTransactionID) String() string { return fmt.Sprintf("%x", id[:]) }

type RemoteTransactionOutcome struct {
	TransactionID RemoteTransactionID
	Handler       string
	RequestID     string
	Succeeded     bool
	Durability    uint8
	FinalizedAt   int64
}

type RemoteSnapshotRecord struct {
	Key          RemoteSnapshotKey
	BaseVersion  uint64
	StateVersion uint64
	MarkerEpoch  uint64
	RouteEpoch   uint64
	Schema       uint32
	Codec        uint16
	Full         bool
	Data         []byte
	Checksum     uint64
}

func (r RemoteSnapshotRecord) Clone() RemoteSnapshotRecord {
	r.Data = append([]byte(nil), r.Data...)
	return r
}

// RemoteDataMutation is a frozen full-document write. It intentionally carries
// no live DAO pointer and no interface-typed patch values, so WAL records are
// deterministic and can be replayed by another process or implementation.
type RemoteDataMutation struct {
	Database      string
	DatabaseScope uint8
	Collection    string
	ID            int64
	Version       uint64
	Mask          uint64
	Data          []byte
}

type RemoteDataDelete struct {
	Database      string
	DatabaseScope uint8
	Collection    string
	ID            int64
}

func (d RemoteDataDelete) Validate() error {
	if d.Collection == "" || d.ID == 0 {
		return fmt.Errorf("%w: invalid data delete", ErrRemoteRejected)
	}
	return nil
}

func (m RemoteDataMutation) Clone() RemoteDataMutation {
	m.Data = append([]byte(nil), m.Data...)
	return m
}

func (m RemoteDataMutation) Validate() error {
	if m.Collection == "" || m.ID == 0 || m.Version == 0 || len(m.Data) == 0 {
		return fmt.Errorf("%w: invalid data mutation", ErrRemoteRejected)
	}
	return nil
}

// RemoteCommit is immutable after FinalizeLocked returns. Implementations must
// not retain aliases to live entity/component/DAO memory.
type RemoteCommit struct {
	TransactionID RemoteTransactionID
	EntityID      int64
	Kind          EntityKind
	Delete        bool
	BaseVersion   uint64
	NextVersion   uint64
	MarkerEpoch   uint64
	LockFence     uint64
	RouteEpoch    uint64
	Schema        uint32
	Codec         uint16
	Mutations     []RemoteDataMutation
	Deletes       []RemoteDataDelete
	Snapshots     []RemoteSnapshotRecord
	Invalidations []RemoteSnapshotKey
	Checksum      uint64
}

func (c RemoteCommit) Clone() RemoteCommit {
	c.Mutations = slices.Clone(c.Mutations)
	for i := range c.Mutations {
		c.Mutations[i] = c.Mutations[i].Clone()
	}
	c.Snapshots = slices.Clone(c.Snapshots)
	c.Deletes = slices.Clone(c.Deletes)
	for i := range c.Snapshots {
		c.Snapshots[i] = c.Snapshots[i].Clone()
	}
	c.Invalidations = slices.Clone(c.Invalidations)
	return c
}

func (c RemoteCommit) Validate() error {
	if c.TransactionID.IsZero() || c.EntityID == 0 || c.Kind == EntityKindNone {
		return fmt.Errorf("%w: invalid identity", ErrRemoteRejected)
	}
	if c.NextVersion != c.BaseVersion+1 {
		return fmt.Errorf("%w: base=%d next=%d", ErrRemoteVersionConflict, c.BaseVersion, c.NextVersion)
	}
	if c.MarkerEpoch == 0 || c.RouteEpoch == 0 {
		return fmt.Errorf("%w: missing ownership epoch", ErrRemoteFenced)
	}
	if len(c.Mutations) == 0 && !c.Delete {
		return fmt.Errorf("%w: empty mutation set", ErrRemoteRejected)
	}
	if c.Delete && (len(c.Mutations) != 0 || len(c.Deletes) == 0) {
		return fmt.Errorf("%w: delete commit contains data mutations", ErrRemoteRejected)
	}
	if !c.Delete && len(c.Deletes) != 0 {
		return fmt.Errorf("%w: update commit contains data deletes", ErrRemoteRejected)
	}
	mutationKeys := make(map[string]struct{}, len(c.Mutations))
	for i := range c.Mutations {
		if err := c.Mutations[i].Validate(); err != nil {
			return fmt.Errorf("remote entity: mutation %d: %w", i, err)
		}
		if c.Mutations[i].ID != c.EntityID {
			return fmt.Errorf("%w: mutation identity mismatch", ErrRemoteRejected)
		}
		if c.Mutations[i].Version != c.NextVersion {
			return fmt.Errorf("%w: mutation version mismatch", ErrRemoteVersionConflict)
		}
		key := fmt.Sprintf("%s\x00%d\x00%s\x00%d", c.Mutations[i].Database, c.Mutations[i].DatabaseScope, c.Mutations[i].Collection, c.Mutations[i].ID)
		if _, exists := mutationKeys[key]; exists {
			return fmt.Errorf("%w: duplicate data mutation", ErrRemoteRejected)
		}
		mutationKeys[key] = struct{}{}
	}
	for i := range c.Deletes {
		if err := c.Deletes[i].Validate(); err != nil || c.Deletes[i].ID != c.EntityID {
			return fmt.Errorf("remote entity: delete %d: %w", i, errors.Join(err, ErrRemoteRejected))
		}
		key := fmt.Sprintf("%s\x00%d\x00%s\x00%d", c.Deletes[i].Database, c.Deletes[i].DatabaseScope, c.Deletes[i].Collection, c.Deletes[i].ID)
		if _, exists := mutationKeys[key]; exists {
			return fmt.Errorf("%w: duplicate data delete", ErrRemoteRejected)
		}
		mutationKeys[key] = struct{}{}
	}
	snapshotKeys := make(map[RemoteSnapshotKey]struct{}, len(c.Snapshots)+len(c.Invalidations))
	for i := range c.Snapshots {
		snapshot := &c.Snapshots[i]
		if !snapshot.Key.Valid() || snapshot.Key.EntityID != c.EntityID || snapshot.Key.Kind != c.Kind || snapshot.BaseVersion != c.BaseVersion || snapshot.StateVersion != c.NextVersion || snapshot.MarkerEpoch != c.MarkerEpoch || snapshot.RouteEpoch != c.RouteEpoch || snapshot.Schema == 0 || len(snapshot.Data) == 0 {
			return fmt.Errorf("%w: invalid snapshot %d", ErrRemoteRejected, i)
		}
		if snapshot.Checksum != 0 && RemoteSnapshotChecksum(snapshot.Data) != snapshot.Checksum {
			return fmt.Errorf("%w: snapshot checksum mismatch", ErrRemoteRejected)
		}
		if _, exists := snapshotKeys[snapshot.Key]; exists {
			return fmt.Errorf("%w: duplicate snapshot", ErrRemoteRejected)
		}
		snapshotKeys[snapshot.Key] = struct{}{}
	}
	for _, key := range c.Invalidations {
		if !key.Valid() || key.EntityID != c.EntityID || key.Kind != c.Kind {
			return fmt.Errorf("%w: invalid snapshot invalidation", ErrRemoteRejected)
		}
		if _, exists := snapshotKeys[key]; exists {
			return fmt.Errorf("%w: duplicate snapshot invalidation", ErrRemoteRejected)
		}
		snapshotKeys[key] = struct{}{}
	}
	return nil
}

type RemoteCommitReceipt struct {
	TransactionID RemoteTransactionID
	EntityID      int64
	StateVersion  uint64
	MarkerEpoch   uint64
	LockFence     uint64
	RouteEpoch    uint64
	CommittedAt   int64
}

type RemoteCommitState uint8

const (
	RemoteCommitUnknown RemoteCommitState = iota
	RemoteCommitAdmitted
	RemoteCommitApplied
	RemoteCommitPublished
	RemoteCommitCommitted
	RemoteCommitRejected
	RemoteCommitIndeterminate
)

type RemoteCommitStatus struct {
	TransactionID RemoteTransactionID
	State         RemoteCommitState
	Receipts      []RemoteCommitReceipt
	Commits       []RemoteCommit
	Cause         string
}

func (s RemoteCommitStatus) Clone() RemoteCommitStatus {
	s.Receipts = append([]RemoteCommitReceipt(nil), s.Receipts...)
	if len(s.Commits) > 0 {
		commits := make([]RemoteCommit, len(s.Commits))
		for i := range s.Commits {
			commits[i] = s.Commits[i].Clone()
		}
		s.Commits = commits
	}
	return s
}

// IRemoteCommitParticipant is generated on a remotely managed entity. Build is
// called with the entity mutex held. Acknowledge and Rollback operate only on
// DirtyTracker atomics and therefore do not expose lock management to business
// code.
type IRemoteCommitParticipant interface {
	BuildRemoteCommitLocked(RemoteWriteLease, RemoteTransactionOutcome) (RemoteCommit, error)
	AcknowledgeRemoteCommit(RemoteCommit) error
	RollbackRemoteCommit(RemoteCommit)
}

// IRemoteCommitter applies immutable commits with storage-side version and
// fence checks. It must be idempotent by TransactionID and reject reuse of a
// TransactionID with different commit content. Applied status must include the
// immutable commits and receipts required for live outbox replay.
type IRemoteCommitter interface {
	CommitRemote(context.Context, RemoteCommit) (RemoteCommitReceipt, error)
	CommitStatus(context.Context, RemoteTransactionID) (RemoteCommitStatus, error)
}

type IRemoteAtomicBatchCommitter interface {
	IRemoteCommitter
	CommitRemoteBatch(context.Context, []RemoteCommit) ([]RemoteCommitReceipt, error)
}

type IRemoteCommitWaiter interface {
	WaitRemoteCommit(context.Context, RemoteTransactionID) (RemoteCommitStatus, error)
}

// IRemoteCommitOutbox exposes storage-applied commits whose snapshot/event
// publication was interrupted. Implementations persist this state atomically
// with entity data and make MarkRemoteCommitPublished idempotent.
type IRemoteCommitOutbox interface {
	PendingRemoteCommits(context.Context, int) ([]RemoteCommitStatus, error)
	MarkRemoteCommitPublished(context.Context, RemoteTransactionID) error
}

type IRemoteStorageInitializer interface {
	EnsureRemoteStorage(context.Context) error
}

// IRemoteCommitAcknowledger is invoked after the authoritative backend has
// accepted the immutable commit. Generated implementations clear only the
// dirty generation captured by that commit.
type IRemoteSnapshotPublisher interface {
	PublishRemoteSnapshot(context.Context, RemoteSnapshotRecord) error
	DeleteRemoteSnapshot(context.Context, RemoteSnapshotKey, uint64) error
}

// RemoteSnapshotScope produces a stable, non-zero scope ID for generated DAO
// views. Explicit protocol scope constants remain preferable for public APIs.
func RemoteSnapshotScope(collection string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(collection))
	scope := h.Sum32()
	if scope == 0 {
		return 1
	}
	return scope
}

// RemoteSnapshotSchema separates decoder namespaces by entity kind and scope.
func RemoteSnapshotSchema(kind EntityKind, scope uint32) uint32 {
	value := uint32(kind)*0x9e3779b1 ^ scope
	if value == 0 {
		return 1
	}
	return value
}

type IRemoteSnapshotLoader interface {
	LoadRemoteSnapshot(context.Context, RemoteSnapshotKey, RemoteReadConsistency, uint64) (RemoteSnapshotEnvelope, bool, error)
}

// RemoteWriteBatch is prepared before Nest locks entities. FinalizeLocked is
// called after a successful handler while entity mutexes remain held; Commit is
// called after those mutexes are released but before write gates are released.
type RemoteWriteBatch interface {
	EntityIDs() []int64
	FinalizeLocked(RemoteTransactionOutcome) error
	Commits() []RemoteCommit
	Commit(context.Context) ([]RemoteCommitReceipt, error)
	Abort(context.Context, error) error
	Indeterminate(context.Context, error) error
	Close(context.Context) error
}

type RemoteWriteBatchManager interface {
	PrepareRemoteWriteBatch(context.Context, []int64) (RemoteWriteBatch, error)
	FlushRemoteEntity(context.Context, int64, uint64) error
	FlushRemoteTransaction(context.Context, RemoteTransactionID) error
	FlushRemoteAll(context.Context) error
	RemoteCommitStatus(context.Context, RemoteTransactionID) (RemoteCommitStatus, error)
}

type RemoteOwnershipManager interface {
	GetRemoteOwnership(context.Context, int64) (RemoteEntityMarkerLease, bool, error)
	ClaimRemoteOwnership(context.Context, int64) (RemoteEntityMarkerLease, error)
	EnterRemoteSharedMode(context.Context, int64) (RemoteEntityMarkerLease, error)
	LeaveRemoteSharedMode(context.Context, int64) (RemoteEntityMarkerLease, error)
	TransferRemoteOwnership(context.Context, int64, int32) (RemoteEntityMarkerLease, error)
}

// RemoteSnapshotInterest is a renewable soft-state subscription. Scope and
// policy are part of the key, enabling generated LOD/projection variants.
type RemoteSnapshotInterest struct {
	ConsumerSID int32
	Key         RemoteSnapshotKey
	ExpiresAt   int64
}

type RemoteSnapshotInterestManager interface {
	RenewRemoteSnapshotInterest(context.Context, RemoteSnapshotKey) error
	ReleaseRemoteSnapshotInterest(context.Context, RemoteSnapshotKey) error
}

type RemoteCommitApplier interface {
	ApplyRemoteCommits(context.Context, RemoteTransactionID, []RemoteCommit) ([]RemoteCommitReceipt, error)
}

type RemoteSnapshotReader interface {
	ReadRemoteSnapshot(context.Context, RemoteSnapshotKey, RemoteReadConsistency, uint64) (RemoteSnapshotEnvelope, bool, error)
}

func ValidateRemoteWriteBatchIDs(ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	normalized := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		meta := ResolveEntityID(id)
		if meta.FullID == 0 || meta.Kind == EntityKindNone || !IsEntityKindRemoteManaged(meta.Kind) {
			return nil, fmt.Errorf("%w: invalid remote entity %d", ErrRemoteRejected, id)
		}
		if _, ok := seen[meta.FullID]; ok {
			continue
		}
		seen[meta.FullID] = struct{}{}
		normalized = append(normalized, meta.FullID)
	}
	slices.Sort(normalized)
	return normalized, nil
}

func NewRemoteTransactionOutcome(id RemoteTransactionID, handler, requestID string, succeeded bool, durability uint8) RemoteTransactionOutcome {
	return RemoteTransactionOutcome{TransactionID: id, Handler: handler, RequestID: requestID, Succeeded: succeeded, Durability: durability, FinalizedAt: time.Now().UnixNano()}
}
