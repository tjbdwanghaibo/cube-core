package entity

import (
	"fmt"
	"github.com/tjbdwanghaibo/cube-core/lock"
	"math"
	"sync/atomic"
)

// IThreadSafeRemoteEntity extends IThreadSafeEntity with remote entity capabilities.
// Remote entities can be shared across servers and require distributed locking.
type IThreadSafeRemoteEntity interface {
	IThreadSafeEntity
	EntityVersion() int64
	SetEntityVersion(int64)
	// ExcludeSId is the current ownership sid. Zero means the entity is
	// remotely shared/owned; a positive sid means that server owns the local
	// fast path. It is not the source of truth for whether the entity is
	// registered as remotely shared.
	ExcludeSId() int32
	// SetExcludeSId updates the current ownership sid.
	SetExcludeSId(int32)
	RemoteVersionVector() RemoteVersionVector
	SetRemoteVersionVector(RemoteVersionVector) error
	RemoteOwnershipState() RemoteOwnershipState
	TransitionRemoteOwnership(RemoteOwnershipState) error
}

// RemoteEntityBase extends EntityBase with remote entity fields.
// Embed this instead of EntityBase for entities that support remote access.
type RemoteEntityBase struct {
	EntityBase
	version    atomic.Pointer[RemoteVersionVector]
	excludeSId atomic.Int32
	ownerState atomic.Uint32
}

func (r *RemoteEntityBase) EntityVersion() int64 {
	version := r.RemoteVersionVector().StateVersion
	if version > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(version)
}

func (r *RemoteEntityBase) SetEntityVersion(v int64) {
	if v < 0 {
		v = 0
	}
	for {
		current := r.version.Load()
		next := RemoteVersionVector{StateVersion: uint64(v)}
		if current != nil {
			next = *current
			next.StateVersion = uint64(v)
		}
		if r.version.CompareAndSwap(current, &next) {
			return
		}
	}
}

func (r *RemoteEntityBase) ExcludeSId() int32 {
	return r.excludeSId.Load()
}

func (r *RemoteEntityBase) SetExcludeSId(sid int32) {
	r.excludeSId.Store(sid)
}

// RemoteVersionVector returns the four independent version dimensions.
func (r *RemoteEntityBase) RemoteVersionVector() RemoteVersionVector {
	current := r.version.Load()
	if current == nil {
		return RemoteVersionVector{}
	}
	return *current
}

// SetRemoteVersionVector rejects a stale fence even if a caller still holds a
// Go reference to this entity after its distributed lease expired.
func (r *RemoteEntityBase) SetRemoteVersionVector(version RemoteVersionVector) error {
	if version.StateVersion > math.MaxInt64 {
		return ErrRemoteVersionConflict
	}
	for {
		current := r.version.Load()
		if current != nil && version.LockFence < current.LockFence {
			return ErrRemoteFenced
		}
		next := version
		if r.version.CompareAndSwap(current, &next) {
			return nil
		}
	}
}

// NewRemoteEntityBase creates a remote-capable entity base with one coherent
// version vector. The vector is published through a single atomic pointer, so
// readers can never observe fields from different ownership generations.
func NewRemoteEntityBase(id int64, category EntityCategory, notAutoPersist bool, kind EntityKind) *RemoteEntityBase {
	return NewRemoteEntityBaseWithMutex(id, category, notAutoPersist, nil, kind)
}

func NewRemoteEntityBaseWithMutex(id int64, category EntityCategory, notAutoPersist bool, mu lock.Mutex, kind EntityKind) *RemoteEntityBase {
	base := &RemoteEntityBase{EntityBase: *NewEntityBaseWithMutex(id, category, notAutoPersist, mu, kind)}
	initial := &RemoteVersionVector{MarkerEpoch: 1, RouteEpoch: 1}
	base.version.Store(initial)
	base.ownerState.Store(uint32(RemoteOwnershipUnknown))
	return base
}

func (r *RemoteEntityBase) RemoteOwnershipState() RemoteOwnershipState {
	return RemoteOwnershipState(r.ownerState.Load())
}

func (r *RemoteEntityBase) TransitionRemoteOwnership(to RemoteOwnershipState) error {
	for {
		from := RemoteOwnershipState(r.ownerState.Load())
		if !ValidRemoteOwnershipTransition(from, to) {
			return fmt.Errorf("%w: %s -> %s", ErrRemoteInvalidStateTransition, from, to)
		}
		if r.ownerState.CompareAndSwap(uint32(from), uint32(to)) {
			return nil
		}
	}
}

// IsRemoteCapable returns true if this entity's ID has the remote-capable bit.
// It does not check the runtime remote marker store.
func (r *RemoteEntityBase) IsRemoteCapable() bool {
	return IsRemoteCapableEntityID(r.GUId())
}
