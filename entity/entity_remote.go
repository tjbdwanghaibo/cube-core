package entity

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

	// OnDataChange applies synced data from another server.
	// Called when a remote owner broadcasts updated entity state.
	OnDataChange(data []byte, version int64)
}

// RemoteEntityBase extends EntityBase with remote entity fields.
// Embed this instead of EntityBase for entities that support remote access.
type RemoteEntityBase struct {
	EntityBase
	entityVersion int64
	excludeSId    int32
}

func (r *RemoteEntityBase) EntityVersion() int64 {
	return r.entityVersion
}

func (r *RemoteEntityBase) SetEntityVersion(v int64) {
	r.entityVersion = v
}

func (r *RemoteEntityBase) ExcludeSId() int32 {
	return r.excludeSId
}

func (r *RemoteEntityBase) SetExcludeSId(sid int32) {
	r.excludeSId = sid
}

// IsRemoteCapable returns true if this entity's ID has the remote-capable bit.
// It does not check the runtime remote marker store.
func (r *RemoteEntityBase) IsRemoteCapable() bool {
	return IsRemoteCapableEntityID(r.GUId())
}
