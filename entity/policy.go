package entity

import "fmt"

// RemotePolicy describes how an entity kind participates in cross-server
// routing/ownership. Remote-capable only means the ID carries the remote bit;
// remote-managed means the remote_entity module may load, lock, save, and sync
// the entity through IThreadSafeRemoteEntity.
type RemotePolicy uint8

const (
	RemotePolicyNone RemotePolicy = iota
	RemotePolicyCapable
	RemotePolicyManaged
	RemotePolicyMirror
)

func (p RemotePolicy) RemoteCapable() bool {
	switch p {
	case RemotePolicyCapable, RemotePolicyManaged, RemotePolicyMirror:
		return true
	default:
		return false
	}
}

func (p RemotePolicy) RemoteManaged() bool {
	return p == RemotePolicyManaged
}

// EntityLifetime describes the memory/persistence lifecycle expected by the
// framework. It is declarative; persistence is still controlled by AutoPersist
// and registered save/load definitions.
type EntityLifetime uint8

const (
	EntityLifetimeDefault EntityLifetime = iota
	EntityLifetimeEphemeral
	EntityLifetimeRuntimeRebuild
	EntityLifetimePersistedHotCold
	EntityLifetimeResident
	EntityLifetimeRemoteManaged
	EntityLifetimeMirrorCache
)

func DefaultEntityLifetime(noPersist bool, remotePolicy RemotePolicy) EntityLifetime {
	if remotePolicy == RemotePolicyManaged {
		return EntityLifetimeRemoteManaged
	}
	if remotePolicy == RemotePolicyMirror {
		return EntityLifetimeMirrorCache
	}
	if noPersist {
		return EntityLifetimeEphemeral
	}
	return EntityLifetimePersistedHotCold
}

func ValidateEntityPolicy(kind EntityKind, noPersist bool, remotePolicy RemotePolicy, lifetime EntityLifetime) error {
	if lifetime == EntityLifetimeDefault {
		lifetime = DefaultEntityLifetime(noPersist, remotePolicy)
	}
	if remotePolicy == RemotePolicyManaged && lifetime != EntityLifetimeRemoteManaged {
		return fmt.Errorf("entity kind %d remote=managed requires remote_managed lifetime, got %d", kind, lifetime)
	}
	if remotePolicy == RemotePolicyMirror && lifetime != EntityLifetimeMirrorCache {
		return fmt.Errorf("entity kind %d remote=mirror requires mirror_cache lifetime, got %d", kind, lifetime)
	}
	if remotePolicy != RemotePolicyManaged && lifetime == EntityLifetimeRemoteManaged {
		return fmt.Errorf("entity kind %d remote_managed lifetime requires remote=managed", kind)
	}
	if remotePolicy != RemotePolicyMirror && lifetime == EntityLifetimeMirrorCache {
		return fmt.Errorf("entity kind %d mirror_cache lifetime requires remote=mirror", kind)
	}
	if noPersist {
		switch lifetime {
		case EntityLifetimePersistedHotCold, EntityLifetimeResident:
			return fmt.Errorf("entity kind %d noPersist=true conflicts with lifetime %d", kind, lifetime)
		}
	}
	return nil
}
