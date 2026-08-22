package nest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

type RemoteAcquireMode = entity.RemoteAcquireMode

const (
	RemoteAcquireReadOnly = entity.RemoteAcquireReadOnly
	RemoteAcquireCache    = entity.RemoteAcquireCache
)

var (
	ErrRemoteAccessAliasDuplicate = errors.New("nest: duplicate remote access alias")
	ErrRemoteAccessMissing        = errors.New("nest: remote snapshot missing")
	ErrRemoteSnapshotTypeMismatch = errors.New("nest: remote snapshot type mismatch")
	ErrRemoteSnapshotIdentity     = errors.New("nest: remote snapshot identity mismatch")
)

type RemoteAccess struct {
	Alias          string
	Ref            entity.RemoteViewRef
	Mode           RemoteAcquireMode
	Scope          uint64
	MinVersion     uint64
	AllowStale     bool
	CacheTTLMillis int64
	Required       bool
	Tenant         uint32
	Policy         uint32
	Consistency    entity.RemoteReadConsistency
}

type RemoteAccessProvider interface {
	RemoteAccess() []RemoteAccess
}

type RemoteSnapshotResolver interface {
	ResolveRemoteSnapshot(access RemoteAccess) (entity.RemoteSnapshot, error)
}

type remoteSnapshotCtxKey struct{}

func prepareRemoteSnapshots(msg *Msg, resolver RemoteSnapshotResolver, manager entity.IRemoteEntityManager) error {
	if msg == nil {
		return nil
	}
	accesses := collectRemoteAccess(msg.Params)
	if len(accesses) == 0 {
		return nil
	}
	snapshots := make(map[string]entity.RemoteSnapshot, len(accesses))
	for _, access := range accesses {
		if access.Alias == "" {
			return fmt.Errorf("%w: empty alias", ErrRemoteAccessMissing)
		}
		if _, exists := snapshots[access.Alias]; exists {
			return fmt.Errorf("%w: alias=%s", ErrRemoteAccessAliasDuplicate, access.Alias)
		}
		if !access.Ref.Valid() {
			return fmt.Errorf("nest: remote access %s has invalid ref", access.Alias)
		}
		if access.Consistency == 0 && access.Mode != RemoteAcquireReadOnly && access.Mode != RemoteAcquireCache {
			return fmt.Errorf("nest: remote access %s has invalid read mode %d", access.Alias, access.Mode)
		}
		if access.Consistency > entity.RemoteReadLinearizable {
			return fmt.Errorf("nest: remote access %s has invalid consistency %d", access.Alias, access.Consistency)
		}
		snapshot, err := resolveRemoteSnapshot(access, resolver, manager)
		if err != nil {
			if access.Required {
				return fmt.Errorf("nest: remote access %s: %w", access.Alias, err)
			}
			continue
		}
		if snapshot.EntityID != access.Ref.EntityID || snapshot.Kind != access.Ref.Kind || snapshot.Scope != access.Scope || snapshot.RouteEpoch != access.Ref.RouteEpoch {
			if access.Required {
				return fmt.Errorf("%w: alias=%s", ErrRemoteSnapshotIdentity, access.Alias)
			}
			continue
		}
		if !snapshot.Accepts(remoteReadOption(access)) {
			if access.Required {
				return fmt.Errorf("nest: remote access %s stale: version=%d min=%d", access.Alias, snapshot.Version, access.MinVersion)
			}
			continue
		}
		snapshots[access.Alias] = snapshot
	}
	if len(snapshots) == 0 {
		return nil
	}
	c := ctx.CurrentContext()
	if c == nil {
		return nil
	}
	c.Set(remoteSnapshotCtxKey{}, snapshots)
	return nil
}

func resolveRemoteSnapshot(access RemoteAccess, resolver RemoteSnapshotResolver, manager entity.IRemoteEntityManager) (entity.RemoteSnapshot, error) {
	if resolver != nil {
		return resolver.ResolveRemoteSnapshot(access)
	}
	consistency := access.Consistency
	if consistency == 0 {
		if access.Mode == RemoteAcquireCache {
			consistency = entity.RemoteReadCached
		} else {
			consistency = entity.RemoteReadMonotonic
		}
	}
	baseCtx := context.Background()
	if current := ctx.CurrentContext(); current != nil && current.Base != nil {
		baseCtx = current.Base
	}
	if manager == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("nest: Remote Entity snapshot reader is not configured")
	}
	envelope, found, err := manager.ReadRemoteSnapshot(baseCtx, entity.RemoteSnapshotKey{
		Tenant: access.Tenant, EntityID: access.Ref.EntityID, Kind: access.Ref.Kind,
		Scope: uint32(access.Scope), Policy: access.Policy,
	}, consistency, access.MinVersion)
	if err != nil {
		return entity.RemoteSnapshot{}, err
	}
	if !found {
		return entity.RemoteSnapshot{}, ErrRemoteAccessMissing
	}
	data, err := entity.DecodeRemoteSnapshot(envelope)
	if err != nil {
		return entity.RemoteSnapshot{}, err
	}
	return entity.RemoteSnapshot{
		EntityID: envelope.Key.EntityID, Kind: envelope.Key.Kind, Scope: uint64(envelope.Key.Scope),
		Version: envelope.StateVersion, RouteEpoch: envelope.RouteEpoch,
		Source: entity.RemoteSnapshotSourceCache, ReadAt: time.Now().UnixMilli(),
		ExpiresAt: envelope.ExpiresAt / int64(time.Millisecond), Data: data,
	}, nil
}

func collectRemoteAccess(params []any) []RemoteAccess {
	if len(params) == 0 {
		return nil
	}
	var accesses []RemoteAccess
	for _, param := range params {
		if provider, ok := param.(RemoteAccessProvider); ok && provider != nil {
			accesses = append(accesses, provider.RemoteAccess()...)
		}
	}
	return accesses
}

func remoteReadOption(access RemoteAccess) entity.RemoteReadOption {
	return entity.RemoteReadOption{
		MinVersion:     access.MinVersion,
		AllowStale:     access.AllowStale,
		CacheTTLMillis: access.CacheTTLMillis,
		NowMillis:      time.Now().UnixMilli(),
	}
}

type RemoteKey[T any] struct {
	Alias string
}

type RemoteScopeProvider interface {
	RemoteScope() uint64
}

type RemoteDefaultTTLMillisProvider interface {
	RemoteDefaultTTLMillis() int64
}

func RemoteScopeOf[T RemoteScopeProvider]() uint64 {
	var v T
	return v.RemoteScope()
}

func RemoteDefaultTTLMillisOf[T any]() int64 {
	var v T
	provider, ok := any(v).(RemoteDefaultTTLMillisProvider)
	if !ok {
		return 0
	}
	return provider.RemoteDefaultTTLMillis()
}

func Remote[T any](key RemoteKey[T]) (T, bool) {
	return remoteSnapshot[T](key.Alias)
}

func MustRemote[T any](key RemoteKey[T]) T {
	v, ok := Remote(key)
	if !ok {
		panic(fmt.Errorf("%w: alias=%s", ErrRemoteAccessMissing, key.Alias))
	}
	return v
}

func remoteSnapshot[T any](alias string) (T, bool) {
	var zero T
	c := ctx.CurrentContext()
	if c == nil {
		return zero, false
	}
	raw, ok := c.Get(remoteSnapshotCtxKey{})
	if !ok {
		return zero, false
	}
	snapshots, ok := raw.(map[string]entity.RemoteSnapshot)
	if !ok {
		return zero, false
	}
	snapshot, ok := snapshots[alias]
	if !ok {
		return zero, false
	}
	data, ok := snapshot.Data.(T)
	if !ok {
		return zero, false
	}
	return data, true
}
