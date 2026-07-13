package nest

import (
	"github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"errors"
	"fmt"
)

type RemoteAcquireMode = entity.RemoteAcquireMode

const (
	RemoteAcquireWrite    = entity.RemoteAcquireWrite
	RemoteAcquireReadOnly = entity.RemoteAcquireReadOnly
	RemoteAcquireCache    = entity.RemoteAcquireCache
)

var (
	ErrRemoteAccessAliasDuplicate = errors.New("nest: duplicate remote access alias")
	ErrRemoteAccessMissing        = errors.New("nest: remote snapshot missing")
	ErrRemoteSnapshotTypeMismatch = errors.New("nest: remote snapshot type mismatch")
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
}

type RemoteAccessProvider interface {
	RemoteAccess() []RemoteAccess
}

type RemoteSnapshotResolver interface {
	ResolveRemoteSnapshot(access RemoteAccess) (entity.RemoteSnapshot, error)
}

type remoteSnapshotCtxKey struct{}

func prepareRemoteSnapshots(msg *Msg, resolver RemoteSnapshotResolver) error {
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
		snapshot, err := resolveRemoteSnapshot(access, resolver)
		if err != nil {
			if access.Required {
				return fmt.Errorf("nest: remote access %s: %w", access.Alias, err)
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

func resolveRemoteSnapshot(access RemoteAccess, resolver RemoteSnapshotResolver) (entity.RemoteSnapshot, error) {
	if resolver != nil {
		return resolver.ResolveRemoteSnapshot(access)
	}
	snapshot, ok, err := entity.ResolveRemoteSnapshot(entity.RemoteSnapshotResolveRequest{
		Ref:    access.Ref,
		Mode:   access.Mode,
		Scope:  access.Scope,
		Option: remoteReadOption(access),
	})
	if !ok {
		return entity.RemoteSnapshot{}, fmt.Errorf("nest: remote snapshot resolver is not configured")
	}
	return snapshot, err
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
