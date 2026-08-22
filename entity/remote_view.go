package entity

import (
	"errors"
	"fmt"
	"time"
)

var ErrRemoteSnapshotStale = errors.New("remote snapshot stale")

// RemoteAcquireMode describes immutable snapshot reads. Remote writes are Nest
// targets and always use the guarded RemoteWriteBatch path.
type RemoteAcquireMode uint8

const (
	RemoteAcquireReadOnly RemoteAcquireMode = iota + 1
	RemoteAcquireCache
)

// RemoteReadOption configures a single read operation. It is intentionally not
// stored in business objects; long-lived consumers store only RemoteViewRef.
type RemoteReadOption struct {
	MinVersion     uint64
	AllowStale     bool
	CacheTTLMillis int64
	NowMillis      int64
}

func NormalizeRemoteReadOption(option RemoteReadOption) RemoteReadOption {
	if option.CacheTTLMillis < 0 {
		option.CacheTTLMillis = 0
	}
	if option.NowMillis < 0 {
		option.NowMillis = 0
	}
	return option
}

func (o RemoteReadOption) Accepts(version int64) bool {
	if o.AllowStale || o.MinVersion == 0 {
		return true
	}
	return version >= 0 && uint64(version) >= o.MinVersion
}

type RemoteSnapshotSource uint8

const (
	RemoteSnapshotSourceUnknown RemoteSnapshotSource = iota
	RemoteSnapshotSourceLocal
	RemoteSnapshotSourceLoaded
	RemoteSnapshotSourceCache
)

func (s RemoteSnapshotSource) String() string {
	switch s {
	case RemoteSnapshotSourceLocal:
		return "local"
	case RemoteSnapshotSourceLoaded:
		return "loaded"
	case RemoteSnapshotSourceCache:
		return "cache"
	default:
		return "unknown"
	}
}

// RemoteSnapshot is an immutable read result for remote read-only/cache paths.
// It is intentionally value-oriented: callers must not receive a mutable entity,
// component, or DAO pointer from read-only remote access.
type RemoteSnapshot struct {
	EntityID   int64
	Kind       EntityKind
	Scope      uint64
	Version    uint64
	RouteEpoch uint64
	Source     RemoteSnapshotSource
	ReadAt     int64
	ExpiresAt  int64
	Data       any
}

func (s RemoteSnapshot) Accepts(option RemoteReadOption) bool {
	if s.expiredByOption(option) {
		return false
	}
	return option.AllowStale || option.MinVersion == 0 || s.Version >= option.MinVersion
}

func (s RemoteSnapshot) Expired(now int64) bool {
	return s.ExpiresAt > 0 && now >= s.ExpiresAt
}

func (s RemoteSnapshot) expiredByOption(option RemoteReadOption) bool {
	if option.NowMillis <= 0 {
		return false
	}
	return s.Expired(option.NowMillis)
}

func (s RemoteSnapshot) AsString() string {
	if v, ok := s.Data.(string); ok {
		return v
	}
	return ""
}

type RemoteSnapshotRequest struct {
	Scope      uint64
	RouteEpoch uint64
	Option     RemoteReadOption
}

type RemoteSnapshotProvider interface {
	RemoteSnapshot(scope uint64) (any, bool)
}

type RemoteSnapshotResolveRequest struct {
	Ref        RemoteViewRef
	Mode       RemoteAcquireMode
	Scope      uint64
	RouteEpoch uint64
	Option     RemoteReadOption
}

// RemoteViewRef is the generic persisted reference to a server-side read model.
// It says which remote entity is being consumed and which version was observed.
type RemoteViewRef struct {
	EntityID   int64
	Kind       EntityKind
	Version    uint64
	RouteEpoch uint64
}

func (r RemoteViewRef) Valid() bool {
	if r.EntityID == 0 || r.Kind == EntityKindNone {
		return false
	}
	meta := ResolveEntityID(r.EntityID)
	return meta.FullID == r.EntityID && meta.Kind == r.Kind
}

func (r RemoteViewRef) UniqueID() int64 {
	if !r.Valid() {
		return 0
	}
	return ResolveEntityID(r.EntityID).UniqueID
}

func NewRemoteViewRef(entityID int64, kind EntityKind, version uint64, routeEpoch uint64) (RemoteViewRef, error) {
	ref := RemoteViewRef{EntityID: entityID, Kind: kind, Version: version, RouteEpoch: routeEpoch}
	if !ref.Valid() {
		return RemoteViewRef{}, fmt.Errorf("remote view ref: invalid entity=%d kind=%d", entityID, kind)
	}
	return ref, nil
}

func RemoteSnapshotReadAt(option RemoteReadOption) int64 {
	option = NormalizeRemoteReadOption(option)
	if option.NowMillis > 0 {
		return option.NowMillis
	}
	return time.Now().UnixMilli()
}

func RemoteSnapshotExpiresAt(readAt int64, option RemoteReadOption) int64 {
	option = NormalizeRemoteReadOption(option)
	if option.CacheTTLMillis <= 0 {
		return 0
	}
	if readAt <= 0 {
		readAt = RemoteSnapshotReadAt(option)
	}
	return readAt + option.CacheTTLMillis
}
