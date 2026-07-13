package entity

import "sync"

// RemoteEntityPrepareFunc prepares remote entities before dispatch.
// Input: remote entity IDs that need to be loaded/locked.
// Output: release func (called after dispatch to release distributed locks), error.
// Application layer implements: acquire distributed lock → load from DB → put into EntityManager.
// Set by application layer during initialization.
type RemoteEntityPrepareFunc func(ids []int64) (release func(), err error)
type RemoteEntityMarkedFunc func(id int64) bool
type RemoteSnapshotResolveFunc func(req RemoteSnapshotResolveRequest) (RemoteSnapshot, error)

type remoteEntityMarkedChecker interface {
	IsRemoteMarked(id int64) bool
}

var remoteEntityHooks struct {
	mu              sync.RWMutex
	preparer        RemoteEntityPrepareFunc
	marked          RemoteEntityMarkedFunc
	snapshotResolve RemoteSnapshotResolveFunc
}

func PrepareRemoteEntities(ids []int64) (func(), bool, error) {
	remoteEntityHooks.mu.RLock()
	preparer := remoteEntityHooks.preparer
	remoteEntityHooks.mu.RUnlock()
	if preparer == nil {
		return nil, false, nil
	}
	release, err := preparer(ids)
	return release, true, err
}

func IsRemoteMarkedEntityID(id int64) bool {
	if !IsRemoteCapableEntityID(id) {
		return false
	}
	remoteEntityHooks.mu.RLock()
	marked := remoteEntityHooks.marked
	remoteEntityHooks.mu.RUnlock()
	return marked != nil && marked(id)
}

func IsRemoteManagedMarkedEntityID(id int64) bool {
	if !IsRemoteMarkedEntityID(id) {
		return false
	}
	kind := GetEntityKindFromID(id)
	return kind != EntityKindNone && IsEntityKindRemoteManaged(kind)
}

func ResolveRemoteSnapshot(req RemoteSnapshotResolveRequest) (RemoteSnapshot, bool, error) {
	remoteEntityHooks.mu.RLock()
	resolver := remoteEntityHooks.snapshotResolve
	remoteEntityHooks.mu.RUnlock()
	if resolver == nil {
		return RemoteSnapshot{}, false, nil
	}
	snapshot, err := resolver(req)
	return snapshot, true, err
}

// BindRemoteEntityManager wires IRemoteEntityManager into framework hooks.
// Called by RemoteEntityMod during Start().
func BindRemoteEntityManager(mgr IRemoteEntityManager) {
	remoteEntityHooks.mu.Lock()
	remoteEntityHooks.preparer = func(ids []int64) (func(), error) {
		return mgr.PrepareRemoteEntities(ids)
	}
	remoteEntityHooks.marked = func(id int64) bool {
		if checker, ok := mgr.(remoteEntityMarkedChecker); ok {
			return checker.IsRemoteMarked(id)
		}
		if w, ok := mgr.Get(id); ok {
			return w.IsMarked()
		}
		return false
	}
	remoteEntityHooks.snapshotResolve = func(req RemoteSnapshotResolveRequest) (RemoteSnapshot, error) {
		return mgr.ResolveRemoteSnapshot(req)
	}
	remoteEntityHooks.mu.Unlock()
}

func UnbindRemoteEntityManager(mgr IRemoteEntityManager) {
	if mgr == nil {
		return
	}
	remoteEntityHooks.mu.Lock()
	remoteEntityHooks.preparer = nil
	remoteEntityHooks.marked = nil
	remoteEntityHooks.snapshotResolve = nil
	remoteEntityHooks.mu.Unlock()
}
