package entitysync

import "github.com/tjbdwanghaibo/cube-core/entity"

type RuntimeFullSnapshotProvider struct {
	Remote entity.IRemoteEntityWrapperManager
}

func NewRuntimeFullSnapshotProvider(remote entity.IRemoteEntityWrapperManager) RuntimeFullSnapshotProvider {
	return RuntimeFullSnapshotProvider{Remote: remote}
}

func (p RuntimeFullSnapshotProvider) PackFullSync(req SyncResyncRequest) (entity.SyncPacket, bool) {
	if packet, ok := targetedFullPacket(req); ok {
		return packet, true
	}
	if p.Remote == nil || req.EntityID == 0 {
		return entity.SyncPacket{}, false
	}
	wrapper, ok := p.Remote.Get(req.EntityID)
	if !ok || wrapper == nil {
		return entity.SyncPacket{}, false
	}
	return packFullFromEntity(wrapper.Entity(), req)
}

func packFullFromEntity(ent entity.IThreadSafeEntity, req SyncResyncRequest) (entity.SyncPacket, bool) {
	if ent == nil || ent.Base() == nil || ent.Base().Sync() == nil {
		return entity.SyncPacket{}, false
	}
	syncState := ent.Base().Sync()
	if req.Topic != "" && syncState.Topic() != req.Topic {
		return entity.SyncPacket{}, false
	}
	return syncState.PackFullForObserver(req.Observer, entity.SyncFullReasonResync)
}
