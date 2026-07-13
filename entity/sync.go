package entity

import (
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"sync"
	"time"
)

type EntitySyncMode uint8

const (
	EntitySyncModeDefault EntitySyncMode = iota
	EntitySyncModeManual
	EntitySyncModeDirty
)

type SyncFlushPolicy uint8

const (
	SyncFlushManual SyncFlushPolicy = iota
	SyncFlushOnEntityRelease
	SyncFlushInterval
	SyncFlushImmediate
)

type SyncPacketType uint8

const (
	SyncPacketTypeNone SyncPacketType = iota
	SyncPacketEnter
	SyncPacketUpdate
	SyncPacketLeave
	SyncPacketCustom
)

const SyncMaskFull uint64 = ^uint64(0)

const (
	SyncFullReasonNone uint32 = iota
	SyncFullReasonDirty
	SyncFullReasonResync
	SyncFullReasonObserver
	SyncFullReasonSchema
)

type SyncObserverKind uint8

const (
	SyncObserverNone SyncObserverKind = iota
	SyncObserverPlayer
	SyncObserverServer
	SyncObserverEntity
	SyncObserverGroup
	SyncObserverCache
)

// SyncObserverRef identifies who observes an entity. Scene AOI, cross-server
// replication, cache subscribers, and entity-to-entity observation all use this
// one reference shape.
type SyncObserverRef struct {
	Kind SyncObserverKind
	ID   int64
	Sid  int32
	Key  string
}

func NewPlayerSyncObserver(playerID int64) SyncObserverRef {
	if playerID == 0 {
		return SyncObserverRef{}
	}
	return SyncObserverRef{Kind: SyncObserverPlayer, ID: playerID}
}

func NewServerSyncObserver(sid int32) SyncObserverRef {
	if sid == 0 {
		return SyncObserverRef{}
	}
	return SyncObserverRef{Kind: SyncObserverServer, Sid: sid}
}

func NewEntitySyncObserver(entityID int64) SyncObserverRef {
	if entityID == 0 {
		return SyncObserverRef{}
	}
	return SyncObserverRef{Kind: SyncObserverEntity, ID: entityID}
}

func NewGroupSyncObserver(key string) SyncObserverRef {
	if key == "" {
		return SyncObserverRef{}
	}
	return SyncObserverRef{Kind: SyncObserverGroup, Key: key}
}

func NewCacheSyncObserver(key string) SyncObserverRef {
	if key == "" {
		return SyncObserverRef{}
	}
	return SyncObserverRef{Kind: SyncObserverCache, Key: key}
}

func (r SyncObserverRef) Normalize() SyncObserverRef {
	if r.Kind == SyncObserverNone {
		switch {
		case r.ID != 0:
			r.Kind = SyncObserverPlayer
		case r.Sid != 0:
			r.Kind = SyncObserverServer
		case r.Key != "":
			r.Kind = SyncObserverGroup
		}
	}
	return r
}

func (r SyncObserverRef) Empty() bool {
	r = r.Normalize()
	return r.Kind == SyncObserverNone || (r.ID == 0 && r.Sid == 0 && r.Key == "")
}

func (r SyncObserverRef) PlayerID() int64 {
	r = r.Normalize()
	if r.Kind != SyncObserverPlayer {
		return 0
	}
	return r.ID
}

type SyncPacket struct {
	Topic         string
	EntityID      int64
	EntityKind    int32
	ObserverID    int64
	Observer      SyncObserverRef
	Type          SyncPacketType
	Version       uint64
	BaseVersion   uint64
	Mask          uint64
	Full          bool
	SchemaVersion uint32
	Reason        uint32
	Body          any
}

type EntitySyncSink interface {
	Enqueue(SyncPacket)
	EnqueueBatch([]SyncPacket)
}

type EntitySyncScheduler interface {
	EntitySyncSink
	MarkDirtyState(*EntitySyncState)
	Flush() []SyncPacket
}

type EntitySyncPacker interface {
	PackSyncEnter(observer SyncObserverRef) (SyncPacket, error)
	PackSyncUpdate(observer SyncObserverRef, mask uint64) (SyncPacket, error)
	PackSyncLeave(observer SyncObserverRef) (SyncPacket, error)
}

type EntitySyncPackFunc struct {
	Enter  func(observer SyncObserverRef) (SyncPacket, error)
	Update func(observer SyncObserverRef, mask uint64) (SyncPacket, error)
	Leave  func(observer SyncObserverRef) (SyncPacket, error)
}

func (f EntitySyncPackFunc) PackSyncEnter(observer SyncObserverRef) (SyncPacket, error) {
	if f.Enter == nil {
		return SyncPacket{}, nil
	}
	return f.Enter(observer.Normalize())
}

func (f EntitySyncPackFunc) PackSyncUpdate(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
	if f.Update == nil {
		return SyncPacket{Mask: mask}, nil
	}
	return f.Update(observer.Normalize(), mask)
}

func (f EntitySyncPackFunc) PackSyncLeave(observer SyncObserverRef) (SyncPacket, error) {
	if f.Leave == nil {
		return SyncPacket{}, nil
	}
	return f.Leave(observer.Normalize())
}

type lockedEntitySyncPacker struct {
	entity IThreadSafeEntityBase
	next   EntitySyncPacker
}

func newLockedEntitySyncPacker(entity IThreadSafeEntityBase, next EntitySyncPacker) EntitySyncPacker {
	if next == nil {
		return nil
	}
	return lockedEntitySyncPacker{entity: entity, next: next}
}

func (p lockedEntitySyncPacker) PackSyncEnter(observer SyncObserverRef) (SyncPacket, error) {
	unlock := p.lock()
	defer unlock()
	return p.next.PackSyncEnter(observer)
}

func (p lockedEntitySyncPacker) PackSyncUpdate(observer SyncObserverRef, mask uint64) (SyncPacket, error) {
	unlock := p.lock()
	defer unlock()
	return p.next.PackSyncUpdate(observer, mask)
}

func (p lockedEntitySyncPacker) PackSyncLeave(observer SyncObserverRef) (SyncPacket, error) {
	unlock := p.lock()
	defer unlock()
	return p.next.PackSyncLeave(observer)
}

func (p lockedEntitySyncPacker) lock() func() {
	if p.entity == nil || p.entity.GetMutex() == nil {
		return func() {}
	}
	if entitySyncLockedInCurrentGuard(p.entity.ID()) {
		return func() {}
	}
	p.entity.GetMutex().Lock()
	return func() {
		p.entity.GetMutex().Unlock()
	}
}

type EntitySyncPackCacheConfig struct {
	Enabled          bool
	ObserverAgnostic bool
}

type EntitySyncCreateParam struct {
	Enabled             bool
	EntityID            int64
	Topic               string
	Mode                EntitySyncMode
	FlushPolicy         SyncFlushPolicy
	MinInterval         time.Duration
	SchemaVersion       uint32
	FullSyncOnDirty     bool
	InitialObserverRefs []SyncObserverRef
	Packer              EntitySyncPacker
	PackCache           EntitySyncPackCacheConfig
}

type EntitySyncBuilderParam struct {
	Enabled         bool
	Topic           string
	Mode            EntitySyncMode
	FlushPolicy     SyncFlushPolicy
	MinInterval     time.Duration
	SchemaVersion   uint32
	FullSyncOnDirty bool
	PackerFactory   func(IThreadSafeEntity) EntitySyncPacker
	PackCache       EntitySyncPackCacheConfig
}

func (p EntitySyncBuilderParam) toCreateParam(e IThreadSafeEntity) EntitySyncCreateParam {
	ret := EntitySyncCreateParam{
		Enabled:         p.Enabled,
		Topic:           p.Topic,
		Mode:            p.Mode,
		FlushPolicy:     p.FlushPolicy,
		MinInterval:     p.MinInterval,
		SchemaVersion:   p.SchemaVersion,
		FullSyncOnDirty: p.FullSyncOnDirty,
		PackCache:       p.PackCache,
	}
	if e != nil {
		ret.EntityID = e.ID()
	}
	if p.PackerFactory != nil {
		ret.Packer = p.PackerFactory(e)
	}
	return ret
}

type EntitySyncObserver struct {
	ObserverID int64
	Ref        SyncObserverRef
	Flags      uint32
	LastMask   uint64
}

type EntitySyncState struct {
	mu              sync.Mutex
	enabled         bool
	entityID        int64
	topic           string
	mode            EntitySyncMode
	flushPolicy     SyncFlushPolicy
	minInterval     time.Duration
	lastFlush       time.Time
	dirtyMask       uint64
	fullDirty       bool
	fullReason      uint32
	version         uint64
	schemaVersion   uint32
	fullSyncOnDirty bool
	observers       map[SyncObserverRef]EntitySyncObserver
	packer          EntitySyncPacker
	packCacheConf   EntitySyncPackCacheConfig
	packCache       map[syncPackCacheKey]SyncPacket
}

type syncPackSnapshot struct {
	entityID      int64
	entityKind    int32
	topic         string
	version       uint64
	baseVersion   uint64
	schemaVersion uint32
	packer        EntitySyncPacker
}

type syncPackCacheKind uint8

const (
	syncPackCacheKindEnter syncPackCacheKind = iota + 1
	syncPackCacheKindUpdate
)

type syncPackCacheKey struct {
	kind     syncPackCacheKind
	observer SyncObserverRef
	mask     uint64
}

var (
	globalSyncSinkMu sync.RWMutex
	globalSyncSink   EntitySyncSink

	globalSyncSchedulerMu sync.RWMutex
	globalSyncScheduler   EntitySyncScheduler
)

func SetEntitySyncSink(sink EntitySyncSink) {
	globalSyncSinkMu.Lock()
	globalSyncSink = sink
	globalSyncSinkMu.Unlock()
}

func GetEntitySyncSink() EntitySyncSink {
	globalSyncSinkMu.RLock()
	defer globalSyncSinkMu.RUnlock()
	return globalSyncSink
}

func SetEntitySyncScheduler(scheduler EntitySyncScheduler) {
	globalSyncSchedulerMu.Lock()
	globalSyncScheduler = scheduler
	globalSyncSchedulerMu.Unlock()
}

func GetEntitySyncScheduler() EntitySyncScheduler {
	globalSyncSchedulerMu.RLock()
	defer globalSyncSchedulerMu.RUnlock()
	return globalSyncScheduler
}

func NewEntitySyncState(param EntitySyncCreateParam) *EntitySyncState {
	if !param.Enabled {
		return nil
	}
	mode := param.Mode
	if mode == EntitySyncModeDefault {
		mode = EntitySyncModeDirty
	}
	s := &EntitySyncState{
		enabled:         true,
		entityID:        param.EntityID,
		topic:           param.Topic,
		mode:            mode,
		flushPolicy:     param.FlushPolicy,
		minInterval:     param.MinInterval,
		schemaVersion:   param.SchemaVersion,
		fullSyncOnDirty: param.FullSyncOnDirty,
		observers:       make(map[SyncObserverRef]EntitySyncObserver, len(param.InitialObserverRefs)),
		packer:          param.Packer,
		packCacheConf:   param.PackCache,
	}
	for _, ref := range param.InitialObserverRefs {
		ref = ref.Normalize()
		if !ref.Empty() {
			s.observers[ref] = EntitySyncObserver{ObserverID: ref.PlayerID(), Ref: ref}
		}
	}
	return s
}

func (s *EntitySyncState) Enabled() bool {
	return s != nil && s.enabled
}

func (s *EntitySyncState) EntityID() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entityID
}

func (s *EntitySyncState) Topic() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.topic
}

func (s *EntitySyncState) Version() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

func (s *EntitySyncState) SetPacker(packer EntitySyncPacker) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.packer = packer
	s.clearPackCacheLocked()
	s.mu.Unlock()
}

func (s *EntitySyncState) SetPackCache(config EntitySyncPackCacheConfig) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.packCacheConf = config
	s.clearPackCacheLocked()
	s.mu.Unlock()
}

func (s *EntitySyncState) AddObserverRef(ref SyncObserverRef) (SyncPacket, bool) {
	ref = ref.Normalize()
	if s == nil || ref.Empty() {
		return SyncPacket{}, false
	}
	s.mu.Lock()
	if !s.enabled {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	if _, ok := s.observers[ref]; ok {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	s.observers[ref] = EntitySyncObserver{ObserverID: ref.PlayerID(), Ref: ref}
	snap := s.snapshotLocked()
	s.mu.Unlock()
	return s.packEnter(snap, ref), true
}

// TryAddObserverRefFromCachedEnter adds a new observer only when the enter
// payload is already cached. Callers can use it on hot visibility paths to
// avoid locking the whole entity just to reuse a clean, observer-agnostic pack.
func (s *EntitySyncState) TryAddObserverRefFromCachedEnter(ref SyncObserverRef) (SyncPacket, bool) {
	ref = ref.Normalize()
	if s == nil || ref.Empty() {
		return SyncPacket{}, false
	}
	s.mu.Lock()
	if !s.enabled {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	if _, ok := s.observers[ref]; ok {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	if !s.packCacheConf.Enabled || len(s.packCache) == 0 {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	packet, ok := s.packCache[s.packCacheKeyLocked(syncPackCacheKindEnter, ref)]
	if !ok {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	s.observers[ref] = EntitySyncObserver{ObserverID: ref.PlayerID(), Ref: ref}
	snap := s.snapshotLocked()
	s.mu.Unlock()
	return fillSyncPacket(cloneSyncPacket(packet), snap, ref, SyncPacketEnter, 0), true
}

func (s *EntitySyncState) RemoveObserverRef(ref SyncObserverRef) (SyncPacket, bool) {
	ref = ref.Normalize()
	if s == nil || ref.Empty() {
		return SyncPacket{}, false
	}
	s.mu.Lock()
	if !s.enabled {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	if _, ok := s.observers[ref]; !ok {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	delete(s.observers, ref)
	if len(s.observers) == 0 {
		s.dirtyMask = 0
		s.fullDirty = false
		s.fullReason = SyncFullReasonNone
	}
	snap := s.snapshotLocked()
	s.mu.Unlock()
	return s.packLeave(snap, ref), true
}

func (s *EntitySyncState) HasObserverRef(ref SyncObserverRef) bool {
	ref = ref.Normalize()
	if s == nil || ref.Empty() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.observers[ref]
	return ok
}

func (s *EntitySyncState) HasAnyObserver() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.observers) > 0
}

func (s *EntitySyncState) ObserverRefs() []SyncObserverRef {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneObserverRefs(s.observers)
}

func (s *EntitySyncState) MarkDirty(mask uint64) {
	if s == nil || mask == 0 {
		return
	}
	s.mu.Lock()
	marked := false
	entityID := s.entityID
	if s.enabled {
		s.clearPackCacheLocked()
		if len(s.observers) > 0 {
			s.dirtyMask |= mask
			marked = true
		}
	}
	flushNow := s.enabled && s.flushPolicy == SyncFlushImmediate && GetEntitySyncSink() != nil
	s.mu.Unlock()
	if marked {
		scheduleEntitySyncDirty(s, entityID)
	}
	if flushNow {
		s.FlushToSink()
	}
}

func (s *EntitySyncState) MarkFullDirty(reason uint32) {
	if s == nil {
		return
	}
	s.mu.Lock()
	marked := false
	entityID := s.entityID
	if s.enabled {
		s.clearPackCacheLocked()
	}
	if s.enabled && len(s.observers) > 0 {
		if reason == SyncFullReasonNone {
			reason = SyncFullReasonDirty
		}
		s.fullDirty = true
		s.fullReason = reason
		s.dirtyMask = 0
		marked = true
	}
	flushNow := s.enabled && s.flushPolicy == SyncFlushImmediate && GetEntitySyncSink() != nil
	s.mu.Unlock()
	if marked {
		scheduleEntitySyncDirty(s, entityID)
	}
	if flushNow {
		s.FlushToSink()
	}
}

func (s *EntitySyncState) DirtyMask() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirtyMask
}

func (s *EntitySyncState) PendingDirty() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirtyMask != 0 || s.fullDirty
}

func (s *EntitySyncState) TakeDirty() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mask := s.dirtyMask
	s.dirtyMask = 0
	if mask != 0 {
		s.fullDirty = false
		s.fullReason = SyncFullReasonNone
	}
	return mask
}

func (s *EntitySyncState) Flush() []SyncPacket {
	return s.flush(false)
}

func (s *EntitySyncState) PackFullForObserver(ref SyncObserverRef, reason uint32) (SyncPacket, bool) {
	ref = ref.Normalize()
	if s == nil || ref.Empty() {
		return SyncPacket{}, false
	}
	if reason == SyncFullReasonNone {
		reason = SyncFullReasonResync
	}
	s.mu.Lock()
	if !s.enabled {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	if _, ok := s.observers[ref]; !ok {
		s.mu.Unlock()
		return SyncPacket{}, false
	}
	baseVersion := s.version
	s.version++
	s.lastFlush = fctx.Now()
	snap := s.snapshotLocked()
	snap.baseVersion = baseVersion
	s.mu.Unlock()
	return s.packFullUpdate(snap, ref, reason), true
}

func (s *EntitySyncState) flush(ignoreMinInterval bool) []SyncPacket {
	if s == nil {
		return nil
	}
	now := fctx.Now()
	s.mu.Lock()
	if !s.enabled || (s.dirtyMask == 0 && !s.fullDirty) {
		s.mu.Unlock()
		return nil
	}
	if !ignoreMinInterval && s.minInterval > 0 && !s.lastFlush.IsZero() && now.Sub(s.lastFlush) < s.minInterval {
		s.mu.Unlock()
		return nil
	}
	full := s.fullDirty || (s.fullSyncOnDirty && s.dirtyMask != 0)
	reason := s.fullReason
	if full && reason == SyncFullReasonNone {
		reason = SyncFullReasonDirty
	}
	mask := s.dirtyMask
	baseVersion := s.version
	s.dirtyMask = 0
	s.fullDirty = false
	s.fullReason = SyncFullReasonNone
	s.version++
	s.lastFlush = now
	observerRefs := cloneObserverRefs(s.observers)
	snap := s.snapshotLocked()
	snap.baseVersion = baseVersion
	s.mu.Unlock()

	if len(observerRefs) == 0 {
		return nil
	}
	packets := make([]SyncPacket, 0, len(observerRefs))
	for _, ref := range observerRefs {
		packet := SyncPacket{}
		if full {
			packet = s.packFullUpdate(snap, ref, reason)
		} else {
			packet = s.packUpdate(snap, ref, mask)
		}
		packets = append(packets, packet)
	}
	if !full {
		s.clearPackCacheKind(syncPackCacheKindUpdate)
	}
	return packets
}

func (s *EntitySyncState) FlushTo(sink EntitySyncSink) []SyncPacket {
	packets := s.Flush()
	if len(packets) == 0 {
		return nil
	}
	if sink != nil {
		sink.EnqueueBatch(packets)
	}
	return packets
}

func (s *EntitySyncState) FlushToSink() []SyncPacket {
	return s.FlushTo(GetEntitySyncSink())
}

func (s *EntitySyncState) snapshotLocked() syncPackSnapshot {
	return syncPackSnapshot{
		entityID:      s.entityID,
		entityKind:    int32(GetEntityKindFromID(s.entityID)),
		topic:         s.topic,
		version:       s.version,
		baseVersion:   s.version,
		schemaVersion: s.schemaVersion,
		packer:        s.packer,
	}
}

func (s *EntitySyncState) packEnter(snap syncPackSnapshot, ref SyncObserverRef) SyncPacket {
	var packet SyncPacket
	if p, ok := s.loadCachedRawEnter(ref); ok {
		packet = p
	} else if snap.packer != nil {
		p, err := snap.packer.PackSyncEnter(ref)
		if err == nil {
			packet = p
			s.storeCachedRawEnter(ref, packet)
		}
	}
	return fillSyncPacket(packet, snap, ref, SyncPacketEnter, 0)
}

func (s *EntitySyncState) packUpdate(snap syncPackSnapshot, ref SyncObserverRef, mask uint64) SyncPacket {
	var packet SyncPacket
	if p, ok := s.loadCachedRawUpdate(ref, mask); ok {
		packet = p
	} else if snap.packer != nil {
		p, err := snap.packer.PackSyncUpdate(ref, mask)
		if err == nil {
			packet = p
			s.storeCachedRawUpdate(ref, mask, packet)
		}
	}
	return fillSyncPacket(packet, snap, ref, SyncPacketUpdate, mask)
}

func (s *EntitySyncState) packFullUpdate(snap syncPackSnapshot, ref SyncObserverRef, reason uint32) SyncPacket {
	packet := s.packEnter(snap, ref)
	packet.Type = SyncPacketUpdate
	packet.Mask = SyncMaskFull
	packet.Full = true
	packet.Reason = reason
	packet.Version = snap.version
	packet.BaseVersion = snap.baseVersion
	packet.SchemaVersion = snap.schemaVersion
	return packet
}

func (s *EntitySyncState) packLeave(snap syncPackSnapshot, ref SyncObserverRef) SyncPacket {
	var packet SyncPacket
	if snap.packer != nil {
		p, err := snap.packer.PackSyncLeave(ref)
		if err == nil {
			packet = p
		}
	}
	return fillSyncPacket(packet, snap, ref, SyncPacketLeave, 0)
}

func (s *EntitySyncState) loadCachedRawEnter(ref SyncObserverRef) (SyncPacket, bool) {
	if s == nil {
		return SyncPacket{}, false
	}
	ref = ref.Normalize()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.packCacheConf.Enabled || len(s.packCache) == 0 {
		return SyncPacket{}, false
	}
	packet, ok := s.packCache[s.packCacheKeyLocked(syncPackCacheKindEnter, ref)]
	if !ok {
		return SyncPacket{}, false
	}
	return cloneSyncPacket(packet), true
}

func (s *EntitySyncState) storeCachedRawEnter(ref SyncObserverRef, packet SyncPacket) {
	if s == nil {
		return
	}
	ref = ref.Normalize()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.packCacheConf.Enabled {
		return
	}
	if s.packCache == nil {
		s.packCache = make(map[syncPackCacheKey]SyncPacket, 1)
	}
	s.packCache[s.packCacheKeyLocked(syncPackCacheKindEnter, ref)] = sanitizeCachedRawSyncPacket(packet)
}

func (s *EntitySyncState) loadCachedRawUpdate(ref SyncObserverRef, mask uint64) (SyncPacket, bool) {
	if s == nil || mask == 0 {
		return SyncPacket{}, false
	}
	ref = ref.Normalize()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.packCacheConf.Enabled || len(s.packCache) == 0 {
		return SyncPacket{}, false
	}
	packet, ok := s.packCache[s.packCacheKeyWithMaskLocked(syncPackCacheKindUpdate, ref, mask)]
	if !ok {
		return SyncPacket{}, false
	}
	return cloneSyncPacket(packet), true
}

func (s *EntitySyncState) storeCachedRawUpdate(ref SyncObserverRef, mask uint64, packet SyncPacket) {
	if s == nil || mask == 0 {
		return
	}
	ref = ref.Normalize()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.packCacheConf.Enabled {
		return
	}
	if s.packCache == nil {
		s.packCache = make(map[syncPackCacheKey]SyncPacket, 1)
	}
	s.packCache[s.packCacheKeyWithMaskLocked(syncPackCacheKindUpdate, ref, mask)] = sanitizeCachedRawSyncPacket(packet)
}

func (s *EntitySyncState) packCacheKeyLocked(kind syncPackCacheKind, ref SyncObserverRef) syncPackCacheKey {
	return s.packCacheKeyWithMaskLocked(kind, ref, 0)
}

func (s *EntitySyncState) packCacheKeyWithMaskLocked(kind syncPackCacheKind, ref SyncObserverRef, mask uint64) syncPackCacheKey {
	ref = ref.Normalize()
	if s.packCacheConf.ObserverAgnostic {
		ref = SyncObserverRef{}
	}
	return syncPackCacheKey{kind: kind, observer: ref, mask: mask}
}

func (s *EntitySyncState) clearPackCacheLocked() {
	if len(s.packCache) == 0 {
		return
	}
	clear(s.packCache)
}

func (s *EntitySyncState) clearPackCacheKind(kind syncPackCacheKind) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.packCache) == 0 {
		return
	}
	for key := range s.packCache {
		if key.kind == kind {
			delete(s.packCache, key)
		}
	}
}

func sanitizeCachedRawSyncPacket(packet SyncPacket) SyncPacket {
	packet = cloneSyncPacket(packet)
	packet.Observer = SyncObserverRef{}
	packet.ObserverID = 0
	packet.Type = SyncPacketTypeNone
	packet.Version = 0
	packet.BaseVersion = 0
	packet.Mask = 0
	packet.Full = false
	packet.SchemaVersion = 0
	packet.Reason = SyncFullReasonNone
	return packet
}

func cloneSyncPacket(packet SyncPacket) SyncPacket {
	return packet
}

func fillSyncPacket(packet SyncPacket, snap syncPackSnapshot, ref SyncObserverRef, packetType SyncPacketType, mask uint64) SyncPacket {
	ref = ref.Normalize()
	if packet.Topic == "" {
		packet.Topic = snap.topic
	}
	if packet.EntityID == 0 {
		packet.EntityID = snap.entityID
	}
	if packet.EntityKind == 0 {
		packet.EntityKind = snap.entityKind
	}
	if packet.Observer.Empty() {
		packet.Observer = ref
	}
	if packet.ObserverID == 0 {
		packet.ObserverID = packet.Observer.PlayerID()
	}
	if packet.Observer.Empty() && packet.ObserverID != 0 {
		packet.Observer = NewPlayerSyncObserver(packet.ObserverID)
	}
	if packet.Type == SyncPacketTypeNone {
		packet.Type = packetType
	}
	if packet.Version == 0 {
		packet.Version = snap.version
	}
	if packet.BaseVersion == 0 {
		packet.BaseVersion = snap.baseVersion
	}
	if packet.SchemaVersion == 0 {
		packet.SchemaVersion = snap.schemaVersion
	}
	if packet.Mask == 0 {
		packet.Mask = mask
	}
	return packet
}

func cloneObserverRefs(observers map[SyncObserverRef]EntitySyncObserver) []SyncObserverRef {
	out := make([]SyncObserverRef, 0, len(observers))
	for ref := range observers {
		if !ref.Empty() {
			out = append(out, ref.Normalize())
		}
	}
	return out
}

func init() {
	RegisterOnEntityRelease(flushEntitySyncOnRelease)
}

func flushEntitySyncOnRelease(ent IThreadSafeEntity) {
	scheduler := GetEntitySyncScheduler()
	if scheduler == nil || ent == nil || ent.Base() == nil {
		return
	}
	syncState := ent.Base().Sync()
	if syncState == nil {
		return
	}
	packets := syncState.flush(true)
	if len(packets) > 0 {
		scheduler.EnqueueBatch(packets)
	}
}

func scheduleEntitySyncDirty(state *EntitySyncState, entityID int64) {
	scheduler := GetEntitySyncScheduler()
	if scheduler == nil {
		return
	}
	if entitySyncLockedInCurrentGuard(entityID) {
		return
	}
	scheduler.MarkDirtyState(state)
}

func entitySyncLockedInCurrentGuard(entityID int64) bool {
	if entityID == 0 {
		return false
	}
	scope := CurrentGuardScope()
	if scope == nil || scope.Guard() == nil {
		return false
	}
	_, ok := scope.Guard().Entities()[entityID]
	return ok
}
