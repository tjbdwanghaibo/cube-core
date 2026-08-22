package entity

import (
	"github.com/tjbdwanghaibo/cube-core/misc"
	"log/slog"
	"sync"
)

// Entity group constants for lock ordering (deadlock prevention).
const (
	// Remote is first because top-level dispatch acquires remote ownership and
	// distributed locks before any local entity mutex.
	EntityGroupRemote = iota
	EntityGroupPlayer
	EntityGroupAlliance
	EntityGroupOther
	EntityGroupCnt
)

// GetEntityGroupFunc is the application-level hook that maps an EntityCategory to an entity group.
// If nil, all non-remote entities fall into EntityGroupOther.
var GetEntityGroupFunc func(category EntityCategory) int

// GetEntityGroup extracts entity group from GUId for lock ordering.
func GetEntityGroup(guid int64) int {
	if IsRemoteCapableEntityID(guid) {
		return EntityGroupRemote
	}
	if GetEntityGroupFunc != nil {
		category := GetEntityCategoryFromID(guid)
		return GetEntityGroupFunc(category)
	}
	return EntityGroupOther
}

type entityReleaseHook struct {
	id uint64
	fn func(IThreadSafeEntity)
}

func (m *EntityManager) RegisterOnEntityRelease(hook func(IThreadSafeEntity)) func() {
	if m == nil || hook == nil {
		return func() {}
	}
	m.hookMu.Lock()
	m.nextHookID++
	id := m.nextHookID
	m.releaseHooks = append(m.releaseHooks, entityReleaseHook{id: id, fn: hook})
	m.hookMu.Unlock()
	return func() {
		m.hookMu.Lock()
		defer m.hookMu.Unlock()
		for i, item := range m.releaseHooks {
			if item.id == id {
				m.releaseHooks = append(m.releaseHooks[:i], m.releaseHooks[i+1:]...)
				return
			}
		}
	}
}

func runOnEntityRelease(ent IThreadSafeEntity) {
	if ent == nil || ent.Base() == nil || ent.Base().owningManager() == nil {
		return
	}
	manager := ent.Base().owningManager()
	manager.hookMu.RLock()
	hooks := append([]entityReleaseHook(nil), manager.releaseHooks...)
	manager.hookMu.RUnlock()
	for _, hook := range hooks {
		hook.fn(ent)
	}
}

// EntityGuard manages per-goroutine entity locks with priority-based deadlock avoidance.
type EntityGuard struct {
	eMap        map[int64]IThreadSafeEntity
	postRelease []func()
}

type GuardScope struct {
	name  string
	guard *EntityGuard
	prev  *GuardScope
}

var guardPool = sync.Pool{
	New: func() interface{} {
		return newEntityGuard()
	},
}

var guardScopes sync.Map // map[int64]*GuardScope

func NewGuardScope(name string) (*GuardScope, func()) {
	prev := CurrentGuardScope()
	scope := &GuardScope{
		name:  name,
		guard: guardPool.Get().(*EntityGuard),
		prev:  prev,
	}
	storeGuardScope(scope)
	return scope, func() {
		releaseGuardScope(scope)
	}
}

func WithGuardScope(name string, fn func(*GuardScope) error) error {
	scope, release := NewGuardScope(name)
	defer release()
	if fn == nil {
		return nil
	}
	return fn(scope)
}

func CurrentGuardScope() *GuardScope {
	if value, ok := guardScopes.Load(misc.GoID()); ok {
		if scope, ok := value.(*GuardScope); ok {
			return scope
		}
	}
	return nil
}

func (s *GuardScope) Name() string {
	if s == nil {
		return ""
	}
	return s.name
}

func (s *GuardScope) Guard() *EntityGuard {
	if s == nil {
		return nil
	}
	return s.guard
}

func storeGuardScope(scope *GuardScope) {
	if scope == nil {
		guardScopes.Delete(misc.GoID())
		return
	}
	guardScopes.Store(misc.GoID(), scope)
}

func releaseGuardScope(scope *GuardScope) {
	if scope == nil {
		return
	}
	cur := CurrentGuardScope()
	if cur != scope {
		slog.Warn("entity guard scope release out of order", "scope", scope.name)
		return
	}
	guard := scope.guard
	callbacks := guard.releaseEntities()
	if scope.prev != nil {
		storeGuardScope(scope.prev)
	} else {
		guardScopes.Delete(misc.GoID())
	}
	scope.guard = nil
	scope.prev = nil
	guard.runPostRelease(callbacks)
	guard.clean()
	guardPool.Put(guard)
}

// GetEntityGuard returns the EntityGuard for the current entity guard scope.
// Without a scope, callers receive a standalone guard and must release it.
func GetEntityGuard() *EntityGuard {
	if scope := CurrentGuardScope(); scope != nil {
		return scope.guard
	}
	return guardPool.Get().(*EntityGuard)
}

// EntityGuardRelease releases the guard.
// If the guard is owned by the current scope, it's a no-op (scope release handles it).
// Otherwise, releases immediately.
func EntityGuardRelease(guard *EntityGuard) {
	if guard == nil {
		return
	}
	if scope := CurrentGuardScope(); scope != nil && scope.guard == guard {
		return
	}
	doGuardReleaseInCurrentGoroutine(guard)
}

func doGuardReleaseInCurrentGoroutine(guard *EntityGuard) {
	prev := CurrentGuardScope()
	temp := &GuardScope{
		name:  "release",
		guard: guard,
		prev:  prev,
	}
	storeGuardScope(temp)
	releaseGuardScope(temp)
}

func newEntityGuard() *EntityGuard {
	return &EntityGuard{
		eMap: make(map[int64]IThreadSafeEntity),
	}
}

func (e *EntityGuard) clean() {
	clear(e.eMap)
	e.postRelease = nil
}

// RequireEntity acquires the entity lock. Returns true on success.
func (e *EntityGuard) RequireEntity(ent IThreadSafeEntity) bool {
	if ent == nil {
		return false
	}
	gId := ent.GUId()
	mu := ent.GetMutex()
	if gId == 0 || mu == nil {
		return false
	}

	if _, exists := e.eMap[gId]; exists {
		return true
	}

	mu.Lock()
	if ent.IsClear() || ent.IsRemoved() {
		mu.Unlock()
		return false
	}
	e.eMap[gId] = ent
	return true
}

// CheckContainAllLock checks if all entities in es are already locked or safe to lock.
// Lock order follows the application group order from lower value to higher
// value: Remote -> Player -> Alliance -> Other. Once a later group is held,
// callers must not acquire an earlier or same-level group through cast.
func (e *EntityGuard) CheckContainAllLock(es []IThreadSafeEntity) bool {
	if len(es) == 0 || len(e.eMap) == 0 {
		return true
	}

	maxLockedGroup := -1
	for k := range e.eMap {
		group := GetEntityGroup(k)
		if group > maxLockedGroup {
			maxLockedGroup = group
		}
	}

	for _, en := range es {
		guid := en.GUId()
		if _, exist := e.eMap[guid]; exist {
			continue
		}
		requiredGroup := GetEntityGroup(guid)
		if requiredGroup > maxLockedGroup {
			continue
		}
		return false
	}
	return true
}

// CheckContainAllIDs performs the same lock-order validation before entities
// are loaded. Remote cast must use this preflight before acquiring a
// distributed lock.
func (e *EntityGuard) CheckContainAllIDs(ids []int64) bool {
	if len(ids) == 0 || len(e.eMap) == 0 {
		return true
	}
	maxLockedGroup := -1
	for id := range e.eMap {
		if group := GetEntityGroup(id); group > maxLockedGroup {
			maxLockedGroup = group
		}
	}
	for _, id := range ids {
		if _, exists := e.eMap[id]; exists {
			continue
		}
		if GetEntityGroup(id) <= maxLockedGroup {
			return false
		}
	}
	return true
}

func (e *EntityGuard) AppendPostRelease(f func()) {
	if f != nil {
		e.postRelease = append(e.postRelease, f)
	}
}

func (e *EntityGuard) GuardEntity(ent IThreadSafeEntity) {
	e.eMap[ent.GUId()] = ent
}

func (e *EntityGuard) ReleaseEntity(id int64) {
	ent := e.eMap[id]
	if ent != nil {
		e.doReleaseEntity(ent)
	}
}

func (e *EntityGuard) ReleaseAll() {
	callbacks := e.releaseEntities()
	e.runPostRelease(callbacks)
}

func (e *EntityGuard) releaseEntities() []func() {
	for _, ent := range e.eMap {
		e.safeReleaseEntity(ent)
	}
	callbacks := e.postRelease
	e.postRelease = nil
	return callbacks
}

func (e *EntityGuard) runPostRelease(callbacks []func()) {
	for _, f := range callbacks {
		if f != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("entity guard post-release callback panic", "err", r)
					}
				}()
				f()
			}()
		}
	}
}

func (e *EntityGuard) safeReleaseEntity(ent IThreadSafeEntity) {
	defer func() {
		if r := recover(); r != nil {
			id := int64(0)
			if ent != nil {
				id = ent.GUId()
			}
			slog.Error("entity release hook panic", "id", id, "err", r)
		}
	}()
	e.doReleaseEntity(ent)
}

func (e *EntityGuard) doReleaseEntity(ent IThreadSafeEntity) {
	mu := ent.GetMutex()
	defer func() {
		delete(e.eMap, ent.GUId())
		mu.Unlock()
	}()

	// Trigger release hooks while lock is held.
	runOnEntityRelease(ent)
}

// Entities returns all currently guarded entities.
func (e *EntityGuard) Entities() map[int64]IThreadSafeEntity {
	return e.eMap
}
