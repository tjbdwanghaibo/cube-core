package nest

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

var (
	ErrCastGetterNotSet  = errors.New("nest: cast getter not set")
	ErrCastNoContext     = errors.New("nest: cast requires entity context")
	ErrCastInvalidTarget = errors.New("nest: invalid cast target")
	ErrCastDeadlockRisk  = errors.New("nest: cast deadlock risk")
	ErrCastTypeMismatch  = errors.New("nest: cast type mismatch")
)

// CastTarget describes an entity to lock in the current entity context.
type CastTarget struct {
	ID int64
}

// NewCastTarget builds a full-ID cast target.
func NewCastTarget(id int64) CastTarget {
	return CastTarget{ID: id}
}

// CastOne retrieves and locks one entity in the current entity context.
func CastOne[E entity.IThreadSafeEntity](id int64) (E, error) {
	return CastTargetOne[E](NewCastTarget(id))
}

// CastTargetOne retrieves and locks one explicit target.
func CastTargetOne[E entity.IThreadSafeEntity](target CastTarget) (E, error) {
	var zero E
	es, err := CastMulti(target)
	if err != nil {
		return zero, err
	}
	if len(es) != 1 || es[0] == nil {
		return zero, ErrEntityNotFound
	}
	ret, ok := es[0].(E)
	if !ok {
		return zero, fmt.Errorf("%w: id=%d entity=%T", ErrCastTypeMismatch, target.ID, es[0])
	}
	return ret, nil
}

func CastTwo[E1, E2 entity.IThreadSafeEntity](t1, t2 CastTarget) (E1, E2, error) {
	var zero1 E1
	var zero2 E2
	es, err := CastMulti(t1, t2)
	if err != nil {
		return zero1, zero2, err
	}
	if len(es) != 2 || es[0] == nil || es[1] == nil {
		return zero1, zero2, ErrEntityNotFound
	}
	e1, ok := es[0].(E1)
	if !ok {
		return zero1, zero2, fmt.Errorf("%w: id=%d entity=%T", ErrCastTypeMismatch, t1.ID, es[0])
	}
	e2, ok := es[1].(E2)
	if !ok {
		return zero1, zero2, fmt.Errorf("%w: id=%d entity=%T", ErrCastTypeMismatch, t2.ID, es[1])
	}
	return e1, e2, nil
}

func CastThree[E1, E2, E3 entity.IThreadSafeEntity](t1, t2, t3 CastTarget) (E1, E2, E3, error) {
	var zero1 E1
	var zero2 E2
	var zero3 E3
	es, err := CastMulti(t1, t2, t3)
	if err != nil {
		return zero1, zero2, zero3, err
	}
	if len(es) != 3 || es[0] == nil || es[1] == nil || es[2] == nil {
		return zero1, zero2, zero3, ErrEntityNotFound
	}
	e1, ok := es[0].(E1)
	if !ok {
		return zero1, zero2, zero3, fmt.Errorf("%w: id=%d entity=%T", ErrCastTypeMismatch, t1.ID, es[0])
	}
	e2, ok := es[1].(E2)
	if !ok {
		return zero1, zero2, zero3, fmt.Errorf("%w: id=%d entity=%T", ErrCastTypeMismatch, t2.ID, es[1])
	}
	e3, ok := es[2].(E3)
	if !ok {
		return zero1, zero2, zero3, fmt.Errorf("%w: id=%d entity=%T", ErrCastTypeMismatch, t3.ID, es[2])
	}
	return e1, e2, e3, nil
}

// CastMulti retrieves and locks targets in the current entity guard scope.
func CastMulti(targets ...CastTarget) ([]entity.IThreadSafeEntity, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: empty targets", ErrCastInvalidTarget)
	}
	if entity.CurrentGuardScope() == nil {
		return nil, ErrCastNoContext
	}
	getter := globalCastGetter()
	if getter == nil {
		return nil, ErrCastGetterNotSet
	}

	guard := entity.GetEntityGuard()
	metas := make([]entity.EntityIDMeta, len(targets))
	ids := make([]int64, len(targets))
	categories := make([]entity.EntityCategory, len(targets))
	for i, target := range targets {
		if target.ID == 0 {
			return nil, fmt.Errorf("%w: index=%d id=0", ErrCastInvalidTarget, i)
		}
		fullID, err := entity.NormalizeFullID(target.ID, entity.EntityKindNone)
		if err != nil {
			return nil, fmt.Errorf("%w: index=%d id=%d: %v", ErrCastInvalidTarget, i, target.ID, err)
		}
		meta := entity.ResolveEntityID(fullID)
		metas[i] = meta
		ids[i] = meta.FullID
		categories[i] = meta.Category
	}

	remoteRelease, err := prepareCastRemoteEntities(guard, metas)
	if err != nil {
		return nil, err
	}
	prepared := remoteRelease != nil
	if prepared {
		defer func() {
			if remoteRelease != nil {
				remoteRelease()
			}
		}()
	}

	es, err := getter.GetMany(ids, categories)
	if err != nil {
		return nil, err
	}
	if len(es) != len(targets) {
		return nil, fmt.Errorf("%w: getter returned %d entities for %d targets", ErrCastInvalidTarget, len(es), len(targets))
	}

	lockEs := make([]entity.IThreadSafeEntity, 0, len(es))
	for i, e := range es {
		if e == nil {
			return nil, fmt.Errorf("%w: index=%d id=%d", ErrEntityNotFound, i, targets[i].ID)
		}
		lockEs = append(lockEs, e)
	}
	if !guard.CheckContainAllLock(lockEs) {
		return nil, fmt.Errorf("%w: ids=%v", ErrCastDeadlockRisk, ids)
	}
	SortEntity(lockEs)

	lockedNow := make([]entity.IThreadSafeEntity, 0, len(lockEs))
	for _, e := range lockEs {
		if _, exists := guard.Entities()[e.GUId()]; exists {
			continue
		}
		if !e.Touch() {
			releaseCastEntities(guard, lockedNow)
			return nil, ErrEntityNotFound
		}
		if !guard.RequireEntity(e) {
			e.UnTouch()
			releaseCastEntities(guard, lockedNow)
			return nil, ErrLockTimeout
		}
		e.UnTouch()
		lockedNow = append(lockedNow, e)
	}
	if prepared {
		release := remoteRelease
		remoteRelease = nil
		guard.AppendPostRelease(release)
	}
	return es, nil
}

// ReleaseCast releases one cast entity before the current context ends.
func ReleaseCast(e entity.IThreadSafeEntity) {
	if e == nil || entity.CurrentGuardScope() == nil {
		return
	}
	entity.GetEntityGuard().ReleaseEntity(e.GUId())
}

func releaseCastEntities(guard *entity.EntityGuard, es []entity.IThreadSafeEntity) {
	for _, e := range es {
		guard.ReleaseEntity(e.GUId())
	}
}

func prepareCastRemoteEntities(guard *entity.EntityGuard, metas []entity.EntityIDMeta) (func(), error) {
	remoteIDs := make([]int64, 0, len(metas))
	seen := make(map[int64]struct{}, len(metas))
	for _, meta := range metas {
		if !shouldPrepareRemoteID(meta) {
			continue
		}
		if _, locked := guard.Entities()[meta.FullID]; locked {
			continue
		}
		if _, ok := seen[meta.FullID]; ok {
			continue
		}
		seen[meta.FullID] = struct{}{}
		remoteIDs = append(remoteIDs, meta.FullID)
	}
	if len(remoteIDs) == 0 {
		return nil, nil
	}
	release, _, err := entity.PrepareRemoteEntities(remoteIDs)
	return release, err
}

var globalGetter struct {
	mu     sync.RWMutex
	getter entity.Getter
}

// InitGlobalGetter sets the getter used by Cast* helpers.
func InitGlobalGetter(getter entity.Getter) {
	globalGetter.mu.Lock()
	globalGetter.getter = getter
	globalGetter.mu.Unlock()
}

func globalCastGetter() entity.Getter {
	globalGetter.mu.RLock()
	defer globalGetter.mu.RUnlock()
	return globalGetter.getter
}
