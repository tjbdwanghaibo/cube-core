package entity

import (
	"fmt"

	flog "github.com/tjbdwanghaibo/cube-core/log"
)

// Create builds and publishes an entity while holding its mutex until the
// short-lived guard scope is released. Later mutations must enter through Nest.
func (m *EntityManager) Create(param *EntityCreateParam) (IThreadSafeEntity, error) {
	if m == nil {
		return nil, ErrEntityNotManaged
	}
	var result IThreadSafeEntity
	err := WithGuardScope("entity_create", func(scope *GuardScope) error {
		value, err := m.CreateInScope(scope, param)
		if err != nil {
			return err
		}
		result = value
		return nil
	})
	return result, err
}

// CreateInScope builds an entity, acquires its mutex in the existing
// deterministic guard scope, and only then publishes it. This ordering keeps a
// newly visible entity from being observed before its creator owns the lock.
func (m *EntityManager) CreateInScope(scope *GuardScope, param *EntityCreateParam) (IThreadSafeEntity, error) {
	if m == nil {
		return nil, ErrEntityNotManaged
	}
	if scope == nil || scope.guard == nil {
		return nil, fmt.Errorf("entity guard scope is required")
	}
	value, err := m.build(param)
	if err != nil {
		return nil, err
	}
	if !scope.guard.RequireEntity(value) {
		return nil, fmt.Errorf("entity guard scope lock failed: %d", value.ID())
	}
	if err := m.TryAdd(value); err != nil {
		return nil, err
	}
	lifetime := EntityLifetimeDefault
	if base := value.Base(); base != nil {
		lifetime = base.Lifetime()
	}
	flog.Debug("entity: created", "id", value.ID(), "category", value.GetEntityCategory(), "kind", value.GetEntityKind(), "lifetime", lifetime)
	return value, nil
}

func (m *EntityManager) build(param *EntityCreateParam) (IThreadSafeEntity, error) {
	if m == nil {
		return nil, ErrEntityNotManaged
	}
	if param == nil {
		return nil, fmt.Errorf("entity create param is nil")
	}
	m.configMu.RLock()
	defer m.configMu.RUnlock()
	if param.IsCreate && param.Id == 0 && param.UniqueID == 0 {
		if m.idGen == nil {
			return nil, ErrIDGeneratorRequired
		}
		rawID, err := m.idGen()
		if err != nil {
			return nil, fmt.Errorf("generate entity id: %w", err)
		}
		if err := param.setRawID(int64(rawID), param.Kind); err != nil {
			return nil, err
		}
	}
	if param.Mutex == nil {
		param.Mutex = m.locks.GetLock(param.Id)
	}
	if param.Mutex.LockId() != param.Id {
		return nil, fmt.Errorf("entity manager: mutex id=%d does not match entity id=%d", param.Mutex.LockId(), param.Id)
	}
	value, err := BuildEntity(param)
	if err != nil {
		return nil, err
	}
	return value, nil
}
