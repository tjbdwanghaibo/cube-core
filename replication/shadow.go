package replication

import "sync"

type shadowObject struct {
	archetype  uint16
	components map[uint16]ComponentState
}

// ShadowStore owns replication-only copies of business state. Network workers
// capture this store and never read Entity fields directly.
type ShadowStore struct {
	mu      sync.RWMutex
	limits  Limits
	objects map[ObjectRef]*shadowObject
}

func NewShadowStore(limits Limits) *ShadowStore {
	return &ShadowStore{
		limits:  normalizeLimits(limits),
		objects: make(map[ObjectRef]*shadowObject),
	}
}

func (s *ShadowStore) UpsertObject(ref ObjectRef, archetype uint16) error {
	if s == nil || !ref.Valid() || archetype == 0 {
		return ErrInvalidObjectRef
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if obj := s.objects[ref]; obj != nil {
		obj.archetype = archetype
		return nil
	}
	if len(s.objects) >= s.limits.MaxObjects {
		return ErrObjectLimit
	}
	s.objects[ref] = &shadowObject{archetype: archetype, components: make(map[uint16]ComponentState)}
	return nil
}

func (s *ShadowStore) RemoveObject(ref ObjectRef) bool {
	if s == nil || !ref.Valid() {
		return false
	}
	s.mu.Lock()
	_, ok := s.objects[ref]
	delete(s.objects, ref)
	s.mu.Unlock()
	return ok
}

func (s *ShadowStore) SetComponent(ref ObjectRef, component ComponentState) error {
	if s == nil || !ref.Valid() || component.TypeID == 0 || component.SchemaVersion == 0 {
		return ErrInvalidFrame
	}
	if len(component.Data) > s.limits.MaxComponentBytes {
		return ErrComponentTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.objects[ref]
	if obj == nil {
		return ErrObjectNotFound
	}
	if _, exists := obj.components[component.TypeID]; !exists && len(obj.components) >= s.limits.MaxComponentsPerObject {
		return ErrComponentLimit
	}
	component.Data = append([]byte(nil), component.Data...)
	obj.components[component.TypeID] = component
	return nil
}

func (s *ShadowStore) RemoveComponent(ref ObjectRef, typeID uint16) bool {
	if s == nil || !ref.Valid() || typeID == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.objects[ref]
	if obj == nil {
		return false
	}
	_, ok := obj.components[typeID]
	delete(obj.components, typeID)
	return ok
}

func (s *ShadowStore) Capture(meta SnapshotMeta) (Snapshot, error) {
	if s == nil {
		return Snapshot{}, ErrInvalidFrame
	}
	s.mu.RLock()
	objects := make([]ObjectState, 0, len(s.objects))
	for ref, obj := range s.objects {
		state := ObjectState{Ref: ref, Archetype: obj.archetype, Components: make([]ComponentState, 0, len(obj.components))}
		for _, component := range obj.components {
			state.Components = append(state.Components, ComponentState{
				TypeID:        component.TypeID,
				SchemaVersion: component.SchemaVersion,
				Data:          append([]byte(nil), component.Data...),
			})
		}
		objects = append(objects, state)
	}
	s.mu.RUnlock()
	return NewSnapshot(meta, objects, s.limits)
}

func (s *ShadowStore) ObjectCount() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.objects)
}

func (s *ShadowStore) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	clear(s.objects)
	s.mu.Unlock()
}
