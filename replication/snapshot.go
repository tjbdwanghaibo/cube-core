package replication

import (
	"fmt"
	"sort"
)

func NewSnapshot(meta SnapshotMeta, objects []ObjectState, limits Limits) (Snapshot, error) {
	if err := meta.validate(); err != nil {
		return Snapshot{}, err
	}
	limits = normalizeLimits(limits)
	if len(objects) > limits.MaxObjects {
		return Snapshot{}, ErrObjectLimit
	}
	out := Snapshot{SnapshotMeta: meta, Objects: cloneObjects(objects)}
	sort.Slice(out.Objects, func(i, j int) bool { return out.Objects[i].Ref.Less(out.Objects[j].Ref) })
	last := ObjectRef{}
	estimated := 32
	for i := range out.Objects {
		obj := &out.Objects[i]
		estimated += 9
		if !obj.Ref.Valid() || obj.Archetype == 0 {
			return Snapshot{}, ErrInvalidObjectRef
		}
		if i > 0 && obj.Ref == last {
			return Snapshot{}, fmt.Errorf("%w: duplicate object %+v", ErrInvalidFrame, obj.Ref)
		}
		last = obj.Ref
		if len(obj.Components) > limits.MaxComponentsPerObject {
			return Snapshot{}, ErrComponentLimit
		}
		sort.Slice(obj.Components, func(i, j int) bool { return obj.Components[i].TypeID < obj.Components[j].TypeID })
		var lastType uint16
		for j := range obj.Components {
			component := &obj.Components[j]
			if component.TypeID == 0 || component.SchemaVersion == 0 {
				return Snapshot{}, fmt.Errorf("%w: invalid component", ErrInvalidFrame)
			}
			if j > 0 && component.TypeID == lastType {
				return Snapshot{}, fmt.Errorf("%w: duplicate component %d", ErrInvalidFrame, component.TypeID)
			}
			lastType = component.TypeID
			if len(component.Data) > limits.MaxComponentBytes {
				return Snapshot{}, ErrComponentTooLarge
			}
			estimated += 12 + len(component.Data)
			if estimated > limits.MaxFrameBytes {
				return Snapshot{}, ErrFrameTooLarge
			}
		}
	}
	return out, nil
}

func (s Snapshot) Clone() Snapshot {
	return Snapshot{SnapshotMeta: s.SnapshotMeta, Objects: cloneObjects(s.Objects)}
}

func cloneObjects(objects []ObjectState) []ObjectState {
	if len(objects) == 0 {
		return nil
	}
	out := make([]ObjectState, len(objects))
	for i, obj := range objects {
		out[i] = ObjectState{Ref: obj.Ref, Archetype: obj.Archetype}
		if len(obj.Components) == 0 {
			continue
		}
		out[i].Components = make([]ComponentState, len(obj.Components))
		for j, component := range obj.Components {
			out[i].Components[j] = ComponentState{
				TypeID:        component.TypeID,
				SchemaVersion: component.SchemaVersion,
				Data:          append([]byte(nil), component.Data...),
			}
		}
	}
	return out
}
