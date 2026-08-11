package replication

import (
	"bytes"
	"fmt"
)

func BuildDelta(base *Snapshot, current Snapshot) (DeltaFrame, error) {
	if err := current.SnapshotMeta.validate(); err != nil {
		return DeltaFrame{}, err
	}
	frame := DeltaFrame{SnapshotMeta: current.SnapshotMeta, Kind: FrameFull}
	if base == nil {
		frame.Objects = make([]ObjectDelta, 0, len(current.Objects))
		for _, obj := range current.Objects {
			frame.Objects = append(frame.Objects, createObjectDelta(obj))
		}
		return frame, nil
	}
	if base.RoomID != current.RoomID || base.Epoch != current.Epoch || base.SchemaVersion != current.SchemaVersion || base.Tick >= current.Tick {
		return DeltaFrame{}, ErrBaselineMismatch
	}
	frame.Kind = FrameDelta
	frame.BaseTick = base.Tick
	frame.Objects = diffObjects(base.Objects, current.Objects)
	return frame, nil
}

func diffObjects(base, current []ObjectState) []ObjectDelta {
	out := make([]ObjectDelta, 0)
	for i, j := 0, 0; i < len(base) || j < len(current); {
		switch {
		case i >= len(base):
			out = append(out, createObjectDelta(current[j]))
			j++
		case j >= len(current):
			out = append(out, ObjectDelta{Operation: ObjectRemove, Ref: base[i].Ref})
			i++
		case base[i].Ref.Less(current[j].Ref):
			out = append(out, ObjectDelta{Operation: ObjectRemove, Ref: base[i].Ref})
			i++
		case current[j].Ref.Less(base[i].Ref):
			out = append(out, createObjectDelta(current[j]))
			j++
		default:
			if base[i].Archetype != current[j].Archetype {
				out = append(out, createObjectDelta(current[j]))
			} else if components := diffComponents(base[i].Components, current[j].Components); len(components) > 0 {
				out = append(out, ObjectDelta{
					Operation:  ObjectUpdate,
					Ref:        current[j].Ref,
					Archetype:  current[j].Archetype,
					Components: components,
				})
			}
			i++
			j++
		}
	}
	return out
}

func diffComponents(base, current []ComponentState) []ComponentDelta {
	out := make([]ComponentDelta, 0)
	for i, j := 0, 0; i < len(base) || j < len(current); {
		switch {
		case i >= len(base):
			out = append(out, setComponentDelta(current[j]))
			j++
		case j >= len(current):
			out = append(out, ComponentDelta{Operation: ComponentRemove, TypeID: base[i].TypeID})
			i++
		case base[i].TypeID < current[j].TypeID:
			out = append(out, ComponentDelta{Operation: ComponentRemove, TypeID: base[i].TypeID})
			i++
		case current[j].TypeID < base[i].TypeID:
			out = append(out, setComponentDelta(current[j]))
			j++
		default:
			if base[i].SchemaVersion != current[j].SchemaVersion || !bytes.Equal(base[i].Data, current[j].Data) {
				out = append(out, setComponentDelta(current[j]))
			}
			i++
			j++
		}
	}
	return out
}

func createObjectDelta(obj ObjectState) ObjectDelta {
	delta := ObjectDelta{Operation: ObjectCreate, Ref: obj.Ref, Archetype: obj.Archetype}
	if len(obj.Components) > 0 {
		delta.Components = make([]ComponentDelta, 0, len(obj.Components))
		for _, component := range obj.Components {
			delta.Components = append(delta.Components, setComponentDelta(component))
		}
	}
	return delta
}

func setComponentDelta(component ComponentState) ComponentDelta {
	return ComponentDelta{
		Operation:     ComponentSet,
		TypeID:        component.TypeID,
		SchemaVersion: component.SchemaVersion,
		Data:          append([]byte(nil), component.Data...),
	}
}

func ApplyDelta(base *Snapshot, frame DeltaFrame, limits Limits) (Snapshot, error) {
	limits = normalizeLimits(limits)
	if err := frame.SnapshotMeta.validate(); err != nil {
		return Snapshot{}, err
	}
	objects := make(map[ObjectRef]ObjectState)
	switch frame.Kind {
	case FrameFull:
		if frame.BaseTick != 0 {
			return Snapshot{}, fmt.Errorf("%w: full frame has baseline", ErrInvalidFrame)
		}
	case FrameDelta:
		if base == nil || frame.BaseTick == 0 || base.Tick != frame.BaseTick || base.RoomID != frame.RoomID || base.Epoch != frame.Epoch || base.SchemaVersion != frame.SchemaVersion {
			return Snapshot{}, ErrBaselineMismatch
		}
		for _, obj := range base.Objects {
			objects[obj.Ref] = cloneObject(obj)
		}
	default:
		return Snapshot{}, fmt.Errorf("%w: unknown frame kind %d", ErrInvalidFrame, frame.Kind)
	}

	for _, delta := range frame.Objects {
		if !delta.Ref.Valid() {
			return Snapshot{}, ErrInvalidObjectRef
		}
		switch delta.Operation {
		case ObjectCreate:
			if delta.Archetype == 0 {
				return Snapshot{}, ErrInvalidObjectRef
			}
			obj := ObjectState{Ref: delta.Ref, Archetype: delta.Archetype}
			components, err := applyComponentDeltas(nil, delta.Components, limits)
			if err != nil {
				return Snapshot{}, err
			}
			obj.Components = components
			objects[delta.Ref] = obj
		case ObjectUpdate:
			obj, ok := objects[delta.Ref]
			if !ok {
				return Snapshot{}, ErrObjectNotFound
			}
			if delta.Archetype != 0 && delta.Archetype != obj.Archetype {
				return Snapshot{}, fmt.Errorf("%w: archetype changed without create", ErrInvalidFrame)
			}
			components, err := applyComponentDeltas(obj.Components, delta.Components, limits)
			if err != nil {
				return Snapshot{}, err
			}
			obj.Components = components
			objects[delta.Ref] = obj
		case ObjectRemove:
			delete(objects, delta.Ref)
		default:
			return Snapshot{}, fmt.Errorf("%w: unknown object operation %d", ErrInvalidFrame, delta.Operation)
		}
		if len(objects) > limits.MaxObjects {
			return Snapshot{}, ErrObjectLimit
		}
	}

	result := make([]ObjectState, 0, len(objects))
	for _, obj := range objects {
		result = append(result, obj)
	}
	return NewSnapshot(frame.SnapshotMeta, result, limits)
}

func applyComponentDeltas(base []ComponentState, deltas []ComponentDelta, limits Limits) ([]ComponentState, error) {
	components := make(map[uint16]ComponentState, len(base)+len(deltas))
	for _, component := range base {
		components[component.TypeID] = ComponentState{
			TypeID: component.TypeID, SchemaVersion: component.SchemaVersion, Data: append([]byte(nil), component.Data...),
		}
	}
	for _, delta := range deltas {
		if delta.TypeID == 0 {
			return nil, fmt.Errorf("%w: zero component type", ErrInvalidFrame)
		}
		switch delta.Operation {
		case ComponentSet:
			if delta.SchemaVersion == 0 || len(delta.Data) > limits.MaxComponentBytes {
				return nil, ErrComponentTooLarge
			}
			components[delta.TypeID] = ComponentState{
				TypeID: delta.TypeID, SchemaVersion: delta.SchemaVersion, Data: append([]byte(nil), delta.Data...),
			}
		case ComponentRemove:
			delete(components, delta.TypeID)
		default:
			return nil, fmt.Errorf("%w: unknown component operation %d", ErrInvalidFrame, delta.Operation)
		}
		if len(components) > limits.MaxComponentsPerObject {
			return nil, ErrComponentLimit
		}
	}
	out := make([]ComponentState, 0, len(components))
	for _, component := range components {
		out = append(out, component)
	}
	return out, nil
}

func cloneObject(obj ObjectState) ObjectState {
	return cloneObjects([]ObjectState{obj})[0]
}
