package replication

import (
	"fmt"
	"sync/atomic"
)

// LODLevel is an application-selected object detail level. Lower values carry
// more detail. Levels 0-7 are available; LODCulled removes the object from the
// session projection entirely.
type LODLevel uint8

const (
	LODFull     LODLevel = 0
	LODReduced  LODLevel = 1
	LODLow      LODLevel = 2
	LODMinimal  LODLevel = 3
	LODCulled   LODLevel = 255
	maxLODLevel          = 7
)

// LODMask selects detail levels at which a component is present. Zero is the
// safe default and means all non-culled levels.
type LODMask uint8

const (
	LODAtFull    LODMask = 1 << LODFull
	LODAtReduced LODMask = 1 << LODReduced
	LODAtLow     LODMask = 1 << LODLow
	LODAtMinimal LODMask = 1 << LODMinimal
	LODAtAll     LODMask = 0
)

func (mask LODMask) Includes(level LODLevel) bool {
	return level <= maxLODLevel && (mask == LODAtAll || mask&(LODMask(1)<<level) != 0)
}

// NewLODMask constructs a component mask for any combination of levels 0-7.
func NewLODMask(levels ...LODLevel) (LODMask, error) {
	var mask LODMask
	for _, level := range levels {
		if level > maxLODLevel {
			return 0, fmt.Errorf("%w: component mask level %d", ErrInvalidLOD, level)
		}
		mask |= LODMask(1) << level
	}
	return mask, nil
}

// ComponentLODPolicy defines the levels at which one registered component is
// present. The zero policy includes it at every non-culled level.
type ComponentLODPolicy struct {
	Levels LODMask
}

// LODDecision controls one object's projection for one client. MinPriority
// removes less important components. MaxRateHz caps non-critical component
// refresh frequency; zero leaves each component's schema MaxRateHz unchanged.
type LODDecision struct {
	Level       LODLevel
	MinPriority Priority
	MaxRateHz   uint8
}

func (decision LODDecision) validate() error {
	if decision.Level != LODCulled && decision.Level > maxLODLevel {
		return fmt.Errorf("%w: level %d", ErrInvalidLOD, decision.Level)
	}
	if decision.MinPriority != 0 && (decision.MinPriority < PriorityCosmetic || decision.MinPriority > PriorityCritical) {
		return fmt.Errorf("%w: minimum priority %d", ErrInvalidLOD, decision.MinPriority)
	}
	return nil
}

type LODSelector interface {
	SelectLOD(ProjectionContext, ObjectState) (LODDecision, error)
}

// LODSelectorFunc adapts a function to LODSelector. A nil function selects
// LODFull for every object.
type LODSelectorFunc func(ProjectionContext, ObjectState) (LODDecision, error)

func (selector LODSelectorFunc) SelectLOD(context ProjectionContext, object ObjectState) (LODDecision, error) {
	if selector == nil {
		return LODDecision{Level: LODFull}, nil
	}
	return selector(context, object)
}

// LODProjectorConfig composes visibility projection, object selection and
// component-level policies. Registry must contain every configured component.
type LODProjectorConfig struct {
	Registry             *SchemaRegistry
	Selector             LODSelector
	Upstream             Projector
	ComponentPolicies    map[uint16]ComponentLODPolicy
	SnapshotRateHz       uint8
	AlwaysFreshAtOrAbove Priority
}

// LODProjector builds a per-session snapshot projection while retaining
// previous component values on ticks suppressed by rate limits.
type LODProjector struct {
	registry             *SchemaRegistry
	selector             LODSelector
	upstream             Projector
	policies             map[uint16]ComponentLODPolicy
	snapshotRateHz       uint8
	alwaysFreshAtOrAbove Priority
	stats                lodCounters
}

type lodCounters struct {
	decisions         atomic.Uint64
	objectsCulled     atomic.Uint64
	componentsOmitted atomic.Uint64
	componentsHeld    atomic.Uint64
	levels            [8]atomic.Uint64
}

// LODProjectorStats contains cumulative projection decisions. ObjectsByLevel
// indexes levels 0-7; culled decisions are reported separately.
type LODProjectorStats struct {
	Decisions         uint64
	ObjectsCulled     uint64
	ComponentsOmitted uint64
	ComponentsHeld    uint64
	ObjectsByLevel    [8]uint64
}

func NewLODProjector(config LODProjectorConfig) (*LODProjector, error) {
	if config.Registry == nil {
		return nil, fmt.Errorf("%w: schema registry is required", ErrInvalidLOD)
	}
	if config.Selector == nil {
		config.Selector = LODSelectorFunc(nil)
	}
	if config.Upstream == nil {
		config.Upstream = ProjectorFunc(nil)
	}
	if config.SnapshotRateHz == 0 {
		config.SnapshotRateHz = DefaultSnapshotRateHz
	}
	if config.AlwaysFreshAtOrAbove == 0 {
		config.AlwaysFreshAtOrAbove = PriorityCritical
	}
	if config.AlwaysFreshAtOrAbove < PriorityCosmetic || config.AlwaysFreshAtOrAbove > PriorityCritical {
		return nil, fmt.Errorf("%w: always-fresh priority %d", ErrInvalidLOD, config.AlwaysFreshAtOrAbove)
	}
	policies := make(map[uint16]ComponentLODPolicy, len(config.ComponentPolicies))
	for typeID, policy := range config.ComponentPolicies {
		if typeID == 0 {
			return nil, fmt.Errorf("%w: zero component type", ErrInvalidLOD)
		}
		if _, ok := config.Registry.Lookup(typeID); !ok {
			return nil, fmt.Errorf("%w: component type %d is not registered", ErrInvalidLOD, typeID)
		}
		policies[typeID] = policy
	}
	return &LODProjector{
		registry: config.Registry, selector: config.Selector, upstream: config.Upstream,
		policies: policies, snapshotRateHz: config.SnapshotRateHz,
		alwaysFreshAtOrAbove: config.AlwaysFreshAtOrAbove,
	}, nil
}

func (projector *LODProjector) Project(session SessionInfo, snapshot Snapshot) (Snapshot, error) {
	return projector.ProjectWithContext(ProjectionContext{Session: session}, snapshot)
}

func (projector *LODProjector) ProjectWithContext(context ProjectionContext, snapshot Snapshot) (Snapshot, error) {
	if projector == nil {
		return Snapshot{}, fmt.Errorf("%w: nil projector", ErrInvalidLOD)
	}
	var projected Snapshot
	var err error
	if contextual, ok := projector.upstream.(ContextProjector); ok {
		projected, err = contextual.ProjectWithContext(context, snapshot)
	} else {
		projected, err = projector.upstream.Project(context.Session, snapshot)
	}
	if err != nil {
		return Snapshot{}, err
	}
	selectionContext := context
	selectionContext.Current = &projected
	previous := indexLODObjects(context.Previous, projected)
	objects := make([]ObjectState, 0, len(projected.Objects))
	for _, object := range projected.Objects {
		decision, err := projector.selector.SelectLOD(selectionContext, object)
		if err != nil {
			return Snapshot{}, err
		}
		if err := decision.validate(); err != nil {
			return Snapshot{}, err
		}
		projector.stats.decisions.Add(1)
		if decision.Level == LODCulled {
			projector.stats.objectsCulled.Add(1)
			continue
		}
		projector.stats.levels[decision.Level].Add(1)
		previousObject, existed := previous[object.Ref]
		filtered := ObjectState{Ref: object.Ref, Archetype: object.Archetype}
		for _, component := range object.Components {
			schema, registered := projector.registry.Lookup(component.TypeID)
			policy := projector.policies[component.TypeID]
			if registered && (!policy.Levels.Includes(decision.Level) || decision.MinPriority != 0 && schema.Policy.Priority < decision.MinPriority) {
				projector.stats.componentsOmitted.Add(1)
				continue
			}
			selected := component
			if registered && !context.FullRefresh && existed && previousObject.Archetype == object.Archetype && !projector.refreshDue(projected.Tick, schema.Policy, decision) {
				if old, ok := findLODComponent(previousObject.Components, component.TypeID); ok && old.SchemaVersion == component.SchemaVersion {
					selected = old
					projector.stats.componentsHeld.Add(1)
				}
			}
			selected.Data = append([]byte(nil), selected.Data...)
			filtered.Components = append(filtered.Components, selected)
		}
		objects = append(objects, filtered)
	}
	return Snapshot{SnapshotMeta: projected.SnapshotMeta, Objects: objects}, nil
}

func (projector *LODProjector) refreshDue(tick uint32, policy ReplicationPolicy, decision LODDecision) bool {
	rate := policy.MaxRateHz
	if policy.Priority < projector.alwaysFreshAtOrAbove && decision.MaxRateHz != 0 && (rate == 0 || decision.MaxRateHz < rate) {
		rate = decision.MaxRateHz
	}
	if rate == 0 || rate >= projector.snapshotRateHz {
		return true
	}
	interval := (uint32(projector.snapshotRateHz) + uint32(rate) - 1) / uint32(rate)
	return tick%interval == 0
}

func (projector *LODProjector) Stats() LODProjectorStats {
	if projector == nil {
		return LODProjectorStats{}
	}
	stats := LODProjectorStats{
		Decisions: projector.stats.decisions.Load(), ObjectsCulled: projector.stats.objectsCulled.Load(),
		ComponentsOmitted: projector.stats.componentsOmitted.Load(), ComponentsHeld: projector.stats.componentsHeld.Load(),
	}
	for level := range stats.ObjectsByLevel {
		stats.ObjectsByLevel[level] = projector.stats.levels[level].Load()
	}
	return stats
}

func indexLODObjects(previous *Snapshot, current Snapshot) map[ObjectRef]ObjectState {
	if previous == nil || previous.RoomID != current.RoomID || previous.Epoch != current.Epoch || previous.SchemaVersion != current.SchemaVersion {
		return nil
	}
	objects := make(map[ObjectRef]ObjectState, len(previous.Objects))
	for _, object := range previous.Objects {
		objects[object.Ref] = object
	}
	return objects
}

func findLODComponent(components []ComponentState, typeID uint16) (ComponentState, bool) {
	for _, component := range components {
		if component.TypeID == typeID {
			return component, true
		}
	}
	return ComponentState{}, false
}

var _ Projector = (*LODProjector)(nil)
var _ ContextProjector = (*LODProjector)(nil)
