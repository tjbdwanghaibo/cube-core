package replication

import (
	"bytes"
	"errors"
	"testing"
)

func TestLODProjectorFiltersAndRateLimitsWithoutDeletingState(t *testing.T) {
	registry := testLODRegistry(t)
	sawCurrentSnapshot := false
	projector, err := NewLODProjector(LODProjectorConfig{
		Registry: registry, SnapshotRateHz: 20,
		ComponentPolicies: map[uint16]ComponentLODPolicy{
			2: {Levels: LODAtFull | LODAtReduced},
			3: {Levels: LODAtFull},
		},
		Selector: LODSelectorFunc(func(context ProjectionContext, _ ObjectState) (LODDecision, error) {
			sawCurrentSnapshot = context.Current != nil && len(context.Current.Objects) == 1
			if context.QualityTier == 1 {
				return LODDecision{Level: LODReduced, MinPriority: PriorityNormal, MaxRateHz: 10}, nil
			}
			return LODDecision{Level: LODFull}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	previous := mustSnapshot(t, 2, []ObjectState{lodObject(10, 20, 30)})
	current := mustSnapshot(t, 3, []ObjectState{lodObject(11, 21, 31)})
	projected, err := projector.ProjectWithContext(ProjectionContext{
		Session: SessionInfo{ID: 1}, QualityTier: 1, Previous: &previous,
	}, current)
	if err != nil {
		t.Fatal(err)
	}
	if !sawCurrentSnapshot {
		t.Fatal("LOD selector did not receive the immutable current projection")
	}
	if len(projected.Objects) != 1 || len(projected.Objects[0].Components) != 2 {
		t.Fatalf("unexpected LOD projection: %+v", projected.Objects)
	}
	components := projected.Objects[0].Components
	if !bytes.Equal(components[0].Data, []byte{11}) {
		t.Fatalf("critical component was not refreshed: %v", components[0].Data)
	}
	if !bytes.Equal(components[1].Data, []byte{20}) {
		t.Fatalf("rate-limited component should retain the previously sent value: %v", components[1].Data)
	}
	stats := projector.Stats()
	if stats.ComponentsHeld != 1 || stats.ComponentsOmitted != 1 || stats.ObjectsByLevel[LODReduced] != 1 {
		t.Fatalf("unexpected LOD stats: %+v", stats)
	}

	due := mustSnapshot(t, 4, []ObjectState{lodObject(12, 22, 32)})
	projected, err = projector.ProjectWithContext(ProjectionContext{
		Session: SessionInfo{ID: 1}, QualityTier: 1, Previous: &previous,
	}, due)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(projected.Objects[0].Components[1].Data, []byte{22}) {
		t.Fatalf("component did not refresh on its sampling tick: %v", projected.Objects[0].Components[1].Data)
	}
}

func TestLODProjectorFullRefreshBypassesRateLimitAndCullRemovesObject(t *testing.T) {
	registry := testLODRegistry(t)
	projector, err := NewLODProjector(LODProjectorConfig{
		Registry: registry, SnapshotRateHz: 20,
		Selector: LODSelectorFunc(func(context ProjectionContext, _ ObjectState) (LODDecision, error) {
			if context.QualityTier == 9 {
				return LODDecision{Level: LODCulled}, nil
			}
			return LODDecision{Level: LODReduced, MaxRateHz: 5}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	previous := mustSnapshot(t, 1, []ObjectState{lodObject(1, 2, 3)})
	current := mustSnapshot(t, 2, []ObjectState{lodObject(4, 5, 6)})
	full, err := projector.ProjectWithContext(ProjectionContext{
		Session: SessionInfo{ID: 1}, Previous: &previous, FullRefresh: true,
	}, current)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(full.Objects[0].Components[1].Data, []byte{5}) {
		t.Fatalf("full refresh retained stale LOD data: %v", full.Objects[0].Components[1].Data)
	}
	culled, err := projector.ProjectWithContext(ProjectionContext{
		Session: SessionInfo{ID: 1}, QualityTier: 9, Previous: &previous,
	}, current)
	if err != nil {
		t.Fatal(err)
	}
	if len(culled.Objects) != 0 {
		t.Fatalf("culled object remained in projection: %+v", culled.Objects)
	}
}

func TestReplicatorQualityTierProducesRemoveAndCreate(t *testing.T) {
	registry := testLODRegistry(t)
	projector, err := NewLODProjector(LODProjectorConfig{
		Registry: registry,
		Selector: LODSelectorFunc(func(context ProjectionContext, _ ObjectState) (LODDecision, error) {
			if context.QualityTier == 1 {
				return LODDecision{Level: LODCulled}, nil
			}
			return LODDecision{Level: LODFull}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	replicator := NewReplicator(ReplicatorConfig{Projector: projector})
	defer replicator.Close()
	if err := replicator.RegisterSession(SessionInfo{ID: 88}); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Publish(mustSnapshot(t, 1, []ObjectState{lodObject(1, 2, 3)})); err != nil {
		t.Fatal(err)
	}
	first, _, err := replicator.BuildLatest(88)
	if err != nil || first.Kind != FrameFull || len(first.Objects) != 1 || first.Objects[0].Operation != ObjectCreate {
		t.Fatalf("initial projection frame=%+v err=%v", first, err)
	}
	if err := replicator.Acknowledge(88, 1); err != nil {
		t.Fatal(err)
	}
	if err := replicator.SetQualityTier(88, 1); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Publish(mustSnapshot(t, 2, []ObjectState{lodObject(4, 5, 6)})); err != nil {
		t.Fatal(err)
	}
	removed, _, err := replicator.BuildLatest(88)
	if err != nil || len(removed.Objects) != 1 || removed.Objects[0].Operation != ObjectRemove {
		t.Fatalf("culled projection frame=%+v err=%v", removed, err)
	}
	if err := replicator.Acknowledge(88, 2); err != nil {
		t.Fatal(err)
	}
	if err := replicator.SetQualityTier(88, 0); err != nil {
		t.Fatal(err)
	}
	if err := replicator.Publish(mustSnapshot(t, 3, []ObjectState{lodObject(7, 8, 9)})); err != nil {
		t.Fatal(err)
	}
	created, _, err := replicator.BuildLatest(88)
	if err != nil || len(created.Objects) != 1 || created.Objects[0].Operation != ObjectCreate {
		t.Fatalf("restored projection frame=%+v err=%v", created, err)
	}
	session, ok := replicator.Session(88)
	if !ok || session.QualityTier != 0 || replicator.Stats().QualityTierChanges != 2 {
		t.Fatalf("quality state=%+v stats=%+v", session, replicator.Stats())
	}
}

func TestLODProjectorRejectsInvalidConfigurationAndDecision(t *testing.T) {
	registry := testLODRegistry(t)
	if _, err := NewLODProjector(LODProjectorConfig{
		Registry: registry, ComponentPolicies: map[uint16]ComponentLODPolicy{99: {}},
	}); !errors.Is(err, ErrInvalidLOD) {
		t.Fatalf("unknown component policy accepted: %v", err)
	}
	projector, err := NewLODProjector(LODProjectorConfig{
		Registry: registry,
		Selector: LODSelectorFunc(func(ProjectionContext, ObjectState) (LODDecision, error) {
			return LODDecision{Level: 8}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projector.Project(SessionInfo{ID: 1}, mustSnapshot(t, 1, []ObjectState{lodObject(1, 2, 3)})); !errors.Is(err, ErrInvalidLOD) {
		t.Fatalf("invalid selector decision accepted: %v", err)
	}
	if mask, err := NewLODMask(LODFull, 7); err != nil || !mask.Includes(LODFull) || !mask.Includes(7) {
		t.Fatalf("custom LOD mask=%08b err=%v", mask, err)
	}
	if _, err := NewLODMask(LODCulled); !errors.Is(err, ErrInvalidLOD) {
		t.Fatalf("culled level was accepted in a component mask: %v", err)
	}
}

func testLODRegistry(t *testing.T) *SchemaRegistry {
	t.Helper()
	registry := NewSchemaRegistry()
	for _, schema := range []ComponentSchema{
		{TypeID: 1, Name: "critical", Version: 1, MaxEncodedSize: 8, Policy: lodPolicy(PriorityCritical, 20)},
		{TypeID: 2, Name: "normal", Version: 1, MaxEncodedSize: 8, Policy: lodPolicy(PriorityNormal, 20)},
		{TypeID: 3, Name: "cosmetic", Version: 1, MaxEncodedSize: 8, Policy: lodPolicy(PriorityCosmetic, 20)},
	} {
		if err := registry.Register(schema); err != nil {
			t.Fatal(err)
		}
	}
	return registry
}

func lodPolicy(priority Priority, rate uint8) ReplicationPolicy {
	return ReplicationPolicy{
		Lane: LaneState, Reliability: ReliabilityUnreliableLatest, Priority: priority,
		MaxRateHz: rate, Visibility: VisibilityPublic, Codec: CodecRaw,
	}
}

func lodObject(critical, normal, cosmetic byte) ObjectState {
	return ObjectState{
		Ref: ObjectRef{ID: 1, Generation: 1}, Archetype: 1,
		Components: []ComponentState{
			{TypeID: 1, SchemaVersion: 1, Data: []byte{critical}},
			{TypeID: 2, SchemaVersion: 1, Data: []byte{normal}},
			{TypeID: 3, SchemaVersion: 1, Data: []byte{cosmetic}},
		},
	}
}
