package entity

import "testing"

func TestRemoteViewRefValidatesEntityIDAndKind(t *testing.T) {
	rawID := int64(1001)
	kind := EntityKind(7)
	MustRegisterEntityKindCategory(kind, EntityCategory(1))
	t.Cleanup(ResetEntityRegistryForTest)

	id, err := BuildEntityID(rawID, kind)
	if err != nil {
		t.Fatalf("build id: %v", err)
	}
	ref := RemoteViewRef{EntityID: id, Kind: kind, Version: 12}
	if !ref.Valid() {
		t.Fatalf("expected ref to be valid: %+v", ref)
	}
	if ref.UniqueID() != rawID {
		t.Fatalf("unique id = %d, want %d", ref.UniqueID(), rawID)
	}

	bad := RemoteViewRef{EntityID: id, Kind: EntityKind(8)}
	if bad.Valid() {
		t.Fatalf("kind-mismatched ref should be invalid")
	}
}

func TestRemoteReadOptionDefaultsRequireFreshData(t *testing.T) {
	option := NormalizeRemoteReadOption(RemoteReadOption{})
	if option.AllowStale {
		t.Fatalf("default read option should not allow stale data")
	}
	if option.MinVersion != 0 {
		t.Fatalf("default min version = %d, want 0", option.MinVersion)
	}
}

func TestRemoteSnapshotAcceptsVersionAndPreservesImmutableData(t *testing.T) {
	snapshot := RemoteSnapshot{
		EntityID:   1001,
		Kind:       EntityKind(7),
		Scope:      3,
		Version:    12,
		RouteEpoch: 5,
		Source:     RemoteSnapshotSourceCache,
		ReadAt:     100,
		ExpiresAt:  200,
		Data:       "view-data",
	}
	if !snapshot.Accepts(RemoteReadOption{MinVersion: 12}) {
		t.Fatalf("snapshot version should satisfy min version")
	}
	if snapshot.Accepts(RemoteReadOption{MinVersion: 13}) {
		t.Fatalf("snapshot version should reject newer min version")
	}
	if got := snapshot.AsString(); got != "view-data" {
		t.Fatalf("snapshot string = %q, want view-data", got)
	}
	if snapshot.Expired(199) {
		t.Fatalf("snapshot should not expire before ExpiresAt")
	}
	if !snapshot.Expired(201) {
		t.Fatalf("snapshot should expire after ExpiresAt")
	}
	if snapshot.Source != RemoteSnapshotSourceCache {
		t.Fatalf("snapshot source = %v, want cache", snapshot.Source)
	}
}
