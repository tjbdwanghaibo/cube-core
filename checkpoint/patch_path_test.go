package checkpoint

import "testing"

func TestMapPatchPathBuildsSafeNestedPath(t *testing.T) {
	path, ok := MapPatchPath("items", int64(100))
	if !ok || path != "items.100" {
		t.Fatalf("path=%q ok=%v", path, ok)
	}

	if path, ok := MapPatchPath("items", "bad.key"); ok {
		t.Fatalf("unsafe key should be rejected, got %q", path)
	}
	if path, ok := MapPatchPath("items", "$bad"); ok {
		t.Fatalf("unsafe key should be rejected, got %q", path)
	}
}

func TestPersistPatchPathHelpers(t *testing.T) {
	set := map[string]any{"items.100": int32(2)}
	unset := []string{"items.200"}
	if !PersistPatchHasPath(set, unset, "items") {
		t.Fatal("expected items path patch")
	}
	if PersistPatchHasPath(set, unset, "friends") {
		t.Fatal("friends should not have path patch")
	}
	if !PersistPatchPathCovered("items.100", map[string]bool{"items": true}) {
		t.Fatal("items full patch should cover items.100")
	}
}
