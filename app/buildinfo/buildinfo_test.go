package buildinfo

import (
	"strings"
	"testing"
)

func TestInfoDefaultsToDevVersion(t *testing.T) {
	resetBuildVars(t)

	info := Info()

	if info.Version != "dev" {
		t.Fatalf("version = %q, want dev", info.Version)
	}
	if info.Commit != "unknown" {
		t.Fatalf("commit = %q, want unknown", info.Commit)
	}
	if info.BuildTime != "unknown" {
		t.Fatalf("build time = %q, want unknown", info.BuildTime)
	}
	if info.Dirty != "unknown" {
		t.Fatalf("dirty = %q, want unknown", info.Dirty)
	}
}

func TestInfoStringIncludesLinkedMetadata(t *testing.T) {
	resetBuildVars(t)
	Version = "1.2.3"
	Commit = "abc1234"
	BuildTime = "2026-05-28T10:00:00Z"
	Dirty = "false"

	got := Info().String()

	for _, want := range []string{"1.2.3", "commit=abc1234", "built=2026-05-28T10:00:00Z", "dirty=false"} {
		if !strings.Contains(got, want) {
			t.Fatalf("Info().String() = %q, missing %q", got, want)
		}
	}
}

func resetBuildVars(t *testing.T) {
	t.Helper()
	oldVersion := Version
	oldCommit := Commit
	oldBuildTime := BuildTime
	oldDirty := Dirty
	Version = ""
	Commit = ""
	BuildTime = ""
	Dirty = ""
	t.Cleanup(func() {
		Version = oldVersion
		Commit = oldCommit
		BuildTime = oldBuildTime
		Dirty = oldDirty
	})
}
