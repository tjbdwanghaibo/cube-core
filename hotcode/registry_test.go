package hotcode

import (
	"errors"
	"testing"
)

func TestRegistryReplaceResolveAndRevert(t *testing.T) {
	reg := NewRegistry()
	name := "test.fn"
	orig := func(v int) int { return v + 1 }
	patch := func(v int) int { return v + 10 }

	if err := reg.Register(name, orig); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := reg.Resolve(name, orig).(func(int) int)(1); got != 2 {
		t.Fatalf("orig = %d", got)
	}
	if err := reg.Replace(name, patch, Meta{Version: "v1"}); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if got := reg.Resolve(name, orig).(func(int) int)(1); got != 11 {
		t.Fatalf("patched = %d", got)
	}
	if err := reg.Revert(name); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if got := reg.Resolve(name, orig).(func(int) int)(1); got != 2 {
		t.Fatalf("reverted = %d", got)
	}
}

func TestRegistryRejectsSignatureMismatch(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("test.fn", func(int) int { return 1 }); err != nil {
		t.Fatalf("Register: %v", err)
	}
	err := reg.Replace("test.fn", func(string) int { return 1 }, Meta{})
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
}
