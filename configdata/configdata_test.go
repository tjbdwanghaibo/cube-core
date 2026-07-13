package configdata

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
)

type testMonsterCfg struct {
	ID      int32  `json:"id"`
	Name    string `json:"name"`
	SceneID int32  `json:"scene_id"`
}

type testWorldCfg struct {
	Width int32 `json:"width"`
}

func TestStoreLoadReloadAndActiveSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":1,"name":"wolf","scene_id":7}]`)
	writeFile(t, filepath.Join(dir, "world.json"), `{"width":100}`)

	reg := NewRegistry()
	MustRegisterTable(reg, TableDef[int32, testMonsterCfg]{
		Name: "monster",
		File: "monster.json",
		Key:  func(v testMonsterCfg) int32 { return v.ID },
		Indexes: []IndexDef[testMonsterCfg]{
			{Name: "scene_id", Key: func(v testMonsterCfg) string { return "scene:" + string(rune(v.SceneID)) }},
		},
		Validate: func(ctx *BuildContext, v testMonsterCfg) error {
			if _, ok := ObjectFrom[testWorldCfg](ctx.Snapshot, "world"); !ok {
				t.Fatalf("object should be visible during table validation")
			}
			return nil
		},
	})
	MustRegisterObject(reg, ObjectDef[testWorldCfg]{
		Name: "world",
		File: "world.json",
	})
	MustRegisterCustom(reg, CustomDef[int]{
		Name: "monster_count",
		Build: func(ctx *BuildContext) (int, error) {
			table := MustTableFrom[int32, testMonsterCfg](ctx.Snapshot, "monster")
			return table.Len(), nil
		},
	})

	store := NewStore(reg, dir)
	snap1, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if snap1.Version != 1 {
		t.Fatalf("version = %d, want 1", snap1.Version)
	}
	table := MustTableFrom[int32, testMonsterCfg](snap1, "monster")
	row, ok := table.Get(1)
	if !ok || row.Name != "wolf" {
		t.Fatalf("monster row = %+v ok=%v", row, ok)
	}
	if count := MustCustomFrom[int](snap1, "monster_count"); count != 1 {
		t.Fatalf("custom count = %d", count)
	}

	_, release := fctx.NewContext()
	defer release()
	if got := ActiveSnapshot(); got != snap1 {
		t.Fatalf("request snapshot should be snap1")
	}

	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":1,"name":"bear","scene_id":7}]`)
	snap2, err := store.Reload(context.Background())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if snap2 == snap1 || snap2.Version != 2 {
		t.Fatalf("unexpected snap2 version")
	}
	if got := ActiveSnapshot(); got != snap1 {
		t.Fatalf("active request should keep request snapshot")
	}

	release()
	if got := ActiveSnapshot(); got != snap2 {
		t.Fatalf("without request context should read latest snapshot")
	}
}

func TestReloadFailureKeepsOldSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":1,"name":"wolf"}]`)
	reg := NewRegistry()
	MustRegisterTable(reg, TableDef[int32, testMonsterCfg]{
		Name: "monster",
		File: "monster.json",
		Key:  func(v testMonsterCfg) int32 { return v.ID },
	})
	store := NewStore(reg, dir)
	snap1, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	writeFile(t, filepath.Join(dir, "monster.json"), `{broken`)
	if _, err := store.Reload(context.Background()); err == nil {
		t.Fatal("reload should fail")
	}
	if got := store.Current(); got != snap1 {
		t.Fatal("failed reload should keep current snapshot")
	}
}

func TestDryRunBuildsSnapshotWithoutPublishing(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":1,"name":"wolf"}]`)
	reg := NewRegistry()
	MustRegisterTable(reg, TableDef[int32, testMonsterCfg]{
		Name: "monster",
		File: "monster.json",
		Key:  func(v testMonsterCfg) int32 { return v.ID },
	})
	store := NewStore(reg, dir)
	snap1, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":2,"name":"bear"}]`)
	candidate, err := store.DryRun(context.Background(), "gray")
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if candidate.Version != 2 {
		t.Fatalf("candidate version = %d, want 2", candidate.Version)
	}
	if got := store.Current(); got != snap1 {
		t.Fatal("dry-run should not publish candidate snapshot")
	}
}

func TestRollbackRestoresPreviousPublishedSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":1,"name":"wolf"}]`)
	reg := NewRegistry()
	MustRegisterTable(reg, TableDef[int32, testMonsterCfg]{
		Name: "monster",
		File: "monster.json",
		Key:  func(v testMonsterCfg) int32 { return v.ID },
	})
	store := NewStore(reg, dir)
	snap1, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":2,"name":"bear"}]`)
	if _, err := store.ReloadWithReason(context.Background(), "publish"); err != nil {
		t.Fatalf("reload: %v", err)
	}

	rolled, err := store.Rollback(context.Background(), "manual rollback")
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if rolled != snap1 || store.Current() != snap1 {
		t.Fatal("rollback should restore previous snapshot")
	}
}

func TestReloadListenerRollbackKeepsOldSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":1,"name":"wolf"}]`)
	reg := NewRegistry()
	MustRegisterTable(reg, TableDef[int32, testMonsterCfg]{
		Name: "monster",
		File: "monster.json",
		Key:  func(v testMonsterCfg) int32 { return v.ID },
	})
	store := NewStore(reg, dir)
	snap1, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	rollbackCalled := false
	wantErr := errors.New("after failed")
	store.AddReloadListener(ReloadHook{
		HookName: "fail_after",
		AfterApply: func(context.Context, ReloadEvent) error {
			return wantErr
		},
		Rollback: func(_ context.Context, _ ReloadEvent, err error) {
			rollbackCalled = errors.Is(err, wantErr)
		},
	})
	writeFile(t, filepath.Join(dir, "monster.json"), `[{"id":2,"name":"bear"}]`)
	if _, err := store.ReloadWithReason(context.Background(), "test"); !errors.Is(err, wantErr) {
		t.Fatalf("reload err = %v, want %v", err, wantErr)
	}
	if !rollbackCalled {
		t.Fatal("rollback hook not called")
	}
	if got := store.Current(); got != snap1 {
		t.Fatal("after-apply failed reload should restore previous snapshot")
	}
}

func TestAddReloadListenerUnregistersUncomparableHook(t *testing.T) {
	store := NewStore(NewRegistry(), t.TempDir())
	unregister := store.AddReloadListener(ReloadHook{
		HookName: "uncomparable",
		AfterApply: func(context.Context, ReloadEvent) error {
			return nil
		},
	})

	unregister()
	unregister()

	if got := len(store.reloadListeners()); got != 0 {
		t.Fatalf("listeners after unregister = %d, want 0", got)
	}
}

func writeFile(t *testing.T, path string, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
