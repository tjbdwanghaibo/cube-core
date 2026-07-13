package log

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
)

func TestContextAttrsArePrepended(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:       slog.LevelInfo,
		Output:      &buf,
		DisableGoID: true,
		FrameFunc:   func() uint64 { return 42 },
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	Info("hello", "entity", 1001)

	line := buf.String()
	if strings.Contains(line, "goid=") {
		t.Fatalf("log should not contain lowercase goid key: %s", line)
	}
	framePos := strings.Index(line, "frame=42")
	timePos := strings.Index(line, "server_time_ms=")
	entityPos := strings.Index(line, "entity=1001")
	if framePos < 0 || timePos < 0 || entityPos < 0 {
		t.Fatalf("missing expected attrs: %s", line)
	}
	if framePos > entityPos || timePos > entityPos {
		t.Fatalf("context attrs should be before business attrs: %s", line)
	}
}

func TestRuntimeAttrsAreWrittenBetweenLevelAndMessage(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:     slog.LevelInfo,
		Output:    &buf,
		FrameFunc: func() uint64 { return 42 },
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	_, release := fctx.NewContext(
		fctx.WithFrame(7),
		fctx.WithMeta(fctx.RequestMeta{Source: "nest", Handler: "handlerA"}),
	)
	defer release()

	Info("hello world", "entity", 1001)

	line := buf.String()
	levelPos := strings.Index(line, "level=INFO")
	goIDPos := strings.Index(line, "goId=")
	framePos := strings.Index(line, "frame=7")
	timePos := strings.Index(line, "server_time_ms=")
	sourcePos := strings.Index(line, "source=nest")
	handlerPos := strings.Index(line, "handler=handlerA")
	msgPos := strings.Index(line, `msg="hello world"`)
	entityPos := strings.Index(line, "entity=1001")
	if levelPos < 0 || goIDPos < 0 || framePos < 0 || timePos < 0 || sourcePos < 0 ||
		handlerPos < 0 || msgPos < 0 || entityPos < 0 {
		t.Fatalf("missing expected attrs: %s", line)
	}
	if !(levelPos < goIDPos && goIDPos < framePos && framePos < timePos &&
		timePos < sourcePos && sourcePos < handlerPos && handlerPos < msgPos && msgPos < entityPos) {
		t.Fatalf("unexpected log order: %s", line)
	}
}

func TestCallerAttrsIncludeFileLineAndFunction(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:        slog.LevelInfo,
		Output:       &buf,
		Caller:       true,
		DisableGoID:  true,
		DisableFrame: true,
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	logCallerAttrsTestHelper()

	line := buf.String()
	if !strings.Contains(line, "caller=") {
		t.Fatalf("log should contain caller file and line: %s", line)
	}
	if !strings.Contains(line, "caller_func=github.com/tjbdwanghaibo/cube-core/log.logCallerAttrsTestHelper") {
		t.Fatalf("log should contain caller function: %s", line)
	}
	if strings.Contains(line, " source=") {
		t.Fatalf("caller output should not use source key: %s", line)
	}
}

func logCallerAttrsTestHelper() {
	Info("caller attrs")
}

func TestELogCallerAttrsIncludeUserFunction(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:        slog.LevelInfo,
		Output:       &buf,
		Caller:       true,
		DisableGoID:  true,
		DisableFrame: true,
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	elogCallerAttrsTestHelper()

	line := buf.String()
	if !strings.Contains(line, "caller_func=github.com/tjbdwanghaibo/cube-core/log.elogCallerAttrsTestHelper") {
		t.Fatalf("elog should contain user caller function: %s", line)
	}
	if strings.Contains(line, "elog.go") {
		t.Fatalf("elog caller should not point to wrapper file: %s", line)
	}
}

func elogCallerAttrsTestHelper() {
	NewELog().Title("caller").Info("elog caller attrs")
}

func TestGoIDKey(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:        slog.LevelInfo,
		Output:       &buf,
		DisableFrame: true,
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	Info("hello")

	line := buf.String()
	if !strings.Contains(line, "goId=") {
		t.Fatalf("log should contain goId key: %s", line)
	}
	if strings.Contains(line, "goid=") {
		t.Fatalf("log should not contain lowercase goid key: %s", line)
	}
}

func TestELogKeepsFixedContextBeforeCallAttrs(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:       slog.LevelDebug,
		Output:      &buf,
		DisableGoID: true,
		FrameFunc:   func() uint64 { return 7 },
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	NewELog().
		Title("battle").
		EntityID(1001).
		OwnerID(2002).
		Debug("started", "target", 3003)

	line := buf.String()
	for _, want := range []string{"frame=7", "title=battle", "entityId=1001", "ownerId=2002", "target=3003"} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %s in %s", want, line)
		}
	}
	titlePos := strings.Index(line, "title=battle")
	entityPos := strings.Index(line, "entityId=1001")
	targetPos := strings.Index(line, "target=3003")
	if titlePos > targetPos || entityPos > targetPos {
		t.Fatalf("elog fixed attrs should be before call attrs: %s", line)
	}
}

func TestELogWithDoesNotMutateBase(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:       slog.LevelDebug,
		Output:      &buf,
		DisableGoID: true,
		FrameFunc:   func() uint64 { return 9 },
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	base := NewELog().Title("mission").EntityID(1001)
	base.With("step", 2).Info("derived")
	base.Info("base")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two log lines, got %d: %q", len(lines), buf.String())
	}
	if !strings.Contains(lines[0], "step=2") {
		t.Fatalf("derived log should contain scoped attr: %s", lines[0])
	}
	if strings.Contains(lines[1], "step=2") {
		t.Fatalf("base log should not inherit derived attr: %s", lines[1])
	}
}

func TestNilELogIsSafe(t *testing.T) {
	var buf bytes.Buffer
	if err := Init(Options{
		Level:       slog.LevelDebug,
		Output:      &buf,
		DisableGoID: true,
		FrameFunc:   func() uint64 { return 11 },
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	defer func() {
		_ = Close()
	}()

	var l *ELog
	l.Info("nil safe", "entityId", 1001)

	line := buf.String()
	if !strings.Contains(line, "nil safe") || !strings.Contains(line, "entityId=1001") {
		t.Fatalf("nil elog should still write through default logger: %s", line)
	}
}

func TestFileLogRotatesByTime(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 26, 12, 30, 0, 0, time.Local)
	if err := Init(Options{
		Level:          slog.LevelInfo,
		File:           true,
		Dir:            dir,
		Filename:       "cube.log",
		RotateInterval: time.Hour,
		NowFunc: func() time.Time {
			return now
		},
		DisableGoID:  true,
		DisableFrame: true,
	}); err != nil {
		t.Fatalf("Init error: %v", err)
	}

	Info("first")
	now = now.Add(time.Hour)
	Info("second")
	if err := Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	firstPath := filepath.Join(dir, "cube.2026052612.log")
	secondPath := filepath.Join(dir, "cube.2026052613.log")
	first, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatalf("read first log: %v", err)
	}
	second, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatalf("read second log: %v", err)
	}
	if !strings.Contains(string(first), "first") || strings.Contains(string(first), "second") {
		t.Fatalf("unexpected first log content: %s", first)
	}
	if !strings.Contains(string(second), "second") || strings.Contains(string(second), "first") {
		t.Fatalf("unexpected second log content: %s", second)
	}
}
