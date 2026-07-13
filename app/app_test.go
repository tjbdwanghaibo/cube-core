package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/viper"
)

var errServeFailed = errors.New("serve failed")

type errService struct{}

func (s *errService) Name() ServiceName { return "game" }
func (s *errService) Init(*Registry) error {
	return nil
}
func (s *errService) Serve(context.Context) error {
	return errServeFailed
}
func (s *errService) Shutdown(context.Context) error {
	return nil
}

func TestAppReturnsServeError(t *testing.T) {
	a := newTestApp(t, &errService{})
	a.RootCmd().SetArgs([]string{"game"})

	err := a.Execute()
	if !errors.Is(err, errServeFailed) {
		t.Fatalf("Execute err = %v, want %v", err, errServeFailed)
	}
}

var (
	errShutdownFailed = errors.New("shutdown failed")
	errModStopFailed  = errors.New("mod stop failed")
)

type shutdownErrService struct{}

func (s *shutdownErrService) Name() ServiceName { return "game" }
func (s *shutdownErrService) Init(*Registry) error {
	return nil
}
func (s *shutdownErrService) Serve(context.Context) error {
	return nil
}
func (s *shutdownErrService) Shutdown(context.Context) error {
	return errShutdownFailed
}

func TestAppReturnsShutdownError(t *testing.T) {
	a := newTestApp(t, &shutdownErrService{})
	a.RootCmd().SetArgs([]string{"game"})

	err := a.Execute()
	if !errors.Is(err, errShutdownFailed) {
		t.Fatalf("Execute err = %v, want shutdown error", err)
	}
}

type failingStopMod struct{}

func (m *failingStopMod) Name() ModName           { return "failing_stop" }
func (m *failingStopMod) Init(*viper.Viper) error { return nil }
func (m *failingStopMod) Provide(*Registry) error { return nil }
func (m *failingStopMod) Start() error            { return nil }
func (m *failingStopMod) Stop()                   {}
func (m *failingStopMod) StopWithContext(context.Context) error {
	return errModStopFailed
}

func TestAppReturnsModStopError(t *testing.T) {
	a := newTestApp(t, &shutdownDeadlineService{}, &failingStopMod{})
	a.RootCmd().SetArgs([]string{"game"})

	err := a.Execute()
	if !errors.Is(err, errModStopFailed) {
		t.Fatalf("Execute err = %v, want mod stop error", err)
	}
}

type shutdownDeadlineService struct {
	deadlineRemaining time.Duration
}

func (s *shutdownDeadlineService) Name() ServiceName { return "game" }
func (s *shutdownDeadlineService) Init(*Registry) error {
	return nil
}
func (s *shutdownDeadlineService) Serve(context.Context) error {
	return nil
}
func (s *shutdownDeadlineService) Shutdown(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("shutdown context has no deadline")
	}
	s.deadlineRemaining = time.Until(deadline)
	return nil
}

func TestAppUsesConfiguredShutdownTimeout(t *testing.T) {
	svc := &shutdownDeadlineService{}
	a := newTestApp(t, svc)
	a.cfg.Set("shutdown.total_timeout", 25*time.Millisecond)
	a.RootCmd().SetArgs([]string{"game"})

	if err := a.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if svc.deadlineRemaining > 500*time.Millisecond {
		t.Fatalf("shutdown deadline remaining = %v, want configured short timeout", svc.deadlineRemaining)
	}
}

type slowSignalService struct {
	started                 chan struct{}
	serveExited             chan struct{}
	shutdownBeforeServeExit atomic.Bool
}

func newSlowSignalService() *slowSignalService {
	return &slowSignalService{
		started:     make(chan struct{}),
		serveExited: make(chan struct{}),
	}
}

func (s *slowSignalService) Name() ServiceName { return "game" }
func (s *slowSignalService) Init(*Registry) error {
	return nil
}
func (s *slowSignalService) Serve(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)
	close(s.serveExited)
	return nil
}
func (s *slowSignalService) Shutdown(context.Context) error {
	select {
	case <-s.serveExited:
	default:
		s.shutdownBeforeServeExit.Store(true)
	}
	return nil
}

func TestAppWaitsForServeExitBeforeShutdownAfterSignal(t *testing.T) {
	svc := newSlowSignalService()
	a := newTestApp(t, svc)
	a.RootCmd().SetArgs([]string{"game"})

	go func() {
		<-svc.started
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()

	if err := a.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if svc.shutdownBeforeServeExit.Load() {
		t.Fatalf("Shutdown ran before Serve exited")
	}
}

type contextStopMod struct {
	stopCalled        atomic.Bool
	stopContextCalled atomic.Bool
}

func (m *contextStopMod) Name() ModName           { return "context_stop" }
func (m *contextStopMod) Init(*viper.Viper) error { return nil }
func (m *contextStopMod) Provide(*Registry) error { return nil }
func (m *contextStopMod) Start() error            { return nil }
func (m *contextStopMod) Stop()                   { m.stopCalled.Store(true) }
func (m *contextStopMod) StopWithContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil stop context")
	}
	m.stopContextCalled.Store(true)
	return nil
}

func TestStopModsReversePrefersContextStopper(t *testing.T) {
	mod := &contextStopMod{}

	if err := stopModsReverseWithContext(context.Background(), []Mod{mod}, "test stop"); err != nil {
		t.Fatalf("stopModsReverseWithContext: %v", err)
	}

	if !mod.stopContextCalled.Load() {
		t.Fatal("StopWithContext was not called")
	}
	if mod.stopCalled.Load() {
		t.Fatal("Stop should not be called when StopWithContext is available")
	}
}

func TestAppPassesLogCallerConfig(t *testing.T) {
	dir := t.TempDir()
	a := New("cube-test", "0.0.0")
	a.RegisterServer("game", &shutdownErrService{})
	a.cfg.Set("log.file", true)
	a.cfg.Set("log.stdout", false)
	a.cfg.Set("log.dir", dir)
	a.cfg.Set("log.caller", true)
	a.cfg.Set("log.rotate_interval", 0)
	a.RootCmd().SetArgs([]string{"game"})

	err := a.Execute()
	if !errors.Is(err, errShutdownFailed) {
		t.Fatalf("Execute err = %v, want shutdown error", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "game-1000.log"))
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	line := string(raw)
	if !strings.Contains(line, "caller=") {
		t.Fatalf("log should contain caller file and line: %s", line)
	}
	if !strings.Contains(line, "caller_func=") {
		t.Fatalf("log should contain caller function: %s", line)
	}
}

func newTestApp(t *testing.T, svc Service, mods ...Mod) *App {
	t.Helper()
	a := New("cube-test", "0.0.0")
	if len(mods) > 0 {
		a.Mods(mods...)
	}
	a.RegisterServer("game", svc)
	a.cfg.Set("log.file", false)
	a.cfg.Set("log.dir", t.TempDir())
	return a
}
