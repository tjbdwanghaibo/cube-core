package app

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/clock"
	"github.com/tjbdwanghaibo/cube-core/lifecycle"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	flog "github.com/tjbdwanghaibo/cube-core/log"
	"github.com/tjbdwanghaibo/cube-core/nest"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// App is the top-level application container.
// It wires Mods and Services together with CLI support.
type App struct {
	name    string
	version string
	rootCmd *cobra.Command

	mods     []Mod
	services map[ServiceName]*serviceEntry

	// runtime
	registry *Registry
	cfg      *viper.Viper
}

// serviceEntry holds a service and its specific mods.
type serviceEntry struct {
	svc  Service
	mods []Mod
}

func New(name, version string) *App {
	a := &App{
		name:     name,
		version:  version,
		services: make(map[ServiceName]*serviceEntry),
		cfg:      viper.New(),
	}
	a.rootCmd = &cobra.Command{
		Use:     name,
		Version: version,
		Short:   fmt.Sprintf("%s game server", name),
	}
	return a
}

// Mods registers infrastructure modules (order matters for init).
func (a *App) Mods(mods ...Mod) *App {
	a.mods = append(a.mods, mods...)
	return a
}

// RegisterServer registers a service as a CLI subcommand.
// Optional mods are service-specific and only start when this service runs.
func (a *App) RegisterServer(serverType ServiceName, svc Service, mods ...Mod) *App {
	a.services[serverType] = &serviceEntry{svc: svc, mods: mods}
	st := string(serverType)
	cmd := &cobra.Command{
		Use:   st,
		Short: fmt.Sprintf("Start %s server", st),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.run(serverType)
		},
	}
	a.rootCmd.AddCommand(cmd)
	return a
}

// Execute parses CLI args and runs the appropriate subcommand.
func (a *App) Execute() error {
	a.rootCmd.PersistentFlags().StringP("config", "c", "", "config file path; empty uses configs/service/config.<service>.yaml")
	a.rootCmd.PersistentFlags().Int("sid", 1000, "server id")
	return a.rootCmd.Execute()
}

// RootCmd returns the root cobra command for further customization.
func (a *App) RootCmd() *cobra.Command {
	return a.rootCmd
}

func (a *App) run(serverType ServiceName) error {
	// --- Load config ---
	cfgPath, _ := a.rootCmd.Flags().GetString("config")
	if cfgPath == "" {
		cfgPath = defaultServiceConfigPath(serverType)
	}
	sid, _ := a.rootCmd.Flags().GetInt("sid")

	a.cfg.SetConfigFile(cfgPath)
	a.cfg.SetDefault("sid", sid)
	a.cfg.SetDefault("server_type", serverType)
	a.cfg.SetDefault("log.level", "info")
	a.cfg.SetDefault("log.json", false)
	a.cfg.SetDefault("log.stdout", true)
	a.cfg.SetDefault("log.file", true)
	a.cfg.SetDefault("log.dir", "log")
	a.cfg.SetDefault("log.caller", false)
	a.cfg.SetDefault("log.rotate_interval", "24h")
	a.cfg.SetDefault("player_protocol.rate_limit.enabled", false)

	if err := a.cfg.ReadInConfig(); err != nil {
		slog.Warn("config file not found, using defaults", "path", cfgPath, "err", err)
	}
	a.cfg.Set("server_type", serverType)
	if a.rootCmd.Flags().Changed("sid") {
		a.cfg.Set("sid", sid)
	}
	if err := ValidateServiceConfig(a.cfg); err != nil {
		return err
	}
	clock.SetOffset(a.cfg.GetDuration("time.logic_offset"))
	fctx.SetRuntimeConfig(a.cfg)
	if err := flog.Init(flog.Options{
		LevelText:        a.cfg.GetString("log.level"),
		JSON:             a.cfg.GetBool("log.json"),
		Stdout:           a.cfg.GetBool("log.stdout"),
		File:             a.cfg.GetBool("log.file"),
		Dir:              a.cfg.GetString("log.dir"),
		Service:          string(serverType),
		Sid:              a.cfg.GetInt("sid"),
		Caller:           a.cfg.GetBool("log.caller"),
		RotateInterval:   a.cfg.GetDuration("log.rotate_interval"),
		RotateTimeFormat: a.cfg.GetString("log.rotate_time_format"),
		FrameFunc:        nest.CurTick,
	}); err != nil {
		return fmt.Errorf("init log: %w", err)
	}

	slog.Info("starting server",
		"name", a.name,
		"version", a.version,
		"type", serverType,
		"sid", a.cfg.GetInt("sid"),
		"config", cfgPath,
	)

	// --- Init registry ---
	a.registry = NewRegistry(a.cfg)
	if err := a.emitLifecycle(context.Background(), lifecycle.Event{
		Phase:   lifecycle.PhaseAppInit,
		Service: string(serverType),
		Name:    a.name,
		Data: map[string]any{
			"sid":    a.cfg.GetInt("sid"),
			"config": cfgPath,
		},
	}); err != nil {
		return err
	}
	sharedMods, err := sortMods(a.mods, nil)
	if err != nil {
		return err
	}
	var startedSharedMods []Mod
	var startedServiceMods []Mod
	var providedSharedMods []Mod
	var providedServiceMods []Mod

	// --- Mods lifecycle: Init → Provide → Start ---
	for _, mod := range sharedMods {
		slog.Info("mod init", "mod", mod.Name())
		if err := mod.Init(a.cfg); err != nil {
			return fmt.Errorf("mod %s init: %w", mod.Name(), err)
		}
	}
	for _, mod := range sharedMods {
		if err := mod.Provide(a.registry); err != nil {
			stopModsReverse(providedSharedMods, "mod stop after provide error")
			return fmt.Errorf("mod %s provide: %w", mod.Name(), err)
		}
		providedSharedMods = append(providedSharedMods, mod)
	}
	for _, mod := range sharedMods {
		slog.Info("mod start", "mod", mod.Name())
		if err := mod.Start(); err != nil {
			stopModsReverse(providedSharedMods, "mod stop")
			return fmt.Errorf("mod %s start: %w", mod.Name(), err)
		}
		startedSharedMods = append(startedSharedMods, mod)
	}

	// --- Service entry ---
	entry, ok := a.services[serverType]
	if !ok {
		stopModsReverse(startedSharedMods, "mod stop")
		return fmt.Errorf("unknown server type: %s", serverType)
	}
	sharedNames := make(map[ModName]struct{}, len(sharedMods))
	for _, mod := range sharedMods {
		sharedNames[mod.Name()] = struct{}{}
	}
	serviceMods, err := sortMods(entry.mods, sharedNames)
	if err != nil {
		stopModsReverse(startedSharedMods, "mod stop")
		return err
	}

	// --- Service-specific Mods lifecycle: Init → Provide → Start ---
	for _, mod := range serviceMods {
		slog.Info("mod init (service-specific)", "mod", mod.Name())
		if err := mod.Init(a.cfg); err != nil {
			stopModsReverse(startedSharedMods, "mod stop")
			return fmt.Errorf("mod %s init: %w", mod.Name(), err)
		}
	}
	for _, mod := range serviceMods {
		if err := mod.Provide(a.registry); err != nil {
			stopModsReverse(providedServiceMods, "mod stop after provide error (service-specific)")
			stopModsReverse(startedSharedMods, "mod stop")
			return fmt.Errorf("mod %s provide: %w", mod.Name(), err)
		}
		providedServiceMods = append(providedServiceMods, mod)
	}
	for _, mod := range serviceMods {
		slog.Info("mod start (service-specific)", "mod", mod.Name())
		if err := mod.Start(); err != nil {
			stopModsReverse(providedServiceMods, "mod stop (service-specific)")
			stopModsReverse(startedSharedMods, "mod stop")
			return fmt.Errorf("mod %s start: %w", mod.Name(), err)
		}
		startedServiceMods = append(startedServiceMods, mod)
	}
	if err := a.emitLifecycle(context.Background(), lifecycle.Event{
		Phase:   lifecycle.PhaseModsStarted,
		Service: string(serverType),
		Name:    a.name,
	}); err != nil {
		stopModsReverse(startedServiceMods, "mod stop (service-specific)")
		stopModsReverse(startedSharedMods, "mod stop")
		return err
	}

	// --- Service lifecycle: Init → Serve ---
	svc := entry.svc
	slog.Info("service init", "service", svc.Name())
	if err := svc.Init(a.registry); err != nil {
		stopModsReverse(startedServiceMods, "mod stop (service-specific)")
		stopModsReverse(startedSharedMods, "mod stop")
		return fmt.Errorf("service %s init: %w", svc.Name(), err)
	}
	if err := a.emitLifecycle(context.Background(), lifecycle.Event{
		Phase:   lifecycle.PhaseServiceStarted,
		Service: string(serverType),
		Name:    string(svc.Name()),
	}); err != nil {
		stopModsReverse(startedServiceMods, "mod stop (service-specific)")
		stopModsReverse(startedSharedMods, "mod stop")
		return err
	}

	// Register signal handling before Serve can expose readiness and receive
	// external termination.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	// Serve in background, wait for signal
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- svc.Serve(ctx)
	}()

	// Wait for signal or serve error
	var serviceErr error
	serveDone := false
	select {
	case sig := <-sigChan:
		slog.Info("received signal, shutting down", "signal", sig)
	case err := <-serveErr:
		serveDone = true
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("service exited with error", "err", err)
			serviceErr = err
		}
	}
	cancel()

	if !serveDone {
		waitTimeout := a.cfg.GetDuration("shutdown.serve_wait_timeout")
		if waitTimeout <= 0 {
			waitTimeout = 5 * time.Second
		}
		timer := time.NewTimer(waitTimeout)
		select {
		case err := <-serveErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("service exited with error during shutdown", "err", err)
				serviceErr = err
			}
		case <-timer.C:
			slog.Warn("service did not exit after context cancellation", "service", svc.Name(), "timeout", waitTimeout)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	// --- Graceful shutdown ---
	shutdownTimeout := a.cfg.GetDuration("shutdown.total_timeout")
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	slog.Info("service shutdown", "service", svc.Name())
	_ = a.emitLifecycle(shutdownCtx, lifecycle.Event{
		Phase:   lifecycle.PhaseServiceStopping,
		Service: string(serverType),
		Name:    string(svc.Name()),
	})
	var shutdownErr error
	if err := svc.Shutdown(shutdownCtx); err != nil {
		slog.Error("service shutdown error", "err", err)
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("service %s shutdown: %w", svc.Name(), err))
	}

	// Stop service-specific mods in reverse order
	if err := stopModsReverseWithContext(shutdownCtx, startedServiceMods, "mod stop (service-specific)"); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
	}

	// Stop shared mods in reverse order
	if err := stopModsReverseWithContext(shutdownCtx, startedSharedMods, "mod stop"); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
	}

	slog.Info("server stopped", "type", serverType)
	_ = a.emitLifecycle(context.Background(), lifecycle.Event{
		Phase:   lifecycle.PhaseServiceStopped,
		Service: string(serverType),
		Name:    string(svc.Name()),
	})
	return errors.Join(serviceErr, shutdownErr)
}

func defaultServiceConfigPath(serverType ServiceName) string {
	return fmt.Sprintf("configs/service/config.%s.yaml", serverType)
}

func stopModsReverse(mods []Mod, msg string) {
	if err := stopModsReverseWithContext(context.Background(), mods, msg); err != nil {
		slog.Error("mod stop failed", "err", err)
	}
}

func stopModsReverseWithContext(ctx context.Context, mods []Mod, msg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var joined error
	for i := len(mods) - 1; i >= 0; i-- {
		slog.Info(msg, "mod", mods[i].Name())
		if stopper, ok := mods[i].(ModStopperWithContext); ok {
			if err := stopper.StopWithContext(ctx); err != nil {
				slog.Error("mod stop failed", "mod", mods[i].Name(), "err", err)
				joined = errors.Join(joined, fmt.Errorf("mod %s stop: %w", mods[i].Name(), err))
			}
			continue
		}
		mods[i].Stop()
	}
	return joined
}

func (a *App) emitLifecycle(ctx context.Context, event lifecycle.Event) error {
	if a == nil || a.registry == nil {
		return fmt.Errorf("app: lifecycle registry unavailable")
	}
	reg, ok := Lookup[*lifecycle.Registry](a.registry, ModLifecycle)
	if !ok || reg == nil {
		return fmt.Errorf("app: capability %q not found or wrong type", ModLifecycle)
	}
	return reg.Emit(ctx, event)
}

func sortMods(mods []Mod, external map[ModName]struct{}) ([]Mod, error) {
	if len(mods) <= 1 {
		return append([]Mod(nil), mods...), nil
	}

	byName := make(map[ModName]Mod, len(mods))
	order := make(map[ModName]int, len(mods))
	for i, mod := range mods {
		if mod == nil {
			return nil, fmt.Errorf("mod entry %d is nil", i)
		}
		name := mod.Name()
		if name == "" {
			return nil, fmt.Errorf("mod entry %d has empty name", i)
		}
		if _, exists := byName[name]; exists {
			return nil, fmt.Errorf("duplicate mod %q", name)
		}
		byName[name] = mod
		order[name] = i
	}

	visiting := make(map[ModName]bool, len(mods))
	visited := make(map[ModName]bool, len(mods))
	out := make([]Mod, 0, len(mods))

	var visit func(ModName) error
	visit = func(name ModName) error {
		if visited[name] {
			return nil
		}
		if visiting[name] {
			return fmt.Errorf("mod dependency cycle at %q", name)
		}
		mod, ok := byName[name]
		if !ok {
			if _, ok := external[name]; ok {
				return nil
			}
			return fmt.Errorf("unknown mod dependency %q", name)
		}
		visiting[name] = true
		if depProvider, ok := mod.(ModDependencyProvider); ok {
			deps := append([]ModName(nil), depProvider.DependsOn()...)
			sort.SliceStable(deps, func(i, j int) bool {
				return order[deps[i]] < order[deps[j]]
			})
			for _, dep := range deps {
				if dep == "" {
					continue
				}
				if err := visit(dep); err != nil {
					return fmt.Errorf("mod %s depends on %s: %w", name, dep, err)
				}
			}
		}
		visiting[name] = false
		visited[name] = true
		out = append(out, mod)
		return nil
	}

	names := make([]ModName, 0, len(mods))
	for name := range byName {
		names = append(names, name)
	}
	sort.SliceStable(names, func(i, j int) bool {
		return order[names[i]] < order[names[j]]
	})
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return out, nil
}
