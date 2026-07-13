package app_test

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/app"
	"fmt"

	"github.com/spf13/viper"
)

// --- Example Mods ---

type MongoMod struct{}

func (m *MongoMod) Name() app.ModName           { return "mongo" }
func (m *MongoMod) Init(cfg *viper.Viper) error { return nil }
func (m *MongoMod) Provide(r *app.Registry) error {
	return r.Register("mongo", "mongo_client_placeholder")
}
func (m *MongoMod) Start() error { return nil }
func (m *MongoMod) Stop()        {}

type RedisMod struct{}

func (m *RedisMod) Name() app.ModName           { return "redis" }
func (m *RedisMod) Init(cfg *viper.Viper) error { return nil }
func (m *RedisMod) Provide(r *app.Registry) error {
	return r.Register("redis", "redis_client_placeholder")
}
func (m *RedisMod) Start() error { return nil }
func (m *RedisMod) Stop()        {}

// --- Example Services ---

type GameService struct{}

func (g *GameService) Name() app.ServiceName              { return "game" }
func (g *GameService) Init(r *app.Registry) error         { return nil }
func (g *GameService) Serve(ctx context.Context) error    { <-ctx.Done(); return nil }
func (g *GameService) Shutdown(ctx context.Context) error { return nil }

type GateService struct{}

func (g *GateService) Name() app.ServiceName              { return "gate" }
func (g *GateService) Init(r *app.Registry) error         { return nil }
func (g *GateService) Serve(ctx context.Context) error    { <-ctx.Done(); return nil }
func (g *GateService) Shutdown(ctx context.Context) error { return nil }

// --- Example Manager ---

type DataLoaderMgr struct{}

func (m *DataLoaderMgr) Name() string                { return "data_loader" }
func (m *DataLoaderMgr) Start(r *app.Registry) error { return nil }
func (m *DataLoaderMgr) Stop()                       {}

type ManagerMod struct {
	managers []app.IManager
}

func (m *ManagerMod) Name() app.ModName           { return "manager" }
func (m *ManagerMod) Init(cfg *viper.Viper) error { return nil }
func (m *ManagerMod) Provide(r *app.Registry) error {
	for _, mgr := range m.managers {
		if err := mgr.Start(r); err != nil {
			return err
		}
	}
	return nil
}
func (m *ManagerMod) Start() error { return nil }
func (m *ManagerMod) Stop() {
	for i := len(m.managers) - 1; i >= 0; i-- {
		m.managers[i].Stop()
	}
}
func (m *ManagerMod) Register(mgr app.IManager) {
	m.managers = append(m.managers, mgr)
}

func Example() {
	// Game-specific managers
	gameMgrs := &ManagerMod{}
	gameMgrs.Register(&DataLoaderMgr{})

	a := app.New("cube", "1.0.0").
		Mods(&MongoMod{}, &RedisMod{}).                   // shared mods
		RegisterServer("game", &GameService{}, gameMgrs). // game with its managers
		RegisterServer("gate", &GateService{})            // gate without managers

	// In real usage:
	//   a.Execute()
	// CLI:
	//   ./cube game -c configs/service/config.game.yaml --sid 2001
	//   ./cube gate -c configs/service/config.gate.yaml --sid 1001

	_ = a
	fmt.Println("app created with shared mods: mongo, redis; game mods: manager; services: game, gate")
	// Output: app created with shared mods: mongo, redis; game mods: manager; services: game, gate
}
