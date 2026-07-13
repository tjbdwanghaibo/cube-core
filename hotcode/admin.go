package hotcode

import (
	"context"
	"sync"

	"github.com/tjbdwanghaibo/cube-core/admin"
)

const (
	AdminCommandList       = "hotcode.list"
	AdminCommandRevert     = "hotcode.revert"
	AdminCommandLoadPlugin = "hotcode.load_plugin"
)

type RevertCommand struct {
	Name string `json:"name"`
}

type LoadPluginCommand struct {
	Path string `json:"path"`
}

var adminOnce sync.Once

func RegisterAdminCommands() {
	adminOnce.Do(func() {
		_ = admin.Register(admin.CommandDef{
			Name:        AdminCommandList,
			Description: "list hot-code patch points",
			Handler: func(context.Context, admin.Command) (admin.Result, error) {
				return admin.Result{Data: map[string]any{"points": List()}}, nil
			},
		})
		_ = admin.Register(admin.CommandDef{
			Name:        AdminCommandRevert,
			Description: "revert one hot-code patch point to its original function",
			Handler: func(_ context.Context, cmd admin.Command) (admin.Result, error) {
				payload, err := admin.DecodePayload[RevertCommand](cmd)
				if err != nil {
					return admin.Result{}, err
				}
				if err := Revert(payload.Name); err != nil {
					return admin.Result{}, err
				}
				return admin.Result{Data: map[string]any{"name": payload.Name}}, nil
			},
		})
		_ = admin.Register(admin.CommandDef{
			Name:        AdminCommandLoadPlugin,
			Description: "load and apply a Go hot-code plugin",
			Handler: func(_ context.Context, cmd admin.Command) (admin.Result, error) {
				payload, err := admin.DecodePayload[LoadPluginCommand](cmd)
				if err != nil {
					return admin.Result{}, err
				}
				bundle, err := LoadPlugin(payload.Path)
				if err != nil {
					return admin.Result{}, err
				}
				return admin.Result{Data: map[string]any{"path": payload.Path, "meta": bundle.Meta()}}, nil
			},
		})
	})
}
