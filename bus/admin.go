package bus

import (
	"context"
	"fmt"

	"github.com/tjbdwanghaibo/cube-core/admin"
)

const (
	AdminCommandBusDLQList    = "bus.dlq.list"
	AdminCommandBusDLQRequeue = "bus.dlq.requeue"
	AdminCommandBusDLQPurge   = "bus.dlq.purge"
)

type DeadLetterCommand struct {
	Module  string `json:"module"`
	MsgName string `json:"msg_name"`
	Start   int64  `json:"start,omitempty"`
	Stop    int64  `json:"stop,omitempty"`
	Limit   int64  `json:"limit,omitempty"`
}

func RegisterAdminCommands(reg *admin.Registry, b *Bus) error {
	if reg == nil {
		return fmt.Errorf("bus: admin registry nil")
	}
	if b == nil {
		return fmt.Errorf("bus: nil bus")
	}
	if err := reg.Register(admin.CommandDef{
		Name:        AdminCommandBusDLQList,
		Description: "list bus dead letter messages",
		Handler: func(ctx context.Context, cmd admin.Command) (admin.Result, error) {
			payload, err := admin.DecodePayload[DeadLetterCommand](cmd)
			if err != nil {
				return admin.Result{}, err
			}
			entries, err := b.DeadLetters(ctx, payload.query())
			if err != nil {
				return admin.Result{}, err
			}
			return admin.Result{Data: map[string]any{
				"count":   len(entries),
				"entries": entries,
			}}, nil
		},
	}); err != nil {
		return err
	}
	if err := reg.Register(admin.CommandDef{
		Name:        AdminCommandBusDLQRequeue,
		Description: "requeue bus dead letter messages",
		Handler: func(ctx context.Context, cmd admin.Command) (admin.Result, error) {
			payload, err := admin.DecodePayload[DeadLetterCommand](cmd)
			if err != nil {
				return admin.Result{}, err
			}
			n, err := b.RequeueDeadLetters(ctx, payload.query())
			if err != nil {
				return admin.Result{}, err
			}
			return admin.Result{Data: map[string]any{"count": n}}, nil
		},
	}); err != nil {
		return err
	}
	return reg.Register(admin.CommandDef{
		Name:        AdminCommandBusDLQPurge,
		Description: "purge bus dead letter messages",
		Handler: func(ctx context.Context, cmd admin.Command) (admin.Result, error) {
			payload, err := admin.DecodePayload[DeadLetterCommand](cmd)
			if err != nil {
				return admin.Result{}, err
			}
			n, err := b.PurgeDeadLetters(ctx, payload.query())
			if err != nil {
				return admin.Result{}, err
			}
			return admin.Result{Data: map[string]any{"count": n}}, nil
		},
	})
}

func (c DeadLetterCommand) query() DeadLetterQuery {
	return DeadLetterQuery{
		Module:  c.Module,
		MsgName: c.MsgName,
		Start:   c.Start,
		Stop:    c.Stop,
		Limit:   c.Limit,
	}
}
