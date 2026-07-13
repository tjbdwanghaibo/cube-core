package entitysync

import (
	"context"
	"fmt"

	"github.com/tjbdwanghaibo/cube-core/admin"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

const (
	AdminCommandEntitySyncFailedList   = "entitysync.failed.list"
	AdminCommandEntitySyncFailedReplay = "entitysync.failed.replay"
	AdminCommandEntitySyncFailedPurge  = "entitysync.failed.purge"
)

type FailedBatchAdminCommand struct {
	ObserverKind uint8  `json:"observer_kind,omitempty"`
	ObserverID   int64  `json:"observer_id,omitempty"`
	ObserverSid  int32  `json:"observer_sid,omitempty"`
	ObserverKey  string `json:"observer_key,omitempty"`
	Start        int64  `json:"start,omitempty"`
	Stop         int64  `json:"stop,omitempty"`
}

type FailedBatchAdminStore interface {
	FailedBatchStore
	ListFailedSyncBatches(context.Context, entity.SyncObserverRef, int64, int64) ([]SyncBatch, error)
	PurgeFailedSyncBatches(context.Context, entity.SyncObserverRef, int64, int64) (int64, error)
}

func RegisterFailedBatchAdminCommands(reg *admin.Registry, store FailedBatchAdminStore, sink entity.EntitySyncSink) error {
	if store == nil {
		return fmt.Errorf("entitysync: failed batch store is nil")
	}
	if reg == nil {
		return fmt.Errorf("entitysync: admin registry nil")
	}
	if err := reg.Register(admin.CommandDef{
		Name:        AdminCommandEntitySyncFailedList,
		Description: "list failed entity sync batches for one observer",
		Handler: func(ctx context.Context, cmd admin.Command) (admin.Result, error) {
			payload, err := admin.DecodePayload[FailedBatchAdminCommand](cmd)
			if err != nil {
				return admin.Result{}, err
			}
			batches, err := store.ListFailedSyncBatches(ctx, payload.observer(), payload.Start, payload.Stop)
			if err != nil {
				return admin.Result{}, err
			}
			return admin.Result{Data: map[string]any{"count": len(batches), "batches": batches}}, nil
		},
	}); err != nil {
		return err
	}
	if err := reg.Register(admin.CommandDef{
		Name:        AdminCommandEntitySyncFailedReplay,
		Description: "replay failed entity sync batches for one observer",
		Handler: func(ctx context.Context, cmd admin.Command) (admin.Result, error) {
			if sink == nil {
				return admin.Result{}, fmt.Errorf("entitysync: replay sink is nil")
			}
			payload, err := admin.DecodePayload[FailedBatchAdminCommand](cmd)
			if err != nil {
				return admin.Result{}, err
			}
			batches, err := store.ListFailedSyncBatches(ctx, payload.observer(), payload.Start, payload.Stop)
			if err != nil {
				return admin.Result{}, err
			}
			for _, batch := range batches {
				if len(batch.Packets) > 0 {
					sink.EnqueueBatch(batch.Packets)
				}
			}
			return admin.Result{Data: map[string]any{"count": len(batches)}}, nil
		},
	}); err != nil {
		return err
	}
	return reg.Register(admin.CommandDef{
		Name:        AdminCommandEntitySyncFailedPurge,
		Description: "purge failed entity sync batches for one observer",
		Handler: func(ctx context.Context, cmd admin.Command) (admin.Result, error) {
			payload, err := admin.DecodePayload[FailedBatchAdminCommand](cmd)
			if err != nil {
				return admin.Result{}, err
			}
			n, err := store.PurgeFailedSyncBatches(ctx, payload.observer(), payload.Start, payload.Stop)
			if err != nil {
				return admin.Result{}, err
			}
			return admin.Result{Data: map[string]any{"count": n}}, nil
		},
	})
}

func (c FailedBatchAdminCommand) observer() entity.SyncObserverRef {
	ref := entity.SyncObserverRef{
		Kind: entity.SyncObserverKind(c.ObserverKind),
		ID:   c.ObserverID,
		Sid:  c.ObserverSid,
		Key:  c.ObserverKey,
	}
	return ref.Normalize()
}
