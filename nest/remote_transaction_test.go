package nest

import (
	"context"
	"errors"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestRemoteManagedDispatchRejectsBroadcast(t *testing.T) {
	const kind entity.EntityKind = 195
	entity.MustRegisterEntityKindDefs(entity.EntityKindDef{Kind: kind, Category: 1, RemotePolicy: entity.RemotePolicyManaged})
	id, err := entity.BuildEntityID(9202, kind)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareRemoteWriteBatch(nil, &Msg{Type: MsgTypeBroadcast, Tids: []int64{id}, HasRemote: true}); !errors.Is(err, ErrRemoteBroadcastUnsupported) {
		t.Fatalf("broadcast error=%v", err)
	}
}

type remoteBatchIntegrationFake struct {
	commit        entity.RemoteCommit
	aborted       bool
	indeterminate bool
	closed        bool
}

func (b *remoteBatchIntegrationFake) EntityIDs() []int64 { return []int64{b.commit.EntityID} }
func (b *remoteBatchIntegrationFake) FinalizeLocked(outcome entity.RemoteTransactionOutcome) error {
	b.commit.TransactionID = outcome.TransactionID
	return nil
}
func (b *remoteBatchIntegrationFake) Commits() []entity.RemoteCommit {
	return []entity.RemoteCommit{b.commit.Clone()}
}
func (b *remoteBatchIntegrationFake) Commit(context.Context) ([]entity.RemoteCommitReceipt, error) {
	return nil, nil
}
func (b *remoteBatchIntegrationFake) Abort(context.Context, error) error {
	b.aborted = true
	return nil
}
func (b *remoteBatchIntegrationFake) Indeterminate(context.Context, error) error {
	b.indeterminate = true
	return nil
}
func (b *remoteBatchIntegrationFake) Close(context.Context) error {
	b.closed = true
	return nil
}

func TestFinalizeRemoteWriteBatchProducesValidWALMutation(t *testing.T) {
	const kind entity.EntityKind = 194
	entity.MustRegisterEntityKindDefs(entity.EntityKindDef{Kind: kind, Category: 1, RemotePolicy: entity.RemotePolicyManaged})
	id, err := entity.BuildEntityID(9201, kind)
	if err != nil {
		t.Fatal(err)
	}
	batch := &remoteBatchIntegrationFake{commit: entity.RemoteCommit{
		EntityID: id, Kind: kind, BaseVersion: 1, NextVersion: 2,
		MarkerEpoch: 1, RouteEpoch: 1, Schema: 1, Codec: 1,
		Mutations: []entity.RemoteDataMutation{{Collection: "players", ID: id, Version: 2, Mask: 1, Data: []byte("state")}},
	}}
	msg := &Msg{RemoteWriteBatch: batch}
	tx := NewRollbackTx(RollbackState)
	tx.handler = "remote.integration"
	if err := msg.finalizeRemoteWriteBatch(tx); err != nil {
		t.Fatal(err)
	}
	record, err := tx.prepareCommitRecord()
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Mutations) != 1 || record.Mutations[0].Remote == nil || len(record.Mutations[0].Data) != 0 {
		t.Fatalf("record=%+v", record)
	}
}

func TestCommitRecordAcceptsMixedRemoteAndOrdinaryMutations(t *testing.T) {
	const kind entity.EntityKind = 193
	entity.MustRegisterEntityKindDefs(entity.EntityKindDef{Kind: kind, Category: 1, RemotePolicy: entity.RemotePolicyManaged})
	id, err := entity.BuildEntityID(9200, kind)
	if err != nil {
		t.Fatal(err)
	}
	txID := newTransactionID()
	remote := entity.RemoteCommit{
		TransactionID: entity.RemoteTransactionID(txID),
		EntityID:      id,
		Kind:          kind,
		BaseVersion:   1,
		NextVersion:   2,
		MarkerEpoch:   1,
		RouteEpoch:    1,
		Schema:        1,
		Codec:         1,
		Mutations: []entity.RemoteDataMutation{{
			Collection: "players",
			ID:         id,
			Version:    2,
			Mask:       1,
			Data:       []byte("remote-state"),
		}},
	}
	record := CommitRecord{
		ID: txID,
		Mutations: []EntityMutation{
			{EntityID: id, Resource: "players", Remote: &remote},
			{EntityID: 42, Resource: "mail", Data: []byte("ordinary-state")},
		},
	}
	if err := validateCommitRecord(record); err != nil {
		t.Fatalf("validate error=%v", err)
	}
}

func TestRemoteTransactionUsesNestOutbox(t *testing.T) {
	tx := NewRollbackTx(RollbackNone)
	tx.remoteWrite = true
	if err := tx.Emit(Effect{Topic: "player.changed", Payload: []byte("event")}); err != nil {
		t.Fatal(err)
	}
	if tx.durability != DurabilityStrict || len(tx.effects) != 1 {
		t.Fatalf("durability=%v effects=%d", tx.durability, len(tx.effects))
	}
}

func TestIndeterminateRemoteBatchIsNotAborted(t *testing.T) {
	batch := &remoteBatchIntegrationFake{}
	msg := &Msg{RemoteWriteBatch: batch, remoteFinalized: true}
	if err := msg.markRemoteWriteIndeterminateLocked(ErrCommitIndeterminate); err != nil {
		t.Fatal(err)
	}
	if err := msg.finishRemoteWriteBatch(context.Background(), ErrCommitIndeterminate); err != nil {
		t.Fatal(err)
	}
	if !batch.indeterminate || batch.aborted || !batch.closed {
		t.Fatalf("indeterminate=%v aborted=%v closed=%v", batch.indeterminate, batch.aborted, batch.closed)
	}
}
