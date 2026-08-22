package nest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/tjbdwanghaibo/cube-core/checkpoint"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

type RollbackPolicy uint8

const (
	RollbackNone RollbackPolicy = iota
	RollbackState
	RollbackUndo
)

func ParseRollbackPolicy(value string) (RollbackPolicy, error) {
	switch value {
	case "", "none":
		return RollbackNone, nil
	case "state":
		return RollbackState, nil
	case "undo":
		return RollbackUndo, nil
	default:
		return RollbackNone, fmt.Errorf("nest: unsupported rollback policy %q", value)
	}
}

func (p RollbackPolicy) String() string {
	switch p {
	case RollbackNone:
		return "none"
	case RollbackState:
		return "state"
	case RollbackUndo:
		return "undo"
	default:
		return "invalid"
	}
}

type HandlerMeta struct {
	Rollback   RollbackPolicy
	Durability DurabilityPolicy
}

// RollbackParticipant can be implemented by an entity, component, or DAO that
// needs custom state rollback beyond the generated DAO snapshot fallback.
type RollbackParticipant interface {
	CaptureRollback(tx *RollbackTx) error
}

type RollbackTx struct {
	id             TransactionID
	policy         RollbackPolicy
	durability     DurabilityPolicy
	handler        string
	state          rollbackTxState
	rollbacks      []func() error
	commits        []func()
	undoKeys       map[undoKey]struct{}
	participants   []CommitParticipant
	participantSet map[CommitParticipant]struct{}
	mutations      []EntityMutation
	mutationKeys   map[mutationKey]struct{}
	effects        []Effect
	effectIDs      map[string]struct{}
	remoteWrite    bool
}

type rollbackTxState uint8

const (
	rollbackTxOpen rollbackTxState = iota
	rollbackTxCommitted
	rollbackTxRolledBack
)

type undoKey struct {
	owner any
	field uint64
	token any
}

type mutationKey struct {
	database string
	resource string
	entityID int64
}

func NewRollbackTx(policy RollbackPolicy) *RollbackTx {
	return &RollbackTx{id: newTransactionID(), policy: policy}
}

func (tx *RollbackTx) ID() TransactionID {
	if tx == nil {
		return TransactionID{}
	}
	return tx.id
}

func (tx *RollbackTx) Policy() RollbackPolicy {
	if tx == nil {
		return RollbackNone
	}
	return tx.policy
}

func (tx *RollbackTx) DeferRollback(fn func() error) {
	if tx != nil && tx.state == rollbackTxOpen && fn != nil {
		tx.rollbacks = append(tx.rollbacks, fn)
	}
}

// RecordUndo records at most one inverse operation for owner/field in this
// transaction. owner must be comparable; generated code passes a DAO pointer.
func (tx *RollbackTx) RecordUndo(owner any, field uint64, fn func() error) error {
	return tx.RecordUndoToken(owner, field, nil, fn)
}

// RecordUndoToken records an inverse operation for a field sub-resource. The
// token lets generated collection setters independently capture multiple map
// keys while still coalescing repeated writes to the same key.
func (tx *RollbackTx) RecordUndoToken(owner any, field uint64, token any, fn func() error) error {
	if tx == nil || tx.state != rollbackTxOpen {
		return ErrTransactionClosed
	}
	if owner == nil || fn == nil {
		return errors.New("nest: invalid undo operation")
	}
	t := reflect.TypeOf(owner)
	if !t.Comparable() {
		return errors.New("nest: undo owner is not comparable")
	}
	if token != nil && !reflect.TypeOf(token).Comparable() {
		return errors.New("nest: undo token is not comparable")
	}
	if tx.undoKeys == nil {
		tx.undoKeys = make(map[undoKey]struct{}, 8)
	}
	key := undoKey{owner: owner, field: field, token: token}
	if _, exists := tx.undoKeys[key]; exists {
		return nil
	}
	tx.undoKeys[key] = struct{}{}
	tx.rollbacks = append(tx.rollbacks, fn)
	return nil
}

// RecordUndo adds an inverse operation to the active Nest transaction. It
// returns false outside a rollback=undo handler.
func RecordUndo(owner any, field uint64, fn func() error) bool {
	tx := CurrentRollbackTx()
	if tx == nil || tx.policy != RollbackUndo {
		return false
	}
	return tx.RecordUndo(owner, field, fn) == nil
}

// RecordUndoToken is the active-transaction counterpart of
// (*RollbackTx).RecordUndoToken.
func RecordUndoToken(owner any, field uint64, token any, fn func() error) bool {
	tx := CurrentRollbackTx()
	if tx == nil || tx.policy != RollbackUndo {
		return false
	}
	return tx.RecordUndoToken(owner, field, token, fn) == nil
}

func (tx *RollbackTx) AfterCommit(fn func()) {
	if tx != nil && tx.state == rollbackTxOpen && fn != nil {
		tx.commits = append(tx.commits, fn)
	}
}

func (tx *RollbackTx) RegisterCommitParticipant(participant CommitParticipant) error {
	if tx == nil || tx.state != rollbackTxOpen {
		return ErrTransactionClosed
	}
	if isNilCommitParticipant(participant) {
		return nil
	}
	t := reflect.TypeOf(participant)
	if !t.Comparable() {
		return errors.New("nest: commit participant is not comparable")
	}
	if tx.participantSet == nil {
		tx.participantSet = make(map[CommitParticipant]struct{}, 4)
	}
	if _, exists := tx.participantSet[participant]; exists {
		return nil
	}
	tx.participantSet[participant] = struct{}{}
	tx.participants = append(tx.participants, participant)
	return nil
}

func isNilCommitParticipant(participant CommitParticipant) bool {
	if participant == nil {
		return true
	}
	v := reflect.ValueOf(participant)
	return (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil()
}

func (tx *RollbackTx) AddMutation(mutation EntityMutation) error {
	if tx == nil || tx.state != rollbackTxOpen {
		return ErrTransactionClosed
	}
	if mutation.EntityID == 0 || mutation.Resource == "" || (len(mutation.Data) == 0 && mutation.Remote == nil) {
		return errors.New("nest: invalid entity mutation")
	}
	if tx.mutationKeys == nil {
		tx.mutationKeys = make(map[mutationKey]struct{}, 4)
	}
	key := mutationKey{database: mutation.Database, resource: mutation.Resource, entityID: mutation.EntityID}
	if _, exists := tx.mutationKeys[key]; exists {
		return fmt.Errorf("nest: duplicate entity mutation %s/%s/%d", mutation.Database, mutation.Resource, mutation.EntityID)
	}
	tx.mutationKeys[key] = struct{}{}
	tx.mutations = append(tx.mutations, cloneMutation(mutation))
	return nil
}

func (tx *RollbackTx) Emit(effect Effect) error {
	if tx == nil || tx.state != rollbackTxOpen {
		return ErrTransactionClosed
	}
	if effect.Topic == "" {
		return errors.New("nest: effect topic is empty")
	}
	if effect.ID == "" {
		effect.ID = tx.id.String() + ":" + fmt.Sprint(len(tx.effects)+1)
	}
	if tx.effectIDs == nil {
		tx.effectIDs = make(map[string]struct{}, 4)
	}
	if _, exists := tx.effectIDs[effect.ID]; exists {
		return fmt.Errorf("nest: duplicate effect id %q", effect.ID)
	}
	tx.effectIDs[effect.ID] = struct{}{}
	// An outbox item is only useful if its admission is durable before the
	// entity lock is released. Upgrade handlers that did not explicitly select
	// a durability policy instead of silently providing a lossy "outbox".
	if tx.durability == DurabilityMemory {
		tx.durability = DurabilityStrict
	}
	tx.effects = append(tx.effects, cloneEffect(effect))
	return nil
}

func Emit(effect Effect) error {
	tx := CurrentRollbackTx()
	if tx == nil {
		return ErrTransactionClosed
	}
	return tx.Emit(effect)
}

func (tx *RollbackTx) Rollback() error {
	if tx == nil {
		return nil
	}
	if tx.state == rollbackTxRolledBack {
		return nil
	}
	if tx.state != rollbackTxOpen {
		return ErrTransactionClosed
	}
	tx.state = rollbackTxRolledBack
	var errs []error
	for i := len(tx.rollbacks) - 1; i >= 0; i-- {
		if tx.rollbacks[i] == nil {
			continue
		}
		if err := tx.rollbacks[i](); err != nil {
			errs = append(errs, err)
		}
	}
	tx.commits = nil
	tx.mutations = nil
	tx.mutationKeys = nil
	tx.effects = nil
	tx.effectIDs = nil
	return errors.Join(errs...)
}

func (tx *RollbackTx) Commit() {
	if tx == nil || tx.state != rollbackTxOpen {
		return
	}
	tx.state = rollbackTxCommitted
	tx.rollbacks = nil
	tx.undoKeys = nil
	tx.participants = nil
	tx.participantSet = nil
	tx.mutationKeys = nil
	tx.effectIDs = nil
	if len(tx.commits) == 0 {
		return
	}
	if msg := currentNestDispatchMsg(); msg != nil && msg.RemoteWriteBatch != nil {
		msg.addPostRemoteCommit(tx.commits...)
		return
	}
	if scope := entity.CurrentGuardScope(); scope != nil && scope.Guard() != nil {
		for _, fn := range tx.commits {
			scope.Guard().AppendPostRelease(fn)
		}
		return
	}
	for _, fn := range tx.commits {
		if fn != nil {
			fn()
		}
	}
}

// abandon closes an indeterminate transaction without executing rollback or
// after-commit hooks. The hosting process is expected to stop accepting writes
// and recover the authoritative outcome from WAL.
func (tx *RollbackTx) abandon() {
	if tx == nil || tx.state != rollbackTxOpen {
		return
	}
	tx.state = rollbackTxCommitted
	tx.rollbacks = nil
	tx.commits = nil
	tx.undoKeys = nil
	tx.participants = nil
	tx.participantSet = nil
	tx.mutationKeys = nil
	tx.effectIDs = nil
}

func (tx *RollbackTx) prepareCommitRecord() (CommitRecord, error) {
	if tx == nil || tx.state != rollbackTxOpen {
		return CommitRecord{}, ErrTransactionClosed
	}
	for _, participant := range tx.participants {
		if isNilCommitParticipant(participant) {
			continue
		}
		if err := participant.PrepareCommit(tx); err != nil {
			return CommitRecord{}, fmt.Errorf("nest: prepare commit: %w", err)
		}
	}
	requestID := tx.requestID()
	record := CommitRecord{
		ID: tx.id, Handler: tx.handler, RequestID: requestID, CreatedAt: time.Now().UnixNano(), Durability: tx.durability,
		Mutations: append([]EntityMutation(nil), tx.mutations...),
		Effects:   append([]Effect(nil), tx.effects...),
	}
	if !record.Empty() {
		if err := validateCommitRecord(record); err != nil {
			return CommitRecord{}, err
		}
	}
	return record, nil
}

func (tx *RollbackTx) requestID() string {
	requestID := ""
	if current := fctx.CurrentContext(); current != nil {
		requestID = current.Trace.TraceID
		if requestID == "" && (current.Meta.PlayerID != 0 || current.Meta.MsgID != 0 || current.Meta.Seq != 0) {
			requestID = fmt.Sprintf("player:%d/msg:%d/seq:%d", current.Meta.PlayerID, current.Meta.MsgID, current.Meta.Seq)
		}
	}
	return requestID
}

func (tx *RollbackTx) durableCommit(ctx context.Context, committer TransactionCommitter) error {
	// Memory-only handlers persist through entity release hooks. Avoid
	// materializing after-images when no WAL/outbox admission is involved.
	if tx.durability == DurabilityMemory && len(tx.effects) == 0 {
		return nil
	}
	record, err := tx.prepareCommitRecord()
	if err != nil {
		return err
	}
	if record.Empty() {
		return nil
	}
	if committer == nil {
		if tx.durability != DurabilityMemory || len(record.Effects) > 0 {
			return ErrCommitterRequired
		}
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := committer.Commit(ctx, record); err != nil {
		if errors.Is(err, ErrCommitIndeterminate) {
			return err
		}
		return errors.Join(ErrCommitRejected, err)
	}
	return nil
}

type rollbackContextKey struct{}

func CurrentRollbackTx() *RollbackTx {
	c := fctx.CurrentContext()
	if c == nil {
		return nil
	}
	v, ok := c.Get(rollbackContextKey{})
	if !ok {
		return nil
	}
	tx, _ := v.(*RollbackTx)
	return tx
}

func AfterCommit(fn func()) bool {
	tx := CurrentRollbackTx()
	if tx == nil {
		return false
	}
	tx.AfterCommit(fn)
	return true
}

func withRollbackTx(tx *RollbackTx, fn func() (any, error)) (any, error) {
	c := fctx.CurrentContext()
	if c == nil || tx == nil {
		return fn()
	}
	old, hadOld := c.Get(rollbackContextKey{})
	c.Set(rollbackContextKey{}, tx)
	defer func() {
		if hadOld {
			c.Set(rollbackContextKey{}, old)
		} else {
			c.Set(rollbackContextKey{}, nil)
		}
	}()
	return fn()
}

func invokeWithTransaction(meta HandlerMeta, es []entity.IThreadSafeEntity, committer TransactionCommitter, handler string, call func() (any, error)) (ret any, err error) {
	msg := currentNestDispatchMsg()
	if meta.Rollback == RollbackNone && meta.Durability == DurabilityMemory && (msg == nil || msg.RemoteWriteBatch == nil) {
		return call()
	}
	tx := NewRollbackTx(meta.Rollback)
	tx.durability = meta.Durability
	tx.handler = handler
	if err := tx.CaptureEntities(es); err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				slog.Error("nest rollback after panic failed", "rollback", meta.Rollback.String(), "err", rbErr)
				panic(errors.Join(fmt.Errorf("panic: %v", r), ErrRollbackFailed, rbErr))
			}
			panic(r)
		}
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", rbErr))
			}
			return
		}
		commitCtx := context.Background()
		if current := fctx.CurrentContext(); current != nil && current.Base != nil {
			commitCtx = current.Base
		}
		if msg := currentNestDispatchMsg(); msg != nil && msg.RemoteWriteBatch != nil {
			if finalizeErr := msg.finalizeRemoteWriteBatch(tx); finalizeErr != nil {
				err = finalizeErr
				if abortErr := msg.abortRemoteWriteBatchLocked(finalizeErr); abortErr != nil {
					err = errors.Join(err, abortErr)
				}
				if rbErr := tx.Rollback(); rbErr != nil {
					err = errors.Join(err, ErrRollbackFailed, rbErr)
				}
				return
			}
		}
		if commitErr := tx.durableCommit(commitCtx, committer); commitErr != nil {
			err = commitErr
			if errors.Is(commitErr, ErrCommitIndeterminate) {
				if msg := currentNestDispatchMsg(); msg != nil && msg.RemoteWriteBatch != nil {
					if markErr := msg.markRemoteWriteIndeterminateLocked(commitErr); markErr != nil {
						err = errors.Join(err, markErr)
					}
				}
				tx.abandon()
				return
			}
			if msg := currentNestDispatchMsg(); msg != nil && msg.RemoteWriteBatch != nil {
				if abortErr := msg.abortRemoteWriteBatchLocked(commitErr); abortErr != nil {
					err = errors.Join(err, abortErr)
				}
			}
			if rbErr := tx.Rollback(); rbErr != nil {
				err = errors.Join(err, ErrRollbackFailed, rbErr)
			}
			return
		}
		if notifier, ok := committer.(TransactionReleaseNotifier); ok {
			txID := tx.ID()
			if msg := currentNestDispatchMsg(); msg != nil && msg.RemoteWriteBatch != nil {
				msg.addAfterUnlock(func() { notifier.TransactionReleased(txID) })
			} else {
				tx.AfterCommit(func() { notifier.TransactionReleased(txID) })
			}
		}
		tx.Commit()
	}()
	return withRollbackTx(tx, call)
}

func (tx *RollbackTx) CaptureEntities(es []entity.IThreadSafeEntity) error {
	if tx == nil {
		return nil
	}
	seen := make(map[int64]struct{}, len(es))
	for _, e := range es {
		if e == nil {
			continue
		}
		if _, ok := seen[e.GUId()]; ok {
			continue
		}
		seen[e.GUId()] = struct{}{}
		remoteManaged := entity.IsEntityKindRemoteManaged(e.GetEntityKind())
		if remoteManaged {
			tx.remoteWrite = true
			msg := currentNestDispatchMsg()
			if tx.durability != DurabilityMemory && (msg == nil || msg.RemoteWriteBatch == nil) {
				return fmt.Errorf("%w: entity=%d kind=%d", ErrDurableRemoteWriteUnsupported, e.ID(), e.GetEntityKind())
			}
		}
		if participant, ok := e.(CommitParticipant); ok && !remoteManaged {
			if err := tx.RegisterCommitParticipant(participant); err != nil {
				return err
			}
		}
		if tx.policy == RollbackNone {
			continue
		}
		if tx.policy == RollbackState {
			if custom, ok := e.(RollbackParticipant); ok {
				if err := custom.CaptureRollback(tx); err != nil {
					return fmt.Errorf("nest rollback capture entity %d: %w", e.ID(), err)
				}
			}
		}
		guardable, ok := e.(entity.Guardable)
		if !ok {
			continue
		}
		var captureErr error
		guardable.RangeDao(func(dao entity.DaoInterface) {
			if captureErr != nil || dao == nil {
				return
			}
			if participant, ok := dao.(CommitParticipant); ok && !remoteManaged {
				if err := tx.RegisterCommitParticipant(participant); err != nil {
					captureErr = err
					return
				}
			}
			captureErr = tx.captureDao(dao)
		})
		if captureErr != nil {
			return captureErr
		}
	}
	return nil
}

type dirtyTrackerDao interface {
	DirtyTracker() *checkpoint.DirtyTracker
}

// RollbackSnapshotter captures all rollback-relevant state without consuming
// persistence patch metadata.
type RollbackSnapshotter interface {
	CaptureRollbackState() ([]byte, error)
	RestoreRollbackState([]byte) error
}

func (tx *RollbackTx) captureDao(dao entity.DaoInterface) error {
	var dirty *checkpoint.DirtyTracker
	var dirtySnapshot checkpoint.DirtySnapshot
	if d, ok := dao.(dirtyTrackerDao); ok {
		dirty = d.DirtyTracker()
		dirtySnapshot = dirty.Snapshot()
	}
	if tx.policy == RollbackUndo {
		if dirty != nil {
			tx.DeferRollback(func() error {
				dirty.Restore(dirtySnapshot)
				return nil
			})
		}
		return nil
	}
	if custom, ok := dao.(RollbackParticipant); ok {
		return custom.CaptureRollback(tx)
	}
	if snapshotter, ok := dao.(RollbackSnapshotter); ok {
		raw, err := snapshotter.CaptureRollbackState()
		if err != nil {
			return err
		}
		raw = append([]byte(nil), raw...)
		tx.DeferRollback(func() error {
			if err := snapshotter.RestoreRollbackState(raw); err != nil {
				return err
			}
			if dirty != nil {
				dirty.Restore(dirtySnapshot)
			}
			return nil
		})
		return nil
	}
	return fmt.Errorf("%w: dao %s/%d requires RollbackSnapshotter or RollbackParticipant", ErrRollbackUnsupported, dao.CollName(), dao.Id())
}
