package nest

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/tjbdwanghaibo/cube-core/checkpoint"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

type RollbackPolicy uint8

const (
	RollbackNone RollbackPolicy = iota
	RollbackDirty
	RollbackState
)

func ParseRollbackPolicy(value string) RollbackPolicy {
	switch value {
	case "dirty":
		return RollbackDirty
	case "state":
		return RollbackState
	default:
		return RollbackNone
	}
}

func (p RollbackPolicy) String() string {
	switch p {
	case RollbackDirty:
		return "dirty"
	case RollbackState:
		return "state"
	default:
		return "none"
	}
}

type HandlerMeta struct {
	Rollback RollbackPolicy
}

// RollbackParticipant can be implemented by an entity, component, or DAO that
// needs custom state rollback beyond the generated DAO snapshot fallback.
type RollbackParticipant interface {
	CaptureRollback(tx *RollbackTx) error
}

type RollbackTx struct {
	policy    RollbackPolicy
	rollbacks []func() error
	commits   []func()
}

func NewRollbackTx(policy RollbackPolicy) *RollbackTx {
	return &RollbackTx{policy: policy}
}

func (tx *RollbackTx) Policy() RollbackPolicy {
	if tx == nil {
		return RollbackNone
	}
	return tx.policy
}

func (tx *RollbackTx) DeferRollback(fn func() error) {
	if tx != nil && fn != nil {
		tx.rollbacks = append(tx.rollbacks, fn)
	}
}

func (tx *RollbackTx) AfterCommit(fn func()) {
	if tx != nil && fn != nil {
		tx.commits = append(tx.commits, fn)
	}
}

func (tx *RollbackTx) Rollback() error {
	if tx == nil {
		return nil
	}
	var errs []error
	for i := len(tx.rollbacks) - 1; i >= 0; i-- {
		if tx.rollbacks[i] == nil {
			continue
		}
		if err := tx.rollbacks[i](); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (tx *RollbackTx) Commit() {
	if tx == nil || len(tx.commits) == 0 {
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

func invokeWithRollback(meta HandlerMeta, es []entity.IThreadSafeEntity, call func() (any, error)) (ret any, err error) {
	if meta.Rollback == RollbackNone {
		return call()
	}
	tx := NewRollbackTx(meta.Rollback)
	if err := tx.CaptureEntities(es); err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				slog.Error("nest rollback after panic failed", "rollback", meta.Rollback.String(), "err", rbErr)
			}
			panic(r)
		}
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", rbErr))
			}
			return
		}
		tx.Commit()
	}()
	return withRollbackTx(tx, call)
}

func (tx *RollbackTx) CaptureEntities(es []entity.IThreadSafeEntity) error {
	if tx == nil || tx.policy == RollbackNone {
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

type snapshotDao interface {
	Marshal() []byte
	Unmarshal([]byte) error
}

func (tx *RollbackTx) captureDao(dao entity.DaoInterface) error {
	var dirty *checkpoint.DirtyTracker
	var dirtySnapshot checkpoint.DirtySnapshot
	if d, ok := dao.(dirtyTrackerDao); ok {
		dirty = d.DirtyTracker()
		dirtySnapshot = dirty.Snapshot()
	}
	if tx.policy == RollbackDirty {
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
	state, ok := dao.(snapshotDao)
	if !ok {
		if dirty != nil {
			tx.DeferRollback(func() error {
				dirty.Restore(dirtySnapshot)
				return nil
			})
		}
		return nil
	}
	raw := append([]byte(nil), state.Marshal()...)
	tx.DeferRollback(func() error {
		if err := state.Unmarshal(raw); err != nil {
			return err
		}
		if dirty != nil {
			dirty.Restore(dirtySnapshot)
		}
		return nil
	})
	return nil
}
