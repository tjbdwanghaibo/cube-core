// Package ai defines entity-aware AI strategy contracts. Scheduling and
// concrete decision algorithms are supplied by adapters and cube-kit.
package ai

import (
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/taskflow"
)

type Context struct {
	Owner      entity.IThreadSafeEntity
	ActionList taskflow.ActionList
	Now        time.Time
}

type Strategy interface {
	Name() string
	Init(ctx *Context) error
	Tick(ctx *Context, now time.Time)
	OnActionEnd(ctx *Context, actionID int64, kind taskflow.ActionKind, reason taskflow.ActionReason)
	OnMissionEnd(ctx *Context, mission taskflow.Mission, reason taskflow.ActionReason)
	CanStopByNext(next Strategy) bool
}

type StoppableStrategy interface {
	Stop(ctx *Context, reason string)
}
