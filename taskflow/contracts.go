package taskflow

import (
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

// ActionContext is callback-scoped. Implementations must not retain its pointer
// after Start, Tick or Cancel returns; runners may reuse the storage.
type ActionContext struct {
	Owner          entity.IThreadSafeEntity
	ActionList     ActionList
	CurrentMission Mission
	Now            time.Time
}

type Action interface {
	Kind() ActionKind
	Start(ctx *ActionContext) error
	Tick(ctx *ActionContext) (done bool, result ActionResult)
	Cancel(ctx *ActionContext, reason string)
}

type ActionList interface {
	Entity() entity.IThreadSafeEntity
	CreateAction(kind ActionKind, param any) (int64, error)
	EnqueueAction(kind ActionKind, param any) (int64, error)
	CurAction(group ActionGroup) Action
	EndCurAction(group ActionGroup, force bool, reason ActionReason)
	EndAllAction(force bool, reason ActionReason)
	ClearNextActions()
	UpdateAction(group ActionGroup, f func(Action) error) error
	Frozen(group ActionGroup)
	Recover(group ActionGroup)
	GetFrozen(group ActionGroup) bool

	MissionManager() MissionManager
	SetMission(kind MissionKind, param any) error
	CurMission() Mission
	EndCurMission(reason ActionReason)
}

type MissionContext struct {
	Owner      entity.IThreadSafeEntity
	ActionList ActionList
	Manager    MissionManager
	Now        time.Time
}

type Mission interface {
	ID() int64
	Kind() MissionKind
	Status() MissionStatus
	Start(ctx *MissionContext, param any) error
	Tick(ctx *MissionContext, now time.Time)
	OnActionEnd(ctx *MissionContext, actionID int64, kind ActionKind, reason ActionReason)
	End(ctx *MissionContext, reason ActionReason)
	CanReplaceBy(kind MissionKind, param any) bool
	MissionInfo() MissionInfo
}

type MissionManager interface {
	StartMission(kind MissionKind, param any) error
	CancelMission(reason string) error
	InMission() bool
	CurrentAction() ActionKind
	CurMission() Mission
	MissionInfo() MissionInfo
	OnActionEnd(actionID int64, kind ActionKind, reason ActionReason)
	EndCurMission(reason ActionReason)
}

type MissionRuntimeSetter interface {
	SetRuntime(id int64, manager MissionManager)
}
