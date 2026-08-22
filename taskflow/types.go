// Package taskflow defines the stable action and mission contracts used by
// game runtimes. Implementations must be externally serialized by the owning
// entity; taskflow deliberately does not add a second lock domain.
package taskflow

type ActionKind uint8
type ActionGroup uint8
type MissionKind uint16

type ActionStatus uint8

const (
	ActionStatusIdle ActionStatus = iota
	ActionStatusRunning
	ActionStatusSuccess
	ActionStatusFailed
	ActionStatusCanceled
	ActionStatusExpired
)

type MissionStatus uint8

const (
	MissionStatusIdle MissionStatus = iota
	MissionStatusRunning
	MissionStatusSuccess
	MissionStatusFailed
	MissionStatusCanceled
	MissionStatusExpired
)

type ActionResult struct {
	Status ActionStatus
	Reason string
}

func (r ActionResult) Terminal() bool {
	switch r.Status {
	case ActionStatusSuccess, ActionStatusFailed, ActionStatusCanceled, ActionStatusExpired:
		return true
	default:
		return false
	}
}

type ActionReason struct {
	Message string
	Err     error
	Result  ActionResult
}

func NewActionReason(message string) ActionReason { return ActionReason{Message: message} }

func NewActionErrorReason(err error) ActionReason {
	if err == nil {
		return ActionReason{}
	}
	return ActionReason{Message: err.Error(), Err: err, Result: ActionResult{Status: ActionStatusFailed, Reason: err.Error()}}
}

func NewActionResultReason(result ActionResult) ActionReason {
	if result.Reason == "" {
		result.Reason = "action done"
	}
	return ActionReason{Message: result.Reason, Result: result}
}

func (r ActionReason) ToActionResult(fallback ActionStatus) ActionResult {
	if r.Result.Status != ActionStatusIdle {
		if r.Result.Reason == "" {
			r.Result.Reason = r.Message
		}
		return r.Result
	}
	reason := r.Message
	if reason == "" && r.Err != nil {
		reason = r.Err.Error()
	}
	return ActionResult{Status: fallback, Reason: reason}
}

type MissionNextMode uint8

const (
	MissionNextUnset MissionNextMode = iota
	MissionNextStep
	MissionNextSuccessEnd
	MissionNextFailedEnd
)

type MissionNext struct {
	Mode MissionNextMode
	Step int
}

func NextMissionStep(step int) MissionNext { return MissionNext{Mode: MissionNextStep, Step: step} }
func MissionSuccessEnd() MissionNext       { return MissionNext{Mode: MissionNextSuccessEnd} }
func MissionFailedEnd() MissionNext        { return MissionNext{Mode: MissionNextFailedEnd} }

type MissionStep struct {
	Action    ActionKind
	Param     any
	OnSuccess MissionNext
	OnFail    MissionNext
}

type MissionPlan struct {
	Steps []MissionStep
	Start int
	Param any
}

type MissionInfo struct {
	Status        MissionStatus
	CurrentStep   int
	CurrentAction ActionKind
	LastResult    ActionResult
}
