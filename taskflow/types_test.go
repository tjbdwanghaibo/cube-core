package taskflow

import (
	"errors"
	"testing"
)

func TestActionReasonPreservesStructuredResult(t *testing.T) {
	reason := NewActionResultReason(ActionResult{Status: ActionStatusCanceled, Reason: "shutdown"})
	if got := reason.ToActionResult(ActionStatusFailed); got.Status != ActionStatusCanceled || got.Reason != "shutdown" {
		t.Fatalf("result = %+v", got)
	}
	err := errors.New("boom")
	if got := NewActionErrorReason(err); !errors.Is(got.Err, err) || got.Result.Status != ActionStatusFailed {
		t.Fatalf("error reason = %+v", got)
	}
}
