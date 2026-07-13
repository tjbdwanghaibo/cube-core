package bus

import (
	"errors"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/errcode"
)

func TestRPCErrorResponseUsesCodeReasonEnvelope(t *testing.T) {
	def := errcode.Define(598877, "bus.rpc_test", "rpc test failed")
	resp := rpcErrorResponse(errcode.Wrap(def, errors.New("storage down")))

	if resp.Code != 598877 || resp.Reason != "rpc test failed" {
		t.Fatalf("resp = %+v, want code/reason envelope", resp)
	}
}
