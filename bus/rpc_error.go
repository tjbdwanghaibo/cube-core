package bus

import "github.com/tjbdwanghaibo/cube-core/errcode"

type rpcErrorEnvelope struct {
	Code   int32  `json:"code"`
	Reason string `json:"reason"`
}

func rpcErrorResponse(err error) rpcErrorEnvelope {
	code, reason := errcode.ClientError(err)
	return rpcErrorEnvelope{Code: code, Reason: reason}
}
