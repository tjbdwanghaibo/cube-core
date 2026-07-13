package nats

import (
	"context"
	"time"
)

// IRpc provides RPC capabilities over NATS.
type IRpc interface {
	// Call performs a synchronous RPC: sends request, blocks until response or context done.
	Call(ctx context.Context, subject string, req []byte) (resp []byte, err error)

	// CallWithTimeout performs a synchronous RPC with a simple timeout.
	CallWithTimeout(subject string, req []byte, timeout time.Duration) (resp []byte, err error)

	// CallAsync performs an asynchronous RPC: sends request, invokes callback when response arrives.
	// The callback executes in a worker pool, not in the caller's goroutine.
	CallAsync(subject string, req []byte, cb RpcCallback)

	// Reply sends a response to a pending RPC request (callee side).
	Reply(replySubject string, resp []byte) error
}

// RpcCallback is invoked when an async RPC response arrives or times out.
type RpcCallback func(resp []byte, err error)

// RetryPolicy controls transport-level retry behavior for RPC calls.
type RetryPolicy struct {
	MaxAttempts  int           // total attempts including first try, default: 3
	BaseInterval time.Duration // initial retry wait, default: 50ms
	MaxInterval  time.Duration // max backoff cap, default: 1s
	Multiplier   float64       // exponential multiplier, default: 2.0
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  3,
		BaseInterval: 50 * time.Millisecond,
		MaxInterval:  time.Second,
		Multiplier:   2.0,
	}
}
