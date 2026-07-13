package nats

import "errors"

var (
	ErrTimeout      = errors.New("nats: request timeout")
	ErrNoResponders = errors.New("nats: no responders")
	ErrClosed       = errors.New("nats: connection closed")
	ErrCancelled    = errors.New("nats: request cancelled")
)
