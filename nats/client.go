package nats

import "time"

// IClient is the abstraction for NATS messaging operations.
type IClient interface {
	// Publish sends data to the given subject asynchronously.
	Publish(subject string, data []byte) error

	// Request sends a request and waits synchronously for a response.
	Request(subject string, data []byte, timeout time.Duration) ([]byte, error)

	// Subscribe subscribes to a subject with the given handler.
	Subscribe(subject string, handler MsgHandler) (ISubscription, error)

	// QueueSubscribe subscribes with a queue group (load-balanced across group members).
	QueueSubscribe(subject string, queue string, handler MsgHandler) (ISubscription, error)

	// Drain gracefully unsubscribes and drains pending messages.
	Drain() error

	// Close force-closes the connection.
	Close()
}

// MsgHandler is the callback for incoming messages.
type MsgHandler func(msg *Msg)

// Msg represents an incoming NATS message.
type Msg struct {
	Subject string
	Reply   string // non-empty for request/reply pattern
	Data    []byte
}

// ISubscription is a handle to a NATS subscription.
type ISubscription interface {
	Unsubscribe() error
	IsValid() bool
}
