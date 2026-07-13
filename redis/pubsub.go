package redis

// IPubSub represents a Redis Pub/Sub subscription.
type IPubSub interface {
	// Channel returns a Go channel for receiving messages.
	Channel() <-chan *PubSubMessage

	// Close unsubscribes and closes the subscription.
	Close() error
}

// PubSubMessage is a message received from a Redis Pub/Sub channel.
type PubSubMessage struct {
	Channel string
	Payload string
}
