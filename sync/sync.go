package sync

// SyncMsg is the wire format for a sync message.
type SyncMsg struct {
	Topic   string // sync topic (e.g. "remote_entity", "config")
	Key     int64  // business key (entity ID, config ID, etc.)
	Version int64  // data version, subscriber uses to discard stale messages
	Data    []byte // serialized business data (nil means delete)
	FromSid int32  // sender server ID
}

// Handler processes an incoming sync message.
// Return error to log warning (message is NOT retried).
type Handler func(msg *SyncMsg) error

// IPublisher publishes sync messages to subscribers.
type IPublisher interface {
	Publish(msg *SyncMsg) error
}

// ISubscriber subscribes to sync topics.
type ISubscriber interface {
	// Subscribe registers a handler for a topic.
	// Returns unsubscribe function.
	Subscribe(topic string, handler Handler) (unsub func(), err error)
}

// ISyncBus combines publish and subscribe capabilities.
type ISyncBus interface {
	IPublisher
	ISubscriber
}
