package etcd

// IWatcher watches key/prefix changes.
type IWatcher interface {
	// EventChan returns a channel for receiving watch events.
	EventChan() <-chan *WatchEvent

	// Close stops watching.
	Close() error
}

// WatchEvent represents a key change event.
type WatchEvent struct {
	Type   EventType
	KV     *KV
	PrevKV *KV // previous value (if WithPrevKV enabled)
}

// EventType indicates the type of watch event.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
)

// WatchOption configures watch behavior.
type WatchOption struct {
	WithPrevKV   bool  // include previous value in events
	WithRevision int64 // start watching from revision (0 = current)
}
