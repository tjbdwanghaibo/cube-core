package etcd

import "context"

// IWatcher watches key/prefix changes.
type IWatcher interface {
	// EventChan returns a channel for receiving watch events.
	EventChan() <-chan *WatchEvent

	// Close stops watching.
	Close() error
}

// IWatcherError is optionally implemented by watchers that can report why
// their event channel closed. In particular, callers can distinguish a normal
// close from an etcd cancellation or a compacted start revision without
// breaking existing IWatcher implementations.
type IWatcherError interface {
	WatchError() error
}

// IWatcherReady is optionally implemented by watchers that can report when
// the server has acknowledged creation of the watch stream.
type IWatcherReady interface {
	Ready() <-chan struct{}
}

// WatchHandler handles events from one watcher in receive order. Handlers are
// called serially and must honor ctx cancellation. Returning an error stops the
// subscription and exposes that error through IWatchSubscription.Err.
type WatchHandler func(ctx context.Context, event *WatchEvent) error

// IWatchSubscription owns a callback consumer and its lifecycle. Err is nil
// for an explicit Close and otherwise reports context cancellation, watcher
// termination, callback failure, callback panic, or subscriber backpressure.
type IWatchSubscription interface {
	Done() <-chan struct{}
	Err() error
	Close() error
	CloseWithContext(ctx context.Context) error
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
	WithPrevKV    bool  // include previous value in events
	WithRevision  int64 // start watching from revision (0 = current)
	CreatedNotify bool  // notify when the server has established the watch
}
