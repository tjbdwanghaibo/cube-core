package etcd

import (
	"context"
	"time"
)

// PrefixSnapshot is a consistent prefix read and the etcd header revision at
// which it was taken. A watcher started at Revision+1 cannot miss writes that
// race with initialization.
type PrefixSnapshot struct {
	KVs      []*KV
	Revision int64
}

// IPrefixSnapshotReader extends IEtcd with a revisioned prefix read. It is kept
// separate from IEtcd so existing third-party implementations remain source
// compatible.
type IPrefixSnapshotReader interface {
	GetPrefixSnapshot(ctx context.Context, prefix string) (*PrefixSnapshot, error)
}

// LocalMirrorConfig configures a typed, process-local mirror of an etcd prefix.
// Decode and Clone must return independent values. Clone is called before a
// value enters the mirror and again before it is returned to a caller, so maps,
// slices, and pointers are never shared between writers and readers. Decode,
// Encode, and Clone may be called concurrently and must be concurrency-safe.
type LocalMirrorConfig[T any] struct {
	Prefix string
	Decode func(key, value string) (T, error)
	Encode func(value T) (string, error)
	Clone  func(value T) (T, error)

	RetryMinInterval time.Duration
	RetryMaxInterval time.Duration
}

// LocalMirrorPublishOptions configures how a value is written to etcd.
// LeaseID=0 creates a persistent key; a non-zero value associates the key with
// an existing etcd lease.
type LocalMirrorPublishOptions struct {
	LeaseID int64
}

// LocalMirrorEntry combines a cloned value with the etcd metadata needed for
// optimistic multi-server writes.
type LocalMirrorEntry[T any] struct {
	Key            string
	Value          T
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Lease          int64
}

// LocalMirrorStatus describes whether the local view is currently backed by
// an active watch. Values may still be returned while Synced is false, but are
// potentially stale and Get/Snapshot return LastError (or ErrMirrorNotSynced).
type LocalMirrorStatus struct {
	Revision  int64
	Synced    bool
	LastError error
}

// LocalMirrorChangeType identifies a mirror callback. Snapshot is emitted
// first for a new subscription (unless disabled) and after every resnapshot,
// so a subscriber can atomically replace derived state after watch recovery.
type LocalMirrorChangeType uint8

const (
	LocalMirrorSnapshot LocalMirrorChangeType = iota
	LocalMirrorPut
	LocalMirrorDelete
)

// LocalMirrorChange is an immutable-in-meaning change notification. Values
// are cloned for each callback invocation, so a handler may mutate Entry,
// Previous, and Snapshot without racing with the mirror or another handler.
//
// Snapshot is populated only for LocalMirrorSnapshot. Entry is populated for
// LocalMirrorPut. Previous is populated when a put replaced an existing value
// and for deletes of an existing value. Key identifies a put/delete even when
// no previous value exists. Revision is the resulting mirror revision.
type LocalMirrorChange[T any] struct {
	Type     LocalMirrorChangeType
	Key      string
	Entry    *LocalMirrorEntry[T]
	Previous *LocalMirrorEntry[T]
	Snapshot map[string]T
	Revision int64
}

// LocalMirrorHandler receives changes for one subscription in revision order.
// The handler never runs while the mirror state lock is held. Returning an
// error terminates only this subscription and is reported by Err.
type LocalMirrorHandler[T any] func(ctx context.Context, change LocalMirrorChange[T]) error

// LocalMirrorSubscribeOptions controls callback delivery. QueueCapacity is a
// per-subscriber bounded queue (64 when zero). A full queue closes only the
// slow subscription with ErrMirrorSubscriberSlow; the etcd watch and other
// subscribers continue. SkipInitialSnapshot disables the first snapshot.
type LocalMirrorSubscribeOptions struct {
	QueueCapacity       int
	SkipInitialSnapshot bool
}

// ILocalMirror is a race-safe typed view of an etcd prefix. Keys are the full
// etcd keys. Get and Snapshot return cloned values that callers may mutate.
type ILocalMirror[T any] interface {
	Get(key string) (T, bool, error)
	GetEntry(key string) (LocalMirrorEntry[T], bool, error)
	Snapshot() (map[string]T, error)
	Revision() int64
	LastError() error
	Status() LocalMirrorStatus

	// WaitForSync waits until a snapshot and its following watch are active.
	// It returns the context error or ErrMirrorClosed if the mirror stops first.
	WaitForSync(ctx context.Context) error

	// Publish and Delete are last-write-wins operations. Local state changes
	// only when the corresponding etcd watch event is observed.
	Publish(ctx context.Context, key string, value T) error
	PublishWithOptions(ctx context.Context, key string, value T, options LocalMirrorPublishOptions) error
	Delete(ctx context.Context, key string) error

	// PublishIfRevision and DeleteIfRevision provide optimistic multi-server
	// concurrency control. expectedRevision=0 means the key must not exist.
	PublishIfRevision(ctx context.Context, key string, expectedRevision int64, value T) (bool, error)
	PublishIfRevisionWithOptions(ctx context.Context, key string, expectedRevision int64, value T, options LocalMirrorPublishOptions) (bool, error)
	DeleteIfRevision(ctx context.Context, key string, expectedRevision int64) (bool, error)

	Done() <-chan struct{}
	Close() error
}

// ILocalMirrorSubscriber extends ILocalMirror without breaking third-party
// implementations of the original interface. Implementations must register a
// subscriber atomically with the initial snapshot so no watch event can be
// missed between snapshot delivery and live callbacks.
type ILocalMirrorSubscriber[T any] interface {
	ILocalMirror[T]
	Subscribe(ctx context.Context, handler LocalMirrorHandler[T], options LocalMirrorSubscribeOptions) (IWatchSubscription, error)
}

// SubscribeLocalMirror subscribes to a mirror without adding methods to the
// already-published ILocalMirror interface. It returns
// ErrMirrorSubscribeUnsupported for third-party mirrors that do not implement
// ILocalMirrorSubscriber.
func SubscribeLocalMirror[T any](mirror ILocalMirror[T], ctx context.Context, handler LocalMirrorHandler[T], options LocalMirrorSubscribeOptions) (IWatchSubscription, error) {
	if isNilInterface(mirror) {
		return nil, ErrMirrorSubscribeUnsupported
	}
	subscriber, ok := mirror.(ILocalMirrorSubscriber[T])
	if !ok {
		return nil, ErrMirrorSubscribeUnsupported
	}
	return subscriber.Subscribe(ctx, handler, options)
}
