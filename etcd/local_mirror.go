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
