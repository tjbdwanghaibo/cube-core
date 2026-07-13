package redis

import (
	"context"
	"time"
)

// IVersionedLock is a distributed lock that carries a version number.
// On acquire: atomically reads current version from Redis.
// On release: atomically writes new version back to Redis.
// This enables optimistic version comparison to skip DB loads.
type IVersionedLock interface {
	// TryLock attempts to acquire the lock once.
	// On success, the current stored version is readable via Version().
	TryLock(ctx context.Context) error

	// Lock acquires with retry (exponential backoff).
	Lock(ctx context.Context) error

	// Unlock releases the lock and atomically stores newVersion.
	// versionTTL: how long the version field persists after owner is cleared.
	Unlock(ctx context.Context, newVersion int64, versionTTL time.Duration) error

	// UnlockWithRetry releases with explicit retry parameters.
	UnlockWithRetry(ctx context.Context, newVersion int64, versionTTL time.Duration, retryCount int, retryInterval time.Duration) error

	// Version returns the version read at acquire time, or last written at unlock.
	Version() int64

	// IsAcquired reports whether this lock is currently held.
	IsAcquired() bool

	// Touch extends lock TTL by duration (additive to remaining TTL, capped at 2×TTL).
	Touch(ctx context.Context, duration time.Duration) error

	// Refresh resets lock TTL to original value.
	Refresh(ctx context.Context) error

	// Close releases if held (version unchanged). For cleanup/panic-recovery.
	Close() error
}

// VersionedLockOptions configures a versioned lock.
type VersionedLockOptions struct {
	Key           string        // logical key prefix
	TTL           time.Duration // lock expiration, must > 0
	RetryInterval time.Duration // retry interval, 0 = no retry
	RetryCount    int           // retry count, 0 = no retry

	// AutoAsyncTouch: if true, background goroutine periodically extends TTL after acquire.
	AutoAsyncTouch     bool
	AsyncTouchExtend   time.Duration // TTL extension per touch; 0 = TTL/2
	AsyncTouchInterval time.Duration // touch period; 0 = TTL/3
}

// DefaultVersionedLockOptions returns reasonable defaults.
func DefaultVersionedLockOptions(key string, ttl time.Duration) VersionedLockOptions {
	return VersionedLockOptions{
		Key:           key,
		TTL:           ttl,
		RetryInterval: 100 * time.Millisecond,
		RetryCount:    5,
	}
}

// IVersionedLockFactory creates versioned locks.
type IVersionedLockFactory interface {
	// NewVersionedLock creates a lock for the given resource.
	// key: logical prefix (e.g. "e" for entity).
	// id: unique resource identifier.
	// opts: lock configuration.
	NewVersionedLock(id int64, opts VersionedLockOptions) IVersionedLock
}
