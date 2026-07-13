package redis

import (
	"context"
	"time"
)

// IDistLock is a Redis-based distributed lock.
type IDistLock interface {
	// Acquire attempts to acquire the lock. Returns true if acquired.
	Acquire(ctx context.Context) (bool, error)

	// Release releases the lock. Returns ErrLockNotHeld if not held.
	Release(ctx context.Context) error

	// Extend extends the lock TTL (for long-running operations).
	Extend(ctx context.Context, ttl time.Duration) (bool, error)
}

// IDistLockFactory creates distributed locks by key.
type IDistLockFactory interface {
	// NewLock creates a distributed lock for the given key with the specified TTL.
	NewLock(key string, ttl time.Duration) IDistLock
}
