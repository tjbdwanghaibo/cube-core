package redis

import "errors"

var (
	ErrNil          = errors.New("redis: nil") // key does not exist
	ErrClosed       = errors.New("redis: client closed")
	ErrLockNotHeld  = errors.New("redis: lock not held")
	ErrLockConflict = errors.New("redis: lock conflict") // lock held by another owner
)
