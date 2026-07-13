package mongo

import "errors"

var (
	ErrNotFound        = errors.New("mongo: document not found")
	ErrDuplicateKey    = errors.New("mongo: duplicate key")
	ErrVersionConflict = errors.New("mongo: version conflict")
	ErrClosed          = errors.New("mongo: client closed")
)
