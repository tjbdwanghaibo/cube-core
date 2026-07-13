package mongo

import "context"

// IDatabase represents a MongoDB database.
type IDatabase interface {
	// Name returns the database name.
	Name() string

	// Collection returns a collection handle.
	Collection(name string) ICollection

	// Drop drops the entire database.
	Drop(ctx context.Context) error
}
