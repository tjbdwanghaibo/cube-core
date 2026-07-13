package mongo

import "context"

// IMongo is the top-level MongoDB client abstraction.
type IMongo interface {
	// Database returns a database by name.
	Database(name string) IDatabase

	// DatabaseForSid returns a database named "{prefix}_{sid}".
	DatabaseForSid(prefix string, sid int32) IDatabase

	// StartSession starts a session for multi-document transactions.
	StartSession(ctx context.Context) (ISession, error)

	// Ping verifies connectivity.
	Ping(ctx context.Context) error

	// Close disconnects the client.
	Close(ctx context.Context) error
}
