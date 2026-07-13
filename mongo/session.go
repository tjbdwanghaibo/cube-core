package mongo

import "context"

// ISession provides multi-document transaction support.
type ISession interface {
	// WithTransaction executes fn within a transaction with automatic retry.
	// If fn returns nil, the transaction commits; otherwise it aborts.
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error

	// EndSession releases the session resources.
	EndSession(ctx context.Context)
}
