package etcd

import "context"

// IElection provides leader election for exclusive services.
type IElection interface {
	// Campaign starts a campaign to become leader.
	// Blocks until elected or context cancelled.
	Campaign(ctx context.Context, value string) error

	// Resign gives up leadership.
	Resign(ctx context.Context) error

	// Leader returns the current leader value.
	Leader(ctx context.Context) (string, error)

	// IsLeader returns true if this instance is the current leader.
	IsLeader() bool

	// LeaderChan returns a channel that is closed when this instance loses leadership.
	LeaderChan() <-chan struct{}
}

// IElectionFactory creates election instances for different election keys.
type IElectionFactory interface {
	// NewElection creates an election for the given prefix.
	// e.g. "/election/center" — all center candidates compete under this prefix.
	NewElection(prefix string) IElection
}
