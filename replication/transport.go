package replication

import "context"

type Transport interface {
	SendDatagram(context.Context, SessionID, []byte) error
	SendReliable(context.Context, SessionID, []byte) error
}

// DatagramBatchTransport admits all fragments of one frame as a unit. A
// transport with a latest-only queue should implement this interface so it
// never replaces or drops an individual fragment from a frame.
type DatagramBatchTransport interface {
	SendDatagramBatch(context.Context, SessionID, [][]byte) error
}

// SessionTransport is an optional lifecycle extension. Replicator invokes it
// automatically, keeping queue/session bookkeeping below the application layer.
type SessionTransport interface {
	RegisterSession(SessionInfo) error
	RemoveSession(SessionID) bool
}

type TransportFunc struct {
	Datagram func(context.Context, SessionID, []byte) error
	Reliable func(context.Context, SessionID, []byte) error
}

func (f TransportFunc) SendDatagram(ctx context.Context, session SessionID, payload []byte) error {
	if f.Datagram == nil {
		return ErrTransportMissing
	}
	return f.Datagram(ctx, session, payload)
}

func (f TransportFunc) SendReliable(ctx context.Context, session SessionID, payload []byte) error {
	if f.Reliable == nil {
		return ErrTransportMissing
	}
	return f.Reliable(ctx, session, payload)
}

type Projector interface {
	Project(SessionInfo, Snapshot) (Snapshot, error)
}

type ProjectorFunc func(SessionInfo, Snapshot) (Snapshot, error)

func (f ProjectorFunc) Project(session SessionInfo, snapshot Snapshot) (Snapshot, error) {
	if f == nil {
		return snapshot.Clone(), nil
	}
	return f(session, snapshot)
}
