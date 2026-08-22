package nest

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

// DurabilityPolicy is independent from rollback policy. Rollback controls
// failures before the commit point; durability controls when the commit point
// is acknowledged.
type DurabilityPolicy uint8

const (
	DurabilityMemory DurabilityPolicy = iota
	DurabilityAsync
	DurabilityStrict
)

func (p DurabilityPolicy) String() string {
	switch p {
	case DurabilityAsync:
		return "async"
	case DurabilityStrict:
		return "strict"
	default:
		return "memory"
	}
}

func ParseDurabilityPolicy(value string) (DurabilityPolicy, error) {
	switch value {
	case "", "memory":
		return DurabilityMemory, nil
	case "async":
		return DurabilityAsync, nil
	case "strict":
		return DurabilityStrict, nil
	default:
		return DurabilityMemory, fmt.Errorf("nest: unsupported durability policy %q", value)
	}
}

// TransactionID is sortable by its process-random prefix and local sequence.
// It avoids a crypto/rand call on every hot-path transaction.
type TransactionID [16]byte

func (id TransactionID) IsZero() bool { return id == TransactionID{} }

func (id TransactionID) String() string { return fmt.Sprintf("%x", id[:]) }

var transactionIDState struct {
	prefix [8]byte
	seq    atomic.Uint64
}

func init() {
	if _, err := rand.Read(transactionIDState.prefix[:]); err != nil {
		binary.BigEndian.PutUint64(transactionIDState.prefix[:], uint64(time.Now().UnixNano()))
	}
}

func newTransactionID() TransactionID {
	var id TransactionID
	copy(id[:8], transactionIDState.prefix[:])
	binary.BigEndian.PutUint64(id[8:], transactionIDState.seq.Add(1))
	return id
}

// EntityMutation is an immutable after-image or generated delta. Codec and
// Schema let replay consumers evolve independently from the WAL format.
type EntityMutation struct {
	EntityID      int64
	Database      string
	DatabaseScope uint8
	Resource      string
	Version       uint64
	Mask          uint64
	Schema        uint32
	Codec         string
	Data          []byte
	// Remote is present only for Remote Entity. It carries the complete
	// immutable, lease-aware commit required by WAL replay.
	Remote *entity.RemoteCommit
}

// Effect is a transactional outbox item. ID must be stable across replay and
// consumers must apply it idempotently.
type Effect struct {
	ID      string
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// CommitRecord is the durable transaction unit. A multi-entity command emits
// one record so replay never loses the relationship between its mutations and
// external effects.
type CommitRecord struct {
	ID         TransactionID
	Handler    string
	RequestID  string
	CreatedAt  int64
	Durability DurabilityPolicy
	Mutations  []EntityMutation
	Effects    []Effect
}

func (r CommitRecord) Empty() bool { return len(r.Mutations) == 0 && len(r.Effects) == 0 }

// CommitFence identifies one WAL record for acknowledgement.
type CommitFence struct {
	TransactionID TransactionID
	Segment       uint64
	Offset        int64
}

// CommitWAL is implemented by infrastructure packages. Append is the commit
// point. Replay must return unacknowledged records in original append order.
type CommitWAL interface {
	Append(ctx context.Context, record CommitRecord) (CommitFence, error)
	Ack(ctx context.Context, fence CommitFence) error
	Replay(ctx context.Context, consume func(CommitFence, CommitRecord) error) error
}

// TransactionCommitter performs durable admission and arranges replay/outbox
// delivery. It must never report failure after it has durably accepted a
// record; post-commit delivery failures are retried out of band.
type TransactionCommitter interface {
	Commit(ctx context.Context, record CommitRecord) error
}

// TransactionReleaseNotifier is an optional committer capability. Nest calls
// it after all entity locks have been released, allowing replay workers to
// avoid racing the normal checkpoint path in the committing process.
type TransactionReleaseNotifier interface {
	TransactionReleased(TransactionID)
}

// CommitParticipant materializes a final after-image/delta exactly once at
// the end of a successful handler. Implementations must not clear dirty or
// patch metadata while preparing the record.
type CommitParticipant interface {
	PrepareCommit(tx *RollbackTx) error
}

func cloneMutation(m EntityMutation) EntityMutation {
	m.Data = append([]byte(nil), m.Data...)
	if m.Remote != nil {
		cloned := m.Remote.Clone()
		m.Remote = &cloned
	}
	return m
}

func cloneEffect(effect Effect) Effect {
	effect.Payload = append([]byte(nil), effect.Payload...)
	if effect.Headers != nil {
		headers := make(map[string]string, len(effect.Headers))
		for key, value := range effect.Headers {
			headers[key] = value
		}
		effect.Headers = headers
	}
	return effect
}

func validateCommitRecord(record CommitRecord) error {
	if record.ID.IsZero() {
		return errors.New("nest: zero transaction id")
	}
	for i, mutation := range record.Mutations {
		if mutation.EntityID == 0 || mutation.Resource == "" || (len(mutation.Data) == 0 && mutation.Remote == nil) {
			return fmt.Errorf("nest: invalid mutation %d", i)
		}
		if mutation.Remote != nil {
			if err := mutation.Remote.Validate(); err != nil {
				return fmt.Errorf("nest: invalid remote mutation %d: %w", i, err)
			}
		}
	}
	for i, effect := range record.Effects {
		if effect.ID == "" || effect.Topic == "" {
			return fmt.Errorf("nest: invalid effect %d", i)
		}
	}
	return nil
}
