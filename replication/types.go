package replication

import (
	"errors"
	"fmt"
)

const (
	ProtocolVersion       uint16 = 1
	DefaultSnapshotRateHz        = 20
	DefaultMaxDatagram           = 1200
)

var (
	ErrInvalidObjectRef   = errors.New("replication: invalid object ref")
	ErrObjectNotFound     = errors.New("replication: object not found")
	ErrObjectLimit        = errors.New("replication: object limit exceeded")
	ErrComponentLimit     = errors.New("replication: component limit exceeded")
	ErrComponentTooLarge  = errors.New("replication: component payload too large")
	ErrFrameTooLarge      = errors.New("replication: frame payload too large")
	ErrInvalidFrame       = errors.New("replication: invalid frame")
	ErrBaselineMismatch   = errors.New("replication: baseline mismatch")
	ErrSnapshotNotFound   = errors.New("replication: snapshot not found")
	ErrSessionNotFound    = errors.New("replication: session not found")
	ErrInvalidAck         = errors.New("replication: invalid acknowledgement")
	ErrInvalidControl     = errors.New("replication: invalid control message")
	ErrPreparedFrameStale = errors.New("replication: prepared frame is stale")
	ErrInvalidDatagram    = errors.New("replication: invalid datagram")
	ErrChecksumMismatch   = errors.New("replication: checksum mismatch")
	ErrFragmentLimit      = errors.New("replication: fragment limit exceeded")
	ErrReassemblyCapacity = errors.New("replication: reassembly capacity exceeded")
	ErrTransportMissing   = errors.New("replication: transport is not configured")
	ErrReplicatorClosed   = errors.New("replication: replicator is closed")
	ErrSchemaFrozen       = errors.New("replication: schema configuration is frozen")
	ErrInvalidLOD         = errors.New("replication: invalid level of detail configuration")
)

type Limits struct {
	MaxObjects             int
	MaxComponentsPerObject int
	MaxComponentBytes      int
	MaxFrameBytes          int
	MaxDatagramBytes       int
	MaxFragments           int
	MaxInflightFrames      int
}

func DefaultLimits() Limits {
	return Limits{
		MaxObjects:             100,
		MaxComponentsPerObject: 64,
		MaxComponentBytes:      64 << 10,
		MaxFrameBytes:          4 << 20,
		MaxDatagramBytes:       DefaultMaxDatagram,
		MaxFragments:           64,
		MaxInflightFrames:      32,
	}
}

func normalizeLimits(limits Limits) Limits {
	defaults := DefaultLimits()
	if limits.MaxObjects <= 0 {
		limits.MaxObjects = defaults.MaxObjects
	}
	if limits.MaxComponentsPerObject <= 0 {
		limits.MaxComponentsPerObject = defaults.MaxComponentsPerObject
	}
	if limits.MaxComponentBytes <= 0 {
		limits.MaxComponentBytes = defaults.MaxComponentBytes
	}
	if limits.MaxFrameBytes <= 0 {
		limits.MaxFrameBytes = defaults.MaxFrameBytes
	}
	if limits.MaxDatagramBytes <= 0 {
		limits.MaxDatagramBytes = defaults.MaxDatagramBytes
	}
	if limits.MaxFragments <= 0 {
		limits.MaxFragments = defaults.MaxFragments
	}
	if limits.MaxInflightFrames <= 0 {
		limits.MaxInflightFrames = defaults.MaxInflightFrames
	}
	return limits
}

type Lane uint8

const (
	LaneState Lane = iota + 1
	LaneLifecycle
	LaneEvent
	LaneStatic
)

type Reliability uint8

const (
	ReliabilityUnreliableLatest Reliability = iota + 1
	ReliabilityUnreliableEvent
	ReliabilityReliableOrdered
)

type Priority uint8

const (
	PriorityCosmetic Priority = iota + 1
	PriorityLow
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

type Visibility uint8

const (
	VisibilityPublic Visibility = iota + 1
	VisibilityOwner
	VisibilityTeam
	VisibilitySpectator
	VisibilityCustom
)

type CodecType uint8

const (
	CodecRaw CodecType = iota + 1
	CodecGeneratedBitset
	CodecGeneratedDelta
)

type ReplicationPolicy struct {
	Lane        Lane
	Reliability Reliability
	Priority    Priority
	MaxRateHz   uint8
	Visibility  Visibility
	Codec       CodecType
}

func (p ReplicationPolicy) validate() error {
	if p.Lane < LaneState || p.Lane > LaneStatic {
		return fmt.Errorf("%w: invalid lane %d", ErrInvalidFrame, p.Lane)
	}
	if p.Reliability < ReliabilityUnreliableLatest || p.Reliability > ReliabilityReliableOrdered {
		return fmt.Errorf("%w: invalid reliability %d", ErrInvalidFrame, p.Reliability)
	}
	if p.Priority < PriorityCosmetic || p.Priority > PriorityCritical {
		return fmt.Errorf("%w: invalid priority %d", ErrInvalidFrame, p.Priority)
	}
	if p.Visibility < VisibilityPublic || p.Visibility > VisibilityCustom {
		return fmt.Errorf("%w: invalid visibility %d", ErrInvalidFrame, p.Visibility)
	}
	if p.Codec < CodecRaw || p.Codec > CodecGeneratedDelta {
		return fmt.Errorf("%w: invalid codec %d", ErrInvalidFrame, p.Codec)
	}
	return nil
}

type ObjectRef struct {
	ID         uint16
	Generation uint16
}

func (r ObjectRef) Valid() bool { return r.ID != 0 && r.Generation != 0 }

func (r ObjectRef) Less(other ObjectRef) bool {
	if r.ID != other.ID {
		return r.ID < other.ID
	}
	return r.Generation < other.Generation
}

type SnapshotMeta struct {
	RoomID        uint64
	Epoch         uint32
	Tick          uint32
	SchemaVersion uint16
}

func (m SnapshotMeta) validate() error {
	if m.RoomID == 0 || m.Epoch == 0 || m.Tick == 0 || m.SchemaVersion == 0 {
		return fmt.Errorf("%w: invalid snapshot metadata", ErrInvalidFrame)
	}
	return nil
}

type ComponentState struct {
	TypeID        uint16
	SchemaVersion uint16
	Data          []byte
}

type ObjectState struct {
	Ref        ObjectRef
	Archetype  uint16
	Components []ComponentState
}

type Snapshot struct {
	SnapshotMeta
	Objects []ObjectState
}

type FrameKind uint8

const (
	FrameFull FrameKind = iota + 1
	FrameDelta
)

type ObjectOperation uint8

const (
	ObjectCreate ObjectOperation = iota + 1
	ObjectUpdate
	ObjectRemove
)

type ComponentOperation uint8

const (
	ComponentSet ComponentOperation = iota + 1
	ComponentRemove
)

type ComponentDelta struct {
	Operation     ComponentOperation
	TypeID        uint16
	SchemaVersion uint16
	Data          []byte
}

type ObjectDelta struct {
	Operation  ObjectOperation
	Ref        ObjectRef
	Archetype  uint16
	Components []ComponentDelta
}

type DeltaFrame struct {
	SnapshotMeta
	Kind     FrameKind
	BaseTick uint32
	Objects  []ObjectDelta
}

type SessionID uint64

type SessionInfo struct {
	ID      SessionID
	OwnerID int64
	TeamID  int64
	Roles   uint64
}
