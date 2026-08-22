package replication

import (
	"encoding/binary"
	"fmt"
)

const (
	controlMagic       uint32 = 0x43524331 // CRC1
	ControlMessageSize        = 32
)

type ControlType uint8

const (
	ControlAck ControlType = iota + 1
	ControlResync
)

type ResyncReason uint8

const (
	ResyncUnknown ResyncReason = iota
	ResyncBaselineMissing
	ResyncReassemblyExpired
	ResyncDecodeFailure
	ResyncSchemaMismatch
)

// ControlMessage is the authenticated client-to-server control plane for the
// snapshot stream. Sequence is monotonic per session and prevents replayed or
// reordered ACK/resync packets from moving session state backwards.
type ControlMessage struct {
	Type     ControlType
	Reason   ResyncReason
	RoomID   uint64
	Epoch    uint32
	Tick     uint32
	Sequence uint32
}

func IsControlPayload(raw []byte) bool {
	return len(raw) == ControlMessageSize && binary.BigEndian.Uint32(raw[0:4]) == controlMagic
}

func EncodeControl(message ControlMessage) ([]byte, error) {
	if err := message.validate(); err != nil {
		return nil, err
	}
	raw := make([]byte, ControlMessageSize)
	binary.BigEndian.PutUint32(raw[0:4], controlMagic)
	binary.BigEndian.PutUint16(raw[4:6], ProtocolVersion)
	raw[6] = byte(message.Type)
	raw[7] = byte(message.Reason)
	binary.BigEndian.PutUint64(raw[8:16], message.RoomID)
	binary.BigEndian.PutUint32(raw[16:20], message.Epoch)
	binary.BigEndian.PutUint32(raw[20:24], message.Tick)
	binary.BigEndian.PutUint32(raw[24:28], message.Sequence)
	binary.BigEndian.PutUint32(raw[28:32], checksumControl(raw))
	return raw, nil
}

func DecodeControl(raw []byte) (ControlMessage, error) {
	if len(raw) != ControlMessageSize || binary.BigEndian.Uint32(raw[0:4]) != controlMagic || binary.BigEndian.Uint16(raw[4:6]) != ProtocolVersion {
		return ControlMessage{}, ErrInvalidControl
	}
	if checksumControl(raw) != binary.BigEndian.Uint32(raw[28:32]) {
		return ControlMessage{}, ErrChecksumMismatch
	}
	message := ControlMessage{
		Type: ControlType(raw[6]), Reason: ResyncReason(raw[7]),
		RoomID: binary.BigEndian.Uint64(raw[8:16]), Epoch: binary.BigEndian.Uint32(raw[16:20]),
		Tick: binary.BigEndian.Uint32(raw[20:24]), Sequence: binary.BigEndian.Uint32(raw[24:28]),
	}
	if err := message.validate(); err != nil {
		return ControlMessage{}, err
	}
	return message, nil
}

func (message ControlMessage) validate() error {
	if message.Type != ControlAck && message.Type != ControlResync || message.RoomID == 0 || message.Epoch == 0 || message.Sequence == 0 {
		return ErrInvalidControl
	}
	if message.Type == ControlAck && (message.Tick == 0 || message.Reason != ResyncUnknown) {
		return ErrInvalidControl
	}
	if message.Type == ControlResync && message.Reason > ResyncSchemaMismatch {
		return ErrInvalidControl
	}
	return nil
}

func checksumControl(raw []byte) uint32 {
	if len(raw) != ControlMessageSize {
		return 0
	}
	// FNV-1a is sufficient here: transport authentication supplies security;
	// this checksum catches accidental corruption and framing mistakes.
	const offset32, prime32 = uint32(2166136261), uint32(16777619)
	hash := offset32
	for index, value := range raw {
		if index >= 28 {
			value = 0
		}
		hash ^= uint32(value)
		hash *= prime32
	}
	return hash
}

func (r *Replicator) HandleControl(id SessionID, raw []byte) error {
	if !r.begin() {
		return ErrReplicatorClosed
	}
	defer r.active.Done()
	message, err := DecodeControl(raw)
	if err != nil {
		r.stats.invalidControls.Add(1)
		return err
	}
	state, err := r.session(id)
	if err != nil {
		return err
	}
	latest, ok := r.ring.Latest()
	if !ok {
		return ErrSnapshotNotFound
	}
	if message.RoomID != latest.RoomID || message.Epoch != latest.Epoch {
		r.stats.invalidControls.Add(1)
		return fmt.Errorf("%w: control room=%d/%d current=%d/%d", ErrInvalidControl, message.RoomID, message.Epoch, latest.RoomID, latest.Epoch)
	}
	if err := state.handleControl(message, latest.Tick); err != nil {
		r.stats.invalidControls.Add(1)
		if message.Type == ControlAck {
			r.stats.invalidAcks.Add(1)
		}
		return err
	}
	if message.Type == ControlResync {
		r.stats.forcedFull.Add(1)
		r.stats.resyncRequests.Add(1)
	}
	return nil
}
