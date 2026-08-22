package entity

import (
	"errors"
	"fmt"
	"hash/crc64"
	"sync"
)

var (
	ErrRemoteSnapshotDecoderMissing   = errors.New("remote snapshot: decoder missing")
	ErrRemoteSnapshotDecoderDuplicate = errors.New("remote snapshot: decoder already registered")
)

type RemoteSnapshotDecodeFunc func([]byte) (any, error)
type RemoteSnapshotDeltaFunc func(base []byte, delta []byte) ([]byte, error)

var remoteSnapshotDecoders struct {
	sync.RWMutex
	values map[uint32]RemoteSnapshotDecodeFunc
	deltas map[uint32]RemoteSnapshotDeltaFunc
}

func RegisterRemoteSnapshotDelta(schema uint32, apply RemoteSnapshotDeltaFunc) error {
	if schema == 0 || apply == nil {
		return fmt.Errorf("remote snapshot: invalid delta registration")
	}
	remoteSnapshotDecoders.Lock()
	defer remoteSnapshotDecoders.Unlock()
	if remoteSnapshotDecoders.deltas == nil {
		remoteSnapshotDecoders.deltas = make(map[uint32]RemoteSnapshotDeltaFunc)
	}
	if _, exists := remoteSnapshotDecoders.deltas[schema]; exists {
		return fmt.Errorf("%w: delta schema=%d", ErrRemoteSnapshotDecoderDuplicate, schema)
	}
	remoteSnapshotDecoders.deltas[schema] = apply
	return nil
}

func applyRemoteSnapshotDelta(schema uint32, base, delta []byte) ([]byte, error) {
	remoteSnapshotDecoders.RLock()
	apply := remoteSnapshotDecoders.deltas[schema]
	remoteSnapshotDecoders.RUnlock()
	if apply == nil {
		return nil, fmt.Errorf("%w: delta schema=%d", ErrRemoteSnapshotDecoderMissing, schema)
	}
	return apply(append([]byte(nil), base...), append([]byte(nil), delta...))
}

var remoteSnapshotChecksumTable = crc64.MakeTable(crc64.ECMA)

func RemoteSnapshotChecksum(data []byte) uint64 {
	return crc64.Checksum(data, remoteSnapshotChecksumTable)
}

func RegisterRemoteSnapshotDecoder(schema uint32, decode RemoteSnapshotDecodeFunc) error {
	if schema == 0 || decode == nil {
		return fmt.Errorf("remote snapshot: invalid decoder registration")
	}
	remoteSnapshotDecoders.Lock()
	defer remoteSnapshotDecoders.Unlock()
	if remoteSnapshotDecoders.values == nil {
		remoteSnapshotDecoders.values = make(map[uint32]RemoteSnapshotDecodeFunc)
	}
	if _, exists := remoteSnapshotDecoders.values[schema]; exists {
		return fmt.Errorf("%w: schema=%d", ErrRemoteSnapshotDecoderDuplicate, schema)
	}
	remoteSnapshotDecoders.values[schema] = decode
	return nil
}

func MustRegisterRemoteSnapshotDecoder(schema uint32, decode RemoteSnapshotDecodeFunc) {
	if err := RegisterRemoteSnapshotDecoder(schema, decode); err != nil {
		panic(err)
	}
}

func DecodeRemoteSnapshot(snapshot RemoteSnapshotEnvelope) (any, error) {
	remoteSnapshotDecoders.RLock()
	decode := remoteSnapshotDecoders.values[snapshot.Schema]
	remoteSnapshotDecoders.RUnlock()
	if decode == nil {
		return nil, fmt.Errorf("%w: schema=%d", ErrRemoteSnapshotDecoderMissing, snapshot.Schema)
	}
	return decode(snapshot.Payload.BytesCopy())
}
