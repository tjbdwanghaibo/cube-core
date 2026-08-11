package replication

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"
)

const (
	datagramMagic      uint32 = 0x43524431 // CRD1
	DatagramHeaderSize        = 42
)

type DatagramFlags uint16

const (
	DatagramFlagFull DatagramFlags = 1 << iota
)

type DatagramHeader struct {
	Protocol   uint16
	Flags      DatagramFlags
	RoomID     uint64
	Epoch      uint32
	Tick       uint32
	BaseTick   uint32
	Sequence   uint32
	ChunkIndex uint16
	ChunkCount uint16
	PayloadLen uint16
	Checksum   uint32
}

func FragmentFrame(frame DeltaFrame, sequence uint32, encoded []byte, maxDatagram int, limits Limits) ([][]byte, error) {
	limits = normalizeLimits(limits)
	if sequence == 0 || len(encoded) == 0 || len(encoded) > limits.MaxFrameBytes {
		return nil, ErrInvalidDatagram
	}
	if maxDatagram <= 0 {
		maxDatagram = DefaultMaxDatagram
	}
	if maxDatagram > limits.MaxDatagramBytes {
		return nil, ErrInvalidDatagram
	}
	maxPayload := maxDatagram - DatagramHeaderSize
	if maxPayload <= 0 || maxPayload > int(^uint16(0)) {
		return nil, ErrInvalidDatagram
	}
	count := (len(encoded) + maxPayload - 1) / maxPayload
	if count <= 0 || count > limits.MaxFragments || count > int(^uint16(0)) {
		return nil, ErrFragmentLimit
	}
	flags := DatagramFlags(0)
	if frame.Kind == FrameFull {
		flags |= DatagramFlagFull
	}
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		start := i * maxPayload
		end := minInt(start+maxPayload, len(encoded))
		header := DatagramHeader{
			Protocol: ProtocolVersion, Flags: flags, RoomID: frame.RoomID, Epoch: frame.Epoch,
			Tick: frame.Tick, BaseTick: frame.BaseTick, Sequence: sequence,
			ChunkIndex: uint16(i), ChunkCount: uint16(count), PayloadLen: uint16(end - start),
		}
		packet, err := encodeDatagram(header, encoded[start:end])
		if err != nil {
			return nil, err
		}
		if len(packet) > maxDatagram {
			return nil, ErrInvalidDatagram
		}
		out = append(out, packet)
	}
	return out, nil
}

func encodeDatagram(header DatagramHeader, payload []byte) ([]byte, error) {
	if header.Protocol != ProtocolVersion || header.RoomID == 0 || header.Epoch == 0 || header.Tick == 0 ||
		header.Sequence == 0 || header.ChunkCount == 0 || header.ChunkIndex >= header.ChunkCount || len(payload) != int(header.PayloadLen) {
		return nil, ErrInvalidDatagram
	}
	packet := make([]byte, DatagramHeaderSize+len(payload))
	binary.BigEndian.PutUint32(packet[0:4], datagramMagic)
	binary.BigEndian.PutUint16(packet[4:6], header.Protocol)
	binary.BigEndian.PutUint16(packet[6:8], uint16(header.Flags))
	binary.BigEndian.PutUint64(packet[8:16], header.RoomID)
	binary.BigEndian.PutUint32(packet[16:20], header.Epoch)
	binary.BigEndian.PutUint32(packet[20:24], header.Tick)
	binary.BigEndian.PutUint32(packet[24:28], header.BaseTick)
	binary.BigEndian.PutUint32(packet[28:32], header.Sequence)
	binary.BigEndian.PutUint16(packet[32:34], header.ChunkIndex)
	binary.BigEndian.PutUint16(packet[34:36], header.ChunkCount)
	binary.BigEndian.PutUint16(packet[36:38], header.PayloadLen)
	copy(packet[DatagramHeaderSize:], payload)
	checksum := checksumDatagram(packet)
	binary.BigEndian.PutUint32(packet[38:42], checksum)
	return packet, nil
}

func DecodeDatagram(packet []byte, limits Limits) (DatagramHeader, []byte, error) {
	return decodeDatagram(packet, limits, true)
}

// InspectDatagram validates the complete packet, including checksum, without
// copying its payload. Transport queues use it to validate frame batches on
// the hot path.
func InspectDatagram(packet []byte, limits Limits) (DatagramHeader, error) {
	header, _, err := decodeDatagram(packet, limits, false)
	return header, err
}

func decodeDatagram(packet []byte, limits Limits, copyPayload bool) (DatagramHeader, []byte, error) {
	limits = normalizeLimits(limits)
	if len(packet) < DatagramHeaderSize || len(packet) > limits.MaxDatagramBytes || binary.BigEndian.Uint32(packet[0:4]) != datagramMagic {
		return DatagramHeader{}, nil, ErrInvalidDatagram
	}
	header := DatagramHeader{
		Protocol:   binary.BigEndian.Uint16(packet[4:6]),
		Flags:      DatagramFlags(binary.BigEndian.Uint16(packet[6:8])),
		RoomID:     binary.BigEndian.Uint64(packet[8:16]),
		Epoch:      binary.BigEndian.Uint32(packet[16:20]),
		Tick:       binary.BigEndian.Uint32(packet[20:24]),
		BaseTick:   binary.BigEndian.Uint32(packet[24:28]),
		Sequence:   binary.BigEndian.Uint32(packet[28:32]),
		ChunkIndex: binary.BigEndian.Uint16(packet[32:34]),
		ChunkCount: binary.BigEndian.Uint16(packet[34:36]),
		PayloadLen: binary.BigEndian.Uint16(packet[36:38]),
		Checksum:   binary.BigEndian.Uint32(packet[38:42]),
	}
	if header.Protocol != ProtocolVersion || header.RoomID == 0 || header.Epoch == 0 || header.Tick == 0 || header.Sequence == 0 ||
		header.ChunkCount == 0 || int(header.ChunkCount) > limits.MaxFragments || header.ChunkIndex >= header.ChunkCount ||
		int(header.PayloadLen) != len(packet)-DatagramHeaderSize || header.Flags & ^DatagramFlagFull != 0 {
		return DatagramHeader{}, nil, ErrInvalidDatagram
	}
	if checksumDatagram(packet) != header.Checksum {
		return DatagramHeader{}, nil, ErrChecksumMismatch
	}
	payload := packet[DatagramHeaderSize:]
	if copyPayload {
		payload = append([]byte(nil), payload...)
	}
	return header, payload, nil
}

func checksumDatagram(packet []byte) uint32 {
	hash := crc32.NewIEEE()
	_, _ = hash.Write(packet[:38])
	_, _ = hash.Write([]byte{0, 0, 0, 0})
	if len(packet) > DatagramHeaderSize {
		_, _ = hash.Write(packet[DatagramHeaderSize:])
	}
	return hash.Sum32()
}

type assemblyKey struct {
	session  SessionID
	roomID   uint64
	epoch    uint32
	tick     uint32
	sequence uint32
}

type frameAssembly struct {
	header   DatagramHeader
	created  time.Time
	chunks   [][]byte
	received int
	total    int
}

type Reassembler struct {
	mu       sync.Mutex
	limits   Limits
	ttl      time.Duration
	inflight map[assemblyKey]*frameAssembly
}

func NewReassembler(limits Limits, ttl time.Duration) *Reassembler {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Reassembler{limits: normalizeLimits(limits), ttl: ttl, inflight: make(map[assemblyKey]*frameAssembly)}
}

func (r *Reassembler) Push(packet []byte, now time.Time) ([]byte, bool, DatagramHeader, error) {
	return r.push(0, packet, now)
}

// PushFor scopes fragmented frames by authenticated session. Gateways that
// share one Reassembler across clients must use this method; per-client
// reassemblers may keep using Push.
func (r *Reassembler) PushFor(session SessionID, packet []byte, now time.Time) ([]byte, bool, DatagramHeader, error) {
	if session == 0 {
		return nil, false, DatagramHeader{}, ErrSessionNotFound
	}
	return r.push(session, packet, now)
}

func (r *Reassembler) push(session SessionID, packet []byte, now time.Time) ([]byte, bool, DatagramHeader, error) {
	if r == nil {
		return nil, false, DatagramHeader{}, ErrInvalidDatagram
	}
	if now.IsZero() {
		now = time.Now()
	}
	header, payload, err := DecodeDatagram(packet, r.limits)
	if err != nil {
		return nil, false, DatagramHeader{}, err
	}
	if header.ChunkCount == 1 {
		return payload, true, header, nil
	}
	key := assemblyKey{session: session, roomID: header.RoomID, epoch: header.Epoch, tick: header.Tick, sequence: header.Sequence}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireLocked(now)
	assembly := r.inflight[key]
	if assembly == nil {
		if len(r.inflight) >= r.limits.MaxInflightFrames {
			return nil, false, header, ErrReassemblyCapacity
		}
		assembly = &frameAssembly{header: header, created: now, chunks: make([][]byte, int(header.ChunkCount))}
		r.inflight[key] = assembly
	} else if assembly.header.ChunkCount != header.ChunkCount || assembly.header.BaseTick != header.BaseTick || assembly.header.Flags != header.Flags {
		delete(r.inflight, key)
		return nil, false, header, ErrInvalidDatagram
	}
	index := int(header.ChunkIndex)
	if existing := assembly.chunks[index]; existing != nil {
		if !bytes.Equal(existing, payload) {
			delete(r.inflight, key)
			return nil, false, header, ErrInvalidDatagram
		}
		return nil, false, header, nil
	}
	if assembly.total+len(payload) > r.limits.MaxFrameBytes {
		delete(r.inflight, key)
		return nil, false, header, ErrFrameTooLarge
	}
	assembly.chunks[index] = payload
	assembly.received++
	assembly.total += len(payload)
	if assembly.received != len(assembly.chunks) {
		return nil, false, header, nil
	}
	out := make([]byte, 0, assembly.total)
	for _, chunk := range assembly.chunks {
		out = append(out, chunk...)
	}
	delete(r.inflight, key)
	return out, true, header, nil
}

func (r *Reassembler) Expire(now time.Time) int {
	if r == nil {
		return 0
	}
	if now.IsZero() {
		now = time.Now()
	}
	r.mu.Lock()
	removed := r.expireLocked(now)
	r.mu.Unlock()
	return removed
}

func (r *Reassembler) expireLocked(now time.Time) int {
	removed := 0
	for key, assembly := range r.inflight {
		if now.Sub(assembly.created) >= r.ttl {
			delete(r.inflight, key)
			removed++
		}
	}
	return removed
}

func (r *Reassembler) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.inflight)
}
