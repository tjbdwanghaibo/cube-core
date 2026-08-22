package replication

import "sync"

type SessionState struct {
	mu          sync.Mutex
	sendMu      sync.Mutex
	info        SessionInfo
	ackTick     uint32
	lastSent    uint32
	sequence    uint32
	forceFull   bool
	closed      bool
	qualityTier uint8
	sent        map[uint32]Snapshot
	sentOrder   []uint32
	maxHistory  int
	generation  uint64
	committed   uint32
	controlSeq  uint32
}

func (s *SessionState) handleControl(message ControlMessage, latestPublished uint32) error {
	if s == nil {
		return ErrSessionNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSessionNotFound
	}
	if message.Sequence <= s.controlSeq {
		return ErrInvalidControl
	}
	s.controlSeq = message.Sequence
	switch message.Type {
	case ControlAck:
		if message.Tick == 0 || message.Tick > latestPublished || message.Tick > s.lastSent {
			return ErrInvalidAck
		}
		if _, ok := s.sent[message.Tick]; !ok {
			return ErrInvalidAck
		}
		if message.Tick > s.ackTick {
			s.ackTick = message.Tick
			s.forceFull = false
		}
	case ControlResync:
		s.forceFull = true
		s.generation++
	default:
		return ErrInvalidControl
	}
	return nil
}

func NewSessionState(info SessionInfo) (*SessionState, error) {
	if info.ID == 0 {
		return nil, ErrSessionNotFound
	}
	return &SessionState{info: info, forceFull: true, sent: make(map[uint32]Snapshot), maxHistory: 64}, nil
}

func (s *SessionState) Info() SessionInfo {
	if s == nil {
		return SessionInfo{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info
}

func (s *SessionState) Acknowledge(tick, latestPublished uint32) error {
	if s == nil {
		return ErrSessionNotFound
	}
	if tick == 0 || tick > latestPublished {
		return ErrInvalidAck
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSessionNotFound
	}
	if tick > s.lastSent {
		return ErrInvalidAck
	}
	if _, ok := s.sent[tick]; !ok {
		return ErrInvalidAck
	}
	if tick > s.ackTick {
		s.ackTick = tick
		s.forceFull = false
	}
	return nil
}

func (s *SessionState) ForceFull() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.forceFull = true
		s.generation++
	}
	s.mu.Unlock()
}

func (s *SessionState) SetQualityTier(tier uint8) error {
	if s == nil {
		return ErrSessionNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSessionNotFound
	}
	if s.qualityTier != tier {
		s.qualityTier = tier
		s.generation++
	}
	return nil
}

func (s *SessionState) QualityTier() uint8 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.qualityTier
}

func (s *SessionState) prepare(targetTick uint32) (SessionInfo, uint8, *Snapshot, *Snapshot, uint32, uint64, bool, error) {
	if s == nil {
		return SessionInfo{}, 0, nil, nil, 0, 0, false, ErrSessionNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return SessionInfo{}, 0, nil, nil, 0, 0, false, ErrSessionNotFound
	}
	s.sequence++
	if s.sequence == 0 {
		s.sequence++
	}
	var previous *Snapshot
	if latest, ok := s.sent[s.lastSent]; ok {
		clone := latest.Clone()
		previous = &clone
	}
	if !s.forceFull && s.ackTick != 0 && s.ackTick < targetTick {
		if base, ok := s.sent[s.ackTick]; ok {
			base = base.Clone()
			return s.info, s.qualityTier, &base, previous, s.sequence, s.generation, false, nil
		}
		s.forceFull = true
	}
	return s.info, s.qualityTier, nil, previous, s.sequence, s.generation, true, nil
}

func (s *SessionState) commitPrepared(snapshot Snapshot, sequence uint32, generation uint64, full bool) error {
	if s == nil {
		return ErrSessionNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSessionNotFound
	}
	if generation != s.generation || sequence <= s.committed {
		return ErrPreparedFrameStale
	}
	s.committed = sequence
	if snapshot.Tick > s.lastSent {
		s.lastSent = snapshot.Tick
	}
	if _, exists := s.sent[snapshot.Tick]; !exists {
		s.sentOrder = append(s.sentOrder, snapshot.Tick)
	}
	s.sent[snapshot.Tick] = snapshot.Clone()
	for len(s.sentOrder) > s.maxHistory {
		oldest := s.sentOrder[0]
		s.sentOrder = s.sentOrder[1:]
		delete(s.sent, oldest)
	}
	if full {
		s.forceFull = false
	}
	return nil
}

func (s *SessionState) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	clear(s.sent)
	s.sentOrder = nil
	s.mu.Unlock()
}

type SessionSnapshot struct {
	Info        SessionInfo
	AckTick     uint32
	LastSent    uint32
	Sequence    uint32
	ForceFull   bool
	Closed      bool
	QualityTier uint8
}

func (s *SessionState) Snapshot() SessionSnapshot {
	if s == nil {
		return SessionSnapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return SessionSnapshot{
		Info: s.info, AckTick: s.ackTick, LastSent: s.lastSent, Sequence: s.sequence,
		ForceFull: s.forceFull, Closed: s.closed, QualityTier: s.qualityTier,
	}
}
