package replication

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

type ReplicatorConfig struct {
	Limits      Limits
	RingSize    int
	MaxDatagram int
	Projector   Projector
	Transport   Transport
}

type Replicator struct {
	mu          sync.RWMutex
	lifecycleMu sync.Mutex
	active      sync.WaitGroup
	limits      Limits
	maxDatagram int
	ring        *SnapshotRing
	projector   Projector
	transport   Transport
	sessions    map[SessionID]*SessionState
	closed      bool
	closeDone   chan struct{}
	stats       replicatorCounters
}

type replicatorCounters struct {
	published    atomic.Uint64
	fullFrames   atomic.Uint64
	deltaFrames  atomic.Uint64
	datagrams    atomic.Uint64
	encodedBytes atomic.Uint64
	sentBytes    atomic.Uint64
	sendErrors   atomic.Uint64
	invalidAcks  atomic.Uint64
	forcedFull   atomic.Uint64
	registered   atomic.Uint64
	unregistered atomic.Uint64
}

type ReplicatorStats struct {
	PublishedSnapshots uint64
	FullFrames         uint64
	DeltaFrames        uint64
	Datagrams          uint64
	EncodedBytes       uint64
	SentBytes          uint64
	SendErrors         uint64
	InvalidAcks        uint64
	ForcedFull         uint64
	RegisteredSessions uint64
	RemovedSessions    uint64
	ActiveSessions     int
	SnapshotsRetained  int
}

func NewReplicator(config ReplicatorConfig) *Replicator {
	limits := normalizeLimits(config.Limits)
	if config.RingSize <= 0 {
		config.RingSize = 64
	}
	if config.MaxDatagram <= 0 {
		config.MaxDatagram = DefaultMaxDatagram
	}
	if config.Projector == nil {
		config.Projector = ProjectorFunc(nil)
	}
	return &Replicator{
		limits: limits, maxDatagram: config.MaxDatagram, ring: NewSnapshotRing(config.RingSize),
		projector: config.Projector, transport: config.Transport, sessions: make(map[SessionID]*SessionState), closeDone: make(chan struct{}),
	}
}

func (r *Replicator) Publish(snapshot Snapshot) error {
	if !r.begin() {
		return ErrReplicatorClosed
	}
	defer r.active.Done()
	normalized, err := NewSnapshot(snapshot.SnapshotMeta, snapshot.Objects, r.limits)
	if err != nil {
		return err
	}
	if err := r.ring.Add(normalized); err != nil {
		return err
	}
	r.stats.published.Add(1)
	return nil
}

func (r *Replicator) RegisterSession(info SessionInfo) error {
	if r == nil || info.ID == 0 {
		return ErrSessionNotFound
	}
	state, err := NewSessionState(info)
	if err != nil {
		return err
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrReplicatorClosed
	}
	if _, exists := r.sessions[info.ID]; exists {
		r.mu.Unlock()
		return fmt.Errorf("replication: session %d already registered", info.ID)
	}
	transport := r.transport
	r.mu.Unlock()
	if lifecycle, ok := transport.(SessionTransport); ok {
		if err := lifecycle.RegisterSession(info); err != nil {
			state.Close()
			return err
		}
	}
	// Publish the core session only after the transport queue is ready. A frame
	// flush running concurrently must never observe a half-registered session.
	r.mu.Lock()
	r.sessions[info.ID] = state
	r.mu.Unlock()
	r.stats.registered.Add(1)
	return nil
}

func (r *Replicator) RemoveSession(id SessionID) bool {
	if r == nil || id == 0 {
		return false
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.mu.Lock()
	state, ok := r.sessions[id]
	delete(r.sessions, id)
	transport := r.transport
	r.mu.Unlock()
	if ok {
		if lifecycle, supported := transport.(SessionTransport); supported {
			lifecycle.RemoveSession(id)
		}
		state.Close()
		r.stats.unregistered.Add(1)
	}
	return ok
}

func (r *Replicator) Acknowledge(id SessionID, tick uint32) error {
	if !r.begin() {
		return ErrReplicatorClosed
	}
	defer r.active.Done()
	state, err := r.session(id)
	if err != nil {
		return err
	}
	if err := state.Acknowledge(tick, r.ring.LatestTick()); err != nil {
		r.stats.invalidAcks.Add(1)
		return err
	}
	return nil
}

func (r *Replicator) ForceFull(id SessionID) error {
	if !r.begin() {
		return ErrReplicatorClosed
	}
	defer r.active.Done()
	state, err := r.session(id)
	if err != nil {
		return err
	}
	state.ForceFull()
	r.stats.forcedFull.Add(1)
	return nil
}

func (r *Replicator) BuildLatest(id SessionID) (DeltaFrame, [][]byte, error) {
	if !r.begin() {
		return DeltaFrame{}, nil, ErrReplicatorClosed
	}
	defer r.active.Done()
	state, err := r.session(id)
	if err != nil {
		return DeltaFrame{}, nil, err
	}
	current, ok := r.ring.Latest()
	if !ok {
		return DeltaFrame{}, nil, ErrSnapshotNotFound
	}
	info, base, sequence, err := state.prepare(current.Tick)
	if err != nil {
		return DeltaFrame{}, nil, err
	}
	current, err = r.projectAndNormalize(info, current)
	if err != nil {
		return DeltaFrame{}, nil, err
	}
	frame, err := BuildDelta(base, current)
	if err != nil {
		return DeltaFrame{}, nil, err
	}
	encoded, err := EncodeFrame(frame, r.limits)
	if err != nil {
		return DeltaFrame{}, nil, err
	}
	packets, err := FragmentFrame(frame, sequence, encoded, r.maxDatagram, r.limits)
	if err != nil {
		return DeltaFrame{}, nil, err
	}
	state.markSent(current, frame.Kind == FrameFull)
	if frame.Kind == FrameFull {
		r.stats.fullFrames.Add(1)
	} else {
		r.stats.deltaFrames.Add(1)
	}
	r.stats.encodedBytes.Add(uint64(len(encoded)))
	r.stats.datagrams.Add(uint64(len(packets)))
	return frame, packets, nil
}

func (r *Replicator) SendLatest(ctx context.Context, id SessionID) (DeltaFrame, error) {
	if !r.begin() {
		return DeltaFrame{}, ErrReplicatorClosed
	}
	defer r.active.Done()
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	transport := r.transport
	r.mu.RUnlock()
	if transport == nil {
		return DeltaFrame{}, ErrTransportMissing
	}
	frame, packets, err := r.BuildLatest(id)
	if err != nil {
		return DeltaFrame{}, err
	}
	if batchTransport, ok := transport.(DatagramBatchTransport); ok {
		if err := batchTransport.SendDatagramBatch(ctx, id, packets); err != nil {
			r.stats.sendErrors.Add(1)
			return frame, err
		}
		for _, packet := range packets {
			r.stats.sentBytes.Add(uint64(len(packet)))
		}
		return frame, nil
	}
	for _, packet := range packets {
		if err := transport.SendDatagram(ctx, id, packet); err != nil {
			r.stats.sendErrors.Add(1)
			return frame, err
		}
		r.stats.sentBytes.Add(uint64(len(packet)))
	}
	return frame, nil
}

func (r *Replicator) SendReliable(ctx context.Context, id SessionID, payload []byte) error {
	if !r.begin() {
		return ErrReplicatorClosed
	}
	defer r.active.Done()
	if _, err := r.session(id); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	transport := r.transport
	r.mu.RUnlock()
	if transport == nil {
		return ErrTransportMissing
	}
	if err := transport.SendReliable(ctx, id, append([]byte(nil), payload...)); err != nil {
		r.stats.sendErrors.Add(1)
		return err
	}
	r.stats.sentBytes.Add(uint64(len(payload)))
	return nil
}

func (r *Replicator) SetTransport(transport Transport) error {
	if r == nil {
		return ErrReplicatorClosed
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ErrReplicatorClosed
	}
	old := r.transport
	infos := make([]SessionInfo, 0, len(r.sessions))
	states := make([]*SessionState, 0, len(r.sessions))
	for _, state := range r.sessions {
		infos = append(infos, state.Info())
		states = append(states, state)
	}
	r.mu.RUnlock()
	if sameTransport(old, transport) {
		return nil
	}
	registered := make([]SessionID, 0, len(infos))
	if lifecycle, ok := transport.(SessionTransport); ok {
		for _, info := range infos {
			if err := lifecycle.RegisterSession(info); err != nil {
				for _, id := range registered {
					lifecycle.RemoveSession(id)
				}
				return err
			}
			registered = append(registered, info.ID)
		}
	}
	r.mu.Lock()
	r.transport = transport
	r.mu.Unlock()
	for _, state := range states {
		state.ForceFull()
	}
	if lifecycle, ok := old.(SessionTransport); ok && !sameTransport(old, transport) {
		for _, info := range infos {
			lifecycle.RemoveSession(info.ID)
		}
	}
	return nil
}

func sameTransport(left, right Transport) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftType := reflect.TypeOf(left)
	if leftType != reflect.TypeOf(right) || !leftType.Comparable() {
		return false
	}
	return left == right
}

func (r *Replicator) SetProjector(projector Projector) error {
	if r == nil {
		return ErrReplicatorClosed
	}
	if projector == nil {
		projector = ProjectorFunc(nil)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrReplicatorClosed
	}
	if len(r.sessions) != 0 || r.ring.Len() != 0 {
		return ErrSchemaFrozen
	}
	r.projector = projector
	return nil
}

func (r *Replicator) Session(id SessionID) (SessionSnapshot, bool) {
	state, err := r.session(id)
	if err != nil {
		return SessionSnapshot{}, false
	}
	return state.Snapshot(), true
}

func (r *Replicator) SessionIDs() []SessionID {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	out := make([]SessionID, 0, len(r.sessions))
	for id := range r.sessions {
		out = append(out, id)
	}
	r.mu.RUnlock()
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func (r *Replicator) Stats() ReplicatorStats {
	if r == nil {
		return ReplicatorStats{}
	}
	r.mu.RLock()
	active := len(r.sessions)
	r.mu.RUnlock()
	return ReplicatorStats{
		PublishedSnapshots: r.stats.published.Load(), FullFrames: r.stats.fullFrames.Load(),
		DeltaFrames: r.stats.deltaFrames.Load(), Datagrams: r.stats.datagrams.Load(),
		EncodedBytes: r.stats.encodedBytes.Load(), SentBytes: r.stats.sentBytes.Load(),
		SendErrors: r.stats.sendErrors.Load(), InvalidAcks: r.stats.invalidAcks.Load(),
		ForcedFull: r.stats.forcedFull.Load(), RegisteredSessions: r.stats.registered.Load(),
		RemovedSessions: r.stats.unregistered.Load(), ActiveSessions: active, SnapshotsRetained: r.ring.Len(),
	}
}

func (r *Replicator) Close() {
	if r == nil {
		return
	}
	r.lifecycleMu.Lock()
	r.mu.Lock()
	if r.closed {
		done := r.closeDone
		r.mu.Unlock()
		r.lifecycleMu.Unlock()
		<-done
		return
	}
	r.closed = true
	done := r.closeDone
	transport := r.transport
	r.mu.Unlock()
	r.lifecycleMu.Unlock()
	r.active.Wait()

	r.mu.Lock()
	sessions := r.sessions
	r.sessions = make(map[SessionID]*SessionState)
	r.transport = nil
	r.mu.Unlock()
	for id, session := range sessions {
		if lifecycle, ok := transport.(SessionTransport); ok {
			lifecycle.RemoveSession(id)
		}
		session.Close()
	}
	r.ring.Reset()
	close(done)
}

func (r *Replicator) begin() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.active.Add(1)
	return true
}

func (r *Replicator) session(id SessionID) (*SessionState, error) {
	if r == nil || id == 0 {
		return nil, ErrSessionNotFound
	}
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, ErrReplicatorClosed
	}
	state := r.sessions[id]
	r.mu.RUnlock()
	if state == nil {
		return nil, ErrSessionNotFound
	}
	return state, nil
}

func (r *Replicator) projectAndNormalize(info SessionInfo, snapshot Snapshot) (Snapshot, error) {
	r.mu.RLock()
	projector := r.projector
	r.mu.RUnlock()
	projected, err := projector.Project(info, snapshot.Clone())
	if err != nil {
		return Snapshot{}, err
	}
	if projected.RoomID != snapshot.RoomID || projected.Epoch != snapshot.Epoch || projected.Tick != snapshot.Tick || projected.SchemaVersion != snapshot.SchemaVersion {
		return Snapshot{}, fmt.Errorf("%w: projector changed snapshot metadata", ErrInvalidFrame)
	}
	return NewSnapshot(projected.SnapshotMeta, projected.Objects, r.limits)
}
