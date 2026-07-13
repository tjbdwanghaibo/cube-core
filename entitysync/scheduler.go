package entitysync

import (
	"context"
	"sync"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

const DefaultSchedulerInterval = 50 * time.Millisecond

// Scheduler collects dirty entity sync states and flushes their packets through
// a coalescing sink. It lets non-scene entities share the same sync delivery path.
type Scheduler struct {
	mu        sync.Mutex
	dirty     map[*entity.EntitySyncState]struct{}
	coalescer *CoalescingSink
	stop      chan struct{}
	stopped   chan struct{}
	started   bool
	stopping  bool
}

func NewScheduler(downstream entity.EntitySyncSink) *Scheduler {
	return &Scheduler{
		dirty:     make(map[*entity.EntitySyncState]struct{}),
		coalescer: NewCoalescingSink(downstream),
	}
}

func (s *Scheduler) SetDownstream(downstream entity.EntitySyncSink) {
	if s == nil {
		return
	}
	s.coalescer.SetDownstream(downstream)
}

func (s *Scheduler) MarkDirtyState(state *entity.EntitySyncState) {
	if s == nil || state == nil {
		return
	}
	s.mu.Lock()
	s.dirty[state] = struct{}{}
	s.mu.Unlock()
}

func (s *Scheduler) Enqueue(packet entity.SyncPacket) {
	if s == nil {
		return
	}
	s.coalescer.Enqueue(packet)
}

func (s *Scheduler) EnqueueBatch(packets []entity.SyncPacket) {
	if s == nil {
		return
	}
	s.coalescer.EnqueueBatch(packets)
}

func (s *Scheduler) Flush() []entity.SyncPacket {
	if s == nil {
		return nil
	}
	states := s.takeDirtyStates()
	for _, state := range states {
		packets := state.Flush()
		if len(packets) == 0 && state.PendingDirty() {
			s.MarkDirtyState(state)
			continue
		}
		s.coalescer.EnqueueBatch(packets)
	}
	return s.coalescer.Flush()
}

func (s *Scheduler) Start(ctx context.Context, interval time.Duration) {
	if s == nil {
		return
	}
	if interval <= 0 {
		interval = DefaultSchedulerInterval
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	stopped := make(chan struct{})
	s.stop = stop
	s.stopped = stopped
	s.started = true
	s.stopping = false
	s.mu.Unlock()

	go s.run(ctx, interval, stop, stopped)
}

func (s *Scheduler) Stop() {
	if err := s.StopWithContext(fctx.BaseContext()); err != nil {
		return
	}
}

func (s *Scheduler) StopWithContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	stop := s.stop
	stopped := s.stopped
	if !s.stopping {
		s.stopping = true
		close(stop)
	}
	s.mu.Unlock()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) run(ctx context.Context, interval time.Duration, stop <-chan struct{}, stopped chan<- struct{}) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		s.mu.Lock()
		if s.stop == stop {
			s.started = false
			s.stopping = false
			s.stop = nil
			s.stopped = nil
		}
		s.mu.Unlock()
		close(stopped)
	}()
	for {
		select {
		case <-ctx.Done():
			s.Flush()
			return
		case <-stop:
			s.Flush()
			return
		case <-ticker.C:
			s.Flush()
		}
	}
}

func (s *Scheduler) takeDirtyStates() []*entity.EntitySyncState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.dirty) == 0 {
		return nil
	}
	states := make([]*entity.EntitySyncState, 0, len(s.dirty))
	for state := range s.dirty {
		states = append(states, state)
		delete(s.dirty, state)
	}
	return states
}

var _ entity.EntitySyncScheduler = (*Scheduler)(nil)
