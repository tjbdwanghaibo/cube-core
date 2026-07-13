package nest

import (
	"github.com/tjbdwanghaibo/cube-core/worker"
	"time"
)

type DispatcherWorkStats struct {
	ProcessedMessages uint64
	Slow200msMessages uint64
}

type DispatcherStats struct {
	Main    worker.PoolStats
	Heart   worker.PoolStats
	Cost    worker.PoolStats
	Delayed int
	Stopped bool
	Work    DispatcherWorkStats
}

func (m *Dispatcher) Stats() DispatcherStats {
	if m == nil {
		return DispatcherStats{}
	}
	m.mu.Lock()
	delayed := len(m.delayed)
	stopped := m.stopped
	m.mu.Unlock()
	stats := DispatcherStats{
		Delayed: delayed,
		Stopped: stopped,
		Work: DispatcherWorkStats{
			ProcessedMessages: m.processed.Load(),
			Slow200msMessages: m.slow200ms.Load(),
		},
	}
	if m.pool != nil {
		stats.Main = m.pool.Stats()
	}
	if m.hbPool != nil {
		stats.Heart = m.hbPool.Stats()
	}
	if m.costPool != nil {
		stats.Cost = m.costPool.Stats()
	}
	return stats
}

func (m *Dispatcher) recordDispatch(cost time.Duration) {
	if m == nil {
		return
	}
	m.processed.Add(1)
	if shouldLogSlowDispatch(cost) {
		m.slow200ms.Add(1)
	}
}

func (mgr *NestMgr) Stats() DispatcherStats {
	if mgr == nil || mgr.dispatcher == nil {
		return DispatcherStats{}
	}
	return mgr.dispatcher.Stats()
}

func (mgr *NestMgr) recordDispatch(cost time.Duration) {
	if mgr == nil || mgr.dispatcher == nil {
		return
	}
	mgr.dispatcher.recordDispatch(cost)
}
