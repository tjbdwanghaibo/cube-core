package nest

import (
	"container/heap"
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	flog "github.com/tjbdwanghaibo/cube-core/log"
	"github.com/tjbdwanghaibo/cube-core/misc"
	"github.com/tjbdwanghaibo/cube-core/obs"
	"github.com/tjbdwanghaibo/cube-core/worker"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Dispatcher struct {
	Name          string
	MsgCap        int
	pool          *worker.Pool[*Msg]
	hbPool        *worker.Pool[*Msg]
	costPool      *worker.Pool[*Msg]
	workerNum     int
	hbWorkerNum   int
	costWorkerNum int
	handler       func(*Msg)
	mu            sync.Mutex
	delayed       map[*delayedMsg]struct{}
	delayedHeap   delayedMsgHeap
	delayNotify   chan struct{}
	delayStop     chan struct{}
	delayDone     chan struct{}
	delaySeq      uint64
	stopped       bool
	observeSeq    uint64
	processed     atomic.Uint64
	slow200ms     atomic.Uint64
}

type delayedMsg struct {
	due time.Time
	seq uint64
	msg *Msg
	idx int
}

type delayedMsgHeap []*delayedMsg

func (h delayedMsgHeap) Len() int { return len(h) }
func (h delayedMsgHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}
func (h delayedMsgHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}
func (h *delayedMsgHeap) Push(x any) {
	item := x.(*delayedMsg)
	item.idx = len(*h)
	*h = append(*h, item)
}
func (h *delayedMsgHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	item.idx = -1
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

func NewDispatcher(name string, workerNum, hbWorkerNum int, msgCap int, handler func(*Msg)) *Dispatcher {
	ret := &Dispatcher{
		Name:        name,
		MsgCap:      msgCap,
		workerNum:   workerNum,
		hbWorkerNum: hbWorkerNum,
		handler:     handler,
	}
	if ret.workerNum <= 0 {
		ret.workerNum = 1
	}
	ret.costWorkerNum = ret.workerNum
	return ret
}

func (m *Dispatcher) OnInit() {
	m.mu.Lock()
	m.delayed = make(map[*delayedMsg]struct{})
	m.delayedHeap = nil
	m.delayNotify = make(chan struct{}, 1)
	m.delayStop = make(chan struct{})
	m.delayDone = make(chan struct{})
	m.stopped = false
	m.mu.Unlock()
	go m.delayLoop()

	m.pool = worker.NewPool[*Msg](worker.PoolConfig{
		Name:      m.Name,
		WorkerNum: m.workerNum,
		QueueCap:  m.MsgCap,
	}, m.handler)

	if m.hbWorkerNum > 0 {
		m.hbPool = worker.NewPool[*Msg](worker.PoolConfig{
			Name:      m.Name + "_hb",
			WorkerNum: m.hbWorkerNum,
			QueueCap:  m.MsgCap,
		}, m.handler)
	}
	m.costPool = worker.NewPool[*Msg](worker.PoolConfig{
		Name:      m.Name + "_cost",
		WorkerNum: m.costWorkerNum,
		QueueCap:  m.MsgCap,
	}, m.handler)
}

func (m *Dispatcher) OnRun() {
	m.pool.Start()
	if m.hbPool != nil {
		m.hbPool.Start()
	}
	if m.costPool != nil {
		m.costPool.Start()
	}
}

func (m *Dispatcher) OnDestroy() {
	if err := m.OnDestroyWithContext(fctx.BaseContext()); err != nil {
		flog.NewELog().Title("nest").Warn("dispatcher stop interrupted", "err", err)
	}
}

func (m *Dispatcher) OnDestroyWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	m.mu.Lock()
	m.stopped = true
	delayed := m.delayed
	m.delayed = make(map[*delayedMsg]struct{})
	m.delayedHeap = nil
	stop := m.delayStop
	done := m.delayDone
	m.delayStop = nil
	m.mu.Unlock()

	if stop != nil {
		close(stop)
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.mu.Lock()
	if m.delayDone == done {
		m.delayDone = nil
	}
	m.mu.Unlock()
	for dm := range delayed {
		recycleMsg(dm.msg)
	}

	var err error
	if m.pool != nil {
		err = errors.Join(err, m.pool.StopWithContext(ctx))
	}
	if m.hbPool != nil {
		err = errors.Join(err, m.hbPool.StopWithContext(ctx))
	}
	if m.costPool != nil {
		err = errors.Join(err, m.costPool.StopWithContext(ctx))
	}
	return err
}

func hashKey(key int64) uint64 {
	return misc.Hash64(uint64(key))
}

const (
	MaxBroadcastIdNum     = 32
	spliceDenseGroupLimit = 8
)

func (m *Dispatcher) SendMsg(msg *Msg) {
	if msg == nil {
		return
	}
	trace := newNestTraceEventInfo(msg)
	m.mu.Lock()
	stopped := m.stopped
	m.mu.Unlock()
	if stopped {
		emitNestTraceEventInfo(trace, "enqueue", "stopped", 0)
		if msg.RetChan != nil {
			msg.RetChan <- ErrNestStopped
		} else {
			logAsyncDispatchFailure(msg, ErrNestStopped)
		}
		recycleMsg(msg)
		return
	}
	msg.OnSend()
	dispatch := func(pool *worker.Pool[*Msg]) {
		if pool == nil {
			emitNestTraceEventInfo(trace, "enqueue", "stopped", 0)
			if msg.RetChan != nil {
				msg.RetChan <- ErrNestStopped
			} else {
				logAsyncDispatchFailure(msg, ErrNestStopped)
			}
			msg.OnRelease()
			return
		}
		if err := pool.TryDispatch(msg.Key(), msg); err != nil {
			emitNestTraceEventInfo(trace, "enqueue", "error", 0)
			if msg.RetChan != nil {
				msg.RetChan <- err
			} else {
				logAsyncDispatchFailure(msg, err)
			}
			msg.OnRelease()
			return
		}
		emitNestTraceEventInfo(trace, "enqueue", "ok", 0)
	}
	if msg.Cost || msg.HasRemote {
		if m.costPool != nil {
			dispatch(m.costPool)
		} else {
			dispatch(m.pool)
		}
	} else {
		if msg.Type == MsgTypeBroadcast {
			if m.hbPool != nil {
				dispatch(m.hbPool)
			} else {
				dispatch(m.pool)
			}
		} else {
			dispatch(m.pool)
		}
	}
	m.observeStatsIfDue()
}

const dispatcherObserveStatsEvery = 1024

func (m *Dispatcher) observeStatsIfDue() {
	if m == nil {
		return
	}
	seq := atomic.AddUint64(&m.observeSeq, 1)
	if seq%dispatcherObserveStatsEvery == 0 {
		m.observeStats()
	}
}

func (m *Dispatcher) observeStats() {
	if m == nil {
		return
	}
	m.observePoolStats("main", m.pool)
	m.observePoolStats("heartbeat", m.hbPool)
	m.observePoolStats("cost", m.costPool)
	obs.SetGauge("nest.dispatch.delayed_messages", obs.Labels{
		"dispatcher": m.Name,
	}, int64(m.delayedCount()))
}

func (m *Dispatcher) observePoolStats(poolName string, pool *worker.Pool[*Msg]) {
	if m == nil || pool == nil {
		return
	}
	stats := pool.Stats()
	labels := obs.Labels{
		"dispatcher": m.Name,
		"pool":       poolName,
	}
	obs.SetGauge("nest.dispatch.queue_len", labels, int64(stats.QueueLen))
	obs.SetGauge("nest.dispatch.worker_num", labels, int64(stats.WorkerNum))
}

func (m *Dispatcher) delayedCount() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.delayed)
}

func logAsyncDispatchFailure(msg *Msg, err error) {
	if msg == nil || err == nil {
		return
	}
	flog.NewELog().Title("nest").Warn("async dispatch failed",
		"handler", msg.Name,
		"type", msg.Type.String(),
		"key", msg.Key(),
		"tid", msg.Tid,
		"tids", len(msg.Tids),
		"groups", len(msg.GroupTIds),
		"cost", msg.Cost,
		"remote", msg.HasRemote,
		"err", err,
	)
}

func (m *Dispatcher) DelaySendMsg(delay time.Duration, msg *Msg) {
	if delay <= 0 {
		m.SendMsg(msg)
		return
	}
	dm := &delayedMsg{
		due: time.Now().Add(delay),
		msg: msg,
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		recycleMsg(msg)
		return
	}
	m.delaySeq++
	dm.seq = m.delaySeq
	m.delayed[dm] = struct{}{}
	heap.Push(&m.delayedHeap, dm)
	notify := m.delayNotify
	m.mu.Unlock()
	notifyDelayLoop(notify)
}

func notifyDelayLoop(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (m *Dispatcher) delayLoop() {
	defer func() {
		m.mu.Lock()
		done := m.delayDone
		m.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()
	for {
		dm, wait, stop, notify := m.nextDelayedWait()
		if stop != nil && dm == nil && wait < 0 {
			select {
			case <-notify:
				continue
			case <-stop:
				return
			}
		}
		if stop == nil {
			return
		}
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-notify:
			case <-stop:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		}
		if dm == nil {
			continue
		}
		m.SendMsg(dm.msg)
	}
}

func (m *Dispatcher) nextDelayedWait() (*delayedMsg, time.Duration, chan struct{}, chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stop := m.delayStop
	notify := m.delayNotify
	if m.stopped {
		return nil, 0, nil, notify
	}
	if len(m.delayedHeap) == 0 {
		return nil, -1, stop, notify
	}
	next := m.delayedHeap[0]
	wait := time.Until(next.due)
	if wait > 0 {
		return nil, wait, stop, notify
	}
	dm := heap.Pop(&m.delayedHeap).(*delayedMsg)
	delete(m.delayed, dm)
	return dm, 0, stop, notify
}

type spliceSparseGroupSlots struct {
	group    int
	slots    [][]int64
	idxSlots [][]int
}

type spliceGroupBuckets struct {
	dense       [spliceDenseGroupLimit][][]int64
	denseIdx    [spliceDenseGroupLimit][][]int
	sparse      []spliceSparseGroupSlots
	sparseIndex map[int]int
}

func (m *Dispatcher) getGroupSlots(b *spliceGroupBuckets, group int) (*[][]int64, *[][]int) {
	if group >= 0 && group < spliceDenseGroupLimit {
		if b.dense[group] == nil {
			b.dense[group] = make([][]int64, m.workerNum)
			b.denseIdx[group] = make([][]int, m.workerNum)
		}
		return &b.dense[group], &b.denseIdx[group]
	}
	if b.sparseIndex == nil {
		b.sparseIndex = make(map[int]int, 2)
	}
	if idx, ok := b.sparseIndex[group]; ok {
		return &b.sparse[idx].slots, &b.sparse[idx].idxSlots
	}
	b.sparse = append(b.sparse, spliceSparseGroupSlots{
		group:    group,
		slots:    make([][]int64, m.workerNum),
		idxSlots: make([][]int, m.workerNum),
	})
	idx := len(b.sparse) - 1
	b.sparseIndex[group] = idx
	return &b.sparse[idx].slots, &b.sparse[idx].idxSlots
}

func (m *Dispatcher) flushGroupSlots(group int, slots [][]int64, idxSlots [][]int, emit func(group int, batch []int64, origIndices []int)) {
	for i := range slots {
		if len(slots[i]) == 0 {
			continue
		}
		emit(group, slots[i], idxSlots[i])
		slots[i] = nil
		idxSlots[i] = nil
	}
}

// ForEachSpliceBatch partitions broadcast IDs into batches by entity group and worker hash.
func (m *Dispatcher) ForEachSpliceBatch(ids []int64, emit func(group int, batch []int64, origIndices []int)) {
	if len(ids) == 0 || emit == nil {
		return
	}
	var buckets spliceGroupBuckets
	for origIdx, id := range ids {
		group := entity.GetEntityGroup(id)
		slots, idxSlots := m.getGroupSlots(&buckets, group)
		slot := int(hashKey(id) % uint64(m.workerNum))
		batch := (*slots)[slot]
		idxBatch := (*idxSlots)[slot]
		if batch == nil {
			batch = make([]int64, 0, MaxBroadcastIdNum)
			idxBatch = make([]int, 0, MaxBroadcastIdNum)
		}
		batch = append(batch, id)
		idxBatch = append(idxBatch, origIdx)
		if len(batch) == MaxBroadcastIdNum {
			emit(group, batch, idxBatch)
			(*slots)[slot] = nil
			(*idxSlots)[slot] = nil
			continue
		}
		(*slots)[slot] = batch
		(*idxSlots)[slot] = idxBatch
	}
	for group := 0; group < spliceDenseGroupLimit; group++ {
		m.flushGroupSlots(group, buckets.dense[group], buckets.denseIdx[group], emit)
	}
	for i := range buckets.sparse {
		m.flushGroupSlots(buckets.sparse[i].group, buckets.sparse[i].slots, buckets.sparse[i].idxSlots, emit)
	}
}
