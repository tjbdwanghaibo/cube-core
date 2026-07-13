package nest

import (
	"github.com/tjbdwanghaibo/cube-core/misc"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var tick uint64

func CurTick() uint64 {
	return atomic.LoadUint64(&tick)
}

func IncTick() uint64 {
	return atomic.AddUint64(&tick, 1)
}

func SetTick(curTick uint64) {
	atomic.StoreUint64(&tick, curTick)
}

var (
	tickMu    sync.RWMutex
	tickCbMap = make(map[TickCallbackName]func(msg TickMsg))
)

type TickCallbackName struct {
	value string
}

func NewTickCallbackName(value string) TickCallbackName {
	return TickCallbackName{value: value}
}

func (n TickCallbackName) String() string {
	return n.value
}

func RegisterTickCallback(name TickCallbackName, cb func(msg TickMsg)) error {
	tickMu.Lock()
	defer tickMu.Unlock()
	if _, exist := tickCbMap[name]; exist {
		return fmt.Errorf("nest: duplicate tick callback %q", name.String())
	}
	tickCbMap[name] = cb
	return nil
}

func MustRegisterTickCallback(name TickCallbackName, cb func(msg TickMsg)) {
	if err := RegisterTickCallback(name, cb); err != nil {
		panic(err)
	}
}

func RangeAllTickCallback(f func(ff func(msg TickMsg))) {
	tickMu.RLock()
	defer tickMu.RUnlock()
	for _, cb := range tickCbMap {
		f(cb)
	}
}

// Ticker is the frame-based timing system (channel-based, no actor).
type Ticker struct {
	duration     time.Duration
	lastTickTime time.Time
	stopChan     chan struct{}
	done         chan struct{}
	started      atomic.Bool
	stopped      atomic.Bool
	stopOnce     sync.Once
}

func NewTicker(duration time.Duration) *Ticker {
	if duration <= 0 {
		duration = 100 * time.Millisecond
	}
	return &Ticker{
		duration:     duration,
		lastTickTime: time.Now(),
		stopChan:     make(chan struct{}),
		done:         make(chan struct{}),
	}
}

func (t *Ticker) Duration() time.Duration {
	if t == nil || t.duration <= 0 {
		return 100 * time.Millisecond
	}
	return t.duration
}

func (t *Ticker) Start() {
	if t.stopped.Load() {
		return
	}
	if t.started.CompareAndSwap(false, true) {
		go t.run()
	}
}

func (t *Ticker) Stop() {
	if !t.stopped.CompareAndSwap(false, true) {
		if t.started.Load() {
			<-t.done
		}
		return
	}
	if !t.started.Load() {
		return
	}
	t.stopOnce.Do(func() {
		close(t.stopChan)
	})
	<-t.done
}

func (t *Ticker) run() {
	defer close(t.done)
	ticker := time.NewTicker(t.duration)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopChan:
			return
		case <-ticker.C:
			t.doTick()
		}
	}
}

func (t *Ticker) doTick() {
	IncTick()
	curFrame := CurTick()
	now := time.Now()
	var elapsed int64
	if curFrame > 1 {
		elapsed = now.Sub(t.lastTickTime).Nanoseconds()
	}
	t.lastTickTime = now
	msg := TickMsg{Elapsed: elapsed, FrameNumber: curFrame}
	RangeAllTickCallback(func(f func(msg TickMsg)) {
		misc.SafeFunc(func() {
			f(msg)
		})
	})
}
