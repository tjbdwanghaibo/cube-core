package clock

import (
	"sync/atomic"
	"time"
)

// Clock exposes the server logic time used by business code.
type Clock interface {
	Now() time.Time
	UnixMilli() int64
	Set(now time.Time)
	SetOffset(offset time.Duration)
	Offset() time.Duration
	Reset()
}

type logicClock struct {
	offsetMilli atomic.Int64
}

var global Clock = NewLogicClock()

func NewLogicClock() Clock {
	return &logicClock{}
}

func Now() time.Time {
	return global.Now()
}

func UnixMilli() int64 {
	return global.UnixMilli()
}

func Set(now time.Time) {
	global.Set(now)
}

func SetOffset(offset time.Duration) {
	global.SetOffset(offset)
}

func Offset() time.Duration {
	return global.Offset()
}

func Reset() {
	global.Reset()
}

func (c *logicClock) Now() time.Time {
	return time.Now().Add(c.Offset())
}

func (c *logicClock) UnixMilli() int64 {
	return c.Now().UnixMilli()
}

func (c *logicClock) Set(now time.Time) {
	c.SetOffset(now.Sub(time.Now()))
}

func (c *logicClock) SetOffset(offset time.Duration) {
	c.offsetMilli.Store(offset.Milliseconds())
}

func (c *logicClock) Offset() time.Duration {
	return time.Duration(c.offsetMilli.Load()) * time.Millisecond
}

func (c *logicClock) Reset() {
	c.offsetMilli.Store(0)
}
