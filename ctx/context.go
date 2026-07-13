package ctx

import (
	stdctx "context"
	"github.com/tjbdwanghaibo/cube-core/clock"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Context is the per-goroutine request context.
// It carries request-scoped metadata for logging, timing, and config access.
type Context struct {
	Now      time.Time
	NowMilli int64
	Config   any
	Base     stdctx.Context
	Frame    uint64
	Meta     RequestMeta
	Trace    TraceMeta
	SyncWait time.Duration
	keyValue map[any]any
}

// ContextSnapshot is an immutable copy of the request context fields that are
// meaningful across goroutines. It intentionally does not carry Now/NowMilli:
// every goroutine should stamp its own logical server time when the context is
// installed.
type ContextSnapshot struct {
	Valid    bool
	Config   any
	Base     stdctx.Context
	Frame    uint64
	Meta     RequestMeta
	Trace    TraceMeta
	SyncWait time.Duration
	KeyValue map[any]any
}

func (s ContextSnapshot) Clone() ContextSnapshot {
	s.Trace = s.Trace.Clone()
	s.KeyValue = cloneKeyValue(s.KeyValue)
	return s
}

type TraceMeta struct {
	TraceID string
	Enabled bool
	Reason  string
	Sampled bool
	Tags    map[string]string
}

var traceSeq atomic.Uint64

func (t TraceMeta) Active() bool {
	return t.Enabled && t.TraceID != ""
}

func (t TraceMeta) Clone() TraceMeta {
	t.Tags = cloneTraceTags(t.Tags)
	return t
}

func NewTraceID(prefix string) string {
	if prefix == "" {
		prefix = "trace"
	}
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), traceSeq.Add(1))
}

type RequestMeta struct {
	Source   string
	Handler  string
	PlayerID int64
	MsgID    uint32
	Seq      uint32
}

func InNestHandler() bool {
	c := CurrentContext()
	return c != nil && c.Meta.Source == "nest"
}

type Option func(*Context)

var contextPool = sync.Pool{
	New: func() interface{} {
		return &Context{}
	},
}

var (
	runtimeConfigMu sync.RWMutex
	runtimeConfig   any
)

// NewContext creates a request Context, binds it to the current goroutine, and returns a release function.
func NewContext(opts ...Option) (*Context, func()) {
	prev := CurrentContext()
	c := contextPool.Get().(*Context)
	c.init()
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	StoreContext(c)
	released := false
	return c, func() {
		if released {
			return
		}
		released = true
		c.Clear()
		if prev != nil {
			StoreContext(prev)
		} else {
			DeleteContext()
		}
		contextPool.Put(c)
	}
}

func (c *Context) init() {
	c.Now = clock.Now()
	c.NowMilli = c.Now.UnixMilli()
	c.Config = RuntimeConfig()
	c.Base = nil
	c.Frame = 0
	c.Meta = RequestMeta{}
	c.Trace = TraceMeta{}
	c.SyncWait = 0
	c.keyValue = nil
}

func Now() time.Time {
	if c := CurrentContext(); c != nil && !c.Now.IsZero() {
		return c.Now
	}
	return clock.Now()
}

func NowMilli() int64 {
	if c := CurrentContext(); c != nil && c.NowMilli != 0 {
		return c.NowMilli
	}
	return clock.UnixMilli()
}

// Clear clears request-scoped state before the context is returned to pool.
func (c *Context) Clear() {
	c.Config = nil
	c.Base = nil
	c.Frame = 0
	c.Meta = RequestMeta{}
	c.Trace = TraceMeta{}
	c.SyncWait = 0
	c.keyValue = nil
}

func SetRuntimeConfig(config any) {
	runtimeConfigMu.Lock()
	runtimeConfig = config
	runtimeConfigMu.Unlock()
}

func RuntimeConfig() any {
	runtimeConfigMu.RLock()
	defer runtimeConfigMu.RUnlock()
	return runtimeConfig
}

// BaseContext returns the request base context when a request Context is bound.
// It falls back to context.Background for framework paths that are detached
// from a direct request.
func BaseContext() stdctx.Context {
	if c := CurrentContext(); c != nil && c.Base != nil {
		return c.Base
	}
	return stdctx.Background()
}

// BindBase temporarily binds a standard context to the current request Context.
// If no request Context exists, it creates one and restores the previous state on
// release. This is useful at service boundaries that accept context.Context
// but delegate into framework code that reads the bound request Context.
func BindBase(base stdctx.Context) func() {
	if base == nil {
		return func() {}
	}
	if c := CurrentContext(); c != nil {
		old := c.Base
		c.Base = base
		return func() {
			c.Base = old
		}
	}
	_, release := NewContext(WithBase(base))
	return release
}

// CaptureSnapshot copies the current request Context for handoff to another
// goroutine. A zero-value snapshot means no request context was active.
func CaptureSnapshot() ContextSnapshot {
	if c := CurrentContext(); c != nil {
		return c.Snapshot()
	}
	return ContextSnapshot{}
}

func (c *Context) Snapshot() ContextSnapshot {
	if c == nil {
		return ContextSnapshot{}
	}
	return ContextSnapshot{
		Valid:    true,
		Config:   c.Config,
		Base:     c.Base,
		Frame:    c.Frame,
		Meta:     c.Meta,
		Trace:    c.Trace.Clone(),
		SyncWait: c.SyncWait,
		KeyValue: cloneKeyValue(c.keyValue),
	}
}

func WithSnapshot(snapshot ContextSnapshot) Option {
	return func(c *Context) {
		c.ApplySnapshot(snapshot)
	}
}

func (c *Context) ApplySnapshot(snapshot ContextSnapshot) {
	if c == nil || !snapshot.Valid {
		return
	}
	c.Config = snapshot.Config
	c.Base = snapshot.Base
	c.Frame = snapshot.Frame
	c.Meta = snapshot.Meta
	c.Trace = snapshot.Trace.Clone()
	c.SyncWait = snapshot.SyncWait
	c.keyValue = cloneKeyValue(snapshot.KeyValue)
}

func WithConfig(config any) Option {
	return func(c *Context) {
		c.Config = config
	}
}

func WithBase(base stdctx.Context) Option {
	return func(c *Context) {
		c.Base = base
	}
}

func WithSyncWait(timeout time.Duration) Option {
	return func(c *Context) {
		c.SyncWait = timeout
	}
}

func (c *Context) Done() <-chan struct{} {
	if c == nil || c.Base == nil {
		return nil
	}
	return c.Base.Done()
}

func WithFrame(frame uint64) Option {
	return func(c *Context) {
		c.Frame = frame
	}
}

func WithSource(source string) Option {
	return func(c *Context) {
		c.Meta.Source = source
	}
}

func WithHandler(handler string) Option {
	return func(c *Context) {
		c.Meta.Handler = handler
	}
}

func WithMeta(meta RequestMeta) Option {
	return func(c *Context) {
		c.SetMeta(meta)
	}
}

func WithTrace(trace TraceMeta) Option {
	return func(c *Context) {
		c.Trace = trace.Clone()
	}
}

func WithPlayerProtocol(playerID int64, msgID uint32, seq uint32) Option {
	return func(c *Context) {
		c.MergeMeta(RequestMeta{
			Source:   "player_protocol",
			PlayerID: playerID,
			MsgID:    msgID,
			Seq:      seq,
		})
	}
}

func (c *Context) SetMeta(meta RequestMeta) {
	if c == nil {
		return
	}
	c.Meta = meta
}

func (c *Context) MergeMeta(meta RequestMeta) {
	if c == nil {
		return
	}
	if meta.Source != "" {
		c.Meta.Source = meta.Source
	}
	if meta.Handler != "" {
		c.Meta.Handler = meta.Handler
	}
	if meta.PlayerID != 0 {
		c.Meta.PlayerID = meta.PlayerID
	}
	if meta.MsgID != 0 {
		c.Meta.MsgID = meta.MsgID
	}
	if meta.Seq != 0 {
		c.Meta.Seq = meta.Seq
	}
}

// Set stores a key-value pair in the context.
func (c *Context) Set(key, value any) {
	if c.keyValue == nil {
		c.keyValue = make(map[any]any)
	}
	c.keyValue[key] = value
}

// Get retrieves a value by key.
func (c *Context) Get(key any) (any, bool) {
	if c.keyValue == nil {
		return nil, false
	}
	v, ok := c.keyValue[key]
	return v, ok
}

func cloneKeyValue(src map[any]any) map[any]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[any]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneTraceTags(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
