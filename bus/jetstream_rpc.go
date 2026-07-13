package bus

import (
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	fnats "github.com/tjbdwanghaibo/cube-core/nats"
	"github.com/tjbdwanghaibo/cube-core/obs"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultJetStreamRPCRequestStream  = "CUBE_RPC_REQUESTS"
	defaultJetStreamRPCResponseStream = "CUBE_RPC_RESPONSES"
	defaultJetStreamRPCAckWait        = 10 * time.Second
	defaultJetStreamRPCMaxDeliver     = 5
	defaultJetStreamRPCRequestTTL     = 30 * time.Second
	defaultJetStreamRPCCallTimeout    = 5 * time.Second
	defaultJetStreamRPCStreamMaxAge   = 30 * time.Minute
	defaultJetStreamRPCSetupTimeout   = 5 * time.Second
)

var (
	ErrJetStreamRPCUnavailable = errors.New("bus: jetstream rpc unavailable")
	ErrJetStreamRPCExpired     = errors.New("bus: jetstream rpc request expired")
)

type JetStreamRPCConfig struct {
	RequestStream  string
	ResponseStream string
	Storage        fnats.JetStreamStorage
	AckWait        time.Duration
	MaxDeliver     int
	RequestTTL     time.Duration
	CallTimeout    time.Duration
	StreamMaxAge   time.Duration
	Duplicates     time.Duration
	Replicas       int
	MaxBytes       int64
	SetupTimeout   time.Duration
}

func (c JetStreamRPCConfig) normalize() JetStreamRPCConfig {
	if c.RequestStream == "" {
		c.RequestStream = defaultJetStreamRPCRequestStream
	}
	if c.ResponseStream == "" {
		c.ResponseStream = defaultJetStreamRPCResponseStream
	}
	if c.Storage == "" {
		c.Storage = fnats.JetStreamStorageFile
	}
	if c.AckWait <= 0 {
		c.AckWait = defaultJetStreamRPCAckWait
	}
	if c.MaxDeliver <= 0 {
		c.MaxDeliver = defaultJetStreamRPCMaxDeliver
	}
	if c.RequestTTL <= 0 {
		c.RequestTTL = defaultJetStreamRPCRequestTTL
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = defaultJetStreamRPCCallTimeout
	}
	if c.StreamMaxAge <= 0 {
		c.StreamMaxAge = defaultJetStreamRPCStreamMaxAge
	}
	if c.Duplicates <= 0 {
		c.Duplicates = c.RequestTTL
	}
	if c.SetupTimeout <= 0 {
		c.SetupTimeout = defaultJetStreamRPCSetupTimeout
	}
	return c
}

type jetStreamRPC struct {
	js              fnats.IJetStream
	cfg             JetStreamRPCConfig
	pending         sync.Map // request id -> *pendingJetStreamRPCCall
	pendingCount    atomic.Int64
	pendingByMethod sync.Map // method -> *atomic.Int64
	seq             atomic.Uint64

	mu   sync.Mutex
	subs []fnats.IJetStreamSubscription
}

type pendingJetStreamRPCCall struct {
	resp chan []byte
}

func (b *Bus) EnableJetStreamRPC(js fnats.IJetStream, cfg JetStreamRPCConfig) error {
	if b == nil {
		return ErrJetStreamRPCUnavailable
	}
	if js == nil {
		return ErrJetStreamRPCUnavailable
	}
	cfg = cfg.normalize()
	rpc := &jetStreamRPC{js: js, cfg: cfg}
	b.jsRPC = rpc
	ctx, cancel := context.WithTimeout(context.Background(), cfg.SetupTimeout)
	defer cancel()
	if err := b.ensureJetStreamRPCStreams(ctx); err != nil {
		b.jsRPC = nil
		return err
	}
	if err := b.subscribeJetStreamRPCResponses(ctx); err != nil {
		b.jsRPC = nil
		return err
	}
	return nil
}

func (b *Bus) jetStreamRPCEnabled() bool {
	return b != nil && b.jsRPC != nil && b.jsRPC.js != nil
}

func (b *Bus) ensureJetStreamRPCStreams(ctx context.Context) error {
	if !b.jetStreamRPCEnabled() {
		return ErrJetStreamRPCUnavailable
	}
	cfg := b.jsRPC.cfg
	requestSubjects := []string{fmt.Sprintf("%s.rpc.>", b.cfg.Prefix)}
	responseSubjects := []string{fmt.Sprintf("%s.rpc_resp.>", b.cfg.Prefix)}
	if err := b.jsRPC.js.EnsureStream(ctx, fnats.JetStreamConfig{
		Name:       cfg.RequestStream,
		Subjects:   requestSubjects,
		Storage:    cfg.Storage,
		MaxAge:     cfg.StreamMaxAge,
		Duplicates: cfg.Duplicates,
		Replicas:   cfg.Replicas,
		MaxBytes:   cfg.MaxBytes,
	}); err != nil {
		return fmt.Errorf("bus: ensure jetstream rpc request stream: %w", err)
	}
	if err := b.jsRPC.js.EnsureStream(ctx, fnats.JetStreamConfig{
		Name:       cfg.ResponseStream,
		Subjects:   responseSubjects,
		Storage:    cfg.Storage,
		MaxAge:     cfg.StreamMaxAge,
		Duplicates: cfg.Duplicates,
		Replicas:   cfg.Replicas,
		MaxBytes:   cfg.MaxBytes,
	}); err != nil {
		return fmt.Errorf("bus: ensure jetstream rpc response stream: %w", err)
	}
	return nil
}

func (b *Bus) subscribeJetStreamRPCResponses(ctx context.Context) error {
	if !b.jetStreamRPCEnabled() {
		return ErrJetStreamRPCUnavailable
	}
	cfg := b.jsRPC.cfg
	name := sanitizeJetStreamRPCName(fmt.Sprintf("rpc_resp_%s_%d", b.cfg.SvcType, b.cfg.Sid))
	sub, err := b.jsRPC.js.Subscribe(ctx, fnats.JetStreamConsumerConfig{
		Stream:        cfg.ResponseStream,
		Name:          name,
		Durable:       name,
		FilterSubject: b.subject.RpcResponseInbox(b.cfg.Sid),
		DeliverPolicy: fnats.JetStreamDeliverAll,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
	}, b.onJetStreamRPCResponse)
	if err != nil {
		return fmt.Errorf("bus: subscribe jetstream rpc responses: %w", err)
	}
	b.addJetStreamRPCSubscription(sub)
	return nil
}

func (b *Bus) subscribeJetStreamRPCMethod(method string) error {
	if !b.jetStreamRPCEnabled() {
		return ErrJetStreamRPCUnavailable
	}
	cfg := b.jsRPC.cfg
	ctx, cancel := context.WithTimeout(context.Background(), cfg.SetupTimeout)
	defer cancel()

	serviceName := sanitizeJetStreamRPCName(fmt.Sprintf("rpc_%s_%s", b.cfg.SvcType, method))
	serviceSub, err := b.jsRPC.js.Subscribe(ctx, fnats.JetStreamConsumerConfig{
		Stream:        cfg.RequestStream,
		Name:          serviceName,
		Durable:       serviceName,
		FilterSubject: b.subject.Rpc(b.cfg.SvcType, method),
		DeliverPolicy: fnats.JetStreamDeliverAll,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
	}, b.onJetStreamRPCRequest)
	if err != nil {
		return fmt.Errorf("bus: subscribe jetstream service rpc %s: %w", method, err)
	}

	instanceName := sanitizeJetStreamRPCName(fmt.Sprintf("rpc_%s_%d_%s", b.cfg.SvcType, b.cfg.Sid, method))
	instanceSub, err := b.jsRPC.js.Subscribe(ctx, fnats.JetStreamConsumerConfig{
		Stream:        cfg.RequestStream,
		Name:          instanceName,
		Durable:       instanceName,
		FilterSubject: b.subject.RpcInstance(b.cfg.SvcType, b.cfg.Sid, method),
		DeliverPolicy: fnats.JetStreamDeliverAll,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
	}, b.onJetStreamRPCRequest)
	if err != nil {
		serviceSub.Stop()
		return fmt.Errorf("bus: subscribe jetstream instance rpc %s: %w", method, err)
	}
	b.addJetStreamRPCSubscription(serviceSub)
	b.addJetStreamRPCSubscription(instanceSub)
	return nil
}

func (b *Bus) addJetStreamRPCSubscription(sub fnats.IJetStreamSubscription) {
	if !b.jetStreamRPCEnabled() || sub == nil {
		return
	}
	b.jsRPC.mu.Lock()
	defer b.jsRPC.mu.Unlock()
	b.jsRPC.subs = append(b.jsRPC.subs, sub)
}

func (b *Bus) stopJetStreamRPCSubscriptions() {
	if !b.jetStreamRPCEnabled() {
		return
	}
	b.jsRPC.mu.Lock()
	subs := append([]fnats.IJetStreamSubscription(nil), b.jsRPC.subs...)
	b.jsRPC.subs = nil
	b.jsRPC.mu.Unlock()
	for _, sub := range subs {
		if sub != nil {
			sub.Stop()
		}
	}
	b.jsRPC.pending.Range(func(key, value any) bool {
		b.jsRPC.pending.Delete(key)
		if pending, ok := value.(*pendingJetStreamRPCCall); ok && pending != nil {
			close(pending.resp)
		}
		return true
	})
}

func (b *Bus) callJetStreamRPC(ctx context.Context, subject string, method string, toSid int32, payload []byte) ([]byte, error) {
	if !b.jetStreamRPCEnabled() {
		return nil, ErrJetStreamRPCUnavailable
	}
	callCtx, cancel := b.jetStreamRPCCallContext(ctx)
	defer cancel()

	reqID := b.nextJetStreamRPCRequestID()
	pending := &pendingJetStreamRPCCall{resp: make(chan []byte, 1)}
	b.jsRPC.pending.Store(reqID, pending)
	b.addJetStreamRPCPending(method, 1)
	defer func() {
		b.jsRPC.pending.Delete(reqID)
		b.addJetStreamRPCPending(method, -1)
	}()

	now := time.Now()
	deadlineAt := now.Add(b.jsRPC.cfg.RequestTTL).UnixMilli()
	if deadline, ok := callCtx.Deadline(); ok {
		deadlineAt = deadline.UnixMilli()
	}
	req := &fnats.NatsMsg{
		FromSid:      b.cfg.Sid,
		ToSid:        toSid,
		MsgName:      method,
		Payload:      payload,
		SessionId:    reqID,
		MsgID:        reqID,
		Attempt:      1,
		CreatedAt:    now.UnixMilli(),
		ReplySubject: b.subject.RpcResponse(b.cfg.Sid, reqID),
		DeadlineAt:   deadlineAt,
	}
	data, err := b.codec.Marshal(req)
	if err != nil {
		b.recordJetStreamRPCCall(method, "marshal_error", "marshal")
		return nil, fmt.Errorf("bus: marshal jetstream rpc req: %w", err)
	}
	if _, err := b.jsRPC.js.Publish(callCtx, subject, data, fnats.JetStreamPublishOptions{MsgID: reqID}); err != nil {
		b.recordJetStreamRPCCall(method, "publish_error", "publish")
		return nil, fmt.Errorf("bus: publish jetstream rpc %s: %w", subject, err)
	}
	select {
	case resp, ok := <-pending.resp:
		if !ok {
			b.recordJetStreamRPCCall(method, "cancel", "closed")
			return nil, fnats.ErrCancelled
		}
		b.recordJetStreamRPCCall(method, "ok", "")
		return resp, nil
	case <-callCtx.Done():
		err := jetStreamRPCContextError(callCtx)
		result := "cancel"
		reason := "cancel"
		if errors.Is(err, fnats.ErrTimeout) {
			result = "timeout"
			reason = "timeout"
		}
		b.recordJetStreamRPCCall(method, result, reason)
		return nil, err
	}
}

func (b *Bus) callJetStreamRPCAsync(subject string, method string, payload []byte, cb func(resp []byte, err error)) {
	go func() {
		ctx, cancel := context.WithTimeout(fctx.BaseContext(), b.jsRPC.cfg.CallTimeout)
		defer cancel()
		resp, err := b.callJetStreamRPC(ctx, subject, method, 0, payload)
		cb(resp, err)
	}()
}

func (b *Bus) jetStreamRPCCallContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.jsRPC.cfg.CallTimeout)
}

func (b *Bus) onJetStreamRPCResponse(_ context.Context, msg *fnats.JetStreamMsg) error {
	if msg == nil {
		return nil
	}
	requestID := b.extractJetStreamRPCResponseID(msg.Subject)
	if requestID == "" {
		slog.Warn("bus: drop jetstream rpc response without request id", "subject", msg.Subject)
		return nil
	}
	value, ok := b.jsRPC.pending.LoadAndDelete(requestID)
	if !ok {
		slog.Debug("bus: drop stale jetstream rpc response", "request_id", requestID, "subject", msg.Subject)
		return nil
	}
	pending, ok := value.(*pendingJetStreamRPCCall)
	if !ok || pending == nil {
		return nil
	}
	select {
	case pending.resp <- append([]byte(nil), msg.Data...):
	default:
		slog.Warn("bus: drop jetstream rpc response because pending channel is full", "request_id", requestID, "subject", msg.Subject)
	}
	return nil
}

func (b *Bus) onJetStreamRPCRequest(ctx context.Context, msg *fnats.JetStreamMsg) error {
	if msg == nil {
		return nil
	}
	var req fnats.NatsMsg
	if err := b.codec.Unmarshal(msg.Data, &req); err != nil {
		return fmt.Errorf("bus: decode jetstream rpc req: %w", err)
	}
	if req.MsgName == "" {
		req.MsgName = b.extractRpcMethod(msg.Subject)
	}
	if msg.NumDelivered > 0 {
		obs.SetGauge("bus_rpc_consumer_delivery", obs.Labels{
			"transport": "jetstream",
			"method":    req.MsgName,
		}, int64(msg.NumDelivered))
	}
	if req.DeadlineAt > 0 && time.Now().UnixMilli() > req.DeadlineAt {
		slog.Warn("bus: drop expired jetstream rpc request", "method", req.MsgName, "request_id", req.SessionId)
		b.recordJetStreamRPCRequest(req.MsgName, "expired")
		return nil
	}
	processCtx, cancel := b.jetStreamRPCProcessContext(req)
	defer cancel()

	b.mu.RLock()
	handler, ok := b.rpcHandlers[req.MsgName]
	b.mu.RUnlock()
	if !ok {
		slog.Warn("bus: no jetstream rpc handler", "method", req.MsgName)
		b.recordJetStreamRPCRequest(req.MsgName, "no_handler")
		data, err := b.codec.Marshal(rpcErrorResponse(fmt.Errorf("%w: %s", ErrNoHandler, req.MsgName)))
		if err != nil {
			return err
		}
		return b.publishJetStreamRPCResponse(processCtx, req.ReplySubject, req.SessionId, data)
	}

	rpcCtx := &RpcContext{
		MsgContext: MsgContext{
			FromSid:   req.FromSid,
			ToModule:  req.ToModule,
			MsgName:   req.MsgName,
			MsgID:     req.MsgID,
			Attempt:   req.Attempt,
			CreatedAt: req.CreatedAt,
			Payload:   req.Payload,
			base:      processCtx,
			codec:     b.codec,
		},
		Method:       req.MsgName,
		ReplySubject: req.ReplySubject,
	}

	_, release := fctx.NewContext(fctx.WithBase(rpcCtx.Context()), fctx.WithSource("bus_jsrpc"), fctx.WithHandler(req.MsgName))
	defer release()
	var resp any
	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = fmt.Errorf("panic: %v", r)
				slog.Error("bus: jetstream rpc handler panic", "method", req.MsgName, "err", r, "stack", string(debug.Stack()))
			}
		}()
		resp, handlerErr = handler(rpcCtx)
	}()

	var data []byte
	var err error
	if handlerErr != nil {
		data, err = b.codec.Marshal(rpcErrorResponse(handlerErr))
	} else {
		data, err = b.codec.Marshal(resp)
	}
	if err != nil {
		b.recordJetStreamRPCRequest(req.MsgName, "marshal_error")
		return fmt.Errorf("bus: marshal jetstream rpc resp %s: %w", req.MsgName, err)
	}
	if err := b.publishJetStreamRPCResponse(processCtx, req.ReplySubject, req.SessionId, data); err != nil {
		b.recordJetStreamRPCRequest(req.MsgName, "publish_error")
		return err
	}
	if handlerErr != nil {
		b.recordJetStreamRPCRequest(req.MsgName, "handler_error")
	} else {
		b.recordJetStreamRPCRequest(req.MsgName, "ok")
	}
	return nil
}

func (b *Bus) jetStreamRPCProcessContext(req fnats.NatsMsg) (context.Context, context.CancelFunc) {
	base := b.baseContext()
	if req.DeadlineAt <= 0 {
		return base, func() {}
	}
	deadline := time.UnixMilli(req.DeadlineAt)
	if time.Now().After(deadline) {
		ctx, cancel := context.WithCancel(base)
		cancel()
		return ctx, func() {}
	}
	return context.WithDeadline(base, deadline)
}

func (b *Bus) publishJetStreamRPCResponse(ctx context.Context, replySubject string, requestID string, data []byte) error {
	if strings.TrimSpace(replySubject) == "" {
		return nil
	}
	if requestID == "" {
		requestID = b.nextJetStreamRPCRequestID()
	}
	_, err := b.jsRPC.js.Publish(ctx, replySubject, data, fnats.JetStreamPublishOptions{MsgID: requestID})
	if err != nil {
		return fmt.Errorf("bus: publish jetstream rpc response %s: %w", replySubject, err)
	}
	return nil
}

func (b *Bus) nextJetStreamRPCRequestID() string {
	seq := b.jsRPC.seq.Add(1)
	service := sanitizeJetStreamRPCName(b.cfg.SvcType)
	if service == "" {
		service = "svc"
	}
	return fmt.Sprintf("%s-%d-%d-%d", service, b.cfg.Sid, time.Now().UnixNano(), seq)
}

func (b *Bus) extractJetStreamRPCResponseID(subject string) string {
	prefix := b.subject.RpcResponse(b.cfg.Sid, "")
	if strings.HasPrefix(subject, prefix) {
		return strings.TrimPrefix(subject, prefix)
	}
	idx := strings.LastIndexByte(subject, '.')
	if idx < 0 || idx+1 >= len(subject) {
		return ""
	}
	return subject[idx+1:]
}

func sanitizeJetStreamRPCName(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	b.Grow(len(s))
	lastUnderscore := false
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if ok {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if len(out) > 200 {
		out = out[:200]
	}
	return out
}

func (b *Bus) addJetStreamRPCPending(method string, delta int64) {
	if b == nil || b.jsRPC == nil {
		return
	}
	total := b.jsRPC.pendingCount.Add(delta)
	if total < 0 {
		total = 0
		b.jsRPC.pendingCount.Store(0)
	}
	methodCounter := b.jetStreamRPCMethodPendingCounter(method)
	methodValue := methodCounter.Add(delta)
	if methodValue < 0 {
		methodValue = 0
		methodCounter.Store(0)
	}
	obs.SetGauge("bus_rpc_pending_total", obs.Labels{
		"transport": "jetstream",
	}, total)
	obs.SetGauge("bus_rpc_pending", obs.Labels{
		"transport": "jetstream",
		"method":    method,
	}, methodValue)
}

func (b *Bus) jetStreamRPCMethodPendingCounter(method string) *atomic.Int64 {
	if b == nil || b.jsRPC == nil {
		return &atomic.Int64{}
	}
	value, _ := b.jsRPC.pendingByMethod.LoadOrStore(method, &atomic.Int64{})
	counter, ok := value.(*atomic.Int64)
	if !ok || counter == nil {
		counter = &atomic.Int64{}
		b.jsRPC.pendingByMethod.Store(method, counter)
	}
	return counter
}

func (b *Bus) recordJetStreamRPCCall(method string, result string, reason string) {
	labels := obs.Labels{
		"transport": "jetstream",
		"method":    method,
		"result":    result,
	}
	if reason != "" {
		labels["reason"] = reason
	}
	obs.IncCounter("bus_rpc_call_total", labels, 1)
}

func (b *Bus) recordJetStreamRPCRequest(method string, result string) {
	obs.IncCounter("bus_rpc_request_total", obs.Labels{
		"transport": "jetstream",
		"method":    method,
		"result":    result,
	}, 1)
}

func jetStreamRPCContextError(ctx context.Context) error {
	if ctx == nil {
		return fnats.ErrCancelled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fnats.ErrTimeout
	}
	return fnats.ErrCancelled
}
