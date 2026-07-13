package bus

import (
	"context"
	fnats "github.com/tjbdwanghaibo/cube-core/nats"
	"github.com/tjbdwanghaibo/cube-core/obs"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJetStreamRPCPublishesRequestAndDeliversResponse(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })
	js := &captureJetStreamRPC{}
	b := New(nil, nil, nil, Config{Sid: 2001, SvcType: "game"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}

	js.publishHook = func(subject string, data []byte, opts fnats.JetStreamPublishOptions) {
		if subject != "cube.rpc.mail.mail.List" {
			return
		}
		var req fnats.NatsMsg
		if err := b.codec.Unmarshal(data, &req); err != nil {
			t.Errorf("unmarshal request: %v", err)
			return
		}
		if req.SessionId == "" || req.ReplySubject == "" || opts.MsgID != req.SessionId {
			t.Errorf("request envelope missing correlation fields: %+v msgID=%q", req, opts.MsgID)
			return
		}
		if req.MsgName != "mail.List" || req.FromSid != 2001 || req.DeadlineAt <= time.Now().UnixMilli() {
			t.Errorf("request envelope = %+v", req)
			return
		}
		if handler := js.handlerForFilter("cube.rpc_resp.2001.>"); handler != nil {
			_ = handler(context.Background(), &fnats.JetStreamMsg{
				Subject: req.ReplySubject,
				Data:    []byte(`{"code":0}`),
			})
		}
	}

	var resp struct {
		Code int `json:"code"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.CallReliable(ctx, "mail", "mail.List", map[string]int64{"player_id": 1001}, &resp); err != nil {
		t.Fatalf("CallReliable: %v", err)
	}
	if resp.Code != 0 {
		t.Fatalf("resp.Code = %d, want 0", resp.Code)
	}
	if !js.hasStream("CUBE_RPC_REQUESTS") || !js.hasStream("CUBE_RPC_RESPONSES") {
		t.Fatalf("streams were not ensured: %+v", js.streams)
	}
	if got := busMetricValue("bus_rpc_call_total", map[string]string{"transport": "jetstream", "method": "mail.List", "result": "ok"}); got != 1 {
		t.Fatalf("ok call metric = %d, want 1", got)
	}
}

func TestJetStreamRPCRecordsTimeoutAndClearsPendingGauge(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })
	js := &captureJetStreamRPC{}
	b := New(nil, nil, nil, Config{Sid: 2001, SvcType: "game"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{CallTimeout: 10 * time.Millisecond, RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}

	var resp struct{}
	err := b.CallReliable(context.Background(), "mail", "mail.List", map[string]int64{"player_id": 1001}, &resp)

	if !errors.Is(err, fnats.ErrTimeout) {
		t.Fatalf("Call error = %v, want %v", err, fnats.ErrTimeout)
	}
	if got := busMetricValue("bus_rpc_call_total", map[string]string{"transport": "jetstream", "method": "mail.List", "result": "timeout"}); got != 1 {
		t.Fatalf("timeout metric = %d, want 1", got)
	}
	if got := busMetricValue("bus_rpc_pending", map[string]string{"transport": "jetstream", "method": "mail.List"}); got != 0 {
		t.Fatalf("pending gauge = %d, want 0", got)
	}
}

func TestJetStreamRPCPendingGaugeIsPerMethod(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })
	js := &captureJetStreamRPC{}
	b := New(nil, nil, nil, Config{Sid: 2001, SvcType: "game"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{CallTimeout: time.Second, RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}

	blockingPublished := make(chan struct{})
	releaseBlocking := make(chan struct{})
	js.publishHook = func(subject string, data []byte, opts fnats.JetStreamPublishOptions) {
		var req fnats.NatsMsg
		if err := b.codec.Unmarshal(data, &req); err != nil {
			t.Errorf("unmarshal request: %v", err)
			return
		}
		switch req.MsgName {
		case "rank.GetTop":
			close(blockingPublished)
			<-releaseBlocking
			if handler := js.handlerForFilter("cube.rpc_resp.2001.>"); handler != nil {
				_ = handler(context.Background(), &fnats.JetStreamMsg{
					Subject: req.ReplySubject,
					Data:    []byte(`{"code":0}`),
				})
			}
		case "mail.Summary":
			if handler := js.handlerForFilter("cube.rpc_resp.2001.>"); handler != nil {
				_ = handler(context.Background(), &fnats.JetStreamMsg{
					Subject: req.ReplySubject,
					Data:    []byte(`{"code":0}`),
				})
			}
		}
		_ = opts
	}

	blockingDone := make(chan error, 1)
	go func() {
		var resp struct{}
		blockingDone <- b.CallReliable(context.Background(), "rank", "rank.GetTop", map[string]int64{"player_id": 1001}, &resp)
	}()
	<-blockingPublished

	var resp struct{}
	if err := b.CallReliable(context.Background(), "mail", "mail.Summary", map[string]int64{"player_id": 1001}, &resp); err != nil {
		t.Fatalf("mail CallReliable: %v", err)
	}
	if got := busMetricValue("bus_rpc_pending", map[string]string{"transport": "jetstream", "method": "mail.Summary"}); got != 0 {
		t.Fatalf("mail pending gauge = %d, want 0 while rank request is still pending", got)
	}
	if got := busMetricValue("bus_rpc_pending", map[string]string{"transport": "jetstream", "method": "rank.GetTop"}); got != 1 {
		t.Fatalf("rank pending gauge = %d, want 1", got)
	}

	close(releaseBlocking)
	if err := <-blockingDone; err != nil {
		t.Fatalf("rank CallReliable: %v", err)
	}
}

func TestJetStreamRPCHandleRpcPublishesResponseAfterHandler(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })
	js := &captureJetStreamRPC{}
	b := New(nil, nil, nil, Config{Sid: 5001, SvcType: "mail"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}

	if err := b.HandleRpc("mail.List", func(ctx *RpcContext) (any, error) {
		var req struct {
			PlayerID int64 `json:"player_id"`
		}
		if err := ctx.Decode(&req); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if req.PlayerID != 1001 || ctx.Method != "mail.List" || ctx.MsgID != "req-1" {
			t.Fatalf("rpc context = %+v req=%+v", ctx, req)
		}
		return map[string]int{"code": 0}, nil
	}); err != nil {
		t.Fatalf("HandleRpc: %v", err)
	}

	handler := js.handlerForFilter("cube.rpc.mail.mail.List")
	if handler == nil {
		t.Fatalf("service rpc subscription was not registered: %+v", js.consumers)
	}
	payload, err := b.codec.Marshal(map[string]int64{"player_id": 1001})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req, err := b.codec.Marshal(&fnats.NatsMsg{
		FromSid:      2001,
		MsgName:      "mail.List",
		SessionId:    "req-1",
		MsgID:        "req-1",
		ReplySubject: "cube.rpc_resp.2001.req-1",
		DeadlineAt:   time.Now().Add(time.Second).UnixMilli(),
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := handler(context.Background(), &fnats.JetStreamMsg{Subject: "cube.rpc.mail.mail.List", Data: req, NumDelivered: 3}); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	pub := js.lastPublish()
	if pub.subject != "cube.rpc_resp.2001.req-1" || pub.opts.MsgID != "req-1" {
		t.Fatalf("response publish = %+v", pub)
	}
	var resp struct {
		Code int `json:"code"`
	}
	if err := b.codec.Unmarshal(pub.data, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Code != 0 {
		t.Fatalf("response code = %d, want 0", resp.Code)
	}
	if got := busMetricValue("bus_rpc_consumer_delivery", map[string]string{"transport": "jetstream", "method": "mail.List"}); got != 3 {
		t.Fatalf("delivery metric = %d, want 3", got)
	}
}

func TestBusCallStaysLightweightWhenJetStreamRPCEnabled(t *testing.T) {
	js := &captureJetStreamRPC{}
	rpc := &lifecycleRpc{}
	b := New(&lifecycleClient{}, rpc, nil, Config{Sid: 2001, SvcType: "game"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}

	var resp struct{}
	if err := b.Call(context.Background(), "mail", "mail.List", map[string]int64{"player_id": 1001}, &resp); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if rpc.subject != "cube.rpc.mail.mail.List" {
		t.Fatalf("lightweight rpc subject = %q", rpc.subject)
	}
	if pub := js.lastPublish(); pub.subject != "" {
		t.Fatalf("ordinary Call should not publish JetStream request, got %+v", pub)
	}
}

func TestHandleRpcRegistersOnlyJetStreamTransportWhenEnabled(t *testing.T) {
	client := &lifecycleClient{}
	js := &captureJetStreamRPC{}
	b := New(client, &lifecycleRpc{}, nil, Config{Sid: 5001, SvcType: "mail"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}

	if err := b.HandleRpc("mail.List", func(ctx *RpcContext) (any, error) {
		return map[string]int{"code": 0}, nil
	}); err != nil {
		t.Fatalf("HandleRpc: %v", err)
	}

	if len(client.subs) != 0 {
		t.Fatalf("lightweight subscription count = %d, want 0 when JetStream RPC is enabled", len(client.subs))
	}
	if js.handlerForFilter("cube.rpc.mail.mail.List") == nil {
		t.Fatalf("jetstream service rpc subscription was not registered: %+v", js.consumers)
	}
	if js.handlerForFilter("cube.rpc.mail.5001.mail.List") == nil {
		t.Fatalf("jetstream instance rpc subscription was not registered: %+v", js.consumers)
	}
}

func TestJetStreamRPCDropsExpiredRequestWithoutCallingHandler(t *testing.T) {
	js := &captureJetStreamRPC{}
	b := New(nil, nil, nil, Config{Sid: 5001, SvcType: "mail"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}
	called := false
	if err := b.HandleRpc("mail.List", func(ctx *RpcContext) (any, error) {
		called = true
		return map[string]int{"code": 0}, nil
	}); err != nil {
		t.Fatalf("HandleRpc: %v", err)
	}
	handler := js.handlerForFilter("cube.rpc.mail.mail.List")
	if handler == nil {
		t.Fatal("service rpc subscription was not registered")
	}
	req, err := b.codec.Marshal(&fnats.NatsMsg{
		MsgName:      "mail.List",
		SessionId:    "req-expired",
		MsgID:        "req-expired",
		ReplySubject: "cube.rpc_resp.2001.req-expired",
		DeadlineAt:   time.Now().Add(-time.Second).UnixMilli(),
		Payload:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := handler(context.Background(), &fnats.JetStreamMsg{Subject: "cube.rpc.mail.mail.List", Data: req}); err != nil {
		t.Fatalf("expired request should ack/drop, got %v", err)
	}
	if called {
		t.Fatal("expired request should not call business handler")
	}
	if pub := js.lastPublish(); pub.subject != "" {
		t.Fatalf("expired request should not publish response, got %+v", pub)
	}
}

func TestJetStreamRPCRequestIgnoresCanceledSubscribeContext(t *testing.T) {
	js := &captureJetStreamRPC{respectPublishContext: true}
	b := New(nil, nil, nil, Config{Sid: 5001, SvcType: "mail"})
	if err := b.EnableJetStreamRPC(js, JetStreamRPCConfig{RequestTTL: time.Second}); err != nil {
		t.Fatalf("EnableJetStreamRPC: %v", err)
	}
	if err := b.HandleRpc("mail.List", func(ctx *RpcContext) (any, error) {
		return map[string]int{"code": 0}, nil
	}); err != nil {
		t.Fatalf("HandleRpc: %v", err)
	}
	handler := js.handlerForFilter("cube.rpc.mail.mail.List")
	if handler == nil {
		t.Fatal("service rpc subscription was not registered")
	}
	req, err := b.codec.Marshal(&fnats.NatsMsg{
		MsgName:      "mail.List",
		SessionId:    "req-canceled-subscribe-ctx",
		MsgID:        "req-canceled-subscribe-ctx",
		ReplySubject: "cube.rpc_resp.2001.req-canceled-subscribe-ctx",
		DeadlineAt:   time.Now().Add(time.Second).UnixMilli(),
		Payload:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handler(ctx, &fnats.JetStreamMsg{Subject: "cube.rpc.mail.mail.List", Data: req}); err != nil {
		t.Fatalf("handler should not use canceled subscribe context for response publish: %v", err)
	}
	if pub := js.lastPublish(); pub.subject != "cube.rpc_resp.2001.req-canceled-subscribe-ctx" {
		t.Fatalf("response publish = %+v", pub)
	}
}

type captureJetStreamRPC struct {
	mu          sync.Mutex
	streams     []fnats.JetStreamConfig
	consumers   []fnats.JetStreamConsumerConfig
	handlers    []fnats.JetStreamHandler
	publishes   []capturedJetStreamPublish
	publishErr  error
	publishHook func(subject string, data []byte, opts fnats.JetStreamPublishOptions)

	respectPublishContext bool
}

type capturedJetStreamPublish struct {
	subject string
	data    []byte
	opts    fnats.JetStreamPublishOptions
}

func (c *captureJetStreamRPC) EnsureStream(_ context.Context, cfg fnats.JetStreamConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams = append(c.streams, cfg)
	return nil
}

func (c *captureJetStreamRPC) Publish(ctx context.Context, subject string, data []byte, opts fnats.JetStreamPublishOptions) (fnats.JetStreamPublishAck, error) {
	if c.respectPublishContext {
		if err := ctx.Err(); err != nil {
			return fnats.JetStreamPublishAck{}, err
		}
	}
	c.mu.Lock()
	c.publishes = append(c.publishes, capturedJetStreamPublish{subject: subject, data: append([]byte(nil), data...), opts: opts})
	hook := c.publishHook
	err := c.publishErr
	c.mu.Unlock()
	if hook != nil {
		hook(subject, data, opts)
	}
	if err != nil {
		return fnats.JetStreamPublishAck{}, err
	}
	return fnats.JetStreamPublishAck{Stream: "test", Sequence: uint64(len(c.publishes))}, nil
}

func (c *captureJetStreamRPC) Subscribe(_ context.Context, cfg fnats.JetStreamConsumerConfig, handler fnats.JetStreamHandler) (fnats.IJetStreamSubscription, error) {
	if strings.TrimSpace(cfg.FilterSubject) == "" {
		return nil, errors.New("filter subject required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumers = append(c.consumers, cfg)
	c.handlers = append(c.handlers, handler)
	return captureJetStreamSub{}, nil
}

func (c *captureJetStreamRPC) handlerForFilter(filter string) fnats.JetStreamHandler {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, cfg := range c.consumers {
		if cfg.FilterSubject == filter {
			return c.handlers[i]
		}
	}
	return nil
}

func (c *captureJetStreamRPC) hasStream(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cfg := range c.streams {
		if cfg.Name == name {
			return true
		}
	}
	return false
}

func (c *captureJetStreamRPC) lastPublish() capturedJetStreamPublish {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.publishes) == 0 {
		return capturedJetStreamPublish{}
	}
	return c.publishes[len(c.publishes)-1]
}

type captureJetStreamSub struct{}

func (captureJetStreamSub) Stop()  {}
func (captureJetStreamSub) Drain() {}
func (captureJetStreamSub) Closed() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

var _ fnats.IJetStream = (*captureJetStreamRPC)(nil)

func busMetricValue(name string, labels map[string]string) int64 {
	for _, metric := range obs.Snapshot() {
		if metric.Name != name {
			continue
		}
		matched := true
		for key, value := range labels {
			if metric.Labels[key] != value {
				matched = false
				break
			}
		}
		if matched {
			return metric.Value
		}
	}
	return 0
}
