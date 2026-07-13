package bus

import (
	"context"
	fnats "github.com/tjbdwanghaibo/cube-core/nats"
	"fmt"
	"testing"
	"time"
)

type lifecycleClient struct {
	failAt int
	subs   []*lifecycleSub
}

func (c *lifecycleClient) Publish(subject string, data []byte) error { return nil }

func (c *lifecycleClient) Request(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	return nil, nil
}

func (c *lifecycleClient) Subscribe(subject string, handler fnats.MsgHandler) (fnats.ISubscription, error) {
	if c.failAt > 0 && len(c.subs)+1 == c.failAt {
		return nil, fmt.Errorf("subscribe failed")
	}
	sub := &lifecycleSub{valid: true, subject: subject, handler: handler}
	c.subs = append(c.subs, sub)
	return sub, nil
}

func (c *lifecycleClient) QueueSubscribe(subject string, queue string, handler fnats.MsgHandler) (fnats.ISubscription, error) {
	if c.failAt > 0 && len(c.subs)+1 == c.failAt {
		return nil, fmt.Errorf("subscribe failed")
	}
	sub := &lifecycleSub{valid: true, subject: subject, queue: queue, handler: handler}
	c.subs = append(c.subs, sub)
	return sub, nil
}

func (c *lifecycleClient) Drain() error { return nil }

func (c *lifecycleClient) Close() {}

type lifecycleSub struct {
	valid       bool
	unsubscribe int
	subject     string
	queue       string
	handler     fnats.MsgHandler
}

func (s *lifecycleSub) Unsubscribe() error {
	s.valid = false
	s.unsubscribe++
	return nil
}

func (s *lifecycleSub) IsValid() bool { return s.valid }

func TestBusStopBeforeStartIsSafe(t *testing.T) {
	b := New(&lifecycleClient{}, nil, nil, Config{Sid: 1, SvcType: "game"})
	b.Stop()
	b.Stop()
}

func TestBusStartFailureCleansPartialSubscriptions(t *testing.T) {
	client := &lifecycleClient{failAt: 2}
	b := New(client, nil, nil, Config{Sid: 1, SvcType: "game"})

	if err := b.Start(); err == nil {
		t.Fatal("expected start failure")
	}
	if b.pool != nil {
		t.Fatal("expected pool to be cleared after start failure")
	}
	if len(b.subs) != 0 {
		t.Fatalf("expected subscriptions to be cleared, got %d", len(b.subs))
	}
	if len(client.subs) != 1 || client.subs[0].unsubscribe != 1 {
		t.Fatalf("expected partial subscription to be unsubscribed once, got %#v", client.subs)
	}

	b.Stop()
}

func TestHandleRpcReturnsSubscribeError(t *testing.T) {
	client := &lifecycleClient{failAt: 1}
	b := New(client, nil, nil, Config{Sid: 1, SvcType: "rank"})

	if err := b.HandleRpc("rank.GetTop", func(*RpcContext) (any, error) { return nil, nil }); err == nil {
		t.Fatal("expected HandleRpc to return subscribe error")
	}
	if len(b.subs) != 0 {
		t.Fatalf("failed rpc subscription should not be retained, got %d", len(b.subs))
	}
}

func TestHandleRpcSubscribesServiceQueueAndInstanceSubject(t *testing.T) {
	client := &lifecycleClient{}
	b := New(client, nil, nil, Config{Sid: 8001, SvcType: "instance"})

	if err := b.HandleRpc("instance.GetState", func(*RpcContext) (any, error) { return nil, nil }); err != nil {
		t.Fatalf("HandleRpc: %v", err)
	}
	if len(client.subs) != 2 {
		t.Fatalf("subscription count = %d, want 2", len(client.subs))
	}
	if client.subs[0].subject != "cube.rpc.instance.instance.GetState" || client.subs[0].queue != "instance_rpc" {
		t.Fatalf("service rpc subscription = subject:%q queue:%q", client.subs[0].subject, client.subs[0].queue)
	}
	if client.subs[1].subject != "cube.rpc.instance.8001.instance.GetState" || client.subs[1].queue != "" {
		t.Fatalf("instance rpc subscription = subject:%q queue:%q", client.subs[1].subject, client.subs[1].queue)
	}
}

type lifecycleRpc struct {
	replies chan []byte
	subject string
}

func (r *lifecycleRpc) Call(_ context.Context, subject string, _ []byte) ([]byte, error) {
	r.subject = subject
	return []byte(`{}`), nil
}

func (r *lifecycleRpc) CallWithTimeout(string, []byte, time.Duration) ([]byte, error) {
	return nil, nil
}

func (r *lifecycleRpc) CallAsync(string, []byte, fnats.RpcCallback) {}

func (r *lifecycleRpc) Reply(_ string, resp []byte) error {
	r.replies <- resp
	return nil
}

func TestBusRpcRawPayloadUsesFullDottedMethodFromSubject(t *testing.T) {
	rpc := &lifecycleRpc{replies: make(chan []byte, 1)}
	b := New(&lifecycleClient{}, rpc, nil, Config{Sid: 5001, SvcType: "mail", WorkerNum: 1})
	handled := make(chan struct{})
	if err := b.HandleRpc("mail.Summary", func(ctx *RpcContext) (any, error) {
		var req struct {
			PlayerID int64 `json:"player_id"`
		}
		if err := ctx.Decode(&req); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if req.PlayerID != 100 {
			t.Fatalf("PlayerID: %d", req.PlayerID)
		}
		close(handled)
		return map[string]int{"code": 0}, nil
	}); err != nil {
		t.Fatalf("HandleRpc: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop()

	payload, err := b.codec.Marshal(struct {
		PlayerID int64 `json:"player_id"`
	}{PlayerID: 100})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	b.onRpcMessage(&fnats.Msg{
		Subject: "cube.rpc.mail.mail.Summary",
		Reply:   "reply",
		Data:    payload,
	})

	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("rpc handler was not called")
	}
	select {
	case resp := <-rpc.replies:
		if len(resp) == 0 {
			t.Fatal("rpc reply should not be empty")
		}
	case <-time.After(time.Second):
		t.Fatal("rpc reply was not sent")
	}
}

func TestBusCallToUsesInstanceRpcSubject(t *testing.T) {
	rpc := &lifecycleRpc{}
	b := New(&lifecycleClient{}, rpc, nil, Config{Sid: 2001, SvcType: "game"})

	var resp struct{}
	if err := b.CallTo(context.Background(), "instance", 8001, "instance.GetState", map[string]int{"id": 1}, &resp); err != nil {
		t.Fatalf("CallTo: %v", err)
	}
	if rpc.subject != "cube.rpc.instance.8001.instance.GetState" {
		t.Fatalf("rpc subject = %q", rpc.subject)
	}
}
