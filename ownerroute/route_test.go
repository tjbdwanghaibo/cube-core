package ownerroute

import (
	"context"
	"testing"
)

type testRoute struct {
	ownerSid int32
}

func (r testRoute) OwnerRouteSid() int32 { return r.ownerSid }

type testResolver map[int64]testRoute

func (r testResolver) GetRoute(_ context.Context, key int64) (testRoute, bool, error) {
	route, ok := r[key]
	return route, ok, nil
}

type testCommand struct {
	PlayerID  int64
	SourceSid int32
}

type testTransport struct {
	sid int32
	cmd *testCommand
}

func (t *testTransport) Send(_ context.Context, sid int32, cmd *testCommand) error {
	t.sid = sid
	t.cmd = cmd
	return nil
}

func TestRouterExecutesLocalCommand(t *testing.T) {
	called := false
	router := &Router[testCommand, int64, testRoute]{
		LocalSid: 1,
		Routes:   testResolver{7: {ownerSid: 1}},
		KeyOf:    func(cmd *testCommand) int64 { return cmd.PlayerID },
		Prepare:  func(cmd *testCommand, sid int32) { cmd.SourceSid = sid },
		Executor: func(_ context.Context, cmd *testCommand) error {
			called = cmd.SourceSid == 1
			return nil
		},
	}
	if err := router.Route(context.Background(), &testCommand{PlayerID: 7}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected local executor")
	}
}

func TestRouterSendsRemoteCommand(t *testing.T) {
	transport := &testTransport{}
	router := &Router[testCommand, int64, testRoute]{
		LocalSid:  1,
		Routes:    testResolver{7: {ownerSid: 2}},
		Transport: transport,
		KeyOf:     func(cmd *testCommand) int64 { return cmd.PlayerID },
	}
	if err := router.Route(context.Background(), &testCommand{PlayerID: 7}); err != nil {
		t.Fatal(err)
	}
	if transport.sid != 2 || transport.cmd == nil || transport.cmd.PlayerID != 7 {
		t.Fatalf("transport = %+v", transport)
	}
}
