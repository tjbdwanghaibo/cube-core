package gateway

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type testSession struct{ principal Principal }

func (s *testSession) Principal() Principal             { return s.principal }
func (s *testSession) Reply(context.Context, any) error { return nil }
func (s *testSession) Close(error) error                { return nil }

func TestChainOrderAndAuthentication(t *testing.T) {
	var order []string
	middleware := func(name string) Middleware {
		return func(next Endpoint) Endpoint {
			return EndpointFunc(func(ctx context.Context, session Session, request Request) (any, error) {
				order = append(order, name+":before")
				ret, err := next.Handle(ctx, session, request)
				order = append(order, name+":after")
				return ret, err
			})
		}
	}
	endpoint := Chain(EndpointFunc(func(context.Context, Session, Request) (any, error) {
		order = append(order, "handler")
		return "ok", nil
	}), RequireAuthenticated, middleware("a"), middleware("b"))

	if _, err := endpoint.Handle(context.Background(), &testSession{}, Request{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unauthenticated error = %v", err)
	}
	order = nil
	ret, err := endpoint.Handle(context.Background(), &testSession{principal: Principal{PlayerID: 7, SessionID: "s"}}, Request{})
	if err != nil || ret != "ok" {
		t.Fatalf("ret=%v err=%v", ret, err)
	}
	want := []string{"a:before", "b:before", "handler", "b:after", "a:after"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
}
