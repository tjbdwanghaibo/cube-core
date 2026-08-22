// Package gateway defines transport-neutral request boundary contracts. It
// deliberately contains no codec, socket or business protocol implementation.
package gateway

import (
	"context"
	"errors"
)

var (
	ErrUnauthenticated = errors.New("gateway: unauthenticated")
	ErrSessionClosed   = errors.New("gateway: session closed")
	ErrInvalidRequest  = errors.New("gateway: invalid request")
)

// Principal is the authenticated identity attached to a session.
type Principal struct {
	PlayerID  int64
	SessionID string
	DeviceID  string
	Claims    map[string]string
}

func (p Principal) Authenticated() bool {
	return p.PlayerID != 0 && p.SessionID != ""
}

// Session is the minimum transport capability exposed to an endpoint.
// Implementations must make Reply and Close safe for concurrent use.
type Session interface {
	Principal() Principal
	Reply(context.Context, any) error
	Close(error) error
}

// Request is the protocol-neutral envelope passed through boundary middleware.
// Payload is owned by the caller and must be treated as immutable.
type Request struct {
	MessageID uint32
	Sequence  uint32
	Payload   any
}

// Endpoint converts one authenticated boundary request into a response. Entity
// access belongs behind a generated Nest sender, not in the endpoint itself.
type Endpoint interface {
	Handle(context.Context, Session, Request) (any, error)
}

type EndpointFunc func(context.Context, Session, Request) (any, error)

func (f EndpointFunc) Handle(ctx context.Context, session Session, request Request) (any, error) {
	return f(ctx, session, request)
}

type Middleware func(Endpoint) Endpoint

// Chain applies middleware in declaration order: the first middleware is the
// outermost request boundary.
func Chain(endpoint Endpoint, middleware ...Middleware) Endpoint {
	if endpoint == nil {
		return EndpointFunc(func(context.Context, Session, Request) (any, error) {
			return nil, ErrInvalidRequest
		})
	}
	for i := len(middleware) - 1; i >= 0; i-- {
		if middleware[i] != nil {
			endpoint = middleware[i](endpoint)
		}
	}
	return endpoint
}

// RequireAuthenticated rejects requests before they can reach a generated
// sender when the session has no stable player identity.
func RequireAuthenticated(next Endpoint) Endpoint {
	return EndpointFunc(func(ctx context.Context, session Session, request Request) (any, error) {
		if ctx == nil {
			ctx = context.Background()
		}
		if session == nil || !session.Principal().Authenticated() {
			return nil, ErrUnauthenticated
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return next.Handle(ctx, session, request)
	})
}
