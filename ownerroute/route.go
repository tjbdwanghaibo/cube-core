package ownerroute

import (
	"context"
	"errors"
	"fmt"
)

var ErrRouteNotFound = errors.New("ownerroute: route not found")

type OwnerRoute interface {
	OwnerRouteSid() int32
}

type Resolver[K comparable, R OwnerRoute] interface {
	GetRoute(ctx context.Context, key K) (R, bool, error)
}

type Transport[C any] interface {
	Send(ctx context.Context, sid int32, cmd *C) error
}

type Router[C any, K comparable, R OwnerRoute] struct {
	LocalSid            int32
	Routes              Resolver[K, R]
	Transport           Transport[C]
	Executor            func(context.Context, *C) error
	KeyOf               func(*C) K
	ValidKey            func(K) bool
	Prepare             func(*C, int32)
	ErrRouteNotFound    error
	ErrExecutorNil      error
	ErrTransportNil     error
	ErrRouteResolverNil error
	ErrInvalidCommand   error
}

func (r *Router[C, K, R]) Route(ctx context.Context, cmd *C) error {
	if r == nil {
		return fmt.Errorf("ownerroute: router is nil")
	}
	if cmd == nil || r.KeyOf == nil {
		return firstErr(r.ErrInvalidCommand, fmt.Errorf("ownerroute: invalid command"))
	}
	if r.Prepare != nil {
		r.Prepare(cmd, r.LocalSid)
	}
	if r.Routes == nil {
		return firstErr(r.ErrRouteResolverNil, fmt.Errorf("ownerroute: route resolver is nil"))
	}
	key := r.KeyOf(cmd)
	if r.ValidKey != nil && !r.ValidKey(key) {
		return firstErr(r.ErrInvalidCommand, fmt.Errorf("ownerroute: invalid command"))
	}
	route, ok, err := r.Routes.GetRoute(ctx, key)
	if err != nil {
		return err
	}
	if !ok || route.OwnerRouteSid() == 0 {
		return firstErr(r.ErrRouteNotFound, ErrRouteNotFound)
	}
	if route.OwnerRouteSid() == r.LocalSid {
		if r.Executor == nil {
			return firstErr(r.ErrExecutorNil, fmt.Errorf("ownerroute: local executor is nil"))
		}
		return r.Executor(ctx, cmd)
	}
	if r.Transport == nil {
		return firstErr(r.ErrTransportNil, fmt.Errorf("ownerroute: transport is nil"))
	}
	return r.Transport.Send(ctx, route.OwnerRouteSid(), cmd)
}

func firstErr(primary error, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}
