package app

import "context"

// Service is the main business logic that consumes Mod capabilities.
// Lifecycle: Init → Serve (blocking) → Shutdown.
type Service interface {
	Name() ServiceName
	Init(r *Registry) error             // consume capabilities from registry
	Serve(ctx context.Context) error    // blocking, ctx cancelled on shutdown
	Shutdown(ctx context.Context) error // graceful shutdown with timeout
}
