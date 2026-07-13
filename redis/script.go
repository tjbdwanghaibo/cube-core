package redis

import "context"

// IScript represents a preloaded Lua script.
// First execution uses EVAL, subsequent calls use EVALSHA for efficiency.
type IScript interface {
	Run(ctx context.Context, client IRedis, keys []string, args ...any) (any, error)
}
