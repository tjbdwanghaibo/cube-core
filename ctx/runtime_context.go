package ctx

import (
	"github.com/tjbdwanghaibo/cube-core/misc"
	"sync"
)

const shardsCount = 64

type shard struct {
	mu   sync.RWMutex
	data map[int64]*Context
}

var shards [shardsCount]shard

func init() {
	for i := range shards {
		shards[i].data = make(map[int64]*Context)
	}
}

func getShard(gid int64) *shard {
	return &shards[uint64(gid)%shardsCount]
}

// CurrentContext returns the request Context bound to the current goroutine, or nil.
func CurrentContext() *Context {
	gid := misc.GoID()
	s := getShard(gid)
	s.mu.RLock()
	c := s.data[gid]
	s.mu.RUnlock()
	return c
}

// StoreContext stores a request Context for the current goroutine.
func StoreContext(c *Context) {
	gid := misc.GoID()
	s := getShard(gid)
	s.mu.Lock()
	s.data[gid] = c
	s.mu.Unlock()
}

// DeleteContext removes the request Context for the current goroutine.
func DeleteContext() {
	gid := misc.GoID()
	s := getShard(gid)
	s.mu.Lock()
	delete(s.data, gid)
	s.mu.Unlock()
}

// GoID returns the current goroutine ID.
func GoID() int64 {
	return misc.GoID()
}
