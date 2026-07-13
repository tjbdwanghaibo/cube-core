package entity

import (
	"context"
	"sync"
)

type OnDemandEntityLoadFunc func(ctx context.Context, id int64, category EntityCategory, kind EntityKind) (IThreadSafeEntity, bool, error)

var onDemandEntityLoader struct {
	mu  sync.RWMutex
	fn  OnDemandEntityLoadFunc
	id  uint64
	seq uint64
}

func SetOnDemandEntityLoader(fn OnDemandEntityLoadFunc) func() {
	onDemandEntityLoader.mu.Lock()
	prev := onDemandEntityLoader.fn
	prevID := onDemandEntityLoader.id
	onDemandEntityLoader.seq++
	id := onDemandEntityLoader.seq
	onDemandEntityLoader.fn = fn
	onDemandEntityLoader.id = id
	onDemandEntityLoader.mu.Unlock()
	return func() {
		onDemandEntityLoader.mu.Lock()
		if onDemandEntityLoader.id == id {
			onDemandEntityLoader.fn = prev
			onDemandEntityLoader.id = prevID
		}
		onDemandEntityLoader.mu.Unlock()
	}
}

func LoadEntityOnDemand(ctx context.Context, id int64, category EntityCategory, kind EntityKind) (IThreadSafeEntity, bool, error) {
	onDemandEntityLoader.mu.RLock()
	fn := onDemandEntityLoader.fn
	onDemandEntityLoader.mu.RUnlock()
	if fn == nil {
		return nil, false, nil
	}
	return fn(ctx, id, category, kind)
}
