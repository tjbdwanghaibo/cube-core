package entity

import (
	"context"
	"sync"
)

type OnDemandDaoLoadFunc func(ctx context.Context, collection string, storageID int64, dao DaoInterface) (loaded bool, err error)

var onDemandDaoLoader struct {
	mu  sync.RWMutex
	fn  OnDemandDaoLoadFunc
	id  uint64
	seq uint64
}

func SetOnDemandDaoLoader(fn OnDemandDaoLoadFunc) func() {
	onDemandDaoLoader.mu.Lock()
	prev := onDemandDaoLoader.fn
	prevID := onDemandDaoLoader.id
	onDemandDaoLoader.seq++
	id := onDemandDaoLoader.seq
	onDemandDaoLoader.fn = fn
	onDemandDaoLoader.id = id
	onDemandDaoLoader.mu.Unlock()
	return func() {
		onDemandDaoLoader.mu.Lock()
		if onDemandDaoLoader.id == id {
			onDemandDaoLoader.fn = prev
			onDemandDaoLoader.id = prevID
		}
		onDemandDaoLoader.mu.Unlock()
	}
}

func LoadDaoOnDemand(ctx context.Context, collection string, storageID int64, dao DaoInterface) (bool, error) {
	onDemandDaoLoader.mu.RLock()
	fn := onDemandDaoLoader.fn
	onDemandDaoLoader.mu.RUnlock()
	if fn == nil {
		return false, nil
	}
	return fn(ctx, collection, storageID, dao)
}
