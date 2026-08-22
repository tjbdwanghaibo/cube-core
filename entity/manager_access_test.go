package entity

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerAccessPreservesOrderAndMissingEntries(t *testing.T) {
	manager := NewEntityManager()
	first := newMgrTestEntity(1001, testEntityCategoryPlayer)
	second := newMgrTestEntity(1002, testEntityCategoryPlayer)
	manager.Add(first)
	manager.Add(second)
	access := NewManagerAccess(manager)
	values, err := access.GetMany(context.Background(), []int64{1002, 9999, 1001}, []EntityCategory{
		testEntityCategoryPlayer, testEntityCategoryPlayer, testEntityCategoryPlayer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[0] != second || values[1] != nil || values[2] != first {
		t.Fatalf("unexpected ordered lookup: %+v", values)
	}
}

type concurrentAggregateLoader struct {
	manager *EntityManager
	active  atomic.Int32
	max     atomic.Int32
}

func (l *concurrentAggregateLoader) LoadEntity(_ context.Context, id int64, _ EntityKind) (IThreadSafeEntity, error) {
	active := l.active.Add(1)
	for current := l.max.Load(); active > current && !l.max.CompareAndSwap(current, active); current = l.max.Load() {
	}
	time.Sleep(10 * time.Millisecond)
	value := newMgrTestEntity(id, testEntityCategoryPlayer)
	if err := l.manager.TryAdd(value); err != nil {
		l.active.Add(-1)
		return nil, err
	}
	l.active.Add(-1)
	return value, nil
}

func TestManagerAccessGetManyLoadsColdEntitiesConcurrently(t *testing.T) {
	manager := NewEntityManager()
	access := NewManagerAccess(manager)
	access.ConfigureLoadConcurrency(4)
	loader := &concurrentAggregateLoader{manager: manager}
	if _, err := access.ConfigureLoader(loader); err != nil {
		t.Fatal(err)
	}
	ids := []int64{2001, 2002, 2003, 2004}
	values, err := access.GetMany(context.Background(), ids, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != len(ids) || loader.max.Load() < 2 {
		t.Fatalf("cold load did not run concurrently: values=%d max=%d", len(values), loader.max.Load())
	}
}
