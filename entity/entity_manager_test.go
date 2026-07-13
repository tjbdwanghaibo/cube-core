package entity

import (
	"sync"
	"testing"
)

// mgrTestEntity implements IThreadSafeEntity for testing.
type mgrTestEntity struct {
	*EntityBase
	ComponentManager
	DaoManager
}

func (e *mgrTestEntity) Base() *EntityBase { return e.EntityBase }

func newMgrTestEntity(id int64, typo EntityCategory) *mgrTestEntity {
	e := &mgrTestEntity{}
	e.EntityBase = NewEntityBase(id, typo, false)
	e.ComponentManager = NewComponentManager()
	e.DaoManager = NewDaoManager()
	return e
}

func TestEntityManager_AddGet(t *testing.T) {
	mgr := NewEntityManager()

	e1 := newMgrTestEntity(1001, testEntityCategoryPlayer)
	e2 := newMgrTestEntity(1002, testEntityCategoryAlliance)

	mgr.Add(e1)
	mgr.Add(e2)

	if mgr.Len() != 2 {
		t.Fatalf("expected 2 entities, got %d", mgr.Len())
	}

	got := mgr.Get(1001)
	if got != e1 {
		t.Fatal("Get(1001) returned wrong entity")
	}

	got = mgr.Get(9999)
	if got != nil {
		t.Fatal("Get(9999) should return nil")
	}
}

func TestEntityManager_GetWithCategory(t *testing.T) {
	mgr := NewEntityManager()
	e := newMgrTestEntity(1001, testEntityCategoryPlayer)
	mgr.Add(e)

	got := mgr.GetWithCategory(1001, testEntityCategoryPlayer)
	if got != e {
		t.Fatal("GetWithCategory should return entity with matching type")
	}

	got = mgr.GetWithCategory(1001, testEntityCategoryAlliance)
	if got != nil {
		t.Fatal("GetWithCategory should return nil on type mismatch")
	}
}

func TestEntityManager_DuplicatePanics(t *testing.T) {
	mgr := NewEntityManager()
	e1 := newMgrTestEntity(1001, testEntityCategoryPlayer)
	mgr.Add(e1)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate add")
		}
	}()

	e2 := newMgrTestEntity(1001, testEntityCategoryPlayer)
	mgr.Add(e2)
}

func TestEntityManager_Remove(t *testing.T) {
	mgr := NewEntityManager()
	e := newMgrTestEntity(1001, testEntityCategoryPlayer)
	mgr.Add(e)

	mgr.Remove(e, testDestroyCommon, false)

	if mgr.Get(1001) != nil {
		t.Fatal("entity should be removed")
	}
	if !e.IsRemoved() {
		t.Fatal("entity should be marked removed")
	}
	if mgr.Len() != 0 {
		t.Fatalf("expected 0, got %d", mgr.Len())
	}
}

func TestEntityManager_GetMany(t *testing.T) {
	mgr := NewEntityManager()
	e1 := newMgrTestEntity(1, testEntityCategoryPlayer)
	e2 := newMgrTestEntity(2, testEntityCategoryPlayer)
	mgr.Add(e1)
	mgr.Add(e2)

	got := mgr.GetMany([]int64{1, 2, 999})
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestEntityManager_Exists(t *testing.T) {
	mgr := NewEntityManager()
	e := newMgrTestEntity(1001, testEntityCategoryPlayer)
	mgr.Add(e)

	if !mgr.Exists(1001) {
		t.Fatal("should exist")
	}
	if mgr.Exists(9999) {
		t.Fatal("should not exist")
	}
}

func TestEntityManager_RangeByCategory(t *testing.T) {
	mgr := NewEntityManager()
	mgr.Add(newMgrTestEntity(1, testEntityCategoryPlayer))
	mgr.Add(newMgrTestEntity(2, testEntityCategoryPlayer))
	mgr.Add(newMgrTestEntity(3, testEntityCategoryAlliance))

	count := 0
	mgr.RangeByCategory(testEntityCategoryPlayer, func(_ IThreadSafeEntity) bool {
		count++
		return true
	})
	if count != 2 {
		t.Fatalf("expected 2 players, got %d", count)
	}
}

func TestEntityManager_CountByCategory(t *testing.T) {
	mgr := NewEntityManager()
	mgr.Add(newMgrTestEntity(1, testEntityCategoryPlayer))
	mgr.Add(newMgrTestEntity(2, testEntityCategoryPlayer))
	mgr.Add(newMgrTestEntity(3, testEntityCategoryAlliance))

	if mgr.CountByCategory(testEntityCategoryPlayer) != 2 {
		t.Fatal("expected 2 players")
	}
	if mgr.CountByCategory(testEntityCategoryAlliance) != 1 {
		t.Fatal("expected 1 alliance")
	}
}

func TestEntityManager_ConcurrentAccess(t *testing.T) {
	mgr := NewEntityManager()

	var wg sync.WaitGroup
	for i := int64(0); i < 100; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			e := newMgrTestEntity(id, testEntityCategoryPlayer)
			mgr.Add(e)
		}(i)
	}
	wg.Wait()

	if mgr.Len() != 100 {
		t.Fatalf("expected 100, got %d", mgr.Len())
	}

	// Concurrent reads
	wg = sync.WaitGroup{}
	for i := int64(0); i < 100; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			got := mgr.Get(id)
			if got == nil {
				t.Errorf("missing entity %d", id)
			}
		}(i)
	}
	wg.Wait()
}
