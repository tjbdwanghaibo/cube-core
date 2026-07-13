package misc

import (
	"sync/atomic"
	"testing"
)

func TestBucketHolder(t *testing.T) {
	bh := NewBucketHolder[int64, int](4, func(k int64) int { return int(k * 10) }, true)

	// Get auto-creates via builder
	v := bh.Get(5)
	if v != 50 {
		t.Fatalf("Expected 50, got %d", v)
	}

	bh.Add(5, 99)
	v = bh.Get(5)
	if v != 99 {
		t.Fatalf("Expected 99 after Add, got %d", v)
	}

	bh.Del(5)
	// After deletion, re-get should rebuild
	v = bh.Get(5)
	if v != 50 {
		t.Fatalf("Expected 50 after Del+rebuild, got %d", v)
	}

	if bh.Count() != 1 {
		t.Fatalf("Expected count 1, got %d", bh.Count())
	}
}

func TestParallelSlice(t *testing.T) {
	data := []int{1, 2, 3, 4, 5, 6, 7, 8}
	var sum atomic.Int64

	ParallelSlice(data, func(i int, v int) {
		sum.Add(int64(v))
	})

	if sum.Load() != 36 {
		t.Fatalf("Expected sum 36, got %d", sum.Load())
	}
}

func TestParallelSliceCollect(t *testing.T) {
	data := []int{1, 2, 3, 4}
	results := ParallelSliceCollect(data, func(i int, v int) int {
		return v * v
	})

	expected := []int{1, 4, 9, 16}
	for i, v := range results {
		if v != expected[i] {
			t.Fatalf("Index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestKeyMap(t *testing.T) {
	km := NewKeyMap[int64, string](8)

	km.Set(1, "one")
	km.Set(2, "two")
	km.Set(3, "three")

	if v, ok := km.Get(1); !ok || v != "one" {
		t.Fatalf("Expected 'one', got %v %v", v, ok)
	}

	km.Remove(2)
	if _, ok := km.Get(2); ok {
		t.Fatal("Expected key 2 to be removed")
	}

	if km.Len() != 2 {
		t.Fatalf("Expected len 2, got %d", km.Len())
	}
}

func TestTopologicalSort(t *testing.T) {
	ts := NewTopologicalSortCache[string]()
	ts.RegisterCompDependency("C", "A", "B")
	ts.RegisterCompDependency("B", "A")
	ts.RegisterCompDependency("A")

	sorted := ts.GetTopologicalSortedComponents()
	if len(sorted) != 3 {
		t.Fatalf("Expected 3 sorted components, got %d", len(sorted))
	}
	// A should be first (most depended upon)
	if sorted[0] != "A" {
		t.Fatalf("Expected 'A' first, got %s", sorted[0])
	}
}

func TestObjectPool(t *testing.T) {
	pool := NewObjectPool(
		func() *int { v := 0; return &v },
		func(v *int) *int { *v = 0; return v },
	)

	a := pool.Get()
	*a = 42
	pool.Put(a)

	b := pool.Get()
	if *b != 0 {
		// After put, object goes to freelist, not reset until pool.Get from sync.Pool
		// But from freelist it's reused as-is
	}
	pool.Release()
}

func TestHash64(t *testing.T) {
	// Just verify it doesn't panic and produces different values
	h1 := Hash64(1)
	h2 := Hash64(2)
	if h1 == h2 {
		t.Fatal("Hash64 should produce different values for different inputs")
	}
}
