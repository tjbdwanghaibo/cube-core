package fmap

import (
	"fmt"
	"sync"
	"testing"
)

func TestSmallSafeMapContract(t *testing.T) {
	exerciseMap(t, NewSmallSafeMap[int64, string](2))
}

func TestShardedSafeMapContract(t *testing.T) {
	exerciseMap(t, NewInt64ShardedSafeMap[string](4))
}

func TestFastMapContract(t *testing.T) {
	exerciseMap(t, NewInt64FastMap[string](2))
}

func exerciseMap(t *testing.T, m IMap[int64, string]) {
	t.Helper()

	if m.Len() != 0 {
		t.Fatalf("new map len = %d", m.Len())
	}
	if _, ok := m.Get(1); ok {
		t.Fatal("empty map should not contain key")
	}
	if m.Delete(1) {
		t.Fatal("delete missing key should return false")
	}

	m.Set(1, "one")
	m.Set(2, "two")
	m.Set(1, "uno")

	if m.Len() != 2 {
		t.Fatalf("len after set/update = %d", m.Len())
	}
	if got, ok := m.Get(1); !ok || got != "uno" {
		t.Fatalf("get updated key: got %q ok=%v", got, ok)
	}
	if got, ok := m.Get(2); !ok || got != "two" {
		t.Fatalf("get second key: got %q ok=%v", got, ok)
	}

	visited := make(map[int64]string)
	m.Range(func(key int64, value string) bool {
		visited[key] = value
		return true
	})
	if len(visited) != 2 || visited[1] != "uno" || visited[2] != "two" {
		t.Fatalf("range visited: %+v", visited)
	}

	stopCount := 0
	m.Range(func(int64, string) bool {
		stopCount++
		return false
	})
	if stopCount != 1 {
		t.Fatalf("range stop count = %d", stopCount)
	}

	if !m.Delete(1) {
		t.Fatal("delete existing key should return true")
	}
	if m.Delete(1) {
		t.Fatal("delete same key twice should return false")
	}
	if m.Len() != 1 {
		t.Fatalf("len after delete = %d", m.Len())
	}
	if _, ok := m.Get(1); ok {
		t.Fatal("deleted key should not exist")
	}

	m.Clear()
	if m.Len() != 0 {
		t.Fatalf("len after clear = %d", m.Len())
	}
	if _, ok := m.Get(2); ok {
		t.Fatal("cleared key should not exist")
	}

	m.Set(3, "three")
	if got, ok := m.Get(3); !ok || got != "three" {
		t.Fatalf("set after clear: got %q ok=%v", got, ok)
	}
}

func TestFastMapGrowthAndTombstoneReuse(t *testing.T) {
	m := NewInt64FastMap[int64](1)
	initialCap := m.Cap()
	for i := int64(0); i < 128; i++ {
		m.Set(i, i*10)
	}
	if m.Len() != 128 {
		t.Fatalf("len after growth = %d", m.Len())
	}
	if m.Cap() <= initialCap {
		t.Fatalf("capacity should grow, initial=%d current=%d", initialCap, m.Cap())
	}
	for i := int64(0); i < 128; i += 2 {
		if !m.Delete(i) {
			t.Fatalf("delete %d failed", i)
		}
	}
	for i := int64(1); i < 128; i += 2 {
		if got, ok := m.Get(i); !ok || got != i*10 {
			t.Fatalf("odd key %d lost: got=%d ok=%v", i, got, ok)
		}
	}
	for i := int64(200); i < 260; i++ {
		m.Set(i, i*10)
	}
	for i := int64(200); i < 260; i++ {
		if got, ok := m.Get(i); !ok || got != i*10 {
			t.Fatalf("new key %d lost: got=%d ok=%v", i, got, ok)
		}
	}
}

func TestFastMapStringKeys(t *testing.T) {
	m := NewStringFastMap[int](2)
	m.Set("alpha", 1)
	m.Set("beta", 2)
	m.Set("alpha", 3)

	if got, ok := m.Get("alpha"); !ok || got != 3 {
		t.Fatalf("alpha: got=%d ok=%v", got, ok)
	}
	if got, ok := m.Get("beta"); !ok || got != 2 {
		t.Fatalf("beta: got=%d ok=%v", got, ok)
	}
	if m.Delete("alpha") != true {
		t.Fatal("delete alpha failed")
	}
	if _, ok := m.Get("alpha"); ok {
		t.Fatal("alpha should be deleted")
	}
}

func TestShardedSafeMapConcurrentAccess(t *testing.T) {
	const (
		workers   = 8
		perWorker = 500
	)
	m := NewInt64ShardedSafeMap[string](8)

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := int64(worker * perWorker)
			for i := 0; i < perWorker; i++ {
				key := base + int64(i)
				value := fmt.Sprintf("%d", key)
				m.Set(key, value)
				if got, ok := m.Get(key); !ok || got != value {
					t.Errorf("get during set key=%d got=%q ok=%v", key, got, ok)
				}
			}
		}()
	}
	wg.Wait()

	wantLen := workers * perWorker
	if got := m.Len(); got != wantLen {
		t.Fatalf("len after concurrent set = %d, want %d", got, wantLen)
	}

	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := int64(worker * perWorker)
			for i := 0; i < perWorker; i += 2 {
				m.Delete(base + int64(i))
			}
		}()
	}
	wg.Wait()

	if got := m.Len(); got != wantLen/2 {
		t.Fatalf("len after concurrent delete = %d, want %d", got, wantLen/2)
	}
}

func TestShardedSafeMapClearConcurrentReaders(t *testing.T) {
	m := NewInt64ShardedSafeMap[int](16)
	for i := int64(0); i < 1000; i++ {
		m.Set(i, int(i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := int64(0); j < 1000; j++ {
				_, _ = m.Get(j)
			}
		}()
	}
	m.Clear()
	wg.Wait()

	if got := m.Len(); got != 0 {
		t.Fatalf("len after clear = %d", got)
	}
}

func TestShardedSafeMapComputeLoadOrStoreAndSnapshot(t *testing.T) {
	m := NewInt64ShardedSafeMap[string](8)
	if got := m.ShardCount(); got != 8 {
		t.Fatalf("shard count = %d, want 8", got)
	}
	if idx := m.ShardIndex(42); idx < 0 || idx >= m.ShardCount() {
		t.Fatalf("shard index out of range: %d", idx)
	}

	value, loaded := m.LoadOrStore(1, "one")
	if loaded || value != "one" {
		t.Fatalf("first LoadOrStore value=%q loaded=%v", value, loaded)
	}
	value, loaded = m.LoadOrStore(1, "uno")
	if !loaded || value != "one" {
		t.Fatalf("second LoadOrStore value=%q loaded=%v", value, loaded)
	}

	got, ok := m.Compute(1, func(old string, exists bool) (string, bool) {
		if !exists || old != "one" {
			t.Fatalf("compute old=%q exists=%v", old, exists)
		}
		return "uno", true
	})
	if !ok || got != "uno" {
		t.Fatalf("compute update got=%q ok=%v", got, ok)
	}
	got, ok = m.Compute(1, func(old string, exists bool) (string, bool) {
		return "", false
	})
	if ok || got != "" {
		t.Fatalf("compute delete got=%q ok=%v", got, ok)
	}
	if m.Len() != 0 {
		t.Fatalf("len after compute delete = %d", m.Len())
	}

	m.Set(2, "two")
	m.Set(3, "three")
	readCalled := false
	readOK := m.Read(2, func(value string, exists bool) {
		readCalled = true
		if !exists || value != "two" {
			t.Fatalf("read value=%q exists=%v", value, exists)
		}
	})
	if !readOK || !readCalled {
		t.Fatalf("read should report existing key, ok=%v called=%v", readOK, readCalled)
	}
	missingOK := m.Read(4, func(value string, exists bool) {
		if exists || value != "" {
			t.Fatalf("missing read value=%q exists=%v", value, exists)
		}
	})
	if missingOK {
		t.Fatal("read should report missing key")
	}
	entries := m.Snapshot()
	if len(entries) != 2 {
		t.Fatalf("snapshot len = %d", len(entries))
	}
	entries[0].Value = "changed"
	if got, _ := m.Get(entries[0].Key); got == "changed" {
		t.Fatal("snapshot should not expose mutable entry storage")
	}
}
