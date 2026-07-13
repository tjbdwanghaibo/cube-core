package query

import "testing"

func TestIndexQuery(t *testing.T) {
	idx := NewOrderedIndex[int64, int32](func(a, b int64) bool { return a < b })
	idx.Upsert(3, 10)
	idx.Upsert(1, 10)
	idx.Upsert(2, 20)
	got := idx.Query(10)
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("Query = %v", got)
	}
	idx.Upsert(1, 20)
	got = idx.Query(10)
	if len(got) != 1 || got[0] != 3 {
		t.Fatalf("Query after move = %v", got)
	}
}
