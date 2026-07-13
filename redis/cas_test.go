package redis

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCompareAndSetAppliesWhenExpectedValueMatches(t *testing.T) {
	client := &casFakeRedis{values: map[string][]byte{"role:1": []byte(`{"version":1}`)}}

	result, err := CompareAndSet(context.Background(), client, CompareAndSetCommand{
		Key:      "role:1",
		Expected: []byte(`{"version":1}`),
		Next:     []byte(`{"version":2}`),
		TTL:      time.Hour,
	})
	if err != nil {
		t.Fatalf("CompareAndSet: %v", err)
	}
	if !result.Applied {
		t.Fatalf("applied = false current=%s", result.Current)
	}
	if got := string(client.values["role:1"]); got != `{"version":2}` {
		t.Fatalf("value = %s", got)
	}
	if client.ttl["role:1"] != time.Hour {
		t.Fatalf("ttl = %v, want %v", client.ttl["role:1"], time.Hour)
	}
}

func TestCompareAndSetReturnsCurrentWhenExpectedValueDiffers(t *testing.T) {
	client := &casFakeRedis{values: map[string][]byte{"role:1": []byte(`{"version":3}`)}}

	result, err := CompareAndSet(context.Background(), client, CompareAndSetCommand{
		Key:      "role:1",
		Expected: []byte(`{"version":1}`),
		Next:     []byte(`{"version":2}`),
	})
	if err != nil {
		t.Fatalf("CompareAndSet: %v", err)
	}
	if result.Applied {
		t.Fatal("applied = true, want conflict")
	}
	if string(result.Current) != `{"version":3}` {
		t.Fatalf("current = %s", result.Current)
	}
}

func TestCompareAndSetCanRequireMissingKey(t *testing.T) {
	client := &casFakeRedis{values: map[string][]byte{}}

	result, err := CompareAndSet(context.Background(), client, CompareAndSetCommand{
		Key:  "role:1",
		Next: []byte(`{"version":1}`),
	})
	if err != nil {
		t.Fatalf("CompareAndSet: %v", err)
	}
	if !result.Applied {
		t.Fatalf("applied = false current=%s", result.Current)
	}
	if string(client.values["role:1"]) != `{"version":1}` {
		t.Fatalf("stored = %s", client.values["role:1"])
	}
}

type casFakeRedis struct {
	IRedis
	values map[string][]byte
	ttl    map[string]time.Duration
}

func (r *casFakeRedis) Eval(_ context.Context, _ string, keys []string, args ...any) (any, error) {
	if len(keys) != 1 || len(args) != 4 {
		return nil, fmt.Errorf("unexpected CAS eval keys=%v args=%v", keys, args)
	}
	if r.values == nil {
		r.values = make(map[string][]byte)
	}
	if r.ttl == nil {
		r.ttl = make(map[string]time.Duration)
	}
	key := keys[0]
	expected := []byte(fmt.Sprint(args[0]))
	next := []byte(fmt.Sprint(args[1]))
	ttlMillis, ok := args[2].(int64)
	if !ok {
		return nil, fmt.Errorf("ttl arg type = %T", args[2])
	}
	expectMissing := fmt.Sprint(args[3]) == "1"
	current, found := r.values[key]
	switch {
	case expectMissing && found:
		return []any{int64(0), string(current)}, nil
	case !expectMissing && (!found || !bytes.Equal(current, expected)):
		if !found {
			return []any{int64(0), nil}, nil
		}
		return []any{int64(0), string(current)}, nil
	default:
		r.values[key] = append([]byte(nil), next...)
		if ttlMillis > 0 {
			r.ttl[key] = time.Duration(ttlMillis) * time.Millisecond
		}
		return []any{int64(1), string(next)}, nil
	}
}
