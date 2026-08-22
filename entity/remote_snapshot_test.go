package entity

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRemoteSnapshotCacheAppliesDeltaAndRejectsGap(t *testing.T) {
	const kind EntityKind = 191
	const schema uint32 = 44001
	MustRegisterEntityKindDefs(EntityKindDef{Kind: kind, Category: 1, RemotePolicy: RemotePolicyManaged})
	id, err := BuildEntityID(9101, kind)
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterRemoteSnapshotDelta(schema, func(base, delta []byte) ([]byte, error) {
		return append(base, delta...), nil
	}); err != nil {
		t.Fatal(err)
	}
	cache := NewRemoteSnapshotCache(RemoteSnapshotCacheConfig{Shards: 4, MaxEntries: 32, MaxBytes: 1 << 20, TTL: time.Minute, MaxWaiters: 4}, nil, nil)
	key := RemoteSnapshotKey{EntityID: id, Kind: kind, Scope: 1}
	full := RemoteSnapshotRecord{Key: key, StateVersion: 1, MarkerEpoch: 1, RouteEpoch: 1, Schema: schema, Codec: 1, Full: true, Data: []byte("a")}
	full.Checksum = RemoteSnapshotChecksum(full.Data)
	if err := cache.ApplyUpdate(context.Background(), full); err != nil {
		t.Fatal(err)
	}
	delta := RemoteSnapshotRecord{Key: key, BaseVersion: 1, StateVersion: 2, MarkerEpoch: 1, RouteEpoch: 1, Schema: schema, Codec: 1, Data: []byte("b")}
	delta.Checksum = RemoteSnapshotChecksum(delta.Data)
	if err := cache.ApplyUpdate(context.Background(), delta); err != nil {
		t.Fatal(err)
	}
	conflict := RemoteSnapshotEnvelope{Key: key, BaseVersion: 1, StateVersion: 2, MarkerEpoch: 1, RouteEpoch: 1, Schema: schema, Codec: 1, Full: true, Payload: CopyFrozenRemoteSnapshotPayload([]byte("different"))}
	if err := cache.Publish(context.Background(), conflict); !errors.Is(err, ErrRemoteVersionConflict) {
		t.Fatalf("same-version conflict error=%v", err)
	}
	got, ok, err := cache.Get(context.Background(), key, RemoteReadMonotonic, 2)
	if err != nil || !ok || string(got.Payload.BytesCopy()) != "ab" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
	delta.BaseVersion = 1
	delta.StateVersion = 3
	if err := cache.ApplyUpdate(context.Background(), delta); !errors.Is(err, ErrRemoteSnapshotGap) {
		t.Fatalf("gap error=%v", err)
	}
}

func TestRemoteSnapshotCacheBoundsVersionWaiters(t *testing.T) {
	const kind EntityKind = 192
	MustRegisterEntityKindDefs(EntityKindDef{Kind: kind, Category: 1, RemotePolicy: RemotePolicyManaged})
	id, err := BuildEntityID(9102, kind)
	if err != nil {
		t.Fatal(err)
	}
	cache := NewRemoteSnapshotCache(RemoteSnapshotCacheConfig{Shards: 1, MaxEntries: 4, MaxBytes: 1024, TTL: time.Minute, MaxWaiters: 1}, nil, nil)
	key := RemoteSnapshotKey{EntityID: id, Kind: kind, Scope: 1}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cache.WaitForVersion(ctx, key, 2) }()
	deadline := time.Now().Add(time.Second)
	for {
		cache.waitMu.Lock()
		count := cache.waiterCount
		cache.waitMu.Unlock()
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first waiter was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	if err := cache.WaitForVersion(context.Background(), key, 2); !errors.Is(err, ErrRemoteOverloaded) {
		t.Fatalf("second waiter error=%v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter error=%v", err)
	}
}

func TestRemoteSnapshotPayloadCannotMutateCache(t *testing.T) {
	const kind EntityKind = 196
	MustRegisterEntityKindDefs(EntityKindDef{Kind: kind, Category: 1, RemotePolicy: RemotePolicyManaged})
	id, err := BuildEntityID(9104, kind)
	if err != nil {
		t.Fatal(err)
	}
	key := RemoteSnapshotKey{EntityID: id, Kind: kind, Scope: 1}
	cache := NewRemoteSnapshotCache(RemoteSnapshotCacheConfig{Shards: 1, MaxEntries: 4, MaxBytes: 1024, TTL: time.Minute}, nil, nil)
	source := []byte("immutable")
	value := RemoteSnapshotEnvelope{Key: key, StateVersion: 1, MarkerEpoch: 1, RouteEpoch: 1, Schema: 1, Full: true, Payload: CopyFrozenRemoteSnapshotPayload(source)}
	if err := cache.Publish(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	source[0] = 'X'
	got, ok, err := cache.Get(context.Background(), key, RemoteReadCached, 0)
	if err != nil || !ok || string(got.Payload.BytesCopy()) != "immutable" {
		t.Fatalf("snapshot=%+v ok=%v err=%v", got, ok, err)
	}
	copyOut := got.Payload.BytesCopy()
	copyOut[0] = 'Y'
	again, _, _ := cache.Get(context.Background(), key, RemoteReadCached, 0)
	if string(again.Payload.BytesCopy()) != "immutable" {
		t.Fatalf("cache payload mutated through read copy")
	}
}

func TestRemoteCommitRejectsMutationVersionDrift(t *testing.T) {
	const kind EntityKind = 197
	MustRegisterEntityKindDefs(EntityKindDef{Kind: kind, Category: 1, RemotePolicy: RemotePolicyManaged})
	id, err := BuildEntityID(9105, kind)
	if err != nil {
		t.Fatal(err)
	}
	var tx RemoteTransactionID
	tx[15] = 1
	commit := RemoteCommit{
		TransactionID: tx, EntityID: id, Kind: kind,
		BaseVersion: 4, NextVersion: 5, MarkerEpoch: 1, RouteEpoch: 1,
		Mutations: []RemoteDataMutation{{Collection: "players", ID: id, Version: 6, Data: []byte("state")}},
	}
	if err := commit.Validate(); !errors.Is(err, ErrRemoteVersionConflict) {
		t.Fatalf("mutation version drift error=%v", err)
	}
}

var remoteSnapshotBenchmarkByte byte

func BenchmarkRemoteSnapshotCacheL1Get4K(b *testing.B) {
	const kind EntityKind = 193
	MustRegisterEntityKindDefs(EntityKindDef{Kind: kind, Category: 1, RemotePolicy: RemotePolicyManaged})
	id, err := BuildEntityID(9103, kind)
	if err != nil {
		b.Fatal(err)
	}
	cache := NewRemoteSnapshotCache(RemoteSnapshotCacheConfig{Shards: 64, MaxEntries: 1024, MaxBytes: 16 << 20, TTL: time.Minute, MaxWaiters: 64}, nil, nil)
	key := RemoteSnapshotKey{EntityID: id, Kind: kind, Scope: 1}
	value := RemoteSnapshotEnvelope{Key: key, StateVersion: 1, MarkerEpoch: 1, RouteEpoch: 1, Schema: 1, Full: true, Payload: TakeFrozenRemoteSnapshotPayload(make([]byte, 4<<10))}
	if err := cache.Publish(context.Background(), value); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, ok, err := cache.Get(context.Background(), key, RemoteReadMonotonic, 1)
		if err != nil || !ok {
			b.Fatal(err)
		}
		remoteSnapshotBenchmarkByte = snapshot.Payload.data[0]
	}
}
