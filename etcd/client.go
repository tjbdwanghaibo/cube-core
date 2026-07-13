package etcd

import (
	"context"
)

// IEtcd is the abstraction for etcd KV + Lease + Txn operations.
type IEtcd interface {
	// --- KV ---
	Get(ctx context.Context, key string) (*KV, error)
	GetWithPrefix(ctx context.Context, prefix string) ([]*KV, error)
	Put(ctx context.Context, key, value string) error
	PutWithLease(ctx context.Context, key, value string, leaseID int64) error
	Delete(ctx context.Context, key string) error
	DeleteWithPrefix(ctx context.Context, prefix string) (int64, error)

	// --- Txn (CAS) ---
	Txn(ctx context.Context, cmp Cmp, onSuccess, onFailure []Op) (*TxnResponse, error)

	// --- Lease ---
	Grant(ctx context.Context, ttl int64) (int64, error)
	KeepAlive(ctx context.Context, leaseID int64) (<-chan struct{}, error)
	Revoke(ctx context.Context, leaseID int64) error

	// --- Watch ---
	Watch(ctx context.Context, key string, opts ...WatchOption) IWatcher
	WatchPrefix(ctx context.Context, prefix string, opts ...WatchOption) IWatcher

	// --- Connection ---
	Close() error
}

// KV represents an etcd key-value pair.
type KV struct {
	Key            string
	Value          string
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Lease          int64
}

// Cmp is a comparison for Txn.
type Cmp struct {
	Key    string
	Target CmpTarget
	Op     CmpOp
	Value  any // int64 for revision/version, string for value
}

// CmpTarget specifies what to compare.
type CmpTarget int

const (
	CmpVersion CmpTarget = iota
	CmpCreateRevision
	CmpModRevision
	CmpValue
)

// CmpOp specifies how to compare.
type CmpOp int

const (
	CmpEqual CmpOp = iota
	CmpNotEqual
	CmpLess
	CmpGreater
)

// Op is a KV operation for Txn.
type Op struct {
	Type  OpType
	Key   string
	Value string
	Lease int64
}

// OpType specifies the operation type.
type OpType int

const (
	OpPut OpType = iota
	OpDelete
)

// TxnResponse is the result of a Txn.
type TxnResponse struct {
	Succeeded bool
	Revision  int64
}
