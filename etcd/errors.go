package etcd

import "errors"

var (
	ErrKeyNotFound      = errors.New("etcd: key not found")
	ErrLeaseExpired     = errors.New("etcd: lease expired")
	ErrNotLeader        = errors.New("etcd: not leader")
	ErrElectionNoLeader = errors.New("etcd: no leader")
	ErrClosed           = errors.New("etcd: client closed")
	ErrTxnFailed        = errors.New("etcd: transaction failed")
)
