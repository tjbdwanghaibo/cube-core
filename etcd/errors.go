package etcd

import "errors"

var (
	ErrKeyNotFound          = errors.New("etcd: key not found")
	ErrLeaseExpired         = errors.New("etcd: lease expired")
	ErrNotLeader            = errors.New("etcd: not leader")
	ErrElectionNoLeader     = errors.New("etcd: no leader")
	ErrClosed               = errors.New("etcd: client closed")
	ErrTxnFailed            = errors.New("etcd: transaction failed")
	ErrWatchClosed          = errors.New("etcd: watch closed")
	ErrWatchCanceled        = errors.New("etcd: watch canceled")
	ErrWatchCompacted       = errors.New("etcd: watch revision compacted")
	ErrWatchInvalidCallback = errors.New("etcd: invalid watch callback")
	ErrWatchCallbackPanic   = errors.New("etcd: watch callback panicked")

	ErrMirrorInvalidConfig        = errors.New("etcd mirror: invalid configuration")
	ErrMirrorClosed               = errors.New("etcd mirror: closed")
	ErrMirrorKeyOutsidePrefix     = errors.New("etcd mirror: key is outside the configured prefix")
	ErrMirrorNotSynced            = errors.New("etcd mirror: not synchronized")
	ErrMirrorInvalidSubscriber    = errors.New("etcd mirror: invalid subscriber")
	ErrMirrorSubscriberSlow       = errors.New("etcd mirror: subscriber queue is full")
	ErrMirrorSubscribeUnsupported = errors.New("etcd mirror: subscriptions are not supported")
)
