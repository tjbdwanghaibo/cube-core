package nats

import (
	"context"
	"time"
)

type JetStreamStorage string

const (
	JetStreamStorageFile   JetStreamStorage = "file"
	JetStreamStorageMemory JetStreamStorage = "memory"
)

type JetStreamDeliverPolicy string

const (
	JetStreamDeliverAll JetStreamDeliverPolicy = "all"
	JetStreamDeliverNew JetStreamDeliverPolicy = "new"
)

type IJetStream interface {
	EnsureStream(context.Context, JetStreamConfig) error
	Publish(context.Context, string, []byte, JetStreamPublishOptions) (JetStreamPublishAck, error)
	Subscribe(context.Context, JetStreamConsumerConfig, JetStreamHandler) (IJetStreamSubscription, error)
}

type JetStreamConfig struct {
	Name       string
	Subjects   []string
	Storage    JetStreamStorage
	MaxAge     time.Duration
	Duplicates time.Duration
	Replicas   int
	MaxBytes   int64
}

type JetStreamPublishOptions struct {
	MsgID string
}

type JetStreamPublishAck struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

type JetStreamConsumerConfig struct {
	Stream        string
	Name          string
	Durable       string
	FilterSubject string
	DeliverPolicy JetStreamDeliverPolicy
	AckWait       time.Duration
	MaxDeliver    int
}

type JetStreamHandler func(context.Context, *JetStreamMsg) error

type JetStreamMsg struct {
	Subject      string
	Data         []byte
	Stream       string
	Consumer     string
	StreamSeq    uint64
	ConsumerSeq  uint64
	NumDelivered uint64
}

type IJetStreamSubscription interface {
	Stop()
	Drain()
	Closed() <-chan struct{}
}
