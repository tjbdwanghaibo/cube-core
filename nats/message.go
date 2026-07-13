package nats

// NatsMsg is the wire format for inter-service communication.
type NatsMsg struct {
	FromSid   int32         `json:"from_sid"`
	ToSid     int32         `json:"to_sid"` // 0 = broadcast
	ToModule  string        `json:"to_module"`
	MsgName   string        `json:"msg_name"`
	Payload   []byte        `json:"payload"` // encoded by application (protobuf/bson/json)
	Broadcast BroadcastType `json:"broadcast"`
	SessionId string        `json:"session_id"` // for RPC correlation
	MsgID     string        `json:"msg_id,omitempty"`
	Attempt   int32         `json:"attempt,omitempty"`
	CreatedAt int64         `json:"created_at,omitempty"`
	// ReplySubject and DeadlineAt are used by JetStream-backed RPC. They are
	// optional so existing core NATS messages remain wire-compatible.
	ReplySubject string `json:"reply_subject,omitempty"`
	DeadlineAt   int64  `json:"deadline_at,omitempty"`
}

// BroadcastType determines message routing scope.
type BroadcastType int32

const (
	BroadcastNone       BroadcastType = 0 // point-to-point
	BroadcastModule     BroadcastType = 1 // all instances of a module
	BroadcastServerType BroadcastType = 2 // all servers of a type
	BroadcastAll        BroadcastType = 3 // all servers
)
