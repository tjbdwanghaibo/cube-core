package bus

import "context"

// MsgContext is the context passed to message handlers.
type MsgContext struct {
	FromSid   int32
	ToModule  string
	MsgName   string
	MsgID     string
	Attempt   int32
	CreatedAt int64
	Payload   []byte
	base      context.Context
	codec     Codec
}

// Decode decodes the message payload into the target struct.
func (c *MsgContext) Decode(v any) error {
	return c.codec.Unmarshal(c.Payload, v)
}

func (c *MsgContext) Context() context.Context {
	if c != nil && c.base != nil {
		return c.base
	}
	return context.Background()
}

// RpcContext is the context passed to RPC handlers.
type RpcContext struct {
	MsgContext
	Method       string
	ReplySubject string
}

func NewRPCContext(base context.Context, method string, payload []byte, codec Codec) *RpcContext {
	if codec == nil {
		codec = JSONCodec{}
	}
	return &RpcContext{
		MsgContext: MsgContext{
			MsgName: method,
			Payload: append([]byte(nil), payload...),
			base:    base,
			codec:   codec,
		},
		Method: method,
	}
}

// HandlerFunc handles an incoming module message.
type HandlerFunc func(ctx *MsgContext)

// RpcHandlerFunc handles an incoming RPC request and returns a response.
type RpcHandlerFunc func(ctx *RpcContext) (resp any, err error)

// handlerEntry stores a registered handler.
type handlerEntry struct {
	module  string
	msgName string
	handler HandlerFunc
}

// rpcHandlerEntry stores a registered RPC handler.
type rpcHandlerEntry struct {
	method  string
	handler RpcHandlerFunc
}
