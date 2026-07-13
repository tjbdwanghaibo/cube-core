package nats

import "fmt"

// SubjectBuilder generates NATS subjects following a consistent naming convention.
// Pattern: {prefix}.{category}.{target}
//
// Categories:
//
//	srv  — server-level routing (by sid)
//	svc  — service-type routing (by type + sid)
//	mod  — module-level broadcast
//	rpc  — RPC requests
type SubjectBuilder struct {
	prefix string
}

func NewSubjectBuilder(prefix string) *SubjectBuilder {
	return &SubjectBuilder{prefix: prefix}
}

// --- Point-to-point ---

// Server returns subject for a specific server: {prefix}.srv.{sid}
func (b *SubjectBuilder) Server(sid int32) string {
	return fmt.Sprintf("%s.srv.%d", b.prefix, sid)
}

// --- Service type ---

// ServiceInstance returns subject for a specific instance: {prefix}.svc.{type}.{sid}
func (b *SubjectBuilder) ServiceInstance(svcType string, sid int32) string {
	return fmt.Sprintf("%s.svc.%s.%d", b.prefix, svcType, sid)
}

// ServiceAll returns broadcast subject for all instances of a type: {prefix}.svc.{type}.all
func (b *SubjectBuilder) ServiceAll(svcType string) string {
	return fmt.Sprintf("%s.svc.%s.all", b.prefix, svcType)
}

// --- Broadcast ---

// All returns broadcast subject for all servers: {prefix}.srv.all
func (b *SubjectBuilder) All() string {
	return fmt.Sprintf("%s.srv.all", b.prefix)
}

// Module returns broadcast subject for a module: {prefix}.mod.{module}
func (b *SubjectBuilder) Module(module string) string {
	return fmt.Sprintf("%s.mod.%s", b.prefix, module)
}

// --- RPC ---

// Rpc returns subject for a specific RPC method: {prefix}.rpc.{service}.{method}
func (b *SubjectBuilder) Rpc(service string, method string) string {
	return fmt.Sprintf("%s.rpc.%s.%s", b.prefix, service, method)
}

// RpcInstance returns subject for a specific service instance RPC method:
// {prefix}.rpc.{service}.{sid}.{method}
func (b *SubjectBuilder) RpcInstance(service string, sid int32, method string) string {
	return fmt.Sprintf("%s.rpc.%s.%d.%s", b.prefix, service, sid, method)
}

// RpcService returns wildcard subject for subscribing to all methods of a service: {prefix}.rpc.{service}.*
func (b *SubjectBuilder) RpcService(service string) string {
	return fmt.Sprintf("%s.rpc.%s.*", b.prefix, service)
}

// RpcResponse returns the JetStream RPC response subject for a caller sid and
// request id: {prefix}.rpc_resp.{sid}.{request_id}
func (b *SubjectBuilder) RpcResponse(sid int32, requestID string) string {
	return fmt.Sprintf("%s.rpc_resp.%d.%s", b.prefix, sid, requestID)
}

// RpcResponseInbox returns the JetStream RPC response inbox filter for a sid.
func (b *SubjectBuilder) RpcResponseInbox(sid int32) string {
	return fmt.Sprintf("%s.rpc_resp.%d.>", b.prefix, sid)
}
