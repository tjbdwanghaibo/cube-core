package etcd

import "context"

// IDiscovery provides service registration and discovery.
type IDiscovery interface {
	// Register registers this service instance with a TTL lease.
	// Kept alive automatically until Deregister or context cancellation.
	Register(ctx context.Context, info *ServiceInfo) error

	// Deregister removes this service instance.
	Deregister(ctx context.Context) error

	// Discover returns all registered instances of a service type.
	Discover(ctx context.Context, serviceType string) ([]*ServiceInfo, error)

	// WatchService watches for changes in a service type's instances.
	WatchService(ctx context.Context, serviceType string) IServiceWatcher
}

// ServiceInfo describes a service instance.
type ServiceInfo struct {
	ServiceType string            // e.g. "game", "gate", "center"
	Sid         int32             // server id
	Addr        string            // accessible address (ip:port)
	Metadata    map[string]string // extra info (version, weight, etc.)
}

// IServiceWatcher notifies about service instance changes.
type IServiceWatcher interface {
	// EventChan returns events for service changes.
	EventChan() <-chan *ServiceEvent

	// Close stops watching.
	Close() error
}

// ServiceEvent is a service instance change notification.
type ServiceEvent struct {
	Type EventType
	Info *ServiceInfo
}
