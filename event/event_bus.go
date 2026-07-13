package event

// EventBus is the per-handler event interface.
// Each module/entity embeds an EventBus to subscribe/publish events.
type EventBus struct {
	mgr     *EventMgr
	handler EventHandler
	async   AsyncDispatcher
	groups  []EventGroupType // groups this bus publishes under
	subbed  []EventType      // tracked for cleanup
}

// NewEventBus creates an EventBus for a handler.
// selfGroup is the handler's own group (used as publish tag).
// extraGroups are additional groups this handler belongs to.
func NewEventBus(mgr *EventMgr, handler EventHandler, async AsyncDispatcher, selfGroup EventGroupType, extraGroups ...EventGroupType) *EventBus {
	groups := append([]EventGroupType{selfGroup}, extraGroups...)
	return &EventBus{
		mgr:     mgr,
		handler: handler,
		async:   async,
		groups:  groups,
	}
}

// SubEvent subscribes to an event type.
func (b *EventBus) SubEvent(eventType EventType) {
	b.mgr.Sub(eventType, b.handler, b.async, b.groups)
	b.subbed = append(b.subbed, eventType)
}

// PubEvent publishes an event from this handler.
func (b *EventBus) PubEvent(d EventData) {
	b.mgr.Pub(d, b.handler, b.groups)
}

// Destroy unsubscribes from all events.
func (b *EventBus) Destroy() {
	for _, eventType := range b.subbed {
		b.mgr.Unsub(eventType, b.handler)
	}
	b.subbed = nil
}
