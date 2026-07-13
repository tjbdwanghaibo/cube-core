package event

import "sync"

// EventUnit manages subscribers for a single EventType.
type EventUnit struct {
	mu   sync.RWMutex
	subs map[EventGroupType][]subInfo
}

type subInfo struct {
	handler EventHandler
	async   AsyncDispatcher // nil for sync-only handlers
	groups  []EventGroupType
}

func newEventUnit() *EventUnit {
	return &EventUnit{
		subs: make(map[EventGroupType][]subInfo),
	}
}

// AddSub registers a handler under the given groups.
func (u *EventUnit) AddSub(handler EventHandler, async AsyncDispatcher, groups []EventGroupType) {
	u.mu.Lock()
	defer u.mu.Unlock()

	si := subInfo{handler: handler, async: async, groups: groups}
	for _, g := range groups {
		u.subs[g] = append(u.subs[g], si)
	}
}

// DelSub removes a handler from all groups.
func (u *EventUnit) DelSub(handler EventHandler) {
	u.mu.Lock()
	defer u.mu.Unlock()

	for g, list := range u.subs {
		filtered := list[:0]
		for _, si := range list {
			if si.handler != handler {
				filtered = append(filtered, si)
			}
		}
		if len(filtered) == 0 {
			delete(u.subs, g)
		} else {
			u.subs[g] = filtered
		}
	}
}

// Publish dispatches an event to all handlers subscribed to any of pubGroups.
// Self-handler receives sync call; others receive async call.
func (u *EventUnit) Publish(d EventData, self EventHandler, pubGroups []EventGroupType) {
	u.mu.RLock()
	seen := make(map[EventHandler]bool)
	handlers := make([]subInfo, 0)
	for _, g := range pubGroups {
		for _, si := range u.subs[g] {
			if seen[si.handler] {
				continue
			}
			seen[si.handler] = true
			handlers = append(handlers, si)
		}
	}
	u.mu.RUnlock()

	for _, si := range handlers {
		if si.handler == self {
			si.handler.SyncHandleEvent(d)
		} else if si.async != nil {
			si.async.AsyncHandleEvent(d)
		} else {
			si.handler.SyncHandleEvent(d)
		}
	}
}

// EventMgr routes events to the appropriate EventUnit.
type EventMgr struct {
	mu    sync.RWMutex
	units map[EventType]*EventUnit
}

// NewEventMgr creates a new EventMgr.
func NewEventMgr() *EventMgr {
	return &EventMgr{
		units: make(map[EventType]*EventUnit),
	}
}

func (m *EventMgr) getOrCreate(eventType EventType) *EventUnit {
	m.mu.RLock()
	u, ok := m.units[eventType]
	m.mu.RUnlock()
	if ok {
		return u
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if u, ok = m.units[eventType]; ok {
		return u
	}
	u = newEventUnit()
	m.units[eventType] = u
	return u
}

// Sub registers a handler for an event type under the given groups.
func (m *EventMgr) Sub(eventType EventType, handler EventHandler, async AsyncDispatcher, groups []EventGroupType) {
	u := m.getOrCreate(eventType)
	u.AddSub(handler, async, groups)
}

// Unsub removes a handler from an event type.
func (m *EventMgr) Unsub(eventType EventType, handler EventHandler) {
	m.mu.RLock()
	u, ok := m.units[eventType]
	m.mu.RUnlock()
	if ok {
		u.DelSub(handler)
	}
}

// Pub publishes an event from self with the given groups.
func (m *EventMgr) Pub(d EventData, self EventHandler, pubGroups []EventGroupType) {
	m.mu.RLock()
	u, ok := m.units[d.Type()]
	m.mu.RUnlock()
	if !ok {
		return
	}
	u.Publish(d, self, pubGroups)
}
