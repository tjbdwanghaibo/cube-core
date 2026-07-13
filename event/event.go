package event

// EventType identifies a specific event kind.
type EventType int32

// EventGroupType is a tag used for pub/sub filtering.
type EventGroupType string

// EventData is implemented by all event structs.
type EventData interface {
	Type() EventType
}
