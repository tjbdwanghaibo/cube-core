package bus

// Codec handles serialization/deserialization of messages on the bus.
type Codec interface {
	// Marshal encodes a message into bytes.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes bytes into the target value.
	Unmarshal(data []byte, v any) error
}
