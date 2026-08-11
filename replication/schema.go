package replication

import (
	"fmt"
	"sync"
)

type ComponentSchema struct {
	TypeID         uint16
	Name           string
	Version        uint16
	MaxEncodedSize uint32
	Policy         ReplicationPolicy
}

func (s ComponentSchema) validate() error {
	if s.TypeID == 0 || s.Name == "" || s.Version == 0 || s.MaxEncodedSize == 0 {
		return fmt.Errorf("%w: invalid component schema", ErrInvalidFrame)
	}
	return s.Policy.validate()
}

type SchemaRegistry struct {
	mu     sync.RWMutex
	byID   map[uint16]ComponentSchema
	byName map[string]uint16
}

func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		byID:   make(map[uint16]ComponentSchema),
		byName: make(map[string]uint16),
	}
}

func (r *SchemaRegistry) Register(schema ComponentSchema) error {
	if r == nil {
		return fmt.Errorf("%w: nil schema registry", ErrInvalidFrame)
	}
	if err := schema.validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, exists := r.byID[schema.TypeID]; exists {
		return fmt.Errorf("replication: component type %d already registered as %q", schema.TypeID, old.Name)
	}
	if id, exists := r.byName[schema.Name]; exists {
		return fmt.Errorf("replication: component name %q already registered as %d", schema.Name, id)
	}
	r.byID[schema.TypeID] = schema
	r.byName[schema.Name] = schema.TypeID
	return nil
}

func (r *SchemaRegistry) Lookup(typeID uint16) (ComponentSchema, bool) {
	if r == nil {
		return ComponentSchema{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	schema, ok := r.byID[typeID]
	return schema, ok
}

func (r *SchemaRegistry) LookupName(name string) (ComponentSchema, bool) {
	if r == nil {
		return ComponentSchema{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byName[name]
	if !ok {
		return ComponentSchema{}, false
	}
	return r.byID[id], true
}

func (r *SchemaRegistry) Snapshot() []ComponentSchema {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	out := make([]ComponentSchema, 0, len(r.byID))
	for _, schema := range r.byID {
		out = append(out, schema)
	}
	r.mu.RUnlock()
	sortSchemas(out)
	return out
}

func sortSchemas(schemas []ComponentSchema) {
	for i := 1; i < len(schemas); i++ {
		for j := i; j > 0 && schemas[j].TypeID < schemas[j-1].TypeID; j-- {
			schemas[j], schemas[j-1] = schemas[j-1], schemas[j]
		}
	}
}
