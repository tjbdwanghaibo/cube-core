package entity

// EntityCategory identifies ownership/access category. It is encoded in the
// low 2 bits of EntityID.
type EntityCategory uint8

const (
	EntityCategoryNone EntityCategory = 0
)

// EntityKind identifies the concrete business entity definition. It is encoded
// in EntityID so any server can choose the right factory/loader from the ID.
type EntityKind uint8

const EntityKindNone EntityKind = 0

// EntityKindCategory declares the ownership category for one concrete entity
// kind. The relation is global because EntityID encodes both fields and every
// server must resolve the same kind to the same category.
type EntityKindCategory struct {
	Kind     EntityKind
	Category EntityCategory
}

// EntityKindDef declares all global ID-visible properties for one concrete
// entity kind. Every server should register the same definition before it
// builds, validates, or routes EntityIDs.
type EntityKindDef struct {
	Kind         EntityKind
	Category     EntityCategory
	RemotePolicy RemotePolicy
}

// ComponentType identifies component type within an entity.
type ComponentType uint16

// EntityDestroyReason describes why an entity is being destroyed.
type EntityDestroyReason uint8

const (
	// DestroyReasonCommon is the neutral default reason for infrastructure-level
	// entity removal. Business packages may alias it with domain-specific names.
	DestroyReasonCommon EntityDestroyReason = 0
)
