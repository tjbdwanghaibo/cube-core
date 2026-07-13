package entity

// EntityIDMeta is the normalized identity view shared by Nest, entity
// lookup, remote entity locking, and persistence.
type EntityIDMeta struct {
	InputID       int64
	UniqueID      int64
	FullID        int64
	Category      EntityCategory
	Kind          EntityKind
	RemoteCapable bool
}

// ResolveEntityID treats full EntityID as the canonical input.
func ResolveEntityID(id int64) EntityIDMeta {
	v := uint64(id) & EntityIDValueMask
	uniqueID := int64((v >> UniqueIDShift) & UniqueIDMask)
	category := EntityCategory(v & EntityCategoryMask)
	kind := GetEntityKindFromID(id)
	remote := GetEntityRemoteFromID(id)

	fullID := id
	if kind != EntityKindNone && IsEntityKindRemoteCapable(kind) {
		remote = true
		fullID = setRemoteCapableBit(fullID)
	}

	return EntityIDMeta{
		InputID:       id,
		UniqueID:      uniqueID,
		FullID:        fullID,
		Category:      category,
		Kind:          kind,
		RemoteCapable: remote,
	}
}
