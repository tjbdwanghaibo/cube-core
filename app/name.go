package app

// ModName identifies an infrastructure module.
type ModName string

// ServiceName identifies a business service.
type ServiceName string

const (
	ModHealth        ModName = "health"
	ModObs           ModName = "obs"
	ModAdmin         ModName = "admin"
	ModAdminMetadata ModName = "admin.metadata"
	ModLifecycle     ModName = "lifecycle"
)
