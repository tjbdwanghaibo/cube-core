package entity

// ComponentBase is the default base implementation of ComponentInterfaceBase.
// Embed this in concrete component structs to get no-op defaults.
type ComponentBase struct{}

func (b *ComponentBase) Name() string { return "" }

func (b *ComponentBase) OnInitFinish(param *EntityCreateParam, isCreate bool) error {
	return nil
}

func (b *ComponentBase) OnDestroy(rea EntityDestroyReason) {}
