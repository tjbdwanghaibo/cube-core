package checkpoint

// DirtyHook is embedded in nested structs to propagate dirty notifications
// to the parent DAO's DirtyTracker. The parent wires the callback via SetNotify.
type DirtyHook struct {
	notify func()
}

// SetNotify sets the dirty callback. Called by generated DAO code during init/unmarshal.
func (h *DirtyHook) SetNotify(f func()) {
	h.notify = f
}

// Mark triggers the dirty notification. Called by generated nested struct setters.
func (h *DirtyHook) Mark() {
	if h.notify != nil {
		h.notify()
	}
}
