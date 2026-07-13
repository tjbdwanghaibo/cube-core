package fmap

const (
	slotEmpty uint8 = iota
	slotFilled
	slotDeleted
)

// FastMap is a non-thread-safe open-addressing map for hot paths already
// protected by an outer lock or owned by a single worker.
type FastMap[K comparable, V any] struct {
	hash   HashFunc[K]
	keys   []K
	values []V
	states []uint8
	size   int
	used   int
}

func NewFastMap[K comparable, V any](capHint int, hash HashFunc[K]) *FastMap[K, V] {
	if hash == nil {
		panic("fmap: nil hash func")
	}
	capacity := fastMapCapacityForHint(capHint)
	return &FastMap[K, V]{
		hash:   hash,
		keys:   make([]K, capacity),
		values: make([]V, capacity),
		states: make([]uint8, capacity),
	}
}

func NewIntegerFastMap[K Integer, V any](capHint int) *FastMap[K, V] {
	return NewFastMap[K, V](capHint, HashInteger[K])
}

func NewInt64FastMap[V any](capHint int) *FastMap[int64, V] {
	return NewIntegerFastMap[int64, V](capHint)
}

func NewUint64FastMap[V any](capHint int) *FastMap[uint64, V] {
	return NewIntegerFastMap[uint64, V](capHint)
}

func NewStringFastMap[V any](capHint int) *FastMap[string, V] {
	return NewFastMap[string, V](capHint, HashString)
}

func (m *FastMap[K, V]) Set(key K, value V) {
	m.ensureWritable()

	idx, found := m.findSlot(key)
	if found {
		m.values[idx] = value
		return
	}
	if m.states[idx] == slotEmpty {
		m.used++
	}
	m.keys[idx] = key
	m.values[idx] = value
	m.states[idx] = slotFilled
	m.size++
}

func (m *FastMap[K, V]) Get(key K) (V, bool) {
	if len(m.states) == 0 {
		var zero V
		return zero, false
	}
	idx, found := m.findExisting(key)
	if !found {
		var zero V
		return zero, false
	}
	return m.values[idx], true
}

func (m *FastMap[K, V]) Delete(key K) bool {
	if len(m.states) == 0 {
		return false
	}
	idx, found := m.findExisting(key)
	if !found {
		return false
	}
	var zeroK K
	var zeroV V
	m.keys[idx] = zeroK
	m.values[idx] = zeroV
	m.states[idx] = slotDeleted
	m.size--
	if m.size == 0 {
		m.Clear()
	}
	return true
}

func (m *FastMap[K, V]) Len() int {
	return m.size
}

func (m *FastMap[K, V]) Clear() {
	clear(m.keys)
	clear(m.values)
	m.keys = nil
	m.values = nil
	m.states = nil
	m.size = 0
	m.used = 0
}

func (m *FastMap[K, V]) Range(f func(key K, value V) bool) {
	if f == nil {
		return
	}
	for i, state := range m.states {
		if state != slotFilled {
			continue
		}
		if !f(m.keys[i], m.values[i]) {
			return
		}
	}
}

func (m *FastMap[K, V]) Cap() int {
	return len(m.states)
}

func (m *FastMap[K, V]) ensureWritable() {
	if len(m.states) == 0 {
		m.rehash(minFastMapCapacity)
		return
	}
	if (m.used+1)*100 <= len(m.states)*fastMapLoadPercent {
		return
	}
	newCapacity := len(m.states) * fastMapRehashFactor
	if m.size*100 < len(m.states)*(fastMapLoadPercent/2) {
		newCapacity = len(m.states)
	}
	m.rehash(newCapacity)
}

func (m *FastMap[K, V]) rehash(capacity int) {
	if capacity < minFastMapCapacity {
		capacity = minFastMapCapacity
	}
	capacity = nextPowerOfTwo(capacity)
	oldKeys := m.keys
	oldValues := m.values
	oldStates := m.states

	m.keys = make([]K, capacity)
	m.values = make([]V, capacity)
	m.states = make([]uint8, capacity)
	m.size = 0
	m.used = 0

	for i, state := range oldStates {
		if state == slotFilled {
			m.setNoGrow(oldKeys[i], oldValues[i])
		}
	}
}

func (m *FastMap[K, V]) setNoGrow(key K, value V) {
	idx, found := m.findSlot(key)
	if found {
		m.values[idx] = value
		return
	}
	if m.states[idx] == slotEmpty {
		m.used++
	}
	m.keys[idx] = key
	m.values[idx] = value
	m.states[idx] = slotFilled
	m.size++
}

func (m *FastMap[K, V]) findExisting(key K) (int, bool) {
	mask := uint64(len(m.states) - 1)
	idx := m.hash(key) & mask
	for {
		switch m.states[idx] {
		case slotEmpty:
			return 0, false
		case slotFilled:
			if m.keys[idx] == key {
				return int(idx), true
			}
		}
		idx = (idx + 1) & mask
	}
}

func (m *FastMap[K, V]) findSlot(key K) (int, bool) {
	mask := uint64(len(m.states) - 1)
	idx := m.hash(key) & mask
	firstDeleted := -1
	for {
		switch m.states[idx] {
		case slotEmpty:
			if firstDeleted >= 0 {
				return firstDeleted, false
			}
			return int(idx), false
		case slotDeleted:
			if firstDeleted < 0 {
				firstDeleted = int(idx)
			}
		case slotFilled:
			if m.keys[idx] == key {
				return int(idx), true
			}
		}
		idx = (idx + 1) & mask
	}
}

var _ IMap[int64, int64] = (*FastMap[int64, int64])(nil)
