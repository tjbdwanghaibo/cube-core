package featureflag

import (
	"sync"
	"sync/atomic"
)

type Flag struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	Note    string `json:"note,omitempty"`
}

type Store struct {
	mu    sync.RWMutex
	flags map[string]Flag
	ver   atomic.Uint64
}

var defaultStore = NewStore()

func DefaultStore() *Store {
	return defaultStore
}

func Enabled(name string) bool {
	return DefaultStore().Enabled(name)
}

func Set(flag Flag) {
	DefaultStore().Set(flag)
}

func NewStore() *Store {
	return &Store{flags: make(map[string]Flag)}
}

func (s *Store) Set(flag Flag) {
	if s == nil || flag.Name == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flags == nil {
		s.flags = make(map[string]Flag)
	}
	s.flags[flag.Name] = flag
	s.ver.Add(1)
}

func (s *Store) Replace(flags []Flag) {
	if s == nil {
		return
	}
	next := make(map[string]Flag, len(flags))
	for _, flag := range flags {
		if flag.Name != "" {
			next[flag.Name] = flag
		}
	}
	s.mu.Lock()
	s.flags = next
	s.mu.Unlock()
	s.ver.Add(1)
}

func (s *Store) Enabled(name string) bool {
	if s == nil || name == "" {
		return false
	}
	s.mu.RLock()
	flag, ok := s.flags[name]
	s.mu.RUnlock()
	return ok && flag.Enabled
}

func (s *Store) Snapshot() []Flag {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Flag, 0, len(s.flags))
	for _, flag := range s.flags {
		out = append(out, flag)
	}
	return out
}

func (s *Store) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.ver.Load()
}
