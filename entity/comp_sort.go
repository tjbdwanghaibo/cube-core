package entity

import (
	"fmt"
	"log/slog"
	"sync"
)

type compTopologicalSort struct {
	cache        []ComponentType
	cacheLock    sync.RWMutex
	isCacheValid bool

	dependLock   sync.RWMutex
	dependencies map[ComponentType][]ComponentType
}

func newCompTopologicalSort() *compTopologicalSort {
	return &compTopologicalSort{
		dependencies: make(map[ComponentType][]ComponentType),
	}
}

func (t *compTopologicalSort) RegisterCompDependency(tT ComponentType, dependencies ...ComponentType) {
	t.dependLock.Lock()
	defer t.dependLock.Unlock()

	if _, exists := t.dependencies[tT]; exists {
		slog.Error("component dependency already registered", "type", tT)
		return
	}
	t.dependencies[tT] = dependencies
	t.cacheLock.Lock()
	t.isCacheValid = false
	t.cache = nil
	t.cacheLock.Unlock()
}

func (t *compTopologicalSort) GetTopologicalSortedComponents() ([]ComponentType, error) {
	t.cacheLock.RLock()
	if t.isCacheValid && t.cache != nil {
		cached := make([]ComponentType, len(t.cache))
		copy(cached, t.cache)
		t.cacheLock.RUnlock()
		return cached, nil
	}
	t.cacheLock.RUnlock()

	t.dependLock.RLock()
	defer t.dependLock.RUnlock()

	allComponents := make(map[ComponentType]bool)
	for compType := range t.dependencies {
		allComponents[compType] = true
	}

	inDegree := make(map[ComponentType]int)
	graph := make(map[ComponentType][]ComponentType)
	for compType := range allComponents {
		inDegree[compType] = 0
		graph[compType] = []ComponentType{}
	}

	for compType, deps := range t.dependencies {
		for _, dep := range deps {
			if _, exists := inDegree[dep]; !exists {
				inDegree[dep] = 0
			}
			inDegree[dep]++
			graph[compType] = append(graph[compType], dep)
		}
	}

	var queue []ComponentType
	for compType, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, compType)
		}
	}

	var sorted []ComponentType
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		sorted = append(sorted, current)
		for _, neighbor := range graph[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(sorted) != len(inDegree) {
		slog.Error("circular dependency detected in component topological sort")
		return nil, fmt.Errorf("circular dependency detected in component topological sort")
	}

	for i, j := 0, len(sorted)-1; i < j; i, j = i+1, j-1 {
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}

	t.cacheLock.Lock()
	t.cache = make([]ComponentType, len(sorted))
	copy(t.cache, sorted)
	t.isCacheValid = true
	t.cacheLock.Unlock()

	result := make([]ComponentType, len(sorted))
	copy(result, sorted)
	return result, nil
}
