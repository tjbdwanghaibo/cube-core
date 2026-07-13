package checkpoint

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// EntityExister checks if an entity is already in memory.
type EntityExister interface {
	Exists(id int64) bool
}

// LoadTemplate describes how to load a specific collection.
type LoadTemplate struct {
	Collection string
	DependsOn  []string               // collections that must load first
	Filter     map[string]any         // additional query filter
	BatchSize  int                    // cursor batch hint (0 = default)
	OnLoad     func(doc RawDoc) error // callback per loaded document
	Strict     bool                   // return callback errors instead of skipping bad documents
}

// Loader orchestrates loading from StorageBackend with dependency resolution.
type Loader struct {
	backend StorageBackend
	exister EntityExister // optional: skip entities already in memory
}

// NewLoader creates a Loader.
func NewLoader(backend StorageBackend, exister EntityExister) *Loader {
	return &Loader{
		backend: backend,
		exister: exister,
	}
}

// LoadAll loads all templates respecting dependency order.
// Templates without dependencies are loaded concurrently.
func (l *Loader) LoadAll(ctx context.Context, templates []LoadTemplate) error {
	if len(templates) == 0 {
		return nil
	}

	// Build dependency graph
	order, err := l.topoSort(templates)
	if err != nil {
		return err
	}

	// Group by dependency level for concurrent execution
	levels := l.buildLevels(order, templates)

	for _, level := range levels {
		if err := l.loadLevel(ctx, level, templates); err != nil {
			return err
		}
	}
	return nil
}

func (l *Loader) loadLevel(ctx context.Context, indices []int, templates []LoadTemplate) error {
	if len(indices) == 1 {
		return l.loadOne(ctx, &templates[indices[0]])
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(indices))

	for _, idx := range indices {
		wg.Add(1)
		go func(t *LoadTemplate) {
			defer wg.Done()
			if err := l.loadOne(ctx, t); err != nil {
				errCh <- err
			}
		}(&templates[idx])
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Loader) loadOne(ctx context.Context, t *LoadTemplate) error {
	start := time.Now()

	op := LoadOp{
		Collection: t.Collection,
		Filter:     t.Filter,
		BatchSize:  t.BatchSize,
	}

	docs, err := l.backend.BulkLoad(ctx, op)
	if err != nil {
		return fmt.Errorf("load %s: %w", t.Collection, err)
	}

	var loaded, skipped int
	for _, doc := range docs {
		// Skip if already in memory
		if l.exister != nil && l.exister.Exists(doc.ID) {
			skipped++
			continue
		}

		if t.OnLoad != nil {
			if err := t.OnLoad(doc); err != nil {
				slog.Error("checkpoint load callback error",
					"coll", t.Collection, "id", doc.ID, "err", err)
				if t.Strict {
					return fmt.Errorf("load %s doc %d: %w", t.Collection, doc.ID, err)
				}
				continue
			}
		}
		loaded++
	}

	slog.Info("checkpoint loaded",
		"coll", t.Collection,
		"total", len(docs),
		"loaded", loaded,
		"skipped", skipped,
		"cost_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// topoSort returns template indices in dependency order.
func (l *Loader) topoSort(templates []LoadTemplate) ([]int, error) {
	// Build adjacency: collection name → index
	nameToIdx := make(map[string]int, len(templates))
	for i, t := range templates {
		nameToIdx[t.Collection] = i
	}

	// Build dependency edges for topological sort
	type node struct {
		idx  int
		deps []int
	}
	nodes := make([]node, len(templates))
	for i, t := range templates {
		nodes[i].idx = i
		for _, dep := range t.DependsOn {
			if depIdx, ok := nameToIdx[dep]; ok {
				nodes[i].deps = append(nodes[i].deps, depIdx)
			}
		}
	}

	// Use misc.TopologicalSortCache equivalent (simple Kahn's algorithm here)
	inDegree := make([]int, len(templates))
	adj := make([][]int, len(templates))
	for i, n := range nodes {
		for _, dep := range n.deps {
			adj[dep] = append(adj[dep], i)
			inDegree[i]++
		}
	}

	var queue []int
	for i, d := range inDegree {
		if d == 0 {
			queue = append(queue, i)
		}
	}

	var order []int
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		order = append(order, cur)
		for _, next := range adj[cur] {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if len(order) != len(templates) {
		return nil, fmt.Errorf("checkpoint loader: circular dependency detected")
	}
	return order, nil
}

// buildLevels groups indices by their topological level for concurrent execution.
func (l *Loader) buildLevels(order []int, templates []LoadTemplate) [][]int {
	nameToIdx := make(map[string]int, len(templates))
	for i, t := range templates {
		nameToIdx[t.Collection] = i
	}

	// Compute level for each node (longest path from source)
	level := make([]int, len(templates))
	for _, idx := range order {
		for _, dep := range templates[idx].DependsOn {
			if depIdx, ok := nameToIdx[dep]; ok {
				if level[depIdx]+1 > level[idx] {
					level[idx] = level[depIdx] + 1
				}
			}
		}
	}

	// Group by level
	maxLevel := 0
	for _, lv := range level {
		if lv > maxLevel {
			maxLevel = lv
		}
	}

	levels := make([][]int, maxLevel+1)
	for idx, lv := range level {
		levels[lv] = append(levels[lv], idx)
	}
	return levels
}
