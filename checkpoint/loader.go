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
	Db         string
	DbScope    DatabaseScope
	Collection string
	DependsOn  []string               // collections that must load first
	Filter     map[string]any         // additional query filter
	BatchSize  int                    // cursor batch hint (0 = default)
	OnLoad     func(doc RawDoc) error // callback per loaded document
	Strict     bool                   // return callback errors instead of skipping bad documents
}

// Loader orchestrates loading from StorageBackend with dependency resolution.
type Loader struct {
	backend     StorageBackend
	exister     EntityExister // optional: skip entities already in memory
	concurrency int
}

// NewLoader creates a Loader.
func NewLoader(backend StorageBackend, exister EntityExister, concurrency ...int) *Loader {
	limit := 4
	if len(concurrency) > 0 && concurrency[0] > 0 {
		limit = concurrency[0]
	}
	return &Loader{
		backend:     backend,
		exister:     exister,
		concurrency: limit,
	}
}

// LoadAll loads all templates respecting dependency order.
// Templates without dependencies are loaded concurrently.
func (l *Loader) LoadAll(ctx context.Context, templates []LoadTemplate) error {
	if l == nil || l.backend == nil {
		return ErrCheckpointBackendRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
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
	if len(indices) == 0 {
		return nil
	}
	if len(indices) == 1 {
		return l.loadOne(ctx, &templates[indices[0]])
	}

	workerCount := min(l.concurrency, len(indices))
	if workerCount < 1 {
		workerCount = 1
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if err := l.loadOne(workCtx, &templates[idx]); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}

sendLoop:
	for _, idx := range indices {
		select {
		case jobs <- idx:
		case <-workCtx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return ctx.Err()
}

func (l *Loader) loadOne(ctx context.Context, t *LoadTemplate) error {
	start := time.Now()

	op := LoadOp{
		Db:         t.Db,
		DbScope:    t.DbScope,
		Collection: t.Collection,
		Filter:     t.Filter,
		BatchSize:  t.BatchSize,
	}

	var loaded, skipped int
	consume := func(doc RawDoc) error {
		// Skip if already in memory
		if l.exister != nil && l.exister.Exists(doc.ID) {
			skipped++
			return nil
		}

		if t.OnLoad != nil {
			if err := t.OnLoad(doc); err != nil {
				slog.Error("checkpoint load callback error",
					"coll", t.Collection, "id", doc.ID, "err", err)
				if t.Strict {
					return fmt.Errorf("load %s doc %d: %w", t.Collection, doc.ID, err)
				}
				return nil
			}
		}
		loaded++
		return nil
	}

	if streaming, ok := l.backend.(StreamingStorageBackend); ok {
		if err := streaming.StreamLoad(ctx, op, consume); err != nil {
			return fmt.Errorf("load %s: %w", t.Collection, err)
		}
	} else {
		docs, err := l.backend.BulkLoad(ctx, op)
		if err != nil {
			return fmt.Errorf("load %s: %w", t.Collection, err)
		}
		for _, doc := range docs {
			if err := consume(doc); err != nil {
				return err
			}
		}
	}

	slog.Info("checkpoint loaded",
		"coll", t.Collection,
		"total", loaded+skipped,
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
		if t.Collection == "" {
			return nil, fmt.Errorf("checkpoint loader: template %d has empty collection", i)
		}
		if _, exists := nameToIdx[t.Collection]; exists {
			return nil, fmt.Errorf("checkpoint loader: duplicate collection %q", t.Collection)
		}
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
			depIdx, ok := nameToIdx[dep]
			if !ok {
				return nil, fmt.Errorf("checkpoint loader: collection %q depends on unknown collection %q", t.Collection, dep)
			}
			nodes[i].deps = append(nodes[i].deps, depIdx)
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
