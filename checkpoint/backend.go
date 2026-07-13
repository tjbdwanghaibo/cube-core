package checkpoint

import "context"

// SaveMode describes how a SaveOp writes DAO data.
type SaveMode uint8

const (
	// SaveModeFull replaces the full persisted DAO payload.
	SaveModeFull SaveMode = iota
	// SaveModePatch updates only the dirty persisted fields.
	SaveModePatch
)

// PersistPatch is a field-level persistence update.
//
// Set and Unset are DAO field names, not database-internal paths. Storage
// backends decide how those fields are represented. FullData is the complete
// DAO payload used when a patch must create a missing document or fall back to a
// safe full write after a write error.
type PersistPatch struct {
	Set      map[string]any
	Unset    []string
	FullData []byte
}

func (p PersistPatch) Empty() bool {
	return len(p.Set) == 0 && len(p.Unset) == 0
}

func (p PersistPatch) SizeHint() int {
	n := len(p.FullData)
	for k := range p.Set {
		n += len(k) + 16
	}
	for _, k := range p.Unset {
		n += len(k) + 8
	}
	return n
}

func (p PersistPatch) Clone() PersistPatch {
	ret := PersistPatch{FullData: p.FullData}
	if len(p.Set) > 0 {
		ret.Set = make(map[string]any, len(p.Set))
		for k, v := range p.Set {
			ret.Set[k] = v
		}
	}
	if len(p.Unset) > 0 {
		ret.Unset = append([]string(nil), p.Unset...)
	}
	return ret
}

// Merge applies next after p and returns the merged patch. Later Set values win;
// setting a field cancels an earlier unset of the same field.
func (p PersistPatch) Merge(next PersistPatch) PersistPatch {
	ret := p.Clone()
	if len(next.FullData) > 0 {
		ret.FullData = next.FullData
	}
	if len(next.Unset) > 0 {
		for _, k := range next.Unset {
			delete(ret.Set, k)
			ret.Unset = append(ret.Unset, k)
		}
	}
	if len(next.Set) > 0 {
		if ret.Set == nil {
			ret.Set = make(map[string]any, len(next.Set))
		}
		for k, v := range next.Set {
			ret.Set[k] = v
			ret.Unset = removeString(ret.Unset, k)
		}
	}
	return ret
}

func removeString(items []string, target string) []string {
	for i := 0; i < len(items); {
		if items[i] == target {
			items = append(items[:i], items[i+1:]...)
			continue
		}
		i++
	}
	return items
}

// PersistPatcher is implemented by generated DAOs that can produce field-level
// persistence patches.
type PersistPatcher interface {
	MarshalPersistPatch(mask uint64) PersistPatch
}

// SaveOp represents a single document save operation.
type SaveOp struct {
	Db         string
	Collection string
	ID         int64
	Version    uint64
	Mask       uint64
	Mode       SaveMode
	Data       []byte       // full serialized document (e.g. BSON)
	Patch      PersistPatch // field-level update when Mode is SaveModePatch
}

// SaveResult is the outcome of a single SaveOp.
type SaveResult struct {
	OK              bool
	VersionConflict bool // CAS failed: stored version >= op version
	Err             error
}

// RemoveOp represents a batch remove operation.
type RemoveOp struct {
	Db         string
	Collection string
	IDs        []int64
}

// RawDoc is a loaded document with metadata.
type RawDoc struct {
	ID            int64
	Version       uint64
	SchemaVersion uint32
	Data          []byte // raw BSON
}

// LoadOp describes a bulk load request.
type LoadOp struct {
	Collection string
	Filter     map[string]any // optional query filter
	BatchSize  int            // cursor batch size hint
}

// StorageBackend abstracts the persistence layer.
type StorageBackend interface {
	// BulkSave writes documents in batch with version-based CAS.
	// Returns one result per op, in the same order.
	BulkSave(ctx context.Context, ops []SaveOp) ([]SaveResult, error)

	// BulkLoad loads all documents matching the criteria.
	BulkLoad(ctx context.Context, op LoadOp) ([]RawDoc, error)

	// BulkRemove deletes documents by IDs.
	BulkRemove(ctx context.Context, op RemoveOp) error
}
