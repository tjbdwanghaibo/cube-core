package checkpoint

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

// SaveMode describes how a SaveOp writes DAO data.
type SaveMode uint8

const (
	// SaveModeFull replaces the full persisted DAO payload.
	SaveModeFull SaveMode = iota
	// SaveModePatch updates only the dirty persisted fields.
	SaveModePatch
)

// DatabaseScope defines how a logical database name is resolved by storage.
// DatabaseScopeServer appends the runtime server ID to the logical name.
type DatabaseScope uint8

const (
	DatabaseScopeGlobal DatabaseScope = iota
	DatabaseScopeServer
)

// ResolveDatabaseScope keeps database scoping optional on DAO types while
// allowing generated persistence code to use one uniform path.
func ResolveDatabaseScope(value any) DatabaseScope {
	if scoped, ok := value.(interface{ DbScope() DatabaseScope }); ok {
		return scoped.DbScope()
	}
	return DatabaseScopeGlobal
}

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
	ret, err := p.Freeze()
	if err != nil {
		// Clone is retained for internal merging of already frozen patches. A
		// value that cannot be made immutable must never escape to an async
		// backend as a patch; force the caller onto its full-data fallback.
		return PersistPatch{FullData: append([]byte(nil), p.FullData...)}
	}
	return ret
}

// Freeze returns an immutable, ownership-independent patch suitable for
// handing to asynchronous persistence workers. It deliberately rejects
// functions, channels, unsafe pointers and cyclic object graphs: retaining
// any of those would reintroduce a race after the entity lock is released.
func (p PersistPatch) Freeze() (PersistPatch, error) {
	ret := PersistPatch{FullData: append([]byte(nil), p.FullData...)}
	if len(p.Set) > 0 {
		ret.Set = make(map[string]any, len(p.Set))
		for k, v := range p.Set {
			frozen, err := freezePersistValue(reflect.ValueOf(v), make(map[visit]bool))
			if err != nil {
				return PersistPatch{}, fmt.Errorf("checkpoint: freeze patch field %q: %w", k, err)
			}
			if frozen.IsValid() {
				ret.Set[k] = frozen.Interface()
			} else {
				ret.Set[k] = nil
			}
		}
	}
	if len(p.Unset) > 0 {
		ret.Unset = append([]string(nil), p.Unset...)
	}
	return ret, nil
}

type visit struct {
	typ reflect.Type
	ptr unsafePointer
}

// unsafePointer is an address used only as an opaque cycle-detection token;
// it does not dereference memory and keeps unsafe out of the persistence API.
type unsafePointer uintptr

var timeType = reflect.TypeOf(time.Time{})

func freezePersistValue(src reflect.Value, visiting map[visit]bool) (reflect.Value, error) {
	if !src.IsValid() {
		return reflect.Value{}, nil
	}
	if src.Type() == timeType {
		return src, nil // time.Time is immutable by contract.
	}
	switch src.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		return src, nil
	case reflect.Interface:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		value, err := freezePersistValue(src.Elem(), visiting)
		if err != nil {
			return reflect.Value{}, err
		}
		ret := reflect.New(src.Type()).Elem()
		ret.Set(value)
		return ret, nil
	case reflect.Pointer:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		key := visit{typ: src.Type(), ptr: unsafePointer(src.Pointer())}
		if visiting[key] {
			return reflect.Value{}, fmt.Errorf("cyclic %s", src.Type())
		}
		visiting[key] = true
		defer delete(visiting, key)
		value, err := freezePersistValue(src.Elem(), visiting)
		if err != nil {
			return reflect.Value{}, err
		}
		ret := reflect.New(src.Type().Elem())
		ret.Elem().Set(value)
		return ret, nil
	case reflect.Slice:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		key := visit{typ: src.Type(), ptr: unsafePointer(src.Pointer())}
		if visiting[key] {
			return reflect.Value{}, fmt.Errorf("cyclic %s", src.Type())
		}
		visiting[key] = true
		defer delete(visiting, key)
		ret := reflect.MakeSlice(src.Type(), src.Len(), src.Len())
		for i := 0; i < src.Len(); i++ {
			value, err := freezePersistValue(src.Index(i), visiting)
			if err != nil {
				return reflect.Value{}, err
			}
			ret.Index(i).Set(value)
		}
		return ret, nil
	case reflect.Array:
		ret := reflect.New(src.Type()).Elem()
		for i := 0; i < src.Len(); i++ {
			value, err := freezePersistValue(src.Index(i), visiting)
			if err != nil {
				return reflect.Value{}, err
			}
			ret.Index(i).Set(value)
		}
		return ret, nil
	case reflect.Map:
		if src.IsNil() {
			return reflect.Zero(src.Type()), nil
		}
		key := visit{typ: src.Type(), ptr: unsafePointer(src.Pointer())}
		if visiting[key] {
			return reflect.Value{}, fmt.Errorf("cyclic %s", src.Type())
		}
		visiting[key] = true
		defer delete(visiting, key)
		ret := reflect.MakeMapWithSize(src.Type(), src.Len())
		iter := src.MapRange()
		for iter.Next() {
			mapKey, err := freezePersistValue(iter.Key(), visiting)
			if err != nil {
				return reflect.Value{}, err
			}
			value, err := freezePersistValue(iter.Value(), visiting)
			if err != nil {
				return reflect.Value{}, err
			}
			ret.SetMapIndex(mapKey, value)
		}
		return ret, nil
	case reflect.Struct:
		ret := reflect.New(src.Type()).Elem()
		for i := 0; i < src.NumField(); i++ {
			field := ret.Field(i)
			if !field.CanSet() || !src.Type().Field(i).IsExported() {
				return reflect.Value{}, fmt.Errorf("unsupported private field in %s", src.Type())
			}
			value, err := freezePersistValue(src.Field(i), visiting)
			if err != nil {
				return reflect.Value{}, err
			}
			field.Set(value)
		}
		return ret, nil
	case reflect.Invalid:
		return reflect.Value{}, nil
	default:
		return reflect.Value{}, fmt.Errorf("unsupported value type %s", src.Type())
	}
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
	DbScope    DatabaseScope
	Collection string
	ID         int64
	Version    uint64
	Fence      uint64
	OwnerSid   int32
	Shared     bool
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
	DbScope    DatabaseScope
	Collection string
	IDs        []int64
	Fence      uint64
	OwnerSid   int32
	Shared     bool
}

// RawDoc is a loaded document with metadata.
type RawDoc struct {
	ID            int64
	Version       uint64
	SchemaVersion uint32
	MarkerEpoch   uint64
	LockFence     uint64
	RouteEpoch    uint64
	DataEnvelope  bool
	Data          []byte // raw BSON
}

// LoadOp describes a bulk load request.
type LoadOp struct {
	Db         string
	DbScope    DatabaseScope
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

// StreamingStorageBackend lets a backend keep load memory bounded by cursor
// batch size. Implementations must stop promptly when consume returns an error.
// Loader falls back to BulkLoad only for older/custom backends that do not
// implement this production capability.
type StreamingStorageBackend interface {
	StreamLoad(ctx context.Context, op LoadOp, consume func(RawDoc) error) error
}

// EntitySnapshotter is implemented by generated persistent entities. Snapshot
// is called while the entity mutex is held and must return immutable items.
type EntitySnapshotter interface {
	Snapshot() []SaveItem
}

// EntityRemoveSnapshotter describes every persisted DAO target for deletion.
// Unlike Snapshot, it must not depend on dirty state.
type EntityRemoveSnapshotter interface {
	RemoveSnapshot() []SaveItem
}
