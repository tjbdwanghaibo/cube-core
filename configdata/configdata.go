package configdata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/lifecycle"
)

// Name is the stable identifier for a business configuration object.
type Name string

var (
	ErrSnapshotNotFound = errors.New("configdata: snapshot not found")
	ErrTableNotFound    = errors.New("configdata: table not found")
	ErrObjectNotFound   = errors.New("configdata: object not found")
	ErrCustomNotFound   = errors.New("configdata: custom data not found")
)

// Snapshot is an immutable, point-in-time view of all business configuration.
// A running handler should keep reading the same snapshot through the bound request Context even if
// the global Store is hot-reloaded while the handler is executing.
type Snapshot struct {
	Version  uint64
	LoadedAt time.Time
	Hash     string

	tables  map[Name]any
	objects map[Name]any
	custom  map[Name]any
}

func newSnapshot(version uint64) *Snapshot {
	return &Snapshot{
		Version:  version,
		LoadedAt: time.Now(),
		tables:   make(map[Name]any),
		objects:  make(map[Name]any),
		custom:   make(map[Name]any),
	}
}

func (s *Snapshot) table(name Name) (any, bool) {
	if s == nil {
		return nil, false
	}
	v, ok := s.tables[name]
	return v, ok
}

func (s *Snapshot) object(name Name) (any, bool) {
	if s == nil {
		return nil, false
	}
	v, ok := s.objects[name]
	return v, ok
}

func (s *Snapshot) customData(name Name) (any, bool) {
	if s == nil {
		return nil, false
	}
	v, ok := s.custom[name]
	return v, ok
}

func (s *Snapshot) finalize() {
	if s == nil {
		return
	}
	var parts []string
	appendPart := func(kind string, values map[Name]any) {
		names := make([]string, 0, len(values))
		for name := range values {
			names = append(names, string(name))
		}
		sort.Strings(names)
		for _, name := range names {
			raw, _ := json.Marshal(values[Name(name)])
			parts = append(parts, kind+":"+name+":"+string(raw))
		}
	}
	appendPart("table", s.tables)
	appendPart("object", s.objects)
	appendPart("custom", s.custom)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	s.Hash = hex.EncodeToString(sum[:])
}

// Table is an immutable keyed business configuration table.
type Table[K comparable, V any] struct {
	name    Name
	rows    []V
	byKey   map[K]int
	indexes map[string]map[string][]int
}

func newTable[K comparable, V any](def TableDef[K, V], rows []V) (*Table[K, V], error) {
	t := &Table[K, V]{
		name:    def.Name,
		rows:    append([]V(nil), rows...),
		byKey:   make(map[K]int, len(rows)),
		indexes: make(map[string]map[string][]int, len(def.Indexes)),
	}
	for i, row := range t.rows {
		key := def.Key(row)
		if _, exists := t.byKey[key]; exists {
			return nil, fmt.Errorf("configdata: table %s duplicate key %v", def.Name, key)
		}
		t.byKey[key] = i
		for _, idx := range def.Indexes {
			if idx.Name == "" || idx.Key == nil {
				continue
			}
			value := idx.Key(row)
			if value == "" && idx.SkipEmpty {
				continue
			}
			m := t.indexes[idx.Name]
			if m == nil {
				m = make(map[string][]int)
				t.indexes[idx.Name] = m
			}
			m[value] = append(m[value], i)
		}
	}
	return t, nil
}

func (t *Table[K, V]) Name() Name {
	if t == nil {
		return ""
	}
	return t.name
}

func (t *Table[K, V]) Len() int {
	if t == nil {
		return 0
	}
	return len(t.rows)
}

func (t *Table[K, V]) Get(key K) (V, bool) {
	var zero V
	if t == nil {
		return zero, false
	}
	idx, ok := t.byKey[key]
	if !ok {
		return zero, false
	}
	return t.rows[idx], true
}

func (t *Table[K, V]) MustGet(key K) V {
	v, ok := t.Get(key)
	if !ok {
		panic(fmt.Sprintf("configdata: table %s key %v not found", t.name, key))
	}
	return v
}

func (t *Table[K, V]) Rows() []V {
	if t == nil {
		return nil
	}
	return append([]V(nil), t.rows...)
}

func (t *Table[K, V]) GetByIndex(indexName string, value string) []V {
	if t == nil {
		return nil
	}
	idx := t.indexes[indexName]
	if len(idx) == 0 {
		return nil
	}
	rowIndexes := idx[value]
	if len(rowIndexes) == 0 {
		return nil
	}
	ret := make([]V, 0, len(rowIndexes))
	for _, rowIndex := range rowIndexes {
		ret = append(ret, t.rows[rowIndex])
	}
	return ret
}

// BuildContext exposes already-loaded config while building and validating a
// new snapshot. It is never shared with live handlers.
type BuildContext struct {
	Dir      string
	Snapshot *Snapshot
}

func TableFrom[K comparable, V any](snap *Snapshot, name Name) (*Table[K, V], bool) {
	raw, ok := snap.table(name)
	if !ok {
		return nil, false
	}
	table, ok := raw.(*Table[K, V])
	return table, ok
}

func MustTableFrom[K comparable, V any](snap *Snapshot, name Name) *Table[K, V] {
	table, ok := TableFrom[K, V](snap, name)
	if !ok || table == nil {
		panic(fmt.Sprintf("%v: %s", ErrTableNotFound, name))
	}
	return table
}

func ObjectFrom[V any](snap *Snapshot, name Name) (V, bool) {
	var zero V
	raw, ok := snap.object(name)
	if !ok {
		return zero, false
	}
	obj, ok := raw.(V)
	if !ok {
		return zero, false
	}
	return obj, true
}

func MustObjectFrom[V any](snap *Snapshot, name Name) V {
	obj, ok := ObjectFrom[V](snap, name)
	if !ok {
		panic(fmt.Sprintf("%v: %s", ErrObjectNotFound, name))
	}
	return obj
}

func CustomFrom[V any](snap *Snapshot, name Name) (V, bool) {
	var zero V
	raw, ok := snap.customData(name)
	if !ok {
		return zero, false
	}
	obj, ok := raw.(V)
	if !ok {
		return zero, false
	}
	return obj, true
}

func MustCustomFrom[V any](snap *Snapshot, name Name) V {
	obj, ok := CustomFrom[V](snap, name)
	if !ok {
		panic(fmt.Sprintf("%v: %s", ErrCustomNotFound, name))
	}
	return obj
}

type tableDef interface {
	name() Name
	file() string
	load(*BuildContext) (any, error)
	validate(*BuildContext, any) error
}

type objectDef interface {
	name() Name
	file() string
	load(*BuildContext) (any, error)
	validate(*BuildContext, any) error
}

type customDef interface {
	name() Name
	build(*BuildContext) (any, error)
	validate(*BuildContext, any) error
}

// IndexDef describes a string-keyed secondary index. Generated config getters
// can wrap the string key with typed helpers.
type IndexDef[V any] struct {
	Name      string
	Key       func(V) string
	SkipEmpty bool
}

// TableDef describes a JSON-backed keyed table.
type TableDef[K comparable, V any] struct {
	Name     Name
	File     string
	Key      func(V) K
	Indexes  []IndexDef[V]
	Validate func(*BuildContext, V) error
}

func (d TableDef[K, V]) name() Name   { return d.Name }
func (d TableDef[K, V]) file() string { return d.File }

func (d TableDef[K, V]) load(ctx *BuildContext) (any, error) {
	if d.Name == "" {
		return nil, errors.New("configdata: table name is empty")
	}
	if d.File == "" {
		return nil, fmt.Errorf("configdata: table %s file is empty", d.Name)
	}
	if d.Key == nil {
		return nil, fmt.Errorf("configdata: table %s key func is nil", d.Name)
	}
	var rows []V
	if err := readJSON(filepath.Join(ctx.Dir, d.File), &rows); err != nil {
		return nil, fmt.Errorf("configdata: load table %s: %w", d.Name, err)
	}
	return newTable(d, rows)
}

func (d TableDef[K, V]) validate(ctx *BuildContext, raw any) error {
	if d.Validate == nil {
		return nil
	}
	table, ok := raw.(*Table[K, V])
	if !ok {
		return fmt.Errorf("configdata: table %s type mismatch", d.Name)
	}
	for _, row := range table.rows {
		if err := d.Validate(ctx, row); err != nil {
			return fmt.Errorf("configdata: validate table %s: %w", d.Name, err)
		}
	}
	return nil
}

// ObjectDef describes a JSON object config.
type ObjectDef[V any] struct {
	Name     Name
	File     string
	Validate func(*BuildContext, V) error
}

func (d ObjectDef[V]) name() Name   { return d.Name }
func (d ObjectDef[V]) file() string { return d.File }

func (d ObjectDef[V]) load(ctx *BuildContext) (any, error) {
	if d.Name == "" {
		return nil, errors.New("configdata: object name is empty")
	}
	if d.File == "" {
		return nil, fmt.Errorf("configdata: object %s file is empty", d.Name)
	}
	var obj V
	if err := readJSON(filepath.Join(ctx.Dir, d.File), &obj); err != nil {
		return nil, fmt.Errorf("configdata: load object %s: %w", d.Name, err)
	}
	return obj, nil
}

func (d ObjectDef[V]) validate(ctx *BuildContext, raw any) error {
	if d.Validate == nil {
		return nil
	}
	obj, ok := raw.(V)
	if !ok {
		return fmt.Errorf("configdata: object %s type mismatch", d.Name)
	}
	if err := d.Validate(ctx, obj); err != nil {
		return fmt.Errorf("configdata: validate object %s: %w", d.Name, err)
	}
	return nil
}

// CustomDef builds runtime-only config from already-loaded table/object data.
type CustomDef[V any] struct {
	Name     Name
	Build    func(*BuildContext) (V, error)
	Validate func(*BuildContext, V) error
}

func (d CustomDef[V]) name() Name { return d.Name }

func (d CustomDef[V]) build(ctx *BuildContext) (any, error) {
	if d.Name == "" {
		return nil, errors.New("configdata: custom name is empty")
	}
	if d.Build == nil {
		return nil, fmt.Errorf("configdata: custom %s build func is nil", d.Name)
	}
	v, err := d.Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("configdata: build custom %s: %w", d.Name, err)
	}
	return v, nil
}

func (d CustomDef[V]) validate(ctx *BuildContext, raw any) error {
	if d.Validate == nil {
		return nil
	}
	obj, ok := raw.(V)
	if !ok {
		return fmt.Errorf("configdata: custom %s type mismatch", d.Name)
	}
	if err := d.Validate(ctx, obj); err != nil {
		return fmt.Errorf("configdata: validate custom %s: %w", d.Name, err)
	}
	return nil
}

// Registry stores schema definitions. It is normally populated during game
// bootstrap, before Store.Load or Store.Reload is called.
type Registry struct {
	mu      sync.RWMutex
	tables  []tableDef
	objects []objectDef
	custom  []customDef
	names   map[Name]string
}

func NewRegistry() *Registry {
	return &Registry{names: make(map[Name]string)}
}

func (r *Registry) RegisterTable(def tableDef) error {
	if def == nil {
		return errors.New("configdata: nil table def")
	}
	return r.register(def.name(), "table", func() {
		r.tables = append(r.tables, def)
	})
}

func (r *Registry) RegisterObject(def objectDef) error {
	if def == nil {
		return errors.New("configdata: nil object def")
	}
	return r.register(def.name(), "object", func() {
		r.objects = append(r.objects, def)
	})
}

func (r *Registry) RegisterCustom(def customDef) error {
	if def == nil {
		return errors.New("configdata: nil custom def")
	}
	return r.register(def.name(), "custom", func() {
		r.custom = append(r.custom, def)
	})
}

func (r *Registry) register(name Name, kind string, appendDef func()) error {
	if name == "" {
		return fmt.Errorf("configdata: %s name is empty", kind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if oldKind, exists := r.names[name]; exists {
		if oldKind == kind {
			return nil
		}
		return fmt.Errorf("configdata: duplicate config name %s old=%s new=%s", name, oldKind, kind)
	}
	r.names[name] = kind
	appendDef()
	return nil
}

func (r *Registry) defs() ([]tableDef, []objectDef, []customDef) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]tableDef(nil), r.tables...), append([]objectDef(nil), r.objects...), append([]customDef(nil), r.custom...)
}

func RegisterTable[K comparable, V any](r *Registry, def TableDef[K, V]) error {
	if r == nil {
		return errors.New("configdata: registry is nil")
	}
	return r.RegisterTable(def)
}

func MustRegisterTable[K comparable, V any](r *Registry, def TableDef[K, V]) {
	if err := RegisterTable(r, def); err != nil {
		panic(err)
	}
}

func RegisterObject[V any](r *Registry, def ObjectDef[V]) error {
	if r == nil {
		return errors.New("configdata: registry is nil")
	}
	return r.RegisterObject(def)
}

func MustRegisterObject[V any](r *Registry, def ObjectDef[V]) {
	if err := RegisterObject(r, def); err != nil {
		panic(err)
	}
}

func RegisterCustom[V any](r *Registry, def CustomDef[V]) error {
	if r == nil {
		return errors.New("configdata: registry is nil")
	}
	return r.RegisterCustom(def)
}

func MustRegisterCustom[V any](r *Registry, def CustomDef[V]) {
	if err := RegisterCustom(r, def); err != nil {
		panic(err)
	}
}

type ReloadEvent struct {
	Reason    string
	Old       *Snapshot
	New       *Snapshot
	StartedAt time.Time
	AppliedAt time.Time
}

type ReloadListener interface {
	Name() string
	ValidateReload(context.Context, ReloadEvent) error
	BeforeApplyReload(context.Context, ReloadEvent) error
	AfterApplyReload(context.Context, ReloadEvent) error
	RollbackReload(context.Context, ReloadEvent, error)
}

type ReloadHook struct {
	HookName    string
	Validate    func(context.Context, ReloadEvent) error
	BeforeApply func(context.Context, ReloadEvent) error
	AfterApply  func(context.Context, ReloadEvent) error
	Rollback    func(context.Context, ReloadEvent, error)
}

func (h ReloadHook) Name() string {
	if h.HookName == "" {
		return "anonymous"
	}
	return h.HookName
}

func (h ReloadHook) ValidateReload(ctx context.Context, event ReloadEvent) error {
	if h.Validate == nil {
		return nil
	}
	return h.Validate(ctx, event)
}

func (h ReloadHook) BeforeApplyReload(ctx context.Context, event ReloadEvent) error {
	if h.BeforeApply == nil {
		return nil
	}
	return h.BeforeApply(ctx, event)
}

func (h ReloadHook) AfterApplyReload(ctx context.Context, event ReloadEvent) error {
	if h.AfterApply == nil {
		return nil
	}
	return h.AfterApply(ctx, event)
}

func (h ReloadHook) RollbackReload(ctx context.Context, event ReloadEvent, cause error) {
	if h.Rollback != nil {
		h.Rollback(ctx, event, cause)
	}
}

// Store owns the current immutable config snapshot and supports hot reload by
// atomically publishing a newly built snapshot.
type Store struct {
	registry  *Registry
	dir       string
	lifecycle *lifecycle.Registry
	current   atomic.Pointer[Snapshot]
	previous  atomic.Pointer[Snapshot]
	version   atomic.Uint64
	mu        sync.Mutex
	listMu    sync.RWMutex
	listener  []reloadListenerEntry
	nextID    uint64
}

type reloadListenerEntry struct {
	id       uint64
	listener ReloadListener
}

func NewStore(registry *Registry, dir string) *Store {
	if registry == nil {
		registry = NewRegistry()
	}
	return &Store{registry: registry, dir: dir}
}

func (s *Store) Registry() *Registry {
	if s == nil {
		return nil
	}
	return s.registry
}

func (s *Store) Dir() string {
	if s == nil {
		return ""
	}
	return s.dir
}

func (s *Store) SetLifecycleRegistry(reg *lifecycle.Registry) {
	if s == nil {
		return
	}
	s.listMu.Lock()
	s.lifecycle = reg
	s.listMu.Unlock()
}

func (s *Store) SetDir(dir string) {
	if s != nil {
		s.dir = dir
	}
}

func (s *Store) Current() *Snapshot {
	if s == nil {
		return nil
	}
	return s.current.Load()
}

func (s *Store) Load(ctx context.Context) (*Snapshot, error) {
	return s.Reload(ctx)
}

func (s *Store) Reload(ctx context.Context) (*Snapshot, error) {
	return s.ReloadWithReason(ctx, "manual")
}

func (s *Store) ReloadWithReason(ctx context.Context, reason string) (*Snapshot, error) {
	if s == nil {
		return nil, errors.New("configdata: store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.current.Load()
	oldVersion := s.version.Load()
	nextVersion := oldVersion + 1
	started := time.Now()
	snap, err := s.build(ctx, nextVersion)
	if err != nil {
		return nil, err
	}
	event := ReloadEvent{Reason: reason, Old: old, New: snap, StartedAt: started}
	listeners := s.reloadListeners()
	if err := runReloadValidate(ctx, listeners, event); err != nil {
		return nil, err
	}
	if err := runReloadBeforeApply(ctx, listeners, event); err != nil {
		return nil, err
	}
	event.AppliedAt = time.Now()
	s.current.Store(snap)
	SetDefaultStore(s)
	fctx.SetRuntimeConfig(snap)
	if err := s.emitConfigReload(ctx, lifecycle.Event{
		Phase: lifecycle.PhaseConfigReload,
		Name:  "configdata",
		Data: map[string]any{
			"reason":  reason,
			"version": snap.Version,
			"hash":    snap.Hash,
		},
	}); err != nil {
		s.current.Store(old)
		s.version.Store(oldVersion)
		fctx.SetRuntimeConfig(old)
		runReloadRollback(ctx, listeners, event, err)
		return nil, err
	}
	if err := runReloadAfterApply(ctx, listeners, event); err != nil {
		s.current.Store(old)
		s.version.Store(oldVersion)
		fctx.SetRuntimeConfig(old)
		runReloadRollback(ctx, listeners, event, err)
		return nil, err
	}
	if old != nil {
		s.previous.Store(old)
	}
	s.version.Store(nextVersion)
	return snap, nil
}

func (s *Store) DryRun(ctx context.Context, reason string) (*Snapshot, error) {
	if s == nil {
		return nil, errors.New("configdata: store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.current.Load()
	started := time.Now()
	snap, err := s.build(ctx, s.version.Load()+1)
	if err != nil {
		return nil, err
	}
	event := ReloadEvent{Reason: reason, Old: old, New: snap, StartedAt: started}
	if err := runReloadValidate(ctx, s.reloadListeners(), event); err != nil {
		return nil, err
	}
	return snap, nil
}

func (s *Store) Rollback(ctx context.Context, reason string) (*Snapshot, error) {
	if s == nil {
		return nil, errors.New("configdata: store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.previous.Load()
	if prev == nil {
		return nil, errors.New("configdata: previous snapshot not found")
	}
	old := s.current.Load()
	started := time.Now()
	event := ReloadEvent{Reason: reason, Old: old, New: prev, StartedAt: started}
	listeners := s.reloadListeners()
	if err := runReloadValidate(ctx, listeners, event); err != nil {
		return nil, err
	}
	if err := runReloadBeforeApply(ctx, listeners, event); err != nil {
		return nil, err
	}
	event.AppliedAt = time.Now()
	s.current.Store(prev)
	SetDefaultStore(s)
	fctx.SetRuntimeConfig(prev)
	if err := s.emitConfigReload(ctx, lifecycle.Event{
		Phase: lifecycle.PhaseConfigReload,
		Name:  "configdata.rollback",
		Data: map[string]any{
			"reason":  reason,
			"version": prev.Version,
			"hash":    prev.Hash,
		},
	}); err != nil {
		s.current.Store(old)
		fctx.SetRuntimeConfig(old)
		runReloadRollback(ctx, listeners, event, err)
		return nil, err
	}
	if err := runReloadAfterApply(ctx, listeners, event); err != nil {
		s.current.Store(old)
		fctx.SetRuntimeConfig(old)
		runReloadRollback(ctx, listeners, event, err)
		return nil, err
	}
	if old != nil {
		s.previous.Store(old)
	}
	s.version.Store(prev.Version)
	return prev, nil
}

func (s *Store) emitConfigReload(ctx context.Context, event lifecycle.Event) error {
	if s == nil {
		return nil
	}
	s.listMu.RLock()
	reg := s.lifecycle
	s.listMu.RUnlock()
	if reg == nil {
		return nil
	}
	return reg.Emit(ctx, event)
}

func (s *Store) AddReloadListener(listener ReloadListener) func() {
	if s == nil || listener == nil {
		return func() {}
	}
	s.listMu.Lock()
	s.nextID++
	id := s.nextID
	s.listener = append(s.listener, reloadListenerEntry{id: id, listener: listener})
	s.listMu.Unlock()
	return func() {
		s.listMu.Lock()
		defer s.listMu.Unlock()
		for i, item := range s.listener {
			if item.id == id {
				s.listener = append(s.listener[:i], s.listener[i+1:]...)
				return
			}
		}
	}
}

func (s *Store) reloadListeners() []ReloadListener {
	if s == nil {
		return nil
	}
	s.listMu.RLock()
	defer s.listMu.RUnlock()
	listeners := make([]ReloadListener, 0, len(s.listener))
	for _, item := range s.listener {
		listeners = append(listeners, item.listener)
	}
	return listeners
}

func runReloadValidate(ctx context.Context, listeners []ReloadListener, event ReloadEvent) error {
	for _, listener := range listeners {
		if listener == nil {
			continue
		}
		if err := listener.ValidateReload(ctx, event); err != nil {
			return fmt.Errorf("configdata: reload validate %s: %w", listener.Name(), err)
		}
	}
	return nil
}

func runReloadBeforeApply(ctx context.Context, listeners []ReloadListener, event ReloadEvent) error {
	for _, listener := range listeners {
		if listener == nil {
			continue
		}
		if err := listener.BeforeApplyReload(ctx, event); err != nil {
			return fmt.Errorf("configdata: reload before apply %s: %w", listener.Name(), err)
		}
	}
	return nil
}

func runReloadAfterApply(ctx context.Context, listeners []ReloadListener, event ReloadEvent) error {
	for _, listener := range listeners {
		if listener == nil {
			continue
		}
		if err := listener.AfterApplyReload(ctx, event); err != nil {
			return fmt.Errorf("configdata: reload after apply %s: %w", listener.Name(), err)
		}
	}
	return nil
}

func runReloadRollback(ctx context.Context, listeners []ReloadListener, event ReloadEvent, cause error) {
	for i := len(listeners) - 1; i >= 0; i-- {
		if listeners[i] != nil {
			listeners[i].RollbackReload(ctx, event, cause)
		}
	}
}

func (s *Store) build(ctx context.Context, version uint64) (*Snapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.dir == "" {
		return nil, errors.New("configdata: data dir is empty")
	}
	if stat, err := os.Stat(s.dir); err != nil {
		return nil, fmt.Errorf("configdata: stat dir %s: %w", s.dir, err)
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("configdata: data dir %s is not a directory", s.dir)
	}
	tables, objects, custom := s.registry.defs()
	snap := newSnapshot(version)
	buildCtx := &BuildContext{Dir: s.dir, Snapshot: snap}
	for _, def := range tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := def.load(buildCtx)
		if err != nil {
			return nil, err
		}
		snap.tables[def.name()] = raw
	}
	for _, def := range objects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := def.load(buildCtx)
		if err != nil {
			return nil, err
		}
		snap.objects[def.name()] = raw
	}
	for _, def := range custom {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := def.build(buildCtx)
		if err != nil {
			return nil, err
		}
		snap.custom[def.name()] = raw
	}
	for _, def := range tables {
		if err := def.validate(buildCtx, snap.tables[def.name()]); err != nil {
			return nil, err
		}
	}
	for _, def := range objects {
		if err := def.validate(buildCtx, snap.objects[def.name()]); err != nil {
			return nil, err
		}
	}
	for _, def := range custom {
		if err := def.validate(buildCtx, snap.custom[def.name()]); err != nil {
			return nil, err
		}
	}
	snap.finalize()
	return snap, nil
}

var (
	defaultStore    atomic.Pointer[Store]
	defaultRegistry = NewRegistry()
)

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func SetDefaultStore(store *Store) {
	defaultStore.Store(store)
}

func DefaultStore() *Store {
	return defaultStore.Load()
}

func Current() *Snapshot {
	if store := DefaultStore(); store != nil {
		return store.Current()
	}
	return nil
}

func ActiveSnapshot() *Snapshot {
	if c := fctx.CurrentContext(); c != nil {
		if snap, ok := c.Config.(*Snapshot); ok && snap != nil {
			return snap
		}
	}
	return Current()
}

func readJSON(path string, out any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "[") {
		return json.Unmarshal(raw, out)
	}
	if strings.HasPrefix(trimmed, "{") {
		if err := json.Unmarshal(raw, out); err == nil {
			return nil
		}
		var wrapped struct {
			Rows    json.RawMessage `json:"rows"`
			Records json.RawMessage `json:"records"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &wrapped); err != nil {
			return err
		}
		for _, candidate := range []json.RawMessage{wrapped.Rows, wrapped.Records, wrapped.Data} {
			if len(candidate) > 0 {
				return json.Unmarshal(candidate, out)
			}
		}
	}
	return json.Unmarshal(raw, out)
}
