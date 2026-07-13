package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrCommandNotFound = errors.New("admin: command not found")
	ErrCommandInvalid  = errors.New("admin: command invalid")
)

const BusMessageName = "AdminCommand"

type Command struct {
	Name      string          `json:"name"`
	TraceID   string          `json:"trace_id,omitempty"`
	Operator  string          `json:"operator,omitempty"`
	Source    string          `json:"source,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt int64           `json:"created_at,omitempty"`
}

func (c *Command) MsgName() string { return BusMessageName }

type Result struct {
	Name      string         `json:"name"`
	TraceID   string         `json:"trace_id,omitempty"`
	OK        bool           `json:"ok"`
	Message   string         `json:"message,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	StartedAt int64          `json:"started_at,omitempty"`
	EndedAt   int64          `json:"ended_at,omitempty"`
}

type Handler func(context.Context, Command) (Result, error)

type CommandDef struct {
	Name        string
	Description string
	Handler     Handler
}

type RiskLevel string

const (
	RiskLow    RiskLevel = "low"
	RiskMedium RiskLevel = "medium"
	RiskHigh   RiskLevel = "high"
)

type CommandMeta struct {
	Name             string         `json:"name"`
	Title            string         `json:"title,omitempty"`
	Description      string         `json:"description,omitempty"`
	TargetScope      []string       `json:"target_scope,omitempty"`
	Risk             RiskLevel      `json:"risk,omitempty"`
	PayloadSchema    map[string]any `json:"payload_schema,omitempty"`
	ApprovalRequired bool           `json:"approval_required,omitempty"`
	DryRunSupported  bool           `json:"dry_run_supported,omitempty"`
	Hidden           bool           `json:"hidden,omitempty"`
}

type Registry struct {
	mu       sync.RWMutex
	commands map[string]CommandDef
}

type MetadataRegistry struct {
	mu       sync.RWMutex
	commands map[string]CommandMeta
}

var defaultRegistry = NewRegistry()
var defaultMetadataRegistry = NewMetadataRegistry()

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func Register(def CommandDef) error {
	return DefaultRegistry().Register(def)
}

func RegisterMetadata(meta CommandMeta) error {
	return defaultMetadataRegistry.Register(meta)
}

func Metadata(name string) (CommandMeta, bool) {
	return defaultMetadataRegistry.Get(name)
}

func MetadataList() []CommandMeta {
	return defaultMetadataRegistry.List()
}

func Execute(ctx context.Context, cmd Command) (Result, error) {
	return DefaultRegistry().Execute(ctx, cmd)
}

func NewRegistry() *Registry {
	return &Registry{commands: make(map[string]CommandDef)}
}

func NewMetadataRegistry() *MetadataRegistry {
	return &MetadataRegistry{commands: make(map[string]CommandMeta)}
}

func (r *Registry) Register(def CommandDef) error {
	if r == nil {
		return fmt.Errorf("%w: registry nil", ErrCommandInvalid)
	}
	if def.Name == "" {
		return fmt.Errorf("%w: name required", ErrCommandInvalid)
	}
	if def.Handler == nil {
		return fmt.Errorf("%w: handler required for %s", ErrCommandInvalid, def.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands[def.Name] = def
	return nil
}

func (r *Registry) Execute(ctx context.Context, cmd Command) (result Result, err error) {
	if r == nil {
		return Result{}, fmt.Errorf("%w: registry nil", ErrCommandInvalid)
	}
	if cmd.Name == "" {
		return Result{}, fmt.Errorf("%w: name required", ErrCommandInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	def, ok := r.commands[cmd.Name]
	r.mu.RUnlock()
	if !ok || def.Handler == nil {
		return Result{}, fmt.Errorf("%w: %s", ErrCommandNotFound, cmd.Name)
	}
	started := time.Now().UnixMilli()
	if cmd.CreatedAt == 0 {
		cmd.CreatedAt = started
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("admin: command %s panic: %v", cmd.Name, recovered)
			result = normalizeResult(cmd, result, started)
			result.OK = false
			result.Message = err.Error()
		}
	}()
	result, err = def.Handler(ctx, cmd)
	result = normalizeResult(cmd, result, started)
	if err != nil {
		result.OK = false
		if result.Message == "" {
			result.Message = err.Error()
		}
		return result, err
	}
	result.OK = true
	return result, nil
}

func normalizeResult(cmd Command, result Result, started int64) Result {
	if result.Name == "" {
		result.Name = cmd.Name
	}
	if result.TraceID == "" {
		result.TraceID = cmd.TraceID
	}
	if result.StartedAt == 0 {
		result.StartedAt = started
	}
	result.EndedAt = time.Now().UnixMilli()
	return result
}

func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.commands))
	for name := range r.commands {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *MetadataRegistry) Register(meta CommandMeta) error {
	if r == nil {
		return fmt.Errorf("%w: metadata registry nil", ErrCommandInvalid)
	}
	if meta.Name == "" {
		return fmt.Errorf("%w: metadata name required", ErrCommandInvalid)
	}
	if meta.Risk == "" {
		meta.Risk = RiskLow
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands[meta.Name] = cloneMeta(meta)
	return nil
}

func (r *MetadataRegistry) Get(name string) (CommandMeta, bool) {
	if r == nil {
		return CommandMeta{}, false
	}
	r.mu.RLock()
	meta, ok := r.commands[name]
	r.mu.RUnlock()
	if !ok {
		return CommandMeta{}, false
	}
	return cloneMeta(meta), true
}

func (r *MetadataRegistry) List() []CommandMeta {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	list := make([]CommandMeta, 0, len(r.commands))
	for _, meta := range r.commands {
		list = append(list, cloneMeta(meta))
	}
	r.mu.RUnlock()
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	return list
}

func DecodePayload[T any](cmd Command) (T, error) {
	var out T
	if len(cmd.Payload) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(cmd.Payload, &out); err != nil {
		return out, err
	}
	return out, nil
}

func cloneMeta(meta CommandMeta) CommandMeta {
	if len(meta.TargetScope) > 0 {
		meta.TargetScope = append([]string(nil), meta.TargetScope...)
	}
	if len(meta.PayloadSchema) > 0 {
		meta.PayloadSchema = cloneMap(meta.PayloadSchema)
	}
	return meta
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch typed := value.(type) {
		case []string:
			out[key] = append([]string(nil), typed...)
		case []any:
			out[key] = append([]any(nil), typed...)
		case map[string]any:
			out[key] = cloneMap(typed)
		default:
			out[key] = value
		}
	}
	return out
}

func MustPayload(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}
