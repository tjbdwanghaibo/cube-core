package log

import (
	"context"
	"fmt"
	"log/slog"
)

type IDProvider interface {
	ID() int64
}

type ELog struct {
	logger  *slog.Logger
	enabled bool
	level   slog.Level
	attrs   []any
}

func NewELog(opts ...ELogOption) *ELog {
	l := &ELog{
		enabled: true,
		level:   slog.LevelDebug,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(l)
		}
	}
	return l
}

type ELogOption func(*ELog)

func WithLogger(logger *slog.Logger) ELogOption {
	return func(l *ELog) {
		l.logger = logger
	}
}

func WithELogLevel(level slog.Level) ELogOption {
	return func(l *ELog) {
		l.level = level
	}
}

func WithELogEnabled(enabled bool) ELogOption {
	return func(l *ELog) {
		l.enabled = enabled
	}
}

func (l *ELog) If(enabled bool) *ELog {
	if l == nil {
		return l
	}
	next := l.clone()
	next.enabled = enabled
	return next
}

func (l *ELog) Log(enabled bool) *ELog {
	return l.If(enabled)
}

func (l *ELog) WithLevel(level slog.Level) *ELog {
	if l == nil {
		return l
	}
	next := l.clone()
	next.level = level
	return next
}

func (l *ELog) With(args ...any) *ELog {
	if l == nil || len(args) == 0 {
		return l
	}
	next := l.cloneWithCapacity(len(args))
	next.attrs = append(next.attrs, args...)
	return next
}

func (l *ELog) WithAttrs(attrs ...slog.Attr) *ELog {
	if l == nil || len(attrs) == 0 {
		return l
	}
	next := l.cloneWithCapacity(len(attrs))
	for _, attr := range attrs {
		next.attrs = append(next.attrs, attr)
	}
	return next
}

func (l *ELog) Title(title string) *ELog {
	if title == "" {
		return l
	}
	return l.With("title", title)
}

func (l *ELog) T(title string) *ELog {
	return l.Title(title)
}

func (l *ELog) ID(id int64) *ELog {
	if id == 0 {
		return l
	}
	return l.With("id", id)
}

func (l *ELog) EntityID(id int64) *ELog {
	if id == 0 {
		return l
	}
	return l.With("entityId", id)
}

func (l *ELog) E(v any) *ELog {
	switch typed := v.(type) {
	case nil:
		return l
	case int64:
		return l.EntityID(typed)
	case int:
		return l.EntityID(int64(typed))
	case uint64:
		return l.EntityID(int64(typed))
	case uint:
		return l.EntityID(int64(typed))
	case IDProvider:
		return l.EntityID(typed.ID())
	default:
		return l.With("entity", typed)
	}
}

func (l *ELog) OwnerID(id int64) *ELog {
	if id == 0 {
		return l
	}
	return l.With("ownerId", id)
}

func (l *ELog) PlayerID(id int64) *ELog {
	if id == 0 {
		return l
	}
	return l.With("playerId", id)
}

func (l *ELog) Kind(kind any) *ELog {
	if kind == nil {
		return l
	}
	return l.With("kind", kind)
}

func (l *ELog) Type(typ any) *ELog {
	if typ == nil {
		return l
	}
	return l.With("type", typ)
}

func (l *ELog) Enabled(level slog.Level) bool {
	if l == nil {
		return Default().Enabled(context.Background(), level)
	}
	if !l.enabled || level < l.level {
		return false
	}
	return l.base().Enabled(context.Background(), level)
}

func (l *ELog) Debug(msg string, args ...any) {
	l.writeWithSkip(slog.LevelDebug, msg, 4, args...)
}

func (l *ELog) Info(msg string, args ...any) {
	l.writeWithSkip(slog.LevelInfo, msg, 4, args...)
}

func (l *ELog) Warn(msg string, args ...any) {
	l.writeWithSkip(slog.LevelWarn, msg, 4, args...)
}

func (l *ELog) Error(msg string, args ...any) {
	l.writeWithSkip(slog.LevelError, msg, 4, args...)
}

func (l *ELog) Debugf(format string, args ...any) {
	l.writef(slog.LevelDebug, format, 5, args...)
}

func (l *ELog) Infof(format string, args ...any) {
	l.writef(slog.LevelInfo, format, 5, args...)
}

func (l *ELog) Warnf(format string, args ...any) {
	l.writef(slog.LevelWarn, format, 5, args...)
}

func (l *ELog) Errorf(format string, args ...any) {
	l.writef(slog.LevelError, format, 5, args...)
}

func (l *ELog) writef(level slog.Level, format string, skip int, args ...any) {
	if !l.Enabled(level) {
		return
	}
	l.writeWithSkip(level, fmt.Sprintf(format, args...), skip)
}

func (l *ELog) write(level slog.Level, msg string, args ...any) {
	l.writeWithSkip(level, msg, 4, args...)
}

func (l *ELog) writeWithSkip(level slog.Level, msg string, skip int, args ...any) {
	if !l.Enabled(level) {
		return
	}
	var attrs []any
	if l != nil {
		attrs = l.attrs
	}
	all := make([]any, 0, len(attrs)+len(args))
	all = append(all, attrs...)
	all = append(all, args...)
	logWithCallerSkip(context.Background(), l.base(), level, msg, skip, all...)
}

func (l *ELog) base() *slog.Logger {
	if l != nil && l.logger != nil {
		return l.logger
	}
	return Default()
}

func (l *ELog) clone() *ELog {
	return l.cloneWithCapacity(0)
}

func (l *ELog) cloneWithCapacity(extra int) *ELog {
	if l == nil {
		return nil
	}
	next := *l
	if len(l.attrs) == 0 {
		next.attrs = nil
		return &next
	}
	next.attrs = make([]any, 0, len(l.attrs)+extra)
	next.attrs = append(next.attrs, l.attrs...)
	return &next
}
