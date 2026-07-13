package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unicode"
)

var preMessageAttrKeys = map[string]struct{}{
	"goId":           {},
	"frame":          {},
	"server_time_ms": {},
	"source":         {},
	"handler":        {},
	"caller":         {},
	"caller_func":    {},
}

type orderedTextHandler struct {
	out     io.Writer
	opts    slog.HandlerOptions
	mu      *sync.Mutex
	attrs   []slog.Attr
	group   string
	groups  []string
	replace func([]string, slog.Attr) slog.Attr
}

func newOrderedTextHandler(out io.Writer, opts *slog.HandlerOptions) slog.Handler {
	h := &orderedTextHandler{
		out: out,
		mu:  &sync.Mutex{},
	}
	if opts != nil {
		h.opts = *opts
		h.replace = opts.ReplaceAttr
	}
	return h
}

func (h *orderedTextHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *orderedTextHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := h.collectAttrs(record)
	preMsg, postMsg := splitPreMessageAttrs(attrs)

	var buf []byte
	buf = appendAttr(buf, slog.Time(slog.TimeKey, record.Time), nil, h.replace)
	buf = append(buf, ' ')
	buf = appendAttr(buf, slog.Any(slog.LevelKey, record.Level), nil, h.replace)
	for _, attr := range preMsg {
		buf = append(buf, ' ')
		buf = appendAttr(buf, attr, h.groups, h.replace)
	}
	if h.opts.AddSource {
		buf = append(buf, ' ')
		buf = appendAttr(buf, sourceAttr(record.PC), nil, h.replace)
	}
	if record.Message != "" {
		buf = append(buf, ' ')
		buf = appendAttr(buf, slog.String(slog.MessageKey, record.Message), nil, h.replace)
	}
	for _, attr := range postMsg {
		buf = append(buf, ' ')
		buf = appendAttr(buf, attr, h.groups, h.replace)
	}
	buf = append(buf, '\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.out.Write(buf)
	return err
}

func (h *orderedTextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := h.clone()
	for _, attr := range attrs {
		next.attrs = append(next.attrs, qualifyAttr(attr, h.group))
	}
	return next
}

func (h *orderedTextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := h.clone()
	next.groups = append(next.groups, name)
	if next.group == "" {
		next.group = name
	} else {
		next.group += "." + name
	}
	return next
}

func (h *orderedTextHandler) clone() *orderedTextHandler {
	next := *h
	if len(h.attrs) > 0 {
		next.attrs = append([]slog.Attr(nil), h.attrs...)
	}
	if len(h.groups) > 0 {
		next.groups = append([]string(nil), h.groups...)
	}
	return &next
}

func (h *orderedTextHandler) collectAttrs(record slog.Record) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(h.attrs)+record.NumAttrs())
	attrs = append(attrs, h.attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, qualifyAttr(attr, h.group))
		return true
	})
	return attrs
}

func splitPreMessageAttrs(attrs []slog.Attr) ([]slog.Attr, []slog.Attr) {
	idx := 0
	for idx < len(attrs) {
		if _, ok := preMessageAttrKeys[attrs[idx].Key]; !ok {
			break
		}
		idx++
	}
	return attrs[:idx], attrs[idx:]
}

func appendAttr(buf []byte, attr slog.Attr, groups []string, replace func([]string, slog.Attr) slog.Attr) []byte {
	if replace != nil {
		attr = replace(groups, attr)
	}
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return buf
	}
	if attr.Value.Kind() == slog.KindGroup {
		return appendGroup(buf, attr, groups, replace)
	}
	key := attr.Key
	if key == "" {
		key = "!BADKEY"
	}
	buf = append(buf, key...)
	buf = append(buf, '=')
	return appendValue(buf, attr.Value)
}

func appendGroup(buf []byte, attr slog.Attr, groups []string, replace func([]string, slog.Attr) slog.Attr) []byte {
	groupAttrs := attr.Value.Group()
	if len(groupAttrs) == 0 {
		return buf
	}
	prefix := attr.Key
	if prefix != "" {
		if len(groups) > 0 {
			groups = append(append([]string(nil), groups...), prefix)
		} else {
			groups = []string{prefix}
		}
	}
	for _, nested := range groupAttrs {
		if prefix != "" {
			nested = qualifyAttr(nested, prefix)
		}
		if len(buf) > 0 && buf[len(buf)-1] != ' ' {
			buf = append(buf, ' ')
		}
		buf = appendAttr(buf, nested, groups, replace)
	}
	return buf
}

func appendValue(buf []byte, value slog.Value) []byte {
	switch value.Kind() {
	case slog.KindString:
		return appendText(buf, value.String())
	case slog.KindInt64:
		return strconv.AppendInt(buf, value.Int64(), 10)
	case slog.KindUint64:
		return strconv.AppendUint(buf, value.Uint64(), 10)
	case slog.KindFloat64:
		return strconv.AppendFloat(buf, value.Float64(), 'g', -1, 64)
	case slog.KindBool:
		return strconv.AppendBool(buf, value.Bool())
	case slog.KindDuration:
		return appendText(buf, value.Duration().String())
	case slog.KindTime:
		return appendText(buf, value.Time().Format(time.RFC3339Nano))
	case slog.KindAny:
		return appendText(buf, fmt.Sprint(value.Any()))
	default:
		return appendText(buf, value.String())
	}
}

func appendText(buf []byte, text string) []byte {
	if needsQuote(text) {
		return strconv.AppendQuote(buf, text)
	}
	return append(buf, text...)
}

func needsQuote(text string) bool {
	if text == "" {
		return true
	}
	for _, r := range text {
		if r == '=' || r == '"' || unicode.IsSpace(r) || !strconv.IsPrint(r) {
			return true
		}
	}
	return false
}

func qualifyAttr(attr slog.Attr, prefix string) slog.Attr {
	if prefix == "" || attr.Key == "" {
		return attr
	}
	attr.Key = prefix + "." + attr.Key
	return attr
}

func sourceAttr(pc uintptr) slog.Attr {
	if pc == 0 {
		return slog.Attr{}
	}
	fs := runtime.CallersFrames([]uintptr{pc})
	frame, _ := fs.Next()
	if frame.File == "" {
		return slog.Attr{}
	}
	return slog.String(slog.SourceKey, frame.File+":"+strconv.Itoa(frame.Line))
}
