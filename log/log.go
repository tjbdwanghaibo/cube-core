package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/misc"
)

var (
	loggerMu      sync.RWMutex
	defaultLogger = slog.Default()
	outputFile    io.Closer
)

type Options struct {
	Level     slog.Level
	LevelText string
	Output    io.Writer
	JSON      bool

	Stdout   bool
	File     bool
	Dir      string
	Filename string
	Service  string
	Sid      int

	RotateInterval   time.Duration
	RotateTimeFormat string
	NowFunc          func() time.Time

	Caller    bool
	FrameFunc func() uint64

	DisableGoID    bool
	DisableFrame   bool
	DisableContext bool
}

type contextHandler struct {
	next slog.Handler
	opts Options
}

func Init(opts Options) error {
	level, err := resolveLevel(opts)
	if err != nil {
		return err
	}
	opts.Level = level

	writers := make([]io.Writer, 0, 2)
	var file io.WriteCloser
	if opts.Output != nil {
		writers = append(writers, opts.Output)
	} else if opts.Stdout || !opts.File {
		writers = append(writers, os.Stdout)
	}
	if opts.File {
		file, err = openLogWriter(opts)
		if err != nil {
			return err
		}
		writers = append(writers, file)
	}
	if len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}

	var output io.Writer = writers[0]
	if len(writers) > 1 {
		output = io.MultiWriter(writers...)
	}

	handlerOpts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if opts.JSON {
		handler = slog.NewJSONHandler(output, handlerOpts)
	} else {
		handler = newOrderedTextHandler(output, handlerOpts)
	}
	handler = contextHandler{next: handler, opts: opts}

	logger := slog.New(handler)
	loggerMu.Lock()
	oldFile := outputFile
	outputFile = file
	defaultLogger = logger
	loggerMu.Unlock()
	if oldFile != nil {
		_ = oldFile.Close()
	}
	slog.SetDefault(logger)
	return nil
}

func Close() error {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	if outputFile == nil {
		return nil
	}
	err := outputFile.Close()
	outputFile = nil
	return err
}

func Default() *slog.Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return defaultLogger
}

func Debug(msg string, args ...any) {
	logWithCallerSkip(context.Background(), Default(), slog.LevelDebug, msg, 3, args...)
}

func Info(msg string, args ...any) {
	logWithCallerSkip(context.Background(), Default(), slog.LevelInfo, msg, 3, args...)
}

func Warn(msg string, args ...any) {
	logWithCallerSkip(context.Background(), Default(), slog.LevelWarn, msg, 3, args...)
}

func Error(msg string, args ...any) {
	logWithCallerSkip(context.Background(), Default(), slog.LevelError, msg, 3, args...)
}

func ParseLevel(text string) (slog.Level, error) {
	var level slog.Level
	text = strings.TrimSpace(text)
	if text == "" {
		return level, nil
	}
	switch strings.ToLower(text) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		if err := level.UnmarshalText([]byte(strings.ToUpper(text))); err != nil {
			return level, fmt.Errorf("log level %q: %w", text, err)
		}
		return level, nil
	}
}

func resolveLevel(opts Options) (slog.Level, error) {
	if strings.TrimSpace(opts.LevelText) == "" {
		return opts.Level, nil
	}
	return ParseLevel(opts.LevelText)
}

func openLogWriter(opts Options) (io.WriteCloser, error) {
	dir := opts.Dir
	if dir == "" {
		dir = "log"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	name := opts.Filename
	if name == "" {
		base := opts.Service
		if base == "" {
			base = "cube"
		}
		if opts.Sid != 0 {
			base = fmt.Sprintf("%s-%d", base, opts.Sid)
		}
		name = base + ".log"
	}
	if opts.RotateInterval > 0 {
		return newTimeRotatingFileWriter(timeRotatingFileOptions{
			Dir:        dir,
			Filename:   name,
			Interval:   opts.RotateInterval,
			TimeFormat: opts.RotateTimeFormat,
			NowFunc:    opts.NowFunc,
		})
	}
	return os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}

func (h contextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h contextHandler) Handle(ctx context.Context, record slog.Record) error {
	attrs := h.contextAttrs(record.PC)
	if len(attrs) == 0 {
		return h.next.Handle(ctx, record)
	}

	next := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	next.AddAttrs(attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		next.AddAttrs(attr)
		return true
	})
	return h.next.Handle(ctx, next)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{next: h.next.WithAttrs(attrs), opts: h.opts}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{next: h.next.WithGroup(name), opts: h.opts}
}

func (h contextHandler) contextAttrs(pc uintptr) []slog.Attr {
	attrs := make([]slog.Attr, 0, 8)
	if !h.opts.DisableGoID {
		attrs = append(attrs, slog.Int64("goId", misc.GoID()))
	}
	frame := uint64(0)
	c := fctx.CurrentContext()
	if c != nil && c.Frame != 0 {
		frame = c.Frame
	}
	if frame == 0 && h.opts.FrameFunc != nil {
		frame = h.opts.FrameFunc()
	}
	if !h.opts.DisableFrame {
		attrs = append(attrs, slog.Uint64("frame", frame))
	}
	nowMilli := time.Now().UnixMilli()
	if c != nil && c.NowMilli != 0 {
		nowMilli = c.NowMilli
	}
	attrs = append(attrs, slog.Int64("server_time_ms", nowMilli))
	if h.opts.DisableContext || c == nil {
		if h.opts.Caller {
			attrs = append(attrs, callerAttrs(pc)...)
		}
		return attrs
	}
	meta := c.Meta
	if meta.Source != "" {
		attrs = append(attrs, slog.String("source", meta.Source))
	}
	if meta.Handler != "" {
		attrs = append(attrs, slog.String("handler", meta.Handler))
	}
	if h.opts.Caller {
		attrs = append(attrs, callerAttrs(pc)...)
	}
	if meta.PlayerID != 0 {
		attrs = append(attrs, slog.Int64("player", meta.PlayerID))
	}
	if meta.MsgID != 0 {
		attrs = append(attrs, slog.Uint64("msg", uint64(meta.MsgID)))
	}
	if meta.Seq != 0 {
		attrs = append(attrs, slog.Uint64("seq", uint64(meta.Seq)))
	}
	return attrs
}

func logWithCallerSkip(ctx context.Context, logger *slog.Logger, level slog.Level, msg string, skip int, args ...any) {
	if logger == nil {
		logger = Default()
	}
	if !logger.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(skip, pcs[:])
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.Add(args...)
	_ = logger.Handler().Handle(ctx, record)
}

func callerAttrs(pc uintptr) []slog.Attr {
	if pc == 0 {
		return nil
	}
	fs := runtime.CallersFrames([]uintptr{pc})
	frame, _ := fs.Next()
	if frame.File == "" {
		return nil
	}
	attrs := []slog.Attr{slog.String("caller", frame.File+":"+strconv.Itoa(frame.Line))}
	if frame.Function != "" {
		attrs = append(attrs, slog.String("caller_func", frame.Function))
	}
	return attrs
}
