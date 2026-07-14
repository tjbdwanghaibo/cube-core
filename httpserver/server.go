package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

const (
	HeaderRequestID        = "X-Request-ID"
	DefaultMaxJSONBody     = int64(1 << 20)
	defaultRequestIDPrefix = "req"
)

type contextKey string

const (
	requestIDContextKey contextKey = "httpserver.request_id"
	maxBodyContextKey   contextKey = "httpserver.max_body"
)

type Config struct {
	MaxBodyBytes      int64
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

type Option func(*Config)

func WithMaxBodyBytes(n int64) Option {
	return func(cfg *Config) {
		if n > 0 {
			cfg.MaxBodyBytes = n
		}
	}
}

func WithTimeouts(readHeader, read, write, idle time.Duration) Option {
	return func(cfg *Config) {
		if readHeader > 0 {
			cfg.ReadHeaderTimeout = readHeader
		}
		if read > 0 {
			cfg.ReadTimeout = read
		}
		if write > 0 {
			cfg.WriteTimeout = write
		}
		if idle > 0 {
			cfg.IdleTimeout = idle
		}
	}
}

func defaultConfig() Config {
	return Config{
		MaxBodyBytes:      DefaultMaxJSONBody,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

type Engine struct {
	router chi.Router
	cfg    Config
}

// RouteRegistrar is the minimal HTTP route contract used by higher-level
// registration packages. Engine and Group both implement it.
type RouteRegistrar interface {
	Get(path string, handler http.HandlerFunc)
	Post(path string, handler http.HandlerFunc)
}

func NewEngine(opts ...Option) *Engine {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	r := chi.NewRouter()
	e := &Engine{router: r, cfg: cfg}
	e.Use(e.requestContextMiddleware, e.recoverMiddleware)
	return e
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if e == nil || e.router == nil {
		http.NotFound(w, r)
		return
	}
	e.router.ServeHTTP(w, r)
}

func (e *Engine) Handler() http.Handler {
	if e == nil {
		return http.NewServeMux()
	}
	return e
}

func (e *Engine) Use(middlewares ...func(http.Handler) http.Handler) {
	if e == nil || e.router == nil {
		return
	}
	e.router.Use(middlewares...)
}

func (e *Engine) Group(prefix string) *Group {
	if e == nil || e.router == nil {
		return &Group{}
	}
	return &Group{router: chi.NewRouter(), mount: func(path string, handler http.Handler) {
		e.router.Mount(path, handler)
	}, prefix: normalizePath(prefix)}
}

func (e *Engine) Get(path string, handler http.HandlerFunc) {
	if e == nil || e.router == nil {
		return
	}
	e.router.Get(path, handler)
}

func (e *Engine) Post(path string, handler http.HandlerFunc) {
	if e == nil || e.router == nil {
		return
	}
	e.router.Post(path, handler)
}

type Group struct {
	router chi.Router
	mount  func(string, http.Handler)
	prefix string
}

func (g *Group) Use(middlewares ...func(http.Handler) http.Handler) {
	if g == nil || g.router == nil {
		return
	}
	g.router.Use(middlewares...)
}

func (g *Group) Get(path string, handler http.HandlerFunc) {
	if g == nil || g.router == nil {
		return
	}
	g.ensureMounted()
	g.router.Get(path, handler)
}

func (g *Group) Post(path string, handler http.HandlerFunc) {
	if g == nil || g.router == nil {
		return
	}
	g.ensureMounted()
	g.router.Post(path, handler)
}

func (g *Group) ensureMounted() {
	if g == nil || g.mount == nil {
		return
	}
	g.mount(g.prefix, g.router)
	g.mount = nil
}

func NewServer(addr string, handler http.Handler, opts ...Option) *http.Server {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if handler == nil {
		handler = NewEngine(opts...)
	}
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}
}

func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(requestIDContextKey).(string)
	return v
}

func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func HandleJSON[TReq any, TResp any](fn func(context.Context, TReq) (TResp, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := BindJSON[TReq](w, r)
		if !ok {
			return
		}
		resp, err := fn(r.Context(), req)
		if err != nil {
			JSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		JSON(w, http.StatusOK, resp)
	}
}

func BindJSON[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var out T
	raw, ok := ReadBody(w, r)
	if !ok || len(raw) == 0 {
		return out, ok
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&out); err != nil {
		JSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "decode request"})
		return out, false
	}
	return out, true
}

func ReadBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r == nil || r.Body == nil {
		return nil, true
	}
	defer r.Body.Close()
	reader := http.MaxBytesReader(w, r.Body, maxBodyBytes(r.Context()))
	raw, err := io.ReadAll(reader)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			JSON(w, http.StatusRequestEntityTooLarge, map[string]any{"ok": false, "error": "request body too large"})
			return nil, false
		}
		JSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "read request"})
		return nil, false
	}
	return raw, true
}

func (e *Engine) requestContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get(HeaderRequestID))
		if requestID == "" {
			requestID = fmt.Sprintf("%s-%d", defaultRequestIDPrefix, time.Now().UnixNano())
		}
		w.Header().Set(HeaderRequestID, requestID)
		ctx := context.WithValue(r.Context(), requestIDContextKey, requestID)
		ctx = context.WithValue(ctx, maxBodyContextKey, e.cfg.MaxBodyBytes)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (e *Engine) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("http server panic",
					"request_id", RequestID(r.Context()),
					"path", r.URL.Path,
					"err", recovered,
					"stack", string(debug.Stack()),
				)
				JSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func maxBodyBytes(ctx context.Context) int64 {
	if ctx != nil {
		if v, ok := ctx.Value(maxBodyContextKey).(int64); ok && v > 0 {
			return v
		}
	}
	return DefaultMaxJSONBody
}

func normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}
