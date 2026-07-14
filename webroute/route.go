// Package webroute contains the runtime support used by generated HTTP routes.
package webroute

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/tjbdwanghaibo/cube-core/httpserver"
)

// Registerer is implemented by a route registry that validates and installs
// generated HTTP handlers.
type Registerer interface {
	Register(method, path string, handler http.HandlerFunc) error
}

// Registrar installs routes through httpserver while rejecting duplicated
// method/path pairs before chi receives them.
type Registrar struct {
	routes httpserver.RouteRegistrar
	mu     sync.Mutex
	seen   map[string]struct{}
}

func NewRegistrar(routes httpserver.RouteRegistrar) *Registrar {
	return &Registrar{routes: routes, seen: make(map[string]struct{})}
}

func (r *Registrar) Register(method, path string, handler http.HandlerFunc) error {
	if r == nil || r.routes == nil {
		return errors.New("webroute: route registrar is required")
	}
	if handler == nil {
		return errors.New("webroute: route handler is required")
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	path = strings.TrimSpace(path)
	if path == "" || !strings.HasPrefix(path, "/") {
		return fmt.Errorf("webroute: invalid route path %q", path)
	}
	if method != http.MethodGet && method != http.MethodPost {
		return fmt.Errorf("webroute: unsupported method %q", method)
	}

	key := method + " " + path
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.seen[key]; exists {
		return fmt.Errorf("webroute: duplicate route %s", key)
	}
	r.seen[key] = struct{}{}

	switch method {
	case http.MethodGet:
		r.routes.Get(path, handler)
	case http.MethodPost:
		r.routes.Post(path, handler)
	}
	return nil
}

// RawRequest is a copy of the HTTP data made available to raw route handlers.
type RawRequest struct {
	Path       string
	Method     string
	RemoteAddr string
	Header     http.Header
	Body       []byte
}

func ReadRaw(w http.ResponseWriter, r *http.Request) (RawRequest, bool) {
	if r == nil {
		return RawRequest{}, false
	}
	body, ok := httpserver.ReadBody(w, r)
	if !ok {
		return RawRequest{}, false
	}
	return RawRequest{
		Path:       r.URL.Path,
		Method:     r.Method,
		RemoteAddr: r.RemoteAddr,
		Header:     r.Header.Clone(),
		Body:       append([]byte(nil), body...),
	}, true
}

// DecodeJSON reads one JSON document into target. Decode errors are returned
// to the caller as the standard HTTP 400 response.
func DecodeJSON[T any](w http.ResponseWriter, r *http.Request, target *T) bool {
	if target == nil {
		writeDecodeError(w)
		return false
	}
	body, ok := httpserver.ReadBody(w, r)
	if !ok {
		return false
	}
	if len(body) == 0 {
		return true
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	if err := dec.Decode(target); err != nil {
		writeDecodeError(w)
		return false
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeDecodeError(w)
		return false
	}
	return true
}

type ErrorMapper func(error) (status int, response any)

func DefaultErrorMapper(err error) (int, any) {
	return http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()}
}

func WriteResult(w http.ResponseWriter, successStatus int, response any, err error) {
	WriteResultWithMapper(w, successStatus, response, err, DefaultErrorMapper)
}

func WriteResultWithMapper(w http.ResponseWriter, successStatus int, response any, err error, mapper ErrorMapper) {
	if err == nil {
		httpserver.JSON(w, successStatus, response)
		return
	}
	if mapper == nil {
		mapper = DefaultErrorMapper
	}
	status, errorResponse := mapper(err)
	httpserver.JSON(w, status, errorResponse)
}

func writeDecodeError(w http.ResponseWriter) {
	httpserver.JSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "decode request"})
}
