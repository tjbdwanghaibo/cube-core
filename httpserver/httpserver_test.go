package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type echoRequest struct {
	Name string `json:"name"`
}

type echoResponse struct {
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func TestEngineRoutesGroupsAndJSONHandlersWithRequestID(t *testing.T) {
	engine := NewEngine(WithMaxBodyBytes(64))
	api := engine.Group("/api")
	api.Post("/echo", HandleJSON(func(ctx context.Context, req echoRequest) (echoResponse, error) {
		return echoResponse{
			Message:   "hello " + req.Name,
			RequestID: RequestID(ctx),
		}, nil
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/echo", strings.NewReader(`{"name":"cube"}`))
	req.Header.Set(HeaderRequestID, "rid-1")
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(HeaderRequestID); got != "rid-1" {
		t.Fatalf("request id header = %q, want rid-1", got)
	}
	var out echoResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Message != "hello cube" || out.RequestID != "rid-1" {
		t.Fatalf("response = %+v", out)
	}
}

func TestEngineRejectsTooLargeJSONBody(t *testing.T) {
	engine := NewEngine(WithMaxBodyBytes(8))
	engine.Post("/echo", HandleJSON(func(context.Context, echoRequest) (echoResponse, error) {
		t.Fatal("handler should not be called for oversized body")
		return echoResponse{}, nil
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader(`{"name":"too-long"}`))
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestReadBodyUsesEngineBodyLimit(t *testing.T) {
	engine := NewEngine(WithMaxBodyBytes(4))
	engine.Post("/raw", func(w http.ResponseWriter, r *http.Request) {
		raw, ok := ReadBody(w, r)
		if !ok {
			return
		}
		JSON(w, http.StatusOK, map[string]string{"body": string(raw)})
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/raw", strings.NewReader("too-large"))
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestEngineRecoversPanicAsJSON(t *testing.T) {
	engine := NewEngine()
	engine.Get("/panic", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("content-type = %q, want json", ct)
	}
	if !strings.Contains(rec.Body.String(), "internal server error") {
		t.Fatalf("body = %s, want internal server error", rec.Body.String())
	}
}

func TestNewServerAppliesProductionTimeouts(t *testing.T) {
	server := NewServer("127.0.0.1:0", NewEngine())
	if server.ReadHeaderTimeout <= 0 || server.ReadTimeout <= 0 || server.WriteTimeout <= 0 || server.IdleTimeout <= 0 {
		t.Fatalf("server timeouts are incomplete: %+v", server)
	}
}
