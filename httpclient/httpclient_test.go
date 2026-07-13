package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/security"
)

type clientRequest struct {
	Name string `json:"name"`
}

type clientResponse struct {
	Message string `json:"message"`
}

func TestClientPostJSONSendsSignatureHeadersAndDecodesResponse(t *testing.T) {
	const secret = "client-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		raw := mustReadAll(t, r)
		if !security.VerifyPayloadSignature(raw, r.Header.Get("X-Cube-Signature"), secret) {
			t.Fatalf("signature header is invalid: %q body=%s", r.Header.Get("X-Cube-Signature"), raw)
		}
		if got := r.Header.Get("X-Request-ID"); got != "rid-2" {
			t.Fatalf("request id = %q, want rid-2", got)
		}
		var req clientRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(clientResponse{Message: "hello " + req.Name})
	}))
	defer server.Close()

	client := New(
		WithBaseURL(server.URL),
		WithTimeout(time.Second),
		WithSigner("X-Cube-Signature", secret),
	)
	var out clientResponse
	err := client.PostJSON(WithRequestID(context.Background(), "rid-2"), "/hello", clientRequest{Name: "cube"}, &out)
	if err != nil {
		t.Fatalf("PostJSON: %v", err)
	}
	if out.Message != "hello cube" {
		t.Fatalf("response = %+v", out)
	}
}

func TestClientPostJSONDecodesErrorBodyAndReturnsStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(clientResponse{Message: "upstream failed"})
	}))
	defer server.Close()

	client := New(WithBaseURL(server.URL), WithTimeout(time.Second))
	var out clientResponse
	err := client.PostJSON(context.Background(), "/fail", clientRequest{Name: "cube"}, &out)
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("err = %v, want StatusError", err)
	}
	if statusErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("status error = %+v", statusErr)
	}
	if out.Message != "upstream failed" {
		t.Fatalf("decoded error response = %+v", out)
	}
}

func TestClientClonePreservesBaseURLAndAddsHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Admin-Token"); got != "ops-token" {
			t.Fatalf("token header = %q, want ops-token", got)
		}
		_ = json.NewEncoder(w).Encode(clientResponse{Message: "ok"})
	}))
	defer server.Close()

	base := New(WithBaseURL(server.URL), WithTimeout(time.Second))
	clone := base.Clone(WithHeader("X-Admin-Token", "ops-token"))
	var out clientResponse
	if err := clone.PostJSON(context.Background(), "/execute", clientRequest{Name: "cube"}, &out); err != nil {
		t.Fatalf("PostJSON: %v", err)
	}
	if out.Message != "ok" {
		t.Fatalf("response = %+v", out)
	}
}

func mustReadAll(t *testing.T, r *http.Request) []byte {
	t.Helper()
	defer r.Body.Close()
	raw := make([]byte, 0, r.ContentLength)
	buf := make([]byte, 64)
	for {
		n, err := r.Body.Read(buf)
		if n > 0 {
			raw = append(raw, buf[:n]...)
		}
		if errors.Is(err, context.Canceled) {
			t.Fatalf("read body canceled: %v", err)
		}
		if err != nil {
			break
		}
	}
	return raw
}
