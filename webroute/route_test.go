package webroute

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/httpserver"
)

func TestRegisterRejectsDuplicateMethodPath(t *testing.T) {
	engine := httpserver.NewEngine()
	registrar := NewRegistrar(engine)
	noop := func(http.ResponseWriter, *http.Request) {}

	if err := registrar.Register(http.MethodPost, "/events", noop); err != nil {
		t.Fatalf("register first route: %v", err)
	}
	if err := registrar.Register(http.MethodPost, "/events", noop); err == nil {
		t.Fatal("duplicate route registration succeeded")
	}
	if err := registrar.Register(http.MethodPut, "/events", noop); err == nil {
		t.Fatal("unsupported method registration succeeded")
	}
}

func TestDecodeJSONRejectsMalformedBody(t *testing.T) {
	engine := httpserver.NewEngine()
	registrar := NewRegistrar(engine)
	if err := registrar.Register(http.MethodPost, "/json", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Name string `json:"name"`
		}
		if !DecodeJSON(w, r, &request) {
			return
		}
		WriteResult(w, http.StatusOK, map[string]string{"name": request.Name}, nil)
	}); err != nil {
		t.Fatalf("register route: %v", err)
	}

	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/json", strings.NewReader(`{"name":`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestDecodeJSONAndWriteResult(t *testing.T) {
	engine := httpserver.NewEngine()
	registrar := NewRegistrar(engine)
	if err := registrar.Register(http.MethodPost, "/json", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Name string `json:"name"`
		}
		if !DecodeJSON(w, r, &request) {
			return
		}
		WriteResult(w, http.StatusOK, map[string]string{"message": "hello " + request.Name}, nil)
	}); err != nil {
		t.Fatalf("register route: %v", err)
	}

	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/json", strings.NewReader(`{"name":"cube"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["message"] != "hello cube" {
		t.Fatalf("response = %#v", response)
	}
}

func TestReadRawCopiesRequestData(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader("payload"))
	request.RemoteAddr = "127.0.0.1:8080"
	request.Header.Set("X-Test", "one")
	rec := httptest.NewRecorder()

	raw, ok := ReadRaw(rec, request)
	if !ok {
		t.Fatalf("read raw failed: %s", rec.Body.String())
	}
	request.Header.Set("X-Test", "changed")
	if got := string(raw.Body); got != "payload" {
		t.Fatalf("body = %q", got)
	}
	if got := raw.Header.Get("X-Test"); got != "one" {
		t.Fatalf("header = %q", got)
	}
}

type testModule struct {
	calls *int
	err   error
}

func (m testModule) RegisterRoutes(Registerer) error {
	*m.calls++
	return m.err
}

func TestRegisterModulesStopsOnFirstError(t *testing.T) {
	calls := 0
	wantErr := errors.New("register failed")
	err := RegisterModules(nil,
		testModule{calls: &calls},
		testModule{calls: &calls, err: wantErr},
		testModule{calls: &calls},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}
