package shared

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zeromicro/go-zero/rest"
)

func TestAddHealthzRouteServesOK(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	AddHealthzRoute(server, "gateway-api")

	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build serverless: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected application/json, got %q", got)
	}

	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["status"] != "ok" {
		t.Fatalf("unexpected status: %#v", response)
	}
	if response["service"] != "gateway-api" {
		t.Fatalf("unexpected service: %#v", response)
	}
}
