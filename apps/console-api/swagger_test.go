package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zeromicro/go-zero/rest"
)

func TestAddSwaggerRoutesServesEmbeddedConsoleSpec(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	addSwaggerRoutes(server)

	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build serverless: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
	rec := httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode swagger doc: %v", err)
	}

	info, ok := response["info"].(map[string]any)
	if !ok {
		t.Fatalf("expected info object in swagger doc, got %#v", response["info"])
	}
	if got := info["title"]; got != "XSonar Console API" {
		t.Fatalf("expected swagger title %q, got %#v", "XSonar Console API", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil)
	rec = httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/swagger/doc.json") {
		t.Fatalf("expected index to reference doc.json, got %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `new URL("./doc.json", window.location.href).href`) {
		t.Fatalf("expected index to derive doc.json from the current page location, got %q", rec.Body.String())
	}
}
