package shared

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zeromicro/go-zero/rest"
)

type testSwaggerDocSource struct {
	doc []byte
	err error
}

func (s testSwaggerDocSource) ReadSwaggerDoc() ([]byte, error) {
	return s.doc, s.err
}

func TestAddSwaggerRoutesServesDocument(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	AddSwaggerRoutes(server, testSwaggerDocSource{
		doc: []byte(`{"swagger":"2.0","info":{"title":"XSonar"}}`),
	})

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
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("expected application/json response, got %q", got)
	}

	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["swagger"] != "2.0" {
		t.Fatalf("unexpected swagger version: %#v", response["swagger"])
	}
}

func TestAddSwaggerRoutesServesIndex(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	AddSwaggerRoutes(server, testSwaggerDocSource{
		doc: []byte(`{"swagger":"2.0"}`),
	})

	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build serverless: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil)
	rec := httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); !strings.Contains(got, "/swagger/doc.json") {
		t.Fatalf("expected index to reference /swagger/doc.json, got %q", got)
	}
}

func TestAddSwaggerRoutesServesAsset(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	AddSwaggerRoutes(server, testSwaggerDocSource{
		doc: []byte(`{"swagger":"2.0"}`),
	})

	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build serverless: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/swagger/swagger-ui.css", nil)
	rec := httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got == "" {
		t.Fatal("expected asset content-type to be set")
	}
	if rec.Body.Len() == 0 {
		t.Fatal("expected asset body")
	}
}

func TestAddSwaggerRoutesReturnsErrorOnDocReadFailure(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	AddSwaggerRoutes(server, testSwaggerDocSource{
		err: errors.New("boom"),
	})

	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build serverless: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
	rec := httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", rec.Code, rec.Body.String())
	}
}
