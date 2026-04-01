package shared

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
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
	body := rec.Body.String()
	if !strings.Contains(body, "/swagger/doc.json") {
		t.Fatalf("expected index to include /swagger/doc.json fallback, got %q", body)
	}
	if !strings.Contains(body, `new URL("./doc.json", window.location.href).href`) {
		t.Fatalf("expected index to derive doc.json from the current page location, got %q", body)
	}
	if strings.Contains(body, "validator.swagger.io") {
		t.Fatalf("expected index to disable external validator, got %q", body)
	}
	if !strings.Contains(body, `validatorUrl: null`) {
		t.Fatalf("expected index to disable validatorUrl, got %q", body)
	}
}

func TestSwaggerRoutesServesIndexWithCustomPrefix(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	server.AddRoutes(SwaggerRoutes("/docs", testSwaggerDocSource{
		doc: []byte(`{"swagger":"2.0"}`),
	}))

	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build serverless: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/docs/index.html", nil)
	rec := httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "/docs/doc.json") {
		t.Fatalf("expected index not to hardcode /docs/doc.json, got %q", body)
	}
	if !strings.Contains(body, `new URL("./doc.json", window.location.href).href`) {
		t.Fatalf("expected index to derive doc.json from the current page location, got %q", body)
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

	assetPaths := []string{
		"/swagger/swagger-ui.css",
		"/swagger/swagger-ui-bundle.js",
		"/swagger/swagger-ui-standalone-preset.js",
	}

	for _, assetPath := range assetPaths {
		req := httptest.NewRequest(http.MethodGet, assetPath, nil)
		rec := httptest.NewRecorder()

		serverless.Serve(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for %s, got %d: %s", assetPath, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Content-Type"); got == "" {
			t.Fatalf("expected asset content-type for %s", assetPath)
		}
		if rec.Body.Len() == 0 {
			t.Fatalf("expected asset body for %s", assetPath)
		}
	}
}

func TestAddSwaggerRoutesServesOAuth2Redirect(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/swagger/oauth2-redirect.html", nil)
	rec := httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("expected html content-type, got %q", got)
	}
	if rec.Body.Len() == 0 {
		t.Fatal("expected oauth2 redirect body")
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

func TestSwaggerRoutesSupportsCustomPrefix(t *testing.T) {
	routes := SwaggerRoutes("/docs", testSwaggerDocSource{doc: []byte(`{"swagger":"2.0"}`)})

	expectedPaths := []string{
		"/docs/doc.json",
		"/docs/index.html",
		"/docs/swagger-ui.css",
		"/docs/swagger-ui-bundle.js",
		"/docs/swagger-ui-standalone-preset.js",
		"/docs/oauth2-redirect.html",
	}

	if len(routes) != len(expectedPaths) {
		t.Fatalf("expected %d routes, got %d", len(expectedPaths), len(routes))
	}
	for _, expectedPath := range expectedPaths {
		if !slices.ContainsFunc(routes, func(route rest.Route) bool {
			return route.Path == expectedPath
		}) {
			t.Fatalf("expected route path %q to be registered, got %#v", expectedPath, routes)
		}
	}
}
