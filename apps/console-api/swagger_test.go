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
	assertSchemes(t, response["schemes"], "http")

	securityDefinitions, ok := response["securityDefinitions"].(map[string]any)
	if !ok {
		t.Fatalf("expected securityDefinitions in swagger doc, got %#v", response["securityDefinitions"])
	}
	if _, ok := securityDefinitions["adminBearer"]; !ok {
		t.Fatalf("expected adminBearer security definition, got %#v", securityDefinitions)
	}

	paths, ok := response["paths"].(map[string]any)
	if !ok {
		t.Fatalf("expected paths object in swagger doc, got %#v", response["paths"])
	}

	tenantPath, ok := paths["/admin/v1/tenants"].(map[string]any)
	if !ok {
		t.Fatalf("expected /admin/v1/tenants path in swagger doc, got %#v", paths["/admin/v1/tenants"])
	}
	listTenantsOperation, ok := tenantPath["get"].(map[string]any)
	if !ok {
		t.Fatalf("expected GET /admin/v1/tenants operation, got %#v", tenantPath["get"])
	}
	assertSchemes(t, listTenantsOperation["schemes"], "http")
	operationSecurity, ok := listTenantsOperation["security"].([]any)
	if !ok || len(operationSecurity) == 0 {
		t.Fatalf("expected GET /admin/v1/tenants security requirement, got %#v", listTenantsOperation["security"])
	}
	adminBearerSecurity, ok := operationSecurity[0].(map[string]any)
	if !ok {
		t.Fatalf("expected security requirement object, got %#v", operationSecurity[0])
	}
	if _, ok := adminBearerSecurity["adminBearer"]; !ok {
		t.Fatalf("expected adminBearer security requirement, got %#v", adminBearerSecurity)
	}

	loginPath, ok := paths["/admin/v1/auth/login"].(map[string]any)
	if !ok {
		t.Fatalf("expected /admin/v1/auth/login path in swagger doc, got %#v", paths["/admin/v1/auth/login"])
	}
	loginOperation, ok := loginPath["post"].(map[string]any)
	if !ok {
		t.Fatalf("expected POST /admin/v1/auth/login operation, got %#v", loginPath["post"])
	}
	if _, ok := loginOperation["security"]; ok {
		t.Fatalf("expected login operation to remain public, got %#v", loginOperation["security"])
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

func assertSchemes(t *testing.T, rawSchemes any, expected string) {
	t.Helper()

	schemes, ok := rawSchemes.([]any)
	if !ok {
		t.Fatalf("expected schemes array, got %#v", rawSchemes)
	}
	if len(schemes) != 1 || schemes[0] != expected {
		t.Fatalf("expected schemes [%q], got %#v", expected, schemes)
	}
}
