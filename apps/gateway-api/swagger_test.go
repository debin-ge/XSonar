package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zeromicro/go-zero/rest"
)

func TestAddSwaggerRoutesServesEmbeddedGatewaySpec(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	addSwaggerRoutes(server, "dev")

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
	if got := info["title"]; got != "XSonar Gateway API" {
		t.Fatalf("expected swagger title %q, got %#v", "XSonar Gateway API", got)
	}
	assertSchemes(t, response["schemes"], "http")

	paths, ok := response["paths"].(map[string]any)
	if !ok {
		t.Fatalf("expected paths object in swagger doc, got %#v", response["paths"])
	}
	userByIDPath, ok := paths["/v1/users/by-id"].(map[string]any)
	if !ok {
		t.Fatalf("expected /v1/users/by-id path in swagger doc, got %#v", paths["/v1/users/by-id"])
	}
	userByIDOperation, ok := userByIDPath["get"].(map[string]any)
	if !ok {
		t.Fatalf("expected GET /v1/users/by-id operation, got %#v", userByIDPath["get"])
	}
	assertSchemes(t, userByIDOperation["schemes"], "http")
	parameters, ok := userByIDOperation["parameters"].([]any)
	if !ok {
		t.Fatalf("expected GET /v1/users/by-id parameters, got %#v", userByIDOperation["parameters"])
	}
	assertParameter(t, parameters, "header", "AppKey", true)
	assertParameter(t, parameters, "header", "AppSecret", true)
	assertParameterAbsent(t, parameters, "Timestamp")
	assertParameterAbsent(t, parameters, "Nonce")
	assertParameterAbsent(t, parameters, "Signature")
	assertParameter(t, parameters, "query", "userId", true)
	assertParameter(t, parameters, "query", "cursor", false)

	req = httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil)
	rec = httptest.NewRecorder()

	serverless.Serve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/swagger/doc.json") {
		t.Fatalf("expected index to reference doc.json, got %q", rec.Body.String())
	}
}

func TestAddSwaggerRoutesServesProductionGatewaySpec(t *testing.T) {
	server := rest.MustNewServer(rest.RestConf{
		Host: "127.0.0.1",
		Port: 0,
	})
	defer server.Stop()

	addSwaggerRoutes(server, "pro")

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

	paths, ok := response["paths"].(map[string]any)
	if !ok {
		t.Fatalf("expected paths object in swagger doc, got %#v", response["paths"])
	}
	userByIDPath, ok := paths["/v1/users/by-id"].(map[string]any)
	if !ok {
		t.Fatalf("expected /v1/users/by-id path in swagger doc, got %#v", paths["/v1/users/by-id"])
	}
	userByIDOperation, ok := userByIDPath["get"].(map[string]any)
	if !ok {
		t.Fatalf("expected GET /v1/users/by-id operation, got %#v", userByIDPath["get"])
	}
	parameters, ok := userByIDOperation["parameters"].([]any)
	if !ok {
		t.Fatalf("expected GET /v1/users/by-id parameters, got %#v", userByIDOperation["parameters"])
	}

	assertParameter(t, parameters, "header", "AppKey", true)
	assertParameter(t, parameters, "header", "Timestamp", true)
	assertParameter(t, parameters, "header", "Nonce", true)
	assertParameter(t, parameters, "header", "Signature", true)
	assertParameterAbsent(t, parameters, "AppSecret")
	assertParameter(t, parameters, "query", "userId", true)
	assertParameter(t, parameters, "query", "cursor", false)
}

func assertParameter(t *testing.T, parameters []any, location, name string, required bool) {
	t.Helper()

	for _, rawParameter := range parameters {
		parameter, ok := rawParameter.(map[string]any)
		if !ok {
			continue
		}
		if parameter["in"] != location || parameter["name"] != name {
			continue
		}
		if gotRequired, _ := parameter["required"].(bool); gotRequired != required {
			t.Fatalf("expected %s %s required=%t, got %#v", location, name, required, parameter)
		}
		return
	}

	t.Fatalf("expected %s parameter %q, got %#v", location, name, parameters)
}

func assertParameterAbsent(t *testing.T, parameters []any, name string) {
	t.Helper()

	for _, rawParameter := range parameters {
		parameter, ok := rawParameter.(map[string]any)
		if !ok {
			continue
		}
		if parameter["name"] == name {
			t.Fatalf("expected parameter %q to be absent, got %#v", name, parameter)
		}
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
