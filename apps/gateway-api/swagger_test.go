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

	securityDefinitions, ok := response["securityDefinitions"].(map[string]any)
	if !ok {
		t.Fatalf("expected securityDefinitions object, got %#v", response["securityDefinitions"])
	}
	if _, ok := securityDefinitions["gatewayBearer"]; !ok {
		t.Fatalf("expected gatewayBearer security definition, got %#v", securityDefinitions)
	}

	paths, ok := response["paths"].(map[string]any)
	if !ok {
		t.Fatalf("expected paths object in swagger doc, got %#v", response["paths"])
	}

	if _, ok := paths["/v1/auth/token"]; ok {
		t.Fatalf("expected /v1/auth/token to be absent, got %#v", paths["/v1/auth/token"])
	}

	userByIDOperation := assertOperation(t, paths, "/v1/users/by-id", http.MethodGet)
	assertSecurity(t, userByIDOperation, "gatewayBearer")
	parameters := assertParameters(t, userByIDOperation)
	assertParameterAbsent(t, parameters, "AppKey")
	assertParameterAbsent(t, parameters, "AppSecret")
	assertParameterAbsent(t, parameters, "Timestamp")
	assertParameterAbsent(t, parameters, "Nonce")
	assertParameterAbsent(t, parameters, "Signature")
	assertParameter(t, parameters, "query", "userId", true)
	assertParameter(t, parameters, "query", "cursor", false)

	collectorOperation := assertOperation(t, paths, "/v1/collector/tasks/periodic", http.MethodPost)
	assertSecurity(t, collectorOperation, "gatewayBearer")

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

func assertOperation(t *testing.T, paths map[string]any, path, method string) map[string]any {
	t.Helper()

	pathItem, ok := paths[path].(map[string]any)
	if !ok {
		t.Fatalf("expected %s path in swagger doc, got %#v", path, paths[path])
	}

	operation, ok := pathItem[strings.ToLower(method)].(map[string]any)
	if !ok {
		t.Fatalf("expected %s %s operation, got %#v", method, path, pathItem[strings.ToLower(method)])
	}

	return operation
}

func assertParameters(t *testing.T, operation map[string]any) []any {
	t.Helper()

	parameters, ok := operation["parameters"].([]any)
	if !ok {
		t.Fatalf("expected operation parameters, got %#v", operation["parameters"])
	}

	return parameters
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

func assertSecurity(t *testing.T, operation map[string]any, scheme string) {
	t.Helper()

	securityEntries, ok := operation["security"].([]any)
	if !ok || len(securityEntries) == 0 {
		t.Fatalf("expected security entries, got %#v", operation["security"])
	}
	first, ok := securityEntries[0].(map[string]any)
	if !ok {
		t.Fatalf("expected security entry object, got %#v", securityEntries[0])
	}
	if _, ok := first[scheme]; !ok {
		t.Fatalf("expected security scheme %q, got %#v", scheme, first)
	}
}

func assertSchemes(t *testing.T, rawSchemes any, expected string) {
	t.Helper()

	schemes, ok := rawSchemes.([]any)
	if !ok || len(schemes) != 1 || schemes[0] != expected {
		t.Fatalf("expected schemes [%q], got %#v", expected, rawSchemes)
	}
}
