package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	accesstestkit "xsonar/apps/access-rpc/testkit"
	consoletestkit "xsonar/apps/console-api/testkit"
	gatewaytestkit "xsonar/apps/gateway-api/testkit"
	policytestkit "xsonar/apps/policy-rpc/testkit"
	providertestkit "xsonar/apps/provider-rpc/testkit"
	schedulertestkit "xsonar/apps/scheduler-rpc/testkit"
	"xsonar/pkg/xlog"
)

func TestCollectorTaskChain(t *testing.T) {
	t.Setenv("ACCESS_RPC_SEED_ADMIN_USERNAME", "admin")
	t.Setenv("ACCESS_RPC_SEED_ADMIN_PASSWORD", "admin123456")

	logger := xlog.NewStdout("collector-task-chain-test")
	const gatewayJWTSecret = "xsonar-gateway-dev-secret"
	const gatewayJWTIssuer = "xsonar-gateway"

	accessClient, accessCleanup, err := accesstestkit.NewClient(logger)
	if err != nil {
		t.Fatalf("new access client: %v", err)
	}
	defer accessCleanup()

	policyClient, policyCleanup, err := policytestkit.NewClient(logger)
	if err != nil {
		t.Fatalf("new policy client: %v", err)
	}
	defer policyCleanup()

	providerClient, providerCleanup, err := providertestkit.NewClient(logger)
	if err != nil {
		t.Fatalf("new provider client: %v", err)
	}
	defer providerCleanup()

	schedulerClient, schedulerCleanup, err := schedulertestkit.NewClient(logger)
	if err != nil {
		t.Fatalf("new scheduler client: %v", err)
	}
	defer schedulerCleanup()

	consoleServer := httptest.NewServer(consoletestkit.NewHandlerWithClients(logger, accessClient, policyClient, providerClient))
	defer consoleServer.Close()

	gatewayServer := httptest.NewServer(gatewaytestkit.NewHandlerWithClientsAndMode(logger, accessClient, nil, nil, schedulerClient, gatewayJWTSecret, gatewayJWTIssuer, ""))
	defer gatewayServer.Close()

	consoleToken := loginConsoleAdmin(t, consoleServer.URL)
	tenantID, appID, _, _ := createTenantAndApp(t, consoleServer.URL, consoleToken)
	gatewayToken := issueGatewayTokenForCollector(t, consoleServer.URL, consoleToken, tenantID, appID, 0)

	createResp := postJSON(t, gatewayServer.URL+"/v1/collector/tasks/periodic", map[string]any{
		"task_id":           "task-chain-1",
		"keyword":           "openai",
		"priority":          5,
		"frequency_seconds": 60,
	}, gatewayToken)
	if createResp.Code != 0 {
		t.Fatalf("unexpected create response: %+v", createResp)
	}

	createData := mustObject(t, createResp.Data)
	if got := stringValue(createData["task_id"]); got != "task-chain-1" {
		t.Fatalf("expected task_id task-chain-1, got %q", got)
	}
	if got := stringValue(createData["created_by"]); got != appID {
		t.Fatalf("expected created_by %q, got %q", appID, got)
	}

	getResp := getJSON(t, gatewayServer.URL+"/v1/collector/tasks/task-chain-1", gatewayToken)
	if getResp.Code != 0 {
		t.Fatalf("unexpected get response: %+v", getResp)
	}

	getData := mustObject(t, getResp.Data)
	if got := stringValue(getData["task_id"]); got != "task-chain-1" {
		t.Fatalf("expected task_id task-chain-1, got %q", got)
	}
	if got := stringValue(getData["keyword"]); got != "openai" {
		t.Fatalf("expected keyword openai, got %q", got)
	}
	if got := stringValue(getData["created_by"]); got != appID {
		t.Fatalf("expected created_by %q, got %q", appID, got)
	}

	rejected := getJSONWithStatus(t, gatewayServer.URL+"/v1/collector/tasks/task-chain-1", consoleToken)
	if rejected.status != http.StatusUnauthorized {
		t.Fatalf("expected console token to be rejected by collector route, got %d: %+v", rejected.status, rejected.envelope)
	}
	if tenantID == "" {
		t.Fatal("expected tenant id to be set")
	}
}

func TestCollectorTaskChainDeployWiring(t *testing.T) {
	repoRoot := repoRootFromTestFile(t)

	assertFileContains(t, filepath.Join(repoRoot, "deploy/configs/local/gateway-api.yaml"),
		"SchedulerRPC:",
		"- scheduler-rpc:9004",
	)
	assertFileContains(t, filepath.Join(repoRoot, "deploy/configs/local/scheduler-rpc.yaml"),
		"Name: scheduler-rpc",
		"ListenOn: 0.0.0.0:9004",
	)
	assertFileContains(t, filepath.Join(repoRoot, "deploy/configs/local/collector-worker-rpc.yaml"),
		"Name: collector-worker-rpc",
		"ListenOn: 0.0.0.0:9005",
		"OutputRootDir: runtime/collector",
	)
	assertFileContains(t, filepath.Join(repoRoot, "deploy/docker/scheduler-rpc.Dockerfile"),
		"/app/runtime/logs/scheduler-rpc",
		"/app/scheduler-rpc",
	)
	assertFileContains(t, filepath.Join(repoRoot, "deploy/docker/collector-worker-rpc.Dockerfile"),
		"/app/runtime/logs/collector-worker-rpc",
		"/app/runtime/collector",
		"/app/collector-worker-rpc",
	)
	assertFileContains(t, filepath.Join(repoRoot, "deploy/xsonar/docker-compose.yml"),
		"GATEWAY_API_JWT_SECRET: \"${GATEWAY_API_JWT_SECRET:-change-me-gateway-jwt-secret}\"",
		"GATEWAY_API_JWT_ISSUER: \"${GATEWAY_API_JWT_ISSUER:-xsonar-gateway}\"",
		"CONSOLE_API_GATEWAY_JWT_SECRET: \"${CONSOLE_API_GATEWAY_JWT_SECRET:-${GATEWAY_API_JWT_SECRET:-change-me-gateway-jwt-secret}}\"",
		"CONSOLE_API_GATEWAY_JWT_ISSUER: \"${CONSOLE_API_GATEWAY_JWT_ISSUER:-${GATEWAY_API_JWT_ISSUER:-xsonar-gateway}}\"",
		"scheduler-rpc:",
		"SCHEDULER_RPC_LISTEN_ON: \"${SCHEDULER_RPC_LISTEN_ON:-0.0.0.0:9004}\"",
		"collector-worker-rpc:",
		"COLLECTOR_WORKER_RPC_LISTEN_ON: \"${COLLECTOR_WORKER_RPC_LISTEN_ON:-0.0.0.0:9005}\"",
		"COLLECTOR_WORKER_RPC_PROVIDER_RPC_TIMEOUT: \"${COLLECTOR_WORKER_RPC_PROVIDER_RPC_TIMEOUT:-10000}\"",
		"scheduler-rpc:\n        condition: service_healthy",
		"../../runtime/collector:/app/runtime/collector",
	)
	assertFileContains(t, filepath.Join(repoRoot, "deploy/xsonar/docker-compose.local.yml"),
		"../../deploy/configs/local/scheduler-rpc.yaml:/app/config/scheduler-rpc.yaml:ro",
		"../../deploy/configs/local/collector-worker-rpc.yaml:/app/config/collector-worker-rpc.yaml:ro",
	)
	assertFileContains(t, filepath.Join(repoRoot, "deploy/xsonar/.env.example"),
		"GATEWAY_API_JWT_SECRET=change-me-gateway-jwt-secret",
		"GATEWAY_API_JWT_ISSUER=xsonar-gateway",
		"CONSOLE_API_GATEWAY_JWT_SECRET=change-me-gateway-jwt-secret",
		"CONSOLE_API_GATEWAY_JWT_ISSUER=xsonar-gateway",
		"SCHEDULER_RPC_NAME=scheduler-rpc",
		"SCHEDULER_RPC_LISTEN_ON=0.0.0.0:9004",
		"COLLECTOR_WORKER_RPC_NAME=collector-worker-rpc",
		"COLLECTOR_WORKER_RPC_LISTEN_ON=0.0.0.0:9005",
		"COLLECTOR_WORKER_RPC_PROVIDER_RPC_TIMEOUT=10000",
	)
}

type envelopeResponse struct {
	Code      int             `json:"code"`
	Message   string          `json:"message"`
	Data      json.RawMessage `json:"data"`
	RequestID string          `json:"request_id"`
}

type responseWithStatus struct {
	status   int
	envelope envelopeResponse
}

func loginConsoleAdmin(t *testing.T, baseURL string) string {
	t.Helper()

	resp := postJSON(t, baseURL+"/admin/v1/auth/login", map[string]any{
		"username": "admin",
		"password": "admin123456",
	}, "")
	if resp.Code != 0 {
		t.Fatalf("unexpected login response: %+v", resp)
	}

	data := mustObject(t, resp.Data)
	token := stringValue(data["token"])
	if token == "" {
		t.Fatalf("expected admin token in response: %+v", data)
	}
	return token
}

func createTenantAndApp(t *testing.T, baseURL, consoleToken string) (tenantID, appID, appKey, appSecret string) {
	t.Helper()

	tenantResp := postJSON(t, baseURL+"/admin/v1/tenants", map[string]any{
		"name": "Collector Tenant",
	}, consoleToken)
	if tenantResp.Code != 0 {
		t.Fatalf("unexpected tenant response: %+v", tenantResp)
	}
	tenantData := mustObject(t, tenantResp.Data)
	tenantID = stringValue(tenantData["tenant_id"])
	if tenantID == "" {
		t.Fatalf("expected tenant_id in response: %+v", tenantData)
	}

	appResp := postJSON(t, baseURL+"/admin/v1/tenants/"+tenantID+"/apps", map[string]any{
		"name":        "Collector App",
		"daily_quota": 100,
		"qps_limit":   10,
	}, consoleToken)
	if appResp.Code != 0 {
		t.Fatalf("unexpected app response: %+v", appResp)
	}
	appData := mustObject(t, appResp.Data)
	appID = stringValue(appData["app_id"])
	appKey = stringValue(appData["app_key"])
	appSecret = stringValue(appData["app_secret"])
	if appID == "" || appKey == "" || appSecret == "" {
		t.Fatalf("expected app payload, got %+v", appData)
	}
	return tenantID, appID, appKey, appSecret
}

func issueGatewayTokenForCollector(t *testing.T, baseURL, consoleToken, tenantID, appID string, ttl int64) string {
	t.Helper()

	resp := postJSON(t, baseURL+"/admin/v1/gateway/token", map[string]any{
		"tenant_id": tenantID,
		"app_id":    appID,
		"ttl":       ttl,
	}, consoleToken)
	if resp.Code != 0 {
		t.Fatalf("unexpected gateway token response: %+v", resp)
	}
	data := mustObject(t, resp.Data)
	token := stringValue(data["token"])
	if token == "" {
		t.Fatalf("expected gateway token in response: %+v", data)
	}
	return token
}

func postJSON(t *testing.T, url string, payload any, bearerToken string) envelopeResponse {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	return doRequest(t, req)
}

func getJSON(t *testing.T, url string, bearerToken string) envelopeResponse {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	return doRequest(t, req)
}

func getJSONWithStatus(t *testing.T, url string, bearerToken string) responseWithStatus {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	var envelope envelopeResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode response envelope: %v", err)
	}
	return responseWithStatus{
		status:   resp.StatusCode,
		envelope: envelope,
	}
}

func doRequest(t *testing.T, req *http.Request) envelopeResponse {
	t.Helper()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var envelope envelopeResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode response envelope: %v", err)
	}
	return envelope
}

func mustObject(t *testing.T, payload json.RawMessage) map[string]any {
	t.Helper()

	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	return data
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func repoRootFromTestFile(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve runtime caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func assertFileContains(t *testing.T, path string, snippets ...string) {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	content := string(body)
	for _, snippet := range snippets {
		if !strings.Contains(content, snippet) {
			t.Fatalf("expected %s to contain %q", path, snippet)
		}
	}
}
