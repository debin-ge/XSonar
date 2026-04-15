package internal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/rest/pathvar"
	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type stubJSONClient struct {
	getFunc  func(ctx context.Context, path string, query url.Values) (*clients.EnvelopeResponse, error)
	postFunc func(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error)
	putFunc  func(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error)
}

func (s stubJSONClient) Get(ctx context.Context, path string, query url.Values) (*clients.EnvelopeResponse, error) {
	if s.getFunc == nil {
		return nil, errors.New("unexpected GET call")
	}
	return s.getFunc(ctx, path, query)
}

func (s stubJSONClient) Post(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
	if s.postFunc == nil {
		return nil, errors.New("unexpected POST call")
	}
	return s.postFunc(ctx, path, payload)
}

func (s stubJSONClient) Put(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
	if s.putFunc == nil {
		return nil, errors.New("unexpected PUT call")
	}
	return s.putFunc(ctx, path, payload)
}

func (s stubJSONClient) Health(ctx context.Context) (*clients.EnvelopeResponse, error) {
	return s.Get(ctx, "/healthz", nil)
}

func (s stubJSONClient) AuthenticateConsoleUser(ctx context.Context, req *accessservice.AuthenticateConsoleUserRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/AuthenticateConsoleUser", req)
}

func (s stubJSONClient) GetAppAuthContextByID(ctx context.Context, req *accessservice.GetAppAuthContextByIDRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/GetAppAuthContextByID", req)
}

func (s stubJSONClient) CheckReplay(ctx context.Context, req *accessservice.CheckReplayRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/CheckReplay", req)
}

func (s stubJSONClient) CheckAndReserveQuota(ctx context.Context, req *accessservice.CheckAndReserveQuotaRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/CheckAndReserveQuota", req)
}

func (s stubJSONClient) ReleaseQuotaOnFailure(ctx context.Context, req *accessservice.ReleaseQuotaOnFailureRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/ReleaseQuotaOnFailure", req)
}

func (s stubJSONClient) RecordUsageStat(ctx context.Context, req *accessservice.RecordUsageStatRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/RecordUsageStat", req)
}

func (s stubJSONClient) ListTenants(ctx context.Context, req *accessservice.ListTenantsRequest) (*clients.EnvelopeResponse, error) {
	_ = req
	return s.Get(ctx, "/rpc/ListTenants", nil)
}

func (s stubJSONClient) CreateTenant(ctx context.Context, req *accessservice.CreateTenantRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/CreateTenant", req)
}

func (s stubJSONClient) ListTenantApps(ctx context.Context, req *accessservice.ListTenantAppsRequest) (*clients.EnvelopeResponse, error) {
	query := url.Values{}
	query.Set("tenant_id", req.GetTenantId())
	return s.Get(ctx, "/rpc/ListTenantApps", query)
}

func (s stubJSONClient) CreateTenantApp(ctx context.Context, req *accessservice.CreateTenantAppRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/CreateTenantApp", req)
}

func (s stubJSONClient) UpdateTenantAppStatus(ctx context.Context, req *accessservice.UpdateTenantAppStatusRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/UpdateTenantAppStatus", req)
}

func (s stubJSONClient) UpdateAppQuota(ctx context.Context, req *accessservice.UpdateAppQuotaRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/UpdateAppQuota", req)
}

func (s stubJSONClient) CheckIpBan(ctx context.Context, req *accessservice.CheckIpBanRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/CheckIpBan", req)
}

func (s stubJSONClient) QueryUsageStats(ctx context.Context, req *accessservice.QueryUsageStatsRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/QueryUsageStats", req)
}

func (s stubJSONClient) ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/ResolvePolicy", req)
}

func (s stubJSONClient) CheckAppPolicyAccess(ctx context.Context, req *policyservice.CheckAppPolicyAccessRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/CheckAppPolicyAccess", req)
}

func (s stubJSONClient) ListPolicies(ctx context.Context, req *policyservice.ListPoliciesRequest) (*clients.EnvelopeResponse, error) {
	_ = req
	return s.Get(ctx, "/rpc/ListPolicies", nil)
}

func (s stubJSONClient) PublishPolicyConfig(ctx context.Context, req *policyservice.PublishPolicyConfigRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/PublishPolicyConfig", req)
}

func (s stubJSONClient) BindAppPolicies(ctx context.Context, req *policyservice.BindAppPoliciesRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/BindAppPolicies", req)
}

func (s stubJSONClient) HealthCheckProvider(ctx context.Context, req *providerservice.HealthCheckProviderRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/HealthCheckProvider", req)
}

func (s stubJSONClient) ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*clients.EnvelopeResponse, error) {
	return s.Post(ctx, "/rpc/ExecutePolicy", req)
}

func TestConsoleLoginProxy(t *testing.T) {
	svc := newConsoleServiceWithConfigAndAllClients(
		ConsoleDefaults(),
		xlog.NewStdout("console-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path != "/rpc/AuthenticateConsoleUser" {
					return nil, errors.New("unexpected path: " + path)
				}
				req := payload.(*accessservice.AuthenticateConsoleUserRequest)
				if req.Username != "admin" {
					t.Fatalf("expected username admin, got %#v", req.Username)
				}
				return okEnvelope(map[string]any{
					"user_id": "user_1",
					"role":    "platform_admin",
				}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/auth/login", strings.NewReader(`{"username":"admin","password":"admin123456"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	svc.handleLogin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Code int `json:"code"`
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != model.CodeOK || response.Data.Token == "" {
		t.Fatalf("unexpected response: %+v", response)
	}
	claims, err := shared.ParseAndValidateJWT(ConsoleDefaults().JWTSecret, response.Data.Token, time.Now())
	if err != nil {
		t.Fatalf("expected valid jwt token, got error: %v", err)
	}
	if claims.Subject != "user_1" || claims.Role != "platform_admin" {
		t.Fatalf("unexpected jwt claims: %+v", claims)
	}
}

func TestConsoleIssueGatewayTokenReturnsGatewayJWT(t *testing.T) {
	cfg := ConsoleDefaults()
	cfg.GatewayJWTSecret = "gateway-secret"
	cfg.GatewayJWTIssuer = "gateway-issuer"

	svc := newConsoleServiceWithConfigAndAllClients(
		cfg,
		xlog.NewStdout("console-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path != "/rpc/GetAppAuthContextByID" {
					return nil, errors.New("unexpected path: " + path)
				}
				req := payload.(*accessservice.GetAppAuthContextByIDRequest)
				if req.AppId != "app_1" {
					t.Fatalf("expected app_1, got %q", req.AppId)
				}
				return okEnvelope(map[string]any{
					"tenant_id":   "tenant_1",
					"app_id":      "app_1",
					"status":      "active",
					"daily_quota": 100,
					"qps_limit":   10,
				}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	adminToken, err := shared.SignJWT(cfg.JWTSecret, cfg.JWTIssuer, "user_1", "platform_admin", time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sign admin jwt: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/gateway/token", strings.NewReader(`{"tenant_id":"tenant_1","app_id":"app_1","ttl":3600}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec := httptest.NewRecorder()

	svc.handleIssueGatewayToken(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response struct {
		Code int `json:"code"`
		Data struct {
			Token            string `json:"token"`
			TokenType        string `json:"token_type"`
			AppID            string `json:"app_id"`
			TenantID         string `json:"tenant_id"`
			ExpiresInSeconds int64  `json:"expires_in_seconds"`
			ExpiresAt        string `json:"expires_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != model.CodeOK {
		t.Fatalf("unexpected response code: %+v", response)
	}
	if response.Data.Token == "" || response.Data.TokenType != "Bearer" {
		t.Fatalf("unexpected response data: %+v", response.Data)
	}
	if response.Data.AppID != "app_1" || response.Data.TenantID != "tenant_1" {
		t.Fatalf("unexpected ids: %+v", response.Data)
	}
	if response.Data.ExpiresInSeconds != 3600 || response.Data.ExpiresAt == "" {
		t.Fatalf("expected 3600-second expiry, got %+v", response.Data)
	}

	claims, err := shared.ParseAndValidateJWT(cfg.GatewayJWTSecret, response.Data.Token, time.Now())
	if err != nil {
		t.Fatalf("parse gateway jwt: %v", err)
	}
	if claims.Subject != "app_1" || claims.Role != "gateway_app" || claims.Issuer != cfg.GatewayJWTIssuer {
		t.Fatalf("unexpected gateway claims: %+v", claims)
	}
}

func TestConsoleIssueGatewayTokenSupportsInfiniteTTL(t *testing.T) {
	cfg := ConsoleDefaults()
	cfg.GatewayJWTSecret = "gateway-secret"
	cfg.GatewayJWTIssuer = "gateway-issuer"

	svc := newConsoleServiceWithConfigAndAllClients(
		cfg,
		xlog.NewStdout("console-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path != "/rpc/GetAppAuthContextByID" {
					return nil, errors.New("unexpected path: " + path)
				}
				req := payload.(*accessservice.GetAppAuthContextByIDRequest)
				if req.AppId != "app_1" {
					t.Fatalf("expected app_1, got %q", req.AppId)
				}
				return okEnvelope(map[string]any{
					"tenant_id":   "tenant_1",
					"app_id":      "app_1",
					"status":      "active",
					"daily_quota": 100,
					"qps_limit":   10,
				}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	adminToken, err := shared.SignJWT(cfg.JWTSecret, cfg.JWTIssuer, "user_1", "platform_admin", time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sign admin jwt: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/gateway/token", strings.NewReader(`{"tenant_id":"tenant_1","app_id":"app_1","ttl":0}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec := httptest.NewRecorder()

	svc.handleIssueGatewayToken(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response struct {
		Code int `json:"code"`
		Data struct {
			Token            string `json:"token"`
			ExpiresInSeconds int64  `json:"expires_in_seconds"`
			ExpiresAt        string `json:"expires_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.ExpiresInSeconds != 0 || response.Data.ExpiresAt != "" {
		t.Fatalf("expected infinite ttl metadata, got %+v", response.Data)
	}

	claims, err := shared.ParseAndValidateJWT(cfg.GatewayJWTSecret, response.Data.Token, time.Now().Add(365*24*time.Hour))
	if err != nil {
		t.Fatalf("parse infinite gateway jwt: %v", err)
	}
	if claims.ExpiresAt != 0 {
		t.Fatalf("expected no exp claim, got %+v", claims)
	}
}

func TestConsoleGetTenantDetailAggregatesTenantAndApps(t *testing.T) {
	svc := newConsoleServiceWithConfigAndAllClients(
		ConsoleDefaults(),
		xlog.NewStdout("console-test"),
		stubJSONClient{
			getFunc: func(_ context.Context, path string, query url.Values) (*clients.EnvelopeResponse, error) {
				switch path {
				case "/rpc/ListTenants":
					return okEnvelope(map[string]any{
						"items": []map[string]any{
							{"tenant_id": "tenant_1", "name": "Acme", "status": "active"},
						},
					}), nil
				case "/rpc/ListTenantApps":
					if query.Get("tenant_id") != "tenant_1" {
						t.Fatalf("expected tenant_id filter, got %#v", query)
					}
					return okEnvelope(map[string]any{
						"items": []map[string]any{
							{"app_id": "app_1", "tenant_id": "tenant_1", "status": "active"},
						},
					}), nil
				default:
					return nil, errors.New("unexpected path: " + path)
				}
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	req := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants/tenant_1", nil)
	req = pathvar.WithVars(req, map[string]string{"id": "tenant_1"})
	token, err := shared.SignJWT(ConsoleDefaults().JWTSecret, ConsoleDefaults().JWTIssuer, "user_1", "platform_admin", time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	svc.handleGetTenantDetail(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Data struct {
			Tenant map[string]any   `json:"tenant"`
			Apps   []map[string]any `json:"apps"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.Tenant["tenant_id"] != "tenant_1" {
		t.Fatalf("unexpected tenant payload: %+v", response.Data.Tenant)
	}
	if len(response.Data.Apps) != 1 {
		t.Fatalf("expected one app, got %d", len(response.Data.Apps))
	}
}

func TestConsoleRequiresAdminToken(t *testing.T) {
	svc := newConsoleServiceWithConfigAndAllClients(ConsoleDefaults(), xlog.NewStdout("console-test"), stubJSONClient{}, stubJSONClient{}, stubJSONClient{})
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil)
	rec := httptest.NewRecorder()

	svc.handleListTenants(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestConsoleServiceHealthIncludesProviderAndUpstream(t *testing.T) {
	svc := newConsoleServiceWithConfigAndAllClients(
		ConsoleDefaults(),
		xlog.NewStdout("console-test"),
		stubJSONClient{
			getFunc: func(_ context.Context, path string, query url.Values) (*clients.EnvelopeResponse, error) {
				if path != "/healthz" {
					return nil, errors.New("unexpected access path: " + path)
				}
				return okEnvelope(map[string]any{"service": "access-rpc", "status": "ok"}), nil
			},
		},
		stubJSONClient{
			getFunc: func(_ context.Context, path string, query url.Values) (*clients.EnvelopeResponse, error) {
				if path != "/healthz" {
					return nil, errors.New("unexpected policy path: " + path)
				}
				return okEnvelope(map[string]any{"service": "policy-rpc", "status": "ok"}), nil
			},
		},
		stubJSONClient{
			getFunc: func(_ context.Context, path string, query url.Values) (*clients.EnvelopeResponse, error) {
				if path != "/healthz" {
					return nil, errors.New("unexpected provider path: " + path)
				}
				return okEnvelope(map[string]any{"service": "provider-rpc", "status": "ok"}), nil
			},
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path != "/rpc/HealthCheckProvider" {
					return nil, errors.New("unexpected provider rpc path: " + path)
				}
				request := payload.(*providerservice.HealthCheckProviderRequest)
				if request.ProviderName != "fapi.uk" {
					t.Fatalf("unexpected provider health payload: %#v", request)
				}
				return okEnvelope(map[string]any{
					"provider_name": "fapi.uk",
					"healthy":       true,
					"reachable":     true,
					"health_state":  "ok",
					"status_code":   200,
					"result_code":   "UPSTREAM_OK",
				}), nil
			},
		},
	)

	req := httptest.NewRequest(http.MethodGet, "/admin/v1/health/services", nil)
	rec := httptest.NewRecorder()

	svc.handleServiceHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response struct {
		Code int `json:"code"`
		Data struct {
			Services []map[string]any `json:"services"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != model.CodeOK {
		t.Fatalf("unexpected envelope code: %d", response.Code)
	}
	if len(response.Data.Services) != 4 {
		t.Fatalf("expected 4 health items, got %#v", response.Data.Services)
	}

	foundProvider := false
	foundUpstream := false
	for _, item := range response.Data.Services {
		if item["name"] == "provider-rpc" {
			foundProvider = true
			if item["healthy"] != true {
				t.Fatalf("expected provider-rpc healthy, got %#v", item)
			}
		}
		if item["name"] == "provider-upstream" {
			foundUpstream = true
			if item["healthy"] != true || item["provider_name"] != "fapi.uk" || item["health_state"] != "ok" {
				t.Fatalf("expected provider upstream healthy, got %#v", item)
			}
		}
	}

	if !foundProvider || !foundUpstream {
		t.Fatalf("missing provider health items: %#v", response.Data.Services)
	}
}

func TestBridgeCreateTenantRejectsOversizedName(t *testing.T) {
	called := false
	bridge := NewBridge(
		ConsoleDefaults(),
		xlog.NewStdout("console-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				called = true
				return okEnvelope(map[string]any{"tenant_id": "tenant_1"}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{"name":"`+strings.Repeat("a", 33)+`"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+mustAdminToken(t))
	rec := httptest.NewRecorder()

	bridge.HandleCreateTenant(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if called {
		t.Fatal("expected oversized tenant name to be rejected before downstream RPC")
	}
}

func TestBridgeCreateTenantAppRejectsInvalidQuotaType(t *testing.T) {
	called := false
	bridge := NewBridge(
		ConsoleDefaults(),
		xlog.NewStdout("console-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				called = true
				return okEnvelope(map[string]any{"app_id": "app_1"}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/tenant_1/apps", strings.NewReader(`{"name":"Primary App","daily_quota":"oops","qps_limit":10}`))
	req = pathvar.WithVars(req, map[string]string{"id": "tenant_1"})
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+mustAdminToken(t))
	rec := httptest.NewRecorder()

	bridge.HandleCreateTenantApp(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if called {
		t.Fatal("expected invalid quota type to be rejected before downstream RPC")
	}
}

func TestBridgeCreateTenantAppAcceptsStandardPathValue(t *testing.T) {
	called := false
	bridge := NewBridge(
		ConsoleDefaults(),
		xlog.NewStdout("console-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				called = true
				if path != "/rpc/CreateTenantApp" {
					return nil, errors.New("unexpected path: " + path)
				}
				req := payload.(*accessservice.CreateTenantAppRequest)
				if req.TenantId != "tenant_1" {
					t.Fatalf("expected tenant_1 tenant id, got %#v", req.TenantId)
				}
				if req.Name != "Primary App" || req.DailyQuota != 100 || req.QpsLimit != 10 {
					t.Fatalf("unexpected create tenant app payload: %#v", req)
				}
				return okEnvelope(map[string]any{"app_id": "app_1"}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/tenant_1/apps", strings.NewReader(`{"name":"Primary App","daily_quota":100,"qps_limit":10}`))
	req.SetPathValue("id", "tenant_1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+mustAdminToken(t))
	rec := httptest.NewRecorder()

	bridge.HandleCreateTenantApp(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !called {
		t.Fatal("expected create tenant app rpc to be called")
	}
}

func okEnvelope(data any) *clients.EnvelopeResponse {
	payload, _ := json.Marshal(data)
	return &clients.EnvelopeResponse{
		Code:    model.CodeOK,
		Message: "ok",
		Data:    payload,
	}
}

func mustAdminToken(t *testing.T) string {
	t.Helper()
	token, err := shared.SignJWT(ConsoleDefaults().JWTSecret, ConsoleDefaults().JWTIssuer, "user_1", "platform_admin", time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	return token
}
