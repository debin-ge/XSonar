package internal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

func TestGatewayProxyAcceptsGatewayBearerJWT(t *testing.T) {
	var recordedUsage *accessservice.RecordUsageStatRequest

	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContextByID":
				req := payload.(*accessservice.GetAppAuthContextByIDRequest)
				if req.AppId != "app-1" {
					t.Fatalf("unexpected app id: %q", req.AppId)
				}
				return okEnvelope(map[string]any{
					"tenant_id":   "tenant-1",
					"app_id":      "app-1",
					"app_key":     "app-key-1",
					"app_secret":  "secret-1",
					"status":      "active",
					"daily_quota": 100,
					"qps_limit":   10,
				}), nil
			case "/rpc/CheckAndReserveQuota":
				return okEnvelope(map[string]any{"allowed": true}), nil
			case "/rpc/RecordUsageStat":
				recordedUsage = payload.(*accessservice.RecordUsageStatRequest)
				return okEnvelope(map[string]any{"recorded": true}), nil
			case "/rpc/ReleaseQuotaOnFailure":
				return okEnvelope(map[string]any{"released": true}), nil
			default:
				return nil, errors.New("unexpected access path: " + path)
			}
		},
	}

	policyClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/ResolvePolicy":
				req := payload.(*policyservice.ResolvePolicyRequest)
				if req.Path != "/v1/users/by-ids" || req.Method != http.MethodGet {
					t.Fatalf("unexpected resolve request: %+v", req)
				}
				return okEnvelope(map[string]any{
					"policy_key":       "users_by_ids_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/usersByIdRestIds",
					"allowed_params":   []string{"userIds"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider-key-1",
				}), nil
			case "/rpc/CheckAppPolicyAccess":
				return okEnvelope(map[string]any{"allowed": true}), nil
			default:
				return nil, errors.New("unexpected policy path: " + path)
			}
		},
	}

	providerClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			if path != "/rpc/ExecutePolicy" {
				return nil, errors.New("unexpected provider path: " + path)
			}
			req := payload.(*providerservice.ExecutePolicyRequest)
			var query map[string]any
			if err := json.Unmarshal([]byte(req.QueryJson), &query); err != nil {
				t.Fatalf("decode provider query: %v", err)
			}
			if query["userIds"] != "1,2" {
				t.Fatalf("unexpected provider query: %#v", query)
			}
			return okEnvelope(map[string]any{
				"status_code":          200,
				"result_code":          "UPSTREAM_OK",
				"body":                 map[string]any{"users": []any{map[string]any{"id": "1"}}},
				"upstream_duration_ms": 12,
			}), nil
		},
	}

	svc := newGatewayServiceWithClients(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient)
	token := mustSignGatewayJWT(t, "gateway-secret", "gateway-issuer", "app-1")
	svc.jwtSecret = "gateway-secret"
	svc.jwtIssuer = "gateway-issuer"

	query := url.Values{}
	query.Set("userIds", "1,2")
	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if recordedUsage == nil {
		t.Fatal("expected usage stat to be recorded")
	}
	if recordedUsage.AppId != "app-1" || recordedUsage.TenantId != "tenant-1" {
		t.Fatalf("unexpected recorded usage payload: %+v", recordedUsage)
	}
}

func TestGatewayProxyAcceptsGatewayBearerJWTWithoutExpiry(t *testing.T) {
	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContextByID":
				return okEnvelope(map[string]any{
					"tenant_id":   "tenant-1",
					"app_id":      "app-1",
					"status":      "active",
					"daily_quota": 100,
					"qps_limit":   10,
				}), nil
			case "/rpc/CheckAndReserveQuota":
				return okEnvelope(map[string]any{"allowed": true}), nil
			case "/rpc/RecordUsageStat":
				return okEnvelope(map[string]any{"recorded": true}), nil
			case "/rpc/ReleaseQuotaOnFailure":
				return okEnvelope(map[string]any{"released": true}), nil
			default:
				return nil, errors.New("unexpected access path: " + path)
			}
		},
	}

	policyClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/ResolvePolicy":
				return okEnvelope(map[string]any{
					"policy_key":       "users_by_ids_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/usersByIdRestIds",
					"allowed_params":   []string{"userIds"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider-key-1",
				}), nil
			case "/rpc/CheckAppPolicyAccess":
				return okEnvelope(map[string]any{"allowed": true}), nil
			default:
				return nil, errors.New("unexpected policy path: " + path)
			}
		},
	}

	providerClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			if path != "/rpc/ExecutePolicy" {
				return nil, errors.New("unexpected provider path: " + path)
			}
			return okEnvelope(map[string]any{
				"status_code":          200,
				"result_code":          "UPSTREAM_OK",
				"body":                 map[string]any{"users": []any{map[string]any{"id": "1"}}},
				"upstream_duration_ms": 12,
			}), nil
		},
	}

	svc := newGatewayServiceWithClients(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient)
	token, err := shared.SignJWT("gateway-secret", "gateway-issuer", "app-1", "gateway_app", 0, time.Now())
	if err != nil {
		t.Fatalf("sign gateway jwt: %v", err)
	}
	svc.jwtSecret = "gateway-secret"
	svc.jwtIssuer = "gateway-issuer"

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?userIds=1,2", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func mustSignGatewayJWT(t *testing.T, secret, issuer, subject string) string {
	t.Helper()

	token, err := shared.SignJWT(secret, issuer, subject, "gateway_app", time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sign gateway jwt: %v", err)
	}
	return token
}
