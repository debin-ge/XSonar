package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type stubJSONClient struct {
	postFunc func(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error)
}

func (s stubJSONClient) call(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
	if s.postFunc == nil {
		return nil, errors.New("unexpected call")
	}
	return s.postFunc(ctx, path, payload)
}

func (s stubJSONClient) CheckIpBan(ctx context.Context, req *accessservice.CheckIpBanRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/CheckIpBan", req)
}

func (s stubJSONClient) GetAppAuthContext(ctx context.Context, req *accessservice.GetAppAuthContextRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/GetAppAuthContext", req)
}

func (s stubJSONClient) CheckReplay(ctx context.Context, req *accessservice.CheckReplayRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/CheckReplay", req)
}

func (s stubJSONClient) CheckAndReserveQuota(ctx context.Context, req *accessservice.CheckAndReserveQuotaRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/CheckAndReserveQuota", req)
}

func (s stubJSONClient) ReleaseQuotaOnFailure(ctx context.Context, req *accessservice.ReleaseQuotaOnFailureRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/ReleaseQuotaOnFailure", req)
}

func (s stubJSONClient) RecordUsageStat(ctx context.Context, req *accessservice.RecordUsageStatRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/RecordUsageStat", req)
}

func (s stubJSONClient) ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/ResolvePolicy", req)
}

func (s stubJSONClient) CheckAppPolicyAccess(ctx context.Context, req *policyservice.CheckAppPolicyAccessRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/CheckAppPolicyAccess", req)
}

func (s stubJSONClient) ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/ExecutePolicy", req)
}

func TestGatewayProxySuccess(t *testing.T) {
	var recordedUsage *accessservice.RecordUsageStatRequest

	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_1",
					"app_id":     "app_1",
					"app_key":    "app_key_1",
					"app_secret": "secret_1",
					"status":     "active",
				}), nil
			case "/rpc/CheckReplay":
				return okEnvelope(map[string]any{"accepted": true}), nil
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
				return okEnvelope(map[string]any{
					"policy_key":       "users_by_ids_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/usersByIdRestIds",
					"allowed_params":   []string{"userIds"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_1",
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
				t.Fatalf("decode query json: %v", err)
			}
			if query["resFormat"] != "json" {
				t.Fatalf("expected resFormat=json injection, got %#v", query["resFormat"])
			}
			if query["userIds"] != "1,2" {
				t.Fatalf("expected userIds to be passed through, got %#v", query["userIds"])
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

	query := url.Values{}
	query.Set("userIds", "1,2")
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	query.Set("timestamp", timestamp)
	query.Set("nonce", "nonce-1")
	query.Set("appKey", "app_key_1")
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/users/by-ids", query, timestamp, "nonce-1")
	query.Set("signature", signature)

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response struct {
		Code int `json:"code"`
		Data struct {
			Users []map[string]any `json:"users"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != model.CodeOK {
		t.Fatalf("expected code 0, got %d", response.Code)
	}
	if len(response.Data.Users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(response.Data.Users))
	}
	if recordedUsage == nil || recordedUsage.PolicyKey != "users_by_ids_v1" {
		t.Fatalf("expected usage stat to be recorded, got %#v", recordedUsage)
	}
}

func TestGatewayRejectsInvalidSignature(t *testing.T) {
	svc := newGatewayServiceWithClients(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path == "/rpc/GetAppAuthContext" {
					return okEnvelope(map[string]any{
						"tenant_id":  "tenant_1",
						"app_id":     "app_1",
						"app_key":    "app_key_1",
						"app_secret": "secret_1",
						"status":     "active",
					}), nil
				}
				if path == "/rpc/CheckIpBan" {
					return okEnvelope(map[string]any{"blocked": false}), nil
				}
				return nil, errors.New("unexpected access path")
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	query := url.Values{}
	query.Set("userIds", "1,2")
	query.Set("timestamp", strconv.FormatInt(time.Now().UTC().Unix(), 10))
	query.Set("nonce", "nonce-1")
	query.Set("appKey", "app_key_1")
	query.Set("signature", "wrong-signature")

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	svc.handleProxy(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayProxyPreservesLargeIntegersFromProviderPayload(t *testing.T) {
	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_1",
					"app_id":     "app_1",
					"app_key":    "app_key_1",
					"app_secret": "secret_1",
					"status":     "active",
				}), nil
			case "/rpc/CheckReplay":
				return okEnvelope(map[string]any{"accepted": true}), nil
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
					"policy_key":       "tweets_detail_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/tweetTimeline",
					"allowed_params":   []string{"tweetId"},
					"required_params":  []string{"tweetId"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_1",
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
			return &clients.EnvelopeResponse{
				Code:    model.CodeOK,
				Message: "ok",
				Data:    json.RawMessage(`{"status_code":200,"result_code":"UPSTREAM_OK","body":{"tweet":{"id":9007199254740993}},"upstream_duration_ms":12}`),
			}, nil
		},
	}

	svc := newGatewayServiceWithClients(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient)

	query := url.Values{}
	query.Set("tweetId", "9007199254740993")
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	query.Set("timestamp", timestamp)
	query.Set("nonce", "nonce-1")
	query.Set("appKey", "app_key_1")
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/tweets/detail", query, timestamp, "nonce-1")
	query.Set("signature", signature)

	req := httptest.NewRequest(http.MethodGet, "/v1/tweets/detail?"+query.Encode(), nil)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response struct {
		Data struct {
			Tweet struct {
				ID json.Number `json:"id"`
			} `json:"tweet"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.Tweet.ID.String() != "9007199254740993" {
		t.Fatalf("expected large integer to be preserved, got %s", response.Data.Tweet.ID.String())
	}
}

func TestGatewayMapsTweetDetailTweetIDToUpstreamID(t *testing.T) {
	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_1",
					"app_id":     "app_1",
					"app_key":    "app_key_1",
					"app_secret": "secret_1",
					"status":     "active",
				}), nil
			case "/rpc/CheckReplay":
				return okEnvelope(map[string]any{"accepted": true}), nil
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
					"policy_key":       "tweets_detail_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/tweetTimeline",
					"allowed_params":   []string{"tweetId", "cursor"},
					"required_params":  []string{"tweetId"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_1",
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
				t.Fatalf("decode query json: %v", err)
			}
			if query["id"] != "1971453180132327700" {
				t.Fatalf("expected upstream id to be mapped, got %#v", query)
			}
			if _, exists := query["tweetId"]; exists {
				t.Fatalf("expected public tweetId to be removed, got %#v", query)
			}
			if query["resFormat"] != "json" {
				t.Fatalf("expected resFormat=json injection, got %#v", query["resFormat"])
			}
			return okEnvelope(map[string]any{
				"status_code":          200,
				"result_code":          "UPSTREAM_OK",
				"body":                 map[string]any{"data": map[string]any{"id": "1971453180132327700"}},
				"upstream_duration_ms": 12,
			}), nil
		},
	}

	svc := newGatewayServiceWithClients(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient)

	query := url.Values{}
	query.Set("tweetId", "1971453180132327700")
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	query.Set("timestamp", timestamp)
	query.Set("nonce", "nonce-1")
	query.Set("appKey", "app_key_1")
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/tweets/detail", query, timestamp, "nonce-1")
	query.Set("signature", signature)

	req := httptest.NewRequest(http.MethodGet, "/v1/tweets/detail?"+query.Encode(), nil)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayRejectsMissingRequiredQueryParam(t *testing.T) {
	providerCalled := false

	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_1",
					"app_id":     "app_1",
					"app_key":    "app_key_1",
					"app_secret": "secret_1",
					"status":     "active",
				}), nil
			case "/rpc/CheckReplay":
				return okEnvelope(map[string]any{"accepted": true}), nil
			case "/rpc/CheckAndReserveQuota":
				return okEnvelope(map[string]any{"allowed": true}), nil
			case "/rpc/ReleaseQuotaOnFailure":
				return okEnvelope(map[string]any{"released": true}), nil
			case "/rpc/RecordUsageStat":
				return okEnvelope(map[string]any{"recorded": true}), nil
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
					"policy_key":       "tweets_detail_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/tweetTimeline",
					"allowed_params":   []string{"tweetId", "cursor"},
					"required_params":  []string{"tweetId"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_1",
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
			providerCalled = true
			return nil, errors.New("provider should not be called")
		},
	}

	svc := newGatewayServiceWithClients(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient)

	query := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	query.Set("timestamp", timestamp)
	query.Set("nonce", "nonce-1")
	query.Set("appKey", "app_key_1")
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/tweets/detail", query, timestamp, "nonce-1")
	query.Set("signature", signature)

	req := httptest.NewRequest(http.MethodGet, "/v1/tweets/detail?"+query.Encode(), nil)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if providerCalled {
		t.Fatal("expected provider to be skipped when required query param is missing")
	}

	var response struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != model.CodeInvalidRequest || response.Message != "parameter is required: tweetId" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestGatewayDevModeAcceptsAppSecretWithoutSignature(t *testing.T) {
	var recordedUsage *accessservice.RecordUsageStatRequest

	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_1",
					"app_id":     "app_1",
					"app_key":    "app_key_1",
					"app_secret": "secret_1",
					"status":     "active",
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
				return okEnvelope(map[string]any{
					"policy_key":       "users_by_ids_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/usersByIdRestIds",
					"allowed_params":   []string{"userIds"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_1",
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
				t.Fatalf("decode query json: %v", err)
			}
			if _, exists := query["appSecret"]; exists {
				t.Fatalf("expected appSecret to be stripped from upstream query, got %#v", query)
			}
			return okEnvelope(map[string]any{
				"status_code":          200,
				"result_code":          "UPSTREAM_OK",
				"body":                 map[string]any{"users": []any{map[string]any{"id": "1"}}},
				"upstream_duration_ms": 12,
			}), nil
		},
	}

	svc := newGatewayServiceWithMode(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient, "dev")

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?userIds=1,2&appKey=app_key_1&appSecret=secret_1", nil)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if recordedUsage == nil || recordedUsage.PolicyKey != "users_by_ids_v1" {
		t.Fatalf("expected usage stat to be recorded, got %#v", recordedUsage)
	}
}

func TestGatewayDevModeRejectsInvalidAppSecret(t *testing.T) {
	svc := newGatewayServiceWithMode(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				switch path {
				case "/rpc/CheckIpBan":
					return okEnvelope(map[string]any{"blocked": false}), nil
				case "/rpc/GetAppAuthContext":
					return okEnvelope(map[string]any{
						"tenant_id":  "tenant_1",
						"app_id":     "app_1",
						"app_key":    "app_key_1",
						"app_secret": "secret_1",
						"status":     "active",
					}), nil
				default:
					return nil, errors.New("unexpected access path: " + path)
				}
			},
		},
		stubJSONClient{},
		stubJSONClient{},
		"dev",
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?appKey=app_key_1", nil)
	req.Header.Set("AppSecret", "wrong-secret")
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayDevModeTrimsAppSecretBeforeVerification(t *testing.T) {
	svc := newGatewayServiceWithMode(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				switch path {
				case "/rpc/CheckIpBan":
					return okEnvelope(map[string]any{"blocked": false}), nil
				case "/rpc/GetAppAuthContext":
					return okEnvelope(map[string]any{
						"tenant_id":  "tenant_1",
						"app_id":     "app_1",
						"app_key":    "app_key_1",
						"app_secret": "secret_1",
						"status":     "active",
					}), nil
				case "/rpc/CheckAndReserveQuota":
					return okEnvelope(map[string]any{"allowed": true}), nil
				case "/rpc/RecordUsageStat":
					return okEnvelope(map[string]any{"recorded": true}), nil
				default:
					return nil, errors.New("unexpected access path: " + path)
				}
			},
		},
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				switch path {
				case "/rpc/ResolvePolicy":
					return okEnvelope(map[string]any{
						"policy_key":       "users_by_ids_v1",
						"upstream_method":  "GET",
						"upstream_path":    "/base/apitools/usersByIdRestIds",
						"allowed_params":   []string{"userIds"},
						"denied_params":    []string{"proxyUrl"},
						"default_params":   map[string]any{"resFormat": "json"},
						"provider_name":    "fapi.uk",
						"provider_api_key": "provider_key_1",
					}), nil
				case "/rpc/CheckAppPolicyAccess":
					return okEnvelope(map[string]any{"allowed": true}), nil
				default:
					return nil, errors.New("unexpected policy path: " + path)
				}
			},
		},
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path != "/rpc/ExecutePolicy" {
					return nil, errors.New("unexpected provider path: " + path)
				}
				return okEnvelope(map[string]any{
					"status_code":          200,
					"result_code":          "UPSTREAM_OK",
					"body":                 map[string]any{"users": []any{}},
					"upstream_duration_ms": 1,
				}), nil
			},
		},
		"dev",
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?userIds=1,2&appKey=app_key_1", nil)
	req.Header.Set("AppSecret", " secret_1 ")
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayRejectsDeniedParam(t *testing.T) {
	var released bool

	svc := newGatewayServiceWithClients(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				switch path {
				case "/rpc/CheckIpBan":
					return okEnvelope(map[string]any{"blocked": false}), nil
				case "/rpc/GetAppAuthContext":
					return okEnvelope(map[string]any{
						"tenant_id":  "tenant_1",
						"app_id":     "app_1",
						"app_key":    "app_key_1",
						"app_secret": "secret_1",
						"status":     "active",
					}), nil
				case "/rpc/CheckReplay":
					return okEnvelope(map[string]any{"accepted": true}), nil
				case "/rpc/CheckAndReserveQuota":
					return okEnvelope(map[string]any{"allowed": true}), nil
				case "/rpc/ReleaseQuotaOnFailure":
					released = true
					return okEnvelope(map[string]any{"released": true}), nil
				default:
					return nil, errors.New("unexpected access path: " + path)
				}
			},
		},
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				switch path {
				case "/rpc/ResolvePolicy":
					return okEnvelope(map[string]any{
						"policy_key":      "users_by_ids_v1",
						"upstream_method": "GET",
						"upstream_path":   "/base/apitools/usersByIdRestIds",
						"allowed_params":  []string{"userIds"},
						"denied_params":   []string{"proxyUrl"},
						"default_params":  map[string]any{"resFormat": "json"},
					}), nil
				case "/rpc/CheckAppPolicyAccess":
					return okEnvelope(map[string]any{"allowed": true}), nil
				default:
					return nil, errors.New("unexpected policy path")
				}
			},
		},
		stubJSONClient{},
	)

	query := url.Values{}
	query.Set("userIds", "1,2")
	query.Set("proxyUrl", "http://evil")
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	query.Set("timestamp", timestamp)
	query.Set("nonce", "nonce-1")
	query.Set("appKey", "app_key_1")
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/users/by-ids", query, timestamp, "nonce-1")
	query.Set("signature", signature)

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	svc.handleProxy(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !released {
		t.Fatal("expected quota release on denied param path")
	}
}

func TestGatewayRejectsBlockedIP(t *testing.T) {
	svc := newGatewayServiceWithClients(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path != "/rpc/CheckIpBan" {
					return nil, errors.New("unexpected access path: " + path)
				}
				request := payload.(*accessservice.CheckIpBanRequest)
				if request.Ip != "203.0.113.10" {
					t.Fatalf("unexpected ip payload: %#v", request)
				}
				return okEnvelope(map[string]any{"blocked": true}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	query := url.Values{}
	query.Set("userIds", "1,2")
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	query.Set("timestamp", timestamp)
	query.Set("nonce", "nonce-1")
	query.Set("appKey", "app_key_1")
	query.Set("signature", "ignored-before-auth")

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayLogsStructuredFailure(t *testing.T) {
	var logs bytes.Buffer

	svc := newGatewayServiceWithClients(
		xlog.NewWithWriter("gateway-test", &logs),
		stubJSONClient{
			postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
				if path != "/rpc/CheckIpBan" {
					return nil, errors.New("unexpected access path: " + path)
				}
				return okEnvelope(map[string]any{"blocked": true}), nil
			},
		},
		stubJSONClient{},
		stubJSONClient{},
	)

	query := url.Values{}
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	query.Set("timestamp", timestamp)
	query.Set("nonce", "nonce-log-1")
	query.Set("appKey", "app_key_1")
	query.Set("signature", "ignored-before-auth")

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}

	entry := decodeSingleLogLine(t, logs.Bytes())
	if entry["service"] != "gateway-test" {
		t.Fatalf("unexpected service in log: %#v", entry)
	}
	if entry["request_id"] == "" || entry["created_at"] == "" {
		t.Fatalf("expected request_id and created_at in log, got %#v", entry)
	}
	if entry["status_code"] != float64(http.StatusForbidden) {
		t.Fatalf("unexpected status_code in log: %#v", entry)
	}
	if entry["result_code"] != "CLIENT_IP_BLOCKED" {
		t.Fatalf("unexpected result_code in log: %#v", entry)
	}
	if entry["client_ip"] != "203.0.113.10" {
		t.Fatalf("unexpected client_ip in log: %#v", entry)
	}
	if entry["error_summary"] != "client ip is blocked" {
		t.Fatalf("unexpected error_summary in log: %#v", entry)
	}
}

func TestSanitizeUpstreamQueryRejectsSensitiveParamsCaseInsensitive(t *testing.T) {
	query := url.Values{}
	query.Set("ProxyUrl", "http://evil")

	if _, err := sanitizeUpstreamQuery(query, nil, []string{"proxyUrl", "auth_token"}, nil); err == nil {
		t.Fatal("expected sensitive param variant to be rejected")
	}

	query = url.Values{}
	query.Set("AUTH_TOKEN", "secret")
	if _, err := sanitizeUpstreamQuery(query, nil, []string{"proxyUrl", "auth_token"}, nil); err == nil {
		t.Fatal("expected auth_token variant to be rejected")
	}
}

func TestSanitizeUpstreamQueryAllowsUnknownParamsWhenAllowlistEmpty(t *testing.T) {
	query := url.Values{}
	query.Set("cursor", "next-token")

	result, err := sanitizeUpstreamQuery(query, nil, []string{"proxyUrl", "auth_token"}, nil)
	if err != nil {
		t.Fatalf("sanitizeUpstreamQuery returned error: %v", err)
	}
	if result["cursor"] != "next-token" {
		t.Fatalf("expected unknown param to pass through when allowlist empty, got %#v", result)
	}
}

func TestSanitizeUpstreamQueryCanonicalizesAllowedParamMatches(t *testing.T) {
	query := url.Values{}
	query.Set("USERIDS", "1,2")

	result, err := sanitizeUpstreamQuery(query, []string{"userIds"}, nil, nil)
	if err != nil {
		t.Fatalf("sanitizeUpstreamQuery returned error: %v", err)
	}
	if result["userIds"] != "1,2" {
		t.Fatalf("expected allowlisted key to use policy casing, got %#v", result)
	}
	if _, exists := result["USERIDS"]; exists {
		t.Fatalf("expected raw caller casing to be normalized, got %#v", result)
	}
}

func TestSanitizeUpstreamQueryRejectsNormalizedDuplicates(t *testing.T) {
	query := url.Values{
		"userIds": {"1,2"},
		"USERIDS": {"3,4"},
	}

	_, err := sanitizeUpstreamQuery(query, []string{"userIds"}, nil, nil)
	if err == nil {
		t.Fatal("expected normalized duplicate params to be rejected")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error, got %v", err)
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

func decodeSingleLogLine(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(payload), &entry); err != nil {
		t.Fatalf("decode log payload: %v; raw=%s", err, string(payload))
	}
	return entry
}
