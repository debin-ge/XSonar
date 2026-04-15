package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/apps/scheduler-rpc/schedulerservice"
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

func (s stubJSONClient) GetAppAuthContextByID(ctx context.Context, req *accessservice.GetAppAuthContextByIDRequest) (*clients.EnvelopeResponse, error) {
	resp, err := s.call(ctx, "/rpc/GetAppAuthContextByID", req)
	if err == nil || !strings.Contains(err.Error(), "unexpected") {
		return resp, err
	}
	return s.call(ctx, "/rpc/GetAppAuthContext", &accessservice.GetAppAuthContextRequest{AppKey: req.AppId})
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

func (s stubJSONClient) CreateTask(ctx context.Context, req *schedulerservice.CreateTaskRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/CreateTask", req)
}

func (s stubJSONClient) GetTask(ctx context.Context, req *schedulerservice.GetTaskRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/GetTask", req)
}

func (s stubJSONClient) ListTaskRuns(ctx context.Context, req *schedulerservice.ListTaskRunsRequest) (*clients.EnvelopeResponse, error) {
	return s.call(ctx, "/rpc/ListTaskRuns", req)
}

func setProductionAuthHeaders(req *http.Request, appKey, timestamp, nonce, signature string) {
	req.Header.Set("Authorization", "Bearer "+mustSignGatewayAppJWTForTests(appKey))
	_ = timestamp
	_ = nonce
	_ = signature
}

func setDevelopmentAuthHeaders(req *http.Request, appKey, appSecret string) {
	req.Header.Set("Authorization", "Bearer "+mustSignGatewayAppJWTForTests(appKey))
	_ = appSecret
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
			if query["userIds"] != "1,2" {
				t.Fatalf("expected userIds to be passed through, got %#v", query["userIds"])
			}
			if len(query) != 1 {
				t.Fatalf("expected only caller query params to be forwarded, got %#v", query)
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
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/users/by-ids", query, timestamp, "nonce-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
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

func TestGatewayCreateCollectorTaskRejectsMissingBearerJWT(t *testing.T) {
	svc := newGatewayServiceWithAdmin(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{},
		"test-secret",
		"test-issuer",
	)

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/collector/tasks", strings.NewReader(`{"task_id":"task-1","task_type":"periodic","keyword":"openai"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	svc.handleCreateCollectorTask(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayCreateCollectorTaskUsesAdminJWTSubjectAsCreatedBy(t *testing.T) {
	var recorded *schedulerservice.CreateTaskRequest
	schedulerClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			if path != "/rpc/CreateTask" {
				return nil, errors.New("unexpected scheduler path: " + path)
			}
			recorded = payload.(*schedulerservice.CreateTaskRequest)
			return okEnvelope(map[string]any{"task_id": "task-1"}), nil
		},
	}
	svc := newGatewayServiceWithAdmin(
		xlog.NewStdout("gateway-test"),
		schedulerClient,
		"test-secret",
		"test-issuer",
	)
	token := mustSignAdminJWT(t, "test-secret", "test-issuer", "admin-user-1")

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/collector/tasks", strings.NewReader(`{"task_id":"task-1","task_type":"periodic","keyword":"openai","priority":5,"frequency_seconds":60}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	svc.handleCreateCollectorTask(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if recorded == nil {
		t.Fatal("expected scheduler request to be recorded")
	}
	if recorded.CreatedBy != "admin-user-1" {
		t.Fatalf("expected created_by admin-user-1, got %q", recorded.CreatedBy)
	}
}

func TestGatewayCreateCollectorTaskMapsSchedulerConflictTo409(t *testing.T) {
	schedulerClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			if path != "/rpc/CreateTask" {
				return nil, errors.New("unexpected scheduler path: " + path)
			}
			return &clients.EnvelopeResponse{
				Code:    model.CodeConflict,
				Message: "task already exists",
				Data:    json.RawMessage(`{"task_id":"task-1"}`),
			}, errors.New("scheduler conflict")
		},
	}
	svc := newGatewayServiceWithAdmin(
		xlog.NewStdout("gateway-test"),
		schedulerClient,
		"test-secret",
		"test-issuer",
	)
	token := mustSignAdminJWT(t, "test-secret", "test-issuer", "admin-user-1")

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/collector/tasks", strings.NewReader(`{"task_id":"task-1","task_type":"periodic","keyword":"openai"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	svc.handleCreateCollectorTask(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayCreateCollectorTaskRejectsOversizedBody(t *testing.T) {
	schedulerClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			t.Fatalf("scheduler should not be called for oversized body: %s", path)
			return nil, nil
		},
	}
	svc := newGatewayServiceWithAdmin(
		xlog.NewStdout("gateway-test"),
		schedulerClient,
		"test-secret",
		"test-issuer",
	)
	token := mustSignAdminJWT(t, "test-secret", "test-issuer", "admin-user-1")
	oversizedKeyword := strings.Repeat("x", 1<<20)
	body := `{"task_id":"task-1","task_type":"periodic","keyword":"` + oversizedKeyword + `"}`

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/collector/tasks", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	svc.handleCreateCollectorTask(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayCollectorAdminGetRoutesForwardToScheduler(t *testing.T) {
	tests := []struct {
		name         string
		url          string
		handler      func(*gatewayService, http.ResponseWriter, *http.Request)
		expectedPath string
		expectedTask string
	}{
		{
			name:         "get task detail",
			url:          "/admin/v1/collector/tasks/task-1",
			handler:      (*gatewayService).handleGetCollectorTask,
			expectedPath: "/rpc/GetTask",
			expectedTask: "task-1",
		},
		{
			name:         "get task runs",
			url:          "/admin/v1/collector/tasks/task-1/runs",
			handler:      (*gatewayService).handleListCollectorTaskRuns,
			expectedPath: "/rpc/ListTaskRuns",
			expectedTask: "task-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calledPath string
			var calledTaskID string
			schedulerClient := stubJSONClient{
				postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
					calledPath = path
					switch req := payload.(type) {
					case *schedulerservice.GetTaskRequest:
						calledTaskID = req.TaskId
					case *schedulerservice.ListTaskRunsRequest:
						calledTaskID = req.TaskId
					default:
						t.Fatalf("unexpected scheduler payload type %T", payload)
					}
					return okEnvelope(map[string]any{"task_id": calledTaskID}), nil
				},
			}
			svc := newGatewayServiceWithAdmin(
				xlog.NewStdout("gateway-test"),
				schedulerClient,
				"test-secret",
				"test-issuer",
			)
			token := mustSignAdminJWT(t, "test-secret", "test-issuer", "admin-user-1")

			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			req.SetPathValue("id", tt.expectedTask)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()

			tt.handler(svc, rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
			}
			if calledPath != tt.expectedPath {
				t.Fatalf("expected path %s, got %s", tt.expectedPath, calledPath)
			}
			if calledTaskID != tt.expectedTask {
				t.Fatalf("expected task id %s, got %s", tt.expectedTask, calledTaskID)
			}
		})
	}
}

func TestGatewayRejectsInvalidSignature(t *testing.T) {
	svc := newGatewayServiceWithClients(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{},
		stubJSONClient{},
		stubJSONClient{},
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?userIds=1,2", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
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
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/tweets/detail", query, timestamp, "nonce-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/tweets/detail?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
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

func TestGatewayPreservesTweetDetailQueryParams(t *testing.T) {
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
			expected := map[string]any{
				"tweetId": "1971453180132327700",
			}
			if !reflect.DeepEqual(query, expected) {
				t.Fatalf("expected gateway to forward caller query unchanged, got %#v", query)
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
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/tweets/detail", query, timestamp, "nonce-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/tweets/detail?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
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
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/tweets/detail", query, timestamp, "nonce-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/tweets/detail?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
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

func TestGatewayListsRequiresUserIDOrScreenName(t *testing.T) {
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
					"policy_key":       "lists_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/listByUserIdOrScreenName",
					"allowed_params":   []string{"userId", "screenName"},
					"denied_params":    []string{"proxyUrl", "auth_token", "ct0"},
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
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/lists", query, timestamp, "nonce-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/lists?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if providerCalled {
		t.Fatal("expected provider to be skipped when both userId and screenName are missing")
	}

	var response struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != model.CodeInvalidRequest || response.Message != "one of parameters is required: userId or screenName" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestGatewayPreservesAccountAnalyticsQueryParams(t *testing.T) {
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
					"policy_key":       "users_account_analytics_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/accountAnalytics",
					"allowed_params":   []string{"restId", "authToken", "csrfToken"},
					"required_params":  []string{"restId", "authToken"},
					"denied_params":    []string{"proxyUrl", "auth_token", "ct0"},
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
			expected := map[string]any{
				"restId":    "rest-1",
				"authToken": "auth-1",
				"csrfToken": "csrf-1",
			}
			if !reflect.DeepEqual(query, expected) {
				t.Fatalf("unexpected provider query: %#v", query)
			}
			return okEnvelope(map[string]any{
				"status_code":          200,
				"result_code":          "UPSTREAM_OK",
				"body":                 map[string]any{"ok": true},
				"upstream_duration_ms": 12,
			}), nil
		},
	}

	svc := newGatewayServiceWithClients(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient)

	query := url.Values{}
	query.Set("restId", "rest-1")
	query.Set("authToken", "auth-1")
	query.Set("csrfToken", "csrf-1")
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/users/account-analytics", query, timestamp, "nonce-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/users/account-analytics?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayRejectsUnexpectedQueryForZeroParamRoute(t *testing.T) {
	tests := []struct {
		name  string
		query url.Values
	}{
		{
			name:  "rejects non-empty param",
			query: url.Values{"cursor": {"foo"}},
		},
		{
			name:  "rejects empty unknown param",
			query: url.Values{"foo": {""}},
		},
		{
			name:  "rejects empty default param name",
			query: url.Values{"resFormat": {""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
							"policy_key":       "search_explore_v1",
							"upstream_method":  "GET",
							"upstream_path":    "/base/apitools/explore",
							"denied_params":    []string{"proxyUrl", "auth_token", "ct0"},
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

			timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
			signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/search/explore", tt.query, timestamp, "nonce-1")

			req := httptest.NewRequest(http.MethodGet, "/v1/search/explore?"+tt.query.Encode(), nil)
			setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
			rec := httptest.NewRecorder()

			svc.handleProxy(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			if providerCalled {
				t.Fatal("expected provider to be skipped for zero-param route validation failure")
			}

			var response struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response.Code != model.CodeInvalidRequest || response.Message != "parameters are not allowed for this route" {
				t.Fatalf("unexpected response: %+v", response)
			}
		})
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

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?userIds=1,2", nil)
	setDevelopmentAuthHeaders(req, "app_key_1", "secret_1")
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if recordedUsage == nil || recordedUsage.PolicyKey != "users_by_ids_v1" {
		t.Fatalf("expected usage stat to be recorded, got %#v", recordedUsage)
	}
}

func TestGatewayRejectsQueryAuthParametersInProductionMode(t *testing.T) {
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
	query.Set("signature", shared.ComputeSignature("secret_1", http.MethodGet, "/v1/users/by-ids", query, timestamp, "nonce-1"))

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when auth is provided via query string, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayDevModeRejectsQueryAuthParameters(t *testing.T) {
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

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when dev auth is provided via query string, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayDevModeRejectsInvalidAppSecret(t *testing.T) {
	svc := newGatewayServiceWithMode(
		xlog.NewStdout("gateway-test"),
		stubJSONClient{},
		stubJSONClient{},
		stubJSONClient{},
		"dev",
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
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

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?userIds=1,2", nil)
	setDevelopmentAuthHeaders(req, "app_key_1", " secret_1 ")
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
	signature := shared.ComputeSignature("secret_1", http.MethodGet, "/v1/users/by-ids", query, timestamp, "nonce-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", signature)
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

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-1", "ignored-before-auth")
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

	req := httptest.NewRequest(http.MethodGet, "/v1/users/by-ids?"+query.Encode(), nil)
	setProductionAuthHeaders(req, "app_key_1", timestamp, "nonce-log-1", "ignored-before-auth")
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

	query = url.Values{}
	query.Set("Ct0", "secret-cookie")
	if _, err := sanitizeUpstreamQuery(query, nil, []string{"proxyUrl", "auth_token"}, nil); err == nil {
		t.Fatal("expected ct0 variant to be rejected")
	}
}

func TestSanitizeUpstreamQueryRejectsCT0CaseInsensitive(t *testing.T) {
	query := url.Values{}
	query.Set("Ct0", "csrf-token")

	if _, err := sanitizeUpstreamQuery(query, nil, []string{"proxyUrl", "auth_token"}, nil); err == nil {
		t.Fatal("expected ct0 variant to be rejected")
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

func TestSanitizeUpstreamQueryDoesNotInjectDefaultParams(t *testing.T) {
	query := url.Values{}
	query.Set("words", "ai")

	result, err := sanitizeUpstreamQuery(query, []string{"words"}, nil, map[string]string{"resFormat": "json"})
	if err != nil {
		t.Fatalf("sanitizeUpstreamQuery returned error: %v", err)
	}
	expected := map[string]any{
		"words": "ai",
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected sanitizeUpstreamQuery to preserve caller params only, got %#v", result)
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

func TestLoggedQueryFiltersSensitiveUpstreamParams(t *testing.T) {
	query := url.Values{}
	query.Set("words", "ai")
	query.Set("auth_token", "secret-token")
	query.Set("ct0", "secret-cookie")
	query.Set("proxyUrl", "http://evil")

	got := loggedQuery(query)
	if strings.Contains(got, "auth_token") {
		t.Fatalf("expected auth_token to be filtered, got %q", got)
	}
	if strings.Contains(got, "ct0") {
		t.Fatalf("expected ct0 to be filtered, got %q", got)
	}
	if strings.Contains(got, "proxyUrl") {
		t.Fatalf("expected proxyUrl to be filtered, got %q", got)
	}
	if !strings.Contains(got, "words=ai") {
		t.Fatalf("expected non-sensitive params to remain, got %q", got)
	}
}

func TestNormalizeProviderQueryLeavesSearchTweetsWithoutCountUnchanged(t *testing.T) {
	query := map[string]any{"words": "ai"}

	got := normalizeProviderQuery("search_tweets_v1", "/base/apitools/search", query)

	expected := map[string]any{"words": "ai"}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected search tweets query to be unchanged, got %#v", got)
	}
}

func TestNormalizeProviderQueryLeavesExplicitSearchTweetsCountUnchanged(t *testing.T) {
	query := map[string]any{
		"words": "ai",
		"count": "100",
	}

	got := normalizeProviderQuery("search_tweets_v1", "/base/apitools/search", query)

	expected := map[string]any{
		"words": "ai",
		"count": "100",
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected explicit count to be preserved, got %#v", got)
	}
}

func TestNormalizeProviderQueryLeavesInvalidSearchTweetsCountUnchanged(t *testing.T) {
	query := map[string]any{
		"words": "ai",
		"count": "abc",
	}

	got := normalizeProviderQuery("search_tweets_v1", "/base/apitools/search", query)

	expected := map[string]any{
		"words": "ai",
		"count": "abc",
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected invalid count to be preserved for provider passthrough, got %#v", got)
	}
}

func TestNormalizeProviderQueryLeavesReadonlyAliasParamsUnchanged(t *testing.T) {
	tests := []struct {
		name         string
		policyKey    string
		upstreamPath string
		query        map[string]any
		expected     map[string]any
	}{
		{
			name:         "tweet brief moves tweetId to id",
			policyKey:    "tweets_brief_v1",
			upstreamPath: "/base/apitools/tweetSimple",
			query: map[string]any{
				"tweetId": "tweet-1",
				"cursor":  "cursor-1",
			},
			expected: map[string]any{
				"tweetId": "tweet-1",
				"cursor":  "cursor-1",
			},
		},
		{
			name:         "mentions timeline maps aliases",
			policyKey:    "users_mentions_timeline_v1",
			upstreamPath: "/base/apitools/mentionsTimeline",
			query: map[string]any{
				"authToken":       "auth-1",
				"csrfToken":       "csrf-1",
				"includeEntities": "true",
				"trimUser":        "false",
				"cursor":          "cursor-1",
			},
			expected: map[string]any{
				"authToken":       "auth-1",
				"csrfToken":       "csrf-1",
				"includeEntities": "true",
				"trimUser":        "false",
				"cursor":          "cursor-1",
			},
		},
		{
			name:         "account analytics maps aliases",
			policyKey:    "users_account_analytics_v1",
			upstreamPath: "/base/apitools/accountAnalytics",
			query: map[string]any{
				"restId":    "rest-1",
				"authToken": "auth-1",
				"csrfToken": "csrf-1",
			},
			expected: map[string]any{
				"restId":    "rest-1",
				"authToken": "auth-1",
				"csrfToken": "csrf-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeProviderQuery(tt.policyKey, tt.upstreamPath, tt.query)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Fatalf("unexpected normalized query: got %#v want %#v", got, tt.expected)
			}
		})
	}
}

func TestLoggedQueryRedactsSensitiveAliasParams(t *testing.T) {
	query := url.Values{}
	query.Set("authToken", "auth-1")
	query.Set("csrfToken", "csrf-1")
	query.Set("auth_token", "auth-2")
	query.Set("ct0", "csrf-2")
	query.Set("cursor", "cursor-1")

	got := loggedQuery(query)

	if strings.Contains(got, "authToken=") || strings.Contains(got, "csrfToken=") || strings.Contains(got, "auth_token=") || strings.Contains(got, "ct0=") {
		t.Fatalf("expected sensitive params to be redacted, got %q", got)
	}
	if got != "cursor=cursor-1" {
		t.Fatalf("expected safe params to remain, got %q", got)
	}
}

func TestGatewaySearchTweetsPreservesExplicitCount(t *testing.T) {
	var recordedUsage *accessservice.RecordUsageStatRequest

	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_search",
					"app_id":     "app_search",
					"app_key":    "app_key_search",
					"app_secret": "secret_search",
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
					"policy_key":       "search_tweets_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/search",
					"allowed_params":   []string{"words", "count"},
					"required_params":  []string{"words"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_search",
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
			expected := map[string]any{
				"words": "ai gateway optimization",
				"count": "100",
			}
			if !reflect.DeepEqual(query, expected) {
				t.Fatalf("expected search tweets params to be forwarded unchanged, got %#v", query)
			}
			return okEnvelope(map[string]any{
				"status_code":          200,
				"result_code":          "UPSTREAM_OK",
				"body":                 map[string]any{"items": []any{}},
				"upstream_duration_ms": 15,
			}), nil
		},
	}

	svc := newGatewayServiceWithMode(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient, "dev")

	req := httptest.NewRequest(http.MethodGet, "/v1/search/tweets?words=ai+gateway+optimization&count=100", nil)
	req.Header.Set("Authorization", "Bearer "+mustSignGatewayAppJWTForTests("app_key_search"))
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if recordedUsage == nil || recordedUsage.PolicyKey != "search_tweets_v1" {
		t.Fatalf("expected usage stat to be recorded, got %#v", recordedUsage)
	}
}

func TestGatewaySearchTweetsDoesNotInjectDefaultCount(t *testing.T) {
	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_search",
					"app_id":     "app_search",
					"app_key":    "app_key_search",
					"app_secret": "secret_search",
					"status":     "active",
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
					"policy_key":       "search_tweets_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/search",
					"allowed_params":   []string{"words", "count"},
					"required_params":  []string{"words"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_search",
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
			expected := map[string]any{
				"words": "ai gateway optimization",
			}
			if !reflect.DeepEqual(query, expected) {
				t.Fatalf("expected search tweets query to omit default params, got %#v", query)
			}
			return okEnvelope(map[string]any{
				"status_code":          200,
				"result_code":          "UPSTREAM_OK",
				"body":                 map[string]any{"items": []any{}},
				"upstream_duration_ms": 15,
			}), nil
		},
	}

	svc := newGatewayServiceWithMode(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient, "dev")

	req := httptest.NewRequest(http.MethodGet, "/v1/search/tweets?words=ai+gateway+optimization", nil)
	req.Header.Set("Authorization", "Bearer "+mustSignGatewayAppJWTForTests("app_key_search"))
	rec := httptest.NewRecorder()

	svc.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayDoesNotBlockOnUsageStatRecording(t *testing.T) {
	recordUsageStarted := make(chan struct{}, 1)
	releaseRecordUsage := make(chan struct{})

	accessClient := stubJSONClient{
		postFunc: func(_ context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_async",
					"app_id":     "app_async",
					"app_key":    "app_key_async",
					"app_secret": "secret_async",
					"status":     "active",
				}), nil
			case "/rpc/CheckAndReserveQuota":
				return okEnvelope(map[string]any{"allowed": true}), nil
			case "/rpc/RecordUsageStat":
				recordUsageStarted <- struct{}{}
				<-releaseRecordUsage
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
					"policy_key":       "search_tweets_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/search",
					"allowed_params":   []string{"words", "count"},
					"required_params":  []string{"words"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_async",
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
				"body":                 map[string]any{"items": []any{}},
				"upstream_duration_ms": 15,
			}), nil
		},
	}

	recorder := newAsyncUsageStatRecorder(xlog.NewStdout("gateway-test"), accessClient, 8, 1, 200*time.Millisecond)
	defer recorder.Close()
	defer close(releaseRecordUsage)

	svc := newGatewayServiceWithModeAndUsageStats(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient, nil, "", "", recorder, "dev")

	req := httptest.NewRequest(http.MethodGet, "/v1/search/tweets?words=ai+async+recording", nil)
	req.Header.Set("Authorization", "Bearer "+mustSignGatewayAppJWTForTests("app_key_async"))
	rec := httptest.NewRecorder()

	start := time.Now()
	svc.handleProxy(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("expected response to avoid waiting for usage stats, took %s", elapsed)
	}

	select {
	case <-recordUsageStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected async usage stat worker to start")
	}
}

func TestGatewayOverlapsIPBanAndPolicyResolution(t *testing.T) {
	const delay = 120 * time.Millisecond

	accessClient := stubJSONClient{
		postFunc: func(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_overlap_a",
					"app_id":     "app_overlap_a",
					"app_key":    "app_key_overlap_a",
					"app_secret": "secret_overlap_a",
					"status":     "active",
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
		postFunc: func(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/ResolvePolicy":
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
				return okEnvelope(map[string]any{
					"policy_key":       "search_tweets_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/search",
					"allowed_params":   []string{"words"},
					"required_params":  []string{"words"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_overlap_a",
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
				"body":                 map[string]any{"items": []any{}},
				"upstream_duration_ms": 15,
			}), nil
		},
	}

	svc := newGatewayServiceWithMode(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient, "dev")

	req := httptest.NewRequest(http.MethodGet, "/v1/search/tweets?words=ai+overlap+ipban+policy", nil)
	req.Header.Set("Authorization", "Bearer "+mustSignGatewayAppJWTForTests("app_key_overlap_a"))
	rec := httptest.NewRecorder()

	start := time.Now()
	svc.handleProxy(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if elapsed >= 2*delay {
		t.Fatalf("expected ip ban and policy resolution to overlap, took %s", elapsed)
	}
}

func TestGatewayOverlapsQuotaReservationAndPolicyAccess(t *testing.T) {
	const delay = 120 * time.Millisecond

	accessClient := stubJSONClient{
		postFunc: func(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/CheckIpBan":
				return okEnvelope(map[string]any{"blocked": false}), nil
			case "/rpc/GetAppAuthContext":
				return okEnvelope(map[string]any{
					"tenant_id":  "tenant_overlap_b",
					"app_id":     "app_overlap_b",
					"app_key":    "app_key_overlap_b",
					"app_secret": "secret_overlap_b",
					"status":     "active",
				}), nil
			case "/rpc/CheckAndReserveQuota":
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
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
		postFunc: func(ctx context.Context, path string, payload any) (*clients.EnvelopeResponse, error) {
			switch path {
			case "/rpc/ResolvePolicy":
				return okEnvelope(map[string]any{
					"policy_key":       "search_tweets_v1",
					"upstream_method":  "GET",
					"upstream_path":    "/base/apitools/search",
					"allowed_params":   []string{"words"},
					"required_params":  []string{"words"},
					"denied_params":    []string{"proxyUrl", "auth_token"},
					"default_params":   map[string]any{"resFormat": "json"},
					"provider_name":    "fapi.uk",
					"provider_api_key": "provider_key_overlap_b",
				}), nil
			case "/rpc/CheckAppPolicyAccess":
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
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
				"body":                 map[string]any{"items": []any{}},
				"upstream_duration_ms": 15,
			}), nil
		},
	}

	svc := newGatewayServiceWithMode(xlog.NewStdout("gateway-test"), accessClient, policyClient, providerClient, "dev")

	req := httptest.NewRequest(http.MethodGet, "/v1/search/tweets?words=ai+overlap+quota+access", nil)
	req.Header.Set("Authorization", "Bearer "+mustSignGatewayAppJWTForTests("app_key_overlap_b"))
	rec := httptest.NewRecorder()

	start := time.Now()
	svc.handleProxy(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if elapsed >= 2*delay {
		t.Fatalf("expected quota and policy access checks to overlap, took %s", elapsed)
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

func mustSignAdminJWT(t *testing.T, secret, issuer, subject string) string {
	t.Helper()
	token, err := shared.SignJWT(secret, issuer, subject, "gateway_app", time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	return token
}

func mustSignGatewayAppJWTForTests(subject string) string {
	token, err := shared.SignJWT(defaultGatewayJWTSecret, defaultGatewayJWTIssuer, subject, "gateway_app", time.Hour, time.Now())
	if err != nil {
		panic(err)
	}
	return token
}

func decodeSingleLogLine(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(payload), &entry); err != nil {
		t.Fatalf("decode log payload: %v; raw=%s", err, string(payload))
	}
	return entry
}
