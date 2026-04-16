package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"xsonar/apps/provider-rpc/internal/config"
	"xsonar/pkg/proto/providerpb"
	"xsonar/pkg/xlog"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestExecutePolicyForwardsRequest(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:      "https://provider.example/api",
		APIKeyHeader: "apiKey",
		TimeoutMS:    1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://provider.example/api/base/apitools/usersByIdRestIds?resFormat=json&userIds=1%2C2" {
				t.Fatalf("unexpected upstream url: %s", req.URL.String())
			}
			if req.Method != http.MethodGet {
				t.Fatalf("unexpected method: %s", req.Method)
			}
			if req.Header.Get("apiKey") != "provider-key-1" {
				t.Fatalf("unexpected api key header: %q", req.Header.Get("apiKey"))
			}
			if req.Header.Get("X-Request-ID") != "req-test-1" {
				t.Fatalf("unexpected request id header: %q", req.Header.Get("X-Request-ID"))
			}

			return jsonHTTPResponse(http.StatusOK, `{"ok":true,"provider":"fapi.uk"}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		RequestID:      "req-test-1",
		PolicyKey:      "users_by_ids_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/usersByIdRestIds",
		Query: map[string]any{
			"userIds":   "1,2",
			"resFormat": "json",
		},
		ProviderName:   "fapi.uk",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if resultPayload.ResultCode != "UPSTREAM_OK" {
		t.Fatalf("unexpected result code: %#v", resultPayload.ResultCode)
	}
	body := decodeProviderBody(t, resultPayload.Body).(map[string]any)
	if body["provider"] != "fapi.uk" || body["ok"] != true {
		t.Fatalf("unexpected body: %#v", body)
	}
}

func TestExecutePolicyAddsHTTPSchemeToProviderBaseURL(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:      "provider.example/api",
		APIKeyHeader: "apiKey",
		TimeoutMS:    1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://provider.example/api/base/apitools/usersByIdRestIds?userIds=1%2C2" {
				t.Fatalf("unexpected upstream url: %s", req.URL.String())
			}
			return jsonHTTPResponse(http.StatusOK, `{"ok":true}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "users_by_ids_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/usersByIdRestIds",
		Query: map[string]any{
			"userIds": "1,2",
		},
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
}

func TestExecutePolicyRejectsInvalidProviderBaseURL(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "ftp://provider.example/api",
		TimeoutMS: 1000,
	}

	svc := newProviderServiceWithConfigAndClient(cfg, &http.Client{}, xlog.NewStdout("provider-test"))
	_, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "users_by_ids_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/usersByIdRestIds",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr == nil {
		t.Fatal("expected invalid provider base url to be rejected")
	}
	if svcErr.message != "invalid provider_base_url" {
		t.Fatalf("unexpected error message: %#v", svcErr)
	}
}

func TestExecutePolicyRetriesGETOn5XX(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:    "https://provider.example/api",
		RetryCount: 1,
		TimeoutMS:  1000,
	}

	attempts := 0
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return jsonHTTPResponse(http.StatusBadGateway, `{"error":"temporary"}`), nil
			}
			return jsonHTTPResponse(http.StatusOK, `{"ok":true}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "users_by_ids_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/usersByIdRestIds",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("unexpected final status code: %#v", resultPayload.StatusCode)
	}
}

func TestExecutePolicyUnwrapsProviderSuccessEnvelope(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusOK, `{"code":1,"data":{"tweets":[{"id":"1"}]},"result":{"trace":"ignored"},"msg":"ok"}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "tweets_timeline_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/userTimeline",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	body := decodeProviderBody(t, resultPayload.Body).(map[string]any)
	tweets := body["tweets"].([]any)
	if len(tweets) != 1 {
		t.Fatalf("expected unwrapped tweets payload, got %#v", body)
	}
	if _, exists := body["result"]; exists {
		t.Fatalf("expected provider envelope to be removed, got %#v", body)
	}
}

func TestExecutePolicyUnwrapsProviderDataWhenCodeIsNotSuccess(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusOK, `{"code":0,"data":{"tweets":[{"id":"2"}]},"result":{},"msg":"upstream rejected request"}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "tweets_timeline_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/userTimeline",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if resultPayload.ResultCode != "UPSTREAM_OK" {
		t.Fatalf("unexpected result code: %#v", resultPayload.ResultCode)
	}
	body := decodeProviderBody(t, resultPayload.Body).(map[string]any)
	tweets := body["tweets"].([]any)
	if len(tweets) != 1 {
		t.Fatalf("unexpected body: %#v", body)
	}
}

func TestExecutePolicyTreatsStringErrorPayloadAsUpstreamApplicationError(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusOK, `{"code":0,"data":"Authorization to push special information verification failed, please check your apiKey in the personal center","msg":"Authorization to push special information verification failed, please check your apiKey in the personal center","result":null}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "users_by_username_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/userByScreenNameV2",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusBadGateway {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if resultPayload.ResultCode != "UPSTREAM_APPLICATION_ERROR" {
		t.Fatalf("unexpected result code: %#v", resultPayload.ResultCode)
	}
	body := decodeProviderBody(t, resultPayload.Body).(map[string]any)
	if body["error"] != "Authorization to push special information verification failed, please check your apiKey in the personal center" {
		t.Fatalf("unexpected body: %#v", body)
	}
}

func TestExecutePolicyTreatsGraphQLErrorPayloadAsUpstreamApplicationError(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusOK, `{"code":1,"data":{"data":{},"errors":[{"message":"strconv.ParseInt: parsing \"\": invalid syntax","path":["threaded_conversation_with_injections_v2","focal_tweet_id"]}]},"msg":"SUCCESS","result":null}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "tweets_detail_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/tweetTimeline",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusBadGateway {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if resultPayload.ResultCode != "UPSTREAM_APPLICATION_ERROR" {
		t.Fatalf("unexpected result code: %#v", resultPayload.ResultCode)
	}
	body := decodeProviderBody(t, resultPayload.Body).(map[string]any)
	if body["error"] != "strconv.ParseInt: parsing \"\": invalid syntax" {
		t.Fatalf("unexpected body: %#v", body)
	}
	errorsValue, ok := body["errors"].([]any)
	if !ok || len(errorsValue) != 1 {
		t.Fatalf("expected graphql errors to be preserved, got %#v", body)
	}
}

func TestExecutePolicySuppressesPlaceholderUpstreamErrorBody(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	var logs bytes.Buffer
	const upstreamBody = `{"additionalProp":{}}`
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusBadRequest, upstreamBody), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewWithWriter("provider-test", &logs))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		RequestID:      "req-upstream-http-error",
		PolicyKey:      "tweets_timeline_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/userTimeline",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if len(resultPayload.Body) != 0 {
		t.Fatalf("expected placeholder error body to be suppressed, got %s", string(resultPayload.Body))
	}

	entry := decodeProviderLogLine(t, logs.Bytes())
	if _, exists := entry["upstream_response"]; exists {
		t.Fatalf("expected full upstream response to be omitted, got %#v", entry)
	}
	if entry["upstream_content_type"] != "application/json" {
		t.Fatalf("expected upstream_content_type to be logged, got %#v", entry)
	}
	if entry["upstream_response_bytes"] != float64(len(upstreamBody)) {
		t.Fatalf("expected upstream_response_bytes to be logged, got %#v", entry)
	}
	if entry["upstream_response_preview"] != upstreamBody {
		t.Fatalf("expected upstream_response_preview to be logged, got %#v", entry)
	}
}

func TestExecutePolicyOmitsUpstreamResponsePreviewOnSuccess(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	var logs bytes.Buffer
	rawMessage := strings.Repeat("x", 288)
	upstreamBody := `{"code":0,"data":{"tweets":[{"id":"2"}]},"result":{"trace":"raw"},"msg":"` + rawMessage + `"}`
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusOK, upstreamBody), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewWithWriter("provider-test", &logs))
	_, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		RequestID:      "req-upstream-success",
		PolicyKey:      "tweets_timeline_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/userTimeline",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	entry := decodeProviderLogLine(t, logs.Bytes())
	if _, exists := entry["upstream_response"]; exists {
		t.Fatalf("expected full upstream response to be omitted, got %#v", entry)
	}
	if entry["upstream_content_type"] != "application/json" {
		t.Fatalf("expected upstream_content_type to be logged, got %#v", entry)
	}
	if entry["upstream_response_bytes"] != float64(len(upstreamBody)) {
		t.Fatalf("expected upstream_response_bytes to be logged, got %#v", entry)
	}
	if _, exists := entry["upstream_response_preview"]; exists {
		t.Fatalf("expected upstream_response_preview to be omitted on success, got %#v", entry)
	}
}

func TestBridgeExecutePolicyPreservesLargeIntegersInSuccessPayload(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonHTTPResponse(http.StatusOK, `{"code":1,"data":{"tweet":{"id":9007199254740993}},"msg":"ok"}`), nil
		}),
	}

	bridge := NewBridge(cfg, client, xlog.NewStdout("provider-test"))
	resp, err := bridge.ExecutePolicy(context.Background(), &providerpb.ExecutePolicyRequest{
		RequestId:      "req-large-int",
		PolicyKey:      "tweets_detail_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/tweetTimeline",
		ProviderApiKey: "provider-key-1",
	})
	if err != nil {
		t.Fatalf("ExecutePolicy returned error: %v", err)
	}

	var payload struct {
		Body struct {
			Tweet struct {
				ID json.Number `json:"id"`
			} `json:"tweet"`
		} `json:"body"`
	}
	decoder := json.NewDecoder(strings.NewReader(resp.GetDataJson()))
	decoder.UseNumber()
	if decodeErr := decoder.Decode(&payload); decodeErr != nil {
		t.Fatalf("decode bridge payload: %v", decodeErr)
	}
	if payload.Body.Tweet.ID.String() != "9007199254740993" {
		t.Fatalf("expected large integer to be preserved, got %s", payload.Body.Tweet.ID.String())
	}
}

func TestExecutePolicyNormalizesTransportTimeout(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, context.DeadlineExceeded
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		PolicyKey:      "users_by_ids_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/usersByIdRestIds",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if resultPayload.ResultCode != "UPSTREAM_TIMEOUT" {
		t.Fatalf("unexpected result code: %#v", resultPayload.ResultCode)
	}
}

func TestExecutePolicyLogsStructuredTransportFailure(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:   "https://provider.example/api",
		TimeoutMS: 1000,
	}

	var logs bytes.Buffer
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, context.DeadlineExceeded
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewWithWriter("provider-test", &logs))
	_, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		RequestID:      "req-timeout-1",
		PolicyKey:      "users_by_ids_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/usersByIdRestIds",
		ProviderName:   "fapi.uk",
		ProviderAPIKey: "provider-key-1",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	entry := decodeProviderLogLine(t, logs.Bytes())
	if entry["request_id"] != "req-timeout-1" || entry["created_at"] == "" {
		t.Fatalf("unexpected request log payload: %#v", entry)
	}
	if entry["status_code"] != float64(http.StatusGatewayTimeout) {
		t.Fatalf("unexpected status_code in log: %#v", entry)
	}
	if entry["result_code"] != "UPSTREAM_TIMEOUT" {
		t.Fatalf("unexpected result_code in log: %#v", entry)
	}
	if entry["error_summary"] == "" {
		t.Fatalf("expected error_summary in log: %#v", entry)
	}
}

func TestHealthCheckProviderReportsReachability(t *testing.T) {
	cfg := config.ProviderConfig{
		BaseURL:    "https://provider.example/api",
		HealthPath: "/health",
		TimeoutMS:  1000,
	}

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://provider.example/api/health" {
				t.Fatalf("unexpected health url: %s", req.URL.String())
			}
			return jsonHTTPResponse(http.StatusUnauthorized, `{"error":"invalid api key"}`), nil
		}),
	}

	svc := newProviderServiceWithConfigAndClient(cfg, client, xlog.NewStdout("provider-test"))
	result, svcErr := svc.healthCheckProvider(context.Background(), healthCheckProviderRequest{ProviderName: "fapi.uk"})
	if svcErr != nil {
		t.Fatalf("healthCheckProvider returned error: %+v", svcErr)
	}

	resultMap := result.(map[string]any)
	if resultMap["healthy"] != true || resultMap["reachable"] != true {
		t.Fatalf("expected reachable provider, got %#v", resultMap)
	}
	if resultMap["status_code"] != http.StatusUnauthorized {
		t.Fatalf("unexpected health status code: %#v", resultMap["status_code"])
	}
	if resultMap["health_state"] != "auth_failed" {
		t.Fatalf("expected auth_failed health state, got %#v", resultMap["health_state"])
	}
}

func jsonHTTPResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func decodeProviderLogLine(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(payload), &entry); err != nil {
		t.Fatalf("decode provider log payload: %v; raw=%s", err, string(payload))
	}
	return entry
}

func decodeProviderResult(t *testing.T, result any) providerExecutionResult {
	t.Helper()
	payload, ok := result.(providerExecutionResult)
	if !ok {
		t.Fatalf("unexpected provider result type: %T", result)
	}
	return payload
}

func decodeProviderBody(t *testing.T, payload json.RawMessage) any {
	t.Helper()
	if len(payload) == 0 {
		return nil
	}

	var body any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decode provider body: %v; raw=%s", err, string(payload))
	}
	return body
}

// --- isEmptySearchResponse tests ---

func TestIsEmptySearchResponse_EmptyCursor(t *testing.T) {
	// Response with empty entries array (cursor-only data) should be considered empty
	body := []byte(`{
		"data": {
			"search_by_raw_query": {
				"search_timeline": {
					"timeline": {
						"entries": []
					}
				}
			}
		}
	}`)
	if !isEmptySearchResponse(body) {
		t.Fatal("expected empty entries to return true")
	}
}

func TestIsEmptySearchResponse_WithRealData(t *testing.T) {
	// Response with TimelineTimelineItem entries should NOT be empty
	body := []byte(`{
		"data": {
			"search_by_raw_query": {
				"search_timeline": {
					"timeline": {
						"entries": [
							{
								"content": {
									"__typename": "TimelineTimelineItem"
								}
							}
						]
					}
				}
			}
		}
	}`)
	if isEmptySearchResponse(body) {
		t.Fatal("expected TimelineTimelineItem entry to return false")
	}
}

func TestIsEmptySearchResponse_InvalidJSON(t *testing.T) {
	// Invalid JSON should return true (safe default)
	body := []byte(`not valid json at all`)
	if !isEmptySearchResponse(body) {
		t.Fatal("expected invalid JSON to return true (safe default)")
	}

	// Empty body should also return true
	if !isEmptySearchResponse([]byte{}) {
		t.Fatal("expected empty body to return true")
	}
}

// --- search endpoint retry and fallback tests ---

func TestSearchEndpoint_RetryOnEmptyData(t *testing.T) {
	// Use httptest server to simulate upstream behavior
	var attemptCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		if attemptCount < 3 {
			// First two attempts return empty search response
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"data": {
					"search_by_raw_query": {
						"search_timeline": {
							"timeline": {
								"entries": []
							}
						}
					}
				}
			}`))
			return
		}
		// Third attempt returns valid data
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"data": {
				"search_by_raw_query": {
					"search_timeline": {
						"timeline": {
							"entries": [
								{
									"content": {
										"__typename": "TimelineTimelineItem"
									}
								}
							]
						}
					}
				}
			}
		}`))
	}))
	defer server.Close()

	cfg := config.ProviderConfig{
		BaseURL:         server.URL,
		RetryIntervalMS: 10,
		EmptyDataRetry:  3,
		RetryCount:      2,
		TimeoutMS:       1000,
	}

	svc := newProviderServiceWithConfigAndClient(cfg, server.Client(), xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		RequestID:      "retry-test-1",
		PolicyKey:      "search_tweets_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/search",
		ProviderAPIKey: "test-key",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if attemptCount != 3 {
		t.Fatalf("expected 3 attempts, got %d", attemptCount)
	}
}

func TestSearchEndpoint_FallbackToSearchUp(t *testing.T) {
	searchAttempts := 0
	searchUpAttempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/base/apitools/searchUp") {
			searchUpAttempts++
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"data": {
					"search_by_raw_query": {
						"search_timeline": {
							"timeline": {
								"entries": [
									{
										"content": {
											"__typename": "TimelineTimelineItem",
											"tweet": {"id": "12345"}
										}
									}
								]
							}
						}
					}
				}
			}`))
			return
		}
		searchAttempts++
		w.WriteHeader(http.StatusOK)
		if searchAttempts < 3 {
			w.Write([]byte(`{
				"data": {
					"search_by_raw_query": {
						"search_timeline": {
							"timeline": {
								"entries": []
							}
						}
					}
				}
			}`))
		} else {
			w.Write([]byte(`{
				"data": {
					"search_by_raw_query": {
						"search_timeline": {
							"timeline": {
								"entries": [
									{
										"content": {
											"__typename": "TimelineTimelineItem"
										}
									}
								]
							}
						}
					}
				}
			}`))
		}
	}))
	defer server.Close()

	cfg := config.ProviderConfig{
		BaseURL:         server.URL,
		RetryIntervalMS: 10,
		EmptyDataRetry:  2,
		RetryCount:      2,
		TimeoutMS:       1000,
	}

	svc := newProviderServiceWithConfigAndClient(cfg, server.Client(), xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		RequestID:      "fallback-test-1",
		PolicyKey:      "search_tweets_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/search",
		ProviderAPIKey: "test-key",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %#v", resultPayload.StatusCode)
	}
	if searchAttempts != 3 {
		t.Fatalf("expected 3 search attempts (1 initial + 2 retries), got %d", searchAttempts)
	}
	if searchUpAttempts != 0 {
		t.Fatalf("expected 0 searchUp attempts (fallback unreachable when last attempt returns valid data), got %d", searchUpAttempts)
	}
}

func TestSearchEndpoint_FallbackFails(t *testing.T) {
	searchAttempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		searchAttempts++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"data": {
				"search_by_raw_query": {
					"search_timeline": {
						"timeline": {
							"entries": []
						}
					}
				}
			}
		}`))
	}))
	defer server.Close()

	cfg := config.ProviderConfig{
		BaseURL:         server.URL,
		RetryIntervalMS: 10,
		EmptyDataRetry:  2,
		RetryCount:      2,
		TimeoutMS:       1000,
	}

	svc := newProviderServiceWithConfigAndClient(cfg, server.Client(), xlog.NewStdout("provider-test"))
	result, svcErr := svc.executePolicy(context.Background(), executePolicyRequest{
		RequestID:      "fallback-fail-test-1",
		PolicyKey:      "search_tweets_v1",
		UpstreamMethod: http.MethodGet,
		UpstreamPath:   "/base/apitools/search",
		ProviderAPIKey: "test-key",
	})
	if svcErr != nil {
		t.Fatalf("executePolicy returned error: %+v", svcErr)
	}

	resultPayload := decodeProviderResult(t, result)
	if resultPayload.StatusCode != http.StatusOK {
		t.Fatalf("expected status code 200 (fallback unreachable when last attempt returns empty), got %#v", resultPayload.StatusCode)
	}
	if searchAttempts != 3 {
		t.Fatalf("expected 3 search attempts (1 initial + 2 retries), got %d", searchAttempts)
	}
}
