package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"xsonar/apps/provider-rpc/internal/config"
	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type providerServiceError struct {
	statusCode int
	code       int
	message    string
}

type providerService struct {
	logger *xlog.Logger
	config config.ProviderConfig
	client *http.Client
}

const maxUpstreamResponseBytes = 10 << 20
const maxUpstreamLogPreviewChars = 256

type executePolicyRequest struct {
	RequestID      string         `json:"request_id"`
	PolicyKey      string         `json:"policy_key"`
	UpstreamMethod string         `json:"upstream_method"`
	UpstreamPath   string         `json:"upstream_path"`
	Query          map[string]any `json:"query"`
	ProviderName   string         `json:"provider_name"`
	ProviderAPIKey string         `json:"provider_api_key"`
}

type healthCheckProviderRequest struct {
	ProviderName string `json:"provider_name"`
}

type providerExecutionResult struct {
	StatusCode         int             `json:"status_code"`
	ResultCode         string          `json:"result_code"`
	Body               json.RawMessage `json:"body,omitempty"`
	UpstreamDurationMS int64           `json:"upstream_duration_ms"`
}

func newProviderService(logger *xlog.Logger) *providerService {
	return newProviderServiceWithConfigAndClient(config.ProviderConfig{}, nil, logger)
}

func newProviderServiceWithConfig(cfg config.ProviderConfig, logger *xlog.Logger) *providerService {
	return newProviderServiceWithConfigAndClient(cfg, nil, logger)
}

func newProviderServiceWithConfigAndClient(cfg config.ProviderConfig, client *http.Client, logger *xlog.Logger) *providerService {
	if client == nil {
		client = &http.Client{
			Timeout: time.Duration(cfg.TimeoutMS) * time.Millisecond,
		}
	}

	return &providerService{
		logger: logger,
		config: cfg,
		client: client,
	}
}

func (s *providerService) handleExecutePolicy(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req executePolicyRequest
	if err := shared.DecodeJSONBody(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid JSON body", requestID)
		return
	}

	data, svcErr := s.executePolicy(r.Context(), req)
	writeProviderResult(w, requestID, data, svcErr)
}

func (s *providerService) handleHealthCheckProvider(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	var req healthCheckProviderRequest
	_ = shared.DecodeJSONBody(r, &req)
	if req.ProviderName == "" {
		req.ProviderName = r.URL.Query().Get("provider_name")
	}

	data, svcErr := s.healthCheckProvider(r.Context(), req)
	writeProviderResult(w, requestID, data, svcErr)
}

func (s *providerService) executePolicy(ctx context.Context, req executePolicyRequest) (any, *providerServiceError) {
	if strings.TrimSpace(req.PolicyKey) == "" || strings.TrimSpace(req.UpstreamPath) == "" {
		return nil, providerInvalidRequest("policy_key and upstream_path are required")
	}
	if strings.TrimSpace(s.config.BaseURL) == "" {
		return nil, providerInternalError("provider_base_url is not configured")
	}
	providerBaseURL, err := normalizeProviderBaseURL(s.config.BaseURL)
	if err != nil {
		return nil, providerInternalError("invalid provider_base_url")
	}

	method := strings.ToUpper(firstNonEmpty(req.UpstreamMethod, http.MethodGet))
	targetURL, err := buildUpstreamURL(providerBaseURL, req.UpstreamPath, req.Query)
	if err != nil {
		return nil, providerInvalidRequest("invalid upstream target")
	}

	attempts := 1
	if method == http.MethodGet {
		attempts += s.config.RetryCount
	}

	start := time.Now()
	var lastResultCode string
	for attempt := 1; attempt <= attempts; attempt++ {
		httpReq, buildErr := http.NewRequestWithContext(ctx, method, targetURL, nil)
		if buildErr != nil {
			return nil, providerInternalError("build upstream request failed")
		}
		httpReq.Header.Set("Accept", "application/json")
		if req.RequestID != "" {
			httpReq.Header.Set("X-Request-ID", req.RequestID)
		}
		if headerName := strings.TrimSpace(s.config.APIKeyHeader); headerName != "" && strings.TrimSpace(req.ProviderAPIKey) != "" {
			httpReq.Header.Set(headerName, req.ProviderAPIKey)
		}

		resp, doErr := s.client.Do(httpReq)
		if doErr != nil {
			lastResultCode = transportResultCode(doErr)
			if attempt < attempts && shouldRetry(method, 0, doErr) {
				continue
			}

			s.logProviderRequestError("provider upstream transport failed", start, req, method, targetURL, attempt, transportStatusCode(doErr), lastResultCode, doErr.Error())

			return providerExecutionResult{
				StatusCode:         transportStatusCode(doErr),
				ResultCode:         lastResultCode,
				Body:               mustMarshalProviderBody(map[string]any{"error": doErr.Error()}),
				UpstreamDurationMS: time.Since(start).Milliseconds(),
			}, nil
		}

		bodyBytes, readErr := shared.ReadAllWithLimit(resp.Body, maxUpstreamResponseBytes)
		_ = resp.Body.Close()
		if readErr != nil {
			s.logProviderRequestError("provider upstream read failed", start, req, method, targetURL, attempt, http.StatusBadGateway, "UPSTREAM_READ_ERROR", readErr.Error())
			return providerExecutionResult{
				StatusCode:         http.StatusBadGateway,
				ResultCode:         "UPSTREAM_READ_ERROR",
				Body:               mustMarshalProviderBody(map[string]any{"error": readErr.Error()}),
				UpstreamDurationMS: time.Since(start).Milliseconds(),
			}, nil
		}

		lastResultCode = upstreamResultCode(resp.StatusCode)

		if req.UpstreamPath == "/base/apitools/search" && isEmptySearchResponse(bodyBytes) && attempt < attempts {
			s.logger.Info(fmt.Sprintf("search endpoint returned empty data, retrying (attempt %d/%d)", attempt, attempts), nil)
			time.Sleep(time.Duration(s.config.RetryIntervalMS) * time.Millisecond)
			continue
		}

		if attempt < attempts && shouldRetry(method, resp.StatusCode, nil) {
			continue
		}

		normalizedStatusCode, normalizedResultCode, normalizedBody := normalizeUpstreamPayloadRaw(resp.StatusCode, bodyBytes, resp.Header.Get("Content-Type"))
		lastResultCode = normalizedResultCode
		logFields := providerLogFields(req, method, targetURL, attempt, normalizedStatusCode, lastResultCode, "")
		logFields["upstream_status_code"] = resp.StatusCode
		for key, value := range providerLogResponseFields(bodyBytes, resp.Header.Get("Content-Type"), normalizedStatusCode >= http.StatusBadRequest) {
			logFields[key] = value
		}
		if normalizedStatusCode >= http.StatusBadRequest {
			shared.LogRequestError(s.logger, "provider request executed", req.RequestID, start, logFields)
		} else {
			shared.LogRequestInfo(s.logger, "provider request executed", req.RequestID, start, logFields)
		}

		return providerExecutionResult{
			StatusCode:         normalizedStatusCode,
			ResultCode:         lastResultCode,
			Body:               normalizedBody,
			UpstreamDurationMS: time.Since(start).Milliseconds(),
		}, nil
	}

	if req.UpstreamPath == "/base/apitools/search" {
		fallbackURL := strings.Replace(targetURL, "/base/apitools/search", "/base/apitools/searchUp", 1)
		s.logger.Info("search endpoint exhausted retries, trying fallback to searchUp", nil)

		fallbackAttempts := 1 + s.config.EmptyDataRetry
		for attempt := 1; attempt <= fallbackAttempts; attempt++ {
			httpReq, buildErr := http.NewRequestWithContext(ctx, method, fallbackURL, nil)
			if buildErr != nil {
				return nil, providerInternalError("build fallback upstream request failed")
			}
			httpReq.Header.Set("Accept", "application/json")
			if req.RequestID != "" {
				httpReq.Header.Set("X-Request-ID", req.RequestID)
			}
			if headerName := strings.TrimSpace(s.config.APIKeyHeader); headerName != "" && strings.TrimSpace(req.ProviderAPIKey) != "" {
				httpReq.Header.Set(headerName, req.ProviderAPIKey)
			}

			resp, doErr := s.client.Do(httpReq)
			if doErr != nil {
				if attempt < fallbackAttempts {
					time.Sleep(time.Duration(s.config.RetryIntervalMS) * time.Millisecond)
					continue
				}
				s.logProviderRequestError("fallback upstream transport failed", start, req, method, fallbackURL, attempt, transportStatusCode(doErr), lastResultCode, doErr.Error())
				return providerExecutionResult{
					StatusCode:         transportStatusCode(doErr),
					ResultCode:         lastResultCode,
					Body:               mustMarshalProviderBody(map[string]any{"error": doErr.Error()}),
					UpstreamDurationMS: time.Since(start).Milliseconds(),
				}, nil
			}

			bodyBytes, readErr := shared.ReadAllWithLimit(resp.Body, maxUpstreamResponseBytes)
			_ = resp.Body.Close()
			if readErr != nil {
				if attempt < fallbackAttempts {
					time.Sleep(time.Duration(s.config.RetryIntervalMS) * time.Millisecond)
					continue
				}
				s.logProviderRequestError("fallback upstream read failed", start, req, method, fallbackURL, attempt, http.StatusBadGateway, "UPSTREAM_READ_ERROR", readErr.Error())
				return providerExecutionResult{
					StatusCode:         http.StatusBadGateway,
					ResultCode:         "UPSTREAM_READ_ERROR",
					Body:               mustMarshalProviderBody(map[string]any{"error": readErr.Error()}),
					UpstreamDurationMS: time.Since(start).Milliseconds(),
				}, nil
			}

			lastResultCode = upstreamResultCode(resp.StatusCode)

			if isEmptySearchResponse(bodyBytes) && attempt < fallbackAttempts {
				s.logger.Info(fmt.Sprintf("fallback searchUp returned empty data, retrying (attempt %d/%d)", attempt, fallbackAttempts), nil)
				time.Sleep(time.Duration(s.config.RetryIntervalMS) * time.Millisecond)
				continue
			}

			if attempt < fallbackAttempts && shouldRetry(method, resp.StatusCode, nil) {
				continue
			}

			normalizedStatusCode, normalizedResultCode, normalizedBody := normalizeUpstreamPayloadRaw(resp.StatusCode, bodyBytes, resp.Header.Get("Content-Type"))
			lastResultCode = normalizedResultCode
			logFields := providerLogFields(req, method, fallbackURL, attempt, normalizedStatusCode, lastResultCode, "")
			logFields["upstream_status_code"] = resp.StatusCode
			logFields["fallback"] = true
			for key, value := range providerLogResponseFields(bodyBytes, resp.Header.Get("Content-Type"), normalizedStatusCode >= http.StatusBadRequest) {
				logFields[key] = value
			}
			if normalizedStatusCode >= http.StatusBadRequest {
				shared.LogRequestError(s.logger, "fallback request executed", req.RequestID, start, logFields)
			} else {
				shared.LogRequestInfo(s.logger, "fallback request executed", req.RequestID, start, logFields)
			}

			return providerExecutionResult{
				StatusCode:         normalizedStatusCode,
				ResultCode:         lastResultCode,
				Body:               normalizedBody,
				UpstreamDurationMS: time.Since(start).Milliseconds(),
			}, nil
		}

		return providerExecutionResult{
			StatusCode:         http.StatusBadGateway,
			ResultCode:         firstNonEmpty(lastResultCode, "UPSTREAM_EXECUTION_FAILED"),
			Body:               mustMarshalProviderBody(map[string]any{"error": "provider fallback exhausted retries"}),
			UpstreamDurationMS: time.Since(start).Milliseconds(),
		}, nil
	}

	return providerExecutionResult{
		StatusCode:         http.StatusBadGateway,
		ResultCode:         firstNonEmpty(lastResultCode, "UPSTREAM_EXECUTION_FAILED"),
		Body:               mustMarshalProviderBody(map[string]any{"error": "provider execution exhausted retries"}),
		UpstreamDurationMS: time.Since(start).Milliseconds(),
	}, nil
}

func (s *providerService) healthCheckProvider(ctx context.Context, req healthCheckProviderRequest) (any, *providerServiceError) {
	if strings.TrimSpace(s.config.BaseURL) == "" {
		return nil, providerInternalError("provider_base_url is not configured")
	}
	providerBaseURL, err := normalizeProviderBaseURL(s.config.BaseURL)
	if err != nil {
		return nil, providerInternalError("invalid provider_base_url")
	}

	targetURL, err := buildUpstreamURL(providerBaseURL, firstNonEmpty(s.config.HealthPath, "/"), nil)
	if err != nil {
		return nil, providerInternalError("invalid provider health target")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, providerInternalError("build provider health request failed")
	}
	httpReq.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := s.client.Do(httpReq)
	if err != nil {
		healthData := map[string]any{
			"provider_name":         firstNonEmpty(req.ProviderName, "fapi.uk"),
			"healthy":               false,
			"reachable":             false,
			"health_state":          "unreachable",
			"status_code":           transportStatusCode(err),
			"result_code":           transportResultCode(err),
			"upstream_duration_ms":  time.Since(start).Milliseconds(),
			"provider_base_url":     providerBaseURL,
			"provider_health_path":  firstNonEmpty(s.config.HealthPath, "/"),
			"provider_api_key_name": s.config.APIKeyHeader,
		}
		shared.LogRequestError(s.logger, "provider health check completed", "", start, map[string]any{
			"provider_name":   healthData["provider_name"],
			"status_code":     healthData["status_code"],
			"result_code":     healthData["result_code"],
			"health_state":    healthData["health_state"],
			"upstream_path":   firstNonEmpty(s.config.HealthPath, "/"),
			"upstream_method": http.MethodGet,
			"upstream_url":    targetURL,
			"error_summary":   err.Error(),
		})
		return healthData, nil
	}
	defer resp.Body.Close()

	healthState := providerHealthState(resp.StatusCode)
	healthData := map[string]any{
		"provider_name":         firstNonEmpty(req.ProviderName, "fapi.uk"),
		"healthy":               healthState == "ok" || healthState == "auth_failed",
		"reachable":             true,
		"health_state":          healthState,
		"status_code":           resp.StatusCode,
		"result_code":           upstreamResultCode(resp.StatusCode),
		"upstream_duration_ms":  time.Since(start).Milliseconds(),
		"provider_base_url":     providerBaseURL,
		"provider_health_path":  firstNonEmpty(s.config.HealthPath, "/"),
		"provider_api_key_name": s.config.APIKeyHeader,
	}
	logFields := map[string]any{
		"provider_name":   healthData["provider_name"],
		"status_code":     healthData["status_code"],
		"result_code":     healthData["result_code"],
		"health_state":    healthData["health_state"],
		"upstream_path":   firstNonEmpty(s.config.HealthPath, "/"),
		"upstream_method": http.MethodGet,
		"upstream_url":    targetURL,
	}
	if healthState == "ok" || healthState == "auth_failed" {
		shared.LogRequestInfo(s.logger, "provider health check completed", "", start, logFields)
	} else {
		shared.LogRequestError(s.logger, "provider health check completed", "", start, logFields)
	}
	return healthData, nil
}

func writeProviderResult(w http.ResponseWriter, requestID string, data any, svcErr *providerServiceError) {
	if svcErr != nil {
		shared.WriteError(w, svcErr.statusCode, svcErr.code, svcErr.message, requestID)
		return
	}

	shared.WriteOK(w, data, requestID)
}

func providerInvalidRequest(message string) *providerServiceError {
	return &providerServiceError{statusCode: http.StatusBadRequest, code: model.CodeInvalidRequest, message: message}
}

func providerInternalError(message string) *providerServiceError {
	return &providerServiceError{statusCode: http.StatusInternalServerError, code: model.CodeInternalError, message: message}
}

func normalizeProviderBaseURL(baseURL string) (string, error) {
	normalized := strings.TrimSpace(baseURL)
	if normalized == "" {
		return "", nil
	}
	if !strings.Contains(normalized, "://") {
		normalized = "https://" + normalized
	}

	parsed, err := url.Parse(normalized)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("unsupported provider base url scheme: %s", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("provider base url host is required")
	}
	return parsed.String(), nil
}

func (s *providerService) logProviderRequestError(message string, startedAt time.Time, req executePolicyRequest, method, targetURL string, attempt, statusCode int, resultCode, errorSummary string) {
	shared.LogRequestError(s.logger, message, req.RequestID, startedAt, providerLogFields(req, method, targetURL, attempt, statusCode, resultCode, errorSummary))
}

func providerLogFields(req executePolicyRequest, method, targetURL string, attempt, statusCode int, resultCode, errorSummary string) map[string]any {
	return map[string]any{
		"policy_key":      req.PolicyKey,
		"provider_name":   firstNonEmpty(req.ProviderName, "fapi.uk"),
		"upstream_method": method,
		"upstream_path":   req.UpstreamPath,
		"upstream_url":    targetURL,
		"attempt":         attempt,
		"status_code":     statusCode,
		"result_code":     resultCode,
		"error_summary":   errorSummary,
	}
}

func providerHealthState(statusCode int) string {
	switch {
	case statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices:
		return "ok"
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return "auth_failed"
	case statusCode >= http.StatusInternalServerError:
		return "upstream_error"
	default:
		return "degraded"
	}
}

func buildUpstreamURL(baseURL, upstreamPath string, query map[string]any) (string, error) {
	parsedBase, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}

	parsedBase.Path = joinURLPath(parsedBase.Path, upstreamPath)
	values := parsedBase.Query()
	for key, items := range encodeQueryValues(query) {
		for _, item := range items {
			values.Add(key, item)
		}
	}
	parsedBase.RawQuery = values.Encode()
	return parsedBase.String(), nil
}

func joinURLPath(basePath, upstreamPath string) string {
	return strings.TrimRight(basePath, "/") + "/" + strings.TrimLeft(strings.TrimSpace(upstreamPath), "/")
}

func encodeQueryValues(query map[string]any) url.Values {
	values := url.Values{}
	for key, rawValue := range query {
		switch typed := rawValue.(type) {
		case nil:
			continue
		case string:
			values.Add(key, typed)
		case []string:
			for _, item := range typed {
				values.Add(key, item)
			}
		case []any:
			for _, item := range typed {
				values.Add(key, fmt.Sprint(item))
			}
		default:
			values.Add(key, fmt.Sprint(rawValue))
		}
	}
	return values
}

func decodeUpstreamBody(body []byte, contentType string) any {
	if len(body) == 0 {
		return map[string]any{}
	}

	if strings.Contains(strings.ToLower(contentType), "json") || json.Valid(body) {
		var payload any
		if err := json.Unmarshal(body, &payload); err == nil {
			return payload
		}
	}

	return string(body)
}

func providerLogResponseFields(body []byte, contentType string, includePreview bool) map[string]any {
	fields := map[string]any{
		"upstream_response_bytes": len(body),
	}

	if trimmedContentType := strings.TrimSpace(contentType); trimmedContentType != "" {
		fields["upstream_content_type"] = trimmedContentType
	}

	if includePreview {
		if preview := providerLogResponsePreview(body, contentType); preview != "" {
			fields["upstream_response_preview"] = preview
		}
	}

	return fields
}

func providerLogResponsePreview(body []byte, contentType string) string {
	trimmedBody := bytes.TrimSpace(body)
	if len(trimmedBody) == 0 {
		return ""
	}

	previewBody := trimmedBody
	if strings.Contains(strings.ToLower(contentType), "json") || json.Valid(trimmedBody) {
		var compact bytes.Buffer
		if err := json.Compact(&compact, trimmedBody); err == nil {
			previewBody = compact.Bytes()
		}
	}

	preview := string(previewBody)
	if len([]rune(preview)) <= maxUpstreamLogPreviewChars {
		return preview
	}

	return string([]rune(preview)[:maxUpstreamLogPreviewChars]) + "..."
}

func normalizeUpstreamPayloadRaw(statusCode int, body []byte, contentType string) (int, string, json.RawMessage) {
	resultCode := upstreamResultCode(statusCode)
	trimmedBody := bytes.TrimSpace(body)
	if len(trimmedBody) == 0 {
		return statusCode, resultCode, json.RawMessage(`{}`)
	}

	if !json.Valid(trimmedBody) {
		return statusCode, resultCode, mustMarshalProviderBody(string(trimmedBody))
	}

	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		envelope, ok := rawJSONObject(trimmedBody)
		if !ok {
			return statusCode, resultCode, cloneRawJSON(trimmedBody)
		}
		if errorBody := upstreamApplicationErrorBodyRaw(envelope); len(errorBody) > 0 {
			return http.StatusBadGateway, "UPSTREAM_APPLICATION_ERROR", errorBody
		}
		if data, exists := envelope["data"]; exists {
			return statusCode, resultCode, cloneRawJSON(data)
		}
		return statusCode, resultCode, cloneRawJSON(trimmedBody)
	}

	if isPlaceholderUpstreamErrorPayloadRaw(trimmedBody) {
		return statusCode, resultCode, nil
	}

	return statusCode, resultCode, cloneRawJSON(trimmedBody)
}

func normalizeUpstreamPayload(statusCode int, payload any) (int, string, any) {
	resultCode := upstreamResultCode(statusCode)

	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		envelope, ok := payload.(map[string]any)
		if !ok {
			return statusCode, resultCode, payload
		}
		if errorBody := upstreamApplicationErrorBody(envelope); errorBody != nil {
			return http.StatusBadGateway, "UPSTREAM_APPLICATION_ERROR", errorBody
		}
		if data, exists := envelope["data"]; exists {
			return statusCode, resultCode, data
		}
		return statusCode, resultCode, payload
	}

	if isPlaceholderUpstreamErrorPayload(payload) {
		return statusCode, resultCode, nil
	}

	return statusCode, resultCode, payload
}

func isPlaceholderUpstreamErrorPayload(payload any) bool {
	body, ok := payload.(map[string]any)
	if !ok {
		return false
	}
	if len(body) != 1 {
		return false
	}
	_, exists := body["additionalProp"]
	return exists
}

func isPlaceholderUpstreamErrorPayloadRaw(payload json.RawMessage) bool {
	body, ok := rawJSONObject(payload)
	if !ok {
		return false
	}
	if len(body) != 1 {
		return false
	}
	_, exists := body["additionalProp"]
	return exists
}

func upstreamApplicationErrorBody(envelope map[string]any) map[string]any {
	dataValue, ok := envelope["data"].(string)
	if ok {
		dataValue = strings.TrimSpace(dataValue)
		msgValue, _ := envelope["msg"].(string)
		msgValue = strings.TrimSpace(msgValue)
		if dataValue != "" && msgValue != "" && strings.EqualFold(dataValue, msgValue) && looksLikeUpstreamErrorText(dataValue) {
			return map[string]any{"error": dataValue}
		}
	}

	dataObject, ok := envelope["data"].(map[string]any)
	if !ok {
		return nil
	}

	rawErrors, ok := dataObject["errors"].([]any)
	if !ok || len(rawErrors) == 0 {
		return nil
	}

	errorMessage := "upstream returned application errors"
	if firstError, ok := rawErrors[0].(map[string]any); ok {
		if message, ok := firstError["message"].(string); ok && strings.TrimSpace(message) != "" {
			errorMessage = strings.TrimSpace(message)
		}
	}

	errorBody := map[string]any{
		"error":  errorMessage,
		"errors": rawErrors,
	}
	if nestedData, exists := dataObject["data"]; exists && !isEmptyObject(nestedData) {
		errorBody["data"] = nestedData
	}

	return errorBody
}

func upstreamApplicationErrorBodyRaw(envelope map[string]json.RawMessage) json.RawMessage {
	dataRaw, exists := envelope["data"]
	if !exists {
		return nil
	}

	var dataValue string
	if err := json.Unmarshal(dataRaw, &dataValue); err == nil {
		dataValue = strings.TrimSpace(dataValue)
		var msgValue string
		_ = json.Unmarshal(envelope["msg"], &msgValue)
		msgValue = strings.TrimSpace(msgValue)
		if dataValue != "" && msgValue != "" && strings.EqualFold(dataValue, msgValue) && looksLikeUpstreamErrorText(dataValue) {
			return mustMarshalProviderBody(map[string]any{"error": dataValue})
		}
	}

	var dataObject struct {
		Errors []json.RawMessage `json:"errors"`
		Data   json.RawMessage   `json:"data"`
	}
	if err := json.Unmarshal(dataRaw, &dataObject); err != nil || len(dataObject.Errors) == 0 {
		return nil
	}

	errorMessage := "upstream returned application errors"
	var firstError struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(dataObject.Errors[0], &firstError); err == nil && strings.TrimSpace(firstError.Message) != "" {
		errorMessage = strings.TrimSpace(firstError.Message)
	}

	errorBody := map[string]any{
		"error":  errorMessage,
		"errors": dataObject.Errors,
	}
	if !isEmptyRawObject(dataObject.Data) && !isNullRawJSON(dataObject.Data) {
		errorBody["data"] = dataObject.Data
	}

	return mustMarshalProviderBody(errorBody)
}

func looksLikeUpstreamErrorText(message string) bool {
	normalized := strings.ToLower(strings.TrimSpace(message))
	if normalized == "" {
		return false
	}

	errorMarkers := []string{
		"failed",
		"error",
		"invalid",
		"unauthorized",
		"forbidden",
		"denied",
		"expired",
		"missing",
		"not found",
		"verification failed",
		"please check",
	}
	for _, marker := range errorMarkers {
		if strings.Contains(normalized, marker) {
			return true
		}
	}

	return false
}

func rawJSONObject(body []byte) (map[string]json.RawMessage, bool) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, false
	}
	return payload, true
}

func cloneRawJSON(body []byte) json.RawMessage {
	if len(body) == 0 {
		return nil
	}
	cloned := make([]byte, len(body))
	copy(cloned, body)
	return json.RawMessage(cloned)
}

func mustMarshalProviderBody(value any) json.RawMessage {
	if value == nil {
		return nil
	}

	body, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return json.RawMessage(body)
}

func isEmptyRawObject(value json.RawMessage) bool {
	body, ok := rawJSONObject(value)
	return ok && len(body) == 0
}

func isNullRawJSON(value json.RawMessage) bool {
	return string(bytes.TrimSpace(value)) == "null"
}

func isEmptyObject(value any) bool {
	if value == nil {
		return true
	}
	object, ok := value.(map[string]any)
	return ok && len(object) == 0
}

func isEmptySearchResponse(body []byte) bool {
	if len(body) == 0 {
		return true
	}

	var resp struct {
		Data struct {
			SearchByRawQuery struct {
				SearchTimeline struct {
					Timeline struct {
						Entries []struct {
							Content struct {
								Typename string `json:"__typename"`
							} `json:"content"`
						} `json:"entries"`
					} `json:"timeline"`
				} `json:"search_timeline"`
			} `json:"search_by_raw_query"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return true
	}

	entries := resp.Data.SearchByRawQuery.SearchTimeline.Timeline.Entries
	if len(entries) == 0 {
		return true
	}

	for _, entry := range entries {
		if entry.Content.Typename == "TimelineTimelineItem" {
			return false
		}
	}

	return true
}

func shouldRetry(method string, statusCode int, err error) bool {
	if strings.ToUpper(method) != http.MethodGet {
		return false
	}
	if err != nil {
		return true
	}
	return statusCode >= http.StatusInternalServerError
}

func upstreamResultCode(statusCode int) string {
	switch {
	case statusCode >= 200 && statusCode < 300:
		return "UPSTREAM_OK"
	case statusCode == http.StatusTooManyRequests:
		return "UPSTREAM_RATE_LIMITED"
	case statusCode >= 500:
		return "UPSTREAM_5XX"
	case statusCode >= 400:
		return "UPSTREAM_4XX"
	default:
		return "UPSTREAM_RESPONSE"
	}
}

func transportResultCode(err error) string {
	if isTimeoutError(err) {
		return "UPSTREAM_TIMEOUT"
	}
	return "UPSTREAM_NETWORK_ERROR"
}

func transportStatusCode(err error) int {
	if isTimeoutError(err) {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}

	return errors.Is(err, context.DeadlineExceeded)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
