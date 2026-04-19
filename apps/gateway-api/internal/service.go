package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/gateway-api/internal/types"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/apps/scheduler-rpc/schedulerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"

	"github.com/zeromicro/go-zero/rest/pathvar"
)

type gatewayAccessClient interface {
	CheckIpBan(ctx context.Context, req *accessservice.CheckIpBanRequest) (*clients.EnvelopeResponse, error)
	GetAppAuthContextByID(ctx context.Context, req *accessservice.GetAppAuthContextByIDRequest) (*clients.EnvelopeResponse, error)
	CheckAndReserveQuota(ctx context.Context, req *accessservice.CheckAndReserveQuotaRequest) (*clients.EnvelopeResponse, error)
	ReleaseQuotaOnFailure(ctx context.Context, req *accessservice.ReleaseQuotaOnFailureRequest) (*clients.EnvelopeResponse, error)
	RecordUsageStat(ctx context.Context, req *accessservice.RecordUsageStatRequest) (*clients.EnvelopeResponse, error)
}

type gatewayPolicyClient interface {
	ResolvePolicy(ctx context.Context, req *policyservice.ResolvePolicyRequest) (*clients.EnvelopeResponse, error)
	CheckAppPolicyAccess(ctx context.Context, req *policyservice.CheckAppPolicyAccessRequest) (*clients.EnvelopeResponse, error)
}

type gatewayProviderClient interface {
	ExecutePolicy(ctx context.Context, req *providerservice.ExecutePolicyRequest) (*clients.EnvelopeResponse, error)
}

type gatewaySchedulerClient interface {
	CreateTask(ctx context.Context, req *schedulerservice.CreateTaskRequest) (*clients.EnvelopeResponse, error)
	GetTask(ctx context.Context, req *schedulerservice.GetTaskRequest) (*clients.EnvelopeResponse, error)
	ListTaskRuns(ctx context.Context, req *schedulerservice.ListTaskRunsRequest) (*clients.EnvelopeResponse, error)
	StopTask(ctx context.Context, req *schedulerservice.StopTaskRequest) (*clients.EnvelopeResponse, error)
}

type gatewayService struct {
	logger          *xlog.Logger
	accessClient    gatewayAccessClient
	policyClient    gatewayPolicyClient
	providerClient  gatewayProviderClient
	schedulerClient gatewaySchedulerClient
	usageStats      usageStatRecorder
	jwtSecret       string
	jwtIssuer       string
}

type providerExecutionPayload struct {
	StatusCode         int             `json:"status_code"`
	ResultCode         string          `json:"result_code"`
	Body               json.RawMessage `json:"body"`
	UpstreamDurationMS int64           `json:"upstream_duration_ms"`
}

type ipBanCheckResult struct {
	resp      *clients.EnvelopeResponse
	err       error
	blocked   bool
	decodeErr error
}

type policyResolveResult struct {
	resp      *clients.EnvelopeResponse
	err       error
	data      map[string]any
	decodeErr error
}

type quotaReserveResult struct {
	resp *clients.EnvelopeResponse
	err  error
}

type policyAccessResult struct {
	resp      *clients.EnvelopeResponse
	err       error
	allowed   bool
	decodeErr error
}

var sensitiveUpstreamQueryParams = []string{"proxyUrl", "auth_token", "ct0"}

const maxCreateCollectorTaskBodyBytes int64 = 1 << 20
const defaultGatewayJWTSecret = "xsonar-gateway-dev-secret"
const defaultGatewayJWTIssuer = "xsonar-gateway"

func newGatewayServiceWithClients(logger *xlog.Logger, accessClient gatewayAccessClient, policyClient gatewayPolicyClient, providerClient gatewayProviderClient) *gatewayService {
	return newGatewayServiceWithModeAndAdmin(logger, accessClient, policyClient, providerClient, nil, "", "", "")
}

func newGatewayServiceWithMode(logger *xlog.Logger, accessClient gatewayAccessClient, policyClient gatewayPolicyClient, providerClient gatewayProviderClient, mode string) *gatewayService {
	return newGatewayServiceWithModeAndAdmin(logger, accessClient, policyClient, providerClient, nil, "", "", mode)
}

func newGatewayServiceWithAdmin(logger *xlog.Logger, schedulerClient gatewaySchedulerClient, jwtSecret, jwtIssuer string) *gatewayService {
	return newGatewayServiceWithModeAndAdmin(logger, nil, nil, nil, schedulerClient, jwtSecret, jwtIssuer, "")
}

func newGatewayServiceWithModeAndAdmin(logger *xlog.Logger, accessClient gatewayAccessClient, policyClient gatewayPolicyClient, providerClient gatewayProviderClient, schedulerClient gatewaySchedulerClient, jwtSecret, jwtIssuer, mode string) *gatewayService {
	return newGatewayServiceWithModeAndUsageStats(
		logger,
		accessClient,
		policyClient,
		providerClient,
		schedulerClient,
		jwtSecret,
		jwtIssuer,
		newInlineUsageStatRecorder(accessClient),
		mode,
	)
}

func newGatewayServiceWithModeAndUsageStats(logger *xlog.Logger, accessClient gatewayAccessClient, policyClient gatewayPolicyClient, providerClient gatewayProviderClient, schedulerClient gatewaySchedulerClient, jwtSecret, jwtIssuer string, usageStats usageStatRecorder, _ string) *gatewayService {
	if usageStats == nil {
		usageStats = newInlineUsageStatRecorder(accessClient)
	}
	if strings.TrimSpace(jwtSecret) == "" {
		jwtSecret = defaultGatewayJWTSecret
	}
	if strings.TrimSpace(jwtIssuer) == "" {
		jwtIssuer = defaultGatewayJWTIssuer
	}
	return &gatewayService{
		logger:          logger,
		accessClient:    accessClient,
		policyClient:    policyClient,
		providerClient:  providerClient,
		schedulerClient: schedulerClient,
		usageStats:      usageStats,
		jwtSecret:       jwtSecret,
		jwtIssuer:       jwtIssuer,
	}
}

func (s *gatewayService) Close() {
	if s == nil || s.usageStats == nil {
		return
	}
	s.usageStats.Close()
}

func (s *gatewayService) handleProxy(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	start := time.Now().UTC()
	clientIP := requestIP(r)
	tenantID := ""
	appID := ""
	policyKey := ""

	writeGatewayError := func(statusCode, code int, message, resultCode, errorSummary string) {
		s.logGatewayError(r, requestID, start, clientIP, tenantID, appID, policyKey, statusCode, resultCode, errorSummary)
		shared.WriteError(w, statusCode, code, message, requestID)
	}

	token := shared.ExtractBearerToken(r.Header.Get("Authorization"))
	if token == "" {
		writeGatewayError(http.StatusUnauthorized, model.CodeUnauthorized, "missing or invalid bearer token", "JWT_INVALID", "missing or invalid bearer token")
		return
	}
	claims, err := shared.ParseAndValidateJWT(s.jwtSecret, token, time.Now())
	if err != nil || claims.Issuer != s.jwtIssuer || claims.Role != "gateway_app" || strings.TrimSpace(claims.Subject) == "" {
		writeGatewayError(http.StatusUnauthorized, model.CodeUnauthorized, "missing or invalid bearer token", "JWT_INVALID", "missing or invalid bearer token")
		return
	}
	appID = strings.TrimSpace(claims.Subject)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	ipBanCh := s.startIPBanCheck(ctx, clientIP)
	policyCh := s.startPolicyResolve(ctx, r.URL.Path, r.Method)

	authResp, authErr := s.accessClient.GetAppAuthContextByID(ctx, &accessservice.GetAppAuthContextByIDRequest{AppId: appID})

	ipBanResult := <-ipBanCh
	if ipBanResult.err != nil {
		s.writeGatewayDownstreamError(w, r, requestID, start, clientIP, tenantID, appID, policyKey, "CheckIpBan", ipBanResult.resp, ipBanResult.err)
		return
	}
	if ipBanResult.decodeErr != nil {
		writeGatewayError(http.StatusBadGateway, model.CodeInternalError, "decode ip ban response failed", "ACCESS_IP_BAN_DECODE_ERROR", ipBanResult.decodeErr.Error())
		return
	}
	if ipBanResult.blocked {
		writeGatewayError(http.StatusForbidden, model.CodeForbidden, "client ip is blocked", "CLIENT_IP_BLOCKED", "client ip is blocked")
		return
	}

	if authErr != nil {
		s.writeGatewayDownstreamError(w, r, requestID, start, clientIP, tenantID, appID, policyKey, "GetAppAuthContext", authResp, authErr)
		return
	}

	authData, authDecodeErr := decodeObject(authResp.Data)
	if authDecodeErr != nil {
		writeGatewayError(http.StatusBadGateway, model.CodeInternalError, "decode auth context failed", "ACCESS_AUTH_CONTEXT_DECODE_ERROR", authDecodeErr.Error())
		return
	}

	appID = stringValue(authData["app_id"])
	tenantID = stringValue(authData["tenant_id"])
	appStatus := stringValue(authData["status"])
	if appStatus != "active" {
		writeGatewayError(http.StatusForbidden, model.CodeForbidden, "app is not active", "APP_INACTIVE", "app is not active")
		return
	}

	policyResult := <-policyCh
	if policyResult.err != nil {
		s.writeGatewayDownstreamError(w, r, requestID, start, clientIP, tenantID, appID, policyKey, "ResolvePolicy", policyResult.resp, policyResult.err)
		return
	}
	if policyResult.decodeErr != nil {
		writeGatewayError(http.StatusBadGateway, model.CodeInternalError, "decode policy response failed", "POLICY_RESOLVE_DECODE_ERROR", policyResult.decodeErr.Error())
		return
	}
	policyData := policyResult.data

	policyKey = stringValue(policyData["policy_key"])

	quotaCh := s.startQuotaReservation(ctx, appID, policyKey, requestID)
	accessCh := s.startPolicyAccessCheck(ctx, appID, policyKey)

	quotaResult := <-quotaCh
	if quotaResult.err != nil {
		s.writeGatewayDownstreamError(w, r, requestID, start, clientIP, tenantID, appID, policyKey, "CheckAndReserveQuota", quotaResult.resp, quotaResult.err)
		return
	}

	accessResult := <-accessCh
	if accessResult.err != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		s.writeGatewayDownstreamError(w, r, requestID, start, clientIP, tenantID, appID, policyKey, "CheckAppPolicyAccess", accessResult.resp, accessResult.err)
		return
	}
	if accessResult.decodeErr != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		writeGatewayError(http.StatusBadGateway, model.CodeInternalError, "decode policy access response failed", "POLICY_ACCESS_DECODE_ERROR", accessResult.decodeErr.Error())
		return
	}
	if !accessResult.allowed {
		_ = s.releaseQuota(ctx, appID, requestID)
		writeGatewayError(http.StatusForbidden, model.CodeForbidden, "policy access denied", "POLICY_ACCESS_DENIED", "policy access denied")
		return
	}

	providerQuery, sanitizeErr := sanitizeUpstreamQuery(r.URL.Query(), stringSlice(policyData["allowed_params"]), stringSlice(policyData["denied_params"]), stringMap(policyData["default_params"]))
	if sanitizeErr != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		writeGatewayError(http.StatusBadRequest, model.CodeInvalidRequest, sanitizeErr.Error(), "PARAM_VALIDATION_FAILED", sanitizeErr.Error())
		return
	}
	if validationErr := validateRequiredQuery(providerQuery, stringSlice(policyData["required_params"])); validationErr != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		writeGatewayError(http.StatusBadRequest, model.CodeInvalidRequest, validationErr.Error(), "PARAM_VALIDATION_FAILED", validationErr.Error())
		return
	}
	if validationErr := validateRouteSpecificQuery(policyKey, stringValue(policyData["upstream_path"]), r.URL.Query(), providerQuery, stringMap(policyData["default_params"])); validationErr != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		writeGatewayError(http.StatusBadRequest, model.CodeInvalidRequest, validationErr.Error(), "PARAM_VALIDATION_FAILED", validationErr.Error())
		return
	}
	providerQuery = normalizeProviderQuery(policyKey, stringValue(policyData["upstream_path"]), providerQuery)

	queryJSON, marshalErr := json.Marshal(providerQuery)
	if marshalErr != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		writeGatewayError(http.StatusInternalServerError, model.CodeInternalError, "encode provider query failed", "PROVIDER_QUERY_ENCODE_ERROR", marshalErr.Error())
		return
	}

	providerResp, providerErr := s.providerClient.ExecutePolicy(ctx, &providerservice.ExecutePolicyRequest{
		RequestId:      requestID,
		PolicyKey:      policyKey,
		UpstreamMethod: stringValue(policyData["upstream_method"]),
		UpstreamPath:   stringValue(policyData["upstream_path"]),
		QueryJson:      string(queryJSON),
		ProviderName:   stringValue(policyData["provider_name"]),
		ProviderApiKey: stringValue(policyData["provider_api_key"]),
	})
	if providerErr != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		s.writeGatewayDownstreamError(w, r, requestID, start, clientIP, tenantID, appID, policyKey, "ExecutePolicy", providerResp, providerErr)
		return
	}

	providerData, providerDecodeErr := decodeProviderExecutionPayload(providerResp.Data)
	if providerDecodeErr != nil {
		_ = s.releaseQuota(ctx, appID, requestID)
		writeGatewayError(http.StatusBadGateway, model.CodeInternalError, "decode provider response failed", "PROVIDER_RESPONSE_DECODE_ERROR", providerDecodeErr.Error())
		return
	}

	statusCode := providerData.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusBadGateway
	}
	resultCode := providerData.ResultCode
	body := providerData.Body

	success := statusCode >= 200 && statusCode < 300
	if !success {
		_ = s.releaseQuota(ctx, appID, requestID)
	}

	if s.usageStats != nil {
		s.usageStats.Record(accessservice.RecordUsageStatRequest{
			TenantId:           tenantID,
			AppId:              appID,
			PolicyKey:          policyKey,
			RequestId:          requestID,
			Success:            success,
			DurationMs:         time.Since(start).Milliseconds(),
			UpstreamDurationMs: providerData.UpstreamDurationMS,
			StatusCode:         int32(statusCode),
			ResultCode:         resultCode,
		})
	}

	if !success {
		s.logGatewayError(r, requestID, start, clientIP, tenantID, appID, policyKey, statusCode, firstNonEmpty(resultCode, "UPSTREAM_REQUEST_FAILED"), "upstream request failed")
		shared.WriteRawEnvelope(w, http.StatusBadGateway, model.CodeInternalError, "upstream request failed", body, requestID)
		return
	}

	shared.LogRequestInfo(s.logger, "gateway request completed", requestID, start, gatewayLogFields(r, clientIP, tenantID, appID, policyKey, statusCode, resultCode, ""))

	shared.WriteOK(w, formatGatewaySuccessData(providerQueryResFormat(providerQuery), body), requestID)
}

func (s *gatewayService) handleCreatePeriodicCollectorTask(w http.ResponseWriter, r *http.Request) {
	s.handleCreateCollectorTaskByType(w, r, "periodic")
}

func (s *gatewayService) handleCreateRangeCollectorTask(w http.ResponseWriter, r *http.Request) {
	s.handleCreateCollectorTaskByType(w, r, "range")
}

func (s *gatewayService) handleCreateCollectorTask(w http.ResponseWriter, r *http.Request) {
	s.handleCreateCollectorTaskByType(w, r, collectorTaskTypeFromBody(r))
}

func (s *gatewayService) handleCreateCollectorTaskByType(w http.ResponseWriter, r *http.Request, taskType string) {
	subject, ok := s.requireGatewayAuthSubject(w, r)
	if !ok {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	if s.schedulerClient == nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "scheduler client is unavailable", requestID)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxCreateCollectorTaskBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			shared.WriteError(w, http.StatusRequestEntityTooLarge, model.CodeInvalidRequest, "request body is too large", requestID)
			return
		}
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid request body", requestID)
		return
	}

	createReq := schedulerservice.CreateTaskRequest{
		TaskType:  taskType,
		CreatedBy: subject,
	}
	switch taskType {
		case "periodic":
			var req types.CreatePeriodicCollectorTaskReq
			if err := json.Unmarshal(body, &req); err != nil {
			shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid request body", requestID)
			return
		}
			createReq.TaskId = req.TaskID
			createReq.Keyword = req.Keyword
			createReq.Priority = req.Priority
			createReq.FrequencySeconds = req.FrequencySeconds
			createReq.PerRunCount = req.PerRunCount
			createReq.RequiredCount = req.RequiredCount
	case "range":
		var req types.CreateRangeCollectorTaskReq
		if err := json.Unmarshal(body, &req); err != nil {
			shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "invalid request body", requestID)
			return
		}
		createReq.TaskId = req.TaskID
		createReq.Keyword = req.Keyword
		createReq.Priority = req.Priority
		createReq.Since = req.Since
		createReq.Until = req.Until
		createReq.RequiredCount = req.RequiredCount
	default:
		shared.WriteError(w, http.StatusInternalServerError, model.CodeInternalError, "collector task type is unsupported", requestID)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, callErr := s.schedulerClient.CreateTask(ctx, &createReq)
	writeAdminDownstreamResult(w, requestID, response, callErr)
}

func (s *gatewayService) handleGetCollectorTask(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireGatewayAuthSubject(w, r); !ok {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	taskID := gatewayPathParam(r, "id")
	if taskID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "task id is required", requestID)
		return
	}
	if s.schedulerClient == nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "scheduler client is unavailable", requestID)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, callErr := s.schedulerClient.GetTask(ctx, &schedulerservice.GetTaskRequest{TaskId: taskID})
	writeAdminDownstreamResult(w, requestID, response, callErr)
}

func (s *gatewayService) handleListCollectorTaskRuns(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireGatewayAuthSubject(w, r); !ok {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	taskID := gatewayPathParam(r, "id")
	if taskID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "task id is required", requestID)
		return
	}
	if s.schedulerClient == nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "scheduler client is unavailable", requestID)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, callErr := s.schedulerClient.ListTaskRuns(ctx, &schedulerservice.ListTaskRunsRequest{TaskId: taskID})
	writeAdminDownstreamResult(w, requestID, response, callErr)
}

func (s *gatewayService) handleStopCollectorTask(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireGatewayAuthSubject(w, r); !ok {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	taskID := gatewayPathParam(r, "id")
	if taskID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "task id is required", requestID)
		return
	}
	if s.schedulerClient == nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "scheduler client is unavailable", requestID)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, callErr := s.schedulerClient.StopTask(ctx, &schedulerservice.StopTaskRequest{TaskId: taskID})
	writeAdminDownstreamResult(w, requestID, response, callErr)
}

func (s *gatewayService) releaseQuota(ctx context.Context, appID, requestID string) error {
	_, err := s.accessClient.ReleaseQuotaOnFailure(ctx, &accessservice.ReleaseQuotaOnFailureRequest{
		AppId:     appID,
		RequestId: requestID,
	})
	return err
}

func (s *gatewayService) startIPBanCheck(ctx context.Context, clientIP string) <-chan ipBanCheckResult {
	resultCh := make(chan ipBanCheckResult, 1)
	if clientIP == "" {
		resultCh <- ipBanCheckResult{}
		return resultCh
	}

	go func() {
		resp, err := s.accessClient.CheckIpBan(ctx, &accessservice.CheckIpBanRequest{Ip: clientIP})
		result := ipBanCheckResult{
			resp: resp,
			err:  err,
		}
		if err == nil {
			data, decodeErr := decodeObject(resp.Data)
			result.decodeErr = decodeErr
			if decodeErr == nil {
				result.blocked, _ = data["blocked"].(bool)
			}
		}
		resultCh <- result
	}()

	return resultCh
}

func (s *gatewayService) startPolicyResolve(ctx context.Context, path, method string) <-chan policyResolveResult {
	resultCh := make(chan policyResolveResult, 1)

	go func() {
		resp, err := s.policyClient.ResolvePolicy(ctx, &policyservice.ResolvePolicyRequest{
			Path:   path,
			Method: method,
		})
		result := policyResolveResult{
			resp: resp,
			err:  err,
		}
		if err == nil {
			result.data, result.decodeErr = decodeObject(resp.Data)
		}
		resultCh <- result
	}()

	return resultCh
}

func (s *gatewayService) startQuotaReservation(ctx context.Context, appID, policyKey, requestID string) <-chan quotaReserveResult {
	resultCh := make(chan quotaReserveResult, 1)

	go func() {
		resp, err := s.accessClient.CheckAndReserveQuota(ctx, &accessservice.CheckAndReserveQuotaRequest{
			AppId:     appID,
			PolicyKey: policyKey,
			RequestId: requestID,
		})
		resultCh <- quotaReserveResult{resp: resp, err: err}
	}()

	return resultCh
}

func (s *gatewayService) startPolicyAccessCheck(ctx context.Context, appID, policyKey string) <-chan policyAccessResult {
	resultCh := make(chan policyAccessResult, 1)

	go func() {
		resp, err := s.policyClient.CheckAppPolicyAccess(ctx, &policyservice.CheckAppPolicyAccessRequest{
			AppId:     appID,
			PolicyKey: policyKey,
		})
		result := policyAccessResult{
			resp: resp,
			err:  err,
		}
		if err == nil {
			data, decodeErr := decodeObject(resp.Data)
			result.decodeErr = decodeErr
			if decodeErr == nil {
				result.allowed, _ = data["allowed"].(bool)
			}
		}
		resultCh <- result
	}()

	return resultCh
}

func (s *gatewayService) writeGatewayDownstreamError(w http.ResponseWriter, r *http.Request, requestID string, startedAt time.Time, clientIP, tenantID, appID, policyKey, downstreamPath string, response *clients.EnvelopeResponse, err error) {
	if response == nil {
		s.logGatewayError(r, requestID, startedAt, clientIP, tenantID, appID, policyKey, http.StatusBadGateway, "DOWNSTREAM_UNAVAILABLE", err.Error())
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, err.Error(), requestID)
		return
	}

	statusCode := statusFromCode(response.Code)
	fields := gatewayLogFields(r, clientIP, tenantID, appID, policyKey, statusCode, "DOWNSTREAM_ERROR", err.Error())
	fields["downstream_path"] = downstreamPath
	fields["downstream_code"] = response.Code
	fields["downstream_message"] = response.Message
	shared.LogRequestError(s.logger, "gateway downstream request failed", requestID, startedAt, fields)
	shared.WriteEnvelope(w, statusCode, response.Code, response.Message, rawGatewayData(response.Data), requestID)
}

func rawGatewayData(data json.RawMessage) any {
	if len(data) == 0 {
		return nil
	}

	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return string(data)
	}
	return decoded
}

func formatGatewaySuccessData(resFormat string, data json.RawMessage) any {
	if len(data) == 0 {
		return nil
	}

	if strings.TrimSpace(resFormat) == "" || strings.EqualFold(strings.TrimSpace(resFormat), "json") {
		if decoded, err := decodeGatewayJSON(data); err == nil {
			return decoded
		}
	}

	return rawGatewayString(data)
}

func writeAdminDownstreamResult(w http.ResponseWriter, requestID string, response *clients.EnvelopeResponse, err error) {
	if err != nil {
		if response == nil {
			shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, err.Error(), requestID)
			return
		}
		shared.WriteEnvelope(w, adminStatusFromCode(response.Code), response.Code, response.Message, rawGatewayData(response.Data), requestID)
		return
	}
	if response == nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "empty downstream response", requestID)
		return
	}
	shared.WriteEnvelope(w, adminStatusFromCode(response.Code), response.Code, response.Message, rawGatewayData(response.Data), requestID)
}

func (s *gatewayService) requireGatewayAuthSubject(w http.ResponseWriter, r *http.Request) (string, bool) {
	requestID := shared.EnsureRequestID(w, r)
	token := shared.ExtractBearerToken(r.Header.Get("Authorization"))
	if token == "" {
		shared.WriteError(w, http.StatusUnauthorized, model.CodeUnauthorized, "missing or invalid bearer token", requestID)
		return "", false
	}
	claims, err := shared.ParseAndValidateJWT(s.jwtSecret, token, time.Now())
	if err != nil || claims.Issuer != s.jwtIssuer || claims.Role != "gateway_app" || claims.Subject == "" {
		shared.WriteError(w, http.StatusUnauthorized, model.CodeUnauthorized, "missing or invalid bearer token", requestID)
		return "", false
	}
	return claims.Subject, true
}

func statusFromCode(code int) int {
	switch code {
	case model.CodeInvalidRequest:
		return http.StatusBadRequest
	case model.CodeUnauthorized:
		return http.StatusUnauthorized
	case model.CodeForbidden:
		return http.StatusForbidden
	case model.CodeNotFound:
		return http.StatusNotFound
	case model.CodeConflict:
		return http.StatusConflict
	case model.CodeRateLimited:
		return http.StatusTooManyRequests
	default:
		return http.StatusBadGateway
	}
}

func adminStatusFromCode(code int) int {
	if code == model.CodeOK {
		return http.StatusOK
	}
	return statusFromCode(code)
}

func collectorTaskTypeFromBody(r *http.Request) string {
	if r != nil {
		switch {
		case strings.Contains(r.URL.Path, "/range"):
			return "range"
		case strings.Contains(r.URL.Path, "/periodic"):
			return "periodic"
		}
	}
	return "periodic"
}

func gatewayPathParam(r *http.Request, key string) string {
	if r == nil {
		return ""
	}
	if value := strings.TrimSpace(r.PathValue(key)); value != "" {
		return value
	}
	if value := strings.TrimSpace(pathvar.Vars(r)[key]); value != "" {
		return value
	}
	return ""
}

func decodeObject(data json.RawMessage) (map[string]any, error) {
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeProviderExecutionPayload(data json.RawMessage) (providerExecutionPayload, error) {
	var result providerExecutionPayload
	if err := json.Unmarshal(data, &result); err != nil {
		return providerExecutionPayload{}, err
	}
	return result, nil
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	case json.Number:
		number, err := typed.Int64()
		if err == nil {
			return int(number)
		}
	}
	return fallback
}

func stringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}

func stringMap(value any) map[string]string {
	result := make(map[string]string)
	typed, ok := value.(map[string]any)
	if !ok {
		return result
	}

	for key, item := range typed {
		if text, ok := item.(string); ok {
			result[key] = text
		}
	}
	return result
}

func sanitizeUpstreamQuery(query url.Values, allowedParams, deniedParams []string, defaultParams map[string]string) (map[string]any, error) {
	result := make(map[string]any)
	allowedSet := normalizedPolicyParamKeys(allowedParams)
	deniedSet := normalizedPolicyParamKeys(append(append([]string(nil), deniedParams...), sensitiveUpstreamQueryParams...))
	seenKeys := make(map[string]string, len(query))

	for key, values := range query {
		if sharedIsAuthField(key) {
			continue
		}
		normalizedKey := normalizePolicyParamKey(key)
		if previous, exists := seenKeys[normalizedKey]; exists {
			return nil, &sanitizeError{message: "duplicate parameter: " + previous}
		}
		seenKeys[normalizedKey] = strings.TrimSpace(key)

		if _, denied := deniedSet[normalizedKey]; denied {
			return nil, &sanitizeError{message: "parameter is denied: " + key}
		}
		canonicalKey := strings.TrimSpace(key)
		if len(allowedSet) > 0 {
			resolvedKey, allowed := allowedSet[normalizedKey]
			if !allowed {
				return nil, &sanitizeError{message: "parameter is not allowed: " + key}
			}
			canonicalKey = resolvedKey
		}
		if len(values) == 1 {
			result[canonicalKey] = values[0]
			continue
		}
		result[canonicalKey] = slices.Clone(values)
	}

	return result, nil
}

func normalizePolicyParamKey(key string) string {
	return strings.ToLower(strings.TrimSpace(key))
}

func normalizedPolicyParamKeys(keys []string) map[string]string {
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		canonical := strings.TrimSpace(key)
		normalized := normalizePolicyParamKey(canonical)
		if normalized == "" {
			continue
		}
		if _, exists := result[normalized]; !exists {
			result[normalized] = canonical
		}
	}
	return result
}

func normalizedDefaultParams(values map[string]string) map[string]struct {
	key   string
	value string
} {
	result := make(map[string]struct {
		key   string
		value string
	}, len(values))
	for key, value := range values {
		canonical := strings.TrimSpace(key)
		normalized := normalizePolicyParamKey(canonical)
		if normalized == "" {
			continue
		}
		if _, exists := result[normalized]; !exists {
			result[normalized] = struct {
				key   string
				value string
			}{
				key:   canonical,
				value: value,
			}
		}
	}
	return result
}

func normalizeProviderQuery(policyKey, upstreamPath string, query map[string]any) map[string]any {
	if policyKey == "search_tweets_v1" && upstreamPath == "/base/apitools/search" && !hasNonEmptyQueryValue(query["product"]) {
		query["product"] = "Top"
	}
	if !hasNonEmptyQueryValue(query["resFormat"]) {
		query["resFormat"] = "json"
	}
	return query
}

func providerQueryResFormat(query map[string]any) string {
	if query == nil {
		return ""
	}
	return stringValue(query["resFormat"])
}

func rawGatewayString(data json.RawMessage) string {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		return text
	}
	return string(data)
}

func decodeGatewayJSON(data json.RawMessage) (any, error) {
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func validateRequiredQuery(query map[string]any, requiredParams []string) error {
	for _, key := range requiredParams {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		if !hasNonEmptyQueryValue(query[name]) {
			return &sanitizeError{message: "parameter is required: " + name}
		}
	}
	return nil
}

func validateRouteSpecificQuery(policyKey, upstreamPath string, rawQuery url.Values, query map[string]any, defaultParams map[string]string) error {
	switch {
	case policyKey == "lists_v1":
		if hasNonEmptyQueryValue(query["userId"]) || hasNonEmptyQueryValue(query["screenName"]) {
			return nil
		}
		return &sanitizeError{message: "one of parameters is required: userId or screenName"}
	case isZeroParamRoute(policyKey):
		if hasCallerControlledQueryParams(rawQuery, query, defaultParams) {
			return &sanitizeError{message: "parameters are not allowed for this route"}
		}
	}

	return nil
}

func isZeroParamRoute(policyKey string) bool {
	switch policyKey {
	case "search_explore_v1", "search_news_v1", "search_sports_v1", "search_entertainment_v1":
		return true
	default:
		return false
	}
}

func hasCallerControlledQueryParams(rawQuery url.Values, query map[string]any, defaultParams map[string]string) bool {
	for key := range rawQuery {
		if sharedIsAuthField(key) {
			continue
		}
		return true
	}

	defaultKeys := normalizedDefaultParams(defaultParams)
	for key, value := range query {
		if _, exists := defaultKeys[normalizePolicyParamKey(key)]; exists {
			continue
		}
		if !hasNonEmptyQueryValue(value) {
			continue
		}
		return true
	}
	return false
}

func hasNonEmptyQueryValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []string:
		for _, item := range typed {
			if strings.TrimSpace(item) != "" {
				return true
			}
		}
		return false
	case []any:
		for _, item := range typed {
			if hasNonEmptyQueryValue(item) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

type sanitizeError struct {
	message string
}

func (e *sanitizeError) Error() string {
	return e.message
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func sharedIsAuthField(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "appkey", "app_key", "appsecret", "app_secret", "timestamp", "nonce", "signature":
		return true
	default:
		return false
	}
}

func secretFingerprint(value string) string {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:6])
}

func loggedQuery(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	filtered := url.Values{}
	for key, values := range query {
		if sharedIsAuthField(key) || isRedactedLoggedQueryKey(key) {
			continue
		}
		filtered[key] = slices.Clone(values)
	}
	return filtered.Encode()
}

func isRedactedLoggedQueryKey(key string) bool {
	switch normalizePolicyParamKey(key) {
	case "authtoken", "csrftoken", "auth_token", "ct0":
		return true
	default:
		return isSensitiveUpstreamQueryParam(key)
	}
}

func isSensitiveUpstreamQueryParam(key string) bool {
	switch normalizePolicyParamKey(key) {
	case "proxyurl", "auth_token", "ct0":
		return true
	default:
		return false
	}
}

func requestIP(r *http.Request) string {
	forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0])
	if forwarded != "" {
		return forwarded
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}

func (s *gatewayService) logGatewayError(r *http.Request, requestID string, startedAt time.Time, clientIP, tenantID, appID, policyKey string, statusCode int, resultCode, errorSummary string) {
	shared.LogRequestError(s.logger, "gateway request failed", requestID, startedAt, gatewayLogFields(r, clientIP, tenantID, appID, policyKey, statusCode, resultCode, errorSummary))
}

func gatewayLogFields(r *http.Request, clientIP, tenantID, appID, policyKey string, statusCode int, resultCode, errorSummary string) map[string]any {
	return map[string]any{
		"tenant_id":     tenantID,
		"app_id":        appID,
		"policy_key":    policyKey,
		"status_code":   statusCode,
		"result_code":   resultCode,
		"error_summary": errorSummary,
		"client_ip":     clientIP,
		"method":        r.Method,
		"path":          r.URL.Path,
		"query":         loggedQuery(r.URL.Query()),
	}
}
