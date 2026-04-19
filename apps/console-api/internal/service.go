package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/console-api/internal/config"
	"xsonar/apps/console-api/internal/types"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/apps/provider-rpc/providerservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type consoleAccessClient interface {
	Health(ctx context.Context) (*clients.EnvelopeResponse, error)
	AuthenticateConsoleUser(ctx context.Context, req *accessservice.AuthenticateConsoleUserRequest) (*clients.EnvelopeResponse, error)
	GetAppAuthContextByID(ctx context.Context, req *accessservice.GetAppAuthContextByIDRequest) (*clients.EnvelopeResponse, error)
	ListTenants(ctx context.Context, req *accessservice.ListTenantsRequest) (*clients.EnvelopeResponse, error)
	CreateTenant(ctx context.Context, req *accessservice.CreateTenantRequest) (*clients.EnvelopeResponse, error)
	ListTenantApps(ctx context.Context, req *accessservice.ListTenantAppsRequest) (*clients.EnvelopeResponse, error)
	CreateTenantApp(ctx context.Context, req *accessservice.CreateTenantAppRequest) (*clients.EnvelopeResponse, error)
	UpdateTenantAppStatus(ctx context.Context, req *accessservice.UpdateTenantAppStatusRequest) (*clients.EnvelopeResponse, error)
	UpdateAppQuota(ctx context.Context, req *accessservice.UpdateAppQuotaRequest) (*clients.EnvelopeResponse, error)
	QueryUsageStats(ctx context.Context, req *accessservice.QueryUsageStatsRequest) (*clients.EnvelopeResponse, error)
}

type consolePolicyClient interface {
	Health(ctx context.Context) (*clients.EnvelopeResponse, error)
	ListPolicies(ctx context.Context, req *policyservice.ListPoliciesRequest) (*clients.EnvelopeResponse, error)
	PublishPolicyConfig(ctx context.Context, req *policyservice.PublishPolicyConfigRequest) (*clients.EnvelopeResponse, error)
	BindAppPolicies(ctx context.Context, req *policyservice.BindAppPoliciesRequest) (*clients.EnvelopeResponse, error)
}

type consoleProviderClient interface {
	Health(ctx context.Context) (*clients.EnvelopeResponse, error)
	HealthCheckProvider(ctx context.Context, req *providerservice.HealthCheckProviderRequest) (*clients.EnvelopeResponse, error)
}

type consoleService struct {
	logger         *xlog.Logger
	config         config.ConsoleConfig
	accessClient   consoleAccessClient
	policyClient   consolePolicyClient
	providerClient consoleProviderClient
}

func newConsoleServiceWithConfigAndAllClients(cfg config.ConsoleConfig, logger *xlog.Logger, accessClient consoleAccessClient, policyClient consolePolicyClient, providerClient consoleProviderClient) *consoleService {
	if providerClient == nil {
		panic("provider client is required")
	}

	return &consoleService{
		logger:         logger,
		config:         cfg,
		accessClient:   accessClient,
		policyClient:   policyClient,
		providerClient: providerClient,
	}
}

func (s *consoleService) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req types.LoginReq
	if !parseValidatedRequest(w, r, &req, validateLoginReq) {
		return
	}
	s.serveLogin(w, r, &accessservice.AuthenticateConsoleUserRequest{
		Username: req.Username,
		Password: req.Password,
	})
}

func (s *consoleService) serveLogin(w http.ResponseWriter, r *http.Request, payload *accessservice.AuthenticateConsoleUserRequest) {
	requestID := shared.EnsureRequestID(w, r)

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.AuthenticateConsoleUser(ctx, payload)
	if err != nil {
		writeDownstreamResult(w, requestID, response, err)
		return
	}

	var authData map[string]any
	if decodeErr := json.Unmarshal(response.Data, &authData); decodeErr != nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "decode auth response failed", requestID)
		return
	}

	userID := stringValue(authData["user_id"])
	role := stringValue(authData["role"])
	if userID == "" || role == "" {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "incomplete auth response", requestID)
		return
	}

	token, tokenErr := shared.SignJWT(
		s.config.JWTSecret,
		s.config.JWTIssuer,
		userID,
		role,
		time.Duration(s.config.JWTTTLMinutes)*time.Minute,
		time.Now(),
	)
	if tokenErr != nil {
		shared.WriteError(w, http.StatusInternalServerError, model.CodeInternalError, "issue admin token failed", requestID)
		return
	}

	authData["token"] = token
	shared.WriteOK(w, authData, requestID)
}

func (s *consoleService) handleListTenants(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.ListTenants(ctx, &accessservice.ListTenantsRequest{})
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleIssueGatewayToken(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}
	if s.accessClient == nil {
		requestID := shared.EnsureRequestID(w, r)
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "access client is unavailable", requestID)
		return
	}

	var req types.IssueGatewayTokenReq
	if !parseValidatedRequest(w, r, &req, validateIssueGatewayTokenReq) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.GetAppAuthContextByID(ctx, &accessservice.GetAppAuthContextByIDRequest{AppId: req.AppID})
	if err != nil {
		writeDownstreamResult(w, requestID, response, err)
		return
	}

	var authData map[string]any
	if decodeErr := json.Unmarshal(response.Data, &authData); decodeErr != nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "decode auth response failed", requestID)
		return
	}
	if stringValue(authData["app_id"]) != req.AppID {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "app auth context is inconsistent", requestID)
		return
	}

	if stringValue(authData["tenant_id"]) != req.TenantID {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "tenant_id and app_id do not match", requestID)
		return
	}
	if stringValue(authData["status"]) != "active" {
		shared.WriteError(w, http.StatusForbidden, model.CodeForbidden, "app is not active", requestID)
		return
	}

	now := time.Now().UTC()
	tokenTTL := time.Duration(req.TTL) * time.Second
	token, tokenErr := shared.SignJWT(
		s.config.GatewayJWTSecret,
		s.config.GatewayJWTIssuer,
		req.AppID,
		"gateway_app",
		tokenTTL,
		now,
	)
	if tokenErr != nil {
		shared.WriteError(w, http.StatusInternalServerError, model.CodeInternalError, "issue gateway token failed", requestID)
		return
	}

	expiresAt := ""
	if req.TTL > 0 {
		expiresAt = now.Add(tokenTTL).Format(time.RFC3339)
	}

	shared.WriteOK(w, map[string]any{
		"token":              token,
		"token_type":         "Bearer",
		"tenant_id":          req.TenantID,
		"app_id":             req.AppID,
		"expires_in_seconds": req.TTL,
		"expires_at":         expiresAt,
	}, requestID)
}

func (s *consoleService) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	var req types.CreateTenantReq
	if !parseValidatedRequest(w, r, &req, validateCreateTenantReq) {
		return
	}
	s.serveCreateTenant(w, r, &accessservice.CreateTenantRequest{
		Name: req.Name,
	})
}

func (s *consoleService) serveCreateTenant(w http.ResponseWriter, r *http.Request, payload *accessservice.CreateTenantRequest) {
	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.CreateTenant(ctx, payload)
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleGetTenantDetail(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	tenantID := pathParam(r, "id")
	if tenantID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "tenant id is required", requestID)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	tenantsResponse, tenantsErr := s.accessClient.ListTenants(ctx, &accessservice.ListTenantsRequest{})
	if tenantsErr != nil {
		writeDownstreamResult(w, requestID, tenantsResponse, tenantsErr)
		return
	}

	appsResponse, appsErr := s.accessClient.ListTenantApps(ctx, &accessservice.ListTenantAppsRequest{TenantId: tenantID})
	if appsErr != nil {
		writeDownstreamResult(w, requestID, appsResponse, appsErr)
		return
	}

	var tenantList struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(tenantsResponse.Data, &tenantList); err != nil {
		shared.WriteError(w, http.StatusInternalServerError, model.CodeInternalError, "decode tenants response failed", requestID)
		return
	}

	var selected map[string]any
	for _, item := range tenantList.Items {
		if item["tenant_id"] == tenantID {
			selected = item
			break
		}
	}
	if selected == nil {
		shared.WriteError(w, http.StatusNotFound, model.CodeNotFound, "tenant not found", requestID)
		return
	}

	var appsList map[string]any
	if err := json.Unmarshal(appsResponse.Data, &appsList); err != nil {
		shared.WriteError(w, http.StatusInternalServerError, model.CodeInternalError, "decode apps response failed", requestID)
		return
	}

	shared.WriteOK(w, map[string]any{
		"tenant": selected,
		"apps":   appsList["items"],
	}, requestID)
}

func (s *consoleService) handleCreateTenantApp(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	tenantID := pathParam(r, "id")
	if tenantID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "tenant id is required", requestID)
		return
	}

	var req types.CreateTenantAppReq
	if !parseValidatedRequest(w, r, &req, validateCreateTenantAppReq) {
		return
	}
	s.serveCreateTenantApp(w, r, &accessservice.CreateTenantAppRequest{
		TenantId:   tenantID,
		Name:       req.Name,
		DailyQuota: req.DailyQuota,
		QpsLimit:   int32(req.QpsLimit),
	})
}

func (s *consoleService) serveCreateTenantApp(w http.ResponseWriter, r *http.Request, payload *accessservice.CreateTenantAppRequest) {
	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.CreateTenantApp(ctx, payload)
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleUpdateAppStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	appID := pathParam(r, "id")
	if appID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "app id is required", requestID)
		return
	}

	var req types.UpdateAppStatusReq
	if !parseValidatedRequest(w, r, &req, validateUpdateAppStatusReq) {
		return
	}
	s.serveUpdateAppStatus(w, r, &accessservice.UpdateTenantAppStatusRequest{
		AppId:  appID,
		Status: req.Status,
	})
}

func (s *consoleService) serveUpdateAppStatus(w http.ResponseWriter, r *http.Request, payload *accessservice.UpdateTenantAppStatusRequest) {
	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.UpdateTenantAppStatus(ctx, payload)
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleUpdateAppQuota(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	appID := pathParam(r, "id")
	if appID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "app id is required", requestID)
		return
	}

	var req types.UpdateAppQuotaReq
	if !parseValidatedRequest(w, r, &req, validateUpdateAppQuotaReq) {
		return
	}
	s.serveUpdateAppQuota(w, r, &accessservice.UpdateAppQuotaRequest{
		AppId:      appID,
		DailyQuota: req.DailyQuota,
		QpsLimit:   int32(req.QpsLimit),
	})
}

func (s *consoleService) serveUpdateAppQuota(w http.ResponseWriter, r *http.Request, payload *accessservice.UpdateAppQuotaRequest) {
	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.UpdateAppQuota(ctx, payload)
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleListPolicies(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.policyClient.ListPolicies(ctx, &policyservice.ListPoliciesRequest{})
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handlePublishPolicyConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	var req types.PublishPolicyConfigReq
	if !parseValidatedRequest(w, r, &req, validatePublishPolicyConfigReq) {
		return
	}
	s.servePublishPolicyConfig(w, r, &policyservice.PublishPolicyConfigRequest{
		PolicyKey:            req.PolicyKey,
		DisplayName:          req.DisplayName,
		PublicMethod:         req.PublicMethod,
		PublicPath:           req.PublicPath,
		UpstreamMethod:       req.UpstreamMethod,
		UpstreamPath:         req.UpstreamPath,
		AllowedParams:        req.AllowedParams,
		DeniedParams:         req.DeniedParams,
		DefaultParams:        req.DefaultParams,
		ProviderCredentialId: req.ProviderCredentialID,
	})
}

func (s *consoleService) servePublishPolicyConfig(w http.ResponseWriter, r *http.Request, payload *policyservice.PublishPolicyConfigRequest) {
	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.policyClient.PublishPolicyConfig(ctx, payload)
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleBindAppPolicies(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	appID := pathParam(r, "id")
	if appID == "" {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "app id is required", requestID)
		return
	}

	var req types.BindAppPoliciesReq
	if !parseValidatedRequest(w, r, &req, validateBindAppPoliciesReq) {
		return
	}
	s.serveBindAppPolicies(w, r, &policyservice.BindAppPoliciesRequest{
		AppId:      appID,
		PolicyKeys: req.PolicyKeys,
	})
}

func (s *consoleService) serveBindAppPolicies(w http.ResponseWriter, r *http.Request, payload *policyservice.BindAppPoliciesRequest) {
	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.policyClient.BindAppPolicies(ctx, payload)
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleUsageReport(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdminAuth(w, r) {
		return
	}

	requestID := shared.EnsureRequestID(w, r)
	payload := &accessservice.QueryUsageStatsRequest{
		TenantId:  r.URL.Query().Get("tenant_id"),
		AppId:     r.URL.Query().Get("app_id"),
		PolicyKey: r.URL.Query().Get("policy_key"),
	}
	for _, target := range []struct {
		key string
		set func(int64)
	}{
		{key: "start_unix", set: func(v int64) { payload.StartUnix = v }},
		{key: "end_unix", set: func(v int64) { payload.EndUnix = v }},
	} {
		if value := r.URL.Query().Get(target.key); value != "" {
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, target.key+" must be unix seconds", requestID)
				return
			}
			target.set(parsed)
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response, err := s.accessClient.QueryUsageStats(ctx, payload)
	writeDownstreamResult(w, requestID, response, err)
}

func (s *consoleService) handleServiceHealth(w http.ResponseWriter, r *http.Request) {
	requestID := shared.EnsureRequestID(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	services := []map[string]any{
		probeServiceHealth(ctx, "access-rpc", clients.AccessRPCAddress, s.accessClient),
		probeServiceHealth(ctx, "policy-rpc", clients.PolicyRPCAddress, s.policyClient),
		probeServiceHealth(ctx, "provider-rpc", clients.ProviderRPCAddress, s.providerClient),
		probeProviderUpstreamHealth(ctx, s.providerClient),
	}

	shared.WriteOK(w, map[string]any{
		"services": services,
	}, requestID)
}

func writeDownstreamResult(w http.ResponseWriter, requestID string, response *clients.EnvelopeResponse, err error) {
	if err != nil {
		if response != nil {
			shared.WriteEnvelope(w, downstreamHTTPStatus(response.Code), response.Code, response.Message, rawJSONData(response.Data), requestID)
			return
		}
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, err.Error(), requestID)
		return
	}

	if response == nil {
		shared.WriteError(w, http.StatusBadGateway, model.CodeInternalError, "empty downstream response", requestID)
		return
	}

	shared.WriteEnvelope(w, http.StatusOK, response.Code, response.Message, rawJSONData(response.Data), requestID)
}

func downstreamHTTPStatus(code int) int {
	switch code {
	case model.CodeOK:
		return http.StatusOK
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
	case model.CodeInternalError:
		return http.StatusInternalServerError
	default:
		return http.StatusBadGateway
	}
}

func rawJSONData(data json.RawMessage) any {
	if len(data) == 0 {
		return nil
	}

	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return string(data)
	}
	return decoded
}

func (s *consoleService) requireAdminAuth(w http.ResponseWriter, r *http.Request) bool {
	requestID := shared.EnsureRequestID(w, r)
	token := shared.ExtractBearerToken(r.Header.Get("Authorization"))
	if token == "" {
		shared.WriteError(w, http.StatusUnauthorized, model.CodeUnauthorized, "missing or invalid admin token", requestID)
		return false
	}
	claims, err := shared.ParseAndValidateJWT(s.config.JWTSecret, token, time.Now())
	if err != nil || claims.Role != "platform_admin" || claims.Subject == "" || claims.Issuer != s.config.JWTIssuer {
		shared.WriteError(w, http.StatusUnauthorized, model.CodeUnauthorized, "missing or invalid admin token", requestID)
		return false
	}
	return true
}

func checkHealth(ctx context.Context, client healthClient) bool {
	response, err := client.Health(ctx)
	return err == nil && response != nil && response.Code == model.CodeOK
}

type healthClient interface {
	Health(ctx context.Context) (*clients.EnvelopeResponse, error)
}

func probeServiceHealth(ctx context.Context, name, baseURL string, client healthClient) map[string]any {
	response, err := client.Health(ctx)
	item := map[string]any{
		"name":     name,
		"kind":     "service",
		"base_url": baseURL,
		"healthy":  err == nil && response != nil && response.Code == model.CodeOK,
	}
	if response != nil {
		item["code"] = response.Code
		item["message"] = response.Message
		item["data"] = rawJSONData(response.Data)
	}
	if err != nil {
		item["error"] = err.Error()
	}
	return item
}

func probeProviderUpstreamHealth(ctx context.Context, client consoleProviderClient) map[string]any {
	response, err := client.HealthCheckProvider(ctx, &providerservice.HealthCheckProviderRequest{ProviderName: "fapi.uk"})
	item := map[string]any{
		"name":    "provider-upstream",
		"kind":    "upstream",
		"healthy": false,
	}
	if response != nil {
		item["code"] = response.Code
		item["message"] = response.Message
		data := rawJSONData(response.Data)
		item["data"] = data
		if payload, ok := data.(map[string]any); ok {
			if healthy, ok := payload["healthy"].(bool); ok {
				item["healthy"] = healthy
			}
			if reachable, ok := payload["reachable"].(bool); ok {
				item["reachable"] = reachable
			}
			if providerName, ok := payload["provider_name"].(string); ok && providerName != "" {
				item["provider_name"] = providerName
			}
			if healthState, ok := payload["health_state"].(string); ok && healthState != "" {
				item["health_state"] = healthState
			}
			if statusCode, ok := payload["status_code"]; ok {
				item["status_code"] = statusCode
			}
			if resultCode, ok := payload["result_code"].(string); ok && resultCode != "" {
				item["result_code"] = resultCode
			}
		}
	}
	if err != nil {
		item["error"] = err.Error()
	}
	return item
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
