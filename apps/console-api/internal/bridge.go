package internal

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"
	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/console-api/internal/types"
	"xsonar/apps/policy-rpc/policyservice"
	"xsonar/pkg/clients"
	"xsonar/pkg/model"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type Bridge struct {
	svc *consoleService
}

func NewBridge(config shared.Config, logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) *Bridge {
	return &Bridge{
		svc: newConsoleServiceWithConfigAndAllClients(config, logger, accessClient, policyClient, providerClient),
	}
}

func (b *Bridge) HandleLogin(w http.ResponseWriter, r *http.Request) {
	var req types.LoginReq
	if !parseValidatedRequest(w, r, &req, validateLoginReq) {
		return
	}
	b.svc.serveLogin(w, r, &accessservice.AuthenticateConsoleUserRequest{
		Username: req.Username,
		Password: req.Password,
	})
}

func (b *Bridge) HandleListTenants(w http.ResponseWriter, r *http.Request) {
	b.svc.handleListTenants(w, r)
}

func (b *Bridge) HandleCreateTenant(w http.ResponseWriter, r *http.Request) {
	if !b.svc.requireAdminAuth(w, r) {
		return
	}
	var req types.CreateTenantReq
	if !parseValidatedRequest(w, r, &req, validateCreateTenantReq) {
		return
	}
	b.svc.serveCreateTenant(w, r, &accessservice.CreateTenantRequest{
		Name: req.Name,
	})
}

func (b *Bridge) HandleGetTenantDetail(w http.ResponseWriter, r *http.Request) {
	b.svc.handleGetTenantDetail(w, r)
}

func (b *Bridge) HandleCreateTenantApp(w http.ResponseWriter, r *http.Request) {
	if !b.svc.requireAdminAuth(w, r) {
		return
	}
	tenantID := pathParam(r, "id")
	if tenantID == "" {
		requestID := shared.EnsureRequestID(w, r)
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "tenant id is required", requestID)
		return
	}
	var req types.CreateTenantAppReq
	if !parseValidatedRequest(w, r, &req, validateCreateTenantAppReq) {
		return
	}
	b.svc.serveCreateTenantApp(w, r, &accessservice.CreateTenantAppRequest{
		TenantId:   tenantID,
		Name:       req.Name,
		DailyQuota: req.DailyQuota,
		QpsLimit:   int32(req.QpsLimit),
	})
}

func (b *Bridge) HandleRotateAppSecret(w http.ResponseWriter, r *http.Request) {
	b.svc.handleRotateAppSecret(w, r)
}

func (b *Bridge) HandleUpdateAppStatus(w http.ResponseWriter, r *http.Request) {
	if !b.svc.requireAdminAuth(w, r) {
		return
	}
	appID := pathParam(r, "id")
	if appID == "" {
		requestID := shared.EnsureRequestID(w, r)
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "app id is required", requestID)
		return
	}
	var req types.UpdateAppStatusReq
	if !parseValidatedRequest(w, r, &req, validateUpdateAppStatusReq) {
		return
	}
	b.svc.serveUpdateAppStatus(w, r, &accessservice.UpdateTenantAppStatusRequest{
		AppId:  appID,
		Status: req.Status,
	})
}

func (b *Bridge) HandleUpdateAppQuota(w http.ResponseWriter, r *http.Request) {
	if !b.svc.requireAdminAuth(w, r) {
		return
	}
	appID := pathParam(r, "id")
	if appID == "" {
		requestID := shared.EnsureRequestID(w, r)
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "app id is required", requestID)
		return
	}
	var req types.UpdateAppQuotaReq
	if !parseValidatedRequest(w, r, &req, validateUpdateAppQuotaReq) {
		return
	}
	b.svc.serveUpdateAppQuota(w, r, &accessservice.UpdateAppQuotaRequest{
		AppId:      appID,
		DailyQuota: req.DailyQuota,
		QpsLimit:   int32(req.QpsLimit),
	})
}

func (b *Bridge) HandleListPolicies(w http.ResponseWriter, r *http.Request) {
	b.svc.handleListPolicies(w, r)
}

func (b *Bridge) HandlePublishPolicyConfig(w http.ResponseWriter, r *http.Request) {
	if !b.svc.requireAdminAuth(w, r) {
		return
	}
	var req types.PublishPolicyConfigReq
	if !parseValidatedRequest(w, r, &req, validatePublishPolicyConfigReq) {
		return
	}
	b.svc.servePublishPolicyConfig(w, r, &policyservice.PublishPolicyConfigRequest{
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

func (b *Bridge) HandleBindAppPolicies(w http.ResponseWriter, r *http.Request) {
	if !b.svc.requireAdminAuth(w, r) {
		return
	}
	appID := pathParam(r, "id")
	if appID == "" {
		requestID := shared.EnsureRequestID(w, r)
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "app id is required", requestID)
		return
	}
	var req types.BindAppPoliciesReq
	if !parseValidatedRequest(w, r, &req, validateBindAppPoliciesReq) {
		return
	}
	b.svc.serveBindAppPolicies(w, r, &policyservice.BindAppPoliciesRequest{
		AppId:      appID,
		PolicyKeys: req.PolicyKeys,
	})
}

func (b *Bridge) HandleUsageReport(w http.ResponseWriter, r *http.Request) {
	b.svc.handleUsageReport(w, r)
}

func (b *Bridge) HandleServiceHealth(w http.ResponseWriter, r *http.Request) {
	b.svc.handleServiceHealth(w, r)
}

func parseValidatedRequest[T any](w http.ResponseWriter, r *http.Request, req *T, validate func(*T) error) bool {
	requestID := shared.EnsureRequestID(w, r)
	if err := httpx.Parse(r, req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, err.Error(), requestID)
		return false
	}
	if err := validate(req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, err.Error(), requestID)
		return false
	}
	return true
}
