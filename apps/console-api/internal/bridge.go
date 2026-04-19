package internal

import (
	"net/http"
	"reflect"
	"strings"

	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/rest/pathvar"
	"xsonar/apps/access-rpc/accessservice"
	"xsonar/apps/console-api/internal/config"
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

func NewBridge(cfg config.ConsoleConfig, logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) *Bridge {
	return &Bridge{
		svc: newConsoleServiceWithConfigAndAllClients(cfg, logger, accessClient, policyClient, providerClient),
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

func (b *Bridge) HandleIssueGatewayToken(w http.ResponseWriter, r *http.Request) {
	b.svc.handleIssueGatewayToken(w, r)
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
	qpsLimit, err := shared.Int32FromInt(req.QpsLimit)
	if err != nil {
		requestID := shared.EnsureRequestID(w, r)
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "qps_limit is out of range", requestID)
		return
	}
	b.svc.serveCreateTenantApp(w, r, &accessservice.CreateTenantAppRequest{
		TenantId:   tenantID,
		Name:       req.Name,
		DailyQuota: req.DailyQuota,
		QpsLimit:   qpsLimit,
	})
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
	qpsLimit, err := shared.Int32FromInt(req.QpsLimit)
	if err != nil {
		requestID := shared.EnsureRequestID(w, r)
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, "qps_limit is out of range", requestID)
		return
	}
	b.svc.serveUpdateAppQuota(w, r, &accessservice.UpdateAppQuotaRequest{
		AppId:      appID,
		DailyQuota: req.DailyQuota,
		QpsLimit:   qpsLimit,
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
	if err := httpx.Parse(withPathVars(r, req), req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, err.Error(), requestID)
		return false
	}
	if err := validate(req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, model.CodeInvalidRequest, err.Error(), requestID)
		return false
	}
	return true
}

func withPathVars[T any](r *http.Request, req *T) *http.Request {
	if r == nil || req == nil {
		return r
	}

	typ := reflect.TypeOf(req)
	if typ.Kind() != reflect.Pointer {
		return r
	}
	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		return r
	}

	vars := clonePathVars(pathvar.Vars(r))
	updated := false
	for index := 0; index < typ.NumField(); index++ {
		pathKey := parsePathTag(typ.Field(index).Tag.Get("path"))
		if pathKey == "" {
			continue
		}
		if strings.TrimSpace(vars[pathKey]) != "" {
			continue
		}
		if value := strings.TrimSpace(r.PathValue(pathKey)); value != "" {
			if vars == nil {
				vars = make(map[string]string)
			}
			vars[pathKey] = value
			updated = true
		}
	}
	if !updated {
		return r
	}
	return pathvar.WithVars(r, vars)
}

func clonePathVars(vars map[string]string) map[string]string {
	if len(vars) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(vars))
	for key, value := range vars {
		cloned[key] = value
	}
	return cloned
}

func parsePathTag(tag string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ""
	}
	parts := strings.SplitN(tag, ",", 2)
	return strings.TrimSpace(parts[0])
}
