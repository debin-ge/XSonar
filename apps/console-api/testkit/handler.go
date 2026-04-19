package testkit

import (
	"net/http"

	consoleinternal "xsonar/apps/console-api/internal"
	consoleconfig "xsonar/apps/console-api/internal/config"
	"xsonar/pkg/clients"
	"xsonar/pkg/xlog"
)

func NewHandlerWithClients(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) http.Handler {
	return NewHandlerWithConfigAndAllClients(logger, consoleconfig.ConsoleConfig{
		JWTSecret:        "xsonar-console-dev-secret",
		JWTIssuer:        "xsonar-console",
		JWTTTLMinutes:    120,
		GatewayJWTSecret: "xsonar-gateway-dev-secret",
		GatewayJWTIssuer: "xsonar-gateway",
	}, accessClient, policyClient, providerClient)
}

func NewHandlerWithConfigAndAllClients(logger *xlog.Logger, cfg consoleconfig.ConsoleConfig, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) http.Handler {
	bridge := consoleinternal.NewBridge(cfg, logger, accessClient, policyClient, providerClient)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/v1/auth/login", bridge.HandleLogin)
	mux.HandleFunc("POST /admin/v1/gateway/token", bridge.HandleIssueGatewayToken)
	mux.HandleFunc("GET /admin/v1/tenants", bridge.HandleListTenants)
	mux.HandleFunc("POST /admin/v1/tenants", bridge.HandleCreateTenant)
	mux.HandleFunc("GET /admin/v1/tenants/{id}", bridge.HandleGetTenantDetail)
	mux.HandleFunc("POST /admin/v1/tenants/{id}/apps", bridge.HandleCreateTenantApp)
	mux.HandleFunc("PUT /admin/v1/apps/{id}/status", bridge.HandleUpdateAppStatus)
	mux.HandleFunc("PUT /admin/v1/apps/{id}/quota", bridge.HandleUpdateAppQuota)
	mux.HandleFunc("GET /admin/v1/policies", bridge.HandleListPolicies)
	mux.HandleFunc("POST /admin/v1/policies", bridge.HandlePublishPolicyConfig)
	mux.HandleFunc("PUT /admin/v1/apps/{id}/policies", bridge.HandleBindAppPolicies)
	mux.HandleFunc("GET /admin/v1/reports/usage", bridge.HandleUsageReport)
	mux.HandleFunc("GET /admin/v1/health/services", bridge.HandleServiceHealth)
	return mux
}
