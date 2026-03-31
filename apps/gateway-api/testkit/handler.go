package testkit

import (
	"net/http"

	gatewayinternal "xsonar/apps/gateway-api/internal"
	"xsonar/pkg/clients"
	"xsonar/pkg/xlog"
)

func NewHandlerWithClients(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) http.Handler {
	return NewHandlerWithClientsAndMode(logger, accessClient, policyClient, providerClient, "")
}

func NewHandlerWithClientsAndMode(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC, mode string) http.Handler {
	bridge := gatewayinternal.NewBridgeWithMode(logger, accessClient, policyClient, providerClient, mode)
	mux := http.NewServeMux()
	for _, route := range []string{
		"GET /v1/users/by-ids",
		"GET /v1/users/by-id",
		"GET /v1/users/by-username",
		"GET /v1/tweets/by-ids",
		"GET /v1/tweets/timeline",
		"GET /v1/tweets/replies",
		"GET /v1/tweets/detail",
		"GET /v1/search/tweets",
		"GET /v1/search/trending",
		"GET /v1/search/trends",
	} {
		mux.HandleFunc(route, bridge.HandleProxy)
	}
	return mux
}
