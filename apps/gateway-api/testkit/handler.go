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
		"GET /v1/communities",
		"GET /v1/lists",
		"GET /v1/search/box",
		"GET /v1/search/entertainment",
		"GET /v1/search/explore",
		"GET /v1/search/news",
		"GET /v1/search/sports",
		"GET /v1/users/by-ids",
		"GET /v1/users/by-id",
		"GET /v1/users/by-username",
		"GET /v1/users/account-analytics",
		"GET /v1/users/articles-tweets",
		"GET /v1/users/followers",
		"GET /v1/users/followings",
		"GET /v1/users/highlights",
		"GET /v1/users/likes",
		"GET /v1/users/mentions-timeline",
		"GET /v1/users/username-changes",
		"GET /v1/tweets/by-ids",
		"GET /v1/tweets/brief",
		"GET /v1/tweets/timeline",
		"GET /v1/tweets/replies",
		"GET /v1/tweets/detail",
		"GET /v1/tweets/favoriters",
		"GET /v1/tweets/quotes",
		"GET /v1/tweets/retweeters",
		"GET /v1/search/tweets",
		"GET /v1/search/trending",
		"GET /v1/search/trends",
	} {
		mux.HandleFunc(route, bridge.HandleProxy)
	}
	return mux
}
