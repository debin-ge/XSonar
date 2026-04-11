package testkit

import (
	"net/http"

	gatewayinternal "xsonar/apps/gateway-api/internal"
	"xsonar/pkg/clients"
	"xsonar/pkg/xlog"
)

func NewHandlerWithClients(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) http.Handler {
	return NewHandlerWithClientsAndMode(logger, accessClient, policyClient, providerClient, nil, "", "", "")
}

func NewHandlerWithClientsAndMode(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC, schedulerClient clients.SchedulerRPC, jwtSecret, jwtIssuer, mode string) http.Handler {
	bridge := gatewayinternal.NewBridgeWithModeAndAdmin(logger, accessClient, policyClient, providerClient, schedulerClient, jwtSecret, jwtIssuer, mode)
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
		"POST /admin/v1/collector/tasks",
		"GET /admin/v1/collector/tasks/:id",
		"GET /admin/v1/collector/tasks/:id/runs",
	} {
		switch route {
		case "POST /admin/v1/collector/tasks":
			mux.HandleFunc(route, bridge.HandleCreateCollectorTask)
		case "GET /admin/v1/collector/tasks/:id":
			mux.HandleFunc(route, bridge.HandleGetCollectorTask)
		case "GET /admin/v1/collector/tasks/:id/runs":
			mux.HandleFunc(route, bridge.HandleListCollectorTaskRuns)
		default:
			mux.HandleFunc(route, bridge.HandleProxy)
		}
	}
	return mux
}
