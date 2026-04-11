package internal

import (
	"net/http"
	"time"

	"xsonar/pkg/clients"
	"xsonar/pkg/xlog"
)

type Bridge struct {
	svc *gatewayService
}

func NewBridge(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC) *Bridge {
	return NewBridgeWithModeAndAdmin(logger, accessClient, policyClient, providerClient, nil, "", "", "")
}

func NewBridgeWithMode(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC, mode string) *Bridge {
	return NewBridgeWithModeAndAdmin(logger, accessClient, policyClient, providerClient, nil, "", "", mode)
}

func NewBridgeWithModeAndAdmin(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC, schedulerClient clients.SchedulerRPC, jwtSecret, jwtIssuer, mode string) *Bridge {
	return &Bridge{
		svc: newGatewayServiceWithModeAndAdmin(logger, accessClient, policyClient, providerClient, schedulerClient, jwtSecret, jwtIssuer, mode),
	}
}

func NewBridgeWithModeAndAsyncUsageStats(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC, schedulerClient clients.SchedulerRPC, jwtSecret, jwtIssuer, mode string, queueSize, workers int, timeout time.Duration) *Bridge {
	return &Bridge{
		svc: newGatewayServiceWithModeAndUsageStats(
			logger,
			accessClient,
			policyClient,
			providerClient,
			schedulerClient,
			jwtSecret,
			jwtIssuer,
			newAsyncUsageStatRecorder(logger, accessClient, queueSize, workers, timeout),
			mode,
		),
	}
}

func (b *Bridge) HandleProxy(w http.ResponseWriter, r *http.Request) {
	b.svc.handleProxy(w, r)
}

func (b *Bridge) HandleCreateCollectorTask(w http.ResponseWriter, r *http.Request) {
	b.svc.handleCreateCollectorTask(w, r)
}

func (b *Bridge) HandleGetCollectorTask(w http.ResponseWriter, r *http.Request) {
	b.svc.handleGetCollectorTask(w, r)
}

func (b *Bridge) HandleListCollectorTaskRuns(w http.ResponseWriter, r *http.Request) {
	b.svc.handleListCollectorTaskRuns(w, r)
}

func (b *Bridge) Close() {
	if b == nil || b.svc == nil {
		return
	}
	b.svc.Close()
}
