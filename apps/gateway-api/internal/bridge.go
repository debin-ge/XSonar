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
	return NewBridgeWithMode(logger, accessClient, policyClient, providerClient, "")
}

func NewBridgeWithMode(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC, mode string) *Bridge {
	return &Bridge{
		svc: newGatewayServiceWithMode(logger, accessClient, policyClient, providerClient, mode),
	}
}

func NewBridgeWithModeAndAsyncUsageStats(logger *xlog.Logger, accessClient clients.AccessRPC, policyClient clients.PolicyRPC, providerClient clients.ProviderRPC, mode string, queueSize, workers int, timeout time.Duration) *Bridge {
	return &Bridge{
		svc: newGatewayServiceWithModeAndUsageStats(
			logger,
			accessClient,
			policyClient,
			providerClient,
			newAsyncUsageStatRecorder(logger, accessClient, queueSize, workers, timeout),
			mode,
		),
	}
}

func (b *Bridge) HandleProxy(w http.ResponseWriter, r *http.Request) {
	b.svc.handleProxy(w, r)
}

func (b *Bridge) Close() {
	if b == nil || b.svc == nil {
		return
	}
	b.svc.Close()
}
