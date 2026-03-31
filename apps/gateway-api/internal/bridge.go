package internal

import (
	"net/http"

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

func (b *Bridge) HandleProxy(w http.ResponseWriter, r *http.Request) {
	b.svc.handleProxy(w, r)
}
