// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	gatewayinternal "xsonar/apps/gateway-api/internal"
	"xsonar/apps/gateway-api/internal/config"
	"xsonar/pkg/clients"
	"xsonar/pkg/xlog"
)

type ServiceContext struct {
	Config config.Config
	Logger *xlog.Logger
	Bridge *gatewayinternal.Bridge
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger := xlog.NewStdout("gateway-api")

	return &ServiceContext{
		Config: c,
		Logger: logger,
		Bridge: gatewayinternal.NewBridgeWithMode(
			logger,
			clients.NewAccessRPC(c.AccessRPC),
			clients.NewPolicyRPC(c.PolicyRPC),
			clients.NewProviderRPC(c.ProviderRPC),
			c.Mode,
		),
	}
}

func (s *ServiceContext) Close() error {
	if s == nil || s.Logger == nil {
		return nil
	}
	return s.Logger.Close()
}
