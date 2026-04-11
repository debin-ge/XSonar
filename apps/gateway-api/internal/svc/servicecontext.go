// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"time"

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
	accessClient := clients.NewAccessRPC(c.AccessRPC)
	policyClient := clients.NewPolicyRPC(c.PolicyRPC)
	providerClient := clients.NewProviderRPC(c.ProviderRPC)
	schedulerClient := clients.NewSchedulerRPC(c.SchedulerRPC)

	return &ServiceContext{
		Config: c,
		Logger: logger,
		Bridge: gatewayinternal.NewBridgeWithModeAndAsyncUsageStats(
			logger,
			accessClient,
			policyClient,
			providerClient,
			schedulerClient,
			c.JWTSecret,
			c.JWTIssuer,
			c.Mode,
			c.UsageStatQueueSize,
			c.UsageStatWorkers,
			time.Duration(c.UsageStatTimeoutMS)*time.Millisecond,
		),
	}
}

func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.Bridge != nil {
		s.Bridge.Close()
	}
	if s.Logger == nil {
		return nil
	}
	return s.Logger.Close()
}
