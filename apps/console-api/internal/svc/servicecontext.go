// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	consoleinternal "xsonar/apps/console-api/internal"
	"xsonar/apps/console-api/internal/config"
	"xsonar/pkg/clients"
	"xsonar/pkg/shared"
	"xsonar/pkg/xlog"
)

type ServiceContext struct {
	Config config.Config
	Logger *xlog.Logger
	Bridge *consoleinternal.Bridge
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger := xlog.NewStdout("console-api")

	return &ServiceContext{
		Config: c,
		Logger: logger,
		Bridge: consoleinternal.NewBridge(
			consoleSharedConfig(c),
			logger,
			clients.NewAccessRPC(c.AccessRPC),
			clients.NewPolicyRPC(c.PolicyRPC),
			clients.NewProviderRPC(c.ProviderRPC),
		),
	}
}

func (s *ServiceContext) Close() error {
	if s == nil || s.Logger == nil {
		return nil
	}
	return s.Logger.Close()
}

func consoleSharedConfig(c config.Config) shared.Config {
	return shared.Config{
		ServiceName:      c.Name,
		JWTSecret:        c.JWTSecret,
		JWTIssuer:        c.JWTIssuer,
		JWTTTLMinutes:    c.JWTTTLMinutes,
		GatewayJWTSecret: c.GatewayJWTSecret,
		GatewayJWTIssuer: c.GatewayJWTIssuer,
	}
}
