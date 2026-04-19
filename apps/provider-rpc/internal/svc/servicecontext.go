package svc

import (
	providerinternal "xsonar/apps/provider-rpc/internal"
	"xsonar/apps/provider-rpc/internal/config"
	"xsonar/pkg/xlog"
)

type ServiceContext struct {
	Config config.Config
	Logger *xlog.Logger
	Bridge *providerinternal.Bridge
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger := xlog.NewStdout("provider-rpc")

	return &ServiceContext{
		Config: c,
		Logger: logger,
		Bridge: providerinternal.NewBridge(c.ToProviderConfig(), nil, logger),
	}
}

func (s *ServiceContext) Close() error {
	if s == nil || s.Logger == nil {
		return nil
	}
	return s.Logger.Close()
}
