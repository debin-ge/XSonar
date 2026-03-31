package svc

import (
	providerinternal "xsonar/apps/provider-rpc/internal"
	"xsonar/apps/provider-rpc/internal/config"
	"xsonar/pkg/shared"
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
		Bridge: providerinternal.NewBridge(providerSharedConfig(c), nil, logger),
	}
}

func (s *ServiceContext) Close() error {
	if s == nil || s.Logger == nil {
		return nil
	}
	return s.Logger.Close()
}

func providerSharedConfig(c config.Config) shared.Config {
	cfg := providerinternal.ProviderDefaults()
	if c.ProviderBaseURL != "" {
		cfg.ProviderBaseURL = c.ProviderBaseURL
	}
	if c.ProviderHealthPath != "" {
		cfg.ProviderHealthPath = c.ProviderHealthPath
	}
	if c.ProviderAPIKeyHeader != "" {
		cfg.ProviderAPIKeyHeader = c.ProviderAPIKeyHeader
	}
	if c.ProviderTimeoutMS > 0 {
		cfg.ProviderTimeoutMS = c.ProviderTimeoutMS
	}
	if c.ProviderRetryCount >= 0 {
		cfg.ProviderRetryCount = c.ProviderRetryCount
	}
	return cfg
}
