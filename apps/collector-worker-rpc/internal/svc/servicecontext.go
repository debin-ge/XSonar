package svc

import (
	"context"

	collectorworkerinternal "xsonar/apps/collector-worker-rpc/internal"
	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/pkg/clients"
	"xsonar/pkg/xlog"
)

type ServiceContext struct {
	Config      config.Config
	Logger      *xlog.Logger
	PolicyRPC   clients.PolicyRPC
	ProviderRPC clients.ProviderRPC
	Service     collectorworkerinternal.CollectorWorkerService
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger := xlog.NewStdout("collector-worker-rpc")
	policyClient := clients.NewPolicyRPC(c.PolicyRPC)
	providerClient := clients.NewProviderRPC(c.ProviderRPC)

	return &ServiceContext{
		Config:      c,
		Logger:      logger,
		PolicyRPC:   policyClient,
		ProviderRPC: providerClient,
		Service:     collectorworkerinternal.NewCollectorWorkerService(c, logger, policyClient, providerClient),
	}
}

func (s *ServiceContext) Start() {
	if s == nil || s.Service == nil {
		return
	}
	s.Service.Start(context.Background())
}

func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.Service != nil {
		if err := s.Service.Close(context.Background()); err != nil {
			return err
		}
	}
	if s.Logger != nil {
		return s.Logger.Close()
	}
	return nil
}
