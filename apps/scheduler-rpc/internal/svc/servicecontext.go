package svc

import (
	"xsonar/apps/scheduler-rpc/internal"
	"xsonar/apps/scheduler-rpc/internal/config"
	"xsonar/pkg/xlog"
)

type ServiceContext struct {
	Config  config.Config
	Logger  *xlog.Logger
	Service internal.SchedulerService
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger := xlog.NewStdout("scheduler-rpc")

	return &ServiceContext{
		Config:  c,
		Logger:  logger,
		Service: internal.NewSchedulerService(c, logger),
	}
}

func (s *ServiceContext) Close() error {
	if s == nil || s.Logger == nil {
		return nil
	}
	return s.Logger.Close()
}
