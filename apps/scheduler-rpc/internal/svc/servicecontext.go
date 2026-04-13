package svc

import (
	"context"

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
