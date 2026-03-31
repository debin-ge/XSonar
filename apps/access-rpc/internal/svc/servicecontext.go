package svc

import (
	"context"

	accessinternal "xsonar/apps/access-rpc/internal"
	"xsonar/apps/access-rpc/internal/config"
	"xsonar/pkg/xlog"
)

type ServiceContext struct {
	Config config.Config
	Logger *xlog.Logger
	Bridge *accessinternal.Bridge
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger := xlog.NewStdout("access-rpc")

	return &ServiceContext{
		Config: c,
		Logger: logger,
		Bridge: accessinternal.NewBridge(logger),
	}
}

func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.Bridge != nil {
		if err := s.Bridge.Close(context.Background()); err != nil {
			return err
		}
	}
	if s.Logger != nil {
		return s.Logger.Close()
	}
	return nil
}
