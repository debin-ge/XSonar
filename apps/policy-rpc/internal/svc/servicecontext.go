package svc

import (
	"context"

	policyinternal "xsonar/apps/policy-rpc/internal"
	"xsonar/apps/policy-rpc/internal/config"
	"xsonar/pkg/xlog"
)

type ServiceContext struct {
	Config config.Config
	Logger *xlog.Logger
	Bridge *policyinternal.Bridge
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger := xlog.NewStdout("policy-rpc")

	return &ServiceContext{
		Config: c,
		Logger: logger,
		Bridge: policyinternal.NewBridge(logger),
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
