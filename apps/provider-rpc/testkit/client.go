package testkit

import (
	"net"
	"net/http"

	providerinternal "xsonar/apps/provider-rpc/internal"
	providerconfig "xsonar/apps/provider-rpc/internal/config"
	providergrpcserver "xsonar/apps/provider-rpc/internal/server"
	providersvc "xsonar/apps/provider-rpc/internal/svc"
	"xsonar/pkg/clients"
	"xsonar/pkg/proto/providerpb"
	"xsonar/pkg/xlog"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

type ProviderConfig = providerconfig.ProviderConfig

func NewClient(logger *xlog.Logger) (clients.ProviderRPC, func(), error) {
	return NewClientWithConfigAndHTTPClient(logger, providerconfig.ProviderConfig{}, nil)
}

func NewClientWithConfig(logger *xlog.Logger, cfg ProviderConfig) (clients.ProviderRPC, func(), error) {
	return NewClientWithConfigAndHTTPClient(logger, cfg, nil)
}

func NewClientWithConfigAndHTTPClient(logger *xlog.Logger, cfg ProviderConfig, client *http.Client) (clients.ProviderRPC, func(), error) {
	if logger == nil {
		logger = xlog.NewStdout("provider-rpc-test")
	}

	svcCtx := &providersvc.ServiceContext{
		Config: providerconfig.Config{
			ProviderBaseURL:      cfg.BaseURL,
			ProviderHealthPath:   cfg.HealthPath,
			ProviderAPIKeyHeader: cfg.APIKeyHeader,
			ProviderTimeoutMS:    cfg.TimeoutMS,
			ProviderRetryCount:   cfg.RetryCount,
		},
		Logger: logger,
		Bridge: providerinternal.NewBridge(cfg, client, logger),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	grpcServer := grpc.NewServer()
	providerpb.RegisterProviderServiceServer(grpcServer, providergrpcserver.NewProviderServiceServer(svcCtx))

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
		_ = svcCtx.Close()
	}

	return clients.NewProviderRPC(zrpc.NewDirectClientConf([]string{listener.Addr().String()}, "", "")), cleanup, nil
}
