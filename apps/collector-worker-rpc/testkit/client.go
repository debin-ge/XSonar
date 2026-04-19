package testkit

import (
	"net"

	collectorworkerinternal "xsonar/apps/collector-worker-rpc/internal"
	collectorworkerconfig "xsonar/apps/collector-worker-rpc/internal/config"
	collectorworkergrpcserver "xsonar/apps/collector-worker-rpc/internal/server"
	collectorworkersvc "xsonar/apps/collector-worker-rpc/internal/svc"
	"xsonar/pkg/clients"
	"xsonar/pkg/proto/collectorworkerpb"
	"xsonar/pkg/xlog"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

func NewClient(logger *xlog.Logger) (clients.CollectorWorkerRPC, func(), error) {
	if logger == nil {
		logger = xlog.NewStdout("collector-worker-rpc-test")
	}

	cfg := collectorworkerconfig.Config{
		WorkerID:      "collector-worker-test",
		QueueStream:   "collector:runs",
		QueueGroup:    "collector-workers",
		OutputRootDir: "runtime/collector",
	}

	svcCtx := &collectorworkersvc.ServiceContext{
		Config:  cfg,
		Logger:  logger,
		Service: collectorworkerinternal.NewCollectorWorkerService(cfg, logger, nil, nil),
	}
	svcCtx.Start()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	grpcServer := grpc.NewServer()
	collectorworkerpb.RegisterCollectorWorkerServiceServer(grpcServer, collectorworkergrpcserver.NewCollectorWorkerServiceServer(svcCtx))

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
		_ = svcCtx.Close()
	}

	return clients.NewCollectorWorkerRPC(zrpc.NewDirectClientConf([]string{listener.Addr().String()}, "", "")), cleanup, nil
}
