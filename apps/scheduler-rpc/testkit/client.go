package testkit

import (
	"net"

	schedulerinternal "xsonar/apps/scheduler-rpc/internal"
	"xsonar/apps/scheduler-rpc/internal/config"
	schedulergrpcserver "xsonar/apps/scheduler-rpc/internal/server"
	schedulersvc "xsonar/apps/scheduler-rpc/internal/svc"
	"xsonar/pkg/clients"
	"xsonar/pkg/proto/schedulerpb"
	"xsonar/pkg/xlog"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

func NewClient(logger *xlog.Logger) (clients.SchedulerRPC, func(), error) {
	if logger == nil {
		logger = xlog.NewStdout("scheduler-rpc-test")
	}

	svcCtx := &schedulersvc.ServiceContext{
		Logger:  logger,
		Service: schedulerinternal.NewSchedulerServiceWithStore(config.Config{}, logger, schedulerinternal.NewFakeSchedulerStore()),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	grpcServer := grpc.NewServer()
	schedulerpb.RegisterSchedulerServiceServer(grpcServer, schedulergrpcserver.NewSchedulerServiceServer(svcCtx))

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
		_ = svcCtx.Close()
	}

	return clients.NewSchedulerRPC(zrpc.NewDirectClientConf([]string{listener.Addr().String()}, "", "")), cleanup, nil
}
