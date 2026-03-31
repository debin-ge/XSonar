package testkit

import (
	"net"

	accessinternal "xsonar/apps/access-rpc/internal"
	accessgrpcserver "xsonar/apps/access-rpc/internal/server"
	accesssvc "xsonar/apps/access-rpc/internal/svc"
	"xsonar/pkg/clients"
	"xsonar/pkg/proto/accesspb"
	"xsonar/pkg/xlog"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

func NewClient(logger *xlog.Logger) (clients.AccessRPC, func(), error) {
	if logger == nil {
		logger = xlog.NewStdout("access-rpc-test")
	}

	svcCtx := &accesssvc.ServiceContext{
		Logger: logger,
		Bridge: accessinternal.NewBridge(logger),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	grpcServer := grpc.NewServer()
	accesspb.RegisterAccessServiceServer(grpcServer, accessgrpcserver.NewAccessServiceServer(svcCtx))

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
		_ = svcCtx.Close()
	}

	return clients.NewAccessRPC(zrpc.NewDirectClientConf([]string{listener.Addr().String()}, "", "")), cleanup, nil
}
