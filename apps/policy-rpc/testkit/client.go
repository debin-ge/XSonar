package testkit

import (
	"net"

	policyinternal "xsonar/apps/policy-rpc/internal"
	policygrpcserver "xsonar/apps/policy-rpc/internal/server"
	policysvc "xsonar/apps/policy-rpc/internal/svc"
	"xsonar/pkg/clients"
	"xsonar/pkg/proto/policypb"
	"xsonar/pkg/xlog"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

func NewClient(logger *xlog.Logger) (clients.PolicyRPC, func(), error) {
	if logger == nil {
		logger = xlog.NewStdout("policy-rpc-test")
	}

	svcCtx := &policysvc.ServiceContext{
		Logger: logger,
		Bridge: policyinternal.NewBridge(logger),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	grpcServer := grpc.NewServer()
	policypb.RegisterPolicyServiceServer(grpcServer, policygrpcserver.NewPolicyServiceServer(svcCtx))

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
		_ = svcCtx.Close()
	}

	return clients.NewPolicyRPC(zrpc.NewDirectClientConf([]string{listener.Addr().String()}, "", "")), cleanup, nil
}
