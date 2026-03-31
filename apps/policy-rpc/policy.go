package main

import (
	"flag"
	"fmt"

	"xsonar/apps/policy-rpc/internal/config"
	"xsonar/apps/policy-rpc/internal/server"
	"xsonar/apps/policy-rpc/internal/svc"
	"xsonar/pkg/proto/policypb"
	"xsonar/pkg/shared"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", shared.EnvString("CONFIG", "etc/policy-rpc.yaml"), "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if err := shared.ApplyEnvOverridesWithPrefixes(&c, "COMMON", "POLICY_RPC"); err != nil {
		panic(err)
	}
	ctx := svc.NewServiceContext(c)
	defer func() { _ = ctx.Close() }()

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		policypb.RegisterPolicyServiceServer(grpcServer, server.NewPolicyServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
