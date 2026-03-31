package main

import (
	"flag"
	"fmt"

	"xsonar/apps/provider-rpc/internal/config"
	"xsonar/apps/provider-rpc/internal/server"
	"xsonar/apps/provider-rpc/internal/svc"
	"xsonar/pkg/proto/providerpb"
	"xsonar/pkg/shared"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", shared.EnvString("CONFIG", "etc/provider-rpc.yaml"), "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if err := shared.ApplyEnvOverridesWithPrefixes(&c, "COMMON", "PROVIDER_RPC"); err != nil {
		panic(err)
	}
	ctx := svc.NewServiceContext(c)
	defer func() { _ = ctx.Close() }()

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		providerpb.RegisterProviderServiceServer(grpcServer, server.NewProviderServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
