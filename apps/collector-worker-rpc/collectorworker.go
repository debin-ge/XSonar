package main

import (
	"flag"
	"fmt"

	"xsonar/apps/collector-worker-rpc/internal/config"
	"xsonar/apps/collector-worker-rpc/internal/server"
	"xsonar/apps/collector-worker-rpc/internal/svc"
	"xsonar/pkg/proto/collectorworkerpb"
	"xsonar/pkg/shared"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", shared.EnvString("CONFIG", "etc/collector-worker-rpc.yaml"), "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if err := shared.ApplyEnvOverridesWithPrefixes(&c, "COMMON", "COLLECTOR_WORKER_RPC"); err != nil {
		panic(err)
	}

	ctx := svc.NewServiceContext(c)
	ctx.Start()
	defer func() { _ = ctx.Close() }()

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		collectorworkerpb.RegisterCollectorWorkerServiceServer(grpcServer, server.NewCollectorWorkerServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
