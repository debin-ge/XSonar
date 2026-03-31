// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package main

import (
	"flag"
	"fmt"

	"xsonar/apps/console-api/internal/config"
	"xsonar/apps/console-api/internal/handler"
	"xsonar/apps/console-api/internal/svc"
	"xsonar/pkg/shared"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", shared.EnvString("CONFIG", "etc/console-api.yaml"), "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if err := shared.ApplyEnvOverridesWithPrefixes(&c, "COMMON", "CONSOLE_API"); err != nil {
		panic(err)
	}

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)
	defer func() { _ = ctx.Close() }()
	shared.AddHealthzRoute(server, "console-api")
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
