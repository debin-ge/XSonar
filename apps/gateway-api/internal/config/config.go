// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

//lint:file-ignore SA5008 go-zero config tags use non-standard json options.
package config

import (
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	AccessRPC    zrpc.RpcClientConf
	PolicyRPC    zrpc.RpcClientConf
	ProviderRPC  zrpc.RpcClientConf
	SchedulerRPC zrpc.RpcClientConf
	JWTSecret    string `json:",default=xsonar-gateway-dev-secret"`
	JWTIssuer    string `json:",default=xsonar-gateway"`

	UsageStatQueueSize int `json:",default=1024"`
	UsageStatWorkers   int `json:",default=2"`
	UsageStatTimeoutMS int `json:",default=500"`
}
