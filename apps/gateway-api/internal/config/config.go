// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package config

import (
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	AccessRPC   zrpc.RpcClientConf
	PolicyRPC   zrpc.RpcClientConf
	ProviderRPC zrpc.RpcClientConf
}
