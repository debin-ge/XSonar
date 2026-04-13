package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	PolicyRPC   zrpc.RpcClientConf
	ProviderRPC zrpc.RpcClientConf

	WorkerID                string `json:",optional"`
	QueueStream             string `json:",default=collector:runs"`
	QueueGroup              string `json:",default=collector-workers"`
	QueueBlockMS            int    `json:",default=2000"`
	RunLeaseTTLMS           int    `json:",default=120000"`
	LeaseRenewIntervalMS    int    `json:",default=30000"`
	PeriodicRunMaxPages     int    `json:",default=20"`
	NDJSONFlushEveryRecords int    `json:",default=100"`
	NDJSONFsyncOnClose      bool   `json:",default=true"`
	OutputRootDir           string `json:",default=runtime/collector"`
}
