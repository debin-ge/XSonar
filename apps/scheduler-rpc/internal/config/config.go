package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	DispatchScanIntervalMS   int `json:",default=1000"`
	QueueBacklogSoftLimit    int `json:",default=100"`
	QueueBacklogHardLimit    int `json:",default=1000"`
	QueueBacklogMaxLagMS     int `json:",default=60000"`
	LeaderLockTTLMS          int `json:",default=30000"`
	ListTaskRunsDefaultLimit int `json:",default=20"`
}
