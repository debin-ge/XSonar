package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	DispatchScanIntervalMS   int `json:",default=1000"`
	QueueBacklogSoftLimit    int `json:",default=5000"`
	QueueBacklogHardLimit    int `json:",default=20000"`
	QueueBacklogMaxLagMS     int `json:",default=300000"`
	LeaderLockTTLMS          int `json:",default=15000"`
	ListTaskRunsDefaultLimit int `json:",default=50"`
}
