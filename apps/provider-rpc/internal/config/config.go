package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	ProviderBaseURL      string `json:",optional"`
	ProviderHealthPath   string `json:",default=/"`
	ProviderAPIKeyHeader string `json:",default=apiKey"`
	ProviderTimeoutMS    int    `json:",default=8000"`
	ProviderRetryCount   int    `json:",default=1"`
}

type ProviderConfig struct {
	BaseURL      string
	HealthPath   string
	APIKeyHeader string
	TimeoutMS    int
	RetryCount   int
}

func (c Config) ToProviderConfig() ProviderConfig {
	return ProviderConfig{
		BaseURL:      c.ProviderBaseURL,
		HealthPath:   c.ProviderHealthPath,
		APIKeyHeader: c.ProviderAPIKeyHeader,
		TimeoutMS:    c.ProviderTimeoutMS,
		RetryCount:   c.ProviderRetryCount,
	}
}
