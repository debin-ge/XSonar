//lint:file-ignore SA5008 go-zero config tags use non-standard json options.
package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	ProviderBaseURL         string `json:",optional"`
	ProviderHealthPath      string `json:",default=/"`
	ProviderAPIKeyHeader    string `json:",default=apiKey"`
	ProviderTimeoutMS       int    `json:",default=8000"`
	ProviderRetryCount      int    `json:",default=1"`
	ProviderRetryIntervalMS int    `json:",default=1000"`
	ProviderEmptyDataRetry  int    `json:",default=3"`
}

type ProviderConfig struct {
	BaseURL         string
	HealthPath      string
	APIKeyHeader    string
	TimeoutMS       int
	RetryCount      int
	RetryIntervalMS int
	EmptyDataRetry  int
}

func (c Config) ToProviderConfig() ProviderConfig {
	return ProviderConfig{
		BaseURL:         c.ProviderBaseURL,
		HealthPath:      c.ProviderHealthPath,
		APIKeyHeader:    c.ProviderAPIKeyHeader,
		TimeoutMS:       c.ProviderTimeoutMS,
		RetryCount:      c.ProviderRetryCount,
		RetryIntervalMS: c.ProviderRetryIntervalMS,
		EmptyDataRetry:  c.ProviderEmptyDataRetry,
	}
}
