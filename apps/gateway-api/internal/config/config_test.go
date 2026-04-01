package config

import (
	"testing"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestConfigLoadsUsageStatDefaultsWhenFieldsAreMissing(t *testing.T) {
	var cfg Config

	err := conf.LoadFromYamlBytes([]byte(`
Name: gateway-api
Host: 0.0.0.0
Port: 8080
Mode: dev
Timeout: 10000
AccessRPC:
  Endpoints:
    - access-rpc:9001
  Timeout: 5000
PolicyRPC:
  Endpoints:
    - policy-rpc:9002
  Timeout: 5000
ProviderRPC:
  Endpoints:
    - provider-rpc:9003
  Timeout: 10000
`), &cfg)
	if err != nil {
		t.Fatalf("expected config to load without usage stat fields, got error: %v", err)
	}

	if cfg.UsageStatQueueSize != 1024 {
		t.Fatalf("expected UsageStatQueueSize default 1024, got %d", cfg.UsageStatQueueSize)
	}
	if cfg.UsageStatWorkers != 2 {
		t.Fatalf("expected UsageStatWorkers default 2, got %d", cfg.UsageStatWorkers)
	}
	if cfg.UsageStatTimeoutMS != 500 {
		t.Fatalf("expected UsageStatTimeoutMS default 500, got %d", cfg.UsageStatTimeoutMS)
	}
}
