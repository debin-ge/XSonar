package shared

import (
	"testing"
	"time"
)

func TestApplyEnvOverrides(t *testing.T) {
	type Embedded struct {
		Host string
		Port int
	}
	type client struct {
		Endpoints []string
		Timeout   int64
		NonBlock  bool
	}
	type config struct {
		Embedded
		AccessRPC      client
		JWTTTLMinutes  int
		KeepaliveTime  time.Duration
		ProviderAPIKey string
	}

	t.Setenv("GATEWAY_API_HOST", "127.0.0.1")
	t.Setenv("GATEWAY_API_PORT", "9090")
	t.Setenv("GATEWAY_API_ACCESS_RPC_ENDPOINTS", "rpc-a:1,rpc-b:2")
	t.Setenv("GATEWAY_API_ACCESS_RPC_TIMEOUT", "7000")
	t.Setenv("GATEWAY_API_ACCESS_RPC_NON_BLOCK", "false")
	t.Setenv("GATEWAY_API_JWT_TTL_MINUTES", "240")
	t.Setenv("GATEWAY_API_KEEPALIVE_TIME", "30s")
	t.Setenv("GATEWAY_API_PROVIDER_API_KEY", "provider-key-1")

	cfg := config{
		Embedded: Embedded{
			Host: "0.0.0.0",
			Port: 8080,
		},
		AccessRPC: client{
			Endpoints: []string{"access-rpc:9001"},
			Timeout:   5000,
			NonBlock:  true,
		},
		JWTTTLMinutes: 120,
	}

	if err := ApplyEnvOverrides("GATEWAY_API", &cfg); err != nil {
		t.Fatalf("ApplyEnvOverrides returned error: %v", err)
	}

	if cfg.Host != "127.0.0.1" || cfg.Port != 9090 {
		t.Fatalf("unexpected host/port override: %#v", cfg)
	}
	if len(cfg.AccessRPC.Endpoints) != 2 || cfg.AccessRPC.Endpoints[1] != "rpc-b:2" {
		t.Fatalf("unexpected endpoints override: %#v", cfg.AccessRPC.Endpoints)
	}
	if cfg.AccessRPC.Timeout != 7000 || cfg.AccessRPC.NonBlock {
		t.Fatalf("unexpected rpc client override: %#v", cfg.AccessRPC)
	}
	if cfg.JWTTTLMinutes != 240 {
		t.Fatalf("unexpected JWT TTL override: %d", cfg.JWTTTLMinutes)
	}
	if cfg.KeepaliveTime != 30*time.Second {
		t.Fatalf("unexpected keepalive override: %s", cfg.KeepaliveTime)
	}
	if cfg.ProviderAPIKey != "provider-key-1" {
		t.Fatalf("unexpected provider api key override: %q", cfg.ProviderAPIKey)
	}
}

func TestApplyEnvOverridesSupportsJSONSliceAndErrorsOnInvalidValue(t *testing.T) {
	type methodTimeout struct {
		FullMethod string
		Timeout    time.Duration
	}
	type config struct {
		MethodTimeouts []methodTimeout
		Timeout        int64
	}

	t.Setenv("PROVIDER_RPC_METHOD_TIMEOUTS", `[{"FullMethod":"/rpc/Foo","Timeout":1000000000}]`)

	cfg := config{}
	if err := ApplyEnvOverrides("PROVIDER_RPC", &cfg); err != nil {
		t.Fatalf("ApplyEnvOverrides returned error: %v", err)
	}
	if len(cfg.MethodTimeouts) != 1 || cfg.MethodTimeouts[0].FullMethod != "/rpc/Foo" {
		t.Fatalf("unexpected method timeouts override: %#v", cfg.MethodTimeouts)
	}

	t.Setenv("PROVIDER_RPC_TIMEOUT", "invalid")
	if err := ApplyEnvOverrides("PROVIDER_RPC", &cfg); err == nil {
		t.Fatal("expected invalid timeout override to return error")
	}
}

func TestApplyEnvOverridesWithPrefixesPrefersLaterPrefix(t *testing.T) {
	type client struct {
		Timeout int64
	}
	type config struct {
		Mode      string
		Timeout   int64
		AccessRPC client
	}

	t.Setenv("COMMON_MODE", "dev")
	t.Setenv("COMMON_TIMEOUT", "5000")
	t.Setenv("COMMON_ACCESS_RPC_TIMEOUT", "2000")
	t.Setenv("GATEWAY_API_TIMEOUT", "10000")
	t.Setenv("GATEWAY_API_ACCESS_RPC_TIMEOUT", "5000")

	cfg := config{}
	if err := ApplyEnvOverridesWithPrefixes(&cfg, "COMMON", "GATEWAY_API"); err != nil {
		t.Fatalf("ApplyEnvOverridesWithPrefixes returned error: %v", err)
	}

	if cfg.Mode != "dev" {
		t.Fatalf("expected common mode override, got %q", cfg.Mode)
	}
	if cfg.Timeout != 10000 {
		t.Fatalf("expected service timeout override, got %d", cfg.Timeout)
	}
	if cfg.AccessRPC.Timeout != 5000 {
		t.Fatalf("expected service rpc timeout override, got %d", cfg.AccessRPC.Timeout)
	}
}
