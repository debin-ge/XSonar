# XSonar Agents

## Project Overview

Go monorepo (go-zero framework) implementing a B2B data access governance platform with 7 microservices.

## Service Architecture

| Service | Type | Port | Responsibility |
|---------|------|------|----------------|
| gateway-api | REST | 8080 | Public gateway, auth, routing |
| console-api | REST | 8081 | Admin API (tenants, apps, policies) |
| access-rpc | gRPC | 9001 | Auth, quota, tenant/app management |
| policy-rpc | gRPC | 9002 | Policy configuration |
| provider-rpc | gRPC | 9003 | Upstream provider proxy |
| scheduler-rpc | gRPC | 9004 | Collector task scheduling |
| collector-worker-rpc | gRPC | 9005 | Data collection workers |

**Infrastructure:** PostgreSQL + Redis

## Key Commands

### Run unit tests
```bash
go test ./...
```

### Run integration tests
```bash
go test ./tests/integration/...
```
Integration tests use in-memory store: `t.Setenv("COMMON_STORE_BACKEND", "memory")`

### Generate code
```bash
# API → handlers (from .api files)
goctl api go -api apps/{service}/{service}.api -dir apps/{service}/

# Proto → gRPC stubs
 protoc --go_out=. --go-grpc_out=. apps/{service}/{service}.proto

# Swagger docs
./scripts/generate-swagger.sh
```

### Build Docker images
```bash
docker build -f deploy/docker/{service}.Dockerfile . -t xsonar-{service}
```

### Run full stack locally
```bash
cd deploy/xsonar
docker compose -f docker-compose.yml -f docker-compose.local.yml up
```

## Configuration

- Service configs: `apps/{service}/etc/{service}.yaml`
- Local dev overrides: `deploy/configs/local/{service}.yaml`
- Docker Compose env: `deploy/xsonar/docker-compose.yml`

### Environment Variable Overrides

All services support env var overrides via `pkg/shared/envconfig.go`:
```bash
# Pattern: {PREFIX}_{FIELD_NAME}
ACCESS_RPC_SEED_ADMIN_USERNAME=admin
ACCESS_RPC_SEED_ADMIN_PASSWORD=admin123456
COMMON_STORE_BACKEND=memory  # Use in-memory store for tests
```

## Code Structure

```
apps/
  {service}/           # One dir per microservice
    {service}.go        # Entry point
    {service}.proto     # gRPC definitions
    {service}.api       # REST API definitions (gateway-api, console-api)
    etc/                # Config files
    internal/           # Service logic
    testkit/            # Test clients

pkg/
  proto/                # Generated gRPC stubs
  shared/               # Shared utilities (envconfig, jwt, config)
  clients/              # RPC client wrappers
  xlog/                 # Logging
  collector/            # Collector schema/keys
  model/                # Domain models

migrations/             # SQL migrations
runtime/                # Logs, collector output
deploy/
  configs/local/        # Local dev configs
  docker/               # Dockerfiles
  xsonar/               # docker-compose files
```

## Testing Patterns

- Unit tests: alongside source files (`*_test.go`)
- Integration tests: `tests/integration/` - full chain tests with testkits
- testkit per service: `apps/{service}/testkit/` provides `NewClient()` / `NewHandlerWithClients()`
- gRPC reflection enabled in dev/test mode (line 36-39 in each gRPC service)

## go-zero Conventions

- Config struct must be a pointer: `var c config.Config`
- Load config: `conf.MustLoad(*configFile, &c)`
- Apply env overrides: `shared.ApplyEnvOverridesWithPrefixes(&c, "COMMON", "{SERVICE}")`
- REST servers: `rest.MustNewServer(c.RestConf)`
- gRPC servers: `zrpc.MustNewServer(c.RpcServerConf, ...)`

## Important Notes

- **No Makefile** - uses direct `go` commands and Docker
- **Proto files** live in `apps/{service}/` but generated stubs go to `pkg/proto/{service}pb/`
- **Env vars for seeds**: integration tests require `ACCESS_RPC_SEED_ADMIN_USERNAME` and `ACCESS_RPC_SEED_ADMIN_PASSWORD`
- **Store backend**: `COMMON_STORE_BACKEND=pgredis` (prod) or `memory` (tests)
