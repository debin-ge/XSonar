# Console API / Gateway API HTTPS TLS 设计

## 1. 背景

当前 `console-api` 与 `gateway-api` 都通过 `go-zero` 的 `rest.MustNewServer(c.RestConf)` 直接启动 HTTP 服务：

- [`apps/console-api/console.go`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.go)
- [`apps/gateway-api/gateway.go`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/gateway.go)

两个服务当前仅暴露 HTTP 端口，`docker-compose`、本地配置和开发文档也全部以 HTTP 为默认入口。项目依赖的 `go-zero v1.10.1` 已在 `rest.RestConf` 中原生支持 `CertFile` 和 `KeyFile`，因此 HTTPS 主监听不需要替换框架，只需要补齐双监听与跳转能力。

本次需求是为 `console-api` 和 `gateway-api` 增加 TLS，使外部访问切换为 HTTPS，同时保留 HTTP 并将请求重定向到 HTTPS。

## 2. 目标

### 2.1 目标

- 为 `console-api` 和 `gateway-api` 增加 HTTPS 监听能力。
- 保留 HTTP 入口，并将所有 HTTP 请求以 `308 Permanent Redirect` 重定向到对应 HTTPS 地址。
- HTTP 与 HTTPS 端口都支持配置。
- 证书通过容器挂载文件提供，并允许通过环境变量覆盖证书文件路径。
- 更新 `docker-compose`、本地配置、健康检查和开发文档，使本地运行与验证路径完整可用。

### 2.2 非目标

- 本次不改造 `access-rpc`、`policy-rpc`、`provider-rpc` 的内部通信。
- 本次不引入 mTLS。
- 本次不自动签发或管理证书。
- 本次不引入额外代理层（如 Nginx、Caddy）作为 TLS 终止点。
- 本次不实现 HSTS、OCSP、自动续期等更深的 TLS 运维特性。

## 3. 约束与已确认决策

- 范围选择：除服务自身外，同时更新 `docker-compose`、healthcheck、示例文档和调用样例。
- 证书来源：通过挂载证书文件到容器中，并支持环境变量覆盖 `CertFile` / `KeyFile` 路径。
- HTTP 行为：保留 HTTP，并将请求重定向到 HTTPS。
- 端口策略：HTTP 与 HTTPS 端口都必须可配置，不固定为某一组默认值。

## 4. 方案对比

### 4.1 方案 A：复用 go-zero 路由，自行补双监听启动层

做法：

- HTTPS 继续使用 `go-zero` 原生 `rest.RestConf` 启动。
- 额外启动一个标准库 `http.Server` 监听 HTTP 端口，只返回到 HTTPS 的 `308` 跳转。

优点：

- 业务路由、中间件、handler 注册方式保持不变。
- 改动集中在 API 服务入口和少量共享启动逻辑。
- 能直接复用 `RestConf.CertFile` / `KeyFile`。

缺点：

- 需要自己处理双 server 的启动、关闭和配置校验。

### 4.2 方案 B：引入额外反向代理处理 TLS 与跳转

做法：

- 应用继续只监听 HTTP。
- 在外部增加 Nginx/Caddy 负责 HTTPS 与 HTTP 跳转。

优点：

- 应用代码改动最少。

缺点：

- TLS 能力不在服务自身内完成。
- 需要新增代理容器与配置，偏离当前需求。

### 4.3 方案 C：抽象通用 API 双协议启动框架

做法：

- 为项目所有 HTTP API 服务抽象一套通用的双协议启动组件。

优点：

- 理论上可供未来服务复用。

缺点：

- 这次只涉及两个服务，抽象成本高于当前收益。

### 4.4 推荐方案

采用方案 A。它最贴合当前代码结构，兼顾可控改动范围与交付速度，且没有额外运行时依赖。

## 5. 推荐设计

本节定义本次实现必须采用的配置、启动、校验和部署行为。

## 5.1 配置模型

`RestConf` 继续承担 HTTPS 主监听配置：

- `Host`
- `Port`
- `CertFile`
- `KeyFile`

其中：

- `Host` / `Port` 表示 HTTPS 实际业务监听地址。
- `CertFile` / `KeyFile` 表示 HTTPS 证书路径。

在 `console-api` 和 `gateway-api` 的配置结构中新增 `HTTPRedirect` 配置块：

```yaml
HTTPRedirect:
  Enabled: true
  Host: 0.0.0.0
  Port: 8080
```

字段定义：

- `Enabled bool`
- `Host string`
- `Port int`

环境变量覆盖继续使用现有反射式覆盖能力，例如：

- `CONSOLE_API_CERT_FILE`
- `CONSOLE_API_KEY_FILE`
- `CONSOLE_API_HTTP_REDIRECT_ENABLED`
- `CONSOLE_API_HTTP_REDIRECT_HOST`
- `CONSOLE_API_HTTP_REDIRECT_PORT`
- `GATEWAY_API_CERT_FILE`
- `GATEWAY_API_KEY_FILE`
- `GATEWAY_API_HTTP_REDIRECT_ENABLED`
- `GATEWAY_API_HTTP_REDIRECT_HOST`
- `GATEWAY_API_HTTP_REDIRECT_PORT`

由于当前 `ApplyEnvOverridesWithPrefixes()` 已支持嵌套结构体路径拼接，因此该配置结构能够直接兼容现有环境变量覆盖机制。

## 5.2 启动模型

每个服务在启用 TLS 后同时启动两个监听端口：

- HTTPS server：承载实际业务请求。
- HTTP redirect server：仅负责重定向到 HTTPS。

实现方式：

1. 保留当前 `rest.MustNewServer(c.RestConf)` 作为 HTTPS 服务构建入口。
2. 在注册业务路由后，以并发方式启动：
   - `go-zero` HTTPS server
   - 标准库 `http.Server` HTTP redirect server
3. 使用统一的错误收集与关闭逻辑，确保任一 server 失败时整个进程退出，避免半可用状态。

### 5.2.1 HTTP 重定向行为

HTTP redirect server 不参与业务处理，只执行：

- 读取来请求的 host、path、raw query
- 构造目标 HTTPS URL
- 返回 `308 Permanent Redirect`

必须选择 `308`，原因：

- `console-api` 存在 `POST /admin/v1/auth/login` 等非幂等请求。
- `301` / `302` 可能导致客户端将方法退化为 `GET`。
- `308` 能保留原始 HTTP method 与请求体语义。

### 5.2.2 重定向 URL 规则

重定向地址构造规则：

- scheme 固定为 `https`
- hostname 来自请求头中的 host，但不信任其中的端口部分
- 端口以当前服务 HTTPS 配置端口为准
- path 与 query 原样保留

示例：

- 请求：`http://127.0.0.1:8080/v1/users/by-ids?userIds=1,2`
- 目标：`https://127.0.0.1:8443/v1/users/by-ids?userIds=1,2`

若 HTTPS 端口为 `443`，则跳转 URL 中可省略端口；非 `443` 时显式写入。

## 5.3 启动校验策略

服务启动时做 fail-fast 校验，任何配置不完整都直接报错退出：

- `Port` 必须表示 HTTPS 监听端口。
- `CertFile` / `KeyFile` 必须同时存在且非空。
- 若 `HTTPRedirect.Enabled=true`：
  - `HTTPRedirect.Port` 必须非零。
  - `HTTPRedirect.Port` 不能与 HTTPS `Port` 相同。
- 证书文件必须可读取。

这样可以避免服务进入“HTTPS 不可用但进程仍然存活”的状态。

## 5.4 日志策略

启动阶段输出两类日志：

- HTTPS 监听地址
- HTTP 重定向监听地址

日志中只打印证书路径，不打印任何证书内容。

## 6. compose 与本地配置设计

## 6.1 配置文件更新

以下配置文件补充 TLS 与 `HTTPRedirect` 示例字段：

- [`apps/gateway-api/etc/gateway-api.yaml`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/etc/gateway-api.yaml)
- [`apps/console-api/etc/console-api.yaml`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/etc/console-api.yaml)
- [`deploy/configs/local/gateway-api.yaml`](/Users/gedebin/Documents/Code/XSonar/deploy/configs/local/gateway-api.yaml)
- [`deploy/configs/local/console-api.yaml`](/Users/gedebin/Documents/Code/XSonar/deploy/configs/local/console-api.yaml)

配置文件中保留容器内证书挂载路径示例，例如：

```yaml
CertFile: /app/certs/gateway/tls.crt
KeyFile: /app/certs/gateway/tls.key
HTTPRedirect:
  Enabled: true
  Host: 0.0.0.0
  Port: 8080
```

具体证书文件由运行环境挂载提供，不提交证书本身到仓库。

## 6.2 docker-compose 更新

[`deploy/xsonar/docker-compose.yml`](/Users/gedebin/Documents/Code/XSonar/deploy/xsonar/docker-compose.yml) 需要同步更新：

- 为 `gateway-api` 暴露两个可配置端口：
  - HTTP redirect 端口
  - HTTPS 业务端口
- 为 `console-api` 暴露两个可配置端口：
  - HTTP redirect 端口
  - HTTPS 业务端口
- 为两个服务增加证书 volume 挂载点。

环境变量命名：

- `GATEWAY_API_HTTP_PORT`
- `GATEWAY_API_HTTPS_PORT`
- `HOST_GATEWAY_API_HTTP_PORT`
- `HOST_GATEWAY_API_HTTPS_PORT`
- `CONSOLE_API_HTTP_PORT`
- `CONSOLE_API_HTTPS_PORT`
- `HOST_CONSOLE_API_HTTP_PORT`
- `HOST_CONSOLE_API_HTTPS_PORT`

此命名与现有 compose 变量风格一致，便于在本地或 CI 中显式覆盖。

## 6.3 healthcheck 更新

当前 healthcheck 仅用 `nc -z` 检查端口连通性，不足以证明 HTTPS 正常提供服务。

更新为：

- 使用 `curl -k https://127.0.0.1:<https-port>/healthz`
- 将 HTTPS 健康接口成功响应作为服务健康依据

说明：

- 本地场景可能使用自签证书，因此使用 `-k` 忽略证书链校验。
- HTTP redirect 端口不作为健康判定依据，因为它只负责跳转，不能代表业务服务可用。

## 7. 文档与示例更新

需要把公开入口和示例从 HTTP 更新为 HTTPS，重点包括：

- 开发接入文档中的默认入口地址
- `curl` 示例
- 本地开发与测试说明
- 现有引用 `http://127.0.0.1:8080` 的 load test 或 README 文档

文档必须明确说明：

- HTTP 入口保留，但会返回 `308` 到 HTTPS
- 本地自签或内网证书场景下可使用 `curl -k`
- 证书文件通过挂载提供，可用环境变量覆盖配置路径

## 8. 测试设计

## 8.1 单元测试

新增覆盖以下行为：

- TLS 配置校验
  - 缺少 `CertFile`
  - 缺少 `KeyFile`
  - 缺少 `HTTPRedirect.Port`
  - HTTP/HTTPS 端口冲突
- 重定向 URL 构造
  - 保留 path
  - 保留 query
  - 使用 HTTPS scheme
  - 使用配置中的 HTTPS 端口而非原请求端口
  - 当 HTTPS 端口为 `443` 时省略端口
- 重定向状态码
  - `GET` 返回 `308`
  - `POST` 也返回 `308`

## 8.2 服务级测试

保持现有业务 handler 测试不变，并补充配置相关测试：

- `console-api` 新配置结构可正常加载
- `gateway-api` 新配置结构可正常加载
- 新增 TLS / redirect 能力不改变既有业务路由响应

## 8.3 集成验证

本地 compose 验证至少覆盖：

1. `http://127.0.0.1:<http-port>/...` 返回 `308`
2. `Location` 指向 `https://127.0.0.1:<https-port>/...`
3. `https://127.0.0.1:<https-port>/healthz` 使用 `curl -k` 成功
4. `console-api` 的登录 `POST` 经过 HTTP 访问时仍使用 `308` 跳转，确保方法语义不丢失

## 9. 风险与边界

### 9.1 风险

- 文档与脚本中可能仍残留旧的 HTTP 默认地址，需要逐步清理。
- 若客户端不自动跟随 `308`，接入方需要显式更新调用地址为 HTTPS。
- 本地开发若未挂载证书，服务会因 fail-fast 校验直接启动失败。

### 9.2 边界

- 本次改造仅保证 `console-api` / `gateway-api` 对外入口为 HTTPS。
- 服务间内部 RPC 与上游 provider 通信不在本次变更范围内。

## 10. 实施摘要

本次实施将采用“HTTPS 主监听 + HTTP 308 跳转”的双监听模式：

- `go-zero` 原生 HTTPS 服务负责真实业务流量。
- 独立 HTTP server 仅负责 308 跳转到对应 HTTPS 地址。
- 证书通过 volume 挂载提供，并允许环境变量覆盖路径。
- `docker-compose`、本地配置、healthcheck、文档样例同步更新为 HTTPS 优先。

该方案能在不重构现有业务路由层的前提下，为 `console-api` 和 `gateway-api` 提供明确、可验证、可配置的 HTTPS 能力。
