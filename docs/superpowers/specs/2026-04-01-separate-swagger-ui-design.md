# Console API 与 Gateway API 独立 Swagger 文档设计

## 背景

`XSonar` 当前的 [`console-api`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.api) 与 [`gateway-api`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/gateway.api) 都基于 go-zero 的 `.api` DSL 维护 HTTP 接口定义，但运行时尚未形成统一的 Swagger 文档接入方式。

当前状态包括：

1. `console-api` 没有正式对外提供 Swagger 文档与 Swagger UI。
2. `gateway-api` 仓库内存在手写的 [`openapi-gateway-api.yaml`](/Users/gedebin/Documents/Code/XSonar/docs/swagger/openapi-gateway-api.yaml)，但它不是当前 `.api` 真源自动生成的产物，也没有通过 `gateway-api` 服务自身稳定公开。

用户目标是：

1. 使用 go-zero 官方 `goctl api swagger` 为 `console-api` 与 `gateway-api` 分别生成 Swagger/OpenAPI 文档。
2. 两个服务各自独立提供自己的 Swagger JSON 与 Swagger UI，不合并到同一个服务。
3. 文档入口无需额外鉴权，可直接公开访问。

## 设计结论

本次采用 go-zero 官方 `goctl api swagger` 作为唯一文档生成方式，并让 `console-api` 与 `gateway-api` 各自公开自己的文档与 UI。

最终方案如下：

1. `console-api` 以 [`apps/console-api/console.api`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.api) 为真源，生成 [`apps/console-api/docs/swagger.json`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/docs/swagger.json)。
2. `gateway-api` 以 [`apps/gateway-api/gateway.api`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/gateway.api) 为真源，生成 [`apps/gateway-api/docs/swagger.json`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/docs/swagger.json)。
3. `console-api` 公开：
   - `GET /swagger/doc.json`
   - `GET /swagger/index.html`
4. `gateway-api` 公开：
   - `GET /swagger/doc.json`
   - `GET /swagger/index.html`
5. 两组入口都完全公开访问，不附加后台 JWT 或网关签名要求。
6. Swagger UI 由服务本身提供，不依赖额外独立站点，不将两份文档聚合到一起。

## 范围

本次设计包含：

1. 为 `console-api` 与 `gateway-api` 的 `.api` 文件补齐 Swagger 生成所需的元信息。
2. 为两个服务生成各自独立的 Swagger/OpenAPI JSON 产物。
3. 在两个服务内分别公开 `/swagger/doc.json` 与 `/swagger/index.html`。
4. 引入 Swagger UI 页面与所需静态资源。
5. 为上述公开文档入口补充自动化测试。

本次设计不包含：

1. 把 `console-api` 与 `gateway-api` 的文档合并为统一门户。
2. 为 Swagger 文档访问增加单独登录、JWT 校验或应用签名要求。
3. 为 RPC 服务同步接入 Swagger。
4. 在本次范围内细化所有 `Envelope.data` 的业务模型。

## 当前约束

### 接口真源

两个服务当前都以 `.api` 文件作为 HTTP 定义真源，因此 Swagger 也应直接从 `.api` 生成，避免再维护一套 Go 注释或手写 OpenAPI 文件。

### 服务边界

`console-api` 与 `gateway-api` 是独立服务，分别运行在不同端口，并且目前都通过各自的启动入口注册路由：

1. [`apps/console-api/console.go`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.go)
2. [`apps/gateway-api/gateway.go`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/gateway.go)

因此文档入口也应留在各自服务内，而不是借由另一侧服务或额外聚合层对外暴露。

### 鉴权要求

本次需求明确要求 Swagger 文档可公开访问，因此 Swagger 相关路由不能复用：

1. `console-api` 的 `requireAdminAuth(...)`
2. `gateway-api` 的应用签名校验逻辑

Swagger 路由应作为单独公开路由挂载，不进入原有业务鉴权链路。

## 方案对比

### 方案 A：两个服务都用 goctl 生成文档，并各自内置 Swagger UI

优点：

1. `.api` 是唯一事实来源，文档与路由不易漂移。
2. 服务边界清晰，符合“分开，不放在一起”的要求。
3. 访问路径稳定，部署后不依赖额外文档站点。
4. 可完全公开访问，满足当前需求。

缺点：

1. `.api` 变更后需要同步刷新 `swagger.json`。
2. 需要在两个服务侧都接入一份 Swagger 路由注册逻辑。

### 方案 B：各自生成 JSON，但共享一个外部 Swagger UI 站点

优点：

1. 静态 UI 资源只维护一份。

缺点：

1. 与“两个服务分开”不完全一致。
2. 多一个部署实体，访问链路更复杂。
3. 文档站点需要额外配置两个上游文档源。

### 方案 C：继续保留 `gateway-api` 的手写 OpenAPI，只有 `console-api` 迁移到 goctl

优点：

1. 初始改动较小。

缺点：

1. 两边文档来源不一致，长期维护成本高。
2. `gateway-api` 文档更容易与 `.api` 漂移。
3. 不符合统一使用 `goctl api swagger` 的目标。

最终采用方案 A。

## 架构设计

### 文档生成层

两个服务都使用 go-zero 官方命令从 `.api` 直接生成文档：

```bash
goctl api swagger \
  --api apps/console-api/console.api \
  --dir apps/console-api/docs \
  --filename swagger

goctl api swagger \
  --api apps/gateway-api/gateway.api \
  --dir apps/gateway-api/docs \
  --filename swagger
```

生成产物分别为：

1. [`apps/console-api/docs/swagger.json`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/docs/swagger.json)
2. [`apps/gateway-api/docs/swagger.json`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/docs/swagger.json)

为保证文档可读性，需要在两个 `.api` 文件中补充合适的 Swagger 元信息，例如：

1. `info`：标题、描述、版本。
2. `server`：默认 host/port 或相对服务说明。
3. 必要的 tag、summary、description。

鉴于本次 Swagger 入口公开访问，不需要在 `.api` 文档元信息中为文档页本身声明额外认证要求。

### 服务暴露层

在 `console-api` 与 `gateway-api` 启动阶段额外挂载 Swagger 路由，方式与现有健康检查路由注册类似，但使用独立的共享注册逻辑。

每个服务新增：

1. `GET /swagger/doc.json`
2. `GET /swagger/index.html`
3. Swagger UI 依赖的本地静态资源路由

职责划分如下：

1. `/swagger/doc.json`
   - 读取当前服务对应的 `swagger.json`
   - 返回 `Content-Type: application/json`
   - 不执行业务鉴权

2. `/swagger/index.html`
   - 返回 Swagger UI 页面
   - 页面默认从同源 `/swagger/doc.json` 读取文档
   - 不执行业务鉴权

3. 其他 Swagger UI 静态资源
   - 由服务自身提供
   - 不依赖浏览器运行时访问公网 CDN

### 共享实现策略

推荐在 [`pkg/shared`](/Users/gedebin/Documents/Code/XSonar/pkg/shared) 下抽出一组通用 Swagger 文档注册能力，例如：

1. Swagger 文档文件读取
2. Swagger UI HTML 渲染
3. Swagger UI 静态资源注册

两个服务仅需在启动时传入：

1. 服务名
2. 对应的 `swagger.json` 文件路径

这样可以避免 `console-api` 与 `gateway-api` 重复实现相同的路由处理逻辑，同时继续保持“每个服务只暴露自己的文档”。

### UI 资源策略

Swagger UI 应采用“项目内托管”的方式，而不是运行时依赖外部 CDN。原因如下：

1. 部署后访问稳定。
2. 内网或无外网环境也能使用。
3. 两个服务对外表现一致。

实现形式可以是：

1. 在仓库中保存 Swagger UI 所需静态资源，并在服务内暴露。
2. 或通过嵌入静态文件方式内置到二进制，再通过 HTTP 返回。

无论采用哪种具体形式，都必须满足：

1. 用户直接访问 `/swagger/index.html` 即可打开 UI。
2. UI 默认指向同源 `/swagger/doc.json`。
3. 不需要任何额外登录或签名即可查看文档。

## 数据流

### 文档生成流

1. 开发者修改 `.api` 文件。
2. 执行 `goctl api swagger` 更新对应服务的 `swagger.json`。
3. 生成产物随代码一起进入仓库与部署包。
4. 服务启动后基于本地文件或嵌入资源对外提供文档。

### 文档访问流

1. 浏览器访问某个服务的 `/swagger/index.html`。
2. 服务直接返回 Swagger UI 页面。
3. 页面再同源请求该服务的 `/swagger/doc.json`。
4. 服务返回当前服务自己的 OpenAPI JSON。
5. 页面完成渲染，用户查看该服务的接口文档。

`console-api` 与 `gateway-api` 的访问流彼此独立，不互相代理，也不共享运行时文档内容。

## 错误处理

Swagger 入口的错误处理以“文档与静态页面语义”为主，不强行复用业务接口统一信封：

1. `/swagger/doc.json` 找不到文档文件或读取失败时返回 `500`。
2. `/swagger/index.html` 正常返回 HTML 页面；若后续文档加载失败，由 Swagger UI 前端直接显示加载错误。
3. 静态资源缺失时返回标准 `404`。

这样更符合 Swagger UI 的浏览器访问模式，也能减少不必要的包装逻辑。

## 测试设计

本次实现需补充自动化测试，至少覆盖以下行为：

1. `console-api` 请求 `/swagger/doc.json` 返回 `200` 且 `Content-Type` 为 JSON。
2. `console-api` 请求 `/swagger/index.html` 返回 `200`，且内容包含 `/swagger/doc.json` 初始化配置。
3. `gateway-api` 请求 `/swagger/doc.json` 返回 `200` 且 `Content-Type` 为 JSON。
4. `gateway-api` 请求 `/swagger/index.html` 返回 `200`，且内容包含 `/swagger/doc.json` 初始化配置。
5. 文档文件缺失时返回 `500`。

测试方式保持与现有 [`apps/console-api/internal/service_test.go`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/internal/service_test.go) 和 [`apps/gateway-api/internal/service_test.go`](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/internal/service_test.go) 一致，以 `httptest` 和 serverless 路由验证为主，不引入真实浏览器 E2E。

## 风险与应对

### 风险 1：`.api` 与 `swagger.json` 漂移

风险描述：

开发者修改了 `.api`，但忘记同步更新生成产物。

应对：

1. 增加统一生成命令入口，例如脚本或 `make swagger`。
2. 在开发流程中要求 `.api` 变更时同步提交对应 `swagger.json`。

### 风险 2：Swagger UI 静态资源组织不当

风险描述：

若资源路径设计不完整，`index.html` 能打开，但 JS/CSS 请求失败，UI 无法正常渲染。

应对：

1. 使用受控的共享注册逻辑统一资源路径。
2. 为 HTML 与静态资源路径分别补充测试。

### 风险 3：旧的 `gateway-api` 手写 OpenAPI 继续被误用

风险描述：

仓库中现有的 [`docs/swagger/openapi-gateway-api.yaml`](/Users/gedebin/Documents/Code/XSonar/docs/swagger/openapi-gateway-api.yaml) 可能与新生成产物并存，导致后续维护混淆。

应对：

1. 明确运行时入口只认 `apps/gateway-api/docs/swagger.json`。
2. 后续根据项目需要决定保留为历史参考或清理掉旧文件。

## 验收标准

满足以下条件即视为完成：

1. `console-api` 启动后可直接访问自己的 `/swagger/index.html` 与 `/swagger/doc.json`。
2. `gateway-api` 启动后可直接访问自己的 `/swagger/index.html` 与 `/swagger/doc.json`。
3. 两个服务的文档入口互不耦合，不需要统一聚合服务。
4. Swagger UI 不依赖额外鉴权，可公开访问。
5. 文档由 `goctl api swagger` 从各自 `.api` 文件生成，不再依赖 `gateway-api` 的手写 OpenAPI 作为运行时主文档来源。
6. 自动化测试覆盖文档入口成功和文档文件缺失两类关键场景。
