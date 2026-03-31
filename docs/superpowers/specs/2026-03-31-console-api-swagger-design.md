# Console API Swagger 文档接入设计

## 背景

`XSonar` 当前的 `console-api` 基于 go-zero 的 `.api` DSL 维护接口定义，HTTP 路由由 [`apps/console-api/console.api`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.api) 与生成代码共同描述。项目内尚未接入 Swagger UI，也没有把 `console-api` 的管理接口文档作为服务能力对外暴露。

用户目标是：

1. 基于现有 `console-api` 输出一份 Swagger API 文档。
2. 文档可直接通过 `console-api` 访问。
3. 文档访问必须和后台管理接口一样走鉴权，不能匿名开放。

## 设计结论

本次采用 go-zero 官方 `goctl api swagger` 作为文档生成方式，不采用 `swaggo/swag` 的 Go 注释生成模式。

原因如下：

1. `console-api` 的接口真源已经是 `.api` 文件，继续使用 go-zero 生成可避免在 Go 代码中再维护一套 Swagger 注释。
2. go-zero 官方已支持从 `.api` 文件直接生成 Swagger JSON/YAML，能够覆盖本项目当前的路由声明、请求结构、path 参数和 Swagger 基本元信息。
3. `swaggo/swag` 更适合以 Go handler 注释作为接口真源的项目，而当前项目并非该模式。

因此本次实现分为两部分：

1. 从 [`apps/console-api/console.api`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.api) 生成 `swagger.json`。
2. 在 `console-api` 内提供受鉴权保护的 Swagger JSON 与 Swagger UI 入口。

## 范围

本次设计包含：

1. 为 `console.api` 增加 Swagger 所需的 `info` 与鉴权声明。
2. 生成并保存 `console-api` 的 Swagger JSON 文档。
3. 在 `console-api` 中暴露 `/swagger/doc.json`。
4. 在 `console-api` 中暴露 `/swagger/index.html`。
5. 对上述两个 Swagger 入口复用现有后台 JWT 校验逻辑。

本次设计不包含：

1. 为所有接口补齐精细化的 `data` 响应模型。
2. 引入独立的 OpenAPI 构建流水线平台。
3. 对 `gateway-api`、RPC 服务或其他服务同步接入 Swagger。
4. 重构现有 `console-api` 鉴权方式为 go-zero 全局 JWT 中间件。

## 当前约束

### 接口定义真源

`console-api` 当前使用 go-zero `.api` 文件定义路由，而不是基于 Go 注释直接生成文档。这意味着 Swagger 文档的准确性应优先绑定在 `.api` 文件上，而不是散落在 handler 注释里。

### 鉴权方式

`console-api` 当前不是通过 go-zero 路由级 `WithJwt(...)` 统一鉴权，而是由业务层显式调用 `requireAdminAuth(...)` 进行后台 JWT 校验。Swagger 文档访问必须复用这一逻辑，才能保持与管理接口一致的权限边界和返回格式。

### 返回体形态

当前 `.api` 文件大量声明 `returns (Envelope)`。因此 Swagger 文档能够稳定表达统一信封结构，但无法在本次范围内细致描述每个接口 `data` 字段的完整业务模型。这是当前 DSL 建模粒度导致的自然结果，不在本次强行扩展。

## 方案对比

### 方案 A：goctl 生成 Swagger JSON，自行在 `console-api` 中挂受保护的 UI

优点：

1. 以 `.api` 为单一事实来源，不重复维护接口描述。
2. 文档 JSON 和 UI 都由 `console-api` 自己提供，部署和访问路径简单。
3. 可以直接复用现有 `requireAdminAuth(...)`，保持权限一致。

缺点：

1. `.api` 变更后需要重新生成 `swagger.json`。
2. 文档的响应模型细节受 `.api` 当前建模粒度限制。

### 方案 B：goctl 生成 Swagger JSON，UI 使用外部 CDN 页面

优点：

1. 实现最轻。

缺点：

1. 浏览器端依赖外部网络资源。
2. 自包含性弱，不适合作为默认交付方案。

### 方案 C：改用 `swaggo/swag` 在 Go 代码中写注释生成

优点：

1. 对 handler 注释风格项目更自然。

缺点：

1. 与现有 `.api` 真源重复维护。
2. 文档和路由更容易漂移。
3. 接入成本更高，长期维护价值更低。

最终采用方案 A。

## 架构设计

### 文档生成层

在 [`apps/console-api/console.api`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.api) 中补充 Swagger 元信息：

1. `info (...)` 中声明标题、描述、版本、`consumes`、`produces`。
2. 通过 `securityDefinitionsFromJson` 声明后台管理文档使用的 Bearer Token 鉴权头。
3. 对登录接口与后台管理接口拆分不同的 `service`/`@server` 声明：
   - 登录接口不声明 `authType`
   - 管理接口声明 `authType: adminBearer`

这样生成出的 Swagger 文档可以正确表达：

1. `/admin/v1/auth/login` 为匿名登录入口。
2. 其他后台接口默认需要 `Authorization` 请求头。

Swagger 生成产物保存为：

- [`apps/console-api/docs/swagger.json`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/docs/swagger.json)

生成命令使用 go-zero 官方命令：

```bash
goctl api swagger \
  --api apps/console-api/console.api \
  --dir apps/console-api/docs \
  --filename swagger
```

### 服务暴露层

`console-api` 新增两个 HTTP 入口：

1. `GET /swagger/doc.json`
2. `GET /swagger/index.html`

职责划分如下：

1. `/swagger/doc.json`
   - 返回仓库内生成好的 `swagger.json`
   - 设置 `Content-Type: application/json`
   - 在返回前执行 `requireAdminAuth(...)`

2. `/swagger/index.html`
   - 返回 Swagger UI 页面
   - 页面默认从同源 `/swagger/doc.json` 拉取文档
   - 在返回前执行 `requireAdminAuth(...)`

Swagger UI 本身采用“由 `console-api` 内部提供页面”的方式，而不是要求使用者另启一个静态站点。这样文档访问入口与服务部署绑定，满足“可通过 `console-api` 直接访问”的目标。

### 鉴权设计

Swagger 路由复用现有后台鉴权函数：

- [`apps/console-api/internal/service.go`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/internal/service.go)

当前 `requireAdminAuth(...)` 的约束包括：

1. 从 `Authorization` 头提取 Bearer Token。
2. 使用 `JWTSecret` 和 `JWTIssuer` 校验签名与签发方。
3. 要求 `claims.Role == "platform_admin"`。

Swagger 入口必须与后台接口共享这一校验，不引入单独的文档令牌或匿名白名单。

### UI 资源策略

Swagger UI 资源应由项目自身托管，而非依赖浏览器访问时再从公网下载。原因如下：

1. 与服务一起部署，访问路径稳定。
2. 避免开发、测试、内网环境下的外网依赖。
3. 更容易在鉴权场景下保持同源访问。

具体实现可采用以下任一等价方式，最终实现时以最小改动为准：

1. 在项目内嵌 Swagger UI 静态资源并通过 `console-api` 提供。
2. 在项目内提供最小化 HTML 页面，并配套本地静态资源。

无论最终选择哪种细节实现，都必须满足：

1. `index.html` 可直接打开。
2. 页面默认指向同源 `/swagger/doc.json`。
3. UI 相关请求不绕开后台鉴权。

## 数据流

### 文档生成流

1. 开发者修改 [`apps/console-api/console.api`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/console.api)。
2. 运行 `goctl api swagger`。
3. 生成或更新 [`apps/console-api/docs/swagger.json`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/docs/swagger.json)。
4. `console-api` 启动后读取该文件或其嵌入产物，用于对外提供文档。

### 文档访问流

1. 浏览器请求 `/swagger/index.html`。
2. `console-api` 先执行 `requireAdminAuth(...)`。
3. 鉴权通过后返回 Swagger UI 页面。
4. 页面再请求 `/swagger/doc.json`。
5. `console-api` 对 JSON 请求再次执行 `requireAdminAuth(...)`。
6. 鉴权通过后返回 Swagger 文档 JSON。
7. 页面完成渲染，并允许用户带着 `Authorization` 头调试后台接口。

## 错误处理

Swagger 相关接口遵循 `console-api` 现有错误处理风格：

1. 缺少或无效后台 Token 时返回 401，并沿用统一错误信封。
2. 文档文件不存在、读取失败或嵌入资源不可用时返回 500。
3. Swagger UI 页面或文档 JSON 的错误响应都应带 `request_id`。

不采用单独的 HTML 错误页，也不为 Swagger 路由引入与现有接口不同的异常输出格式。

## 测试设计

本次实现需补充自动化测试，至少覆盖以下行为：

1. 未携带 Token 请求 `/swagger/doc.json` 返回 401。
2. 携带有效后台 Token 请求 `/swagger/doc.json` 返回 200，且 `Content-Type` 为 JSON。
3. 未携带 Token 请求 `/swagger/index.html` 返回 401。
4. 携带有效后台 Token 请求 `/swagger/index.html` 返回 200，且返回内容包含 Swagger UI 初始化标记。
5. 文档加载目标 URL 固定为同源 `/swagger/doc.json`。

测试风格应保持与现有 [`apps/console-api/internal/service_test.go`](/Users/gedebin/Documents/Code/XSonar/apps/console-api/internal/service_test.go) 一致，以 `httptest` 为主，不依赖真实网络服务。

## 风险与应对

### 风险 1：`swagger.json` 与 `.api` 漂移

风险描述：

开发者修改了 `.api`，但忘记重新生成 `swagger.json`。

应对：

1. 在实现阶段补充明确的生成命令入口。
2. 在文档或开发流程中要求 `.api` 变更时同步刷新产物。

### 风险 2：Swagger UI 资源路径与 go-zero 路由方式不兼容

风险描述：

Swagger UI 往往需要多份静态资源，请求路径较多；如果直接照搬第三方默认路由模式，可能和现有路由注册方式不完全匹配。

应对：

1. 优先采用可由 `console-api` 明确控制的页面与静态资源组织方式。
2. 实现时以受控页面和同源 JSON 为中心，而不是强依赖第三方默认目录结构。

### 风险 3：Swagger 文档的返回模型不够细

风险描述：

当前 `Envelope` 返回体会让 Swagger 中的大部分 `data` 字段较泛化。

应对：

1. 本次先保证文档可访问、可调试、可表达认证要求。
2. 后续若需要更强可读性，再单独细化 `.api` 返回模型。

## 验收标准

满足以下条件即视为本次设计目标完成：

1. 可从 `console.api` 稳定生成 `console-api` 的 `swagger.json`。
2. `console-api` 启动后可访问 `/swagger/index.html` 和 `/swagger/doc.json`。
3. 以上两个 Swagger 入口均要求后台管理员 JWT。
4. Swagger UI 默认读取同源文档地址，无需额外启动其他服务。
5. 登录接口在文档中表现为匿名访问，其他后台接口表现为需要 `Authorization` 头。

