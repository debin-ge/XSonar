# Tweet Interaction Read APIs 设计

## 1. 背景

当前项目对外公开的只读上游 API 固定为 10 个接口，入口集中在：

- [apps/gateway-api/gateway.api](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/gateway.api)
- [apps/gateway-api/internal/handler/routes.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/internal/handler/routes.go)
- [apps/policy-rpc/internal/service.go](/Users/gedebin/Documents/Code/XSonar/apps/policy-rpc/internal/service.go)

网关请求统一走 `gateway-api -> policy-rpc -> provider-rpc` 的固定策略代理链路。`gateway-api` 负责鉴权、参数治理、限额、策略解析和上游转发，`policy-rpc` 负责公开路径到上游路径的策略映射，`provider-rpc` 负责真正访问上游服务。

本次需求是在现有固定策略模式下，继续补一批“纯只读、无需 auth_token”的推文详情与互动查询接口。

## 2. 目标

### 2.1 目标

- 新增 4 个公开只读接口：
  - `GET /v1/tweets/brief`
  - `GET /v1/tweets/quotes`
  - `GET /v1/tweets/retweeters`
  - `GET /v1/tweets/likers`
- 继续复用统一代理链路，不为单个接口新增专用业务 handler。
- 继续复用固定策略模式，在默认策略中内置这 4 个接口。
- 保持现有参数治理规则：
  - 自动注入 `resFormat=json`
  - 拒绝非白名单参数
  - 继续统一拒绝 `proxyUrl`、`auth_token`、`ct0`
- 为新增接口补齐 gateway 单测和 swagger 覆盖。

### 2.2 非目标

- 本次不支持任何需要 `auth_token` 的上游接口，例如 `HomeTimeline`。
- 本次不引入 `ids-only` 变体，例如 `retweeter ids`。
- 本次不做公开 API 注册表的抽象重构，继续采用显式增量方式。
- 本次不扩展 `mentions timeline`，因为它更接近用户提及时间线而不是单推文互动接口。
- 本次不处理关系、列表、社区、搜索发现、用户扩展能力。

## 3. 已确认决策

- 只做“纯只读、无需 `auth_token`”的接口。
- 公网路径沿用当前项目风格，由实现侧统一设计，不贴上游原始命名。
- 第一批只暴露富数据接口，不增加 `ids-only` 变体。
- 第一批范围固定为 4 个新增接口：
  - `brief`
  - `quotes`
  - `retweeters`
  - `likers`
- 实现方式采用最小增量方案，不顺手做结构抽象。
- 上游路径名当前没有真实清单，先按功能名定义占位映射并实现，后续可再替换为真实路径。

## 4. 方案对比

### 4.1 方案 A：最小增量扩展固定策略

做法：

- 在 `gateway.api` 中显式增加请求类型和公开接口声明。
- 在 `routes.go` 中显式增加 4 条 GET 路由。
- 在 `policy-rpc` 默认策略中显式增加 4 条种子策略。
- 在 `gateway-api` 单测中补充参数与转发覆盖。

优点：

- 与现有 10 个接口完全同构。
- 变更面清晰，可控，回归风险最低。
- 便于后续继续按同样模式扩接口。

缺点：

- 每增加一个接口都要同步修改多个文件。

### 4.2 方案 B：提取统一公开 API 注册表

做法：

- 将公开路径、策略键、参数规则、上游路径集中到单一表结构。
- 由 `gateway-api` 和 `policy-rpc` 共用该表生成路由和默认策略。

优点：

- 长期更利于继续扩接口。

缺点：

- 当前只增加 4 个接口，抽象成本高于收益。
- 会把当前任务从“补接口”扩展成“补接口 + 重构”。

### 4.3 推荐方案

采用方案 A。当前需求最适合用显式增量方式稳定落地。

## 5. 推荐设计

## 5.1 公开接口与策略键

新增接口与策略键固定为：

| 公开路径 | 方法 | 策略键 |
| --- | --- | --- |
| `/v1/tweets/brief` | `GET` | `tweets_brief_v1` |
| `/v1/tweets/quotes` | `GET` | `tweets_quotes_v1` |
| `/v1/tweets/retweeters` | `GET` | `tweets_retweeters_v1` |
| `/v1/tweets/likers` | `GET` | `tweets_likers_v1` |

命名约束：

- 公网路径继续使用复数资源风格和短语义后缀。
- 策略键继续采用 `<resource>_<action>_v1` 风格，和已有 `tweets_detail_v1`、`search_tweets_v1` 保持一致。

## 5.2 占位上游映射

由于当前缺少真实上游路径清单，本次先定义占位上游路径，并在代码与文档中显式标注其占位属性：

| 策略键 | 占位上游路径 |
| --- | --- |
| `tweets_brief_v1` | `/base/apitools/tweetBriefInfoV2` |
| `tweets_quotes_v1` | `/base/apitools/tweetQuotesV2` |
| `tweets_retweeters_v1` | `/base/apitools/tweetRetweetersV2` |
| `tweets_likers_v1` | `/base/apitools/tweetFavoritersV2` |

约束：

- 这些路径只作为当前实现占位值使用。
- 后续若拿到真实上游路径，优先只替换 `policy-rpc` 默认策略中的 `upstream_path`，不改变公网路径和策略键。

## 5.3 参数模型

4 个接口统一以 `tweetId` 作为公开必填参数：

| 公开路径 | 必填参数 | 可选参数 |
| --- | --- | --- |
| `/v1/tweets/brief` | `tweetId` | 无 |
| `/v1/tweets/quotes` | `tweetId` | `cursor` |
| `/v1/tweets/retweeters` | `tweetId` | `cursor` |
| `/v1/tweets/likers` | `tweetId` | `cursor` |

参数治理规则：

- `policy-rpc` 为每条策略定义 `allowed_params` 和 `required_params`。
- `gateway-api` 继续调用现有的 `sanitizeUpstreamQuery()` 和 `validateRequiredQuery()`。
- `default_params` 继续注入 `resFormat=json`。
- 敏感参数继续由统一逻辑拒绝，不为新接口单独开例外。

## 5.4 参数归一化

当前已有 `tweets_detail_v1` 在网关层做 `tweetId -> id` 的归一化修正。新增 4 个接口可能存在两类情况：

1. 上游本身接受 `tweetId`
2. 上游实际需要 `id`

设计决策：

- 保持归一化逻辑集中在 [apps/gateway-api/internal/service.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/internal/service.go) 的 `normalizeProviderQuery()`。
- 若占位上游接口延续“详情接口需要 `id`”的模式，则为对应策略键增加 `tweetId -> id` 映射。
- 若占位上游接口接受 `tweetId`，则不做额外改写。

为了避免过度猜测，本次实现采用“仅在测试明确要求时增加归一化”的原则，不为所有新接口默认强行重写参数名。

## 5.5 组件改动范围

### 5.5.1 gateway-api

需要修改：

- [apps/gateway-api/gateway.api](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/gateway.api)
- [apps/gateway-api/internal/handler/routes.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/internal/handler/routes.go)
- [apps/gateway-api/internal/service.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/internal/service.go)
- [apps/gateway-api/internal/service_test.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/internal/service_test.go)
- [apps/gateway-api/swagger_test.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/swagger_test.go)

职责：

- 定义公开路径和请求参数。
- 保持统一 `HandleProxy` 入口。
- 在必要时扩展参数归一化。
- 补充接口级 gateway 单测和 swagger 断言。

### 5.5.2 policy-rpc

需要修改：

- [apps/policy-rpc/internal/service.go](/Users/gedebin/Documents/Code/XSonar/apps/policy-rpc/internal/service.go)

职责：

- 在 `defaultPolicies()` 中新增 4 条内置种子策略。
- 将新增公开路径映射到对应占位上游路径。
- 为每条策略声明参数白名单、必填参数和默认参数。

### 5.5.3 provider-rpc

本次不新增专用接口，只复用：

- [apps/provider-rpc/internal/service.go](/Users/gedebin/Documents/Code/XSonar/apps/provider-rpc/internal/service.go)

职责不变：

- 继续通过 `ExecutePolicy` 构造上游 URL、发起 HTTP 请求、解包响应并记录日志。

## 5.6 测试策略

本次只做以下 3 类测试：

### 5.6.1 gateway 单测

在 [apps/gateway-api/internal/service_test.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/internal/service_test.go) 增加聚焦测试：

- 新路径能正确解析策略并完成转发。
- 缺少 `tweetId` 时会返回参数缺失错误。
- `cursor` 能在允许的接口中透传。
- `resFormat=json` 会继续自动注入。
- 若新增了归一化规则，验证归一化后发送给 provider 的查询参数。

### 5.6.2 swagger 测试

在 [apps/gateway-api/swagger_test.go](/Users/gedebin/Documents/Code/XSonar/apps/gateway-api/swagger_test.go) 增加至少一条新增接口断言：

- 路径存在于 swagger。
- `tweetId` 为必填 query 参数。
- `cursor` 在需要的接口上为可选参数。
- 认证头部规则保持现有 dev/prod 差异。

### 5.6.3 policy 默认策略覆盖

优先复用现有默认策略行为，不额外写大而全的枚举测试。只有在当前测试无法间接覆盖新增策略时，才增加最小粒度断言。

## 5.7 文档与兼容性

由于项目当前文档多处仍把公开 API 写死为 10 个接口，本次实现后需要接受一个事实：

- 代码中的实际公开接口数将先于部分说明文档扩展到 14 个。

本次实现阶段优先保证代码、策略和 swagger 一致。用户侧接入文档与功能清单可在后续单独更新，避免把当前任务扩大成“接口实现 + 全量文档整理”。

## 6. 实施顺序

1. 在 `gateway.api` 增加 4 个请求类型与 4 条路由声明。
2. 在 `routes.go` 中补充 4 条 GET 路由。
3. 在 `policy-rpc` 默认策略中增加 4 条种子策略。
4. 视测试需要扩展 `normalizeProviderQuery()`。
5. 增加 gateway 单测。
6. 增加 swagger 断言。
7. 运行聚焦测试验证。

## 7. 风险与缓解

### 7.1 上游路径为占位值

风险：

- 当前没有真实上游路径，接口可能在真实环境不可用。

缓解：

- 将占位路径限制在默认策略层。
- 不把占位路径扩散到公网路径和策略键。
- 后续替换真实路径时只需要改策略映射和相应测试断言。

### 7.2 参数名猜测偏差

风险：

- 某些上游接口可能要求 `id`、`focalTweetId` 等字段，而不是 `tweetId`。

缓解：

- 统一先对外暴露 `tweetId`。
- 把参数改写能力保留在 `normalizeProviderQuery()`。
- 通过 gateway 单测把当前假设钉住，后续按真实上游再调整。

### 7.3 文档与代码短期不一致

风险：

- 仓库里的产品文档和开发接入文档目前仍宣称只有 10 个公开接口。

缓解：

- 本次先保证代码行为正确和 swagger 可见。
- 后续将文档更新作为单独任务处理。

## 8. 验收标准

- 4 个新增公开路径在 `gateway-api` 中可见。
- `policy-rpc` 默认策略可解析这 4 个路径。
- 请求缺少 `tweetId` 时被拒绝。
- 合法请求会被转发到对应占位上游路径。
- swagger 中能看到新增接口及其 query/header 参数定义。
- 现有 gateway 核心代理测试不回归。
