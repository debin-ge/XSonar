# Public Read-Only Provider APIs 设计

## 1. 背景

当前公开只读路由通过固定链路工作：

- `gateway-api` 负责鉴权、配额、公开参数校验、策略解析和请求透传
- `policy-rpc` 负责把公开路径映射为上游路径与参数规则
- `provider-rpc` 负责按策略访问上游 Provider

现有公开路由和默认策略已经覆盖用户、推文、搜索的基础能力，但缺少更多只读查询接口。用户本次确认的交付边界是：

- 新增为 `gateway-api` 的公开只读路由
- 在 `policy-rpc` 中预置默认策略
- 沿用当前简洁路径风格
- 保持响应为上游 JSON 透传，不做网关层重塑
- 只做只读接口，不引入写操作

上游接口说明来源于 [SJ5 API Reference](https://sj5.readme.io/reference)。从其嵌入 OpenAPI 可确认本次所需的大部分目标路径。

## 2. 目标与非目标

### 2.1 目标

- 为以下能力补齐公开只读路由与默认策略：
  - 推文详情与互动：`tweet brief`、`quotes`、`retweeters`、`favoriters`、`mentions timeline`
  - 搜索与发现：`search box`、`explore page`、`news`、`sports`、`entertainment`
  - 用户扩展：`username changes`、`user likes`、`highlights`、`articles tweets`、`account analytics`
  - 关系/列表/社区：`followers`、`followings`、`lists`、`communities`
- 继续复用现有 `HandleProxy` 和统一策略代理模型。
- 保持 `resFormat=json` 默认注入和白名单参数治理。
- 在需要上游认证参数的接口上，只暴露网关别名参数，不直接暴露上游原始敏感参数名。
- 为新增能力补充 gateway、policy、swagger 的回归测试。

### 2.2 非目标

- 不新增 `provider-rpc` 专用 RPC 或协议字段。
- 不改响应体结构，不做字段裁剪或二次封装。
- 不开放上游原始参数名 `auth_token`、`ct0`、`proxyUrl` 给公网调用方。
- 不把当前任务扩展成策略系统重构，例如通用“参数组 OR 校验”模型重写。
- 不新增任何写接口或需要状态变更的能力。

## 3. 已确认决策

- `detail timeline` 已由现有 `GET /v1/tweets/detail` 覆盖，对应上游 `/base/apitools/tweetTimeline`，本次不新增重复路由。
- 公网路径继续保持现有资源风格，例如 `/v1/tweets/brief`、`/v1/search/box`、`/v1/users/likes`。
- `gateway-api` 只负责鉴权、参数校验、策略解析和透传，不做业务封装。
- 对需要敏感 Cookie 参数的上游接口，网关只开放别名参数：
  - `authToken -> auth_token`
  - `csrfToken -> ct0`
  - `restId -> rest_id`
  - `includeEntities -> include_entities`
  - `trimUser -> trim_user`
- `lists` 路由采用最小增量设计：公开一个简洁入口 `/v1/lists`，映射上游 `listByUserIdOrScreenName`，并在网关层实现 “`userId` 或 `screenName` 至少传一个” 的专用校验。

## 4. 方案对比

### 4.1 方案 A：只接不需要敏感参数的接口

做法：

- 只接无需 `auth_token` / `ct0` 的上游接口。
- 其余接口留待后续再做。

优点：

- 变更最小。
- 不需要调整现有敏感参数治理。

缺点：

- 覆盖不完整，`mentions timeline`、`account analytics` 以及部分个性化查询能力会缺失。
- 与用户确认的接口范围不一致。

### 4.2 方案 B：推荐方案，公开别名参数并在网关内归一化

做法：

- 在 `gateway-api` 暴露简洁公开路径。
- `policy-rpc` 默认策略继续只声明公开参数白名单和上游路径。
- `gateway-api` 在透传前做公开参数到上游参数的归一化。
- 对需要特殊约束的路由做网关级补充校验。

优点：

- 保持现有架构不变。
- 能完整覆盖本次范围。
- 不会把上游原始敏感参数名直接暴露给公网调用者。

缺点：

- 网关层需要新增一小段参数别名和专用校验逻辑。

### 4.3 方案 C：直接允许原始 `auth_token` / `ct0` 透传

做法：

- 放宽当前网关对敏感上游参数名的限制。
- 调用方直接传原始上游参数。

优点：

- 实现最快。

缺点：

- 会削弱现有公开网关的安全边界。
- 与当前已存在的敏感参数拒绝策略冲突。
- 后续日志、监控和文档都更难收敛。

### 4.4 推荐结论

采用方案 B。它在不改现有代理架构的前提下满足完整范围，并保持对敏感上游参数名的封装与治理。

## 5. 推荐设计

### 5.1 公开路由、策略键与上游映射

除已存在的 `/v1/tweets/detail` 外，本次新增以下公开只读路由：

| 分类 | 公开路径 | 策略键 | 上游路径 |
| --- | --- | --- | --- |
| tweets | `/v1/tweets/brief` | `tweets_brief_v1` | `/base/apitools/tweetSimple` |
| tweets | `/v1/tweets/quotes` | `tweets_quotes_v1` | `/base/apitools/quotesV2` |
| tweets | `/v1/tweets/retweeters` | `tweets_retweeters_v1` | `/base/apitools/retweetersV2` |
| tweets | `/v1/tweets/favoriters` | `tweets_favoriters_v1` | `/base/apitools/favoritersV2` |
| users | `/v1/users/mentions-timeline` | `users_mentions_timeline_v1` | `/base/apitools/mentionsTimeline` |
| search | `/v1/search/box` | `search_box_v1` | `/base/apitools/searchBox` |
| search | `/v1/search/explore` | `search_explore_v1` | `/base/apitools/explore` |
| search | `/v1/search/news` | `search_news_v1` | `/base/apitools/news` |
| search | `/v1/search/sports` | `search_sports_v1` | `/base/apitools/sports` |
| search | `/v1/search/entertainment` | `search_entertainment_v1` | `/base/apitools/entertainment` |
| users | `/v1/users/username-changes` | `users_username_changes_v1` | `/base/apitools/usernameChanges` |
| users | `/v1/users/likes` | `users_likes_v1` | `/base/apitools/userLikeV2` |
| users | `/v1/users/highlights` | `users_highlights_v1` | `/base/apitools/highlightsV2` |
| users | `/v1/users/articles-tweets` | `users_articles_tweets_v1` | `/base/apitools/UserArticlesTweets` |
| users | `/v1/users/account-analytics` | `users_account_analytics_v1` | `/base/apitools/accountAnalytics` |
| users | `/v1/users/followers` | `users_followers_v1` | `/base/apitools/followersListV2` |
| users | `/v1/users/followings` | `users_followings_v1` | `/base/apitools/followingsListV2` |
| lists | `/v1/lists` | `lists_v1` | `/base/apitools/listByUserIdOrScreenName` |
| communities | `/v1/communities` | `communities_v1` | `/base/apitools/getCommunitiesByScreenName` |

设计约束：

- 公网路径保持当前项目的简洁风格，不直接镜像上游原始路径名。
- 现有 `GET /v1/tweets/detail` 继续保留，不重复添加 `detail timeline` 路由。
- `policy-rpc` 仍使用 `<resource>_<action>_v1` 这一风格的策略键。

### 5.2 公开参数模型

新增路由的公开参数按“对外可读、与现有路由风格一致”的原则设计。

| 公开路径 | 必填参数 | 可选参数 |
| --- | --- | --- |
| `/v1/tweets/brief` | `tweetId` | `cursor` |
| `/v1/tweets/quotes` | `tweetId` | `cursor`, `authToken` |
| `/v1/tweets/retweeters` | `tweetId` | `cursor`, `authToken` |
| `/v1/tweets/favoriters` | `tweetId` | `cursor`, `authToken` |
| `/v1/users/mentions-timeline` | `authToken` | `csrfToken`, `sinceId`, `maxId`, `includeEntities`, `trimUser` |
| `/v1/search/box` | `words` | `searchType` |
| `/v1/search/explore` | 无 | 无 |
| `/v1/search/news` | 无 | 无 |
| `/v1/search/sports` | 无 | 无 |
| `/v1/search/entertainment` | 无 | 无 |
| `/v1/users/username-changes` | `screenName` | 无 |
| `/v1/users/likes` | `userId` | `cursor`, `authToken` |
| `/v1/users/highlights` | `userId` | `cursor`, `authToken` |
| `/v1/users/articles-tweets` | `userId` | `cursor`, `authToken` |
| `/v1/users/account-analytics` | `restId`, `authToken` | `csrfToken` |
| `/v1/users/followers` | `userId` | `cursor` |
| `/v1/users/followings` | `userId` | `cursor` |
| `/v1/lists` | `userId` 或 `screenName` 二选一至少一个 | 无 |
| `/v1/communities` | `screenName` | 无 |

统一规则：

- `policy-rpc` 继续为每条策略声明 `allowed_params`、`required_params`、`denied_params`、`default_params`。
- `default_params` 统一注入 `resFormat=json`。
- 原始敏感参数名 `proxyUrl`、`auth_token`、`ct0` 在公开网关层仍然视为非法输入。

### 5.3 参数归一化与敏感参数治理

参数归一化逻辑集中放在 `gateway-api` 现有归一化流程，不改 `provider-rpc` 协议。

本次新增的归一化规则：

- `/v1/tweets/brief`：`tweetId -> id`
- 所有允许认证别名的路由：`authToken -> auth_token`
- `mentions timeline` 与 `account analytics`：`csrfToken -> ct0`
- `account analytics`：`restId -> rest_id`
- `mentions timeline`：`includeEntities -> include_entities`
- `mentions timeline`：`trimUser -> trim_user`

安全约束：

- `sanitizeUpstreamQuery()` 仍然负责白名单筛选和默认参数注入。
- 网关继续拒绝原始敏感参数名，避免调用方跳过别名层。
- 请求日志中的敏感查询参数需要同时脱敏原始名和公开别名：
  - `auth_token`
  - `ct0`
  - `authToken`
  - `csrfToken`

### 5.4 特殊校验规则

当前策略模型只支持 `required_params` 的“全部必填”语义，不支持原生 OR 约束，因此本次采用最小增量实现：

- `lists_v1` 在网关层增加专用校验：`userId` 或 `screenName` 至少传一个。
- 该校验发生在默认白名单筛选后、provider 调用前。
- `policy-rpc` 对 `lists_v1` 的 `required_params` 留空，避免与 OR 语义冲突。

除 `lists_v1` 外，其余接口继续复用现有必填参数校验函数。

### 5.5 组件改动范围

#### 5.5.1 `gateway-api`

需要修改：

- `apps/gateway-api/gateway.api`
- `apps/gateway-api/internal/handler/routes.go`
- `apps/gateway-api/internal/service.go`
- `apps/gateway-api/internal/service_test.go`
- `apps/gateway-api/swagger_test.go`

职责：

- 增加新的公开路由定义和请求结构。
- 保持所有新接口继续走统一 `HandleProxy`。
- 扩展参数归一化、日志脱敏和 `lists` 路由专用校验。
- 通过 swagger 测试保证文档对外可见。

#### 5.5.2 `policy-rpc`

需要修改：

- `apps/policy-rpc/internal/service.go`

职责：

- 在 `defaultPolicies()` 中新增默认策略。
- 为每条策略写明公开路径、上游路径、参数白名单、必填参数、拒绝参数、默认参数。
- 对需要别名的接口，策略层只认识公开参数名，不直接暴露原始敏感参数名。

#### 5.5.3 `provider-rpc`

本次不改协议，也不新增专用 RPC。现有策略执行逻辑足够承接新增路由。

### 5.6 错误处理

错误处理继续复用当前网关模式：

- 非白名单参数直接返回无效请求错误。
- 缺少必填参数直接返回无效请求错误。
- `lists_v1` 若同时缺少 `userId` 与 `screenName`，返回无效请求错误，错误文案显式说明二选一约束。
- 上游返回的成功/失败响应继续通过现有代理链路透传，不新增网关层业务级错误翻译。

### 5.7 测试策略

本次至少覆盖以下测试面：

#### 5.7.1 `gateway-api` 单测

- 每个新增路径都能解析到对应策略并调用 provider。
- `resFormat=json` 会自动注入。
- `tweetId -> id`、`authToken -> auth_token`、`csrfToken -> ct0`、`restId -> rest_id` 等映射正确。
- 直接传 `auth_token`、`ct0`、`proxyUrl` 仍会被拒绝。
- `lists_v1` 能通过 “`userId` / `screenName` 至少一个” 校验。

#### 5.7.2 `policy-rpc` 单测

- 默认策略集合包含全部新增策略键。
- 关键路由的 `public_path`、`upstream_path`、参数集合和默认参数符合设计。
- `lists_v1` 的 `required_params` 为空，避免错误表达 OR 语义。

#### 5.7.3 swagger 测试

- swagger 文档包含新增公开路径。
- 每条路由的 query 参数与是否必填符合设计。
- 认证头部描述继续保持现有 dev/prod 差异规则。

## 6. 实施顺序

1. 更新 spec 并确认无歧义。
2. 在 `gateway.api` 增加公开路由与请求类型。
3. 生成或同步 `routes.go`。
4. 在 `policy-rpc` 默认策略中加入新增策略。
5. 在 `gateway-api` 扩展参数归一化、日志脱敏和 `lists` 专用校验。
6. 先补 failing tests，再补实现。
7. 跑 `gateway-api`、`policy-rpc` 和 swagger 相关测试。
8. 视需要再跑全量回归。

## 7. 风险与缓解

### 7.1 上游参数要求与文档存在偏差

风险：

- Readme 文档可能与真实上游行为存在轻微差异，例如某些可选认证参数在真实环境下实际必需。

缓解：

- 保持公开路由和策略键稳定。
- 若真实上游行为要求更严格，只在默认策略或网关归一化层微调，不改变公网路径。

### 7.2 路由数量显著增加

风险：

- 本次新增 19 条公开路由，容易遗漏 swagger、路由表或默认策略中的任意一处。

缓解：

- 通过路由、默认策略、swagger 三层测试交叉校验。
- 实施阶段优先按分类提交，缩小回归面。

### 7.3 敏感参数治理回退

风险：

- 为支持认证别名时，可能误放开原始 `auth_token` / `ct0`。

缓解：

- 明确要求只允许公开别名。
- 单测同时覆盖“别名通过、原始名拒绝”。
