# Collector Worker 崩溃一致性修复设计

**日期：** 2026-04-19

**状态：** 待审查草案

**目标：** 修复 `collector-worker-rpc` 在 worker 进程崩溃场景下可能出现的 NDJSON 静默丢数问题。在本次修复中，系统接受极小概率的重复 NDJSON 行，但不再接受“帖子已标记 seen/usage，结果文件却永久缺失”的数据丢失模式。

## 1. 背景

当前 `collector-worker-rpc` 在执行分页采集时，同时维护两类状态：

- 文件输出状态：将帖子原始结果追加到 `.part` NDJSON 文件
- 账本状态：将帖子写入任务级去重账本（`task_seen_posts`）与关键词自然月计量账本（`keyword_monthly_usage`）

当前实现的问题在于，账本状态提交发生在 NDJSON 记录真正刷出之前。若 worker 在页面处理中途崩溃，则可能出现以下结果：

- 帖子已经写入 `task_seen_posts`
- 帖子已经写入 `keyword_monthly_usage`
- 但帖子对应的 NDJSON 行仍停留在 `bufio.Writer` 中，未进入 `.part` 文件

此时重试路径会把该帖子视为重复数据直接跳过，最终造成永久结果丢失。

本问题对应的 review finding 为：

- `apps/collector-worker-rpc/internal/runner.go` 中“先记账本，后写文件”导致 worker crash recovery 时可能静默丢数

## 2. 设计边界

### 2.1 本次覆盖的故障模型

本次修复仅覆盖以下场景：

- `collector-worker-rpc` 进程崩溃
- goroutine panic
- worker 被 kill 或被调度系统重启，但共享卷和宿主机本身未丢失内核页缓存

### 2.2 本次不覆盖的故障模型

本次修复不试图覆盖以下更强故障模型：

- 宿主机突然断电
- Pod 所在节点崩溃导致尚未真正落盘的数据丢失
- 共享卷不保证 `rename` / advisory lock / 缓存刷盘语义

在这些更强故障模型下，仅依赖 `bufio.Flush()` 不足以提供严格持久化保证，需要引入 `fsync`、单一持久化域或 outbox/journal 机制。这不属于本次短期修复范围。

### 2.3 本次接受的行为变化

本次修复明确接受以下取舍：

- 接受 worker 在“文件已 flush、账本未提交”窗口崩溃时，重试后产生极小概率的重复 NDJSON 行
- 不接受任何由于账本先提交而导致的永久 NDJSON 丢数

换句话说，本次修复把执行语义从“可能静默丢数”调整为“at-least-once 文件输出”。

## 3. 现状问题分析

当前页面处理逻辑可概括为：

1. 遍历页面帖子
2. 逐条调用 `RecordKeywordMonthlyUsage`
3. 逐条调用 `RecordTaskSeenPost`
4. 对首次见到的帖子调用 `writer.AppendAndFlush`
5. 页面结束后调用 `persistRunProgress`

问题不在于是否使用了 `bufio.Writer`，而在于提交顺序：

- `usage/seen` 的事实来源是数据库
- 输出文件的事实来源是 `.part`
- 两者没有统一事务
- 当前却把数据库状态推进到了文件状态之前

因此只要 worker 在第 2、3 步和第 4 步之间崩溃，就会出现数据库“认为已成功处理”、文件“实际上没有该记录”的不一致。

## 4. 方案对比

### 方案 A：保持现状，仅恢复逐条 `AppendAndFlush`

优点：

- 改动最小
- 能缩小部分丢数窗口

缺点：

- 仍然无法消除“先记账本、后写文件”的根因
- 每条记录都 `Flush`，吞吐退化明显
- 与当前引入缓冲写的优化目标冲突

结论：不采用。

### 方案 B：先批量写文件并 `Flush`，后批量提交 `usage/seen/progress`

优点：

- 直接修复当前 review finding 的根因
- 页面内只需少量 `Flush`，性能明显优于逐条 `AppendAndFlush`
- 不需要在本次修复中引入 Redis journal 或新的存储子系统
- 满足“防 worker 进程崩溃”的已确认故障模型

缺点：

- 在 `Flush` 之后、账本提交之前崩溃时，重试可能产生重复 NDJSON 行
- 需要补充“先查 seen、后批量落账本”的执行路径，避免历史重复数据被重新写入文件

结论：推荐采用。

### 方案 C：引入 Redis pending journal / outbox

优点：

- 可进一步缩小文件与账本之间的不一致窗口
- 为后续更强的一致性模型打基础

缺点：

- 新增一套提交协议和恢复路径
- 复杂度明显上升
- 对当前“只防 worker 崩溃”的目标来说过重

结论：不作为本次短期修复方案。

## 5. 推荐方案

采用“单页批量写文件，`Flush` 成功后再统一提交账本与进度”的方案。

核心原则：

- 页面内文件输出先行
- 账本提交后行
- 进度状态最后提交
- 恢复策略优先保证“不丢”，允许极小概率“重复”

## 6. 处理流程设计

### 6.1 页面内处理顺序

对单页结果按如下顺序处理：

1. 解析当前页帖子列表
2. 提取本页全部 `post_id`
3. 查询当前任务下哪些 `post_id` 已经存在于 `task_seen_posts`
4. 在内存中过滤：
   - 跳过历史已见帖子
   - 跳过当前页内重复帖子
   - 仅保留待写入文件的 `pendingRecords`
5. 将 `pendingRecords` 逐条 `writer.Append(...)`
6. 页面内全部追加完成后，仅执行一次 `writer.Flush()`
7. `Flush` 成功后，再统一提交：
   - 关键词自然月计量
   - 任务 seen 账本
   - run progress

### 6.2 为什么必须先查 seen

如果简单把 `RecordTaskSeenPost` 后移，而不增加“先查是否已见”的步骤，则历史重复帖子仍会先写入 NDJSON，再因为后续 `RecordTaskSeenPost` 返回重复而无法回滚文件。这会把去重语义从“避免重复输出”退化成“只避免重复记账”，不可接受。

因此必须新增批量读取接口，在文件写入前先完成历史去重判断。

### 6.3 崩溃窗口语义

#### 窗口 1：崩溃发生在 `writer.Flush()` 之前

结果：

- 文件中没有这批新记录
- `usage/seen/progress` 也尚未提交

恢复后：

- 重试会重新处理该批帖子
- 不会丢数据
- 不会因为历史账本而错误跳过

#### 窗口 2：崩溃发生在 `writer.Flush()` 之后、账本提交之前

结果：

- `.part` 文件中已经存在这批记录
- `usage/seen/progress` 还没有全部提交

恢复后：

- 重试时可能再次把同一批帖子写入文件
- 会产生重复 NDJSON 行
- 但不会出现静默丢数

#### 窗口 3：崩溃发生在账本提交之后、`UpdateRunProgress` 之前

结果：

- 文件已经有数据
- `usage/seen` 已提交
- `progress` 可能滞后

恢复后：

- 重试时会基于已提交的 seen 跳过历史帖子
- 不会丢数据
- 最多造成进度状态回补

## 7. Store 接口变更

### 7.1 新增批量查询接口

`workerStore` 需要新增批量查询接口，用于在文件写入前读取任务内历史已见帖子：

```go
ListTaskSeenPosts(ctx context.Context, taskID string, postIDs []string) (map[string]bool, error)
```

要求：

- 返回值表示给定 `post_id` 是否已存在于该 `task_id` 的 seen 账本中
- 空输入返回空集合
- 不负责页面内去重；页面内去重在 runner 内存中完成

### 7.2 是否需要新增批量提交接口

推荐新增一个页面级批量提交接口，例如：

```go
CommitFlushedPage(ctx context.Context, params flushedPageCommitParams) error
```

该接口将以下动作放进同一事务：

- 批量写入 `keyword_monthly_usage`
- 批量写入 `task_seen_posts`
- 更新 `collector_task_runs`

推荐原因：

- 能减少崩溃窗口中的中间态
- 降低多次 round trip 开销
- 让页面级提交语义更清晰

但如果为了缩小实现范围，也允许第一版先复用现有接口逐条提交，只要提交顺序改为“先 `Flush`，后账本，再进度”即可。该简化方案的行为仍满足本次目标。

## 8. Runner 逻辑调整

### 8.1 新的计数口径

需要在内存中显式区分以下计数：

- `duplicateCount`
  - 包含历史已见帖子
  - 包含同页重复帖子
- `newCount`
  - 仅在文件成功 `Flush` 且账本提交成功后累加
- `fetchedCount`
  - 维持现有页面抓取口径

### 8.2 `required_count` / `per_run_count`

本次方案保持现有精度要求，不引入“按批次明显超采”的行为变化。

实现要求：

- 在页面过滤阶段基于“待写入文件的唯一新帖子”计算可消费数量
- 达到 `required_count` 或 `per_run_count` 时，可以只截取页面内前 N 条待提交记录
- 不需要为了减少 `Flush` 次数而故意把停止点推迟到整页之后

也就是说，本次方案接受“页面级 flush”，但不接受“页面级超采”。

### 8.3 `persistRunProgress`

`persistRunProgress` 保持“调用前先确保 writer 已经 `Flush` 成功”的约束。

该函数不再承担纠正文件/账本顺序的问题；顺序保证必须由页面处理主流程负责。

## 9. PostgreSQL / Redis 后端要求

### 9.1 PostgreSQL 实现

对于 `pgredis` 后端：

- 新增 `ListTaskSeenPosts` 查询
- 若实现页面级批量提交，则在 PostgreSQL 事务中统一完成 `usage/seen/progress`
- 页面级批量提交中使用 `INSERT ... ON CONFLICT DO NOTHING` 保持幂等

### 9.2 Memory 实现

对于 memory store：

- 同样新增 `ListTaskSeenPosts`
- 行为需与 PostgreSQL 版本一致
- 以保证单元测试和集成测试在内存后端下仍能覆盖相同语义

### 9.3 Redis 的角色

本次修复不新增 Redis journal，也不引入新的 Redis 一致性协议。

Redis 仍仅承担：

- run lease
- worker 协调
- 原有调度协作职责

## 10. 测试设计

### 10.1 Runner 单元测试

新增或调整以下测试：

- 历史已见帖子不会再被写入 NDJSON
- 同页重复帖子仅写一次
- `Flush` 之前失败时，不会留下已提交 seen/usage
- `Flush` 之后、账本提交之前失败时，允许文件重复，但不允许数据丢失
- `required_count` / `per_run_count` 仍按唯一新帖子精确截断
- `duplicateCount` 对历史重复与页内重复都计数正确

### 10.2 Store 测试

新增以下测试：

- `ListTaskSeenPosts` 在 PostgreSQL 后端返回正确集合
- `ListTaskSeenPosts` 在 memory 后端返回正确集合
- 若实现批量提交接口，验证其事务性和幂等性

### 10.3 回归测试

重点确认以下行为不被破坏：

- 周期任务和区间任务的输出路径与完成状态
- `resume_cursor` / `resume_offset` 更新逻辑
- NDJSON `.part -> final` 的提交流程
- 任务首次见贴去重与关键词月度计量口径

## 11. 风险与后续演进

### 11.1 已接受风险

本次修复后仍保留一个已知风险：

- 若 worker 在 `Flush` 之后、账本提交之前崩溃，重试可能造成重复 NDJSON 行

该风险是本次明确接受的设计取舍，因为它优于静默丢数。

### 11.2 后续演进方向

若后续要覆盖更强故障模型，应优先考虑以下方向之一：

- 页面级 `Flush + fsync`
- Redis / PostgreSQL outbox journal
- 以数据库为唯一真实来源，再异步导出 NDJSON

这些方案不属于本次短期修复范围。

## 12. 实施范围

本次实现只覆盖 Finding 1，不顺带修改以下问题：

- worker 本地 policy cache
- gateway `X-Forwarded-For` 信任边界
- gateway token TTL 限制
- compose 默认 JWT secret
- 其他 review findings

这样可以确保本次改动范围单一、验证边界清晰。

## 13. 验证清单

- [ ] `runner` 在 worker 崩溃故障模型下不再出现静默丢数
- [ ] 历史重复帖子不会被重新写入 NDJSON
- [ ] 同页重复帖子不会被重复写入 NDJSON
- [ ] `required_count` / `per_run_count` 仍保持精确截断
- [ ] `memory` 与 `pgredis` 后端语义一致
- [ ] `go test ./apps/collector-worker-rpc/internal/...` 通过
- [ ] 相关集成测试在既有基线下保持不退化
