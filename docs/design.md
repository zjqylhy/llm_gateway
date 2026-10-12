# 网关技术方案

同一 Go 代码库提供 gateway、admin、worker 三个进程。开发模式在一个进程中组合运行，使用内存存储和确定性的本地 provider；生产模式要求 MySQL、Redis、Kafka、etcd、Milvus 配置，不允许自动回退到内存。

worker 显式使用 Kafka ConsumerGroup generation 获取分区分配，再创建不带 GroupID 的分区 reader。gateway/admin/初始化命令与恢复 reader 不加入计量组；恢复以 SQL checkpoint 定位，不沿用最新组 offset。每分区按读取顺序处理，所有权代次/epoch 拒绝旧 worker 写账。

## 模块

应用路由通过 `Config.app_routes` 完整覆盖获授权的全局别名，统一解析用于 Chat、Embedding、Rerank 和灰度。调用记录保存应用名与路由作用域/策略/版本快照，灰度按实际作用域采样和回滚。控制台分应用业务与网关管理，业务计量摘要按应用权限返回，运维详情保持管理员权限。规格与验收见 [application-governance-spec.md](application-governance-spec.md) 和 [application-governance-validation.md](application-governance-validation.md)。

domain 定义应用、凭据、模型、路由、调用事件和账本。store 使用版本化配置快照，MySQL 事务防止并发管理操作丢失。config 发布至 etcd 并记录实例版本。security 管理摘要、加密、会话和权限。governance 实施全局额度、预占和熔断。provider 负责协议转换及增量读取。cache 隔离向量和响应。eventbus 持久化事件；worker 使用唯一键和事务实现账本幂等。

## 计量与一致性

started 事件持久化后才能调用厂商，completed 事件在正常结束响应之前持久化。Kafka 至少一次投递，MySQL 以事件键去重。usage 修正使用版本递增的事件，账本按差额更新而非再次加总。未完成调用保留 started 记录并进入对账视图。

Redis 预占与结算使用原子操作和 attempt 幂等键；已完成事件先入账后结算预占。预占失效不能直接抹除潜在费用，需转为 pending。数据库与 Redis 不使用跨系统事务，依靠可重放事件及幂等结算恢复。

Redis 治理操作有 1 秒总期限；建连及池等待各上限 1 秒，读取/写入各 250 毫秒，并显式限制建连和命令重试次数。操作期限覆盖叠加的 DNS/连接/重试等待，防止 Redis 停止后 HTTP 请求长时间挂起。首次启动和离线恢复的整体期限另行控制。

生产网关先将 started 保存至 SQL outbox，为 completed 预留空间，再等待 Kafka ACK。completed 尝试 outbox 后，即使 SQL 失败也继续尝试 Kafka；后台发送者从 outbox 补发。账本仅由消费者写入，消费者结算 Redis 后释放补偿容量。事件 ID/版本内容冲突明确失败，不靠 INSERT IGNORE 吞掉。

usage 修正事务合并版本分配、审计与 outbox，支持 Idempotency-Key 重试。计量依赖故障通过 SQL 控制状态和 Redis 冻结标记停止新增付费调用，既有重放及结算继续；readyz 同时核验计量状态。清除冻结需停写核验，未知 started 必须核实用量后修正。

消费者在同一 SQL 事务写账本与分区 next_offset，再结算 Redis、确认 outbox 交付、推进 Redis 分区结算水位，最后提交 Kafka offset。已实现只读 metering-recover plan、显式 apply、存量 checkpoint-init 及共用独占维护锁；恢复可中断续跑，完成保持冻结。来源核验使用集群/分区、stream header 与消息指纹，当前客户端不提供 topic UUID。完整规格见 [metering-recovery-spec.md](metering-recovery-spec.md)。

worker 在独占 SQL 连接锁下检查来源、SQL 控制、Redis generation 和已提交 Kafka/Redis 水位，每秒续发有效期 3 秒的 etcd 许可。冻结状态与许可同处配置前缀下的 metering 命名空间；网关通过 etcd 事务读取两者，登记准备/在途 attempt，Redis Lua 再检查绝对到期时间、全局和应用代次。许可不可续期或存储不明时拒绝新费用；SQL 检查点可以领先 Redis，不按正常异步延迟冻结。

正式核验将目标应用置 quiescing，串行化新入口登记、started 保存与管理修正，等待在途登记退出，再捕获 Kafka 固定水位并排空该应用补偿。分页比较月份计数、预占、最新版本、事件哈希以及孤立键；失败保持该应用 frozen，未知 started 不按超时清零。详细核验串行轮转，成功后至少隔 60 秒再核验同一应用，最多等待 150 秒；该应用期间会拒绝新付费调用。控制台与指标展示报告、冻结原因、未核验数量和轮转时间，这不是无中断或所有租户 10 秒内必检的方案。

每实例默认最多 8 个付费在途 attempt（GW_MAX_METERING_INFLIGHT），槽位覆盖准备、上游调用及完成计量收尾，等待最多 1 秒；started/completed 保存各最多 8 并发，started 保存槽位满直接拒绝。仅限制登记并发会把已接单费用集中到完成队列并耗尽 SQL 预算，不能保护收尾。登记前预检 outbox 剩余容量以减少满容量时的预占/取消洪峰；最终容量检查仍在原子 outbox 事务中。生产门禁拒绝且尚未调用厂商时保留指标，不再把请求汇总加入受压队列；已经产生的费用、取消和完成事件仍须持久化。

## 安全与边界

文本重排通过模型 `rerank_protocol` 显式选择兼容或 DashScope 协议，分别转换请求与响应。模型声明文档数、单条输入及总输入限制；保守预占考虑查询参与每个文档评分。厂商 `total_tokens` 作为纯输入用量结算，未知用量保持估算；结果验证失败也保留已报告的有效用量。两种接口共用既有请求治理与 attempt 计量链路，控制台可配置模型及路由，详情见 [rerank-spec.md](rerank-spec.md)。

管理 Cookie 为 HttpOnly、SameSite=Strict，生产环境 Secure；写操作校验 CSRF 和 Origin。凭据用 AES-GCM，加密主密钥由部署 Secret 提供。日志不打印 token、请求正文或上游完整错误体。厂商地址在管理时校验 HTTPS，生产禁止内网及重定向，以降低 SSRF 风险。

缓存响应和向量属于应用数据。实时请求由应用请求头明确声明；敏感字段检测是额外保护，不能代替应用正确标记。缓存匹配时同时核对 route/model/parameter scope 和有效期。

## 验证边界

当前 Windows 已具备 Docker Desktop，本地依赖与应用容器已启动。`scripts/verify-infrastructure.ps1` 为完整数据链路及配额恢复创建独立验收资源，使用真实依赖和可控模拟上游。测试在生产模式构造运行时，仅替换测试进程的上游 HTTP 客户端；生产启动及内网地址限制保持不变。真实 API、容量和云部署验证单独记录，具体状态见 `validation.md`。

## 资源隔离与有界处理

配额客户端与缓存客户端分别连接独立 Redis，连接池均最多 32 条连接；仅缓存允许淘汰。预占 Lua 在创建新 attempt 前读取 INFO memory 并核验策略/水位；既有 attempt 的幂等返回在水位检查之前，完成结算不受 90% 接单阈值限制，仍受 Redis 硬限额约束。该检查不会增加一次网络往返。内存预算及权限无法核验时拒绝付费调用，恢复操作需先扩容并核对 pending。

HTTP 监听器使用 LimitListener 约束 TCP 接入；Engine/Admin 各自共享 admission 槽，控制台 playground 同时受管理与业务槽约束。拒绝在鉴权和正文解析前执行；响应包装器保留 Unwrap/FlushError，按块更新底层 socket 写期限，不将 SSE 的总时限当作单次写期限。监控抓取同样限并发和处理期限。

Kafka 事件及预取、配置快照、记录查询和报表均限制结果规模。MySQL 连接保留 socket 超时；管理查询有 30 秒上下文期限。Go 软内存预算配合容器硬限额使用，不保证任意配置和负载下无 OOM。离线账本/去重达到容量后失败关闭；缓存按字节淘汰，避免只限制条数而被大响应撑满。
