# 计量故障恢复补强：需求与技术方案

> 2026-10-18 后续变更：运行实现统一后台容量，协议版本及升级步骤见 [唯一后台规格](outbox-single-capacity-spec.md)。本文保留此前设计或部署的历史事实。

日期：2026-10-11。状态：用户已确认按后续讨论实施，分阶段验证；具体完成范围以 validation.md 为准。最终分工为网关保存 outbox，消费者统一入账。现有行为与验收见 [design.md](design.md)、[operations.md](operations.md)、[validation.md](validation.md)。

阶段 1、2、3 的代码及验收结果均已获人工确认。阶段 3 实现短期许可、Redis 代次与结算水位校验、应用屏障核验和管理监控；用户已确认实现提交 1b1d833 的代码、测试结果及性能边界。性能目标尚未达成，具体结果见 validation.md。恢复、基线初始化、核验和解冻命令共用数据库级独占锁。

## 1. 目标与保证边界

保留 Redis 原子预占、MySQL 权威账本、Kafka 至少一次投递。补齐已产生费用但事件发布失败的补偿、旧备份恢复后的回放、配额损坏检测和故障演练。本轮不替换成 SQL 配额，不引入 Hive/归档，不操作现有业务数据。

- 正常成功响应仍以 completed 获得 Kafka ACK 为必要条件；SSE 已输出部分内容时，计量失败不得发送正常 DONE。尚未结算时保留预占。
- 相同 event_id/revision 重试不能重复计费；相同 ID 不同载荷必须报冲突并冻结，不能被 INSERT IGNORE 静默吞掉。
- MySQL 或 Kafka 单独失效时，只有完整 completed 被另一存储确认持久化，才能保证恢复后自动补账。接单仍失败关闭，不能因为有备用保存路径而无限产生费用。
- 上游调用不是数据库事务。两路持久化同时失效，或进程收到 usage 前崩溃，可能只留下 started 与预占，需要厂商账单和人工修正。不能用零费用或超时释放掩盖未知费用。
- 旧备份恢复要求备份与检查点一致，所需事件仍在 Kafka 或核验过的恢复来源中。来源缺失时冻结并报缺口，不承诺无条件零数据丢失。
- reserve 不是厂商 usage 的已证明上界；actual 超过 reserve、向上修正仍可能超预算。本轮不能宣称解决所有超额路径。

## 2. 现有代码的缺口

| 路径 | 已有能力 | 需要补强 |
| --- | --- | --- |
| SQL 入账失败 | worker 不提交 Kafka offset | 有界重试、故障状态、恢复检查点 |
| SQL 成功、Redis 失败 | SQL 幂等重放，再结算 Redis | 跨实例门禁与对账证据 |
| Redis 数据丢失 | ready 丢失拒绝；quota-rebuild 重建 | ready 尚在但 key 缺失、旧快照和版本倒退检测 |
| completed 发布失败 | 返回计量错误，保留预占 | 持久化补偿与后台发送 |
| SQL 恢复旧备份 | consumer 沿用当前组 offset | 从备份内 SQL 检查点回放 |
| usage 修正 | 事务分配 revision 后发布 | revision、审计、账本和补偿同一事务 |

Engine.Refresh 在 SQL 心跳报错前已更新配置时间；配置新鲜不等于计量健康。恢复门禁须独立于配置刷新与现有 ready 标记。

## 3. SQL outbox 与付费调用

选择 MySQL outbox 保存补偿事件，gateway 无需本地 WAL/独立持久卷，适用于 Compose 和云上多实例。outbox 记录发送状态，不作为第三份可累加的账本。

1. Redis 原子检查额度并预占，保持现有预算串行化边界。
2. SQL 短事务检查全局/应用恢复门禁和补偿容量，写 started outbox，为 completed 留足空间。网关不更新账本；事务失败或结果不明时不调用厂商。
3. started 获得 Kafka ACK 后才调用厂商；后台重复发送沿用同一事件标识。
4. 得到 completed 后先尝试 SQL 事务保存 outbox；SQL 失败仍在独立超时预算内尝试 Kafka，不能短路健康的备用通道。Kafka 确认后无需为了补偿再补写 outbox；消费者补入账并释放原有补偿容量。SQL 超时若实际已提交，后续重复发送仍应幂等。
5. Kafka ACK 后可以正常结束；SQL 成功但 Kafka 未 ACK 时返回计量错误，后台补发。两路都失败时保留预占、冻结并标记待核实。
6. worker 事务更新账本与检查点，按版本结算 Redis，再提交 Kafka offset。SQL 入账领先 Redis 的窗口由预占保护。

Redis 预占与步骤 2 不原子。明确未调用上游的失败路径生成零费用取消事件，在独立有界上下文中持久化并结算；取消失败保留孤立预占。崩溃且不能证明未调用厂商时不自动取消。对账必须纳入 Redis 中没有 SQL started 的记录，不能只计算 SQL pending。

覆盖 started/completed、重试/降级、内部 Embedding、管理端 usage 修正。request 汇总也补偿，但不能据其推断付费 attempt 已结算。管理修正的版本分配、审计和 outbox 合并事务，使用操作幂等标识，最终账本仍由消费者更新。

### 数据与发送约束

| 新表 | 关键字段/约束 | 职责 |
| --- | --- | --- |
| gw_metering_outbox | event_id 唯一、attempt_id、revision、app_id、payload/hash、state、claim_epoch、lease_until、next_retry_at | 完整事件与发送状态；相同 ID 内容不可变 |
| gw_metering_capacity | scope/id、reserved_bytes、pending_bytes、version | 全局及应用的补偿容量预留 |
| gw_metering_checkpoint | stream_id、topic、partition、next_offset、owner_epoch | 与 SQL 入账一致的回放位置 |
| gw_metering_control | scope/id、generation、state、reason、audit_id | 冻结与恢复代次 |
| gw_metering_recovery | recovery_id、manifest_hash、phase、progress、audit_id | 恢复任务与核验记录 |

发送者用短事务 FOR UPDATE SKIP LOCKED 领取队列，事务外发 Kafka，按 claim_epoch 条件更新。ACK 丢失、租约过期和进程崩溃允许重复发送；不可凭“发送中”删除。SKIP LOCKED 仅用于队列，财务核验不能使用这种不完整视图。[MySQL 锁定读文档](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)

建议默认每批最多 32 条且合计 4MiB，单次发布 3 秒，领取租约 15 秒，退避 1～30 秒加抖动。达到重试阈值只告警/冻结，不丢弃。事件允许乱序，通过 attempt/revision 单调应用保证正确性；同一 ID 内容冲突不能自动覆盖。容量锁统一按全局、应用顺序获取，持锁期间不访问 Kafka/Redis。

新增接单与补偿任务不能耗尽所有 SQL 连接：分别限制入口和后台并发，给已产生费用的结算保留连接预算。具体池大小随压测确定，不能把每个进程的连接上限当作整个数据库的上限。全局容量行是潜在热点，首轮压测必须包含锁等待；若需要分片额度，另出设计，不能绕开原子容量约束。

建议未交付载荷预算全局 512MiB、每应用 64MiB，可分别配置。这是共享 SQL 实例内的逻辑容量，不是每租户独占物理内存/磁盘。新 attempt 按 started 实际字节加 1MiB completed 上限预留；已有费用不能等存储满后再拒绝 completed。修正单独预留，未投递事件禁止淘汰。

保留单事件 1MiB 上限；采样正文可舍弃，usage、费用、价格和幂等字段不可截断。completed 超限先移除采样正文，仍超限则报错/冻结、保留 pending。索引、undo/binlog、备份等不计入载荷预算，另需磁盘监控与余量；逻辑限额不保证 MySQL 永不写满。

只有 Kafka ACK、SQL 入账、Redis 结算证据都成立，才释放对应未交付容量、清除冗余 outbox payload；保留标识/哈希/状态用于审计。SQL 事件及账本不按日期删除。未完成、冲突和来源缺口不能通过清理换取接单空间。

outbox 将补偿容量或修正版本/审计与待发事件一起提交，账本由消费者更新。发送仍可能重复，消费者必须幂等；不能提供与上游、Redis、Kafka 的跨系统事务。[AWS outbox 模式说明](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)

## 4. 检查点与旧备份回放

consumer 将事件入账与分区 next_offset 放在同一 SQL 事务，再结算 Redis，最后提交组 offset。SQL checkpoint 不表示 Redis 已追平，需分别核验。

每分区只允许有效 owner_epoch，按读取顺序串行；重分配先取得所有权，旧 owner 的 SQL 事务受 fencing 拒绝。不能简单取 MAX(offset) 越过未处理事件，不能假定可见业务消息 offset 恰好加一。流位置与业务 revision 是两个概念。

组位置落后 SQL checkpoint 可幂等重放补 Redis；领先时停止消费/接单，要求明确恢复，不能默认为已处理。存量迁移须停写、排空、核验后在一致屏障下初始化检查点，禁止在线猜一个 offset。

已实现的维护命令：

- metering-recover plan：只读核验备份代次、SQL checkpoint、Kafka 来源/分区、earliest/end 与 outbox，输出脱敏 manifest、缺口和工作量。
- metering-recover apply：指定 manifest 哈希与 writers-stopped 确认，独占恢复锁、置 recovering，按分区回放并保存进度。
- metering-audit：一致屏障核验账本、配额、消费位置和孤立记录，输出审计报告，通过后才允许显式解冻。

恢复步骤：

1. 摘除付费入口，停止 gateway/admin 写账本、正常 consumer、outbox sender、自动修正及相关维护。备份当前状态、记录 Kafka 来源与位置。
2. 使用同一一致性备份恢复账本、outbox、checkpoint、控制表；切换新代次 recovering，不沿用快照 ready。
3. 核验来源身份、分区与保留区间。next_offset 小于 earliest、topic 被重建/分区缺失时拒绝回放，需要备份/binlog/核验归档或人工对账。
4. 捕获固定分区 end offset，从 SQL checkpoint 直接读到边界；恢复 reader 不加入正常消费组，不静默 reset 到 earliest/latest。
5. 停写状态下补发未交付 outbox，重新捕获终点并排空。有限批次、内存、超时，失败可从任务检查点重试。
6. 根据最新 SQL 账本与事件构建 Redis 新 generation 的结算、预占、幂等记录，核验所有分区、各应用月份、pending 及孤立记录；整个过程保持门禁。
7. 对齐正常消费组位置，审计变更前后值；无未解决缺口后发布恢复代次与接单许可，最后启动入口。

Kafka 超过保留窗口后旧位置可能不存在，自动位置重置无法补回删除的数据。[Kafka 消费位置说明](https://kafka.apache.org/41/configuration/consumer-configs/)。Go 实现须显式核验范围，不能直接套用 Java 客户端参数名。

当前 kafka-go 0.4.51 Metadata API 最高 v8，没有 topic UUID。实现核验 cluster ID、topic/group、分区集合、SQL stream ID，以及检查点最后一条消息的键/载荷/headers 指纹。新消息必须带 gw-stream-id；显式存量基线之前的旧消息允许原格式。来源指纹失效或锚点被保留策略删除时拒绝自动恢复，即使 next_offset 仍在保留区间；这比仅检查 next_offset 更严格。它不是 broker topic UUID 的等价替代，也不防止具有 Kafka 写权限者伪造同源消息。恢复依赖受控的计量 topic，cleanup.policy 必须为 delete，不允许 compaction。

apply 保存 replay/outbox/rebuild/complete 阶段；账本与检查点是持久化进度。同一个 manifest 可续跑，未完成任务禁止用新 manifest 绕过或解冻。补发后重新捕获有限终点，失败重试可能重复补发，账本幂等处理。恢复读者不加入消费组，执行前和 offset 对齐前检查正常组没有活跃成员。独占锁连接同时承载恢复事务；连接丢失会中断写账，重建 Redis 每批检查该连接。操作者仍须停止网关/admin/其他写入者，SQL 锁不能替代全系统维护窗口。

完成后核验账本、Redis 和分区位置，审计记录关联 manifest 哈希，控制状态保留 frozen。quota-rebuild 复用同一回放/重建流程，不再从当前组 offset 盲目消费；完成也不自动解冻。pending 仍需已核实的 usage 修正，再运行 metering-resume 并重启有本地冻结闩锁的实例。阶段 3 重建发布 Redis generation/结算水位，显式 resume 生成新代次和核验报告，正常 worker 再颁发短期许可。

当前本地 topic 保留 7 天、单副本，不具备云多副本容灾保证。可恢复备份窗口须短于事件保留窗口并留出发现/恢复时间。云环境需多副本、ACK/min ISR、SQL 备份/PITR 和演练；本轮不自动扩大保留期，不声称通过云容灾验收。

## 5. 门禁与一致性对账

控制状态为 healthy、frozen、recovering，包含 generation，与配置 ready 分离。SQL 持久化控制、etcd 发布冻结与短期许可，Redis Lua 校验代次/配额状态/许可；started SQL 事务再次检查控制。许可过期、来源未知、代次不符拒绝新调用，已有结算与补偿继续。

建议许可有效期 3 秒，worker 仅在 SQL、消费进度、Redis 代次和未解决告警可核验时续期。探测成功不清除已有冻结。失效后在许可期限加单次操作预算内停止新调用；不承诺瞬间中止已开始的上游。进程发现持久化故障须停本实例接单并尝试发布冻结；无法发布时靠 SQL started 门禁及许可过期限制其他实例。

旧 Redis 快照可能同时保留旧 ready/generation，代次校验不能独自证明一致，还需消费/结算进度与对账。受控恢复强制切换代次；任意 key 删除在检测前仍有窗口，首期保证有界检测后冻结，不能宣称瞬时检测一切损坏。

当前轻量检查每秒轮询许可、积压、未交付字节/调度年龄、结算水位与代次。新应用每次最多初始化 32 个；详细对账串行按应用分页轮转，成功后至少隔 60 秒再核验同一应用，孤立预占在详细核验中检查。展示完整轮转时间与未核验数量，不能把轻量检查周期宣称为所有租户差账发现时限，也不每轮扫描整本账。

正式对账先持久化冻结目标应用的新付费调用/修正，等待准备阶段及在途调用退出，排空 outbox/消费，再取得一致快照。分区中其他应用持续写入时，以捕获的水位核验，必要时全局停写。在途流式等待既有 120 秒上限，超时保持冻结，未知调用保留 pending。

核对 settled tokens/cost、reserved tokens/cost、pending、最新 revision、载荷哈希、孤立 Redis 记录。SQL 已入账、Redis 尚未结算属于合法延迟；屏障后仍不一致才认定差账。已完成事件可幂等补结算，未知费用、载荷冲突和来源缺口不能修成零。

应用差账优先冻结该应用；公共存储/代次异常冻结全局。管理员恢复必须关联核验报告、原因和操作者。启动检查不能把非空状态当作新部署初始化，也不能自动改 ready 绕过修复。

## 6. 实施与验收

用户已确认新增同步 SQL outbox 事务、账本由消费者统一写入、Kafka ACK 仍为正常成功必要条件。存量迁移及旧备份修复需要明确停写窗口；上述容量、批次和许可期限作为可调初始值。保留 Redis 的决定在完整链路压测后再复核。

分三阶段交付，每阶段验证、提交 commit、展示代码及测试结果供人工确认：

1. started/completed、取消/修正事务与 outbox：有界发送、容量预留、载荷冲突检测。
2. 分区 ownership/checkpoint、存量离线初始化、只读恢复计划/显式回放、quota-rebuild 代次整合。
3. 门禁、屏障对账、管理审计/监控，完整容器故障演练与重新压测。

只用隔离数据库/topic/组/Redis/控制前缀和专用容器，不强杀现有控制台依赖、不清空其数据。覆盖以下断点，恢复后逐应用核验账本、配额、版本与消费位置：

| 演练 | 验收条件 |
| --- | --- |
| 上游完成后 Kafka 失败、SQL 健康 | 计量报错，无正常 DONE；outbox 持久保存，重启后补齐、不重收费 |
| 上游完成后 SQL 失败、Kafka 健康 | completed 仍发布，新调用停止，SQL 恢复后补账正确 |
| SQL 提交后 Redis 失败/worker 被杀 | 预占保留，重复消费只按最新版本结算一次 |
| Kafka ACK 后 sender 被杀、未标记发送 | 重复发送不改变最终费用 |
| SQL 事务 ACK 丢失，取消/修正重试 | 未确认 started 不调用上游；同一操作不重复版本/收费 |
| SQL 恢复旧快照、组 offset 已前进 | 从备份检查点补回组位置之前的事件 |
| checkpoint 超出 retention/topic 重建 | plan/apply 明确报缺口，保持冻结 |
| rebalance 后旧 worker 继续写入 | fencing 拒绝，检查点不越过未处理事件 |
| Redis 部分 key 丢失/恢复旧 AOF | 检测、冻结、重建，不盲目清 pending |
| 两路存储同时失败/usage 前崩溃 | 保留已持久化 started/预占，待核实，不假装恢复精确费用 |
| 一个应用耗尽 outbox 预算 | 拒绝该应用新增；completed 有预留；其他应用在全局余量内可继续 |
| 同 ID 不同 payload | 报冲突、审计和冻结，不静默忽略 |
| 对账与流式调用重叠 | 等待屏障，不误报正常延迟，不超时释放未知费用 |

重新测量 overhead、SQL 池等待、outbox 积压、单/多应用并发、恢复时间。[此前配额对比](quota-comparison-report.md) 未包含新增同步 SQL 链路，不能沿用其吞吐数字。先测量再定容量/恢复 SLO，少量故障实验不证明全年可用性。

测试与交付状态记录在 validation.md，未完成部分不得据本文视为已上线。存量部署不自动迁移或重建，实际升级需遵循 operations.md 的停写、备份和核验流程。
