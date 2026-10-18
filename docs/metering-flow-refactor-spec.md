# 计量流程收敛与崩溃恢复重构规格

> 2026-10-18 后续变更：运行实现统一后台容量，协议版本及升级步骤见 [唯一后台规格](outbox-single-capacity-spec.md)。本文保留此前设计或部署的历史事实。

本次重构把准入登记、尝试生命周期和完成事件空间预留统一到 MySQL 尝试记录；网关同步记录开始状态，Kafka 只传完成、费用修正和请求汇总事件。Redis 保留同步预算预占，Worker 负责异步结算。目标是减少重复发布、消费和独立状态表，同时保留进程崩溃后的证据、幂等恢复及跨系统核验。

技术方案已于 2026-10-14 由用户确认实施。基线为 `4a3e8f7017045ff855ffa58ae6640e2693240516`；实现位于隔离分支，代码及测试结果在交付时另行确认。

### 已确认的补偿与监控补充

- prepared 的时间阈值只触发核验。后台取消与 Arm 对同一 attempt 做条件转换；只有取消赢才补偿，inflight 不自动退款或重跑模型。
- 执行取消与补偿完成分开持久化。保存 Redis 取消确认、SQL 剩余空间、领取代次/租期、下次重试时间；Worker 启动受控后台任务，用索引、小批、短事务、退避恢复。
- 参考用户监控截图提供实例、应用/API、模型筛选，QPS、流式/非流式、成功率、保护拒绝、token/s、RT 平均及分位数、首 token、缓存命中和限流。请求和 attempt 分母分开；费用估计不能标成已确认结算。
- 增加 Prepare/Arm/完成事务、待补偿数量及年龄、prepared/inflight/held、Outbox 容量/积压、GC、Worker 追平、DB 连接等待和资源指标。标签不得包含 request/attempt ID、密钥或正文。

## 重构范围与保留约束

- 保留现有推理 API、模型路由、主推理最多三次尝试、缓存 embedding 独立计费、SSE 已输出后禁止切换模型，以及请求汇总不重复收费。
- 保留 Redis Lua 同步预算检查与预占。异步结算未追平时，在途预占仍计入可用预算计算。
- 保留 MySQL 完成事件保存与 Kafka 备用发布：完成保存失败后仍独立尝试 Kafka；任一通道不可确认时保留未知状态并停止新付费准入。
- 保留当前成功响应契约：普通 JSON 成功和 SSE `[DONE]` 均等待完成事件 Kafka ACK；即使 SQL 已保存但没有 ACK，也不伪装成功结束，Relay 后续补发。
- 保留应用屏障、短期准入许可、generation、分区 owner/epoch、SQL 检查点、Redis 结算水位和 Kafka 安全消费位置。它们覆盖不同故障边界，不因图中步骤多而删除。
- 基础设施仍为 MySQL、Redis、Kafka、etcd；不引入第四套财务存储，不把预算权威迁到另一套扣费机制。
- 本轮只实现和验收单主 Redis。Sentinel 切主保护、Redis Cluster 适配与网络分区安全属于后续优化项，本轮不实现，也不宣称当前可直接接入分布式 Redis。

## 当前职责与收敛方式

| 当前行为 | 拟议方式 | 不能丢掉的状态 |
|---|---|---|
| `gw_metering_admissions` 登记准备和在途尝试 | 合入 `gw_attempts.execution_phase`，以状态筛选准备/在途记录 | 尚未写开始事件的准备阶段也能被屏障发现 |
| `gw_outbox_slots` 存每个 attempt 的剩余预留 | 合入 `gw_attempts.reserved_remaining_bytes` | 完成事件的空间归属、使用和幂等释放 |
| 网关保存 started Outbox 并同步发布 Kafka | 网关在调用前同步保存 started 账本，不发布 started Kafka | 待完成证据、价格快照、预占值和事件 hash |
| Worker 消费 started、写账本、确认预占 | 开始账本由网关同步写入；预占 Lua 同时记录开始事件 hash | Redis 重建来源以及预占内容核验 |
| 多处根据表是否存在猜测处理阶段 | 明确执行阶段、财务状态和交付状态，分别更新 | 调用结束不等于费用核实，也不等于消费完成 |
| 已交付 Outbox 永久保留元数据 | 延迟、分批清理，依赖账本去重凭据拒绝旧事件重建占用 | 补发、消费重放和同 ID 内容冲突检查 |
| 超时估计完成后直接释放预算 | 完成事件可以入账，但保留待核实的预算暴露额度 | 未知费用不能因为响应结束就变成可用余额 |

`gw_metering_capacity` 保留。全局和应用保护的是不同边界；其计数仍是逻辑额度，不是数据库物理空间。合并独立表并不等于删除预留和准备登记语义。

## 一次尝试的拟议流程

1. **Prepare MySQL 事务**：校验许可与全局/应用屏障；锁定容量行；创建 attempt 准备记录，保存应用、月份、模型与价格快照、预计 token/金额、调用方 generation；登记完成事件预留并增加全局/应用容量。提交后状态为 `prepared`，模型尚未获准调用。
2. **同步 Redis 预占**：Lua 核验当前许可与应用状态，在一个脚本内检查预算、增加预占并保存该 attempt 的开始事件 hash。预算不足时不调用模型；网络结果不确定时保留 prepared 记录，进入受控核实。
3. **Arm MySQL 事务**：再次核验当前屏障与登记 generation，使用条件更新把 prepared 改为 `inflight`；同步写入 revision 1 开始账本。只有该事务明确提交成功，且当前准入证据有效，才允许调用模型。
4. **调用模型**：使用唯一 attempt ID 执行一次上游调用。模型重试生成新 attempt，不复用旧记录。进程崩溃后的恢复流程禁止自动重跑上游。
5. **保存并发布完成事件**：revision 2 表示执行收尾。SQL 在同一事务中使用 attempt 的预留写入 completed Outbox，并标记执行已结束/完成待交付；再发布 Kafka。SQL 不可用时独立尝试 Kafka，保留原 inflight 证据并冻结新付费准入。成功终止标记仍等待 Kafka ACK。
6. **Worker 结算**：校验事件来源和分区所有权；幂等写完成/修正账本与 SQL 检查点；按财务状态结算或保留 Redis 暴露额度；同一 SQL 事务释放 Outbox 和剩余空间预留；推进 Redis 结算水位；最后批量提交 Kafka offset。

首次准备只预留完成事件上界，不再为 started 的 Kafka 补发保存一份 Outbox payload。started 账本与 attempt 元数据的物理空间独立测量和预算。

Prepare、Redis 预占、Arm 三个步骤保留，是因为 MySQL 与 Redis 没有共同事务。prepared 证明尚未获准调用；inflight 证明可能已经调用。若只在 Redis 预占后第一次写 SQL，崩溃可能留下没有持久归属的预占；若 Prepare 后直接调用，恢复又无法区分预算拒绝与可能已经发生费用。

## 事务提交边界与性能验证

调用前有两个独立的短 MySQL 事务，不是一笔贯穿模型调用的长事务：

```text
Begin Prepare → SQL 状态/容量/准备记录 → Commit Prepare
Redis Lua 检查及预占
Begin Arm → SQL 开始账本及调用状态 → Commit Arm
HTTP 调用模型，读取 JSON 或转发 SSE
Begin Completion → SQL 完成事件及预留余额 → Commit Completion
Kafka 发布并等待 ACK
```

etcd 许可读取或刷新在 SQL 事务之外完成；事务内只用持久控制行、generation、有效期及条件更新核验这份证据。Redis Lua、Kafka 网络发送、模型请求、响应读取、SSE 转发都不能放在 SQL 事务中。每次 Commit 明确成功后归还连接及释放事务锁，再进入下一外部操作。Commit 返回结果不确定时按持久状态核验，不以超时等同回滚，更不能直接进入模型调用。

原方案本来就有调用前事务：`beginAdmission` 提交准备登记后才返回到 Redis Reserve；`SaveOutbox(started)` 提交后 `DurableBus.Publish` 才发送 Kafka。原方案也不持有 SQL 事务等待模型。本轮收敛不应宣称消除了此前不存在的“模型调用期间长事务”。

| 每次正常主尝试的步骤 | 当前实现 | 拟议实现 |
|---|---|---|
| 调用前第一笔 SQL 事务 | admissions 登记及只读容量预检 | Prepare attempt、预留完成额度 |
| 同步 Redis 预算操作 | 原子预占 | 原子预占并记录开始 hash |
| 调用前第二笔 SQL 事务 | started Outbox、slots 与容量记账 | Arm 状态及同步开始账本 |
| 调用前 Kafka 发布和 ACK | started 发布及 ACK | 移除 |
| Worker 的 started 处理 | SQL 账本/检查点、Redis、Outbox 释放和水位/offset | 新事件移除，兼容旧事件重放 |
| 尝试结束的单独 SQL 删除 | 删除 admissions，独立提交的单条语句 | 生命周期随完成保存/消费更新，不另删登记 |

两笔前置 SQL 事务数不增加，但单笔工作量改变：Prepare 提前竞争容量行、写更完整的 attempt；Arm 把 started 的事件账本同步写入请求路径。JSON 元数据序列化、索引维护、锁等待和持久提交仍产生网关开销。因此“减少 Kafka 往返及后台重复工作”是确定的步骤变化，“端到端一定更快”不是已经成立的结论。

验证要独立记录 Prepare/Arm/Completion 的持续时间、SQL 语句与提交次数、数据库行锁和连接池等待、started Kafka ACK 的原开销、主尝试及请求 P50/P95/P99、保护性拒绝率和 Worker 追平时间。按相同价格、额度、完成事件上界、可控上游、到达率和数据量做前后对照，并覆盖 Redis 拒绝后的 prepared 取消成本。不能仅用少一次 Commit 或表数减少证明性能改善。

## 数据与状态归属

| 数据 | 权威位置 | 说明 |
|---|---|---|
| 准备/可能调用/执行收尾 | `gw_attempts.execution_phase` | 拟议值为 prepared、inflight、completed、cancelled；completed 只代表执行结束 |
| 待核实或已确认费用 | `gw_attempts.billing_state` | held、settled、reconciled；与执行阶段分开 |
| 预占 token/金额与价格快照 | attempt 持久记录 | 用于审计、Redis 重建和修正，不靠进程内存恢复 |
| 剩余完成事件空间 | `gw_attempts.reserved_remaining_bytes` | 单位字节；不等于预算预占 |
| 最新已处理财务版本/hash | attempt 及事件账本 | 相同版本不同内容冲突；旧版本不能覆盖新版本 |
| 应用/月度预算占用 | Redis | 已确认结算 + 待核实暴露额度；由 Lua 同步准入，由 SQL 账本受控重建 |
| 未完成交付的事件正文 | `gw_metering_outbox` | 只负责完成/修正/请求汇总的可靠发布和交付跟踪 |
| 全局/应用待交付额度 | `gw_metering_capacity` | payload 字节 + attempt 剩余空间预留 |

prepared 阶段需要保留完整上下文及登记 generation，但不能被报表当成已经确认付费。Arm 写开始事件时保持价格、月份和开始事件 hash 不变。SQL 开始账本写入必须与 Arm 状态变化同事务；消费回放允许遇到网关已经登记的 attempt，仍检查归属和 hash。

Redis 预占 Lua 的重复路径增加开始事件 hash 核验，不仅检查 reserved 是否存在。完成/修正的 hash 与开始 hash 需要分字段或明确状态规则，避免把合法阶段变化误判成同版本冲突。

## 未知费用的处理

- 成功收尾但没有 usage、传输超时、SSE 中断且没有完整 usage，都可以产生 revision 2；财务状态仍为 held。报表保存估计值，预算准入继续计入待核实暴露额度。
- 暴露额度至少保留原预占；若已知部分用量或估计费用更高，则增加暴露额度并阻止后续预算超卖。实际厂商费用仍可能超过预估上界，不能据此承诺厂商账单绝不超预算。
- held revision 2 不把估计金额当成已确认结算后释放全部预占。Redis 保存已计入总账的金额、当前暴露额度、财务状态和版本；原来的“首次完成一律释放预占”规则改为按财务状态执行。
- 厂商用量核实或管理员修正后，以更高版本转换成 settled/reconciled，原子释放暴露额度并按已计入金额的差额调整总账。同版本重放无副作用，旧版本不覆盖新版本。
- 仅在确认没有调用上游的取消路径中记零费用并释放。HTTP 状态码小于 500 本身不能证明零费用；408 等不确定错误不得直接记 reported 零用量。
- held 完成事件已经可靠入账并结算为预算占用后，可以释放 Outbox 空间；“预算保留”与“补偿存储保留”是不同资源。只有没有收尾证据的 inflight 才继续保留完成空间等待核实。

## 崩溃窗口与恢复决策

| 中断位置 | 持久证据 | 恢复动作 |
|---|---|---|
| Prepare 提交前 | 没有新记录，也没有允许模型调用 | 不生成调用；已有其他尝试按原恢复规则处理 |
| Prepare 提交后、Redis 预占前 | prepared，预算可能尚未占用，空间已预留 | fencing 旧调用方后核实 Redis，持久取消并释放空间；不调用模型 |
| Redis 预占已执行，但网关没收到结果 | prepared，Redis 结果不确定 | 不直接重跑模型；先阻断旧代次，再核实/幂等取消预占 |
| Redis 预占成功、Arm 提交前 | prepared，预算已占用 | 未获准调用；取消前核验阶段和 owner，Redis 写取消凭据防止迟到预占重建 |
| Arm 提交结果不确定，或提交后尚未发 HTTP | prepared/inflight 需读取核验 | 查询明确状态；无法证明未调用时保留未知，不按年龄自动退款 |
| 上游调用中崩溃 | inflight、开始账本、预占与空间预留 | 保留预算和 pending；人工/厂商证据修正，禁止自动重跑上游 |
| 完成 Outbox 提交后、Kafka ACK 前 | 完整 completed Outbox | Relay 补发同 ID；不再调用上游；响应不伪装成功 |
| 完成 SQL 保存失败、Kafka ACK 成功 | 开始账本仍在，Kafka 有 completed | Worker 恢复后补写完成账本，幂等结算并释放原预留 |
| SQL 和 Kafka 都无法确认完成 | inflight 和预占仍在 | 冻结新增付费，保留未知费用；两套外部存储同时丢失时不承诺最终 usage 可恢复 |
| Worker SQL 已入账、Redis 未结算 | 事件/检查点已写，结算水位未推进 | 从安全 offset 重放；账本幂等，继续 Redis 结算或 held 处理 |
| Redis 已结算、Outbox 未释放 | Redis 版本已更新，SQL 仍有预留 | 重放时不重复扣费，再释放一次空间 |
| 全部结算完成、Kafka offset 未提交 | SQL/Redis/交付状态已完成 | 重放全部幂等步骤，再提交安全位置 |
| Redis 丢失或恢复旧状态 | SQL 的 prepared、inflight、held、settled 记录 | 停写、切换 generation、重建各类预算占用和幂等状态，审计后恢复 |

prepared 取消必须持久记录取消版本，并在 Redis 写终止凭据；不允许仅删除 SQL 行、释放预算后留一个可能迟到的 Reserve。进程内槽位、owner 与屏障的退出规则必须随状态一起重构，应用核验不能再只等旧 admissions 表为空。

## 预留与清理规则

首轮保持 completed 最大 1 MiB 的现有上界，不凭平均事件大小降低预留。移除 started payload 只能减少对应实际字节和处理次数，不宣称解决全部保守拒绝。后续测量正常、最大元数据、采样和修正事件的大小，再决定是否用经过证明的更小上界。

容量恒等式调整为：全局 used_bytes 等于未释放 Outbox charged_bytes 总和加 attempt reserved_remaining_bytes 总和；应用计数按 app_id 分组。准备和完成的占用、扣减与状态变化必须在同一 SQL 事务里提交。已预留完成路径不得重新竞争全局容量行；锁顺序需要覆盖 Prepare、Arm、完成、消费释放和修正，禁止等待容量行时仍持有 attempt 行。

拟议的统一取锁顺序是：全局/应用控制行 → 分区 checkpoint（仅消费）→ 全局容量 → 应用容量 → Outbox 事件行 → 账本事件行 → attempt 行。各事务只取需要的子集；需要回退申请新增容量时，先结束已持有 attempt 锁的事务，重新按顺序开启，不能逆序补取。Arm 的开始事件与旧事件兼容消费也须遵守事件行先于 attempt 行的顺序。完成保存仍在锁定 attempt 后重新核验事件是否已由并发请求写入。该顺序是待实现约束，必须用真实 MySQL 并发和故障测试验证。

Outbox 行只在账本已确认该事件、交付状态完成、相关空间释放后进入 GC。GC 的保留期必须覆盖允许的补发和重放窗口，并处理已领取 Relay 的迟到结果；已删除事件再次进入保存接口时，先查账本事件 ID/hash/版本，不能重新分配容量。请求汇总、修正、Kafka 备用发布缺少 SQL 完成行也要覆盖。

增加明确的交付完成时间 `delivered_at` 和对应清理索引，保留期从交付完成算起，不以事件发生时间或 next_retry_at 代替。已领取 Relay 即使在 GC 后迟到发布，账本仍必须识别同 ID/hash 的重复；迟到状态更新不得重建已删除的 Outbox。GC 用小批事务并限制单轮时间，不持有容量锁扫描历史行。

attempt 的执行/财务记录是恢复来源，不能随着 Outbox GC 一并删除。prepared、inflight、held 不自动过期；已结算记录的热存储保留和财务归档另设策略，归档前校验重放边界及恢复来源。去重凭据的保留期必须覆盖所有支持的重试来源，而不能仅依据 Kafka retention。

专项统计 `gw_metering_outbox`、`gw_attempts`、`gw_events` 的行数、数据和索引页分配；按清空 payload 后的 1 万/10 万/更高记录量及多轮写入/释放测增长。表统计是估计值，还需测实际数据卷、binlog 和备份空间。只有历史数据有界且已测开销时，才可给 512 MiB 逻辑限额配置例如 600 MiB 的物理预算；不把 88 MiB 当成已验证的安全余量。

## 迁移与恢复范围

必须停写并冻结旧代次迁移，不允许两版网关/Worker 混写。先备份，排空可处理事件，把 admissions、slots 和未完成尝试迁入新字段，逐应用核对预算、预留、账本版本和 Kafka 水位，再切换 generation、审计、开放入口。无法核实的尝试保留为相应 prepared/inflight/held 状态；不能为了通过迁移把未知费用归零。

旧 started Kafka 事件仍可能重放。新 Worker 必须兼容其语义并验证网关同步开始账本；待回放边界明确超过最后一条旧 started 后，才移除兼容分支。源身份、checkpoint 和锚点不能静默 reset。用户于 2026-10-14 确认清理旧数据和依赖：旧 slots/admissions 表仅在备份、停写确认、状态迁入 attempt、两表证据核验和容量审计全部通过后删除；任一核验失败保留两表并冻结准入。metering_protocol=2 先于删除提交，避免删除中断后重复导入旧预留。回滚依赖迁移前的一致性备份/binlog，不能直接运行旧代码。

去掉 started Kafka 后，新的开始证据仅由 MySQL 持久化。它保留进程崩溃和 Redis 丢失后的恢复能力，但失去“Kafka 也有一份 started”的额外副本。MySQL 数据库物理丢失或回退到旧备份时，必须依赖一致性备份/binlog 恢复开始证据；不能声称只靠 completed Kafka 就能找回所有 started-only 尝试。若要求保留 Kafka 独立的开始证据副本，应采用兼容收敛方案：仍发布 started，仅合并登记/预留、补齐未知费用保留和 GC。

MySQL 必须采用实际可验证的持久提交配置和持久卷。该重构不声称覆盖 MySQL/Kafka 两套持久数据同时丢失，也不提供厂商侧 exactly-once 调用。对外的“可靠恢复”须说明其故障范围。

## 分布式 Redis 后续优化项

本轮不实现这些能力。原子预算预占只说明一个主节点内“读取额度、判断、增加预占”不被其他命令穿插，不证明切主之后已确认写入一定保留。实际厂商费用高于预估、held 暴露额度和财务修正的边界在分布式部署下仍存在。

| 后续方向 | 初步实现思路 | 独立验收要求 |
|---|---|---|
| 多网关共享单主 | 所有网关对同应用/月使用相同预算 key，仍由 Lua 原子检查与预占 | 多实例预算争抢、超时重复调用和幂等取消 |
| Sentinel 高可用 | 增加可替换 Redis 客户端；把主节点身份或拓扑代次变化接入准入冻结，禁止客户端发现新主后直接恢复付费。隔离旧调用方、切换 generation，以 MySQL prepared/inflight/held/settled 重建预算并核验后恢复 | 已确认预占尚未复制时强杀主节点、旧主隔离后继续接受连接、恢复旧 AOF；未核实期间没有新模型调用 |
| Cluster 分片 | 使用 Cluster 客户端；同应用的月度总账、attempt、应用控制证据统一 hash tag，并把脚本访问的所有 key 显式传入 KEYS。全局许可/冻结不能直接跨槽读取，需要单独设计按应用的证据投影及 SQL Arm 屏障核验，不能把异步投影当作瞬时全局一致 | CROSSSLOT/重分片 TRYAGAIN、缺失 key、节点切换、应用控制投影滞后和同应用预算争抢 |
| 减小写丢失窗口 | 评估 AOF 策略、最少副本写入约束及 WAIT；WAIT 必须在 EVAL 后使用同一物理连接，并检查超时结果。复制确认失败后不能直接换 ID 重试预算；按 attempt 核实和冻结 | 主节点 ACK 与复制之间的强杀、客户端确认超时但服务端已写入、丢失预占后不重复使用额度 |
| 更严格的预算权威 | 若要求网络分区和切主期间仍不重复使用预算，另立 spec 评估具备已验证一致性保证的事务/共识存储或持久额度分配协议；Redis 可以保留限流和查询投影角色。单纯把存储名字换成 MySQL 并不自动证明灾难切换安全 | 明确一致性和持久性假设，对双主、旧备份、预算租约和恢复边界给出故障证明 |

Cluster 按应用分片可以分散不同应用负载，但单个应用的一份预算仍必须由同一槽位原子决定。不能把同一总预算复制到多个分片后各自允许花完整额度。现有预算/attempt key 共享 `{app:...}`，但脚本还访问全局 ready/generation/permit/frozen 和没有同标签的应用控制 key，当前客户端也为 `redis.NewClient`，因此不是更换连接地址即可完成适配。

切主事件的发现时延、旧主仍可写的窗口和新主预算回退都属于需要验证的问题；“最终可以从 SQL 重建”不能证明重建前接单不会超卖。WAIT 只能降低风险，不把 Redis 变成强一致存储。这些约束对应 [Redis Cluster 文档](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)、[Sentinel 文档](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)及 [WAIT 一致性说明](https://redis.io/docs/latest/commands/wait/)。

## 实施顺序与验收

1. 先实现生命周期字段、准备/调用边界、合并预留及核验逻辑，完成旧数据停写迁移与旧 started 回放兼容；不得双写两套预算总账。
2. 实现网关同步开始账本和预占 hash，移除新 started Kafka 发布与正常消费；保持 completed 双发布通道和响应契约。
3. 实现 held 预算语义、修正差额、Redis 重建，消除 408 直接零费用和估计完成立即退款的假设。
4. 实现安全 GC、账本去重回查和真实存储测量；有界清理不能阻塞完成事件持久化。
5. 跑完整协议、跨系统故障、旧版本回放、迁移/回滚及持续负载，再根据证据决定是否降低完成预留上界。

| 验收类别 | 必须证明的结果 |
|---|---|
| 并发预算 | 同应用/月的检查与预占原子；Worker 延迟期间预占不失效；不同 attempt 独立计费 |
| 准备阶段故障 | Prepare、Reserve、Arm 各边界强杀；没有获得 Arm 的尝试不调用模型；取消后迟到预占不复活 |
| 真实调用故障 | JSON/SSE 中断、网关 SIGKILL 保留未知；恢复不重跑模型；不发送虚假成功终止标记 |
| 未知费用 | 无 usage 成功、超时、408、部分 SSE usage 的费用质量正确；held 不退全部预算；修正只调整差额 |
| 双通道完成 | MySQL 失败/Kafka 成功和 Kafka 失败/SQL 成功均保留可恢复证据；两者失败保持未知与冻结 |
| 消费恢复 | 在账本、Redis、Outbox、水位、offset 各步骤后强杀 Worker，重放不重复计费、不重复释放容量 |
| 预留并发 | 全局容量行被占用时，已预留完成事件仍可保存；计数等于 payload 与剩余预留之和 |
| 版本与来源 | 同版本不同 hash 拒绝，旧版本不覆盖，分区旧 owner 不写账；旧 started 兼容回放正确 |
| GC 与迁移 | 清理/迟到 Relay/重复保存交错；已处理事件不重建占用；未知记录保留；迁移及回滚前后总账一致 |
| 长期容量 | 持续记录数据库数据/索引/卷占用与 GC 延迟，证明历史开销有界，不用内存 RSS 代替磁盘测量 |
| 性能证据 | 同环境可控上游对比 SQL 事务/语句、Kafka 事件数、成功率、保护拒绝、网关开销、结算追平、资源占用 |

每次真实尝试不再发布 started，主尝试开始/完成的 Kafka 事件由两条变成一条；请求汇总、缓存 embedding 和修正需分别计数，不能把整个请求链路概括成固定减半。当前准备登记、保存 started、退出删除三次 SQL 事务改成 Prepare 与 Arm 两次前置事务，但具体完整路径语句数、锁等待和端到端改善必须实测。

原有“生产者不能写 started 账本”的测试断言随职责变化改为“上游调用前，开始账本与 inflight 已同事务提交”；不能仅删除这条测试。离线模式的接口应镜像相同生命周期，但继续明确其重启丢失状态的边界。

成功率、保护拒绝、完整 JSON/SSE、真实用量与估计/held 状态分开报告。可控上游证明本地机制；真实厂商小流量、云长期容量、最高观察吞吐分别验证，不互相外推。真实厂商成本及云变更需要单独授权。

## 当前代码依据

- [准入登记与屏障](../internal/gateway/metering_gate.go)：`beginAdmission` 在 Redis 预占前登记，退出删除；容量预检不代替最终事务检查。
- [请求尝试](../internal/gateway/engine.go)：同步 Reserve、started 发布、上游执行、completed 收尾与 SSE 终止顺序。
- [事件预留与释放](../internal/gateway/outbox_sql.go)：slots 剩余额度、预留完成快路径、全局/应用锁和消费释放。
- [持久发布与补发](../internal/gateway/outbox.go)：完成 SQL 保存失败不短路 Kafka；Relay 直接补发而不再次 SaveOutbox。
- [账本与检查点](../internal/gateway/checkpoint.go)、[消费结算](../internal/gateway/partition_consumer.go)：SQL 账本/检查点、Redis、Outbox、水位、offset 的顺序。
- [预算脚本](../internal/gateway/guard.go)：首次同步预占，现有完成脚本按版本结算差额；估计完成仍释放预占。
- [受控恢复](../internal/gateway/metering_recover.go)、[应用核验](../internal/gateway/metering_online_audit.go)：当前从最新 SQL 账本重建 Redis，并对准备登记、未知 started 和位置证据设屏障。
- [现有故障回归](../internal/gateway/reliability_test.go)、[基础设施回归](../internal/gateway/integration_test.go)：网关崩溃保留 pending、双通道完成、容量锁竞争和分区所有权。
- [运行边界](operations.md)：逻辑容量不覆盖历史元数据与索引，恢复需要来源、水位和停写核验。
