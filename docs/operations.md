# 运行与故障处理

## 配置、密钥与会话

MySQL 保存期望配置，etcd 保存已发布版本；实例每秒获取版本，控制台展示各实例心跳。保存但发布失败时返回待发布状态，应重试发布并检查每个存活实例版本。旧版本通过 CAS 不能覆盖新版本。

密钥吊销传播目标 3 秒；实例 3 秒未成功获取配置便停止鉴权。轮换可指定 0–86400 秒宽限期，旧 Key 到期不再可用。厂商凭据解密失败应停用该配置并重新录入；不可随意更换 `GW_MASTER_KEY`，更换前需计划逐条重加密及备份。

管理会话在生产 Redis 中共享，8 小时有效，密码修改立即使旧会话无效。Redis 删除会话失败时退出登录返回失败，不能把本机清 Cookie 当作全局退出成功。管理 API 写入需要 Origin 与 CSRF。

## 用量与对账

所有实际推理、重试及内部向量化分别产生 started/completed 事件。Kafka ACK 后才开始上游调用；completed ACK 后才发送成功终止标记。队列不可用时拒绝新付费调用，流式计量失败会发送错误而不发送 `[DONE]`。

worker 按事件 ID 去重、按 attempt revision 更新账本和配额。MySQL 入账后 Redis 结算，再提交 Kafka offset；任何一步失败停止 worker，修复后重放。同一个事件重放不会重复计费。Kafka lag、写入错误、MySQL 事务延迟和 Redis 预占量需要监控；默认基础设施指标接入由部署环境提供。

`reported` 表示厂商返回 usage，`estimated` 表示缺失 usage 时的保守估算，`pending` 表示上游尝试没有终结记录。报表将 started 和 estimated 计入待核实，不能视为已确认账单。进程突然退出后，不能凭 TCP 断开推断厂商没有收费。

管理员通过 `POST /admin/usage-correction` 提交 `request_id`、`attempt_id` 和修正后的 `usage`（prompt_tokens、completion_tokens、cached_tokens）；保存原尝试的价格版本，服务器分配唯一递增 revision 并记录审计。生产模式版本/审计/outbox 在同一事务中保存，由消费者最终入账。重试请发送相同 `Idempotency-Key`，响应也返回该 header；同 key 不同修正返回 409。发布未确认返回 503 时，修正可能已在 outbox 中，不应换 key 盲目重试。异步计量完成前查看调用记录确认 revision，金额以最新账本为准。

80% 预算告警和成本突增告警显示在控制台；100% 按应用 warn/reject 设置处理。warn 不阻止请求。当前告警没有自动发送邮件或企业聊天消息；可在部署环境通过 Prometheus/云监控配置告警接收器，渠道发送需单独授权和配置。

## Redis 配额恢复

额度实例使用 noeviction 和持久化，禁止把缓存 Redis 的 LRU 策略用到配额账本。配额初始化标记丢失时，付费调用失败关闭，bootstrap 也不会在已有账本上重置额度。

恢复步骤：摘除入口，停止 gateway/admin/worker 及其他账本写入者；保留 MySQL 与 Kafka 数据。设置 `GW_QUOTA_REBUILD_ACK=writers-stopped`，执行 `quota-rebuild`。先将尚未发布的 outbox 排空至 Kafka，再清理匹配 `gw:{app:*}:*` 的配额/attempt 键，重建 MySQL 最新版本及 started 预占，排空未入账事件，核验所有分区 offset 后恢复 ready。失败保持 rebuilding，不得人工改为 ready。该命令不清理其他业务键，也不直接解除计量冻结。

## outbox 与计量冻结恢复

生产模式启动前需完成新迁移。网关保存 outbox，不更新账本；worker 后台重试，消费入账并结算 Redis 后清除冗余载荷、释放逻辑容量。未交付容量默认全局 512MiB、每应用 64MiB，环境变量为 `GW_OUTBOX_GLOBAL_BYTES`、`GW_OUTBOX_APP_BYTES`；应用 `outbox_bytes` 可覆盖默认，每个新调用为 completed 预留 1MiB。限额不覆盖索引、已交付元数据及其他 SQL 表，不能代替磁盘、备份和连接池监控。

网关保存/发布计量失败及消费者事件冲突会触发 SQL/Redis 冻结并使 readyz 返回 503；启动新网关也要检查 SQL 控制状态。已有结算继续，新付费调用停止。worker 每秒核验消费/结算水位并续发 3 秒许可；worker 停止或依赖无法核验时许可失效。累计积压超过 10,000 条或未交付 outbox 的调度年龄超过 30 秒时不续期；这不是瞬时检测所有 Redis 键损坏。两路 completed 保存都失败或上游未返回 usage 时，精确费用仍可能需要厂商记录核实；不自动释放 pending。普通计量失败返回 `metering_unavailable`，可能已经产生费用，请按 X-Request-Id 检查记录再决定重试。

以下命令仅用于确认所有写入者已停止后的维护窗口，不要在正常 consumer/outbox sender/管理员修正仍运行时执行：

```powershell
# 先停止写入者、保留备份并修复依赖，再执行恢复和核验。
$env:GW_QUOTA_REBUILD_ACK='writers-stopped'
go run ./cmd/quota-rebuild
$env:GW_METERING_RECOVERY_ACK='writers-stopped'
go run ./cmd/metering-audit
go run ./cmd/metering-resume
```

命令使用当前环境中的生产模式连接配置。容器对应 `/app/quota-rebuild`、`/app/metering-audit`、`/app/metering-resume`，同样传入上述确认变量。audit 校验 SQL/Redis 月度结算与预占、attempt/revision、未交付 outbox 和消费位置；最多 10,000 个应用月份及 100,000 个 Redis 键，超限明确失败，不截断为成功。resume 要求无未解决 started，并记录审计，清除 SQL/Redis 冻结后重启业务进程以清除本实例故障闩锁。

有未知 started 时先核实费用、通过管理修正保存持久事件，再停止写入者、排空与重新核验；不能为恢复接单把未知用量改零。CLI 的 writers-stopped 是操作者承诺；恢复、核验、解冻与基线初始化共用 SQL GET_LOCK 独占锁，恢复事务复用该连接，且检查正常消费组没有活跃成员。在线应用屏障对账与短期许可已接入；任意键损坏仍存在发现窗口，以完整核验轮转时间衡量。

恢复结束后核对各应用当前月份的 token、金额、预占及待核实记录，再重新启动服务和开放入口。恢复命令需要独占消费组；不得在生产 worker 同时运行时执行。历史数据被清理、Kafka 超出保留窗口或账本损坏时，需要备份恢复或人工对账，不能只运行重建命令。

## 分区检查点与旧 SQL 备份恢复

新部署 bootstrap 仅在 SQL 账本/outbox 和 Kafka topic 均为空时自动初始化检查点。存量升级必须先用旧版 worker/sender 排空，停写备份并记录 Kafka 位置，执行新迁移后运行 `metering-checkpoint-init`。该命令核验配额、全部保留事件与 SQL 内容，建立明确基线并保持冻结；已有未入账消息会拒绝初始化，不猜组 offset。基线以前已经删除的 Kafka 历史无法借此验证，保留原备份/账单核验材料。

```powershell
# 使用当前环境的连接配置；先停 gateway/admin/worker/sender，保留一致性备份。
$env:GW_METERING_RECOVERY_ACK='writers-stopped'
go run ./cmd/metering-checkpoint-init  # 仅存量升级首次建立基线时执行

# 恢复已有 checkpoint 的一致性 SQL 备份后，从备份水位回放，而非最新组 offset。
go run ./cmd/metering-recover plan --output recovery-plan.json
$manifestHash='替换为已核验的manifest_hash'
go run ./cmd/metering-recover apply --manifest recovery-plan.json --hash $manifestHash
go run ./cmd/metering-audit
go run ./cmd/metering-resume  # 未知 started 必须先核实并修正，禁止直接改零
```

容器分别执行 `/app/metering-checkpoint-init`、`/app/metering-recover plan/apply`。manifest 文件须保留以便同哈希重试，容器运行时挂载到维护实例；无本地 WAL 或长期 PVC 要求。单次 CLI 最长 10 分钟，分区逐条处理、查询/发送有界；超时后保留 SQL 检查点和任务阶段，不自动解冻。quota-rebuild 同样走该流程，已有未完成任务时复用其 manifest，不创建新任务绕过。

消费事务同时写事件/最新账本和 next_offset；随后结算 Redis，最后提交 Kafka offset。SQL checkpoint 不证明 Redis 已结算。旧 owner 的 epoch 或 Kafka generation 不匹配时拒绝写账。恢复 reader 不加入正常组；apply 检查正常组没有活跃成员，持有数据库独占锁，复用该连接写恢复事务。SQL 锁无法阻止所有外部程序，请确保整个维护窗口停写。

来源核验包含集群、分区集合、stream ID、新消息 header 和上条已入账消息指纹。客户端未暴露 topic UUID，这些核验不能等同 UUID，不能防止授权写入者伪造源消息。锚点或检查点超出保留区间、topic 被重建后位置/指纹变化、拓扑变化会拒绝回放，保持冻结。计量 topic 要求 `cleanup.policy=delete`，Kafka ACL 需允许读取 topic 配置、DescribeGroup 和维护时提交组 offset。超过保留窗口需要一致性备份、binlog 或人工核账，禁止静默 earliest/latest reset。

恢复补发 outbox 后重新捕获终点，按 SQL 最新账本重建 Redis 结算/预占/幂等记录，再对齐组位置并核验；未解决 pending 不释放。审计记录关联 manifest 哈希，恢复任务完成仍保持 frozen。显式 resume 后重启故障闩锁实例，再启动入口。重建和显式恢复会更新 Redis generation/水位，旧许可失效；启动 worker 后等待新许可，再启动付费入口。

## 接单许可与应用核验

生产模式依赖 worker 续发许可。只启动 gateway/admin 不会获得付费接单资格。许可为 etcd lease 3 秒，Redis 同时保存绝对到期时间，旧 AOF 的 TTL 不会重新激活已经过期的许可。应用、worker 与 Redis 的主机应同步时钟。正常检测不会修改丢失的 Redis generation 或结算水位；出现来源/水位异常必须按离线恢复流程修复。

准备阶段先登记 gw_metering_admissions 再预占；退出时删除登记。进程崩溃留下登记不能按年龄自动删除。应用核验将其置 quiescing，等候准备/在途退出，排空固定消费水位后比较 SQL 与 Redis。超过 150 秒、未知 started、哈希冲突或孤立记录均保持冻结，其他应用可在公共依赖健康时继续。等待屏障与扫描本身会导致该应用出现 503，持续吞吐和可用性必须包含这部分影响。

worker 在核验中退出（包括正常发布重启）也会使本次核验失败并保留应用冻结；清理保存失败时可能留下 quiescing。不要直接修改控制表恢复接单，按停写、重建、核验和显式 resume 流程处理。当前版本没有自动接续被中断的在线核验，发布前应检查应用状态及报告。

管理员 GET /admin/metering 可查看全局状态、最近 100 份报告和最多 100 个应用状态；按返回 next 作为 after 参数继续分页。控制台概览展示近期核验结果与异常应用。离线 metering-resume 重新核验，记录报告及 gw_audit 关联标识，可设置 GW_METERING_ACTOR/GW_METERING_REASON 记录操作者和原因；默认 recovery-command/verified offline recovery。显式恢复新代次后仍需正常 worker 颁发许可，不能手工改 ready 或删除冻结键来代替核验。

关注 gateway_metering_lag_records、gateway_metering_permit_expires_seconds、gateway_metering_unchecked_apps、gateway_metering_rotation_seconds、gateway_metering_outbox_bytes 和 SQL 连接池等待指标。未核验数量包含冻结应用。每次核验最多 100,000 个 attempt、10,000 个月份、110,000 个 Redis 键；超出边界失败，不输出截断的成功账单。应用控制总数上限 10,000。详细核验逐应用轮转，不能宣称全部应用在一个 60 秒周期内完成。

## 缓存与灰度

缓存服务超时绕过，额度和计量依赖失败则拒绝。内部向量化即使未命中也计费。清除缓存递增应用 epoch，旧向量不能再次命中；模型/路由或参数变化使 scope 改变。当前 scope 还包含全局配置版本，因此无关配置变更可能降低命中率，但不会跨应用复用。

Milvus 保存向量及 scope/有效期，Redis 保存响应并按 TTL 过期。过期向量不会命中，但需要定期按 expire 删除或维护 collection，避免索引持续增长。不同维度必须分开 collection；升级向量空间需使用新的模型版本和 collection。

灰度 10%、50%、100% 使用应用及请求 user 字段的稳定分组；没有 user 时以应用分组。每次放量形成独立 run_id，最低 10 个完整尝试后检查最近 5 分钟错误率与最大延迟，异常自动回滚，started 不算错误样本。100% 放量期间仍可回滚；完成发布后由管理员保存新的正式候选路由。

## 数据保留与安全

默认只存调用元数据。应用可显式设置 log_sample，采样内容在脱敏及截断后加密；每段最多 32KiB，只允许管理员/对应应用开发者在 7 天内查看。常见邮箱、手机、身份证和 Key 脱敏属于额外保护，不能覆盖所有敏感信息。工具流式输出目前只在元数据中审计，不保证完整正文采样。

Kafka、MySQL、Redis 和 Milvus 都属于应用数据存储，需访问控制、备份和加密。当前 7 天限制是 API 查看期限，密文的物理清理由运维保留策略执行。可在备份及对账确认后分批用 JSON_REMOVE 删除两张事件表旧 payload_ciphertext 字段；清理 Kafka 对应过期事件，避免重放恢复密文。90 天元数据、长期财务归档等具体保留期由实际部署业务确认。

Redis 限流键随限流周期自动过期；配额和 attempt 幂等键不自动淘汰。确认历史月份不再对账、重放窗口已结束并完成归档后，按月份清理对应账本与配额幂等键；不得只删 Redis 幂等键再重放旧 Kafka 事件。监控存储增长，并规划一致的归档策略。

## 容量与过载处理

配额 Redis 在 90% 内存水位拒绝新增预占，返回 503；`maxmemory=0`、错误淘汰策略或无法读取 `INFO memory` 同样拒绝。达到 Redis 硬限额后写操作可能返回 OOM，进程仍可读；不要改成 LRU 或删除幂等键来恢复接单。先摘入口、扩容，核对 pending/预占和 worker lag，处理已入账未结算事件，再恢复服务。已有 attempt 的重复预占不会新增数据，水位检查保留重放路径；持续高水位可能使 worker 重建缺失预占失败，仍需扩容后重放。

缓存 Redis 按 LRU 淘汰；向量命中而响应已淘汰时按 cache miss 处理。单项响应、向量及基本元数据超过 512KiB 时不缓存。worker 每 5 分钟删除 Milvus 过期向量，实体删除后的物理空间由 Milvus compaction 回收；没有 worker 或清理失败时需补跑并检查压缩积压。淘汰后尚未到期的孤立向量最多留到原 TTL 结束，API 最大 TTL 为 24 小时。

网关和管理 API 有实例并发上限，槽位满时立即拒绝，不排队；健康探针不占业务槽。每个 HTTP 监听器默认最多接纳 256 条 TCP 连接（`GW_MAX_CONNECTIONS`），包括慢请求头和空闲连接；满额时连接留在内核等待队列，健康检查也可能等待，需配合入口连接限额和超时。SSE 单事件（包含所有行）最多 2MiB，单行限制同样保留。普通响应/SSE 每次写入或 flush 最多阻塞 5 秒，超时中断且释放槽位；上游 60/120 秒期限不因持续读写而延长。普通管理 API 总期限 30 秒，playground 使用业务期限。超载计数为 `gateway_overload_rejections_total{role}`，已添加对应 Prometheus 告警；指标抓取也限制为两条并发和 5 秒处理期限。

MySQL 默认 socket 建连/读/写超时为 3/5/5 秒，可通过 DSN 显式调整。配置快照最大 1MiB，超限更新回滚；生产记录查询累计 JSON 超过 16MiB 返回 `413 records_too_large`，需减少分页数量或缩小筛选。成本报表超过 10,000 组返回 `413 report_too_large`，不返回不完整账单。实例列表只展示最近 24 小时心跳且最多 10,000 个。Kafka 单事件最多 1MiB、每分区读取预取队列 1 项/批次上限 1MiB，最多支持 128 个分区，避免大批次预取挤占业务内存。

离线内存缓存总数据预算 64MiB、最多 10,000 项；账本最多 20,000 个 attempt、序列化数据预算 64MiB，配额去重最多 10,000 次尝试。达到上限后拒绝新增事件/付费预占，已有去重记录仍保留；采样正文可能使账本提前达到预算，失败结算保留待核实状态。离线审计只保留最近 10,000 条。需要长期运行时切换真实基础设施，离线重启仍会丢失所有数据。

Compose 的 Docker 日志轮转为每文件 10MiB、最多 3 份。云部署须同步配置节点日志轮转、Redis 80% 水位及 RSS/AOF/拒绝写告警、Kafka lag/日志盘、MySQL 表及临时盘、Milvus/对象存储容量告警。Kafka 保留窗口不能随意缩短来隐藏积压，财务数据归档和配额去重清理必须协同实施。

剩余容量边界：公网反向代理/Ingress 接入层、监控标签随配置长期变更的累计数量、数据库历史元数据归档及云依赖的资源配额仍需专项治理。本轮容量限制不能证明所有压力场景下无 OOM；需在云预发做持续压力和最大报文测试，并验证告警接收与扩容流程。

99.95% 是持续 SLO，需要真实可用性采样、错误预算、基础设施 HA、故障演练和运维响应；当前本地测试不能证明该指标。
