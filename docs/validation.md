# 验证报告

验证环境：Windows，Go 1.27.1，Node.js 24.21.0，2026-10-10。本轮按用户要求先完成本地验证，没有配置真实厂商 API Key，没有部署云资源。

## 已执行

- `go test ./... -count=1`、`go vet ./...`、`go build ./...`：本轮通过。默认回归不启用基础设施开关；基础设施集成另行显式执行并通过。包括并发预占、加密 AAD、重复/乱序入账与修正、SSE 增量与中断、降级计量、缓存隔离和灰度自动回滚。
- miniredis Lua 回归：100 个并发预占中仅 10 个在额度内通过，结算重复及修正不重复入账，配额初始化标记丢失会拒绝。
- 管理端回归：RBAC、跨应用拒绝、CSRF、密码变更失效、最后一个管理员保护、密钥宽限/吊销、待发布时一次性 Key 保留、跨实例会话及退出。
- 协议回归：文本、SSE usage、function tools、JSON 输出、Embedding 批量、参数拒绝、客户端断开取消、计量持久化失败不产生正常终止标记。
- 采样隐私回归：正文密文保存，已知敏感字段脱敏，跨应用不能读取采样正文。
- 前端 `npm run build`：通过；React/TypeScript/Vite 静态资源已生成。
- 浏览器人工链路：本地登录、创建应用、保存发布、Playground 主模型故障后流式降级、成本页展示多次尝试；页面布局已查看。
- 部署 YAML 校验、24 项 Grafana dashboard JSON 校验：通过。Linux amd64 交叉编译：通过。
- Docker Desktop 本地构建：`docker compose --env-file .env -f deploy/compose/compose.yaml build gateway` 通过，生成 `llm-gateway:local`。Docker Hub 认证域名连通性异常，使用已核实版本并固定摘要的官方镜像缓存（Go 1.27.2 Alpine、Node 24.21.0 Alpine），Go 模块通过 goproxy.cn 下载并保留校验；八个 Go 命令编译及前端生产打包通过。构建参数只写入未提交的本地 `.env`，默认镜像来源保持不变。
- 本地容器启动验证：依赖容器运行，MySQL、Redis、Kafka、etcd、Milvus 健康检查通过，topic 初始化退出码 0；显式 `migrate`、`bootstrap` 退出码 0。`gateway`、`admin`、`worker`、`edge` 均运行。因本机 8080 被占用，网关映射到 18080，HTTPS 控制台为 localhost:8443；控制台首页、管理端及网关 `/readyz` 均返回 HTTP 200。首次 HTTPS 诊断临时跳过证书校验，随后使用导出的 Caddy 根证书验证证书链和主机名，返回 HTTP 200；浏览器登录未纳入本轮验收。
- 2026-10-10 真实基础设施验收：`scripts/verify-infrastructure.ps1` 最终版本通过，7 个子场景通过，用时约 53 秒。真实 MySQL/Redis/Kafka/etcd/Milvus，生产模式运行时，仅测试客户端使用本机可控模拟上游。验证配置 CAS、防止重复/乱序入账及修正重复结算、降级多 attempt 计量、流式中断不降级且不发送 DONE、正常 SSE usage、Embedding 批量、语义缓存命中及内部向量化费用、跨应用/模型版本缓存隔离、预算拒绝前不调用厂商、消费者重建后的消息重放。
- 配额恢复验收：删除配额状态标记后，付费请求返回 503、上游调用数不增加；bootstrap 拒绝静默初始化。专用 Redis 清空后，重建程序合并 SQL 已有账本和 Kafka 未消费的重复事件，消费位置追平后恢复 ready；逐应用比较 SQL/Redis Token、金额和预占余额一致，随后请求正常。消费者在同一测试进程内关闭并重建，不等同于容器强杀或节点故障。
- 验收资源清理完成：专用数据库、topic、消费组、etcd 配置和 Milvus collection 删除，临时 Redis 移除，进程环境变量还原。现有十个应用/依赖容器继续运行，网关 `/readyz` 返回 200。结果文件为本地忽略目录 `data/acceptance/gw_acceptance_28a6047d30b6.log`；PowerShell 语法、gofmt 和 `git diff --check` 通过。

此前离线覆盖率约 45%，本轮未重新统计；真实依赖及测试容器路径通过单独验收覆盖，不把覆盖率当作正确性证明。

## 多实例、压力与容器故障

2026-10-10，完整 `scripts/verify-infrastructure.ps1 -Reliability` 通过，7 个子场景通过，用时约 264 秒。结果目录为 `data/acceptance/gw_acceptance_ca93d6e46722/`，父测试日志为同名 `.log`。另有显式跳过压力的快速故障回归通过，不以该次跳过结果代替完整验收。

发现并修复两处运行时问题：公共运行时提前创建 Kafka Reader，使 gateway 等非消费者加入消费组并占用分区；Reader 现仅在 worker 消费及离线配额重建时创建。Redis 客户端默认重连、DNS 和命令重试叠加，停机请求曾等待超过 35 秒；治理操作现有 1 秒总预算并限制重试，新增不响应 TCP 连接回归，Redis 单独停机时的四次 HTTP 请求均在 3 秒内返回 503，上游调用数不增加。

验证内容：

- 两网关运行时消费组只有一个 worker。Token 和预算分别用 100 请求争抢 5 次预占，两实例合计各放行 5、拒绝 95；结算后各为 160 Token、260 微单位、无剩余预占。
- 3,700 次普通/SSE 调用无协议错误，各阶段消费位置追平后 SQL 和 Redis 账本一致。缓存关闭，固定测试价格和 usage，确定性模拟上游；网关角色是 Linux 测试二进制，只有测试客户端允许访问本机模拟服务，不修改生产上游安全策略。
- worker 容器 SIGKILL 后，20 次成功调用暂未落库，重启后持久化消息补齐，640 Token、1,040 微单位、无重复结算和剩余预占。等待 Kafka 已失联成员会话失效后确认消费组稳定。
- 临时 Redis 容器 SIGKILL 时拒绝付费调用；原容器重启后 AOF 恢复 ready 和既有配额，逐应用核对 SQL/Redis 一致，正常请求恢复。
- 网关在首个 SSE 事件后 SIGKILL，连接中断且无 DONE。SQL 保留 started/pending attempt，Redis 预占保留，重启后新请求成功；不能把上游崩溃时的未知费用当作零费用或立即释放预占。

Docker Engine 为 32 CPU、约 15.18 GiB 内存，同机仍运行其他项目；网关每实例 2 CPU/256 MiB，worker 1 CPU/256 MiB，上游 1 CPU/128 MiB，Redis 1 CPU/256 MiB。资源文件是测试结束后的快照，不是峰值内存测量。依赖为单机 MySQL/Kafka 等本地服务，结果只作为本地基线。

| 并发 | 请求数 | 类型 | 协议失败 | 端到端 P50/P95/P99（ms） | 请求返回后账本追平（s） |
| --- | --- | --- | --- | --- | --- |
| 10 | 500 | 普通 | 0 | 15.73 / 22.95 / 25.36 | 28.83 |
| 50 | 1000 | 普通 | 0 | 17.48 / 27.75 / 33.09 | 57.44 |
| 100 | 2000 | 普通 | 0 | 24.62 / 35.79 / 41.94 | 115.15 |
| 50 | 200 | SSE | 0 | 18.12 / 25.54 / 25.78 | 11.94 |

SSE 首事件 P95 为 21.78 ms。普通阶段请求批次仅持续约 0.38–0.82 秒，JSON 中的短时 RPS 不能当作持续吞吐；后台逐条处理及提交存在明显积压，需要优先优化和持续压测。

两实例合计 3,710 个 inference overhead 样本（含额度争抢的成功调用）：仅 208 个 <=10 ms，3,498 个 <=30 ms，3,709 个 <=50 ms。固定桶只能确定 P50 在 (10,30] ms、P99 在 (30,50] ms，不能算出精确分位数；当前本地实验**未达到** spec 的 P50 <10 ms、P99 <30 ms。这是内部 overhead 指标，区别于上表端到端延迟。

随后串行重跑基础设施验收，7 个子场景全部通过，约 53 秒，日志为 `data/acceptance/gw_acceptance_33fa4cf31415.log`；包含 quota-rebuild 的 SQL/Kafka 合并、重复结算和缓存隔离。最终 Go 测试、vet、编译、27 份 YAML、24 个监控面板及 PowerShell 语法校验通过。

本轮修复进入源码和本地 commit，日常应用容器未重建；需使用新镜像重建 gateway/admin/worker 后生效。完整验收结束后检查无验收容器/数据库/topic 遗留，临时凭据文件已删除，现有十个应用/依赖容器继续运行，网关 `/readyz` 返回 200。真实厂商、云节点、网络半开故障、持续容量和年度 SLO 均未据此宣称通过。

## 待执行

1. 优先定位并优化逐条计量消费/提交积压和网关 overhead，再执行持续压力、全局 QPS 与厂商 RPM/TPM 的跨实例验证。Token、预算争抢和三个验收容器强杀已通过；阶段 3 已验证测试网关 SQL/Kafka 连接断开，半开连接、整节点故障仍待执行。
2. Linux `go test -race`：当前 Windows CGO=0，未安装 C 编译器；GitHub Actions 已配置，但尚未推送执行。
3. DeepSeek、Qwen、OpenAI 真实协议回归：待配置账号、准确模型名称、价格和 Qwen 地域。不能用 mock 工具或 JSON 响应证明厂商具备对应能力。
4. 可控上游 10,000 并发、延迟目标、长连接及故障恢复演练；k6 脚本已提供，尚未执行。当前不宣称 P50/P99、缓存 P95 或 99.95% 达标。
5. 云集群 dry-run/预发、托管服务 TLS/Auth、SSE 负载均衡、滚动升级、扩缩容、备份及 quota-rebuild 演练。

## 重现基础设施测试

在本机使用 `deploy/compose/integration-ports.yaml` 合并默认端口映射并启动依赖；确保 `.env` 中数据库密码和主密钥有效，然后从项目根目录执行：

```powershell
./scripts/verify-infrastructure.ps1
```

脚本生成随机验收名称并创建专用资源，环境变量只在本次进程生效。测试要求数据库、topic、消费组、配置前缀、collection 与验收名称匹配，并要求 Redis 初始为空。直接设置 `GW_RUN_INFRA_TESTS=1` 不足以启动测试，不要手工指向日常数据库或 Redis。日志保存在 `data/acceptance`；配置和 collection 由测试清理，数据库、消息资源和临时 Redis 由脚本 finally 清理，失败时检查清理提示。测试不会创建真实厂商凭据。

压测示例（k6 需单独安装）：

```powershell
$env:GW_LOADTEST_ACK='controlled-upstream'
$env:GW_API_KEY='仅本次使用的测试应用 Key'
$env:GW_VUS='50'
k6 run loadtest/gateway.js
```

逐步增加到目标并发，分别执行 stream=true/false；记录机器规格、容器资源、上游延迟、请求长度、配额、缓存策略、各百分位及完整日志。k6 标准 HTTP 接口会缓冲 SSE，首事件延迟应从专门流式客户端或 Prometheus 指标测量。网关 overhead 指标为尝试时间减去上游读取/转发时间，包含持久化计量 ACK，与用户总等待时间分开解读。




## 2026-10-11 资源防护验证

本轮 Go 全量回归（count=1）、go vet、go build、部署结构验证（27 份 YAML、24 个监控面板）、Docker Compose config --quiet 和 git diff --check 均通过。新增回归覆盖：32 个过载请求在读取正文前拒绝且无上游调用；释放槽位后恢复；慢客户端写入/flush 超时；SSE 多行事件总大小；缓存字节预算与缓冲所有权；离线账本/去重容量；Redis DB 编号不能代替实例隔离；超大配置回滚；TCP 连接上限及释放。

真实 MySQL、两台独立 Redis、Kafka、etcd、Milvus 的 12 个隔离验收通过，最终运行 gw_acceptance_4065989ebcbf，约 58 秒。新增验收证明缓存确实触发淘汰而配额不变；90% 水位拒绝新调用，已有预占可重放并留出结算余量；错误配额淘汰策略拒绝预占；硬限额 OOM 后仍可读取配额、无新增上游调用；超大 SQL 配置回滚；10,001 组报表明确拒绝；包含大正文的记录页超过 16MiB 拒绝，缩小分页可用；过期 Milvus 实体确实删除。

初次容量验收 gw_acceptance_f2e8ca469d9b 失败：大块 SET 返回 OOM，但小型预占脚本仍可利用剩余空间，未达到测试期待的提前拒绝。补入 INFO memory/90% 水位核验后重新通过；未把该失败记录覆盖或列为通过。

多实例故障回归 gw_acceptance_899786912ee0 通过约 48 秒：仅 worker 入消费组；100 并发在两实例争抢 Token/预算，各恰好允许 5 次；强杀 worker 后补账；Redis 停止拒绝调用并由 AOF 恢复；网关流中崩溃保留 pending。该次明确使用 -Reliability -SkipPressure，6 项通过、压力项跳过，不能作为性能达标证据。之前记录的 overhead 和计量积压问题仍待性能优化。

验收数据库、topic/组、配置前缀、collection、角色容器、两台测试 Redis 及临时凭据文件均隔离并清理。现有应用容器未重建、现有配额未变；只读检查当前 Redis used_memory 1,281,544 字节、maxmemory=0、noeviction。新配置需按 deployment.md 升级才生效，不执行 down -v。

Linux race、真实厂商、最大报文持续压力、云预发、入口代理连接治理、监控标签长期累积及财务元数据归档仍待专项验证/治理。本轮不宣称所有 OOM 风险已消除，也不宣称云容量和 SLO 达标。

## 2026-10-11 配额存储对比

用户要求比较使用与不使用 Redis 的配额性能，新增测试专用 SQL 基线及独立容器脚本，生产代码和既有服务未切换。最终 gw_quota_491965d693b7 完成 36 组采样，约 171 秒；所有无错误组逐应用账本/配额一致且无预占残留，前置验证重复事件、递增修正和并发争抢额度。MySQL 单应用 64 并发发生 31% 超时及未完成预占，作为饱和结果保留，不算性能通过。其他组和 Redis 全部组无错误；全部组已结算数值与 SQL 最新账本一致。

方法、完整结果、局限及前置失败记录见 [quota-comparison-report.md](quota-comparison-report.md) 和 [quota-comparison-results.json](quota-comparison-results.json)。这是存储周期短时对比，未覆盖端到端 Kafka/HTTP/真实厂商及云长时间实验，不能替代既有网关压力/SLO 验收。

## 2026-10-11 计量恢复阶段 1

按用户最终确认，网关保存 SQL outbox，Kafka 消费者统一入账。已实现 started 保存及 completed 空间预留、completed 的 SQL/Kafka 双路径、后台补发/租约/有界退避、消费结算后释放容量、同 ID/版本载荷冲突拒绝、取消事件持久化、usage 修正版本/审计/outbox 原子保存及操作幂等。SQL 控制与 Redis 冻结标记拒绝新付费调用，既有重放继续；提供停写 metering-audit/metering-resume，quota-rebuild 先排空未发布 outbox。

全量 Go 回归（count=1）、go vet、go build、gofmt、git diff --check、27 份 YAML/24 面板结构校验和 Docker Compose config --quiet 均通过。新语义回归覆盖 SQL started 失败不调用上游、completed SQL 失败仍发 Kafka、Kafka 失败重启补发且不重调模型、两路失败保留预占/不输出 DONE、冲突冻结、采样超限仍保存财务字段、冻结不阻止结算重放、调用方取消不冻结健康计量。运行时故障注入仅在测试代码，生产没有绕过开关。

最终真实依赖验收为 gw_acceptance_06a4cfc53453，19 项通过，测试约 128 秒。新增验证：保存 outbox 时账本为空；单应用满不阻止其他应用、completed 使用预留容量；丢失 ACK 后重复发送无重复收费；修正幂等及内容冲突；completed 没有 outbox 也能从真实 Kafka 入账、释放预留且不补写 outbox；64 并发容量事务不死锁/泄漏；停写核验拒绝未知 pending 和丢失 attempt 键；核实后解除冻结；Redis 重建包含尚未发布补偿并保留其他业务键。

容器故障回归 gw_acceptance_ddfe6f051375：6 项通过，测试约 56 秒，压力项明确跳过。两实例 100 并发争抢 Token/预算各只允许 5 次调用；强杀 worker 后补账；Redis 停止时拒绝新调用、AOF 恢复；网关流中崩溃保留 pending。角色使用 Linux 编译的验收程序，但不等于完整应用镜像构建或云环境验证。

保留前置失败：gw_acceptance_21413de265c6 在离线核验阶段失败，发现原 quota-rebuild 及新增 audit 的 SCAN 模式未覆盖带后缀的配额键，改为 gw:{app:*}:*，并增加孤立键清理/其他业务键保留验证后通过。gw_acceptance_e18bdd98f327 初次跨实例争抢未满足 5 次调用；排查修正容量行 INSERT IGNORE 共享锁升级为直接取排他锁，随后容器回归及 64 并发容量验收通过。早期 15 项及 17/18 项成功结果不替代最终 19 项结果。

验收仅创建随机独立数据库、Kafka topic/组、配置前缀、collection、临时 Redis 和角色容器，结束后清理；日志在忽略的 data/acceptance，临时凭据文件已删除。现有应用未迁移、重建或部署；Compose/Kubernetes 已更新配置与 155 秒停机宽限，Dockerfile 增加两个核验命令。

本阶段不包含 Kafka 分区 SQL 检查点/旧 SQL 备份回放、自动周期对账与代次许可、云预发、真实厂商、SQL/Kafka 容器断网或整节点损坏、完整端到端压测/SLO。completed SQL/Kafka 双路径失败由方法/传输边界注入；不能据此宣称真实存储整节点故障已演练。Windows CGO=0 且无 GCC，本轮未执行 race。全局容量行、同步 SQL 保存与更多消费事务增加成本，旧性能结果不能沿用。

代码与本阶段结果需按 AGENTS.md 人工确认，再进入检查点与旧备份恢复阶段。运行与升级步骤见 operations.md、deployment.md。

## 2026-10-11 计量恢复阶段 2

阶段 1 代码及测试结果已获用户确认。阶段 2 新增显式 Kafka generation/分区所有权 fencing、账本与 SQL checkpoint 原子提交、stream 来源身份及消息指纹、只读 manifest 计划、显式有锁回放和持久化任务阶段。恢复直接从 SQL 检查点定位，不加入正常组；旧 SQL 快照遇到更靠前的 Kafka 组 offset 会停止正常消费，恢复后重建 Redis、对齐位置、核验并保持冻结。存量基线必须停写核验，未完成恢复禁止换 manifest 绕过或解冻。

最终基础设施验收 gw_acceptance_1605a57eaa82：25 项通过，约 67.5 秒。除阶段 1 回归，验证 checkpoint 事务失败不推进、旧 generation/epoch 拒绝写账、SQL 账本及 checkpoint 回滚而 Kafka/Redis 保持最新后补回 42 tokens/53 micro、表写锁中断后的同 manifest 续跑、独占锁拒绝并行恢复、未完成任务禁止解冻/换新计划、消息指纹与位置异常/过期计划拒绝、存量离线基线、双 worker 真实重平衡及存活 worker 接续、真实删除并重建隔离 topic 后旧检查点拒绝回放。旧备份由测试库事务回滚对应账本和检查点模拟，不是完整 SQL 物理备份/PITR 演练；位置范围和指纹失效测试不等于真实 retention 定时清理演练。

最终容器故障验收 gw_acceptance_3b74c044ce61：6 项通过，约 57.2 秒；完整压力阶段显式跳过。覆盖仅 worker 加入组、双网关争抢 token/金额、专用 worker SIGKILL 后补账、测试 Redis SIGKILL/AOF 恢复、测试网关 SIGKILL 中断流式并保留 pending。角色测试使用最新 Go 源码编译的 Linux 二进制；现有应用容器未升级。此前故障轮 gw_acceptance_7c75f288401f 也通过，不替代最终结果。

全量 Go test(count=1)、vet、所有命令 build、gofmt、git diff --check、27 份 YAML/24 面板校验及 Compose config --quiet 通过。新增单元验证 manifest 篡改、区间缺口、尾随内容、来源 header 缺失/重复与消息指纹变化。并发构建多个 CLI 时 Go 工具链曾耗尽 Windows 主机内存；以 GOMAXPROCS=4、-p 1 串行执行全量验证后通过，不将该次构建失败计作应用压力或运行时 OOM 验证。

保留前置失败：gw_acceptance_fd2b072fdb6f 编译失败，补齐 Reader 重构后的引用/导入；gw_acceptance_03114856c5aa 旧测试假定 rebuild 自动解冻，改为明确核验/解冻；gw_acceptance_91ddf9d3c537 发现 Kafka generation 已结束而取消传播未到 SQL 的竞态，事务回滚被误判故障，修正代次/父上下文检查。该失败轮测试清理还重复等待已取走的 workerDone，结束已核验路径的测试进程并修复清理后，脚本释放隔离资源。中间 22/24 项成功轮不替代最终 25 项。

客户端 Metadata 不提供 broker topic UUID，采用集群/分区、SQL stream ID、消息 header 和锚点指纹；不声称等价 UUID 或防恶意同源伪造。锚点被删除会更严格地拒绝回放，需保留窗口内一致性备份或人工核账。只读计划最多支持 128 分区/10,000 未交付 outbox 项；超限失败，不输出部分成功结果。未测试真实厂商、物理 SQL/Kafka 整节点灾难、云预发、Linux race 或全链路容量/SLO。

本轮未迁移或更新现有应用，不操作其账本/配额。初始化、恢复、审计与解冻均须停写；SQL 独占锁与消费组检查不能替代停止全部外部写入者。短期接单许可、Redis generation/Lua 强制校验、在线周期对账及新增同步 SQL 链路完整压测仍为阶段 3；代码及本阶段结果需人工确认后继续。

## 2026-10-11 计量恢复阶段 3

阶段 2 代码及结果已获用户确认。本阶段实现 worker 每秒核验并续发 3 秒 etcd lease、SQL/Redis generation 与分区结算水位、Redis Lua 绝对到期检查、准备/在途登记、逐应用一致屏障核验、报告与恢复审计、管理查询及控制台概览。来源/水位损坏保持冻结，普通探测超时停止续期；在线核验失败只冻结目标应用，未知费用不按超时清零。新应用仅在空账本/补偿/在途/配额的证明下两阶段初始化，不能自动修复存量损坏。

最终基础设施验收 `gw_acceptance_09252d577e5a`：**29 项通过，约 76.7 秒**。新增验证缺失许可不调用模型、与流式调用重叠的屏障等待、单应用暂停不影响另一应用、attempt 丢键与旧水位拒绝放行、离线修复与显式恢复、在途槽位保持及释放、真实 SQL 连接池耗尽不误判检查点损坏、真实 SQL/Kafka TCP 连接中断后的双路径持久化与补账。网络代理仅断开测试网关连接，共享节点和其他客户端继续工作，不等于存储整节点灾难演练。

最终完整容器验收 `gw_acceptance_075e23a00365`：**8 项通过，约 114.9 秒，未跳过压力项**。覆盖 worker 实际周期核验并保存报告、仅 worker 入组、双网关各 100 并发争抢 Token/金额仅放行 5 次、四组压力及 SSE、worker SIGKILL 后旧许可过期拒绝接单且补账不重复、Redis SIGKILL/AOF 数据恢复后停写核验/切换代次/等待新许可、网关 SIGKILL 中断流式且保留 started/pending。AOF 恢复不等于重新获得接单许可；恢复测试按实际 CLI 新建客户端，不能复用故障前失效 socket 假定恢复成功。

### 压力与性能边界

原始工况、计量开销桶和 SQL 连接池指标保存在 [metering-pressure-results.json](metering-pressure-results.json)。Windows 客户端和 Docker Desktop Linux VM（32 CPU、约 15.2 GiB 内存）；两个网关各 2 CPU/256MiB，worker 1 CPU/256MiB，上游 1 CPU/128MiB。配额 Redis 1 CPU/256MiB、maxmemory 8MiB、AOF always/noeviction；缓存 4MiB/allkeys-lru。共享 MySQL/Kafka/etcd/Milvus 没有容器 CPU/内存限额，其他本地应用也在运行。Kafka 测试 topic 三分区、单副本；这些条件不能推广为云容量或 HA 保证。

入口 HTTP 并发配置为每实例 128，付费在途每实例 8（`GW_MAX_METERING_INFLIGHT`），覆盖准备、上游及完成计量收尾；默认单应用 outbox 64MiB、全局 512MiB。每次 started 预留最多 1MiB 完成空间，使有限突发在消费者排空前触及容量保护。每组为固定请求数的闭环短时突发，不是固定到达率的持续负载；503 明确统计为保护性拒绝，不能计为成功吞吐。

| 并发/协议 | 请求数 | 成功 | 保护性拒绝 | 协议错误 | 成功 P99 ms | 成功 RPS（突发） | 末尾结算秒 | 含结算周期的成功 RPS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10 / JSON | 500 | 73 | 427 | 0 | 812.0 | 13.82 | 9.18 | 5.05 |
| 50 / JSON | 1000 | 63 | 937 | 0 | 2224.8 | 16.16 | 7.84 | 5.37 |
| 100 / JSON | 2000 | 63 | 1937 | 0 | 2272.7 | 15.74 | 7.94 | 5.27 |
| 50 / SSE | 200 | 63 | 137 | 0 | 2321.5 | 15.75 | 7.95 | 5.27 |

SSE 首事件 P95 为成功调用的 1329.4ms，拒绝响应不参与该分位数。四组共接受 262 次，排空后 SQL 与 Redis 每组完全一致：合计 8384 tokens/13624 micro，无剩余预占；零费用 cancelled 记录不算成功付费调用。压力后能恢复接单，并继续完成故障回归。

两实例合计 272 个 inference overhead 样本（含 Token/金额争抢的 10 次成功）：均值约 1.03 秒，131 个 <=1 秒、272 个 <=2 秒，P99 只能确定在 (1,2] 秒；JSON 中约 1.98 秒是固定桶线性估计，不是精确分位数。没有 <=30ms 的样本，**未达到 P50<10ms、P99<30ms 开销目标**。本轮拒绝比例为 68.5%～96.85%，只能证明保护与计费正确，不能宣称目标负载下可用性达标。两个网关采集时 SQL 连接池等待计数均为零，不能据此排除 SQL 行锁、事务持久化及消费者逐条提交瓶颈；这些等待不属于连接池指标。

此前 Redis/MySQL 比较是短存储周期实验，不能拿其成绩替代本轮新增 outbox/许可/登记/检查点的端到端成本。当前没有实现同一完整生产链路的无 Redis 切换或 A/B 对照，不据本轮数据直接改掉已确认架构。后续应先分析计量事务和消费瓶颈，再比较完整链路的实现与迁移成本。

### 失败记录与交付检查

保留失败而不算为最终通过：`gw_acceptance_749117821068` 压力后 outbox 超时导致全局冻结；started 槽位改为满即拒绝仍未解决 completed 积压，`gw_acceptance_a892099195ac`、`gw_acceptance_da806c9398ef` 的诊断明确 SQL 保存超时而 Kafka 成功。将 8 个槽位从仅登记并发改为覆盖整个付费 attempt，保留完成事件独立保存通道后，四组压力通过。此前 SQL 读取超时被归类为检查点损坏，以及 Kafka 终点快照早于新提交位置的误判也已修复，并增加真实连接池耗尽回归。

`gw_acceptance_024d68899304` 在 AOF ready 后直接调用，尚未等候新许可而失败；`gw_acceptance_ef2203dfd040` 恢复测试复用旧 Redis 连接失败，改按实际恢复 CLI 新建 runtime。早期基础设施轮还发现 outbox BLOB 的 JSON_EXTRACT 需先转换 utf8mb4、直接入账测试夹具缺少 started 哈希，以及正常取消表现为 sql.ErrTxDone 的竞态，均修复后由最终 29 项覆盖。中间 28/29 项或仅压力成功的轮次不替代最终结果。

全量 Go test(count=1)、vet、全部命令 build（GOMAXPROCS=4、-p 1）、gofmt、前端 TypeScript/Vite build、Prettier、27 份 YAML/32 个监控面板、Compose config --quiet 和 git diff --check 均通过。并发回归证明 started 保存槽位满时立即拒绝且不冻结全局，completed 仍可独立保存。

仅操作随机专用数据库/topic/组/配置前缀/collection/Redis 与测试角色，结束清理且删除临时凭据文件。清理检查发现早期及最终各一条测试 etcd 状态遗留：测试函数清理时角色尚未退出，worker 可以续发许可重新创建前缀。脚本改为角色退出后再次删除本次明确前缀，PowerShell 语法校验及实际清理/空前缀检查通过；确认无验收容器、最终测试数据库或临时凭据文件遗留。源码、部署模板和文档已更新，现有应用未迁移或升级。本轮不包含真实厂商、云预发、Linux race、SQL 物理备份/PITR、整节点故障、长期持续容量或年度 SLO 验证。

运维限制：详细核验期间目标应用返回 503；worker 在核验中退出可留下 frozen/quiescing，需要显式恢复，当前不自动接续。单应用超过 100000 attempt/10000 月份/110000 Redis 键、控制行超过 10000 应用时明确失败；本轮未新增历史数据归档或自动删除。

用户已按 AGENTS.md 确认阶段 3 实现提交 1b1d833 的代码、测试结果及上述性能边界。此确认不表示性能目标达标，也不授权升级现有容器；当时计量性能优化仍待后续需求及技术方案确认。

## 2026-10-11 计量性能修复第一阶段

用户确认企业内部多部门/应用共用场景，并确认首期计量性能技术方案。实现安全分区 offset 批量提交、预留 completed 避开全局容量锁、减少容量行无变化写入、明确 utf8mb4 时的驱动参数转义，以及计量分阶段指标/拒绝原因/批量面板。保留 app_id 隔离、所有容量上限、代次 fencing、事件幂等与先落盘再调用上游的约束。

全量 Go test（count=1）、vet、所有命令 build（GOMAXPROCS=4、-p 1）、gofmt、27 份 YAML/35 面板校验、Compose config --quiet 和 git diff --check 通过。新增单元回归验证安全提交位置、空闲尾部、取消、结算/提交失败停止读取、环境配置边界及诊断标签基数。

真实基础设施 gw_acceptance_e42f90b8a6ee：30 项通过，约 73.6 秒，包括全局容量锁竞争时预留完成事件、并发重复事件/跨应用拒绝、UTF-8/JSON 特殊字符往返、旧账本恢复与真实重平衡。此后仅增加 request 摘要跳过无效预留探测，由最终全量 Go 和容器回归覆盖。保留前置失败 gw_acceptance_5009ca3cd9cb：参数转义将 JSON 字节参数标为 binary 导致 MySQL 3144；改为 JSON 字符串参数并新增真实安全往返回归后通过。

最终容器 gw_acceptance_8b1d7477b571：9 项通过，约 206.4 秒，包含全部压力和 SIGKILL/AOF 故障场景。四组短突发零协议错误，仍有 68%～96.7% 保护性拒绝；成功 P99 相比本轮优化前基线有所下降，SSE 首事件 P95 未改善。按事件数折算 offset 提交次数减少约 76%，突发阶段推理内部开销均值约 1061→719ms。五组各 20 秒固定到达率多应用样本共 420 次全部成功，无拒绝/调度丢弃/协议错误，逐应用 SQL/Redis 一致且无预占残留；10次/秒成功 P99 为 449.4ms。详细方法、前后数值和测量限制见 [metering-performance-report.md](metering-performance-report.md) 与 [metering-performance-results.json](metering-performance-results.json)。

随机验收容器及临时凭据文件清理检查通过，现有应用未升级。20 秒样本不证明长期容量，原开销 P50<10ms/P99<30ms 未达标；周期屏障暂停/中断接续、历史生命周期、真实厂商、云预发、Linux race、物理备份/PITR 与整节点故障仍待后续。代码与测试结果待按 AGENTS.md 人工确认，再进入后续方案阶段。
