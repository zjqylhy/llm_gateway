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

1. 优先定位并优化逐条计量消费/提交积压和网关 overhead，再执行持续压力、全局 QPS 与厂商 RPM/TPM 的跨实例验证。Token、预算争抢和三个验收容器强杀已通过；SQL/Kafka 断网、半开连接、整节点故障仍待执行。
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
