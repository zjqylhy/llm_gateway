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

已运行覆盖率统计，Go 核心包覆盖率约 45%，SQL/Kafka/etcd/Milvus 及进程启动路径未由离线测试覆盖，不把覆盖率当作正确性证明。

## 待执行

1. 容器强杀、依赖网络中断、跨实例限流和配额争抢：当前消费者重建和 Redis 配额数据丢失场景已验证，其余仍待执行。
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



