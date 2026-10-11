# 本地联调与云部署

自己试用、少量调用时，使用 [2 核 4 GiB 新手指南](lite-deployment.md)和独立 `deploy/lite/compose.yaml`，不要直接启动本文的完整联调栈。低配方案全局关闭语义缓存，保留真实计量依赖；实际容量边界见 [低配验收报告](lite-deployment-report.md)。

## 两种运行方式

`cmd/dev` 组合运行三个模块，适合离线调试。`gateway`、`admin`、`worker` 分别运行，使用共享外部服务，适合单台云主机容器或云 Kubernetes。生产模式缺少必要依赖配置会退出，不自动切换内存。

Dockerfile 使用多阶段 Go/Node 构建，最终镜像包含所有命令和控制台静态资源，以非 root 用户运行。镜像没有 shell，不在镜像中写入凭据。修改配置后须重启应用进程；业务配置由 etcd 动态同步。

应用调用监控需在管理端配置 `GW_PROMETHEUS_URL`，例如完整 Compose 栈的 `http://prometheus:9090`。Compose 默认连接栈内 Prometheus；Kubernetes 和轻量部署需显式设置私有监控服务地址。按随项目提供的抓取配置，job 使用 `gateway`/`admin`，抓取所有网关实例的 `/metrics`；仅消费计量的 worker 不计入调用 QPS。监控依赖新增的 `gateway_calls_total` 和 `gateway_app_qps_limit`，历史采样需覆盖欲查看区间。控制台限定最近 24 小时内选择，按应用权限查询；未配置、查询失败或缺失采样显示无数据。详见 [监控规格](app-traffic-spec.md)。

轻量部署可显式叠加 `-f deploy/lite/compose.monitoring.yaml`，再启动 `prometheus` 和重建 `admin`，启用私有采集。该覆盖文件不发布端口，额外预留 256 MiB 内存，保留 26 小时采样，TSDB 大小上限 256 MB；实际磁盘占用还包括 WAL。所有后续 Compose 运维命令应带上此覆盖文件。若应用/标签数量增长导致磁盘上限提前淘汰历史，应调整容量后再验收一天窗口。

构建若在访问 `auth.docker.io` 获取匿名令牌时超时，应检查主机 DNS 和 Docker Desktop 的代理链路。可在本地 `.env` 设置以下构建参数，选择 [Docker 官方 AWS 公共仓库](https://gallery.ecr.aws/docker/)中的同版本基础镜像，然后重新执行 `docker compose ... build gateway`：

```dotenv
GW_BUILD_NODE_IMAGE=public.ecr.aws/docker/library/node:24.21.0-bookworm-slim
GW_BUILD_GO_IMAGE=public.ecr.aws/docker/library/golang:1.27.1-bookworm
GW_BUILD_GOPROXY=https://goproxy.cn,direct
```

`GW_BUILD_GOPROXY` 仅用于构建时下载 Go 依赖，按网络情况选择；模块校验机制保留。独立 `docker build` 可分别用 `--build-arg NODE_IMAGE=...`、`--build-arg GO_IMAGE=...`、`--build-arg GO_PROXY=...`。这些参数不影响运行时的数据库、模型厂商或云部署配置。

若备用仓库的镜像层下载仍然缓慢，也可使用本机已有的官方镜像缓存：先用 `docker run --rm --network none --entrypoint /usr/local/go/bin/go <Go镜像> version` 和相应 Node 命令核实版本，再用 `docker image inspect <镜像> --format '{{json .RepoDigests}}'` 获取摘要。将构建镜像参数设置为已缓存的 `镜像名@sha256:摘要`，以固定实际内容；Go 版本须满足 `go.mod`，Node 须满足前端依赖。同一 Docker 构建器已有对应缓存时，可避免重复下载镜像层，构建仍需访问依赖包仓库。

## 本机容器基础设施联调

复制根目录 `.env.example` 为 `.env`，设置随机主密钥、管理员密码、各本机中间件密码和 Grafana 密码。用随机生成的 32 字节 base64 设置 `GW_MASTER_KEY`，不要发送到聊天或提交 Git。DSN 中的数据库密码及 Redis URL 密码必须与本机初始化密码一致，URL 特殊字符需编码。

从仓库根目录依次运行：

```powershell
docker compose --env-file .env -f deploy/compose/compose.yaml up -d mysql redis redis-cache kafka kafka-init etcd minio milvus
docker compose --env-file .env -f deploy/compose/compose.yaml build gateway
docker compose --env-file .env -f deploy/compose/compose.yaml run --rm migrate
docker compose --env-file .env -f deploy/compose/compose.yaml run --rm bootstrap
docker compose --env-file .env -f deploy/compose/compose.yaml up -d gateway admin worker edge prometheus grafana
```

等待各依赖健康后再运行迁移。首次 bootstrap 初始化管理员及空账本的配额标记；再次运行只重发配置，不覆盖已有密码。DDL 由显式 migrate 命令执行。云端应使用单独迁移账号，运行账号只授予必要 DML 权限。

启动 Docker Desktop 或只启动依赖容器不会启动管理页面。执行 `migrate` 和 `bootstrap` 后，还需 `up -d gateway admin worker edge`，其中 `edge` 对外提供 `https://localhost:8443`。用 `docker compose ... ps` 确认这四个应用服务均已运行。

若本机 8080 被其他进程占用，在 `.env` 设置 `GW_LOCAL_GATEWAY_PORT=18080`，重新运行应用启动命令即可；只改变宿主机映射，容器内仍为 8080。Docker Hub 认证不可达时，可设置 `GW_LOCAL_EDGE_IMAGE=public.ecr.aws/docker/library/caddy:2.10` 使用 Docker 官方的 AWS 分发地址；已下载并核实的镜像可进一步固定摘要。

若只启动依赖、在 Windows 上运行 Go 进程，可加上本机端口覆盖文件：

```powershell
$composeArgs = @('--env-file', '.env', '-f', 'deploy/compose/compose.yaml', '-f', 'deploy/compose/integration-ports.yaml')
docker compose @composeArgs pull mysql redis redis-cache kafka etcd minio milvus
docker compose @composeArgs up -d mysql redis redis-cache kafka kafka-init etcd minio milvus
docker compose @composeArgs ps
```

本机连接地址分别为 MySQL `127.0.0.1:13306`、配额 Redis `127.0.0.1:16379`、缓存 Redis `127.0.0.1:16380`、Kafka `127.0.0.1:19092`、etcd `http://127.0.0.1:12379`、Milvus `http://127.0.0.1:19530`。容器内应用使用服务名称。Go 命令不会自动加载 `.env`，需要在各进程启动前注入环境变量；管理页面仍需 HTTPS 反向代理以支持 Secure Cookie。

依赖启动后，从项目根目录执行以下命令可验收真实数据链路和配额恢复：

```powershell
./scripts/verify-infrastructure.ps1
```

脚本从本地 `.env` 读取数据库密码和主密钥，临时注入测试环境变量，不调用真实厂商。它创建专用数据库、Kafka topic/消费组、etcd 配置前缀、Milvus collection 及临时 Redis；结束后清理，日志保存在 `data/acceptance`，现有应用容器继续运行。测试只对该 Redis 执行数据清空。此脚本使用上述默认 loopback 端口，需要本机 Go 和 Docker，结果边界见 `docs/validation.md`。

多实例及容器故障验收使用：

```powershell
./scripts/verify-infrastructure.ps1 -Reliability
```

此模式交叉编译 Linux amd64 测试二进制，使用已缓存的 distroless 镜像启动两份测试网关、worker 和模拟上游，连接独立测试数据库/topic/配置前缀及 AOF Redis。生产命令不包含模拟上游开关。网关各限制 2 CPU/256 MiB，worker 1 CPU/256 MiB，上游 1 CPU/128 MiB，Redis 1 CPU/256 MiB；测试端口显式绑定本机空闲端口，重启后保持一致。仅强杀本次带验收标签的容器，清理时删除测试匿名卷；临时凭据文件随后删除。JSON 压力摘要、Prometheus 指标、容器日志及资源快照保存到 `data/acceptance/<验收名称>/`。

压力是有限请求的短批次实验，同时记录账本追平耗时，不将短时 RPS 作为可持续容量。模拟上游及依赖位于本机，不能替代真实网络、真实厂商、长时间压测或云节点故障验收。

调试故障场景时可加 `-SkipPressure` 明确跳过压力阶段；该结果不作为完整验收通过。清理会等待强杀消费者的 Kafka 会话失效，再删除专用消费组；若仍有清理警告，按随机验收名称检查遗留资源。

MinIO 使用 [Milvus 官方部署配置](https://github.com/milvus-io/milvus/blob/master/deployments/docker/standalone/docker-compose.yml)中的 `milvusdb/minio` 同版本镜像，原 `minio/minio` 地址在本机拉取时返回仓库访问错误。普通停止、重建容器保留命名卷中的数据。

本机管理页面为 `https://localhost:8443`，Caddy 使用本地证书。应导入 Caddy 根证书建立本机信任，浏览器证书警告需用户处理。该 Compose 中间件配置仅供本机联调，不能作为公网生产服务。

注册向量模型后设置真实输出维度及 `GW_CACHE_DIMENSIONS`，运行 `cache-init` 创建 Milvus collection。collection 中只使用一种维度；需要不同维度时配置新的 collection 和相应网关部署。缓存默认关闭，未创建 collection 时会绕过并记录缓存错误。

线上 Kafka 至少 3 个 broker，事件 topic 建议 6 个以上分区、3 副本、`min.insync.replicas=2`、禁用非同步副本选主。保留期覆盖最大离线对账窗口，示例为 7 天；按吞吐和数据政策调整。配额 Redis 与缓存 Redis 必须是独立实例，使用单主 HA endpoint；当前客户端没有 Redis Cluster/Sentinel 自动发现适配。

### Redis 容量与既有部署升级

`GW_REDIS_URL` 保存配额、计量去重及会话，须开启持久化、设置正数 `maxmemory` 并使用 `noeviction`；`GW_CACHE_REDIS_URL` 只保存缓存响应，使用 `allkeys-lru` 或 `allkeys-lfu`。不同 DB 编号不隔离 Redis 的内存和淘汰策略；运行时拒绝相同地址，不得以不同 DNS 别名指向同一个服务。生产预占脚本必须有 `INFO memory` 权限，用它核验配额实例的限额和策略，并在计入淘汰判断的内存达到 90% 时拒绝新增付费调用。剩余空间用于已发生费用的结算，不能保证任意积压规模下结算永远成功。

本地模板配额 Redis 为 `128mb / noeviction`，容器限制 `512m`；缓存 Redis 为 `64mb / allkeys-lru`，容器限制 `192m`，不持久化。额外空间用于客户端、碎片和持久化重写，需按实际负载调整。[Redis 官方说明](https://redis.io/docs/latest/develop/reference/eviction/)指出 AOF/复制缓冲不计入淘汰限额，因此 `maxmemory` 不能直接等于容器内存。

Compose 自动给应用注入缓存服务 URL，默认复用本机 Redis 密码；若密码含 URL 特殊字符，显式设置经过 URL 编码的 `GW_CACHE_REDIS_URL`。在 Windows 直接运行 Go 时也必须显式设置该变量，使用 `127.0.0.1:16380`。云部署为两个托管实例分别配置凭据和 TLS URL，不使用本地默认地址。

已有部署先核验配额 Redis 数据量和 AOF 备份；若接近或超过新限额，先扩容并调整 Compose 中的限额及容器内存。重建 Redis 容器保留现有 `redis` 命名卷，不执行 `down -v`。新增 `redis-cache` 后重建应用，旧响应缓存自然到期，新缓存按需填充，不搬迁配额或去重键。检查两台 Redis 的 `INFO memory`，确认限额及策略，并核对当前月份配额。

```powershell
docker compose --env-file .env -f deploy/compose/compose.yaml build gateway
docker compose --env-file .env -f deploy/compose/compose.yaml up -d redis redis-cache
docker compose --env-file .env -f deploy/compose/compose.yaml up -d --force-recreate gateway admin worker
```

应用容器限制 1GiB，`GOMEMLIMIT=768MiB` 为 Go 软内存预算；Kubernetes ConfigMap 提供相同默认值。配置 `GW_MAX_CONCURRENT_REQUESTS=16`、`GW_MAX_ADMIN_REQUESTS=8`，实例过载立即返回 `503 gateway_overloaded` 和 `Retry-After: 1`。提高并发前须测量最大请求/响应下的内存，不应只提高容器限制；离线 Compose 为 512MiB、软预算 384MiB、业务 8 槽和管理 4 槽。Kafka 本地堆上限 512MiB、容器限制 1536MiB；这些是联调默认值，不是云容量承诺。

## 云 Kubernetes

模板位于 `deploy/k8s`，使用外部 MySQL、Redis、Kafka、etcd、Milvus，不创建云资源。替换 `kustomization.yaml` 镜像仓库和提交标签，替换域名、Ingress class、TLS Secret、网络出口范围和资源配额。

1. 使用云 Secret Manager 或 External Secrets 创建 `gateway-runtime`，键名与 `.env.example` 一致。至少包含主密钥、DSN、两个独立 Redis URL（`GW_REDIS_URL` 与 `GW_CACHE_REDIS_URL`）、Kafka broker、etcd endpoint、Milvus URL，按实际服务补齐认证和 OTLP。不要把初始管理员密码放入长期运行的 Secret。
2. 单独创建 `gateway-bootstrap` 存放初始管理员密码，以及 `gateway-tls` 保存域名证书。
3. 应用 namespace、service account 和 ConfigMap，然后使用 `setup-job.yaml` 依次执行 migrate、bootstrap、可选 cache-init；每个 Job 使用不同名称。初始化完成后再应用全部 workload。
4. 执行 `kubectl apply -k deploy/k8s`，检查 Deployment、Pod readiness、实例配置版本及 Kafka lag。需要 metrics-server 使 HPA 生效，CNI 必须支持 NetworkPolicy。
5. 内部监控从 Service 或 Pod 的 `/metrics` 抓取，Ingress 仅暴露业务及控制台路径。导入 Prometheus 规则及 Grafana JSON，接入云端 Kafka/MySQL/Redis/etcd 指标和告警接收器。

运行层支持多副本：数据库事务序列化配置及修正版本，Redis 共享配额和管理会话，Kafka 消费组分配计量分区，etcd CAS 阻止旧版本覆盖。熔断和延迟 EWMA 仍为每实例状态，基础设施治理指标由外部服务汇总。

共享 Kafka/etcd 的不同部署环境应使用不同 `GW_KAFKA_TOPIC`、`GW_KAFKA_GROUP` 和 `GW_ETCD_PREFIX`；同一环境的各 worker 使用相同消费组。MySQL 数据库、Redis 数据库及 Milvus collection 也须按环境隔离。默认消费组 `gateway-metering`、配置前缀 `/llm-gateway` 与既有部署一致，新增可选参数无需重配现有环境。

TLS 使用系统信任 CA：MySQL DSN 使用 `tls=true`，Redis 使用 `rediss://`，Kafka 打开 `GW_KAFKA_TLS` 并配置 SASL，etcd 使用 `https://`，Milvus 使用 HTTPS。私有 CA 可通过镜像信任库或标准证书环境配置安装；当前不提供 mTLS client certificate 参数。选择与这些能力兼容的服务认证方式。

每个组件默认两个副本，有 startup/readiness/liveness probes、资源 requests/limits、PDB、HPA、无权限服务账号和只读文件系统。默认网络模板按端口放行出口，上线时应收紧为实际 VPC CIDR/私网服务、OTLP collector 及厂商 HTTPS 地址范围。

## SSE 和更新

### 计量性能参数

`GW_METERING_COMMIT_BATCH_SIZE` 默认 32，范围 1～256；`GW_METERING_COMMIT_INTERVAL_MS` 默认 100，范围 10～1000。每分区仅合并提交连续完成 SQL 入账、Redis 结算、outbox 交付与结算水位的消费位置。空闲分区到期提交尾部，正常停机尽力提交，单次提交期限 1 秒；慢处理步骤可能超过时间阈值，不能据此承诺绝对 100ms 提交。强杀或重平衡后允许幂等重放，设置批次 1 可恢复逐条提交，不改变 Redis 的逐事件结算。

MySQL DSN 显式包含 `charset=utf8mb4`，且没有显式 `interpolateParams` 设置、没有其他字符集的 collation 时，默认启用驱动参数转义，减少临时 prepare/close 往返。所有 JSON 列以字符串参数保存，BLOB 仍为字节载荷；默认连接超时不变。需要回退时在 DSN 设置 `interpolateParams=false`。未明确 utf8mb4 的连接保留预处理路径，本次不会修改现有 `.env` 或重启容器。

本版本无需新增表或清空历史数据。生产升级仍须先确认当前计量状态，在预发验证配置、故障恢复及完整核验期间的影响；周期屏障核验及其中断后的恢复流程保持现有规则。

Ingress 必须关闭响应缓冲，读取/发送超时至少 130 秒，负载均衡器也应配置相同或更长空闲超时。应用流式期限 120 秒，计量保存/补偿另有最长 9 秒操作预算。收到 SIGTERM 后停止监听并等待最多 150 秒；Compose/Kubernetes grace 为 155 秒，留出计量和请求汇总收尾时间。先摘流量再终止，滚动更新保持可用副本。

## outbox 版本升级

许可/屏障版本新增 gw_metering_apps、gw_metering_admissions、gw_metering_reports。已有 checkpoint 的存量部署也需停写备份、migrate、quota-rebuild、metering-resume，建立事件哈希、Redis generation 和结算水位；不能因旧 ready 尚在而跳过重建。先启 worker 等待许可，再启入口；此过程不会由应用启动自动执行。etcd 权限须覆盖同一 GW_ETCD_PREFIX 下 /metering/：gateway/admin 读取许可及状态、写入 /metering/state 发布冻结；worker 读写状态和许可并管理 lease。生产当前使用独立非 Cluster 配额 Redis，Lua 访问全局与应用键，不支持 Redis Cluster 跨槽执行。

检查点版本还新增 stream、checkpoint、recovery 表。存量升级先用旧 worker/sender 排空，摘流量并停止所有写入者，保留一致性备份，执行新 migrate；确认停写后用 `/app/metering-checkpoint-init` 建基线、核验并显式恢复接单，再启新角色。新 worker 遇到缺失检查点不会在线猜位置。新部署 bootstrap 只对空 SQL/Kafka 自动初始化。已具备检查点的旧备份通过 metering-recover plan/apply 回放，详见 operations.md。

新增 outbox、容量、冻结控制与修正幂等表，运行服务不会自动执行 DDL。首次升级在维护窗口摘流量，停止 gateway/admin/worker 和其他计量写入者，备份 SQL/Redis 并记录 Kafka 位置；构建新镜像，用迁移凭据执行 `/app/migrate`，再更新角色。已有账本/配额不清空，不执行 down -v。

Compose 与云 ConfigMap 均提供 `GW_OUTBOX_GLOBAL_BYTES=536870912`、`GW_OUTBOX_APP_BYTES=67108864`。应用可配置 `outbox_bytes` 覆盖默认，每应用逻辑限额与全局共享限额同时生效；这不是物理磁盘隔离。新接单与 completed 保存各最多 8 个 SQL 保存槽，后台发送按最多 32 条/4MiB 领取，数据库总连接数仍需按角色副本数核算。

`GW_MAX_METERING_INFLIGHT` 默认每实例 8，约束整个付费 attempt 到计量收尾，独立于 HTTP 请求并发限制。上游慢时它也会占用槽位；调大前必须验证 completed SQL 保存、Kafka ACK 与消费者的容量，不能只按入口能处理多少请求来配置。全局 SQL 容量行仍是共享瓶颈，水平扩网关不保证计量容量同步增长。

启动后核对 worker 消费与 outbox 状态、配额和 readyz；有计量冻结按 operations.md 停写重建/核验再恢复。新同步 SQL 写入与额外事务改变了成本和延迟，既有配额压测结果不能作为新版本容量保证。本轮代码与隔离验收不等于已更新现有容器或完成云预发。

使用提交标签发布镜像并记录 digest，发布前在预发环境执行基础设施与真实厂商协议回归。数据库迁移先行，保持前后版本兼容。使用 `kubectl rollout undo deployment/gateway -n llm-gateway` 等回滚代码；配置灰度由管理页面回滚，避免恢复过期 Key 配置。

模板目前通过 YAML 和 Linux 编译校验，本机已具备 Docker Desktop 和 Linux 容器引擎。容器启动、基础设施联调和云部署仍须分别验证后再接入生产流量。

配置依据：[Milvus REST collection 创建](https://milvus.io/api-reference/restful/v2.6.x/v2/Collection%20%28v2%29/Create.md)、[Milvus standalone 部署](https://milvus.io/docs/v2.6.x/install_standalone-docker-compose.md)、[Kafka 官方 Docker 配置](https://github.com/apache/kafka/tree/4.0.0/docker/examples)。镜像示例锁定版本，上线前仍应执行镜像漏洞扫描并更新到验证过的补丁版本。
