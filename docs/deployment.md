# 本地联调与云部署

## 两种运行方式

`cmd/dev` 组合运行三个模块，适合离线调试。`gateway`、`admin`、`worker` 分别运行，使用共享外部服务，适合单台云主机容器或云 Kubernetes。生产模式缺少必要依赖配置会退出，不自动切换内存。

Dockerfile 使用多阶段 Go/Node 构建，最终镜像包含所有命令和控制台静态资源，以非 root 用户运行。镜像没有 shell，不在镜像中写入凭据。修改配置后须重启应用进程；业务配置由 etcd 动态同步。

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
docker compose --env-file .env -f deploy/compose/compose.yaml up -d mysql redis kafka kafka-init etcd minio milvus
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
docker compose @composeArgs pull mysql redis kafka etcd minio milvus
docker compose @composeArgs up -d mysql redis kafka kafka-init etcd minio milvus
docker compose @composeArgs ps
```

本机连接地址分别为 MySQL `127.0.0.1:13306`、Redis `127.0.0.1:16379`、Kafka `127.0.0.1:19092`、etcd `http://127.0.0.1:12379`、Milvus `http://127.0.0.1:19530`。容器内应用仍使用 `.env.example` 中的服务名称。Go 命令不会自动加载 `.env`，需要在各进程启动前注入环境变量；管理页面仍需 HTTPS 反向代理以支持 Secure Cookie。

依赖启动后，从项目根目录执行以下命令可验收真实数据链路和配额恢复：

```powershell
./scripts/verify-infrastructure.ps1
```

脚本从本地 `.env` 读取数据库密码和主密钥，临时注入测试环境变量，不调用真实厂商。它创建专用数据库、Kafka topic/消费组、etcd 配置前缀、Milvus collection 及临时 Redis；结束后清理，日志保存在 `data/acceptance`，现有应用容器继续运行。测试只对该 Redis 执行数据清空。此脚本使用上述默认 loopback 端口，需要本机 Go 和 Docker，结果边界见 `docs/validation.md`。

MinIO 使用 [Milvus 官方部署配置](https://github.com/milvus-io/milvus/blob/master/deployments/docker/standalone/docker-compose.yml)中的 `milvusdb/minio` 同版本镜像，原 `minio/minio` 地址在本机拉取时返回仓库访问错误。普通停止、重建容器保留命名卷中的数据。

本机管理页面为 `https://localhost:8443`，Caddy 使用本地证书。应导入 Caddy 根证书建立本机信任，浏览器证书警告需用户处理。该 Compose 中间件配置仅供本机联调，不能作为公网生产服务。

注册向量模型后设置真实输出维度及 `GW_CACHE_DIMENSIONS`，运行 `cache-init` 创建 Milvus collection。collection 中只使用一种维度；需要不同维度时配置新的 collection 和相应网关部署。缓存默认关闭，未创建 collection 时会绕过并记录缓存错误。

线上 Kafka 至少 3 个 broker，事件 topic 建议 6 个以上分区、3 副本、`min.insync.replicas=2`、禁用非同步副本选主。保留期覆盖最大离线对账窗口，示例为 7 天；按吞吐和数据政策调整。配额 Redis 使用独立数据库、持久化和 noeviction，使用单主 HA endpoint；当前客户端没有 Redis Cluster/Sentinel 自动发现适配。

## 云 Kubernetes

模板位于 `deploy/k8s`，使用外部 MySQL、Redis、Kafka、etcd、Milvus，不创建云资源。替换 `kustomization.yaml` 镜像仓库和提交标签，替换域名、Ingress class、TLS Secret、网络出口范围和资源配额。

1. 使用云 Secret Manager 或 External Secrets 创建 `gateway-runtime`，键名与 `.env.example` 一致。至少包含主密钥、DSN、Redis URL、Kafka broker、etcd endpoint、Milvus URL，按实际服务补齐认证和 OTLP。不要把初始管理员密码放入长期运行的 Secret。
2. 单独创建 `gateway-bootstrap` 存放初始管理员密码，以及 `gateway-tls` 保存域名证书。
3. 应用 namespace、service account 和 ConfigMap，然后使用 `setup-job.yaml` 依次执行 migrate、bootstrap、可选 cache-init；每个 Job 使用不同名称。初始化完成后再应用全部 workload。
4. 执行 `kubectl apply -k deploy/k8s`，检查 Deployment、Pod readiness、实例配置版本及 Kafka lag。需要 metrics-server 使 HPA 生效，CNI 必须支持 NetworkPolicy。
5. 内部监控从 Service 或 Pod 的 `/metrics` 抓取，Ingress 仅暴露业务及控制台路径。导入 Prometheus 规则及 Grafana JSON，接入云端 Kafka/MySQL/Redis/etcd 指标和告警接收器。

运行层支持多副本：数据库事务序列化配置及修正版本，Redis 共享配额和管理会话，Kafka 消费组分配计量分区，etcd CAS 阻止旧版本覆盖。熔断和延迟 EWMA 仍为每实例状态，基础设施治理指标由外部服务汇总。

共享 Kafka/etcd 的不同部署环境应使用不同 `GW_KAFKA_TOPIC`、`GW_KAFKA_GROUP` 和 `GW_ETCD_PREFIX`；同一环境的各 worker 使用相同消费组。MySQL 数据库、Redis 数据库及 Milvus collection 也须按环境隔离。默认消费组 `gateway-metering`、配置前缀 `/llm-gateway` 与既有部署一致，新增可选参数无需重配现有环境。

TLS 使用系统信任 CA：MySQL DSN 使用 `tls=true`，Redis 使用 `rediss://`，Kafka 打开 `GW_KAFKA_TLS` 并配置 SASL，etcd 使用 `https://`，Milvus 使用 HTTPS。私有 CA 可通过镜像信任库或标准证书环境配置安装；当前不提供 mTLS client certificate 参数。选择与这些能力兼容的服务认证方式。

每个组件默认两个副本，有 startup/readiness/liveness probes、资源 requests/limits、PDB、HPA、无权限服务账号和只读文件系统。默认网络模板按端口放行出口，上线时应收紧为实际 VPC CIDR/私网服务、OTLP collector 及厂商 HTTPS 地址范围。

## SSE 和更新

Ingress 必须关闭响应缓冲，读取/发送超时至少 130 秒，负载均衡器也应配置相同或更长空闲超时。应用流式期限 120 秒，收到 SIGTERM 后停止监听并等待最多 125 秒；Kubernetes termination grace 为 130 秒。先摘流量再终止，滚动更新保持可用副本。

使用提交标签发布镜像并记录 digest，发布前在预发环境执行基础设施与真实厂商协议回归。数据库迁移先行，保持前后版本兼容。使用 `kubectl rollout undo deployment/gateway -n llm-gateway` 等回滚代码；配置灰度由管理页面回滚，避免恢复过期 Key 配置。

模板目前通过 YAML 和 Linux 编译校验，本机已具备 Docker Desktop 和 Linux 容器引擎。容器启动、基础设施联调和云部署仍须分别验证后再接入生产流量。

配置依据：[Milvus REST collection 创建](https://milvus.io/api-reference/restful/v2.6.x/v2/Collection%20%28v2%29/Create.md)、[Milvus standalone 部署](https://milvus.io/docs/v2.6.x/install_standalone-docker-compose.md)、[Kafka 官方 Docker 配置](https://github.com/apache/kafka/tree/4.0.0/docker/examples)。镜像示例锁定版本，上线前仍应执行镜像漏洞扫描并更新到验证过的补丁版本。
