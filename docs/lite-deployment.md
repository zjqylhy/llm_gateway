# 阿里云个人部署操作指南：2 核 4 GiB

适用：自己试用、少量调用、中国内地服务器、备案通过后公开访问。方案见 [需求规格](lite-deployment-spec.md)，实际验证范围见 [验收报告](lite-deployment-report.md)。这是容量起点，整机规格仍需实机验证。

本指南按“第一次部署服务”编写。Windows 上的命令在 PowerShell 7 中运行；服务器上的命令在 SSH 登录后的 Linux 终端运行。示例域名 `example.com`、公网 IP `203.0.113.10` 必须换成自己的值。不要把密钥发到聊天或提交到 Git。

## 1. 先了解需要买什么

| 项目 | 用途 | 本次建议起点 |
| --- | --- | --- |
| ECS | 运行程序及数据库 | 中国内地、x86_64、2 vCPU / 4 GiB、Ubuntu 24.04 LTS |
| 系统盘 | Docker 镜像、账本、消息和日志 | 推荐 60 GiB；预算优先可先用 40 GiB |
| 公网 IP 与带宽 | SSH、HTTPS 和调用厂商 | 有公网 IPv4；少量文本调用先评估 1–3 Mbps |
| 域名 | 给 IP 一个稳定名称 | 在阿里云中国站注册可备案域名，完成实名认证 |
| HTTPS 证书 | 加密浏览器/API 流量 | Caddy 自动申请和续期，不需另购付费证书 |
| 模型厂商账号 | 提供实际推理服务 | 另外充值/计费，服务器不会本地运行大模型 |

磁盘和带宽是实施起点，不是测出的容量；1 Mbps 理论上约 125 KB/s，较大响应和多路流会受带宽限制。费用比较要包含首购、续费、磁盘、带宽/流量、域名续费、快照和模型费用。购买页的即时价格为准，不把促销价当长期成本。[阿里云 ECS 计费说明](https://www.alibabacloud.com/help/zh/ecs/billing-overview)

个人少量调用先使用一个系统盘即可，不必另购数据盘。2026-10-11 本机检查中，应用、MySQL、Redis、Kafka、etcd 和 Caddy 镜像的标称大小合计约 2.4 GiB；持续验收中三个主要数据卷合计约 0.42 GiB。这是初始测试数据，不能预测长期用量。还需给 Ubuntu、Docker 文件、镜像更新期间新旧版本共存、账本和消息增长留余量。40 GiB 可作为节省预算的起点；初次部署推荐 60 GiB，长期保留较多记录或本地备份再评估 100 GiB。

保留至少 20% 空闲空间，定期用 `df -h /`、`sudo docker system df` 查看占用；本配置已限制容器日志轮转，但数据库和账本不会自动因磁盘不足被删除。备份应另存到本机或对象存储，避免仅留在同一系统盘。后续可以扩容云盘，还需按文件系统类型扩展分区和文件系统。[阿里云 Linux 云盘扩容说明](https://help.aliyun.com/zh/ecs/user-guide/resize-linux-cloud-disks/)

本配置关闭 Milvus、MinIO、缓存 Redis、Prometheus、Grafana，不提供语义缓存。保留 MySQL、配额 Redis、Kafka、etcd、控制台和计量 worker。三个 Go 角色各限制 256 MiB，常驻容器硬上限合计 3200 MiB，剩余空间留给系统。硬上限并不等于实际占用，也不能证明 4 GiB 整机一定稳定。

## 2. 注册账号、购买域名并申请备案

1. 登录阿里云中国站，完成账号实名认证并启用账号多因素认证。
2. 在域名注册页面搜索名称。初次可选常见的 `.com`，付款前检查首年和续费价，以及是否可用于当前备案主体。
3. 提交域名实名模板。个人备案时按备案要求使用本人资料；核对域名持有人与备案主体信息，等待域名实名状态通过。
4. 在 ECS 购买页选择中国内地地域、2 核 4 GiB、x86_64、Ubuntu 24.04、有公网带宽。地域通常选靠近自己的内地节点。
5. **付款前核对备案资格。** 当前 [服务码 FAQ](https://help.aliyun.com/zh/icp-filing/basic-icp-service/support/server-check-faq)写 ECS 包年包月购买时长“大于 3 个月”，[ECS 限制页](https://www.alibabacloud.com/help/zh/ecs/user-guide/limitations)写“3 个月及以上”。以备案系统校验为准；拿不准时，先向阿里云确认，或评估满足两种表述的 6 个月方案。免费试用、按量付费实例不能直接按此申请备案。
6. 进入阿里云 ICP 备案系统，按本人真实用途填写主办者、网站信息，上传资料并完成真实性核验、阿里云初审、工信部短信核验和管局审核。个人项目可否按其实际内容备案，应由当前省份规则和审核结果确认，不使用不实的网站描述。[官方备案流程](https://help.aliyun.com/zh/icp-filing/basic-icp-service/user-guide/icp-filing-application-overview)

备案期间先完成后面的内网部署测试。保持公开入口关闭，域名解析和 80/443 开放放在备案通过后执行。公网开放后还应展示备案号，并按官方说明在网站开通后 30 天内办理公安联网备案；这与购买域名是不同步骤。[备案后处理要求](https://help.aliyun.com/zh/icp-filing/basic-icp-service/user-guide/icp-filing-application-overview)

## 3. 设置安全组并连接服务器

在 ECS 控制台找到实例，记录公网 IP。找到关联安全组，开始只允许 TCP 22，来源填自己当前公网 IP 的 `/32`，例如 `你的公网IP/32`。不要开放 3306、6379、9092、2379、8080、8081、8082；这些都是内部端口。80/443 等备案后再开放。[安全组说明](https://www.alibabacloud.com/help/zh/ecs/user-guide/start-using-security-groups)

选择 SSH 密钥登录。Windows PowerShell 中执行：

```powershell
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE/.ssh" | Out-Null
ssh-keygen -t rsa -b 4096 -f "$env:USERPROFILE/.ssh/aliyun_gateway"
Get-Content "$env:USERPROFILE/.ssh/aliyun_gateway.pub"
```

把 `.pub` 公钥导入与实例同地域的阿里云密钥对，并按控制台指引绑定实例；私钥保存在自己电脑。这里选择官方支持导入的 RSA 类型。[密钥管理说明](https://help.aliyun.com/zh/ecs/user-guide/instance-logon-credential-management)。确认控制台显示的 SSH 用户名，以下以 `ubuntu` 为例；如果显示 `root` 或其他名称，要替换后文所有 SSH/scp 的 `ubuntu`：

```powershell
ssh -i "$env:USERPROFILE/.ssh/aliyun_gateway" ubuntu@203.0.113.10
```

第一次连接核对服务器指纹再接受。连不上先检查实例已启动、用户名、密钥是否绑定、22 端口来源 IP，或使用阿里云 Workbench。不要因连不上就把所有端口全部开放。

## 4. 在 Linux 上安装 Docker

以下仅用于全新 Ubuntu 24.04 服务器，使用 Ubuntu 发行版维护的 Docker/Compose 包；已有 Docker 不重复安装。

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 openssl nano ca-certificates
sudo systemctl enable --now docker
sudo docker version
sudo docker compose version
uname -m
free -h
df -h /
```

需要看到 Docker Engine/Compose 版本，CPU 架构为 `x86_64`，内存和磁盘与购买规格一致。这里使用 `sudo docker`，无需把用户加入具有高权限的 docker 组。包来源：[Ubuntu docker.io](https://packages.ubuntu.com/noble/docker.io)、[Ubuntu Compose v2](https://packages.ubuntu.com/noble/docker-compose-v2)。如需 Docker 官方仓库版本，按 [官方 Ubuntu 安装指南](https://docs.docker.com/engine/install/ubuntu/)操作。

准备目录：

```bash
sudo install -d -m 750 -o "$USER" -g "$(id -gn)" /opt/llm_gateway
```

## 5. 在 Windows 构建并上传

本机需要 Go（满足 `go.mod`）、Node.js 24、PowerShell 7、Docker Desktop 的 Linux 容器模式。构建在本机完成，服务器只加载镜像。

```powershell
Set-Location C:\Users\LHY95\Documents\Codex\llm_gateway
./scripts/build-lite.ps1 -Image llm-gateway:lite-v1
docker save -o data/llm-gateway-lite-v1.tar llm-gateway:lite-v1
tar -czf data/llm-gateway-lite-config.tar.gz deploy/lite docs/lite-deployment.md docs/operations.md
scp -i "$env:USERPROFILE/.ssh/aliyun_gateway" data/llm-gateway-lite-v1.tar data/llm-gateway-lite-config.tar.gz ubuntu@203.0.113.10:/opt/llm_gateway/
```

服务器解包并加载：

```bash
cd /opt/llm_gateway
tar -xzf llm-gateway-lite-config.tar.gz
sudo docker load -i llm-gateway-lite-v1.tar
cp deploy/lite/env.example .env
chmod 600 .env
```

Docker 镜像拉取失败时先检查服务器网络及仓库访问。可在 Windows 下载本配置使用的 MySQL/Redis/Kafka/etcd/Caddy 镜像，再用 `docker save` 导出、`scp` 上传和 `docker load` 加载；这不会把 amd64 镜像变成 ARM 镜像。不要使用来源不明的镜像。运行镜像也可通过脚本的 `-RuntimeImage` 参数指定已核验的镜像摘要。

## 6. 生成并填写凭据

在 Linux 中生成主密钥与密码，分别复制到 `.env` 对应项，不把输出发给他人：

```bash
openssl rand -base64 32
openssl rand -hex 16
openssl rand -hex 16
openssl rand -hex 16
openssl rand -hex 16
nano .env
```

| 配置项 | 填写内容 |
| --- | --- |
| GW_IMAGE | `llm-gateway:lite-v1` |
| GW_MASTER_KEY | 第一个命令生成的 32 字节 base64 主密钥 |
| GW_ADMIN_PASSWORD | 一个独立随机密码，12–72 字节 |
| GW_LOCAL_DB_PASSWORD | 独立随机十六进制数据库密码 |
| GW_LOCAL_ROOT_PASSWORD | 独立随机十六进制数据库 root 密码 |
| GW_LOCAL_REDIS_PASSWORD | 独立随机十六进制 Redis 密码 |
| GW_API_HOST | `api.你的域名`，不带 `https://` |
| GW_CONSOLE_HOST | `console.你的域名`，不带 `https://` |
| GW_ADMIN_ORIGIN | 备案前保持 `https://localhost:8443` |

`nano` 用 Ctrl+O、回车保存，Ctrl+X 退出。本配置自动生成容器内连接地址；密码用十六进制字符可避免 DSN/URL 转义问题。全局缓存在 Compose 中强制关闭，连接池和并发也由该配置注入，仅修改 `.env` 的数字不会覆盖这些硬性初值。

主密钥用于解密厂商凭据，必须与数据备份一起妥善保存；已有数据不能随意换它。MySQL/Redis 密码修改需要同步修改服务端账户或配置，单改 `.env` 不会更新已初始化数据库中的用户密码。

## 7. 分步初始化，不开放公网入口

为避免误操作，把本次固定项目名与配置封装成命令。在 Linux 项目目录执行；换 SSH 窗口后重新定义函数：

```bash
cd /opt/llm_gateway
dc() { sudo docker compose --env-file .env -p llm-gateway-lite -f deploy/lite/compose.yaml "$@"; }
dc config --quiet
dc up -d --wait mysql redis kafka etcd
dc run --rm kafka-init
dc run --rm migrate
dc run --rm bootstrap
dc up -d gateway admin worker edge-local
dc ps
dc logs --tail 60 gateway admin worker edge-local
```

`config --quiet` 成功没有输出。不要把不加 `--quiet` 的完整渲染配置发给别人，其中会包含凭据。四个依赖需健康，初始化命令需返回成功，三个应用进程和 `edge-local` 需处于运行状态。首次 Kafka/MySQL 初始化可能耗时，命令失败先看对应日志，不继续执行后续命令。

首次 bootstrap 如果仅报 Kafka `Not Coordinator For Group`，等待 5–10 秒后重新执行 `dc run --rm bootstrap`；这时消费组协调器可能尚未完成初始化。bootstrap 可重复执行，不会覆盖已有管理员密码。其他错误应先排查，不直接循环重试。

本方案的 `edge-local` 只绑定服务器 `127.0.0.1:8443`；即使服务器有公网 IP，外部也不能直接访问这个端口。不要执行根目录离线模拟 Compose，也不要把本配置叠加到既有完整部署。

## 8. 通过 SSH 隧道访问控制台

Windows 新开 PowerShell，保持该窗口运行：

```powershell
ssh -i "$env:USERPROFILE/.ssh/aliyun_gateway" -N -L 8443:127.0.0.1:8443 ubuntu@203.0.113.10
```

浏览器打开 `https://localhost:8443`。这是 Caddy 本地 CA 证书，需要建立信任。服务器执行以下命令导出**根证书，不导出私钥**：

```bash
dc exec -T edge-local cat /data/caddy/pki/authorities/local/root.crt > local-ca.crt
```

Windows 下载并导入当前用户受信任根证书（核对文件来自自己的服务器）：

```powershell
scp -i "$env:USERPROFILE/.ssh/aliyun_gateway" ubuntu@203.0.113.10:/opt/llm_gateway/local-ca.crt data/aliyun-gateway-local-ca.crt
certutil -user -addstore Root data/aliyun-gateway-local-ca.crt
```

重新打开浏览器，用 `.env` 的管理员账号和密码登录。若本机 8443 被占用，可把隧道本地端口改为 9443，并将服务器 `GW_ADMIN_ORIGIN` 改为 `https://localhost:9443`、重建 admin；隧道远端仍指向 8443。保持浏览器地址与 Origin 完全一致。

## 9. 配置真实厂商并发起第一次调用

1. 登录真实模型厂商控制台，取得 API Key，并确认在当前 ECS 网络上可以访问厂商 API。
2. 在本项目控制台“模型与路由”中新增厂商，填写其官方 OpenAI 兼容 Base URL、凭据；Base URL 通常到 `/v1`，不要重复附加 `/chat/completions`。不同厂商以官方文档为准。
3. 添加模型：模型 ID、厂商、厂商模型名称、上下文及输出上限、支持的能力、实际价格版本。界面单价是“微单位 / 百万 Token”：例如 CNY 2 元 / 百万输入 Token 填 `2000000`；人民币汇率填 `1000000`，其他币种按实际汇率填写。
4. 添加 `chat` 路由，候选指向该模型。若需要 Embedding，另加支持该能力的模型及路由；关闭语义缓存不影响直接 Embedding API。
5. 在“应用与密钥”新建自己的应用，只允许必要路由。建议先设 QPS=1、有限月 token 和预算、预算动作“拒绝”，正文采样=0；语义缓存开关应不可开启。
6. 创建 API Key，保存到密码管理器。等待配置发布完成、所有存活实例版本同步，在 Playground 先试普通请求，再试流式请求。

Windows API 示例，Key 使用环境变量临时注入；未备案时通过隧道调用 localhost：

```powershell
$env:GATEWAY_API_KEY = Read-Host '输入应用 API Key' -MaskInput
$body = '{"model":"chat","messages":[{"role":"user","content":"你好"}],"max_tokens":64}'
Invoke-RestMethod -Uri https://localhost:8443/v1/chat/completions -Method Post -Headers @{Authorization="Bearer $env:GATEWAY_API_KEY"} -ContentType application/json -Body $body
$env:GATEWAY_API_KEY = $null
```

调用成功后，在控制台调用审计、成本和额度中核对记录。流式请求只有正常结束并且完成计量确认后才返回 `[DONE]`；遇到计量错误，不盲目自动重试，先按请求 ID 核查费用。

## 10. 备案通过后启用域名与公开 HTTPS

先取得真实备案号。在 Windows 重新构建，把备案号编译到登录页及控制台页脚；备案号不是运行时 `.env` 项：

```powershell
./scripts/build-lite.ps1 -Image llm-gateway:lite-v2 -IcpNumber '你的真实ICP备案号'
docker save -o data/llm-gateway-lite-v2.tar llm-gateway:lite-v2
scp -i "$env:USERPROFILE/.ssh/aliyun_gateway" data/llm-gateway-lite-v2.tar ubuntu@203.0.113.10:/opt/llm_gateway/
```

在阿里云云解析 DNS 中为已备案域名添加两个 A 记录：

| 记录类型 | 主机记录 | 记录值 |
| --- | --- | --- |
| A | api | ECS 公网 IPv4 |
| A | console | 同一 ECS 公网 IPv4 |

两个子域名复用同一注册域名，无需另购两个域名。沿用已备案网站对应的域名和实际用途，变更按备案要求处理。不要在没有可用 IPv6 时额外添加 AAAA。[阿里云 DNS 添加记录](https://help.aliyun.com/zh/dns/add-a-dns-record)

安全组开放 TCP 80 和 443，来源为 `0.0.0.0/0`，供公网访问及 Caddy 证书校验；SSH 22 仍限制本人 IP。业务访问仍需应用 Key，控制台仍需管理员登录。确认 DNS 后，Linux 执行：

```bash
cd /opt/llm_gateway
sudo docker load -i llm-gateway-lite-v2.tar
nano .env
# 修改 GW_IMAGE=llm-gateway:lite-v2
# 修改 GW_ADMIN_ORIGIN=https://console.你的域名
dc stop gateway admin
dc up -d gateway admin worker
dc stop edge-local
dc --profile public up -d edge
dc logs --tail 80 edge
```

将已有写入请求排空后执行切换。等待 Caddy 申请证书，浏览器应能正常验证 `https://console.你的域名`，首页应显示真实备案号及查询链接；API 地址改成 `https://api.你的域名/v1`。Caddy 需要正确 DNS 和公网 80/443，证书目录使用持久卷，正常续期无需手动操作。[Caddy HTTPS 官方指南](https://caddyserver.com/docs/quick-starts/https)

本配置公网入口不暴露指标、健康检查或中间件端口。两个入口分别提供 API 与控制台，实际仍为同一台服务器。备案号展示、公安备案及可能的省份特殊要求，按审核材料和官方指引补齐后再正式使用。

## 11. 日常检查、备份和更新

常用 Linux 命令：

```bash
dc ps
sudo docker stats --no-stream
free -h
df -h /
dc logs --since 10m --tail 100 gateway admin worker
dc exec -T redis sh -c 'REDISCLI_AUTH=$REDIS_PASSWORD redis-cli INFO memory'
```

关注 OOM、容器异常重启、内存持续逼近上限、磁盘空间、计量积压、应用冻结和 MySQL 池等待。Redis 最大逻辑内存为 64 MiB，接近限额会拒绝新增费用，不能改成 LRU 淘汰来“修复”。两路并发满时 `503 overloaded` 是保护行为；周期核验期间可能短暂拒绝该应用，新请求在核验后恢复。预算或配额拒绝属于独立限制。

首次用真实厂商、小额度调用并观察至少一小时。若出现依赖 OOM 或明显持续冻结，先保留诊断并查瓶颈；无法满足时升配，不删除计量组件。不要把 swap 当作满足内存预算的证明。

购买实例后，在阿里云云监控中为自己的 ECS 设置磁盘告警：

1. 打开目标实例的主机监控页面，按控制台指引安装/启用监控插件，确认能看到系统盘对应文件系统的磁盘使用率数据；仅有磁盘读写速度不能说明剩余容量。
2. 创建报警规则，范围选该实例和系统盘挂载点，指标选磁盘使用率，先设置达到 80% 且连续 5 分钟告警（若界面按采样点配置，按所选采样周期换算）。这是本方案的起始阈值。
3. 创建只包含自己的报警联系人组，完成邮箱验证并关联规则。先查看所选监控及通知方式的计费，不默认启用付费电话或短信。
4. 检查规则有监控数据且处于启用状态，后续定期查看报警历史。不要通过填满业务磁盘来测试告警。

监控插件必须正常上报磁盘使用率，规则才会触发；控制台版本的菜单和支持周期可能不同，以当前界面为准。[官方报警规则说明](https://help.aliyun.com/zh/cms/cloudmonitor-1-0/user-guide/create-an-alert-rule-1)

备份采用停写一致性窗口：停止公开入口及 gateway/admin，等待 worker 的 outbox、消费和结算追平，再停止 worker，按 [运维指南](operations.md)完成停写核验并停止数据库依赖，再备份**全部命名卷、.env 主密钥、配置和对应镜像版本**。备份另存到服务器之外；恢复先在独立环境验证。不提供“在线复制数据库文件”或“只备份 MySQL 就能完整恢复”的捷径。

更新时保留旧镜像标签及一致性备份，上传新标签，先读迁移说明、安排停写窗口，再迁移/切换镜像和核对状态。没有不兼容迁移时可改回旧 GW_IMAGE 并重建应用；已经做了不兼容迁移时，要按备份恢复流程处理，不能只回退镜像。

不要对业务项目运行 `down -v`、`docker volume prune` 或清空 Redis/Kafka。`verify-lite.ps1` 的卷删除仅用于其自己生成的随机验收项目。故障恢复及 pending 费用核实详见 [运维指南](operations.md)。

## 12. 复现隔离验收

Windows 项目根目录执行：

```powershell
./scripts/build-lite.ps1 -Image llm-gateway:lite-acceptance
./scripts/verify-lite.ps1 -Minutes 60 -Image llm-gateway:lite-acceptance
```

脚本不读取自己的 `.env`，创建随机项目、独立数据卷、临时密码及 loopback 端口，使用生产迁移/bootstrap 命令；转发测试仅在测试二进制中允许访问可控私网上游。覆盖两路普通/SSE、慢请求、大输入输出、断流、过载、周期核验、worker 强杀、Redis AOF 重启及停写核验，最后启动生产入口检查健康。

日志、每 30 秒资源快照、汇总 JSON 留在 `data/acceptance/gw_lite_<随机值>/`；结束清理该项目卷和临时凭据。`-Minutes 1` 仅用于排错，不能算 60 分钟验收。即使该测试通过，也只能说明所列容器限额下的链路情况，不能代替独立 2 核 4 GiB 整机、真实厂商与阿里云实机验收。

## 常见问题

| 现象 | 首先检查 |
| --- | --- |
| SSH 超时 | 实例状态、安全组来源 IP、22 端口、密钥和用户名 |
| localhost 控制台打不开 | SSH 隧道是否仍在运行，edge-local/admin 是否启动 |
| 登录 403 | 浏览器地址是否与 GW_ADMIN_ORIGIN 完全一致 |
| 登录后缺少配置 | bootstrap 是否成功，MySQL/etcd 是否健康及配置是否发布 |
| `governance_unavailable` | worker、计量许可、Redis/etcd、周期核验及冻结原因 |
| `upstream_unavailable` | 模型名称、厂商地址、凭据、网络和厂商错误；留存请求 ID |
| 域名 HTTPS 失败 | A/AAAA、80/443、安全组及 edge 证书日志 |
| 容器 exit 137 / OOM | 内存峰值、较大请求和依赖预算；记录后调整或升配 |

参考来源于 2026-10-11 查阅的官方文档；价格、界面和备案规则可能变化，提交和购买时再次核对。
