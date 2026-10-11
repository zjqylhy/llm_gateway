# 本地及阿里云部署记录

日期：2026-10-11（北京时间）。先完成本地部署及真实调用验证，再部署 ECS。两套服务均保持运行，使用独立密码、主密钥和持久卷；项目根目录原有 `.env` 未修改。

## 访问与登录

| 项目 | 本地 | 云端 |
| --- | --- | --- |
| 浏览器地址 | `https://localhost:8443` | `https://localhost:9443`，通过 SSH 隧道 |
| API Base URL | `https://localhost:8443/v1` | `https://localhost:9443/v1` |
| Compose 项目 | `llm-gateway-local-lite` | `llm-gateway-lite` |
| 管理员 | `admin` | `admin` |
| 本机凭据文件 | `data/local-lite/runtime.env` | `data/cloud-lite/runtime.env` |
| 本机应用 Key 文件 | `data/local-lite/application-key.txt` | `data/cloud-lite/application-key.txt` |
| 云端目录 | — | `/opt/llm_gateway` |

在 Windows 的项目目录打开对应凭据文件，查找 `GW_ADMIN_PASSWORD` 后登录。两个密码不同；凭据文件包含其他敏感项，不发到聊天或提交 Git。应用 Key 用于 API 调用，与管理员密码不同。

```powershell
Set-Location C:\Users\LHY95\Documents\Codex\llm_gateway
notepad data/local-lite/runtime.env
notepad data/cloud-lite/runtime.env
```

已配置真实 DeepSeek 厂商、`personal-deepseek-flash` 模型及 `chat` 路由。凭据来自现有 `DEEPSEEK_API_KEY`，通过管理接口加密保存；Qwen 未配置或验证。测试应用分别名为“本地个人测试”和“云端个人测试”，QPS=1、月额度 10000 Token、预算 1 元并超限拒绝，语义缓存关闭，正文采样为 0。上游价格按 2026-10-11 官方公开的高峰 CNY 单价保守估算，不代表实际厂商账单；继续试用前可在控制台按实际需求调整额度和价格。[DeepSeek 价格](https://api-docs.deepseek.com/zh-cn/quick_start/pricing/)

本机两个部署的公有根证书已加入当前用户信任库，浏览器正常显示登录页；没有导出 CA 私钥。

## 云端连接与备案阶段

ECS 为 `47.113.231.252`，SSH 用户 `root`、端口 `22`。原下载私钥权限过宽，建立了仅当前用户可访问的副本 `data/local-lite/cloud-ssh.pem`。绑定生效后已成功登录。公钥指纹：`SHA256:osSiuNzlVKi71miycppzY3ppgUGxuz/RnaDGNocABQc`。

本机已建立后台 SSH 隧道。电脑重启或隧道中断后，在新 PowerShell 窗口运行以下命令，并保持窗口开启；已连接时不用重复执行：

```powershell
Set-Location C:\Users\LHY95\Documents\Codex\llm_gateway
ssh -i data/local-lite/cloud-ssh.pem -p 22 -o IdentitiesOnly=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -N -L 127.0.0.1:9443:127.0.0.1:8443 root@47.113.231.252
```

云端入口只监听 `127.0.0.1:8443`，三个应用及中间件均未发布公网端口。`9443` 是本人电脑上的隧道端口，不能直接访问 `47.113.231.252:9443`。云端 `GW_ADMIN_ORIGIN=https://localhost:9443`，需用相同浏览器地址；本地的 8443 可同时使用。

备案审核期间不开放公网 Web 入口。阿里云明确说明，未备案成功前通过 IP 对外提供网站访问同样受备案要求约束。备案通过后按[部署指南第 10 节](lite-deployment.md#10-备案通过后启用域名与公开-https)切换域名、Origin 与公开 HTTPS。[官方备案说明](https://www.alibabacloud.com/help/zh/icp-filing/basic-icp-service/product-overview/icp-filing-requirements-for-a-regular-website)

## 运行环境与结果

| 检查 | 本地 | 云端 |
| --- | --- | --- |
| 宿主机 | Docker Desktop，32 CPU / 约 15.18 GiB | 实际 2 CPU，Ubuntu 24.04.5 LTS，内存可用总量 3627 MiB，无 swap |
| 系统盘 | 使用现有 Docker 存储 | 60 GiB 规格，根文件系统约 59 GiB，采样已用 6.3 GiB / 可用 50 GiB |
| Docker / Compose | 现有 Docker Desktop | Ubuntu 维护的 Docker 29.1.3 / Compose 2.40.3 |
| 镜像 | `llm-gateway:lite-reviewed` | 同一镜像；上传包 SHA-256 校验一致 |
| 依赖 | MySQL / Redis / Kafka / etcd 健康 | 同左 |
| 应用 | gateway / admin / worker 就绪 | 同左 |
| HTTPS / 管理员登录 / 页面 | 通过 | 隧道访问通过 |
| 普通 / SSE 可见回答 | 均返回 `OK`，SSE 有 `[DONE]` | 同左 |
| 真实计量 | 4 次完成，225 Token | 4 次完成，269 Token |
| 网关估算费用 | 0.000984 元 | 0.001336 元 |
| 应用重启 | 配置及调用历史保留 | 同左 |
| OOM / 异常重启 | 采样未出现 | 采样未出现 |

应用镜像 ID：`sha256:2f3ddc11ac52900444832313743f9699412e8eb41c68049d988e0c31b9622566`。部署文件使用 Kafka 4.0.2。测试分两轮：首轮普通/SSE 各一次（输出上限 64），补充可见文本验证各一次（输出上限 512），没有持续发起付费请求。最终核对时间约 22:16。原始检查结果留在本机 `data/local-lite/checks.json` 和 `data/cloud-lite/checks.json`。

云端调用后一次采样：容器内存用量约 771 MiB（MySQL 233、Kafka 424、其他合计约 114），主机 `MemAvailable` 约 2355 MiB。重启后约 2365 MiB；此数据是瞬时采样，不是峰值或负载容量保证。宿主机文件缓存可回收，不能仅根据 `free` 列判断内存是否不足。

本轮通过实际 2C4G 上的初始化、少量真实请求及重启验证；未完成该 ECS 上的 60 分钟持续负载、故障恢复压力验收、实例整机重启或公网域名/TLS 验收。先前一小时隔离验收见[低配验收报告](lite-deployment-report.md)，不将其当作本次 ECS 持续容量结果。

## 查看、重启与停止

### 控制台并发修复（2026-10-11）

云端控制台正常加载时并行读取四个管理接口，原 `GW_MAX_ADMIN_REQUESTS=2` 导致 `503 gateway_overloaded`。现调整管理并发为 8，业务调用及在途计量仍为 2；前端合并重叠刷新并等待失败组的其余请求结束。

云端仅更新 admin 容器，镜像为 `llm-gateway:console-concurrency-20261011`，基于原已验证运行镜像加入新前端；gateway、worker 和中间件未重建。原配置和环境文件备份位于服务器 `/opt/llm_gateway/releases/console-concurrency-20261011/`，环境备份权限为 0600。

验证：4 项前端刷新回归通过；前端构建与格式检查、`go test -count=1 ./...`、`go vet ./...`、`go build ./...`、部署 YAML 和有效 Compose 配置检查通过。云端 HTTPS 证书验证、管理员登录、新前端资源通过；20 轮接口分组并发读取共 140 次均为 HTTP 200，管理过载计数增量为 0。本机浏览器经 `https://localhost:9443/` 正常展示登录页。未重复执行付费模型调用或长期容量验收。

代码及测试结果通过 PR 供用户人工确认。

### 运维命令

本机执行（停止不会删除数据）：

```powershell
docker compose --env-file data/local-lite/runtime.env -p llm-gateway-local-lite -f deploy/lite/compose.yaml ps
docker compose --env-file data/local-lite/runtime.env -p llm-gateway-local-lite -f deploy/lite/compose.yaml logs --tail 50 gateway admin worker
docker compose --env-file data/local-lite/runtime.env -p llm-gateway-local-lite -f deploy/lite/compose.yaml restart gateway admin worker
docker compose --env-file data/local-lite/runtime.env -p llm-gateway-local-lite -f deploy/lite/compose.yaml stop
# 需要再次运行时：
docker compose --env-file data/local-lite/runtime.env -p llm-gateway-local-lite -f deploy/lite/compose.yaml start
```

云端操作先登录：

```powershell
ssh -i data/local-lite/cloud-ssh.pem -p 22 root@47.113.231.252
```

在云端 Linux 终端执行：

```bash
cd /opt/llm_gateway
dc() { docker compose --env-file .env -p llm-gateway-lite -f deploy/lite/compose.yaml "$@"; }
dc ps
dc logs --tail 50 gateway admin worker
dc restart gateway admin worker
# 暂停整套服务或恢复运行：
dc stop
dc start
```

使用 `stop/start` 保留配置与数据，不执行 `down -v`。备份时同时保存主密钥和数据；后续域名切换、备份、磁盘告警及资源观察沿用[部署指南](lite-deployment.md)。
