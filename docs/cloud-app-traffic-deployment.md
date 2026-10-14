# 阿里云重排与应用监控部署记录

> 历史版本记录：2026-10-14 的 Grafana 控制台嵌入已替换旧手绘曲线及 app-traffic 接口。当前方案见 [监控嵌入规格](grafana-console-monitoring-spec.md)，下文保留当时的验证证据。
2026-10-12，用户授权部署并推送代码。服务器为既有 ECS，部署目录为 /opt/llm_gateway；访问仍使用 SSH 隧道 https://localhost:9443。

## 发布内容

- Go 服务使用已验收的 520e296 提交，包含文本重排入口、两种百炼适配协议、应用调用埋点及监控查询。
- 显式启用 Lite 私有监控覆盖文件（83dc3ef）。Prometheus 不发布宿主机端口，保留 26 小时采样，内存限额 256 MiB、TSDB 大小上限 256 MB。
- 补充 153ed96 前端修复：全天窗口的少量调用以有效位显示，避免非零 QPS 被四舍五入为 0。前端 8 项测试、生产构建及格式检查通过。
- 管理接口并发上限为 8。现有数据库、业务配置、厂商凭据、主密钥和命名卷保留；未执行迁移或 bootstrap。
- 共享工作区存在另一项未验收需求，发布二进制按已提交版本隔离构建，该需求未包含在本次部署和推送中。

## 实际版本与校验

| 组件 | 实际镜像 |
| --- | --- |
| gateway / worker | llm-gateway:app-traffic-20261012-520e296 |
| admin | llm-gateway:app-traffic-20261012-153ed96 |
| Prometheus | llm-gateway:prometheus-3.5.0-verified |

云端无法连接 Docker Hub。监控镜像使用[官方 Prometheus v3.5.0 发布包](https://github.com/prometheus/prometheus/releases/tag/v3.5.0)，校验 SHA-256 为 e811827af26d822afb09a4f28314f61b618b12cff5369835a67f674d8b46f39a，再基于原已验证运行镜像封装；没有使用第三方镜像源。

主发布包 SHA-256：3b35a29dbd2e6c22ab12beb4c87088bfed666cf190429fd95c5c1c604468e021。
前端修复包 SHA-256：d4c6fe7b899e80dd5ff054f9c0bd3b8a96ed9dec9ece84819f6ff61530cb8e56。
服务器逐项验证发布文件哈希；发布前备份配置和镜像版本，先停止接单、确认计量追平，再更新消费者和业务服务。

## 通过的实机检查

- 有效 Compose 配置验证；部署静态检查覆盖 30 份 YAML、38 个 Grafana 面板。
- 真实 promtool 验证抓取配置和 9 条告警规则。
- gateway/admin/worker 的 readyz 均通过，Prometheus 就绪；三个采集目标均为 up。
- HTTPS 证书验证、管理员登录及新前端资源通过；浏览器确认最近一天监控、错误码表及非零低 QPS 显示正常。
- 管理端真实 PromQL 汇总和趋势查询通过：最近 24 小时、自定义历史区间、超过一天返回 400。
- Chat 参数错误、Embedding 与两个 Rerank 入口的模型权限拒绝进入对应应用失败指标。
- 新容器未发生 OOM 或异常重启；待交付 outbox 和在途 admission 均为 0，付费 attempt 数在本次验证前后保持 27。

瞬时采样中 Prometheus 使用约 23 MiB，主机 MemAvailable 约 2.1 GiB，根文件系统剩余约 49 GiB。这是短时检查，不代表持续容量验收。

## 运维与回滚

后续通过以下入口操作，始终保留监控覆盖文件：

~~~bash
cd /opt/llm_gateway
./manage.sh ps
./manage.sh logs --tail 50 gateway admin worker prometheus
./manage.sh restart gateway admin worker
~~~

备份目录：/opt/llm_gateway/releases/app-traffic-20261012/backup，环境备份权限为 0600。
恢复整个发布前版本时，先安排停写窗口并排空计量，再恢复 env.previous 和 lite/ 下的原配置；使用 backup/rollback-images.yaml 作为最后一个 Compose 覆盖文件启动三个应用，可以恢复各自的原镜像。停用本次新增的 Prometheus 容器会保留其采样卷。未执行回滚演练或删除业务数据。

## 验证边界

- 新埋点的历史从本次上线开始积累，旧调用记录不会自动变为历史 QPS。
- 本次没有新增付费模型请求。重排厂商真实调用仍需配置相应百炼凭据、模型及路由，接口权限检查不能代替厂商成功调用验证。
- Grafana 面板配置已同步到云端，Lite 本次未运行独立 Grafana 容器，控制台通过私有 Prometheus 直接查询。
- 未更改公网入口或执行持续压测。部署结果留在本机 data/cloud-release/app-traffic-20261012/；真实监控截图保存在本机 data/acceptance/cloud-app-traffic-20261012.jpg，不上传实际业务监控截图到 Git。

## 2026-10-12 合并 QPS 图表正式发布

用户确认验收后，将 codex/combined-qps-chart 分支推送至既有 zjqylhy/llm_gateway 远端，并发布已验收镜像。分支包含最新 master 0b5d2b4（PR #3 Token 用量及费用明细）；合并图表代码为 74315c7，后端使用同一分支中 01b1427 的已验证 Go 源码，后续提交仅调整前端及记录。

三个运行服务 gateway/admin/worker 统一使用 llm-gateway:combined-qps-20261012-74315c7，镜像 ID 为 sha256:cfdd701663cfa385fc426ca9f464c9437d4147e1c5e17b39249ecce43c22cf13。镜像标签记录完整代码修订 74315c789604a2a2811682336be7e567e6826513。Prometheus 继续使用既有私有镜像及采样卷。前端包 SHA-256 为 407f5b62696f69d2c057430b2baacabb4cc26d159f0404221d17daba2214c162；构建前验证归档及文件哈希。

发布前备份原环境和各服务镜像，停止接单并确认 outbox/admission 为 0，再切换 worker/gateway/admin。没有执行数据库迁移或 bootstrap。管理并发上限仍为 8，业务容量配置保持既有值。备份目录为 /opt/llm_gateway/releases/combined-qps-20261012/backup，env.previous 权限为 0600；rollback-images.yaml 记录各自原镜像，作为最后一个 Compose 覆盖文件配合 env.previous 恢复，可用于后续回滚。未执行回滚演练。

发布后检查通过：三个 Go 服务 readyz 和 Prometheus 就绪，三个采集目标均为 up；正式 HTTPS 验证证书、管理员登录和最新前端资源；真实 1 小时/24 小时查询返回按 400 invalid_request_error、403 model_not_allowed 拆分的失败趋势，25 小时请求返回 400。真实 MySQL 成本报表两组的费用分项加未分摊费用等于总费用，保留已合入的 Token 明细功能。

浏览器在正式 HTTPS 页面验证每个应用只显示一张合并图，成功和分错误码失败共用左轴，上限使用明确标注的右轴。选中 400 时同时保留 success/400/limit，刷新监控后仍保持该选择；恢复全部错误码后同时显示两种失败曲线。正式截图保存在本机 data/acceptance/combined-qps-deployed-20261012.jpg，发布检查记录保存在 data/cloud-release/combined-qps-20261012/，均不提交实际业务截图及部署秘密。此次发布验证没有新增付费模型请求。

临时预览容器、预览 SSH 隧道及临时环境副本已清理，正式访问继续使用既有 https://localhost:9443 隧道。完整回滚需按前述计量排空顺序安排停写窗口，再恢复原配置和镜像。
