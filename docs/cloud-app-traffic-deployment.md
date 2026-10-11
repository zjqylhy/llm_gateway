# 阿里云重排与应用监控部署记录

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
