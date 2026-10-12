# 应用治理云端部署记录

2026-10-12（北京时间），用户授权部署到既有 ECS。已完成发布、HTTPS/API 核对及浏览器验收。

## 发布版本

- PR #5 已合入远端 master `6adf5c4`，其代码树与已验收提交 `2cc0ae1` 一致。
- 构建来源：固定提交 `2cc0ae11b77640ae2a415cccbf1cc4ed710b0e8b` 的 Git 归档；使用锁文件构建前端，Linux/amd64、CGO 关闭、trimpath 构建全部 11 个服务与维护命令。
- gateway/admin/worker 统一镜像：`llm-gateway:application-governance-20261012-2cc0ae1`。
- 镜像 ID：`sha256:dedf132d7c667c269dc4b7f27d8b6e56c8389d5b1f470d5bbf63a113d7d47597`，OCI revision 为完整代码提交。
- 发布包 SHA256：`320daa7a8291bc07a9123b96be31d4e50203513c31027466c76187e3b1b1b821`。服务器验证归档及逐文件哈希后，基于既有已验证运行镜像离线封装。

## 发布过程

备份原环境及各服务镜像记录，暂停 gateway/admin 接单和管理写入，保留 worker 结算；确认未交付 outbox、在途 admission 和 started 付费 attempt 均为 0 后，停止旧 worker。仅切换镜像设置，先启动并验证新 worker，再启动 gateway/admin。三个服务均通过 readyz。

没有执行迁移、bootstrap、配额重建、清空账本或命名卷。应用路由、密钥、厂商凭据、主密钥、预算、配额和私有监控配置延续原部署。配置版本仍为 v10；此次发布使新增应用覆盖策略和业务/运维视角可用，没有替用户修改现有应用策略。

## 验证结果

- PR 的 push/pull_request CI 均通过，包含 Go race 测试、vet/build、前端检查、部署静态检查和应用镜像构建。
- gateway/admin/worker 均运行同一镜像，无 OOM、异常重启；全局计量 healthy、调用许可有效，三应用控制健康。
- HTTPS 使用现有 CA 验证证书；管理员登录、新前端 HTML/JS/CSS 文件哈希与发布成品一致。
- 三应用的路由查询、全局继承来源、按应用过滤的计量状态和最近核验报告通过。实例接口显示三个在线服务角色，17 条旧心跳记录归入历史。
- 真实 Prometheus 的 1 小时、24 小时业务趋势查询可用；Prometheus 就绪。两个真实成本分组的分项金额加未分摊金额等于总费用。
- 发送一次 `max_tokens=1000000` 的 Chat 验证请求。发送前确认所有 Chat 模型上限低于此值，网关在创建上游 attempt 之前返回 `400 unsupported_capability`。
- 该请求的 request 汇总事件经过真实持久化计量链路入账，应用名称、route_scope、route_strategy 和 config_version 保留，按记录所属应用的 trace 查询通过。没有产生 inference attempt，付费 attempt 数发布前后均为 10；最终 outbox/admission/未终结付费记录均为 0。
- 浏览器验证业务与运维导航分组、三实例和历史折叠、应用有效路由、最新请求的应用溯源和请求详情。已有调用显示当前应用名称及 ID，缺失历史路由快照明确提示，不补造旧数据。

原始非敏感检查结果留在本机 `data/cloud-release/application-governance-20261012-2cc0ae1/`。真实页面截图位于 `data/acceptance/cloud-application-governance-{ops,routing,audit}-20261012.png`，不提交实际业务截图或部署秘密到 Git。

## 访问与备份

正式访问仍为 [https://localhost:9443](https://localhost:9443)，通过本机到 ECS 的 SSH 隧道；公网入口没有新增开放。隧道已恢复，电脑重启后按既有部署文档重新建立。

备份目录：`/opt/llm_gateway/releases/application-governance-20261012-2cc0ae1/backup`，目录权限 0700；`env.previous` 权限 0600，`images.json` 与 `rollback-images.yaml` 保存原镜像。

后续回滚需暂停入口并用现版本 worker 排空新事件，先核对计量水位和账本，再恢复原环境与镜像。旧版 worker 不认识新事件快照字段，存在未交付或需重放的新事件时不可直接切换旧消费者。本次没有执行回滚演练、付费厂商调用、持续压力或整机故障恢复测试。
