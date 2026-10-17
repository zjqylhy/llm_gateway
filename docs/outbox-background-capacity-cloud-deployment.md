# 后台 Outbox 容量管理：云端部署验证

日期：2026-10-18（北京时间）。用户确认采用后台容量管理，并授权服务器部署验证及创建 PR。服务器已启用 `background`、`GW_OUTBOX_CAPACITY_WAIT=true`，计量协议为 3、容量协议为 1。机器可读证据见 [部署结果](outbox-background-capacity-cloud-deployment.json)；本地性能与故障覆盖见 [实验报告](outbox-background-capacity-report.md)。

## 版本与切换

- 固定来源提交：`1f814e4944b3e2f54a169f8c660b70d947c1ade3`。11 个 Linux/amd64 运行和维护程序统一构建，CGO 关闭。
- 镜像：`llm-gateway:outbox-background-20261018-1f814e4`。
- 镜像 ID：`sha256:1d381a55c3c44d3c33f8f4bd411a5e7f007e2a73e59c47523ca75eb8d705d35e`。
- 发布包 SHA-256：`b85185d699ba172d38a41cbf8f6686477618cf48ba5da35418ece9996cc345e0`。

先在生产快照的随机克隆库验证：已有历史数据、缺少 `writers-stopped` 确认时拒绝迁移；显式停写确认后 background → locked → background 切换通过，账务、业务配置、Kafka 源身份及水位均保持不变。演练约 13.09 秒，克隆库及其授权已清理。

首次演练发现 Windows 归档未保留 Linux 可执行位，迁移程序启动被拒绝；此时正式服务未切换。对已核验 SHA-256 的程序显式设置 0755 后重建镜像，重新演练通过。未将第一次失败记成通过。

正式切换依次关闭私有入口、停止 gateway/admin、让 worker 排空、停止 worker 并等待心跳退出；执行旧模式完整审计、迁移和新模式完整审计，再统一启动 gateway/admin/worker。开放前确认角色就绪、采样快照新鲜、计量许可有效，历史账务、配置和 Kafka 源身份/水位不变。

首次开放前验证用时 16.415 秒；真实调用后另一次停写审计及重启复核用时 14.998 秒，两者均未包含随后 edge 容器启动时间，不能把首次时间当作本轮总停机时间。第二次复核已恢复入口，最终 HTTPS、登录和计量状态再次通过。

只替换业务程序并在现有 `deploy/lite/compose.lifecycle.yaml` 中设置统一镜像与容量模式。MySQL、Redis、Kafka、etcd、Prometheus、Grafana 容器 ID 未改变；前端哈希及业务配置 v31 保持不变，预算、容量与并发上限未调整。入口仍为既有 SSH 隧道下的 `https://localhost:9443`。

## 功能及账务核验

| 真实模型调用 | HTTP | 完整响应耗时 | 厂商报告 Token | 可见回复 |
| --- | ---: | ---: | ---: | --- |
| JSON | 200 | 3169.08 ms | 47 | OK |
| SSE | 200 | 2921.19 ms | 51 | OK，收到 DONE |

两笔主 attempt 均为完成 revision 2、quality=reported、billing_state=settled；合计 98 Token、364 micro（按配置价格计量，不代表厂商发票）。另有两条请求汇总，不计为两次额外模型调用。

| SQL 全量统计 | 发布前 | 调用并追平后 |
| --- | ---: | ---: |
| attempt 表总行数（包含请求汇总） | 1410 | 1414 |
| Token | 955596 | 955694 |
| 费用 micro | 2659793 | 2660157 |

新增的两笔主 attempt 逐笔核对通过。停写后的完整 SQL/Redis 审计检查 2 个配额组、678 个 Redis attempt，pending=0；容量占用、未交付 Outbox 和本地未覆盖预留均为 0。审计检查的 Redis attempt 数不等于 SQL 总行数。

同时验证私有 CA 与主机名、管理员登录/退出、未授权 `/v1/models` 返回 401、授权模型列表正常。三角色优雅停止后重新启动，账务、配置、源身份与审计时水位保持一致，快照恢复新鲜、许可有效。没有 OOM 或自动重启。

重启后瞬时内存约为 gateway 7.5 MiB、admin 5.8 MiB、worker 10.1 MiB；这是初始化后的观测值，不能当作负载峰值。

## 备份、回退及适用范围

停写 SQL 一致性备份、原环境和 Compose 保存在服务器权限受限目录 `/opt/llm_gateway/releases/outbox-capacity-20261018-1f814e4/backup/`，并复制到本机仅当前用户可访问的 `data/cloud-release/outbox-capacity-20261018-1f814e4/backup/`。SQL SHA-256 为 `cd1d2f1a7606d9ec6b3349e6cfca136a1d8cd6ecbfb047ba5d2c5c3c64473044`，异机副本已核验。备份、凭据和实际业务载荷不提交 Git。

回退必须重新关闭入口、排空并停止所有写入者，用本次新镜像的维护程序以 `locked` 和 `writers-stopped` 执行迁移，恢复协议 2 后才允许启动原程序。不能在协议 3 上直接启动原版本；出现新请求后保留现有账务，禁止用发布前 SQL 覆盖新费用。克隆库已演练协议回退，正式环境本次无需回退。

后台容量采用软上限，多实例允许短暂超额；仍保留 Prepare/Redis 预占/Arm、持久完成事件及消费结算。源码默认模式继续为 locked，已有部署必须显式停写迁移，避免升级程序时隐式改变容量协议。

本次云端为两次真实模型功能冒烟、历史数据迁移及完整账务审计，没有做云端持续压测或最大吞吐验证。满额等待、取消、过期快照、worker 故障及多实例超额的覆盖来自已通过的本地隔离集成测试；线上未注入虚假账单或关闭计量保护来制造满额。
