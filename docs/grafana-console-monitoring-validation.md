# Grafana 应用隔离与控制台嵌入验收

日期：2026-10-14。对应规格：[grafana-console-monitoring-spec.md](grafana-console-monitoring-spec.md)。
状态：本地实现和验证完成，代码与测试结果待人工验收；尚未创建新 PR、尚未变更云端。

## 交付内容

- 管理员完整可靠性仪表盘 36 个面板；developer/cost 应用仪表盘 13 个面板。新增应用请求 QPS、成功率、错误码/HTTP 状态、Token 视图；成功率分母包含所有请求，不被错误码筛选缩小。
- 控制台用同源 Grafana iframe 替换旧手绘曲线，无二次 Grafana 登录。保留计量、成本、近期调用与审计；删除 TrafficPanel、traffic.ts、app-traffic API/服务及其废弃测试与样式。
- developer 的权限由当前用户及 app.Owner 决定。每个 PromQL 输入选择器强制取自有应用交集，固定业务指标和 Prometheus 数据源；所有查询、标签枚举、仪表盘 DTO 和搜索均不能绕过范围。cost 沿用全局应用数据权限，admin 可查看全局可靠性指标。
- 凭据仅在服务端使用专用 Viewer；客户端身份、Cookie 与上游 Set-Cookie 不透传。只读路由白名单、同源 POST、编码路径检查和重定向阻断覆盖实际 Grafana 13 新 DTO 接口。
- 代理独立总并发 24、查询并发 4、有界排队 2 秒。查询窗口最多最近 24 小时，最多 16 条表达式/请求，最多 1500 点，最小步长 10 秒。静态资源流式传输，支持 gzip 和授权后的私有缓存；动态响应不缓存。
- 配置/重连可用性预检最长 2 秒。Grafana 停机时显示明确提示，其它管理读取继续可用；SQL、Redis 预算预占、Prepare/Arm、计量恢复、未知费用、Outbox/GC 均未改动，无业务 DDL。

## 验证证据

| 验证 | 结果 |
| --- | --- |
| Linux Go 1.27 完整 `go test -race -p 1 ./...` | 通过 |
| Linux `go vet ./...`、`go build ./...` | 通过 |
| 前端生产构建、12 项保留测试、Prettier | 通过 |
| 部署结构校验 | 32 个 YAML 文档通过 |
| Prometheus 3.5.0 promtool | 两份仪表盘共 67 条表达式、14 条告警及短租约回归通过 |
| Grafana 13.2.3 + Prometheus 真实集成 | 专用 Viewer HTML、资源、DTO 和所有面板查询通过 |
| 两个不同 Owner 的应用 | developer 成功请求合计 123；admin/cost 合计 1122，foreign app 查询与标签枚举不泄漏 |
| 权限及绕行回归 | 空应用、应用转移、注销、全局仪表盘、其它数据源、__expr__、全局指标、裸选择器、嵌套查询、元数据、任意 /apis、后端 ad hoc 重写字段均验证 |
| 代理资源/故障边界 | 管理并发独立、满槽拒绝、断连槽位释放、伪造 Cookie/身份头、gzip 与外部跳转阻断通过 |
| 浏览器故障注入 | 仅停止本地 Grafana，重连显示不可用；刷新仍可读应用、计量与成本区域 |

真实集成用显式环境变量启动 `TestGrafanaRealIntegration`，普通测试默认跳过外部依赖。测试账户和业务数据均为隔离环境的合成数据，不访问真实模型。浏览器阶段直接生成指标来验证展示，没有执行持续负载，也没有新增最大吞吐、长期容量或真实模型时延结论。图中同一 app 的历史信息缺失时回退为 ID，保留历史采样。

![开发者仅看自有应用的嵌入监控](images/grafana-developer-console.png)

![Grafana 停机与其它管理功能隔离](images/grafana-outage-isolated.png)

## 发布与运维边界

- 用户已授权新 PR 和远端部署。按仓库 AGENTS.md，代码及测试结果人工确认后再执行发布；不会合并新 PR。
- 远端只读核对：现有 gateway/admin/worker、Grafana、Prometheus 均在运行，重启次数为 0，没有 OOM；这不是本次部署完成的证据。
- 发布前备份配置、Compose 覆盖文件和 Grafana 数据，保存财务/配置快照。沿用原 worker 排空与停止写入顺序更换业务镜像；不清理依赖数据卷，不回退已重新开放入口后的财务数据。
- Grafana root_url 为控制台 HTTPS 地址加 `/admin/grafana/`。私有维护隧道入口相应使用该子路径；不开放公网 Grafana，也不启用匿名访问。
- 发布后需验证真实控制台、Viewer 角色、应用隔离、错误计数、财务状态、Worker/Outbox/容量及 Prometheus 抓取和告警，并追加实际发布版本、结果与回滚位置。
