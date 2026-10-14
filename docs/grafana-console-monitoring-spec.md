# Grafana 应用与错误码监控、控制台嵌入规格

状态：技术方案待人工确认。日期：2026-10-14。
基线：PR #9 已合并的 master，隔离分支 codex/grafana-console-monitoring。

## 目标与已知事实

- Grafana 增加可读的应用维度和网关业务错误码维度；应用 ID 作为稳定身份，名称用于展示。
- 应用概览用同源 Grafana iframe 替换当前 TrafficPanel 手绘趋势曲线；保留计量、成本、近期调用与审计功能。
- 验证后提交一个新的 PR 并部署原私有 ECS。用户已授权 PR 与部署，不合并新 PR。
- 现有 gateway_api_calls_total 已含 app/api/stream/outcome/http_status/error_code；gateway_calls_total 也有应用及错误码。重用这些请求计数，不另造一份计费/请求事实。
- HTTP 200 的 SSE 失败仍按业务错误码计入失败；上游重试失败后最终成功，不能把 attempt 失败算为请求失败。错误标签只用现有固定枚举，未知错误归为 http_error，不用原始错误文本、请求 ID 或密钥作标签。

## 展示与口径

- 保留原可靠性面板；新增按应用的请求 QPS、成功/失败比例、Token 速率及错误码分布/错误 QPS等分组视图。
- 增加 error_code 筛选，并说明仅作用于错误码相关面板；总请求成功率仍以全量请求作分母，不能因只选一种错误而得到错误的成功率。Token/延迟没有错误码标签，不伪装为该错误码的子集。
- 应用筛选、图例或表格显示应用名及稳定 ID。若需新增应用信息 gauge，每个已配置应用只保留当前名称的一个 series，重命名删除旧名称，缺少名称映射的历史样本保留 ID，不丢弃历史流量。
- 无法归属的认证失败显示为未知应用；不将未知请求归给任何真实应用。
- 控制台应用选择、时间范围和自动刷新同步为 iframe 参数；不把 iframe 重载当作每 10 秒刷新方式，由 Grafana 刷新查询。iframe 自适应宽度、有可用的高度和同源打开入口。
- 监控数据不等同预算结算总账，reported/estimated 与请求/attempt 口径保持原约束。

## 推荐的权限范围（需要确认）

- 推荐本次完整 Grafana 嵌入只向 admin 开放。developer/cost 不通过 iframe获得全局 Prometheus 访问；继续使用现有授权范围的计量、成本与审计界面。删除旧手绘曲线后，developer 不再显示旧趋势图。
- 若要求 developer 也使用 Grafana，需要另加服务端应用范围约束与受限数据源/查询接口；不能依靠 var-app 或隐藏筛选器。此需求若确认则先修订方案，未获确认不实施权限扩展。
- 依据：Grafana OSS Viewer 可以直接查询组织内数据源；仪表盘权限与变量不能实现按 app 的数据隔离。

## 嵌入技术方案

- 管理服务提供 /admin/grafana/ 同源反向代理，复用 gw_session 会话；每次资源/API请求都校验有效会话与 admin 角色。退出、过期和账号撤销后立即拒绝新请求。
- iframe 不直接连接 localhost:13000；浏览器只访问控制台当前 HTTPS 域名。现有会话 Cookie 的 /admin 路径适用于该前缀。
- 服务端使用专用 Grafana Viewer 账户并注入 Basic Authorization，随机凭据仅保存在权限受限运行配置中。不是 Grafana 管理员账户，也不传给浏览器；真实 Grafana 13.2.3 验证 Viewer 对 HTML、资源及查询的支持后再交付。
- 代理剥离客户端 Authorization、Grafana Cookie 与可伪造身份头，过滤上游 Set-Cookie/不允许的外部跳转。不能以浏览器已有 Grafana 管理员会话绕过只读 Viewer。
- 只开放实际渲染所需的只读路径；POST 仅允许数据查询，校验同源 Origin，限制请求大小与并发。禁止保存/删除仪表盘、改数据源、账户管理、登录、注释写入等操作。Grafana POST 查询不依赖浏览器未知的网关 CSRF Token，独立按只读查询及同源策略处理；其它 /admin API 的 CSRF 校验不变。
- Grafana root_url 设置为控制台 URL + /admin/grafana/，启用子路径和 allow_embedding；仅该代理响应使用 SAMEORIGIN / frame-ancestors self，其它控制台端点维持原防嵌入策略。禁用匿名/注册，私有绑定和现有资源上限不扩大。
- Grafana Live 非必需，不让长连接占用管理接口槽位。代理使用独立有界并发、超时与连接池；静态资源可以在授权检查后使用合适的缓存和传输 deadline，查询/HTML不缓存。避免慢客户端或资源加载耗尽业务管理并发。
- 未配置或 Grafana 不可用时显示明确监控不可用及重试入口，不影响预算、请求网关及其它管理功能。

## 清理与不变量

- 删除被替换的 TrafficPanel、仅用于手绘图的前端辅助逻辑和测试。审查旧 /admin/app-traffic 与 TrafficPrometheus 的调用方；确认无运行依赖后一起退休，更新当前操作说明，历史验收材料标注版本阶段而不抹掉证据。
- 不迁移业务表，不改 Lua、预算预占、Prepare/Arm、未知费用 held、补偿、Outbox/GC、版本/hash 和 Kafka 检查点规则。
- 本次无持续负载需求，不重跑长负载。使用短时可控请求与既有可靠性回归；若需要持续样本仍不得超过用户已要求的 10 分钟。

## 验收与发布

- Go 测试验证请求/错误码计数、应用重命名与未知应用，以及代理会话、角色、伪造头、只读方法、路径编码/跳转、同源查询、上游不可用与并发边界。必须验证匿名和 developer 不能伪造 var-app 或路径查询其它应用。
- 前端构建/测试与格式检查；iframe 参数、筛选、刷新和失败状态验证；确认旧曲线与轮询请求已移除。
- 使用真实 Grafana 13.2.3、Prometheus 验证专用 Viewer 渲染、子路径静态资源和数据查询、应用/错误码筛选及所有面板 PromQL；promtool 告警回归继续通过。
- 代码和测试结果再交人工确认，然后更新新 PR、部署。部署前备份修改配置和 Grafana 持久数据；按旧 worker 排空/停止写入顺序更换业务镜像，无业务 DDL，不清理依赖数据卷。
- 开放前验证财务状态和业务配置保留；HTTPS 控制台登录后 iframe 加载，无二次 Grafana 登录、无暴露凭据；401/403 和带错误码的可控无付费请求进入正确图例。核查 worker 健康、Outbox/容量、Prometheus targets/rules、内存和 OOM。
- 上线报告分别记录可控验证与真实部署，不将 iframe 展示或几个请求声称为最大吞吐或长期容量结论。

## 官方依据

- 配置、子路径与嵌入：https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/
- Viewer 与数据源权限：https://grafana.com/docs/grafana/latest/administration/roles-and-permissions/
