# Grafana 应用与错误码监控、控制台嵌入规格

状态：技术方案已由用户确认，本地实现与验证已完成，代码及测试结果待人工验收。日期：2026-10-14。
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

## 已确认的权限需求与修订方案

- 用户明确要求 developer 也能使用 Grafana，但只能访问自己拥有的应用。“自己”取当前控制台用户与配置中的 app.Owner，不取客户端参数。
- admin：完整可靠性仪表盘与应用仪表盘，可查全局应用和未知应用流量。
- developer：只开放专用应用仪表盘；应用列表、名称和查询数据全部限定为当前 Owner 的应用，不开放全局资源、容量、Worker、其它应用或未知应用流量。可以选择一个或多个自有应用；没有自有应用时显示空状态，不构造匹配所有应用的空正则。
- cost：沿用现有角色的全局应用计量/成本权限，开放应用仪表盘；不扩大到管理员的基础设施及全局可靠性面板。
- 仪表盘变量只控制展示。数据隔离由管理服务的认证代理执行，Grafana Viewer 权限、隐藏入口或 var-app 均不作为隔离依据。
- 依据：Grafana OSS Viewer 可以直接查询组织内数据源；仪表盘权限与变量不能实现按 app 的数据隔离。

## 嵌入技术方案

- 管理服务提供 /admin/grafana/ 同源反向代理，复用 gw_session 会话；每次资源/API 请求都校验有效会话，并按当前角色和应用归属决定可访问的仪表盘及数据。退出、过期、账号撤销或应用归属变化后，新请求立即按当前权限处理。
- iframe 不直接连接 localhost:13000；浏览器只访问控制台当前 HTTPS 域名。现有会话 Cookie 的 /admin 路径适用于该前缀。
- 服务端使用专用 Grafana Viewer 账户并注入 Basic Authorization，随机凭据仅保存在权限受限运行配置中。不是 Grafana 管理员账户，也不传给浏览器；真实 Grafana 13.2.3 验证 Viewer 对 HTML、资源及查询的支持后再交付。
- 代理剥离客户端 Authorization、Grafana Cookie 与可伪造身份头，过滤上游 Set-Cookie/不允许的外部跳转。不能以浏览器已有 Grafana 管理员会话绕过只读 Viewer。
- 只开放实际渲染所需的只读路径；POST 仅允许数据查询，校验同源 Origin，限制请求大小与并发。禁止保存/删除仪表盘、改数据源、账户管理、登录、注释写入等操作。Grafana POST 查询不依赖浏览器未知的网关 CSRF Token，独立按只读查询及同源策略处理；其它 /admin API 的 CSRF 校验不变。
- Grafana root_url 设置为控制台 URL + /admin/grafana/，启用子路径和 allow_embedding；仅该代理响应使用 SAMEORIGIN / frame-ancestors self，其它控制台端点维持原防嵌入策略。禁用匿名/注册，私有绑定和现有资源上限不扩大。
- Grafana Live 非必需，不让长连接占用管理接口槽位。代理使用独立有界并发、超时与连接池；静态资源可以在授权检查后使用合适的缓存和传输 deadline，查询/HTML不缓存。避免慢客户端或资源加载耗尽业务管理并发。
- 未配置或 Grafana 不可用时显示明确监控不可用及重试入口，不影响预算、请求网关及其它管理功能。

## 应用数据隔离的实现边界

- 不直接向浏览器开放 Prometheus。管理服务只代理渲染所需的 Grafana 路由；admin/developer/cost 共用专用只读 Viewer 身份，但每条代理请求仍按控制台会话鉴权。应用数据不在共享 HTML 或代理缓存中缓存，静态公共资源可缓存。
- developer/cost 只能读取指定应用仪表盘。仪表盘搜索和元数据限制在其可访问范围；不透传全局仪表盘、共享注释、告警、数据源管理或其它插件数据源。直接私有 Grafana 管理入口仍只通过 SSH 隧道访问。
- 数据源固定为 Prometheus UID prometheus。POST /api/ds/query 校验每条 query 的 datasource、表达式、查询数量、时间范围和请求大小；拒绝替换数据源、__expr__ 服务端表达式及不支持的查询类型。
- 使用官方 Prometheus PromQL parser 解析语法树，遍历所有向量选择器，为每一个增加 app 的强制匹配条件。允许范围从服务端配置计算，应用 ID 正则转义；保留原有 app 条件并与强制范围取交集。嵌套函数、子查询、二元表达式、join 两侧都必须处理，不能只改最外层，也不使用字符串替换解析 PromQL。解析失败或不支持的语法拒绝执行。
- developer/cost 限制可查询指标为应用仪表盘使用的固定业务指标集合，包括请求、错误、Token、延迟、应用限额及应用名称信息；不允许通过正则指标名、裸选择器或其它接口读取全局运维指标。聚合可以丢弃 app 标签，但聚合前的所有输入均须已隔离。
- 若实际 Grafana 版本通过 datasource resources/proxy 调用 Prometheus query/query_range/series/labels/label values，同样按表达式或每个 match[] 强制应用范围；无 match[] 时补限定业务指标与应用范围的选择器。应用列表优先由控制台当前配置返回，错误码只来自固定枚举；其它标签值接口也不能无条件枚举全局数据。
- 不开放未处理的 Prometheus metadata、targets、rules、federate、exemplars 或任意转发路径。Grafana 13 的 /apis 等新接口不能整段放行；只允许实测必需且已按角色处理的具体路由，其它一律拒绝。编码路径、重复参数、重定向不能绕过同一规则。
- 查询时间范围最多最近 24 小时，并限制表达式长度、范围选择器/offset/subquery、查询数量、步长和返回大小；Grafana 自动刷新最短 10 秒，独立并发和超时限制保护管理接口。全局面板只给 admin，不用给无 app 的指标补一个空 app 来伪装隔离。
- 若真实 Grafana 13.2.3 UI 必需的路由无法安全限定，先修正集成；不退化为仅靠 iframe 参数隔离，也不开放开发者的全局数据源权限。

## 清理与不变量

- 删除被替换的 TrafficPanel、仅用于手绘图的前端辅助逻辑和测试。审查旧 /admin/app-traffic 与 TrafficPrometheus 的调用方；确认无运行依赖后一起退休，更新当前操作说明，历史验收材料标注版本阶段而不抹掉证据。
- 不迁移业务表，不改 Lua、预算预占、Prepare/Arm、未知费用 held、补偿、Outbox/GC、版本/hash 和 Kafka 检查点规则。
- 本次无持续负载需求，不重跑长负载。使用短时可控请求与既有可靠性回归；若需要持续样本仍不得超过用户已要求的 10 分钟。

## 验收与发布

- Go 测试验证请求/错误码计数、应用重命名与未知应用，以及代理会话、角色、伪造头、只读方法、路径编码/跳转、同源查询、上游不可用与并发边界。必须验证匿名不能访问、developer 只能查询自有应用，覆盖伪造 var-app、嵌套 PromQL/多选择器、外部 app 正则、另一数据源、__expr__、series/labels、编码路径、全局仪表盘与 /apis 绕行，以及应用转移、空应用集合和会话注销。
- 前端构建/测试与格式检查；iframe 参数、筛选、刷新和失败状态验证；确认旧曲线与轮询请求已移除。
- 使用真实 Grafana 13.2.3、Prometheus 验证专用 Viewer 渲染、子路径静态资源和数据查询、应用/错误码筛选及所有面板 PromQL；用两个不同 Owner 的应用验证手工查询和界面筛选均不能跨应用取数，cost 与 admin 权限分别验证；promtool 告警回归继续通过。
- 代码和测试结果再交人工确认，然后更新新 PR、部署。部署前备份修改配置和 Grafana 持久数据；按旧 worker 排空/停止写入顺序更换业务镜像，无业务 DDL，不清理依赖数据卷。
- 开放前验证财务状态和业务配置保留；HTTPS 控制台登录后 iframe 加载，无二次 Grafana 登录、无暴露凭据；401/403 和带错误码的可控无付费请求进入正确图例。核查 worker 健康、Outbox/容量、Prometheus targets/rules、内存和 OOM。
- 上线报告分别记录可控验证与真实部署，不将 iframe 展示或几个请求声称为最大吞吐或长期容量结论。

## 官方依据

- 配置、子路径与嵌入：https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/
- Viewer 与数据源权限：https://grafana.com/docs/grafana/latest/administration/roles-and-permissions/

- PromQL 语法树：https://github.com/prometheus/prometheus/tree/main/promql/parser
- 标签强制约束的参考边界：https://github.com/prometheus-community/prom-label-proxy
