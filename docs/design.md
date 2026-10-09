# 网关技术方案

同一 Go 代码库提供 gateway、admin、worker 三个进程。开发模式在一个进程中组合运行，使用内存存储和确定性的本地 provider；生产模式要求 MySQL、Redis、Kafka、etcd、Milvus 配置，不允许自动回退到内存。

## 模块

domain 定义应用、凭据、模型、路由、调用事件和账本。store 使用版本化配置快照，MySQL 事务防止并发管理操作丢失。config 发布至 etcd 并记录实例版本。security 管理摘要、加密、会话和权限。governance 实施全局额度、预占和熔断。provider 负责协议转换及增量读取。cache 隔离向量和响应。eventbus 持久化事件；worker 使用唯一键和事务实现账本幂等。

## 计量与一致性

started 事件持久化后才能调用厂商，completed 事件在正常结束响应之前持久化。Kafka 至少一次投递，MySQL 以事件键去重。usage 修正使用版本递增的事件，账本按差额更新而非再次加总。未完成调用保留 started 记录并进入对账视图。

Redis 预占与结算使用原子操作和 attempt 幂等键；已完成事件先入账后结算预占。预占失效不能直接抹除潜在费用，需转为 pending。数据库与 Redis 不使用跨系统事务，依靠可重放事件及幂等结算恢复。

## 安全与边界

管理 Cookie 为 HttpOnly、SameSite=Strict，生产环境 Secure；写操作校验 CSRF 和 Origin。凭据用 AES-GCM，加密主密钥由部署 Secret 提供。日志不打印 token、请求正文或上游完整错误体。厂商地址在管理时校验 HTTPS，生产禁止内网及重定向，以降低 SSRF 风险。

缓存响应和向量属于应用数据。实时请求由应用请求头明确声明；敏感字段检测是额外保护，不能代替应用正确标记。缓存匹配时同时核对 route/model/parameter scope 和有效期。

## 验证边界

当前 Windows 无 Docker，先验证 Go 单测、并发测试、HTTP 集成链路、前端构建和本地管理交互。基础设施集成、真实 API 回归及万级压测需在具备对应环境后执行，报告保持明确状态。
