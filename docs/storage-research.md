# 租户容量与历史数据保留调研

调研日期：2026-10-11。本文为待确认方案，不代表生产已实现。

## 当前项目边界

应用 app_id 是现有隔离单元，尚无独立组织 tenant_id。当前缓存 Redis 64MiB、配额 Redis 128MiB 是实例总容量，不是租户容量；QPS、Token 和费用额度不能限制存储字节。MySQL、Kafka、Milvus 和 Redis 历史幂等记录尚无完整租户存储额度、自动财务归档或容量联动停止接单。

## 容量隔离

Redis Open Source 的 maxmemory 是实例级设置，选择不同 DB 编号和 key 前缀不能得到独立内存限额。Redis Software/Cloud 的数据库级额度是另一种产品能力，不能直接当成本项目 redis:7.4 的功能。

来源：[Redis 淘汰与 maxmemory](https://redis.io/docs/latest/develop/reference/eviction/)、[Redis Software 数据库内存额度](https://redis.io/docs/latest/operate/rs/databases/memory-performance/memory-limit/)。

| 数据 | 共享资源池的应用层额度 | 达到额度后的处理 |
| --- | --- | --- |
| 响应缓存 | 序列化字节、项数、单项大小、TTL | 淘汰本应用缓存或跳过缓存；正常推理仍可执行 |
| Milvus 向量 | 应用向量项数、写入速率、TTL | 停止该应用新增缓存向量；不能只限制 Redis 响应 |
| 配额幂等记录 | 保留窗口内的 attempt 数及保守字节预算 | 调用厂商前拒绝新增 attempt；为已预占调用的结算、修正留余量，不淘汰计费依据 |
| MySQL 调用明细 | 行数、正文采样字节、保留期、持续写入速率 | 优先减少可选正文；必要时拒绝该应用新增付费调用，不能丢弃已发生费用的结算 |
| 运行资源 | 应用 QPS、并发、排队长度、存储写入速率 | 对该应用限流，避免仅限制空间仍耗尽公共 CPU/连接/磁盘 I/O |

建议首期先以 app_id 为单元实现逻辑额度，后续如一个组织有多个应用，再引入 tenant_id 和组织总额。字节计数必须原子更新，并处理过期、替换、淘汰及重复写入；Redis MEMORY USAGE 更适合作为校验和容量抽样，不能代替可靠的预占计数。数据结构开销、碎片和共享缓冲需另留全局空间。所有额度合计不能按数据字节恰好分满实例。

逻辑额度降低单应用挤占风险，但不能保证共享实例故障、磁盘耗尽或 CPU 阻塞不影响其他应用。要求资源强隔离的租户应使用独立 Redis/MySQL 等实例或资源池，并对节点、磁盘、CPU/I/O 和连接资源配额；仅拆 key、DB 或表不足以隔离物理资源。

## 同类系统公开能力

| 产品 | 官方文档确认的能力 | 范围与限制 |
| --- | --- | --- |
| LiteLLM | 可配置 spend logs 保留期和定时分批删除；清理有批次、时间与语句预算；支持 S3 等外部日志输出 | 保留清理文档明确为开源功能；S3 输出是日志管道，不等价于完整计费账本的校验归档/恢复 |
| Portkey | 日志有按套餐设置的保留期；企业版可创建日志导出任务，按时间与字段筛选，支持任务状态和失败重试 | 导出是企业能力；自托管企业需要 Data Service；当前文档单任务最多 50,000 条；导出不自动等价于 MySQL 账本搬迁 |
| Kong Gateway / AI Gateway | AI 日志记录 usage、费用等；HTTP Log 插件将请求响应日志发送到外部 HTTP 接收端 | 长期存储生命周期可由外部日志系统负责；所查文档未确认网关内建 MySQL→Hive 财务归档流程 |

来源：[LiteLLM 日志保留](https://docs.litellm.ai/docs/proxy/spend_logs_deletion)、[LiteLLM S3 日志输出](https://docs.litellm.ai/docs/proxy/logging#s3-buckets)、[Portkey 日志保留](https://portkey.ai/docs/product/observability/logs)、[Portkey 日志导出](https://portkey.ai/docs/product/observability/logs-export)、[Kong AI 日志](https://developer.konghq.com/ai-gateway/ai-audit-log-reference/)、[Kong HTTP Log](https://developer.konghq.com/plugins/http-log/)。版本、部署方式及套餐可能改变，应在接入时再次核对。

从以上资料推断：可配置保留期、受控清理和向外部存储输出，是同类系统已有的能力；未发现足以证明“直接接 Hive 保存一年”是所有网关必备功能的证据。运行日志可允许的丢失/重试策略不能直接用于财务账本。

## 本项目建议分期

1. 优先补齐存储增长统计、剩余容量和 Kafka 最旧未消费事件年龄的告警，以及数据库不可写/长期积压时的入口保护。阈值需按扩容耗时和数据增长率确定，不能等磁盘满了才开始清理。
2. 把保留期、正文与元数据的不同策略、未完成/争议记录的例外写入 spec；先测每个付费 attempt 在所有表与索引中的平均占用，再估算容量：每日 attempt 数 × 每 attempt 存储增量 × 在线保留天数，另计 binlog、备份和临时空间。
3. 如有一年历史查询、审计或容量压力，增加对象存储归档：导出不可变计量事件及版本、校验行数/金额/Token/校验和、生成 manifest 与状态，再分批删除在线明细。归档应可重试、可核验、可恢复，未完成调用与未解决修正不能按日期直接删除。
4. 优先输出可被离线工具读取的格式；有实际离线分析环境后再接 Hive/Trino。对象存储生命周期负责保留一年，Hive 负责查询；不为首期独立部署完整 Hadoop/Hive 集群。

删除前必须先设计月度结算检查点、幂等重放水位及历史修正路径。当前 quota-rebuild 读取 MySQL 明细；简单删掉历史行会改变恢复依据。Kafka 保留窗口、在线/归档事件和 Redis 幂等记录必须协同管理；财务汇总或去重索引不能随日志保留期一起删除。

建议把“容量保护和生命周期设计”列为首期需要处理的运行能力；将“校验归档与恢复”作为有长期数据需求时启用的扩展，将“Hive 集成”作为可选分析适配。是否在线 90 天/离线 1 年仍需业务确认，本文不直接启用删除。
