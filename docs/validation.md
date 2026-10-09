# 验证报告

验证环境：Windows，Go 1.27.1，Node.js 24.21.0，2026-10-10。本轮按用户要求先完成本地验证，没有配置真实厂商 API Key，没有部署云资源。

## 已执行

- `go test ./...`、`go vet ./...`：通过，27 个测试通过，1 个基础设施集成测试明确跳过；包括并发预占、加密 AAD、重复/乱序入账与修正、SSE 增量与中断、降级计量、缓存隔离和灰度自动回滚。
- miniredis Lua 回归：100 个并发预占中仅 10 个在额度内通过，结算重复及修正不重复入账，配额初始化标记丢失会拒绝。
- 管理端回归：RBAC、跨应用拒绝、CSRF、密码变更失效、最后一个管理员保护、密钥宽限/吊销、待发布时一次性 Key 保留、跨实例会话及退出。
- 协议回归：文本、SSE usage、function tools、JSON 输出、Embedding 批量、参数拒绝、客户端断开取消、计量持久化失败不产生正常终止标记。
- 采样隐私回归：正文密文保存，已知敏感字段脱敏，跨应用不能读取采样正文。
- 前端 `npm run build`：通过；React/TypeScript/Vite 静态资源已生成。
- 浏览器人工链路：本地登录、创建应用、保存发布、Playground 主模型故障后流式降级、成本页展示多次尝试；页面布局已查看。
- 部署 YAML 校验、24 项 Grafana dashboard JSON 校验：通过。Linux amd64 交叉编译：通过。

已运行覆盖率统计，Go 核心包覆盖率约 45%，SQL/Kafka/etcd/Milvus 及进程启动路径未由离线测试覆盖，不把覆盖率当作正确性证明。

## 待执行

1. 容器镜像构建与 MySQL/Redis/Kafka/etcd/Milvus 实际联调：本机没有 Docker。`TestInfrastructureIntegration` 已提供显式开关，默认跳过，防止误访问生产服务。
2. Linux `go test -race`：当前 Windows CGO=0，未安装 C 编译器；GitHub Actions 已配置，但尚未推送执行。
3. DeepSeek、Qwen、OpenAI 真实协议回归：待配置账号、准确模型名称、价格和 Qwen 地域。不能用 mock 工具或 JSON 响应证明厂商具备对应能力。
4. 可控上游 10,000 并发、延迟目标、长连接及故障恢复演练；k6 脚本已提供，尚未执行。当前不宣称 P50/P99、缓存 P95 或 99.95% 达标。
5. 云集群 dry-run/预发、托管服务 TLS/Auth、SSE 负载均衡、滚动升级、扩缩容、备份及 quota-rebuild 演练。

## 重现基础设施测试

在单独本机联调环境使用 `deploy/compose/integration-ports.yaml` 合并端口映射。设置 Go 进程环境：MySQL 127.0.0.1:13306、Redis 127.0.0.1:16379、Kafka 127.0.0.1:19092、etcd 127.0.0.1:12379、Milvus 127.0.0.1:19530，并配置对应密码、主密钥及初始管理员密码。

```powershell
$env:GW_RUN_INFRA_TESTS='1'
go test ./internal/gateway -run TestInfrastructureIntegration -count=1 -v
```

测试明确要求所有依赖为 loopback 地址；会迁移表、初始化账号、创建测试应用、写入事件及新建测试 collection。只使用专用测试数据卷，测试完按保留策略处理，不指向真实生产库。

压测示例（k6 需单独安装）：

```powershell
$env:GW_LOADTEST_ACK='controlled-upstream'
$env:GW_API_KEY='仅本次使用的测试应用 Key'
$env:GW_VUS='50'
k6 run loadtest/gateway.js
```

逐步增加到目标并发，分别执行 stream=true/false；记录机器规格、容器资源、上游延迟、请求长度、配额、缓存策略、各百分位及完整日志。k6 标准 HTTP 接口会缓冲 SSE，首事件延迟应从专门流式客户端或 Prometheus 指标测量。网关 overhead 指标为尝试时间减去上游读取/转发时间，包含持久化计量 ACK，与用户总等待时间分开解读。



