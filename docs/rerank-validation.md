# 文本重排验证记录

2026-10-11，Windows 本地验证。需求与方案见 [rerank-spec.md](rerank-spec.md)。

## 通过

- `go test ./internal/gateway -run 'TestRerank|TestAdminRerank' -count=1`：兼容与 DashScope 两种协议、两个网关入口、授权、参数和输入限制、指令能力、结果归一化、原文补齐、不使用语义缓存。
- 计量回归：total-only 及 prompt/total 响应按纯输入费用入账；缺失、非法及矛盾用量保留估算；结果错误但有效用量已报告时仍入账；跨协议降级独立 attempt 与累计费用一致，预占释放；预算拒绝不调用上游。
- 故障回归：上游 400 不降级，429/503 可降级，最多三次尝试；熔断后跳过上游；客户端取消传递到上游。
- 管理回归：两种模型协议可保存，非法协议/限制/输出价格/混合能力拒绝；重排路由不能选择 Chat 模型；开发者无法修改模型。
- `go test ./... -count=1`、`go vet ./...`、`go build ./...`、gofmt 检查、`git diff --check`：通过。全量回归包括原有 Chat、SSE、Embedding、配额及计量测试；默认未开启基础设施集成开关。
- 控制台 `npm run build`、`npm test`（4 项）、Prettier src/tests 格式检查：通过。

Go 协议测试需要本机 HTTP 服务，沙箱内原有 Chat 测试先出现上游不可用，允许本机网络后通过。Vite 的 Windows realpath 在沙箱内触发 EPERM，允许构建所需文件访问后生产构建通过；这些失败未标为代码验证成功。

## 未通过与未执行

- `node scripts/validate-deploy.mjs`：当前工作区失败，报 `lite gateway: console/business concurrency mismatch`。开始本次开发前已有未提交的 `deploy/lite/compose.yaml` 将 `GW_MAX_ADMIN_REQUESTS` 从 8 改为 2，校验脚本要求 8。本次未修改或提交该已有改动。
- 真实百炼模型、容器基础设施与线上控制台联调未执行；需对应地域的业务空间、API Key 和已开通模型。本次使用确定性本机上游验证协议和计量，不证明模型实际服务可用性。

代码和本记录交付用户进行人工验收；本次提交不部署或推送远端。
