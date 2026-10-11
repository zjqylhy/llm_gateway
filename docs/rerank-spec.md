# 文本重排需求与技术方案

需求和技术方案于 2026-10-11 经用户确认。首版仅处理文本重排，沿用现有请求治理和持久化计量链路。

## 对外接口

`POST /v1/rerank` 与 `POST /v1/reranks` 等价。请求使用统一扁平格式：

```json
{"model":"rerank","query":"什么是网关？","documents":["网关统一管理模型调用","今天下雨"],"top_n":1,"return_documents":true}
```

`model` 是授权路由别名；`query` 为非空字符串；`documents` 为非空字符串数组，最多 500 条。可选 `top_n` 必须为正整数，省略或超过文档数时返回全部；`return_documents` 默认为 false。可选字符串 `instruct` 需要模型声明 `rerank_instruct` 能力。拒绝未知参数、流式和多模态内容，不执行召回和生成，也不使用语义缓存。

结果统一为 `id`、`object:list`、`model`、`results`、`usage` 和 `usage_quality`（reported 或 estimated）。每条结果包含原始 `index`、`relevance_score` 以及可选 `document:{text:...}`。原文由网关依据输入索引补齐。结果按分数降序排列；非法索引、重复索引、非法分数、缺少结果及不完整结果判定为上游错误。

## 上游与配置

模型增加 `rerank_protocol`（必选 compatible 或 dashscope）、`max_documents`、`max_item_tokens`；能力为 `rerank`，可附加 `rerank_instruct`。输入总上限沿用 `context`，输出上限及输出/缓存输入单价必须为零。

- compatible：厂商 Base URL 配为 `https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-api/v1`，追加 `/reranks`；扁平请求及顶层 `results`，对应 qwen3-rerank。
- dashscope：Base URL 配为 `https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api/v1`，追加 `/services/rerank/text-rerank/text-rerank`；转换为 `input`、`parameters`，读取 `output.results`，覆盖 gte-rerank-v2 和其他支持该文本协议的模型。

地域/业务空间由厂商 URL 配置，密钥沿用加密存储。两种 Base URL 可注册为不同厂商条目；不根据模型名称猜测协议。模型限制须按所用地域和模型文档配置。qwen3-rerank 示例上限为 500 条、单条 4000 Token、总输入 120000 Token；gte-rerank-v2 总输入为 30000 Token。已有 Chat 和 Embedding 配置无需迁移。

控制台配置顺序：创建 qwen 厂商条目并填写相应 Base URL 和密钥；新建模型，能力填写 `rerank` 或 `rerank,rerank_instruct`，选择接口、文档数及单条上限，将上下文设为总输入上限并配置实际输入价格；创建 rerank 类型路由；在应用授权模型别名中加入该路由。输入价格沿用每百万 Token 微单位，不在代码中固定厂商价格。配置生效后使用应用 Key 调用，厂商 Key 不传给客户端。

## 治理与计量

复用鉴权、应用权限、QPS/RPM/TPM、预算预占、超时取消、至多三次候选尝试、熔断、独立 attempt 计量、审计和成本报表。仅对可重试故障降级；每次实际尝试分别计费。

重排预估输入为查询与指令 UTF-8 字节数乘文档数，加文档 UTF-8 字节数总和及每对文本的预留开销；没有厂商分词器时以保守估计做单条/总输入校验和预占，可能提前拒绝实际 Token 数较少的长文本，不宣称精确分词。用量优先采用厂商 `usage.total_tokens`，作为输入 Token，输出为零；支持同时存在 `prompt_tokens` 的格式。缺失或非法 usage 保留 estimated 标记和保守费用，不将其算成免费调用。结果校验失败时仍记录已经报告的有效用量。

控制台支持重排模型协议、输入限制及重排路由配置。灰度发布沿用现有仅 Chat 的范围。

## 验收

测试两种上游 URL/请求转换、参数校验、模型能力与输入限制、结果统一/原文补齐、非法及不完整结果、total-only 用量、缺失用量估算、权限隔离、预算拒绝、重试上限/独立计费、熔断及取消。运行 Go 全量回归、vet、build，控制台构建/测试和格式检查。真实百炼联调需要地域匹配的凭据和已开通模型，单独记录执行状态。

官方依据：[API 文档](https://help.aliyun.com/zh/model-studio/text-rerank-api)、[重排指南](https://help.aliyun.com/zh/model-studio/rerank)。代码和测试结果交付后由用户验收。
