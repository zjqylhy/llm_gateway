# 厂商凭据管理云端部署记录

2026-10-12（北京时间），用户确认厂商修复代码及测试结果后，沿用已授权的云端升级流程完成 admin 发布。厂商管理现提供已保存列表和独立的新增、编辑入口。

## 发布版本与范围

- 来源：固定提交 `31c1a00ac85cc0d34f875704b83338806564099d`，分支 `codex/model-and-provider-resources`。
- 独立归档中按锁文件离线构建前端，Linux/amd64、CGO 关闭、trimpath 构建 admin。
- 新 admin 镜像：`llm-gateway:provider-resources-20261012-31c1a00`。
- 镜像 ID：`sha256:c50253997b8ee39b07876e794e17706892770ff93102403d0acf09305bca90d6`。
- 基础 admin 镜像：`llm-gateway:model-resources-20261012-f6f90aa`，ID 为 `sha256:0477cf5a6ccecc06c759559f86aab8fd38d7ed85831f215660294d91c7532d99`。
- 发布包 SHA256：`9a004980fb60490fb3fc9594bbf288e288ed6365aa52d4987a5d82e09e7c807b`，大小 10,118,904 字节；云端验证包及逐文件哈希后离线封装镜像。
- 仅替换 admin 程序及前端。gateway、worker 继续使用 `llm-gateway:application-governance-20261012-2cc0ae1`，容器 ID 与发布前一致。继承的其他程序仍为此前版本。

## 发布与验证

备份 Compose、环境及服务容器信息，校验有效 Compose 配置，再更新既有 `admin.image` 覆盖，仅执行 admin 的 `up -d --no-deps --pull never`。发布脚本采用部署锁并配置失败恢复路径，本次阶段记录为 `verified`。

- 正式 HTTPS 证书、独立管理员登录会话及前端 HTML/JS/CSS 哈希核对通过。
- 发布前后配置均为 v12，含 3 个应用、1 个厂商、1 个已保存模型；脱敏配置 SHA256 均为 `1d24f3bf4645a94cda63b10c51b0cccf24989077ae7962d3c626f5483809fb7d`。
- 使用故意不一致的路径与正文编号探测厂商 PUT 接口，返回 400 `provider_id_mismatch`；随后重新读取配置，指纹保持一致。该校验在配置事务之前拒绝请求。
- 浏览器确认“已保存厂商”列表、空白新增表单及必填密钥、编辑编号只读及空白密钥保留提示；全部取消，未提交真实凭据修改。
- admin、gateway、worker 均运行、就绪，无 OOM、重启计数均为 0；计量 healthy、许可有效、未交付事件为 0。
- 源码提交 `31c1a00` 的两项 GitHub `verify` 检查均成功；本地 Go 测试/vet/build、前端构建及 16 项测试、格式与部署静态检查已通过，详见[规格与验证](provider-resource-management-spec.md)。

本次未执行迁移、初始化、配额重建或数据清理，未发起付费厂商调用。真实厂商新增、编辑写操作已在隔离本地环境验证；本次没有在云端改动真实密钥，也未执行回滚演练。

原始非敏感验证、发布包及页面截图留在本机 `data/cloud-release/provider-resources-20261012-31c1a00/`，不提交实际业务截图及凭据。

## 访问与恢复

访问 [https://localhost:9443](https://localhost:9443)，沿用 SSH 隧道。用户重命名后的私钥 `C:\Users\LHY95\Downloads\aliyun_sk.pem` 已实际登录验证，公网入口未调整。

服务器备份目录为 `/opt/llm_gateway/releases/provider-resources-20261012-31c1a00/backup`，权限 0700；环境备份 `env.previous` 权限 0600。目录包含 `compose.previous.yaml`、`admin-image.previous` 及 gateway、worker 容器信息。

回滚前先核对是否有后续 Compose 变更，再恢复旧 admin 镜像配置并仅重建 admin。后续统一版本发布须同时更新或移除 `deploy/lite/compose.yaml` 中的 `admin.image` 覆盖，单改 `GW_IMAGE` 不会更新 admin。
