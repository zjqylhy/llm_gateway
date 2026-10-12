# 模型资源管理云端部署记录

2026-10-12（北京时间），用户确认本次代码及验证结果，随后明确授权云端升级并推送代码。已完成 admin 发布、正式 HTTPS/API 核对及浏览器验收。

## 发布版本与范围

- 来源：已验收提交 `f6f90aaddf4938a83b972e08e8385b0559c530fe` 的固定 Git 归档。
- 在独立归档中使用锁文件离线安装前端依赖并构建，Linux/amd64、CGO 关闭、trimpath 构建 admin。
- admin 镜像：`llm-gateway:model-resources-20261012-f6f90aa`。
- admin 镜像 ID：`sha256:0477cf5a6ccecc06c759559f86aab8fd38d7ed85831f215660294d91c7532d99`。
- 运行基础镜像：`llm-gateway:application-governance-20261012-2cc0ae1`，摘要为 `sha256:dedf132d7c667c269dc4b7f27d8b6e56c8389d5b1f470d5bbf63a113d7d47597`；本次仅替换其中的 admin 程序和前端成品。
- 发布包 SHA256：`0c26f049886c3abf6725e9414e1fc7d0901337bf194f4b7eca2b922fa7073c42`，大小 10,117,576 字节。云端验证包及逐文件哈希后离线封装镜像。
- 代码推送到 `zjqylhy/llm_gateway` 的 `codex/model-resource-management` 分支，未合入 master。

## 发布过程

核对既有镜像和就绪状态，备份 Compose、环境及三个服务的容器信息。校验有效 Compose 配置后，在服务器 Compose 的 `admin.image` 单独指定新镜像，仅执行 admin 的 `up -d --no-deps --pull never`。

admin 已重建并通过 readyz；gateway、worker 的容器 ID 和镜像未改变，中间件未重建。没有执行迁移、初始化、配额重建、业务配置修改或数据清理。新 admin 和前端使用 `f6f90aa`，gateway、worker 及继承的维护程序仍为此前版本。

本次 admin 镜像覆盖保存在服务器 `deploy/lite/compose.yaml`，既有 `manage.sh` 会继续使用该镜像。后续统一版本发布应同时更新或移除该覆盖，不能只修改 `GW_IMAGE`。

## 验证结果

- 正式 HTTPS 的证书、管理员登录及前端 HTML/JS/CSS 成品哈希全部通过。
- 发布前后脱敏配置快照指纹一致，版本均为 v12：3 个应用、1 个厂商、1 个已保存模型，业务配置保持原值。
- 模型引用接口返回已保存模型的完整引用数组，包括默认路由与应用路由，共 2 处。
- 首次验证脚本错误要求引用映射条目数等于已保存模型数。现有关闭的缓存配置还保留了已移除 embedding 资源的引用；修正为逐个验证已保存资源的引用完整性后通过。该保留配置在发布前后指纹中一致，本次未修改。
- 浏览器确认“已保存模型”列表、独立“新增模型”空白表单、编辑标题和编号锁定、引用位置列表、被引用模型禁用删除确认；所有表单均取消，未修改真实模型。
- admin、gateway、worker 均运行且 readyz 通过，无 OOM、异常重启；计量 healthy、许可有效、未交付事件为 0。
- 本次没有发起付费厂商调用，没有执行云端模型新增、修改或删除；这些写操作已通过本地回归验证。回滚脚本具备失败恢复路径，本次未执行回滚演练。

原始非敏感核对记录、发布包和真实页面截图保存在本机 `data/cloud-release/model-resources-20261012-f6f90aa/`，不提交凭据或实际业务截图到 Git。

## 访问与备份

正式访问仍为 [https://localhost:9443](https://localhost:9443)，通过 SSH 隧道。此次恢复了后台隧道，使用用户重命名后的本机私钥 `C:\Users\LHY95\Downloads\aliyun_sk.pem`，已实际登录验证。公网入口未调整。

备份目录为 `/opt/llm_gateway/releases/model-resources-20261012-f6f90aa/backup`，权限 0700；`env.previous` 权限 0600。`compose.previous.yaml` 和 `admin-image.previous` 保留发布前配置及 admin 镜像，另有 gateway、worker 的原容器信息。

如果需要回滚，先核对发布后是否还有其他 Compose 变更，再恢复备份中的 admin 镜像配置，仅重建 admin。gateway 和 worker 保持运行，不切换消费者版本。
