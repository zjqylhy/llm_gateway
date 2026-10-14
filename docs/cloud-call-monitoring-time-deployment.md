# 调用监控与时间展示云端发布

2026-10-13（北京时间），用户确认代码及测试结果后继续发布；随后明确批准时间列前置补丁。正式地址仍为 [https://localhost:9443](https://localhost:9443)。

## 发布范围与版本

先以生产管理端来源 `31c1a00ac85cc0d34f875704b83338806564099d` 的 Git 归档为基线，叠加修复提交 `048ac565a80d8156929aff49aae0a8d5dc7f0716` 的七个代码及测试文件。逐项确认这些文件在修复前与生产基线一致，隔离源码 Go test/vet、前端 18 项测试及生产构建通过，离线构建 Linux/amd64、CGO 关闭的 gateway/admin。分支继承的可靠性性能改动未进入发布包。

核心镜像 `llm-gateway:call-monitoring-20261013-048ac56`，ID `sha256:fbeed59707e9281b1d60ea25a974863e3be5423e4631b20dad05bbff39b86a29`。包 SHA-256 为 `e32f1669d6003393782dc998cde4971fbdc301b83b798d581c14451c289e06bb`。发布前校验包及逐文件哈希，备份配置、环境和旧镜像信息；停止 gateway/admin 接单，确认 outbox/admission 均为 0 后仅重建这两个服务。

浏览器验收发现窄概览列表需横向滚动才能看全时间。提交 `14a6e11122d6616a91398f2bb5dd11c2704fbd50` 将时间列前置，前端 18 项测试、构建和格式检查通过。用户单独批准后，仅替换 admin 的前端镜像，不重建 gateway/worker。最终 admin 为 `llm-gateway:call-monitoring-ui-20261013-14a6e11`，ID `sha256:5e4d9941f91656bf6d619b44dd38bd94b9cb3d5bbb1fc8e73171819db24d3732`；补丁包 SHA-256 为 `25ef1852a1ad48eb213b9b666844c1db31ed745d5357133a3a9321ba9a554d6d`。

最终 gateway 使用核心镜像；worker 继续使用 `llm-gateway:application-governance-20261012-2cc0ae1`。正式 Compose 为 gateway/admin 分别固定对应镜像，后续整批升级须处理这两项覆盖。

## 验收

- 真实最近 1 小时和 24 小时监控查询可用，历史 `http_error / HTTP 200` 不再导致整批指标被丢弃。浏览器确认成功、历史失败及上限曲线并存，低 QPS 仍显示非零值。
- 正式调用审计和概览近期调用的首列均展示“调用时间（北京时间）”。最近记录显示 `2026-10-13 05:00:13`，与接口 UTC 时间对应，完整日期和时分秒无需横向滚动即可查看。
- 正式前端 HTML/JS/CSS 哈希与补丁包一致；本机 HTTPS 校验证书后返回 200，既有 SSH 隧道已恢复。
- 业务配置指纹与发布前一致，版本保持 v29；三个 Go 服务 readyz 通过，Prometheus 三个抓取目标均为 up，计量 healthy、许可有效，outbox/admission 均为 0，无 OOM。
- worker、Prometheus、MySQL、Redis、Kafka、etcd 和入口容器未重建。本次没有发起付费模型请求、数据库迁移或业务配置写入；没有推送远端。

脱敏发布记录与真实截图保存在本机 `data/cloud-release/call-monitoring-20261013-048ac56/`、`data/cloud-release/call-monitoring-ui-20261013-14a6e11/`，不提交实际业务截图或凭据。

## 备份与回滚

两次发布各自的备份目录为 `/opt/llm_gateway/releases/call-monitoring-20261013-048ac56/backup` 和 `/opt/llm_gateway/releases/call-monitoring-ui-20261013-14a6e11/backup`，环境副本权限 0600，目录 0700。发布脚本均有失败自动恢复路径，本次未触发，也未执行额外回滚演练。

仅撤回时间列前置时，先确认没有后续配置变更，再恢复 UI 备份的 `compose.previous.yaml`，仅重建 admin。撤回核心修复时，须停止 gateway/admin 接单并确认计量排空，再恢复核心备份 Compose 并重建 gateway/admin；保留 worker、监控和业务持久卷。
