# 部署与更新约定

## 代码先行原则

从本次提交开始，所有修改按以下顺序执行：

1. 在本仓库创建、检查并提交源码变更；
2. 推送到 `珍爱网` 分支；
3. 以该 Git 提交为唯一部署来源更新服务器；
4. 完成服务、认证、数据接口和页面验证后，记录部署结果。

服务器上的 `/root/zhenai` 不是后续功能变更的源头。生产服务器仅保存运行状态、环境变量与 SQLite 数据库。

## 生产运行文件

运行目录严格为三个持久文件：

```text
/root/zhenai/app.py
/root/zhenai/.env
/root/zhenai/zhenai.db
```

`.env` 只在服务器配置，至少包含采集会话、Telegram 配置及 `DASHBOARD_ACCESS_TOKEN`。它绝不能进入 Git。`zhenai.db` 保存会员数据、断点、自动重启倒计时、去重记录及背景图片资源；它同样不得进入 Git。

## 部署注意事项

部署 `app.py` 后，应通过应用初始化确保 SQLite 中存在 `site_assets` 表，并将仓库中的两张 JPEG 图片写入表内：

| 仓库图片 | SQLite 键 |
|---|---|
| `cover_background.jpg` | `cover_background` |
| `dashboard_background.jpg` | `dashboard_background` |

服务由 `zhenai-dashboard.service` 管理。部署后必须确认服务处于 `active`、匿名访问仅显示口令封面、登录会话可进入看板，以及生产目录仍为三文件。

## 自动循环

完整城市遍历正常结束后，应用写入 30 分钟重启倒计时。该倒计时保存在数据库中，因此服务重启后会恢复。手动暂停会取消倒计时，避免用户主动停止后重新拉起任务。
