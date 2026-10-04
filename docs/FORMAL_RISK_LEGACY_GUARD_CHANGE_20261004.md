# 旧 27 只采集任务：已完成现场替换

10-04 已按用户认可范围完成备份与现场替换，文件对应 main 提交 `7baeb08122923d39e950969569295e378e9a268e`。**只做 daemon-reload，没有启动旧 service、没有重载插件、没有重启容器**。
用户补充的实际发送后记录 attempt、永久适配器路径和新服务写权限要求均已实现；全套测试和发布检查通过后合入 main 并推送。新研究采集仍未安装启用。

## 改什么、效果是什么

1. 旧脚本不再直接匿名登录；改为从同一个 owner guard 进入。全部登录、登出、成功/失败请求和额外分页，记入原有 `/home/pi/apps/stock-fund-fetch-20260929/baostock_request_ledger.json`，不另开一本账。
2. 锁忙时最多等 7200 秒，每 5 秒检查；等待不登录、不取数、不删活锁。拿到锁才重新看日期与当日剩余额度。
3. 合计硬限 40000，业务请求在 34999 前停，留 1 条登出；登出也不能越过 35000 或封禁停手规则。旧任务最多 `2 × 股票数 + 2` 条传输消息，含分页余量；不够就停，绝不绕过共享预算。
4. 任一股票返回空表、日期/代码错误、价格冲突或接口失败，即结束本次，不能继续冒充完整证据。首次底层 socket.send 返回正字节数后才写独占 attempt 记录，不保存报文；即使接收失败也禁止同批次自动重试。锁等待超时、额度不足或连接失败等实际零发送退出不留 attempt，后续小时可以再试。拿锁后发送前重查 attempt，避免排队者重复取数；成功后的已有发布检查保持原样。
5. 仍选原研究池股票，仍写原证据位置；**不改选股打分，不增加风险许可，不写数据库**。盘口/风险规则本身不因这次限额补丁被放宽。

## 确切现场文件

- 替换：`/home/pi/apps/stock-watch-data-probe/capture_research_risk.py`，对应仓库 `tools/verification/capture_research_risk.py`。
- 新增共享适配器：`/home/pi/apps/stock-fund-fetch-20260929/shared_baostock.py`，对应 `tools/shared_baostock.py`。永久保留，不随 20 日归档清理；旧任务不依赖归档目录。使用原 owner `baostock_guard.py`，不替换、不清空其账本。
- 新 BaoStock 服务的 `ReadWritePaths` 包含归档目录和 `/home/pi/apps/stock-fund-fetch-20260929`，覆盖账本原子写入及共享锁。
- 新增 unit drop-in：`/etc/systemd/system/stock-watch-research-risk.service.d/stock-watch-shared-guard.conf`，对应 `tools/operations/stock-watch-shared-guard.conf`，内容仅为 `TimeoutStartSec=7920`，避免原 10 分钟总超时把排队任务杀掉。
- 不改旧 timer 的 18:30–23:30 时刻，不关闭其他服务。安装时只 `systemctl daemon-reload` 重读 unit，不为了这项改动重启 AstrBot 或容器。

## 备份与回退（已执行）

安装前只读核对旧脚本哈希、服务是否运行、原 drop-in 是否存在。若服务正在工作，先等它正常结束，不抢锁覆盖。
备份旧脚本、原 unit/drop-in（若有）到本次独占备份目录，记录权限和 SHA256；新适配器/脚本先放 staging、做导入及哈希检查，再原子替换。
回退只恢复旧脚本和这一个 drop-in、重读 unit；**不回退账本的已用计数**，不重启容器。
所有安装文件必须能对应 `main` 上的提交，安装前展示该提交与完整检查结果；研究采集准备不等于正式名单验收。

## 已做与没做

离线测试涵盖排队时不发消息、等待超时保留锁、拿锁后重新看预算/最晚启动时间、预留登出，以及发送成功后接收失败不重试、零发送下小时可再试、排队者发送前重新查 attempt。
干净工作树全套：`python -m pytest tools/verification -q`，639 passed、3 subtests passed；`python tools/release_check.py` PASS，版本 0.13.3/schema 24。用户检查单哈希保留不变。

现场结果（北京时间 10-04 22:42）：

- 备份：`/home/pi/apps/stock-watch-data-probe/backups/shared-guard-20261004T144218Z`，保存旧脚本、缺失文件标记、权限/哈希、分步安装记录和回退脚本。回退副本字节核对及脚本编译通过，未在生产执行回退。
- 回退命令：`sudo /usr/bin/python3 /home/pi/apps/stock-watch-data-probe/backups/shared-guard-20261004T144218Z/rollback.py`；执行前要求旧 service inactive、当前文件仍为本次哈希。只恢复这三处，不回退账本、不重启容器。
- 新旧脚本/永久 helper 均完成哈希及 pi 用户导入核对。重读配置后 `TimeoutStartUSec=2h 12min`，旧 service inactive、旧 timer active。
- 安装前后账本哈希一致；容器仍 running，StartedAt `2026-09-30T11:10:55.379022936Z`、RestartCount 0。
- 安装后另做一次有界 BaoStock 真请求：09-30 日线一行，首次实际发送回调恰好一次；登录/查询/登出共 3 条消息，原账本 68 → 71/40000，没有封禁/空回复/重试。该试采不触发旧任务、不写插件数据库、不属于 20 日窗口验收。

仍未验证：两个真实进程竞争时的排队现场演练、旧任务下一次定时完整发布、官网 17:30 原文、东方财富同股字段语义和全市场分页完整性。排队与失败边界目前是离线验证，不能冒充现场验证。正式风险许可保持为空。
