# 旧 27 只采集任务：待现场替换的具体改动

这份改动已在本地准备，但**还没有替换树莓派上的脚本、没有修改服务、没有重启容器**。
用户已确认本范围，并补充实际发送后记录 attempt、永久适配器路径和新服务写权限要求；现场替换仍须完整测试/发布检查通过并合入 main。

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

## 备份与回退（拟定，未执行）

安装前只读核对旧脚本哈希、服务是否运行、原 drop-in 是否存在。若服务正在工作，先等它正常结束，不抢锁覆盖。
备份旧脚本、原 unit/drop-in（若有）到本次独占备份目录，记录权限和 SHA256；新适配器/脚本先放 staging、做导入及哈希检查，再原子替换。
回退只恢复旧脚本和这一个 drop-in、重读 unit；**不回退账本的已用计数**，不重启容器。
所有安装文件必须能对应 `main` 上的提交，安装前展示该提交与完整检查结果；研究采集准备不等于正式名单验收。

## 已做与没做

已有离线测试涵盖排队时不发消息、等待超时保留锁、拿锁后重新看预算/最晚启动时间、预留登出，以及同批次失败不重试。
真实锁竞争、官网说明、联网字段语义与系统服务安装尚待现场验收；不能用合成测试代替这些检查。
