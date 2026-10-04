# 两项独立采集启用与东方财富批量改动

用户 10-04 已授权：先安装启用停牌存档和 BaoStock 日采，不等东方财富；cool8/基本面是手动脚本，确认 owner guard 和 40000 硬限即可，20 日采集窗口期间不手动运行。
原冻结协议字节和八项门槛不变。执行依赖补充单为 `FORMAL_RISK_COLLECTION_AMENDMENT_20261004.json`，不发正式风险许可。

## 两项独立采集

- 停牌：20 个声明交易日北京时间 16:10，保存 `stock_tfp_em` 原始 HTTP 响应、原表、真实取数时间、哈希和逐条筛选理由；不确定的整日停牌语义仍未知，不假装已经有合格正例。
- BaoStock：同日北京时间 18:00，最晚 20:00 实际开始；先在共享 guard 下取完当天 `query_stock_basic()`，用股票类型、上市状态、上市/退市日期确定原支持范围内的采集对象，然后查询日历和逐股不复权日线。**不再等东方财富 universe 文件**。
- 只按当日基本资料确定采集范围，不按行情是否成功返回缩小范围；ST、停牌、新上市股票仍在采集范围。代码、上市资料冲突或日线空/旧日期均 `partial`，不猜安全状态、不重试。
- 10-04 已有真实全表 8991 行可解析出 4605 只支持范围上市股票；只证明采集范围计算可运行，不作为 10-08 股票名单。每日预计 `N+4+P` 条消息，当前规模约 `4609+P`；单次 7000、合计业务软停 35000、硬限 40000，原共享锁等待最多 7200 秒。
- 该 BaoStock 采集名单**不是独立股票池验收证据**。原冻结独立全市场名单、逐字段比对和覆盖门槛仍须补齐，缺失时最终验收 `incomplete`。不把自采名单冒充独立参考。
- 两个 timer 仅明确列出原 20 个日期，首次分别为 10-08 16:10 和 18:00，最后为 11-04；不补跑错过的日期。BaoStock service 14900 秒总超时、停牌 service 240 秒；共享 owner 目录纳入 BaoStock `ReadWritePaths`。

## 手动历史入口核对

`Host fact`：`cool8_capture_20260930/capture_cool8.py` 第 1001–1002 行显式把 owner 规则、账本、锁传给 `BaoStockGuard`；`ashare_stable_v3_20260929/fetch_fundamentals_v3.py` 从父目录导入 owner 模块，第 238 行使用同一 guard 默认账本/锁。owner 规则硬限 40000，guard 每次发送前执行合计硬限检查。
这些文件不修改、不重跑、不更新其历史规则/哈希；用户承诺采集窗口期间不手动运行。现有 timer 清单没有 cool8/基本面对应定时任务。它们不再作为新任务启用阻塞。

## 现场范围、备份和停止

以下是 `Safety rule`，不是 AstrBot 官方运维命令；本轮不替换插件、不重载插件、不重启容器。

- 根目录 `/home/pi/apps/stock-watch-risk-archive-20261008`：安装 `archive_risk_daily.py`、共用的 `archive_eastmoney_close.py`、原协议、执行补充单及绑定两个哈希的批准记录。
- 安装两个 service/timer 到 `/etc/systemd/system`，不安装启用东方财富 service/timer。共用依赖文件包含未启用的批量模式，导入不会触发行情请求。
- 安装前确认新根目录和四个 unit 不存在、旧服务 inactive、永久 helper 哈希正确；备份缺失状态和 unit 状态，staging 哈希/导入/日历和 unit 校验通过后原子安装。根目录曾存在或 unit 状态不一致就停下，不覆盖不明文件。
- 只 `systemctl daemon-reload`，再 `systemctl enable --now` **这两个 timer**；不启动 service 取假期数据。核对两个 timer active/enabled、首次目标日期、STOP/锁和容器启动时间。
- 原始存档：`suspensions/YYYY-MM-DD/run-*`、`baostock/YYYY-MM-DD/run-*`；文件日志分别为 `logs/suspensions-YYYY-MM-DD.jsonl`、`logs/baostock-YYYY-MM-DD.jsonl`。systemd 启停信息在各自 journal。20 日还没开始，不能声称已有这些日期的结果。
- 回退先执行停止方案、核对进程退出，再按安装清单删除本轮新增 unit 和精确安装文件；保留任何已产生的行情、日志、共享账本/锁及旧任务，不递归删除根目录，不恢复旧计数。

停止两项新采集（启用后）：

```sh
touch /home/pi/apps/stock-watch-risk-archive-20261008/STOP
sudo systemctl disable --now stock-watch-suspension-archive.timer stock-watch-baostock-risk-archive.timer
sudo systemctl stop stock-watch-suspension-archive.service stock-watch-baostock-risk-archive.service
systemctl is-active stock-watch-suspension-archive.timer stock-watch-baostock-risk-archive.timer stock-watch-suspension-archive.service stock-watch-baostock-risk-archive.service
```

不删 STOP 自行重跑；恢复要再确认。暂停不重置消息账本，也不删活锁。

## 两次东方财富试采：各一次，不重试

北京时间 10-04 22:59:14–22:59:15，SDK 1.18.97：

1. `stock_bid_ask_em(symbol="000001")`：仅 1 次 HTTP 调用，`ConnectionError`，没有收到原始 HTTP 响应，不能伪造一份响应文件。
2. `clist/get` 请求 `f350/f351`：仅 1 次 HTTP 调用，HTTP 200；原始 SHA256 为 `5f9b6568e37edab3b7fc877fbe098abe9d27e06a12e2601e8379c1fe54736736`。收到了：

| 股票 | 收盘 f2 | 前收盘 f18 | 上限候选 f350 | 下限候选 f351 |
|---|---:|---:|---:|---:|
| 平安银行 000001 | 1157 | 1135 | 1249 | 1022 |
| 万科A 000002 | 426 | 408 | 449 | 367 |
| PT金田A 000003 | - | 271 | - | - |
| 国华退 000004 | - | 51 | - | - |
| ST星源 000005 | - | 83 | - | - |

`fltt=1` 的这些数值按分解释：平安银行上下限候选 12.49/10.22，万科 4.49/3.67，与 10% 四舍五入边界相符。已有 09-30 BaoStock 原始日线的平安银行收盘 11.57、前收盘 11.35 和成交额与本页相同。**规则数值相符与主价格相符，不等于同股 stock/get 语义交叉核对通过**；ST/创业板/特殊样本、全量分页和正式日期覆盖仍未验收。
本页还包含退市/无报价证券，返回 total=12394；不据一页宣称已拿到完整当日上市股票池，也不据无报价认定停牌。

## 已准备的批量代码改动（等待用户看完确认，不安装东方财富 timer）

- 批量字段换成 `f350/f351`；不使用该端点的 `f51/f52`，不以 v1 算出来的边界冒充供应商上下限。
- 显式 `--batch` 模式，每页原始响应只存一次，每只股票的候选结果关联同一个原始字节哈希和真实收到时间；无需逐股 `stock/get`，只按声明预算分页。
- 继续核对各页 total、页大小、代码/市场、重码、当日收盘后时间；不完整/非法名单保留 partial。完整原始响应仍不等于验收。
- `f2/f18/f350/f351` 必须为有效正整数分报价。缺失上下限仍未知；有效同日名称与正成交只提供对应字段的候选参考。候选标记为 `probe_candidate_not_formal_acceptance`，所有许可仍关闭。
- 本次只准备/测试批量模式，不安装、启用或运行全市场东方财富任务。等用户审核本改动后再推进其安装；不影响两个已授权独立任务。
