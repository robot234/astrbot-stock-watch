# 库容量与 Web 查询预算实测（审计 O07 / O08，2026-10-07）

只读实测，没有改任何数据、索引或保留策略。脚本 `.local_records/session_20261007/ops_capacity_probe.py`（在树莓派上以只读方式打开已发布的 Web 快照库，不碰插件的在线库），原始输出 `ops_capacity_probe_result.json`、`ops_latency_result.json`。

## O07：库里是什么在占空间

快照库 `stock_watch.sqlite3` 1.47 GB（359,912 页 × 4 KB），其中空闲页 39,397 页（约 154 MB，10.9%）。`dbstat` 统计（54.7 秒）的前几名：

| 对象 | 大小 | 行数 | 说明 |
|---|---|---|---|
| `raw_page_metadata` | **924.5 MB（63%）** | 720 | 每个批次·交易日一行，存 Tushare 原始分页留证；平均约 1.3 MB / 行 |
| `partition_bars` | 125.8 MB | 772,104 | 140 个交易日分区的日线 |
| `partition_bars` 两个主键 / 唯一索引 | 98.3 MB | — | |
| `idx_partition_bars_code` | 21.1 MB | — | 个股页按代码取 K 线用 |
| `daily_quotes` / `snapshot_requests` / `daily_bars` | 20.0 / 19.3 / 16.8 MB | 127,682 / — / 161,373 | 旧日线表只剩兼容用途 |
| `screen_runs` | 1.3 MB | 678 | 其中诊断 JSON 合计 0.97 MB；S03/S04 每次约多 30 KB |

批次：`batch_days` 720 行 = 6 个批次 × 120 个交易日，`partition_bars` 涉及 140 个分区。**大头是原始分页留证，不是行情本身**：6 个批次各留了一份 120 天的原始页，而 Web 和筛选只读当前 active 批次。

### 保留策略建议（未执行，生产清理要另行授权）

1. `raw_page_metadata` 只保留 active 批次和上一代批次的原始页（回滚和复核够用），更早批次的原始页先导出成压缩归档（按批次一个文件、记 SHA256）再删除。按现在 6 代估算可少约 600 MB。
2. 删除后在维护窗口做一次 `VACUUM`（需要约等于库大小的临时空间；树莓派剩余约 38 GB，够用），回收现有 154 MB 空闲页。
3. 旧 `daily_bars` / `daily_quotes` 在确认没有读取方之后再处理（Web 只在 active raw 缺失时回退读旧表）。
4. 研究冻结、原始证据导出和复盘需要的记录（screen_runs、研究池、推荐记录）不动。

做之前要先确认：插件里谁在读 `raw_page_metadata`（发布核验 / 重放）、需要保留几代；这属于改生产数据，按 O11 流程先备份、写回滚，再由你授权执行。

## O08：Web 接口耗时

在树莓派上经局域网地址访问线上 Web（release `caps-20261007T1825CST`，revision `8bddc27`），每个接口连续 3 次（毫秒）：

| 接口 | 耗时 | 响应 |
|---|---|---|
| health | 873 / 873 / 784 | 16 KB |
| overview | 516 / 480 / 480 | 5 KB |
| stocks/600857 | 407 / 408 / 422 | 63 KB |
| candidates | 393 / 388 / 406 | 30 KB |
| search?q=600 | 402 / 401 / 379 | 3 KB |
| performance?horizon=5 | 343 / 343 / 339 | 3 KB |
| settings / signals | 约 300 | 38 KB / 2 KB |
| research_signals / intraday / version / research_catalog | 3—15 | 读文件，不开库 |

- 全部在每次查询 3 秒的预算内，最慢的健康页约 0.85 秒。
- 只要开快照库，接口就有约 300 毫秒的底（`signals` 只返回 2 KB 也要 300 毫秒），说明耗时主要在每次请求都重算的元数据（来源汇总、快照检查等），不在某条缺索引的查询上。**现在不需要加索引**；以后如果要优化，先按快照 revision 缓存这部分元数据。
- 打开页面不会触发行情或模型请求：这些接口只读快照库和本地文件。
