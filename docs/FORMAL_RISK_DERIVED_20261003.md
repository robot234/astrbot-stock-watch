# 正式名单风险门修复：未许可研究阶段

基线：`de65bd8871a7efa55954fc72a405e22dfc55a9ad`，分支：
`codex/formal-risk-derived-20261003`。2026-10-03 核对 GitHub main 后创建。
本轮不改变选股策略，不修改策略检查单，不采用失败的 COOL8/HERMES40。
当前只完成推导研究模块和有限来源核对，**正式链尚未接入，许可仍为空**。

## 先只读核对的三个问题

1. **Host fact：原始风险字段。** 通过已有 SSH 身份访问 `my-pi`，确认
   容器 `astrbot`、Python 3.12.13、数据挂载 `/home/pi/astrbot/data` →
   `/AstrBot/data`。仅以 SQLite `mode=ro`、`query_only=on` 读取
   `/AstrBot/data/plugin_data/astrbot_stock_watch/stock_watch.sqlite3`。
   2026-09-01 已有 5546 条日线，四个风险字段全 NULL；2026-09-08 为
   5549 条全 NULL；2026-09-30 为 5561 条全 NULL，其中 5500 条的
   `risk_source=eastmoney:companion`，61 条来源为空，版本全部为空。
   09-08 和 09-30 的保存原始页只有 OHLC、pre_close、量额等字段，
   本来没有风险四字段。因此不能说“09-08 开始来源字段消失”，也不能
   用已写入的 NULL 还原 companion 屏蔽前的原始风险值。
2. **Host fact：27 只来源。** 系统级
   `stock-watch-research-risk.timer/service` 调用
   `/home/pi/apps/stock-watch-data-probe/capture_research_risk.py`。
   `_read_pool` 读取最新 `research_pool_runs` 的 `research_pool_picks`，
   不是全市场风险任务。09-23、09-24、09-28、09-29、09-30 的旁路文件
   都是 27 只；BaoStock 只提供停牌/ST，AKShare 池只记录正例，缺席为 unknown。
   这既不能覆盖全市场，也不能证明四字段安全。
3. **交易所原文：节后首日。** 上交所 2026-09-17 公告
   `上证公告〔2026〕22号` 明确 10-01 至 10-07 休市，
   **2026-10-08（星期四）照常开市**，不是 10-09。
   生产库只缓存到 10-02，不能用该缓存验证 10-08；运行时仍需当天日历门。
   来源：https://www.sse.com.cn/disclosure/announcement/general/c/c_20260915_10832273.shtml

## 看结果前冻结的门槛

机器协议：`docs/FORMAL_RISK_DERIVED_ACCEPTANCE_20261003.json`。
固定 09-02 至 09-30 的 20 个交易日；每日支持范围至少 2500 只；
每日完整四字段及逐字段比对覆盖至少 80%；逐字段一致率至少 99.9%；
每字段至少 10 个风险正例；错误安全 False 为 0；未解释分歧为 0。
任何必需来源、逐日证据、时间/价格/批次/特殊状态依据缺失，结论均为 incomplete。
阈值未随结果修改。不能把正例吻合率替代全市场负例接受率。

## 实现边界

`derived_daily_risk.py` 提供 `derived:tushare-daily` / `2026-10-v1`：

- Decimal ROUND_HALF_UP 取分；主板普通状态 10%、创业板 300/301 普通状态 20%。
- 停牌 S 记录为 True；完整当日 suspend_d 响应并且量额为正才可推 False。
  无行情、量为零、接口失败不自动判 False，也不凭缺行情猜停牌 True。
- ST 需要当日有效、公告不晚于当日且无重叠的完整 namechange 记录。
  当前名称不直接冒充历史非 ST 证据。
- 新股前五个交易日、上市日期/完整日历缺失、重新上市等特殊制度没有当日
  明确依据时，涨跌停保持 None。ST 已阻断正式筛选，v1 不凭“ST 全为 5%”
  填涨跌停；创业板 ST 等制度必须先核对适用版本。
- 按原始批次、日期、来源和真实取数时间绑定；输出输入哈希、风险来源/版本、
  原因和 `licensed=False`。当前没有 provider/缓存/正式筛选调用此模块。

特殊状态输入 `exchange:trading-regime` 是待验证的证据接口约定，**不是已经
有了一个可用来源**。不能伪造 ordinary=True 来凑覆盖；缺失就阻断。
Tushare 已有日线不代表已有 suspend_d/namechange 权限，本轮没有读取
配置、token 或凭据文件去尝试它们。

## 实测证据与尚未通过的门

本地私有证据目录：`.local_records/formal_risk_derived_20261003/`，不发布原始数据。

- 只读导出 latest frozen batch `batch-92fbaa09bf134cc693ffb606e15b6bd2`：
  20 日、111042 条原始行情，其中主板/创业板支持范围为 91829 条。
- AKShare 两类历史池共 40 次请求均返回：1030 个正例，37 个不在支持日线
  范围；其余 993 个正例的收盘价和普通制度取分公式全部吻合，分歧 0。
  这是**只验证正例的算术诊断**，不证明池外个股非涨跌停。
- 原 companion 使用 `ulist.np/get`。600000/000001/300750 实测返回的
  `f43` 为数十亿、`f86` 为 2194/2512/2003，且没有停牌/ST 字段；
  它们不符合代码假定的价格与 Unix 时间语义。
- 改换假设，只读试 `stock/get`：三只的 `f43/f51/f52/f59/f86`
  经标度换算后可与 09-30 日线价格吻合，但仍没有明确停牌/ST 字段，
  也没有 20 日历史 companion。这不是许可或生产修复。
- 全风险推导比对结论 **incomplete**：缺完整逐日 namechange、suspend_d、
  上市/交易日历及特殊状态证据；现有 27 只旁路不能替代全市场输入。
  推导四字段完整覆盖为 0，不计算虚假的全风险一致率，不申请放行。
- 21 项人工合成 smoke 通过，原未许可来源回归 `test_v0162` 9 项通过。
  合成测试不是来源验收；新增正式链测试留待用户确认后接入时补齐。
- 为备份未许可研究分支，现有 `tools/verification` 全部回归通过：
  425 passed，3 subtests passed；这不表示新增来源或生产已验收。

协议和比对工具保留 raw/protocol/deriver/evaluator SHA256。
比对输出独占创建，不能覆盖首次结果。代码验证后的再次运行使用另一个文件名。

## 复现与下一关

```powershell
python tools/compare_derived_daily_risk.py --input .local_records/formal_risk_derived_20261003/comparison_input.json --protocol docs/FORMAL_RISK_DERIVED_ACCEPTANCE_20261003.json --output .local_records/formal_risk_derived_20261003/comparison_result_new.json
```

退出码 5 表示 incomplete，1 表示 fail，0 只表示等待用户接受，不会自动许可。
输入格式可参照私有 `prepare_comparison.py`，每份引用必须有来源、交易日、
批次、代码、参考收盘、真实首次观测时间及原始证据哈希；AKShare 不能输入 False。

下一关是补齐上述逐日证据并解决 companion 端点/字段语义，然后按同一冻结协议
重新核对。用户明确确认通过后，才许可精确来源/版本/字段，接入 provider 和
持久化消费、补测试、完整验证、合回 main。发布前必须所有验证和 release_check
通过；部署前展示具体 diff/测试，再按 astrbot-operations 备份及插件级重载。
本轮未写生产文件/数据库、未重载插件、未重启容器、未生成正式名单。
