# AstrBot A 股选股与自选监听

一个面向研究和模拟盘的 AstrBot 插件，提供收盘选股、盘中自选股监听和市场故事提醒。

## 功能

- 收盘后按本地技术规则筛选候选股
- 每个交易日抓取一次全市场日快照并保存到 SQLite
- Tushare 当天暂无数据时自动寻找最近有数据的交易日
- 盘中轮询自选股，触发信号后推送提醒
- 盘中行情按会话合并抓取，信号冷却状态持久化到 SQLite
- SQLite 使用可重复执行的 v7→v14 迁移；交易日、快照请求、候选运行、价位来源和事件均保留状态
- v0.13.3 的 Tushare 日线采用 append-only raw 批次、活动 generation 和读取 provenance；未完成或校验失败的批次不会切换活动数据，默认切换已校验批次为 active；跨进程快照使用带 fence 的持久租约，租约丢失时不会发布旧 owner 的批次
- 技术指标只使用明确标记为未复权的日线；只有通过交易日、收盘价偏差和价位顺序校验的收盘计划才会用于告警和回放
- 股票代码和名称共享本地索引；`/行情`、`/自选` 支持已同步名称，名称有歧义时会要求改用更完整名称或代码
- 连续确认状态持久化到 SQLite，插件重启后可继续累计
- 分层深筛最多处理 300 只技术对象和 100 只因子对象，持久日线优先，空结果受覆盖率保护
- Tushare 临时不可用时，`/全市场选股` 可生成一次性的东方财富降级预览；预览不写入候选池、收盘计划、回放或因子/行情缓存
- 候选池按当前行业分类归组展示；只有带当前分类的记录使用东方财富当前行业，旧候选快照行业会明确标注
- 已完成分钟线批量落库，重启恢复当日历史并按 7 天清理；不会恢复未完成 bar 或虚构成交量基线
- 轮询 RSS 或公告流，去重后推送市场故事
- 可选调用 OpenAI 兼容模型 API，总结新闻和事件
- 支持群聊、私聊推送白名单
- 不提供自动下单，默认仅研究/模拟盘

## 安装

将整个目录放入 AstrBot 插件目录，在 AstrBot WebUI 中启用插件并安装依赖。

运行环境：Python 3.11+、`httpx`。

## 命令

```text
/选股 [数量]
/全市场选股 [数量]
/股票同步 [数量]
/候选池 [数量]
/自选 添加 600000 [成本价]
/自选 删除 600000
/自选
/监听 开启
/监听 关闭
/监听 状态
/状态
/白名单 状态
/行情 600000
/行情 浦发银行
/故事 [关键词]
```

## 配置

### 股票与缓存

- `universe_codes`：选股代码，逗号分隔。留空时使用各会话的自选股。
- `daily_cache_enabled`：是否启用每日全市场快照缓存，默认 `true`。
- `daily_cache_keep_days`：日快照保留天数，默认 180 天。
- `calendar_ttl_seconds` / `calendar_unknown_ttl_seconds`：已确认/未知交易日状态的缓存 TTL，默认 86400/900 秒。东方财富旧日线只能证明最近已有交易日，不能证明请求日闭市。
- `daily_market_url`：自定义全市场快照接口。留空使用东方财富；自定义接口需要返回兼容格式的 JSON，并支持 `pn`、`pz`、`fs`、`fields` 分页参数。
- `tushare_url`：Tushare Pro 接口地址，通常留空即可，默认使用 `https://api.tushare.pro`。
- `tushare_token`：Tushare Pro Token。填写后每日快照优先调用 Tushare `daily` 接口；留空则不调用 Tushare，使用东方财富。
- `tushare_bulk_page_size` / `tushare_retry_attempts`：Tushare `daily` 分页大小和网络/限流重试次数，默认 6000/3。
- `tushare_bj_calendar_policy`：raw universe 的北交所市场策略。`require_bse` 要求独立 universe 同时包含 `SH`、`SZ`、`BJ`；`sse_fallback` 也保留三市场 universe，但交易日证据统一使用公共 SSE 日历；`exclude` 只允许 `SH`、`SZ` 并过滤 BJ 行。交易日历请求本身使用 SSE 作为公共会话证据，不会因为 `require_bse` 额外探测 BSE。
- `tushare_raw_dataset_key`：raw 日线数据集标识，默认 `tushare_daily`。
- `tushare_raw_session_count`：每个 raw 批次保留的目标交易日数量，默认 120；日历查询会按周末和节假日扩展自然日包络。`tushare_raw_lookback_days` 已弃用，仅作为旧配置的交易日数量别名，不再表示自然日范围。`tushare_raw_max_stale_trading_days`：网络失败时允许读取活动 raw 缓存的最大交易日年龄，默认 2 天。
- `tushare_raw_publish_enabled`：完成所有 raw 校验后是否切换为 active，默认 `true`。只有明确设为 `false` 时才发布为 `shadow`，该批次不会参与当前筛选、不会自动晋级，也不会立即触发东方财富降级。
- `tushare_snapshot_lease_ttl_seconds` / `tushare_snapshot_lease_wait_seconds` / `tushare_snapshot_lease_poll_seconds`：跨进程 Tushare 日快照的租约有效期、等待其他 owner 完成的最长时间和轮询间隔，默认 1800/30/0.25 秒。等待超时或租约丢失只读取现有 raw 缓存，不会由 waiter 重复请求或发布。
- `tushare_raw_chunk_size`：raw 历史指标的分块处理大小，默认 500 只。
- `tushare_raw_min_overall_coverage` / `tushare_raw_min_market_coverage` / `tushare_raw_min_market_median_ratio`：raw 批次全市场和单市场覆盖率门槛，默认 97%/95%/95%；每日数量低于窗口中位数门槛的批次会整体拒绝，不会切换 active generation。每个 raw/evaluation 批次都必须有独立 universe 证据，不能仅凭当前 raw 分区行数自证完整。留空 `tushare_raw_universe_counts` 时，插件会独立读取 Tushare `stock_basic` 的 `L`/`D`/`P` 列表，按目标交易日计算有效 membership，并记录版本、有效日期、市场计数、状态计数、BJ 日历策略、`suspension_method=not_available_ratios_only` 和 canonical digest；这表示暂停状态未单独取得，只能按数量比率做覆盖判断。`tushare_raw_universe_version` 可限制允许的证据版本，留空时使用观测生成的版本；`tushare_raw_require_universe_evidence` 保留为兼容配置，但设为 `false` 也不会关闭 fail-closed 保护。
- `quote_interval_seconds`：盘中自选股行情轮询间隔，默认 30 秒。
- `minute_enabled`：是否记录盘中一分钟聚合行情，默认开启；只用于观测和后续指标，不改变现有评分。成交量/额按行情源累计值计算为分钟增量，开始监听前的累计部分不会回溯。
- `minute_bar_history`：每只股票保留的已完成分钟线数量，默认 120 根。
- `minute_bar_keep_days`：SQLite 保留已完成分钟线的天数，默认 7 天。
- `deep_screen_limit` / `factor_screen_limit`：技术深筛和因子终评上限，默认 300/100。
- `screen_min_indicator_coverage`：完整空结果允许清理候选池所需的技术指标覆盖率，默认 0.8。
- `minute_trigger_enabled`：是否启用分钟线突破提醒，默认关闭。开启后要求连续上涨并突破近几根分钟线高点，只发研究提醒，不自动下单。
- `minute_trigger_lookback`、`minute_trigger_min_bars`：突破参考窗口和最少分钟线数量，默认都是 5 根。
- `minute_trigger_consecutive_up`：连续上涨根数，默认 3 根。
- `minute_trigger_step_pct`、`minute_trigger_breakout_pct`：每根最小涨幅和突破幅度，默认 0.1% 和 0.5%。
- `intraday_failure_threshold`：连续行情失败达到该次数后暂缓信号推送；默认 0，仅统计不暂缓。
- `cost_profit_threshold_pct`：相对成本达到该盈利幅度时发送收益阈值事件提醒，默认 5%；仅用于复核，不是交易指令，未计手续费和滑点。
- `cost_risk_threshold_pct`：相对成本达到该亏损幅度时发送风险观察，默认 5%；仅用于复核，不是交易指令。
- 成本价必须是有限正数；非法、零值、负值和布尔值会被拒绝，不会写入自选股。
- `min_score`：技术评分最低分；分数只是规则筛选结果，不代表收益概率。
- `factor_mode`：`report_only` 只展示行业、基本面和大盘因子；`score` 才把它们加入综合排序。建议先使用 `report_only`。
- `factor_source`：因子来源。`auto`/`eastmoney` 使用东方财富公开字段；`tushare` 会在权限允许时补估值、ROE、营收增速和经营现金流字段；`custom` 使用自定义 JSON 接口。
- `factor_data_url`：可选自定义因子接口。留空时使用 `factor_source` 指定的内置来源。接口返回 `data` 数组，每项至少包含 `code`，可选 `industry`、`industry_score`、`fundamental_score`；也可直接给原始 `roe`、`profit_growth`、`cash_quality`、`pe`、`pb`、`st_flag`、`audit_flag`，插件会计算基本面分。
- `market_min_snapshot_size`：只有本地快照达到该数量才按“完整市场”计算大盘环境，默认 4000；不足时报告会标为 `partial`。
- `confirmation_enabled`：启用连续信号确认，默认关闭；开启后需连续满足条件才推送技术信号，确认进度会保存到 SQLite。
- `confirmation_periods`：连续确认次数，默认 2 次。
- `confirmation_max_gap_seconds`：连续确认最大间隔，默认 90 秒。

自定义快照接口的返回格式示例：

```json
{
  "data": {
    "total": 1,
    "diff": [
      {
        "f12": "600000",
        "f14": "浦发银行",
        "f2": 10.5,
        "f3": 2.1,
        "f5": 123456,
        "f6": 987654321
      }
    ]
  }
}
```

### 推送白名单

- `push_whitelist`：填写 `unified_msg_origin`，多个值用逗号或换行分隔。留空时默认不发送后台推送。
- `push_max_chars`：单次后台推送的最大字符数，超出时按行拆分发送，默认 3500。
- `daily_snapshot_min_size`：判定完整收盘快照的最少有效股票数，默认 4000；Tushare raw 任一交易日低于此数量会整体拒绝并等待后续补抓。
- `report_candidate_limit`：全市场选股报告默认展示的候选数量，默认 10；完整候选仍可用 `/候选池` 查看。
- `/候选池 [数量]`：读取当前 active 候选运行，默认展示配置数量、最多 100 只；当前行业按东方财富分类分组，只有带当前分类的记录才使用该标签，旧 `industry_name` 会标为“候选快照行业”，全局编号且每只固定三行。行业标签仅为当前展示分类，不回写历史行业因子；行情涨跌优先按候选实际交易日回补，找不到时显示“涨跌未记录”。
- 候选池顶部会标注“东方财富当前行业分类，仅展示，非历史因子”、旧记录标注语义和数据日期；长消息按 `push_max_chars` 分段，板块标题会在跨段时重复。
- 默认由配置中的 `push_whitelist` 管理。只有明确打开 `allow_self_whitelist` 后，目标群聊或私聊才能通过 `/白名单 开启` 自行加入。
- 执行 `/监听 状态` 可以查看当前会话标识。
- 只有白名单内且已执行 `/监听 开启` 的会话，才能收到自动推送。
- 手动执行 `/选股`、`/行情`、`/故事` 的回复不受白名单限制。

### 新闻与模型

- `news_rss_url`：RSS 或公告聚合地址；留空则关闭故事监听。
- `llm_enabled`：是否调用模型 API 总结新闻，默认 `false`。
- `llm_annotation_enabled`：是否调用模型 API 解释盘中候选，默认 `false`。模型只补充解释，不改变规则信号。
- `llm_annotation_interval_seconds`：盘中模型解释间隔，默认 180 秒。
- `llm_annotation_limit`：每批模型解释候选数量，默认 10 只。
- `llm_annotation_max_tokens`：每批模型解释最大 Token 数，默认 800。
- `llm_min_interval_seconds`：模型请求最小间隔，默认 10 秒。
- `llm_daily_request_limit`：模型每日最大请求次数，默认 100 次。
- `llm_base_url`、`llm_api_key`、`llm_model`：OpenAI 兼容接口配置。
- `paper_trading_only`：保持为 `true`。本项目没有下单接口。

## 数据源说明

- 每日全市场快照默认使用东方财富公开接口，也可以通过 `daily_market_url` 替换。
- 配置 `tushare_token` 后，每日全市场快照和技术历史只使用 Tushare raw 批次；请求前优先读取新鲜 active generation，请求失败或发生并发竞态后再次读取；仅在明确分类的 Tushare 瞬时、提供商或历史失败且没有可用 active raw 时才退回东方财富，不会退回 Tencent 或 legacy `daily_bars`。
- 若 Tushare 无法提供可用 raw（网络、限流、熔断、尚未发布、交易日历/发布/覆盖校验或历史数据校验失败），`/全市场选股` 会在实际交易日经东方财富指数日期验证后提供临时降级预览。该预览只使用同批东方财富快照和未复权东方财富日线，来源标为 `eastmoney_fallback`、质量为 `degraded`，不写入任何候选池或历史存储，也不产生可回放价位；快照或历史覆盖不足时明确提示“降级数据不可用”，不能解读为 0 候选。
- 盘中自选股默认使用新浪批量行情接口，适合少量自选股轮询。
- 未配置 `tushare_token` 时，历史日线仍按兼容旧版路径使用东方财富接口；配置 token 后，历史指标只从活动 Tushare raw generation 计算。
- 东方财富因子字段仅用于当前研究报告，包含行业标签和部分估值/ROE，质量会标为 `partial`；它不作为历史回放的完整基本面真值。
- 历史交易日不会使用东方财富的当前财务字段倒灌；插件会优先读取该日期已缓存的因子快照，否则标为未知。
- `candidate_plan_valid_days`：收盘候选价位计划用于盘中监听的有效天数，默认 10 天。
- `price_plan_close_tolerance_pct`：收盘计划参考价与未复权日线最后收盘价允许的最大偏差，默认 1%。偏差、日期、口径或价位顺序校验失败时，计划只保留为不可用记录，不会触发价位告警或回放。
- 盘中价位使用最近一次通过校验的收盘候选保存的价位计划，不会随着盘中重新计算而移动。
- `/验证 [天数]` 从与筛选 generation 隔离的 Tushare evaluation raw 批次读取基准日之后的已完成交易日，不读取 screening batch 的未来行，也不混用 legacy/复权数据；网络和 evaluation 缓存都不可用时会明确报告样本不足。没有明确校验标记的历史计划不会回放；`/结果` 只展示未复权且校验通过的回放记录。
- 行业强弱由同批已补齐日线的股票计算行业 5 日相对动量、上涨占比和成交活跃度；样本不足会标为未知，不强行加分。
- Tushare Pro 更适合每日和历史数据，不建议用于高频盘中监听。Token 只填入 AstrBot 配置或环境变量，不要放进 URL 或提交到 GitHub。

公开接口可能出现限流、延迟或临时不可用。未配置 Tushare Token 时，快照失败仍会退回 `universe_codes` 或自选股扫描；配置 Token 时，全市场命令按上文规则提供隔离的东方财富临时预览。若明确关闭 `tushare_raw_publish_enabled`，命令会报告 shadow-only 状态并停止，不调用东方财富。

## 免责声明

本插件只提供信息整理和研究辅助，不构成投资建议，也不保证数据实时、完整或准确。任何交易决定都应由用户自行确认。
