# AstrBot A 股选股与自选监听

一个面向研究和模拟盘的 AstrBot 插件，提供收盘选股、盘中自选股监听和市场故事提醒。

## 功能

- 收盘后按本地技术规则筛选候选股
- 每个交易日抓取一次全市场日快照并保存到 SQLite
- Tushare 当天暂无数据时自动寻找最近有数据的交易日
- 盘中轮询自选股，触发信号后推送提醒
- 盘中行情按会话合并抓取，信号冷却状态持久化到 SQLite
- 连续确认状态持久化到 SQLite，插件重启后可继续累计
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
/自选 添加 600000 [成本价]
/自选 删除 600000
/自选
/监听 开启
/监听 关闭
/监听 状态
/状态
/白名单 开启
/白名单 关闭
/白名单 列表
/白名单 状态
/行情 600000
/故事 [关键词]
```

## 配置

### 股票与缓存

- `universe_codes`：选股代码，逗号分隔。留空时使用各会话的自选股。
- `daily_cache_enabled`：是否启用每日全市场快照缓存，默认 `true`。
- `daily_cache_keep_days`：日快照保留天数，默认 180 天。
- `daily_market_url`：自定义全市场快照接口。留空使用东方财富；自定义接口需要返回兼容格式的 JSON，并支持 `pn`、`pz`、`fs`、`fields` 分页参数。
- `tushare_url`：Tushare Pro 接口地址，通常留空即可，默认使用 `https://api.tushare.pro`。
- `tushare_token`：Tushare Pro Token。填写后每日快照优先调用 Tushare `daily` 接口；留空则不调用 Tushare，使用东方财富。
- `quote_interval_seconds`：盘中自选股行情轮询间隔，默认 30 秒。
- `minute_enabled`：是否记录盘中一分钟聚合行情，默认开启；只用于观测和后续指标，不改变现有评分。成交量/额按行情源累计值计算为分钟增量，开始监听前的累计部分不会回溯。
- `minute_bar_history`：每只股票保留的已完成分钟线数量，默认 120 根。
- `minute_trigger_enabled`：是否启用分钟线突破提醒，默认关闭。开启后要求连续上涨并突破近几根分钟线高点，只发研究提醒，不自动下单。
- `minute_trigger_lookback`、`minute_trigger_min_bars`：突破参考窗口和最少分钟线数量，默认都是 5 根。
- `minute_trigger_consecutive_up`：连续上涨根数，默认 3 根。
- `minute_trigger_step_pct`、`minute_trigger_breakout_pct`：每根最小涨幅和突破幅度，默认 0.1% 和 0.5%。
- `intraday_failure_threshold`：连续行情失败达到该次数后暂缓信号推送；默认 0，仅统计不暂缓。
- `cost_profit_threshold_pct`：相对成本达到该盈利幅度时发送收益阈值事件提醒，默认 5%；仅用于复核，不是交易指令，未计手续费和滑点。
- `cost_risk_threshold_pct`：相对成本达到该亏损幅度时发送风险观察，默认 5%；仅用于复核，不是交易指令。
- `min_score`：技术评分最低分；分数只是规则筛选结果，不代表收益概率。
- `factor_mode`：`report_only` 只展示行业、基本面和大盘因子；`score` 才把它们加入综合排序。建议先使用 `report_only`。
- `factor_source`：因子来源。`auto`/`eastmoney` 使用东方财富公开字段；`tushare` 使用已配置 Token 的估值字段；`custom` 使用自定义 JSON 接口。
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
- 也可以在目标群聊或私聊中执行 `/白名单 开启` 动态加入。
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
- 配置 `tushare_token` 后，每日全市场快照优先使用 Tushare Pro；Tushare 请求失败会退回东方财富。
- 盘中自选股默认使用新浪批量行情接口，适合少量自选股轮询。
- 历史日线和技术指标使用东方财富接口。
- 东方财富因子字段仅用于当前研究报告，包含行业标签和部分估值/ROE，质量会标为 `partial`；它不作为历史回放的完整基本面真值。
- 历史交易日不会使用东方财富的当前财务字段倒灌；插件会优先读取该日期已缓存的因子快照，否则标为未知。
- 行业强弱由同批已补齐日线的股票计算行业 5 日相对动量、上涨占比和成交活跃度；样本不足会标为未知，不强行加分。
- Tushare Pro 更适合每日和历史数据，不建议用于高频盘中监听。Token 只填入 AstrBot 配置或环境变量，不要放进 URL 或提交到 GitHub。

公开接口可能出现限流、延迟或临时不可用。快照失败时插件会退回 `universe_codes` 或自选股扫描。

## 免责声明

本插件只提供信息整理和研究辅助，不构成投资建议，也不保证数据实时、完整或准确。任何交易决定都应由用户自行确认。
