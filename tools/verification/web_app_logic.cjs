"use strict";
// Loads webapp/static/app.js with minimal DOM stubs and checks display logic without a browser.
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "..", "webapp", "static", "app.js"), "utf8");
const element = () => ({
  innerHTML: "", textContent: "", className: "", value: "", disabled: false, dataset: {}, style: {}, title: "",
  classList: {add() {}, remove() {}, toggle() {}}, addEventListener() {}, removeEventListener() {},
  insertAdjacentHTML() {}, setAttribute() {}, removeAttribute() {}, focus() {}, querySelector: () => null,
  querySelectorAll: () => [], firstElementChild: null, parentElement: null,
});
const context = {
  console, URLSearchParams, setTimeout, clearTimeout, Promise, Date, Intl,
  document: {querySelector: () => element(), querySelectorAll: () => [], addEventListener() {}, hidden: true, title: ""},
  location: {hash: "#health", hostname: "127.0.0.1", host: "127.0.0.1:8765"},
  history: {replaceState() {}}, matchMedia: () => ({matches: false, addEventListener() {}}),
  fetch: () => Promise.reject(new Error("offline")), setInterval() {}, devicePixelRatio: 1,
  ResizeObserver: class { observe() {} disconnect() {} }, AbortController, addEventListener() {},
};
context.window = context;
vm.createContext(context);
vm.runInContext(source + `
;globalThis.__app = {providerKind, providerText, sessionBand, primaryDate, coveragePanel, snapshotAgeText, bj, state,
  dataState, renderResearch, renderPerformance, renderHealth, renderSettings, reasonText, candidateEmpty, stockStatusNotice,
  hotBadge, indexTrendPanel, overheatStockPanel, macdStockPanel, funnelPanel, auditPanel, funnelVerdict, stockScreenLine,
  maSet, movingAverage, chartTools, chartSeries, chartCaption, liveQuoteHtml, limitText, riskReminderPanel};`, context, {filename: "app.js"});

const app = context.__app;
const failures = [];
const check = (name, condition) => { if (!condition) failures.push(name); };
const now = Date.now() / 1000;

// Rate-limit rows: deadlines decide pauses, success telemetry is not invented.
const rate = extra => ({name: "stock_basic", telemetry: "api_rate_limit_state", failure_streak: 0, error: null, ...extra});
check("rate_only_unknown", app.providerKind(rate({blocked_active: false, blocked_until: new Date((now - 86400) * 1000).toISOString()})) === "unk");
check("rate_only_active_block", app.providerKind(rate({blocked_active: true, blocked_until: new Date((now + 3600) * 1000).toISOString()})) === "warn");
check("rate_only_failures", app.providerKind(rate({blocked_active: false, failure_streak: 2})) === "warn");
check("expired_text", app.providerText(rate({blocked_active: false})).includes("未采集成功记录"));
const healthy = {name: "tushare", telemetry: "provider_health", success_at: "2026-09-30T08:00:00", quality: "good"};
check("provider_ok", app.providerKind(healthy) === "ok");
check("legacy_expired_unix_seconds", app.providerKind({...healthy, blocked_until: now - 60}) === "ok");
check("legacy_future_unix_seconds", app.providerKind({...healthy, blocked_until: now + 3600}) === "warn");
check("invalid_deadline_not_pause", app.providerKind({...healthy, blocked_until: "garbage"}) === "ok");
check("unix_seconds_display", app.bj(1759800000) !== "未知");
const legacy = {name: "eastmoney", telemetry: "provider_health", stale: true, last_activity_at: "2026-08-29T04:07:44+00:00", success_at: "2026-08-29T04:07:44+00:00", quality: "partial"};
check("stale_telemetry_unknown", app.providerKind(legacy) === "unk" && app.providerText(legacy).includes("旧版遥测"));

app.state.ctx.overview = {meta: {status: "available"}, data: {batches: [{state: "published"}], quality: "good", complete: true, data_date: "2026-09-30"}};
app.state.ctx.health = {data: {providers: [rate({blocked_active: false})]}};
check("rate_only_not_outage", app.dataState().kind === "ok");
app.state.ctx.health = {data: {providers: [legacy]}};
check("stale_telemetry_not_outage", app.dataState().kind === "ok");
app.state.ctx.health = {data: {providers: [rate({blocked_active: true, blocked_until: new Date((now + 3600) * 1000).toISOString()})]}};
check("active_block_partial", app.dataState().kind === "warn");

// Missing intraday quotes are not a holiday unless the calendar says so.
const missing = {status: "unknown", reason: "artifact_missing"};
check("closed_day", app.sessionBand(missing, {calendar: "closed", phase: "closed_day"}).includes("休市日"));
const trading = app.sessionBand(missing, {calendar: "open", phase: "trading"});
check("open_trading_missing", trading.includes("交易时段，但没有可用的盘中行情") && trading.includes("closed-band warn") && !trading.includes("休市日"));
check("open_after_close", app.sessionBand(missing, {calendar: "open", phase: "after_close"}).includes("已收盘，没有实时行情"));
check("calendar_unknown", app.sessionBand(missing, {calendar: "unknown", phase: "unknown"}).includes("交易日历未知"));
check("live_available", app.sessionBand({status: "available"}, {calendar: "open", phase: "trading"}) === "");

// The headline date follows the shown dataset, not the legacy daily_bars table.
check("primary_raw", app.primaryDate({primary_source: {dataset: "raw_active_generation", date: "2026-09-30", generation: 21}}).startsWith("2026-09-30 · active raw"));
check("primary_skips_legacy", app.primaryDate({sources: [{dataset: "daily_bars", date: "2026-08-28"}, {dataset: "daily_snapshot_meta", date: "2026-09-30"}]}) === "2026-09-30");

const coverage = app.coveragePanel({market: {count: 5561, complete: true, raw_date: "2026-09-30"}, risk: {known: 0, total: 5561},
  indicator: {status: "not_applicable", coverage: null, enriched: 0, targets: 0}, formal: {risk_confirmed: 0, candidates: 0, visible: 0}, run_date: "2026-09-30"});
check("coverage_not_applicable", coverage.includes("不适用：没有股票通过风险门槛") && !coverage.includes("0.0%（"));
check("coverage_risk_ratio", coverage.includes("0 / 5,561（0.0%）"));

check("snapshot_unchanged", app.snapshotAgeText({status: "stale", check: {status: "recent", result: "unchanged", checked_at: "2026-10-07T01:15:56+00:00"}}).includes("源数据未变化"));
check("snapshot_no_check", app.snapshotAgeText({status: "stale", check: {status: "unknown"}}).includes("没有检查记录"));

app.state.ctx.research = {status: "empty", reason: "no_research_freeze"};
check("research_empty", app.renderResearch().includes("暂无研究冻结") && app.renderResearch().includes("研究池表已存在"));
app.state.ctx.research = {status: "unavailable", reason: "research_not_exposed"};
check("research_not_exposed", app.renderResearch().includes("部署与 main 对应的 Web 后端"));
app.state.ctx.research = {status: "stale", reason: "superseded_by_newer_market_snapshot", trade_date: "2026-09-29", primary: [], radar: [],
  paper_history: [], selection_policy: "technical-research-v1", independent_of: ["price_min"],
  parameters: {deep_limit: 300, primary_limit: 7, radar_limit: 20, price_min: 2, price_max: 80}};
const research = app.renderResearch();
check("research_parameters", research.includes("深筛前 300") && research.includes("重点 7") && research.includes("technical-research-v1"));
check("research_stale_reason", research.includes("已有更新交易日的行情"));

// Offline research freezes render as separate, research-only lists.
app.state.ctx.catalog = {meta: {status: "available"}, data: {unavailable: [], entries: [
  {id: "ULTRASHORT_REVERSAL_V1", file: "ULTRASHORT_REVERSAL_V1_FROZEN.json", file_sha256: "a".repeat(64), label: "未通过检验，仅观察",
   stages: ["not_passed", "forward_pending"], frozen_at: "2026-10-05T19:16:33+08:00", input_as_of: "2026-09-30",
   items: [{rank: 1, code: "600857", name: "宁波中百", close: 17.22, return5: -0.2896, amount20: 222121814}]},
  {id: "LLM_SECTOR_FIRST_EXP_V0", file: "LLM_SECTOR_FIRST_EXP_V0_FROZEN.json", file_sha256: "b".repeat(64), label: "研究观察，未验证收益",
   stages: ["exploration"], items: [{rank: 1, code: "300110", name: "华仁药业", sector: "C27医药制造业", close: 3.17, return5: -0.1975}]}]}};
const catalog = app.renderResearch();
check("catalog_separate_lists", catalog.includes("ULTRASHORT_REVERSAL_V1") && catalog.includes("LLM_SECTOR_FIRST_EXP_V0") && catalog.includes("不写入正式候选或推荐表"));
check("catalog_stages", catalog.includes("未通过检验") && catalog.includes("待前瞻") && catalog.includes("探索"));
check("catalog_return_percent", catalog.includes("-28.96%") && catalog.includes("#stock/600857"));
app.state.ctx.catalog = {meta: {status: "unavailable", reason: "route_not_found"}, data: null};
check("catalog_unavailable", app.renderResearch().includes("研究成果目录不可用"));

check("performance_empty", app.renderPerformance({sample_count: 0, records: [], status_counts: {}}).includes("暂无正式推荐样本"));

const health = app.renderHealth({database: "readable", integrity: "not_checked", tables: 72, data_date: "2026-09-30",
  providers: [rate({blocked_active: false, blocked_until: "2026-10-06T01:00:00+00:00"})],
  daily_acceptance: [{date: "2026-09-30", status: "critical", checked_at: "2026-09-30T07:40:00"}, {date: "2026-09-28", status: "critical", checked_at: "2026-09-28T07:40:00"}],
  jobs: [{date: "2026-09-30", name: "automatic_close", state: "failed", error: "coverage"}], batches: [], failures: []});
check("health_expired_deadline", health.includes("（已过期）"));
check("health_history_folded", health.includes("更早记录 1 条"));
check("health_version_unknown", health.includes("version_unavailable"));
check("health_limits_shown", health.includes("最近 10 次") && health.includes("最近 15 条"));
app.state.ctx.overview = {meta: {status: "available"}, data: {batches: [{state: "published"}], quality: "good", complete: true, data_date: "2026-09-30",
  acceptance: {trade_date: "2026-09-30", status: "critical", findings: [{code: "candidate_freeze_missing", severity: "critical"}]}}};
const helped = app.renderHealth({database: "readable", integrity: "not_checked", tables: 72, data_date: "2026-09-30", providers: [],
  daily_acceptance: [{date: "2026-09-30", status: "critical", checked_at: "2026-09-30T07:40:00"}], jobs: [], batches: [], failures: []});
check("finding_help", helped.includes("影响：当天没有冻结正式候选") && helped.includes("下一步："));
check("finding_reason_text", app.reasonText("candidate_freeze_missing") === "缺少候选冻结记录");

app.state.ctx.research = {status: "research_only", trade_date: "2026-09-30", paper_history: [], parameters: null, independent_of: [],
  primary: [{rank: 1, code: "600276", name: "恒瑞医药", record_id: "research:batch:600276", score: 25, risk_level: "unknown", eligibility: "research_only",
             confirmation: "not_assessed", paper_status: "not_entered", observation_reference_close: 47.2}], radar: []};
app.state.ctx.catalog = null;
const pools = app.renderResearch();
check("research_mobile_cards", pools.includes('class="mlist"') && pools.includes('class="m-name"') && pools.includes("#stock/600276"));
app.state.candidates = [];
check("candidates_empty_links_research", app.candidateEmpty().includes("查看研究观察池（重点 1 / 警戒 0"));
app.state.ctx.catalog = {data: {entries: [{id: "ULTRASHORT_REVERSAL_V1", label: "未通过检验，仅观察", items: [{rank: 1, code: "600857"}]}]}};
check("stock_offline_membership", app.stockStatusNotice({code: "600857", bar_source: {kind: "active_raw"}, formal_status: "no_formal_candidate"})
  .includes("离线研究名单：ULTRASHORT_REVERSAL_V1 第 1 名"));

// Jobs say which attempt stopped and why; acceptance rows list later runs without rewriting the verdict.
const explained = app.renderHealth({database: "readable", integrity: "not_checked", tables: 72, data_date: "2026-10-08", providers: [], batches: [], failures: [],
  automatic_close_limits: {max_attempts: 6, retry_seconds: 300, retry_window_seconds: 14400},
  jobs: [{date: "2026-10-08", name: "automatic_close", state: "missed", stop: "gate_unpassable", attempts: 1, failure_codes: ["risk_evidence_missing"],
          gate: {attempt: 1, phase: "fail_closed:risk_evidence_missing", generation: 22, counts: {input: 5561, risk_tuple_complete: 0, tradable: 0},
                 unpassable: ["risk_evidence_missing"], retryable: []}},
         {date: "2026-10-08", name: "automatic_close", state: "missed", stop: "retry_exhausted", attempts: 6, failure_codes: ["indicator_coverage"]},
         {date: "2026-10-08", name: "automatic_close", state: "failed", attempts: 2, failure_codes: ["waiting_snapshot"], next_retry_at: "2026-10-08T07:16:00+00:00"}],
  daily_acceptance: [{date: "2026-10-08", status: "critical", checked_at: "2026-10-08T07:40:00", review: "late_screen_not_formal", publication: null,
                      runs: [{job: "daily_screen", status: "completed", candidates: 0, report_version: 1, finished_at: "2026-10-09T01:03:00", after_check: true}]}]});
check("job_gate_unpassable", explained.includes("门槛不可通过·已停止") && explained.includes("第 1 / 6 次尝试后停止") && explained.includes("不可能通过"));
check("job_funnel", explained.includes("漏斗：行情 5,561 → 风险四项已知 0 → 可交易 0") && explained.includes("输入第 22 代"));
check("job_retry_exhausted", explained.includes("重试用完·已终止") && explained.includes("上限 6 次 / 4.0 小时") && explained.includes("指标覆盖不足"));
check("job_waiting_retry", explained.includes("等待重试") && explained.includes("下次尝试"));
check("acceptance_late_run", explained.includes("（验收之后）") && explained.includes("补跑不算正式冻结") && explained.includes("没有自动收盘正式发布"));

// Settings show the plugin's load-time values, mark changes, and never print hidden strings.
const settings = app.renderSettings({snapshot: {status: "plugin_snapshot", written_at: "2026-10-07T04:00:00+00:00", plugin_version: "0.13.3",
  code_sha256: "a".repeat(64), matches_web_build: true, deprecated_settings: ["confirmation_enabled"]},
  items: [{key: "max_concurrency", group: "common", state: "shown", effective: 7, default: 5, differs: true, source: "plugin_snapshot"},
          {key: "tushare_token", group: "advanced", state: "custom", effective: null, default: "", differs: true, source: "plugin_snapshot"},
          {key: "news_rss_url", group: "advanced", state: "unknown", effective: null, default: "", differs: null, source: "effective_unknown"}]});
check("settings_snapshot_head", settings.includes("插件加载时写出的配置快照") && settings.includes("与当前 Web 构建是同一提交") && settings.includes("aaaaaaaaaaaa"));
check("settings_changed", settings.includes("与默认值不同 · 2 项") && settings.includes("已改"));
check("settings_hidden_value", settings.includes("已修改（值不公开）") && settings.includes("没有读到插件配置"));
check("settings_deprecated", settings.includes("confirmation_enabled") && settings.includes("实际行为不变"));
check("settings_missing", app.renderSettings({snapshot: {status: "missing"}, items: []}).includes("没有读到插件配置快照"));
const checked = app.renderSettings({snapshot: {status: "plugin_snapshot", setting_issues: [
  {level: "error", keys: ["price_min", "price_max"], message: "price_min 90 高于 price_max 80", effect: "正式候选会一直为空"},
  {level: "warning", keys: ["screen_min_indicator_coverage"], message: "<b>95</b> 超出允许范围", effect: "实际按 1 使用"}]}, items: []});
check("settings_issues", checked.includes("notice crit") && checked.includes("price_min 90 高于 price_max 80") && checked.includes("正式候选会一直为空")
  && checked.includes("price_min · price_max") && checked.includes("&lt;b&gt;95&lt;/b&gt;") && !checked.includes("<b>95</b>"));
check("settings_no_issues", !app.renderSettings({snapshot: {status: "plugin_snapshot"}, items: []}).includes("配置检查"));
const caps = app.renderSettings({snapshot: {status: "plugin_snapshot"}, items: [
  {key: "deep_screen_limit", state: "shown", effective: 300, default: 300, differs: false},
  {key: "factor_screen_limit", state: "shown", effective: 80, default: 100, differs: true},
  {key: "price_min", state: "shown", effective: 2, default: 2}, {key: "price_max", state: "unknown", effective: null, default: 80}]});
check("settings_caps_chain", caps.includes("筛选上限怎么串起来") && caps.includes("2. 深筛名额") && caps.includes("3. 因子名额") && caps.includes("80<small>默认 100</small>")
  && caps.includes("进不了正式候选") && caps.includes("未知</span><small>默认 80</small>") && caps.indexOf("深筛名额") < caps.indexOf("因子名额"));

// Evening research signals: overheat badges, the index thermometer and the stock panel stay display-only.
const signals = (extra = {}) => ({meta: {status: "available"}, data: {status: "available", generated_at: "2026-10-07T10:40:00+00:00",
  overheat: {status: "available", trade_date: "2026-09-30", evaluated: 2990, hot: [{code: "600010", name: "测试", score: 0.83, pct: 0.991}]},
  index_trend: {status: "available", indices: {"000905": {name: "中证500", date: "2026-09-30", close: 7000, ma200: 6500, above_ma200: true,
    distance_ma200: 0.0769, sessions_on_side: 12, above_ma120: false}}},
  stock: {code: "600010", status: "evaluated", hot: true, score: 0.83, pct: 0.991, indicators: {R20: 0.31, IVOL20: 0.025, ABTURN: 1.8, RSI14: 81.2}},
  ...extra}});
check("hot_badge_shown", app.hotBadge("600010", signals()).includes("过热") && app.hotBadge("600010", signals()).includes("99.1%"));
check("hot_badge_absent", app.hotBadge("600011", signals()) === "" && app.hotBadge("600010", {meta: {status: "partial"}, data: {status: "missing"}}) === "");
const thermo = app.indexTrendPanel(signals());
check("thermometer_above", thermo.includes("200 日均线上方") && thermo.includes("+7.69%") && thermo.includes("已持续 12 个交易日") && thermo.includes("120 日均线下方"));
check("thermometer_partial", thermo.includes("中证1000") && thermo.includes("这次没有取到") && thermo.includes("不是交易规则"));
check("thermometer_missing", app.indexTrendPanel({meta: {status: "partial"}, data: {status: "missing"}}).includes("研究信号还没有生成"));
check("thermometer_stale", app.indexTrendPanel(signals({status: "stale"})).includes("超过 3 天没有更新"));
const hotPanel = app.overheatStockPanel("600010", signals());
check("stock_panel_hot", hotPanel.includes("过热综合分位") && hotPanel.includes("99.1%") && hotPanel.includes("+31.00%") && hotPanel.includes("2.50%") && hotPanel.includes("1.80 倍"));
const excluded = app.overheatStockPanel("600002", signals({stock: {code: "600002", status: "excluded", reason: "corporate_action_60d", indicators: {}}}));
check("stock_panel_excluded", excluded.includes("不在评估范围") && excluded.includes("近 60 日有除权除息"));
check("stock_panel_job_error", app.overheatStockPanel("600010", signals({overheat: {status: "unavailable", reason: "job_error"}})).includes("研究信号任务这次没算出来"));

// Stock MACD state: golden or dead side from the server, always marked unverified.
const macd = extra => ({code: "600010", macd: {status: "available", state: "golden", trade_date: "2026-09-30", last_bar_date: "2026-09-30",
  sessions: 120, first_session: "2026-04-09", cross_date: "2026-09-28", sessions_since_cross: 2, recent_golden_cross: true,
  dif: 0.1234, dea: 0.0987, histogram: 0.0494, verified: false, ...extra}});
const golden = app.macdStockPanel(macd());
check("macd_golden", golden.includes('up">金叉') && golden.includes("未验证") && golden.includes("2026-09-28 DIF 上穿 DEA · 2 个交易日前")
  && golden.includes("0.123") && golden.includes("120 个交易日（2026-04-09 起）") && !golden.includes("按价格不变计算"));
const dead = app.macdStockPanel(macd({state: "dead", cross_date: "2026-09-30", sessions_since_cross: 0, recent_golden_cross: false,
  last_bar_date: "2026-09-29", histogram: -0.02}));
check("macd_dead", dead.includes('down">死叉') && !dead.includes('up">金叉') && dead.includes("下穿 DEA · 当天") && dead.includes("之后的交易日按价格不变计算"));
check("macd_no_cross", app.macdStockPanel(macd({state: "dead", cross_date: null, sessions_since_cross: null})).includes("本批次内 DIF 一直在 DEA 下方"));
check("macd_short", app.macdStockPanel({macd: {status: "insufficient_history", sessions: 20, min_sessions: 60}}).includes("行情不足 60 个交易日"));
check("macd_legacy", app.macdStockPanel({macd: {status: "unavailable", reason: "no_active_raw"}}).includes("旧日线表缺前收盘"));
check("macd_missing", app.macdStockPanel({}).includes("MACD 暂不可用"));

// Screening funnel (S04) and audit rows (S03): every step keeps its count and reasons; scores only rank.
const stages = [{key: "input", count: 5561, gate: true, excluded: {}}, {key: "price", count: 5200, gate: true, excluded: {price_out_of_range: 361}, price_min: 2, price_max: 80},
  {key: "risk_state", count: 4800, gate: true, excluded: {risk_state_unknown: 300, st: 100}}, {key: "deep_screen", count: 300, gate: true, excluded: {beyond_deep_limit: 4500}, limit: 300},
  {key: "indicators", count: 290, gate: false, excluded: {history_failed: 10}}, {key: "risk_review", count: 0, gate: true, excluded: {factor_not_checked: 200, st_audit_unknown: 100}},
  {key: "min_score", count: 0, gate: true, excluded: {}, min_score: 10}, {key: "candidates", count: 0, gate: true, excluded: {}, limit: 30, fallback: 0}];
const screen = {status: "available", run_id: "r1", latest_run_id: "r1", is_latest: true, date: "2026-10-08", job_name: "automatic_close", run_status: "completed",
  funnel: {version: 1, stages}, audit_total: 300, audit: [
    {code: "600001", name: "<b>甲</b>", rank: 1, status: "factor_not_checked", base_score: 30, score: 30, risk_level: "unknown", risk_flags: ["ST/审计状态未知"],
     reasons: ["均线多头趋势+12"], indicators: {rsi6: 55.2, momentum5: 3.4, momentum20: -1.2}, indicator_status: "raw_batch", comparable: false, missing_inputs: ["volume_ratio"]}]};
const funnel = app.funnelPanel(screen);
check("funnel_rows", funnel.includes("5,561") && funnel.includes("价格不在区间 361") && funnel.includes("2.00–80.00 元") && funnel.includes("超出深筛名额 4,500"));
check("funnel_indicator_aside", funnel.includes("只统计、不单独拦截"));
check("funnel_verdict", funnel.includes("在「风险复核」这一步全部被拦下，主要原因：不在因子名额内，没查 ST / 审计"));
check("funnel_valid_empty", app.funnelVerdict(stages.map(s => s.key === "risk_review" ? {...s, count: 290, excluded: {}} : s.key === "min_score" ? {...s, excluded: {below_min_score: 290}} : s))
  === "在「达到最低分」这一步全部被拦下，主要原因：低于最低分。");
check("funnel_fallback", app.funnelVerdict(stages.map(s => s.key === "risk_review" ? {...s, count: 290} : s.key === "candidates" ? {...s, count: 5, fallback: 5} : s)).includes("列出 5 只未达最低分的观察候选"));
check("funnel_empty_market", app.funnelVerdict([{key: "input", count: 0}]) === "当日行情为空，后面各步都没有数据。");
check("funnel_older", app.funnelPanel({...screen, is_latest: false, latest_run_id: "r2"}).includes("下面是更早的一次"));
check("funnel_not_recorded", app.funnelPanel({status: "not_recorded"}).includes("插件更新到带筛选漏斗的版本后"));
const auditHtml = app.auditPanel(screen);
check("audit_rows", auditHtml.includes("深筛明细 · 前 1 / 共 300") && auditHtml.includes("没查 ST / 审计") && auditHtml.includes("风险：ST/审计状态未知")
  && auditHtml.includes("缺 1 项") && auditHtml.includes("+3.40%") && auditHtml.includes("不是胜率"));
check("audit_escaped", auditHtml.includes("&lt;b&gt;甲&lt;/b&gt;") && !auditHtml.includes("<b>甲</b>"));
check("audit_absent", app.auditPanel({status: "not_recorded"}) === "");
check("stock_screen_listed", app.stockScreenLine({screen_audit: {status: "listed", date: "2026-10-08", audit_total: 300, row: screen.audit[0]}})
  .includes("深筛第 1 / 300 名 · 没查 ST / 审计 · 技术分 30（只用于排序）"));
check("stock_screen_not_listed", app.stockScreenLine({screen_audit: {status: "not_listed", date: "2026-10-08", audit_total: 300, audit_shown: 60}}).includes("不在深筛明细前 60 名里"));
check("stock_screen_none", app.stockScreenLine({screen_audit: {status: "not_recorded"}}) === "" && app.stockScreenLine({}) === "");

// Chart: default and custom moving averages, the MACD sub-chart series and the forward-adjusted view.
check("ma_default", JSON.stringify(app.maSet(null).map(m => m.n)) === "[5,13,34,55,120]");
check("ma_custom_clean", JSON.stringify(app.maSet([{n: 20}, {n: 20}, {n: 1}, {n: 300}, {n: 60, on: false}, "x"])) === JSON.stringify([{n: 20, on: true}, {n: 60, on: false}]));
check("ma_empty_allowed", app.maSet([]).length === 0);
check("ma_average", JSON.stringify(app.movingAverage([{close: 1}, {close: 2}, {close: 3}], 2).slice(1)) === "[1.5,2.5]");
const chartBars = Array.from({length: 120}, (_, i) => ({date: `d${String(i).padStart(3, "0")}`, open: 10, high: 11, low: 9, close: i < 100 ? 20 : 10, volume: 1}));
Object.assign(app.state, {maList: null, period: 60, ma: true, stock: {bars: chartBars, macd: {status: "available",
  series: chartBars.map((b, i) => ({date: b.date, dif: 0.1, dea: 0.05, hist: 0.1, adj: i < 100 ? 0.5 : 1}))}}});
const chartToolbar = app.chartTools();
check("chart_tools", ["MA5", "MA13", "MA34", "MA55", "MA120"].every(t => chartToolbar.includes(`>MA${t.slice(2)}<`)) && chartToolbar.includes('id="ma-toggle"')
  && chartToolbar.includes('id="macd-toggle"') && chartToolbar.includes('id="adjust-toggle"') && chartToolbar.includes("添加均线") && !chartToolbar.includes("style="));
const unadjusted = app.chartSeries(chartBars);
check("chart_unadjusted_default", unadjusted.adjusted === false && unadjusted.rows[0].close === 20 && unadjusted.macd.length === 120 && unadjusted.macd[0].dif === 0.1);
app.state.adjusted = true;
const forward = app.chartSeries(chartBars);
check("chart_forward_adjusted", forward.adjusted && forward.rows[0].close === 10 && forward.rows[0].high === 5.5 && forward.rows[119].close === 10);
const chartNote = app.chartCaption(forward);
check("chart_caption", chartNote.includes("前复权") && chartNote.includes("MA120 只画得出后半段") && !chartNote.includes("MA55 只") && !chartNote.includes("style="));
app.state.stock.macd = {status: "insufficient_history"};
const noFactors = app.chartSeries(chartBars);
check("chart_adjust_unavailable", !noFactors.adjusted && noFactors.macd === null && app.chartCaption(noFactors).includes("先按未复权显示"));
app.state.macdOn = false; app.state.stock.macd = {status: "available", series: []};
check("chart_macd_off", app.chartSeries(chartBars).macd === null);

// Live intraday quote on the stock page: only while the artifact is current, and only for watched codes.
const liveHtml = app.liveQuoteHtml({status: "available", source: "sina", quote: {code: "600857", price: 17.5, pct_change: 1.2, provider_ts: "2026-10-08T02:31:05+00:00", source: "sina"}});
check("live_quote", liveHtml.includes("盘中") && liveHtml.includes("17.50") && liveHtml.includes("+1.20%") && liveHtml.includes("sina"));
check("live_not_watched", app.liveQuoteHtml({status: "partial", quote: null}).includes("不在插件盯盘名单"));
check("live_outside_session", app.liveQuoteHtml({status: "unknown", reason: "outside_session", quote: null}) === "" && app.liveQuoteHtml(undefined) === "");
const limits = app.limitText({bar_date: "2026-09-30", limits: {up: 11.06, down: 9.05, rate: 0.1, board: "main", st: false}});
check("limit_text", limits.includes("11.06") && limits.includes("9.05") && limits.includes("2026-09-30 收盘和主板 ±10%") && limits.includes("未核验"));
check("limit_unknown", app.limitText({limits: null}) === "未知");

// Lockup-expiry and margin-crowding reminders: list badges and the stock panel, display only.
const riskSignals = (risk, extra = {}) => signals({unlock: {status: "available", window: ["2026-10-07", "2026-11-06"], heavy: [{code: "301683", date: "2026-10-08", ratio: 0.0568}]},
  margin: {status: "available", trade_date: "2026-09-29", crowded: [{code: "600519", buy_share: 0.2}]}, risk, ...extra});
const unlockBadge = app.hotBadge("301683", riskSignals(null)), marginBadge = app.hotBadge("600519", riskSignals(null));
check("risk_badges", unlockBadge.includes("解禁") && unlockBadge.includes("5.7%") && !unlockBadge.includes("过热") && marginBadge.includes("两融拥挤") && marginBadge.includes("20.0%"));
check("risk_badges_absent", app.hotBadge("600011", riskSignals(null)) === "");
const riskPanel = app.riskReminderPanel(riskSignals({unlock: {status: "scheduled", heavy: true, ratio_total: 0.0568, events: [{date: "2026-10-08", type: "首发机构配售股份", ratio: 0.0568}]},
  margin: {status: "listed", buy: 229076790, balance: 17318617925, buy_share: 0.2, pct: 0.99, crowded: true}}));
check("risk_panel", riskPanel.includes("解禁压力大") && riskPanel.includes("2026-10-08 首发机构配售股份 占流通 5.68%") && riskPanel.includes("两融拥挤")
  && riskPanel.includes("融资余额 173.19 亿") && riskPanel.includes("未验证"));
const quietPanel = app.riskReminderPanel(riskSignals({unlock: {status: "none", events: []}, margin: {status: "not_listed"}}));
check("risk_panel_quiet", quietPanel.includes("30 天内没有解禁") && quietPanel.includes("不在两融标的里"));
check("risk_panel_failed", app.riskReminderPanel(riskSignals(null, {margin: {status: "unavailable", reason: "margin_unpublished"}})).includes("margin_unpublished"));
check("risk_panel_missing", app.riskReminderPanel({meta: {status: "partial"}, data: {status: "missing"}}).includes("研究信号还没有生成"));

if (failures.length) {
  console.error(JSON.stringify({status: "failed", failures}));
  process.exit(1);
}
console.log(JSON.stringify({status: "passed"}));
