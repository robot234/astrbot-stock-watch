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
  dataState, renderResearch, renderPerformance, renderHealth};`, context, {filename: "app.js"});

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

app.state.ctx.overview = {meta: {status: "available"}, data: {batches: [{state: "published"}], quality: "good", complete: true, data_date: "2026-09-30"}};
app.state.ctx.health = {data: {providers: [rate({blocked_active: false})]}};
check("rate_only_not_outage", app.dataState().kind === "ok");
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

check("performance_empty", app.renderPerformance({sample_count: 0, records: [], status_counts: {}}).includes("暂无正式推荐样本"));

const health = app.renderHealth({database: "readable", integrity: "not_checked", tables: 72, data_date: "2026-09-30",
  providers: [rate({blocked_active: false, blocked_until: "2026-10-06T01:00:00+00:00"})],
  daily_acceptance: [{date: "2026-09-30", status: "critical", checked_at: "2026-09-30T07:40:00"}, {date: "2026-09-28", status: "critical", checked_at: "2026-09-28T07:40:00"}],
  jobs: [{date: "2026-09-30", name: "automatic_close", state: "failed", error: "coverage"}], batches: [], failures: []});
check("health_expired_deadline", health.includes("（已过期）"));
check("health_history_folded", health.includes("更早记录 1 条"));
check("health_version_unknown", health.includes("version_unavailable"));

if (failures.length) {
  console.error(JSON.stringify({status: "failed", failures}));
  process.exit(1);
}
console.log(JSON.stringify({status: "passed"}));
