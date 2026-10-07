"use strict";
const $ = (s, root = document) => root.querySelector(s);
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const finite = n => n !== null && n !== undefined && n !== "" && Number.isFinite(Number(n));
const fmt = (n, digits = 2) => finite(n) ? Number(n).toLocaleString("zh-CN", {minimumFractionDigits:digits, maximumFractionDigits:digits}) : "—";
const pct = (n, ratio = false) => finite(n) ? `${Number(n) > 0 ? "+" : ""}${fmt(Number(n) * (ratio ? 100 : 1))}%` : "—";
const stockHref = code => /^(?:\d{6}|DEMO\d{2})$/.test(String(code ?? "")) ? `#stock/${encodeURIComponent(String(code))}` : "#stock";
const percentWidth = value => finite(value) ? Math.max(0, Math.min(100, Number(value))).toFixed(2) : "0";
const tone = n => finite(n) ? Number(n) > 0 ? "up" : Number(n) < 0 ? "down" : "flat" : "muted";
const LIMITS = {candidates:500, events:200, recommendations:1000, bars:120, announcements:200};
const MIN_SAMPLE = 30;

// Lucide icon paths (ISC license, see /icons/LICENSE).
const ICONS = {
  "layout-dashboard": '<rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/>',
  "activity": '<path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/>',
  "list-filter": '<path d="M2 5h20"/><path d="M6 12h12"/><path d="M9 19h6"/>',
  "chart-candlestick": '<path d="M9 5v4"/><rect width="4" height="6" x="7" y="9" rx="1"/><path d="M9 15v2"/><path d="M17 3v2"/><rect width="4" height="8" x="15" y="5" rx="1"/><path d="M17 13v3"/><path d="M3 3v16a2 2 0 0 0 2 2h16"/>',
  "chart-no-axes-combined": '<path d="M12 16v5"/><path d="M16 14.639V21"/><path d="M20 10.656V21"/><path d="m22 3-8.646 8.646a.5.5 0 0 1-.708 0L9.354 8.354a.5.5 0 0 0-.707 0L2 15"/><path d="M4 18.463V21"/><path d="M8 14.656V21"/>',
  "heart-pulse": '<path d="M2 9.5a5.5 5.5 0 0 1 9.591-3.676.56.56 0 0 0 .818 0A5.49 5.49 0 0 1 22 9.5c0 2.29-1.5 4-3 5.5l-5.492 5.313a2 2 0 0 1-3 .019L5 15c-1.5-1.5-3-3.2-3-5.5"/><path d="M3.22 13H9.5l.5-1 2 4.5 2-7 1.5 3.5h5.27"/>',
  "settings-2": '<path d="M14 17H5"/><path d="M19 7h-9"/><circle cx="17" cy="17" r="3"/><circle cx="7" cy="7" r="3"/>',
  "refresh-cw": '<path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/><path d="M8 16H3v5"/>',
  "search": '<path d="m21 21-4.34-4.34"/><circle cx="11" cy="11" r="8"/>',
  "arrow-up-down": '<path d="m21 16-4 4-4-4"/><path d="M17 20V4"/><path d="m3 8 4-4 4 4"/><path d="M7 4v16"/>',
  "arrow-left": '<path d="m12 19-7-7 7-7"/><path d="M19 12H5"/>',
  "chevron-right": '<path d="m9 18 6-6-6-6"/>',
  "eye": '<path d="M2.062 12.348a1 1 0 0 1 0-.696 10.75 10.75 0 0 1 19.876 0 1 1 0 0 1 0 .696 10.75 10.75 0 0 1-19.876 0"/><circle cx="12" cy="12" r="3"/>',
  "flask-conical": '<path d="M14 2v6a2 2 0 0 0 .245.96l5.51 10.08A2 2 0 0 1 18 22H6a2 2 0 0 1-1.755-2.96l5.51-10.08A2 2 0 0 0 10 8V2"/><path d="M6.453 15h11.094"/><path d="M8.5 2h7"/>',
  "database": '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/>',
  "triangle-alert": '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
  "circle-check": '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>',
  "circle-help": '<circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><path d="M12 17h.01"/>',
  "clock": '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
  "lock": '<rect width="18" height="11" x="3" y="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>',
  "shield-alert": '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/><path d="M12 8v4"/><path d="M12 16h.01"/>',
  "ban": '<circle cx="12" cy="12" r="10"/><path d="m4.9 4.9 14.2 14.2"/>',
  "ellipsis": '<circle cx="12" cy="12" r="1"/><circle cx="19" cy="12" r="1"/><circle cx="5" cy="12" r="1"/>',
  "megaphone": '<path d="m3 11 18-5v12L3 14v-3z"/><path d="M11.6 16.8a3 3 0 1 1-5.8-1.6"/>',
  "inbox": '<polyline points="22 12 16 12 14 15 10 15 8 12 2 12"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/>',
  "calendar": '<path d="M8 2v4"/><path d="M16 2v4"/><rect width="18" height="18" x="3" y="4" rx="2"/><path d="M3 10h18"/>',
  "x": '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>'
};
const ic = (name, cls = "") => `<svg class="i ${cls}" viewBox="0 0 24 24" aria-hidden="true">${ICONS[name] || ""}</svg>`;
function hydrateIcons(root = document) {
  root.querySelectorAll("[data-icon]").forEach(el => {if (!el.firstElementChild) el.innerHTML = ic(el.dataset.icon);});
}

// Beijing time display. Timestamps without a zone are stored as UTC.
const pad2 = n => String(n).padStart(2, "0");
const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;
function toDate(value) {
  if (!value) return null;
  const text = String(value).trim();
  if (DATE_ONLY.test(text)) return null;
  if (/^\d{9,}(?:\.\d+)?$/.test(text)) return new Date(Number(text) * 1000);
  const iso = text.replace(" ", "T");
  const date = new Date(/(?:Z|[+-]\d{2}:?\d{2})$/.test(iso) ? iso : `${iso}Z`);
  return Number.isNaN(date.getTime()) ? null : date;
}
function bjParts(date) {
  const b = new Date(date.getTime() + 8 * 3600 * 1000);
  return {y:b.getUTCFullYear(), mo:pad2(b.getUTCMonth() + 1), d:pad2(b.getUTCDate()), h:pad2(b.getUTCHours()), mi:pad2(b.getUTCMinutes()), s:pad2(b.getUTCSeconds())};
}
function bj(value) {
  if (!value) return "未知";
  const text = String(value).trim();
  if (DATE_ONLY.test(text)) return text;
  const date = toDate(text);
  if (!date) return "未知";
  const p = bjParts(date), now = bjParts(new Date());
  return `${p.y !== now.y ? `${p.y}-` : ""}${p.mo}-${p.d} ${p.h}:${p.mi}:${p.s}`;
}
const weekday = text => DATE_ONLY.test(String(text || "")) ? `周${"日一二三四五六"[new Date(`${text}T00:00:00Z`).getUTCDay()]}` : "";

// Status vocabulary: Chinese text in the body, machine codes only in tooltips or reason codes.
const STATUS = {
  ok:["正常","ok"], good:["正常","ok"], healthy:["正常","ok"], available:["可用","ok"], complete:["完成","ok"], completed:["完成","ok"],
  fresh:["新鲜","ok"], published:["已发布","ok"], readable:["可读","ok"], mature:["已到期","ok"], eligible:["可跟踪","ok"],
  validated:["已验证","ok"], comparable:["可比","ok"], confirmed:["已确认","ok"], read_only:["只读","ok"], valuation_complete:["收盘估值完成","ok"],
  partial:["部分可用","warn"], degraded:["部分可用","warn"], warning:["警告","warn"], rate_limited:["限流","warn"], stale:["数据过期","warn"],
  watch_only:["需复核","warn"], data_unverified:["数据未核验","warn"], trading_flags_clear_only:["交易状态字段已核","warn"],
  critical:["严重","crit"], unavailable:["不可用","crit"], missing:["不可用","crit"], failed:["失败","crit"], blocked:["已拦截","crit"],
  pending:["待到期","info"], running:["运行中","info"], valuation_in_progress:["估值中","info"], stored_not_live:["已存非实时","info"],
  unknown:["未知","unk"], unknown_order:["未知","unk"], not_checked:["未检查","unk"], closed:["休市","unk"], closed_day:["休市","unk"], live_market_missing:["无盘中行情","unk"],
  not_applicable:["不适用","unk"], recent:["近期","ok"], unchanged:["源未变化","ok"],
  not_passed:["未通过检验","warn"], exploration:["探索","lab"], forward_pending:["待前瞻","info"], passed:["已通过","ok"],
  empty:["暂无冻结","unk"], not_comparable:["不可比","unk"], plan_expired:["计划过期","unk"], not_entered:["未入场","unk"],
  unfilled:["模拟未成交","unk"], not_filled:["未成交","unk"], not_executable:["不可执行","unk"],
  unverified:["未验证","lab"], research_only:["未验证","lab"], simulated_fill:["模拟成交","lab"], entered:["已模拟入场","lab"]
};
const DELIVERY = {pending:["待发送","info"], sending:["发送中","info"], sent:["已发送","ok"], unknown_delivery:["投递不明","warn"], failed:["失败","crit"], cancelled:["已取消","unk"]};
const AI_DECISION = {keep:["保留","ok"], watch:["观察","warn"], veto:["否决","crit"]};
const SIGNAL = {attention_entry:"进入风险区间", risk_invalidated:"风险失效", confirmed_breakout:"突破建议买入价", breakout_confirmed:"突破建议买入价", plan_expired:"计划过期"};
const CLOSE_REGIME = {risk_on:"风险偏好", neutral:"中性", risk_off:"风险偏弱", unknown:"未知"};
const LIVE_REGIME = {strong:"强势", neutral:"中性", weak:"偏弱", risk_off:"风险偏弱", unknown:"未知"};
const KIND_ICON = {ok:"circle-check", warn:"triangle-alert", crit:"shield-alert", unk:"circle-help", lab:"flask-conical", info:"clock"};
const KIND_ORDER = {crit:0, warn:1, unk:2, lab:3, info:4, ok:5};
const FINDINGS = {
  calendar_unverified:"交易日历未核验", daily_snapshot_missing:"缺少当日收盘快照", candidate_freeze_missing:"缺少候选冻结记录",
  ai_review_missing:"缺少 AI 影子评审", ai_review_terminal_problem:"AI 影子评审异常结束", ai_review_stale_pending:"AI 影子评审长时间未完成",
  recommendation_outcomes_blocked:"推荐结果评估被阻断", recommendation_outcomes_overdue:"推荐结果评估逾期"
};
const REASONS = {
  database_missing:"数据库文件不存在或不可读", database_busy_or_schema_unavailable:"数据库被占用或表结构不可用",
  input_or_source_unavailable:"输入或数据源不可用", route_not_found:"页面路由不存在", stock_unavailable:"没有这只股票的已存数据",
  invalid_stock_code:"股票代码格式无效", research_pool_schema_unavailable:"研究池表或字段不可用", artifact_missing:"盘中报价文件不存在",
  research_not_exposed:"当前 Web 后端版本没有研究池接口；需部署与 main 对应的 Web 后端，不需要重新生成名单",
  no_research_freeze:"研究池表已存在，但还没有任何冻结批次", superseded_by_newer_market_snapshot:"已有更新交易日的行情，这是旧批次研究池",
  old_business_date:"研究池业务日已超过 7 天，属于旧批次", freeze_identity_or_time_unverified:"冻结身份或时间无法核实，按未知处理",
  search_query_invalid:"请输入股票代码或名称", daily_bars_not_collected:"没有这只股票的已存日线（active raw 与旧日线表都没有）",
  no_formal_samples:"暂无正式推荐样本，无法计算；不等于 0%", insufficient_history:"已存日线少于 20 根，均线不完整",
  research_catalog_unavailable:"当前 Web 后端没有研究成果目录接口，或冻结文件未随发布包部署",
  live_market_missing:"没有可用的盘中行情", filters_excluded_all:"当前筛选条件排除了全部候选", snapshot_metadata_missing:"快照元数据缺失",
  daily_acceptance_missing:"尚无每日链路验收记录", daily_acceptance_stale:"链路验收日期与数据日期不一致", no_candidates:"当前有效批次没有可见候选",
  no_records:"当前数据源下没有记录", no_signal_records:"当前 origin 下没有已存信号", bars_unavailable:"缺少可用的未复权 OHLC 数据",
  announcements_unavailable:"公告数据源不可用；不代表无风险", no_announcements:"没有已存公告；不代表无风险",
  no_verified_same_window_benchmark:"缺少同窗口可验证基准", holdout_source_unavailable:"留出期检验没有数据源",
  candidates_unavailable:"候选数据读取失败", health_unavailable:"健康数据读取失败", overview_unavailable:"总览数据读取失败",
  "连接不可用":"无法连接本地只读服务"
};
const reasonText = code => REASONS[code] || (/[\u4e00-\u9fa5]/.test(String(code || "")) ? String(code) : "读取失败，详见原因代码");

function badge(code, map = STATUS) {
  const key = code === null || code === undefined || code === "" ? "unknown" : String(code);
  const [text, kind] = map[key] || STATUS[key] || ["其他状态", "unk"];
  return `<span class="badge ${kind}" title="${esc(key)}">${ic(KIND_ICON[kind])}${esc(text)}</span>`;
}
const kindBadge = (kind, text, title = "") => `<span class="badge ${kind}" title="${esc(title)}">${ic(KIND_ICON[kind])}${esc(text)}</span>`;
const kindOf = (code, map = STATUS) => (map[code] || STATUS[code] || [null, "unk"])[1];
const link = (code, name) => `<a class="stock-link" href="${stockHref(code)}">${esc(name || code)}<small>${esc(code)}</small></a>`;
function empty(title = "暂无可用记录", code = "no_records", steps = [], extra = "") {
  return `<div class="empty"><span class="ei">${ic("inbox")}</span><div><h2>${esc(title)}</h2><p>${esc(reasonText(code))}</p>${steps.length ? `<ol>${steps.map(s => `<li>${esc(s)}</li>`).join("")}</ol>` : ""}<div class="reason">原因代码 ${esc(code)}</div>${extra}</div></div>`;
}
function table(heads, rows, min = 600, opts = {}) {
  if (!rows.length) return opts.empty || empty("暂无记录", "no_records");
  return `<div class="table-wrap"><table class="${opts.cls || ""}" data-min-width="${min}"><thead><tr>${heads.map(h => typeof h === "string" ? `<th>${h}</th>` : `<th class="${h.cls || ""}">${h.t}</th>`).join("")}</tr></thead><tbody>${rows.join("")}</tbody></table></div>`;
}
const panel = (title, body, extra = "") => `<section class="panel"><div class="panel-h"><h2>${title}</h2>${extra}</div>${body}</section>`;
const pb = body => `<div class="panel-b">${body}</div>`;
const kpi = (label, value, sub = "", opts = {}) => `<div class="kpi ${opts.cls || ""}"><div class="lbl">${label}</div><div class="val ${opts.val || ""}">${value}</div><div class="sub">${sub}</div></div>`;
const notice = (kind, icon, body) => `<div class="notice ${kind}">${ic(icon)}<div>${body}</div></div>`;
const titleMap = {overview:"今日总览",signals:"盯盘",intraday:"盯盘",candidates:"候选池",stock:"个股详情",research:"研究观察池",performance:"历史表现",health:"系统健康",settings:"策略设置"};
const defaultFilter = () => ({query:"", risk:"", sort:"desc", board:"all", cheap:false, noSt:false});
const state = {view:"overview", watchSeg:"signals", selectedCode:null, horizon:5, period:60, ma:true, candidates:[], signals:[], performance:null, stock:null, request:0, chartCleanup:null, controller:null, dataRevision:null, snapshotRevision:null, artifactRevision:null, polling:false, meta:null, ctx:{overview:null, health:null, research:null, perf3:null, version:null, catalog:null}, filter:defaultFilter()};

async function api(route, signal) {
  const response = await fetch(`/api/${route}`, {signal, cache:"no-store"});
  if (!response.ok) throw new Error("连接不可用");
  const payload = await response.json();
  if (payload.meta?.status === "unavailable") throw new Error(payload.meta.reason || "数据源不可用");
  return payload;
}
async function soft(route, signal) {
  try { return await api(route, signal); }
  catch (error) { return {meta:{status:"unavailable", reason:error.message || "unavailable"}, data:null}; }
}
const LOOPBACK = ["127.0.0.1", "localhost", "::1", "[::1]"];
const isLoopback = () => LOOPBACK.includes(location.hostname);
const accessText = () => isLoopback() ? "回环访问 · 无鉴权" : `局域网地址 ${location.host} · 无鉴权`;
function isDemo(meta = state.meta) {
  return meta?.dataset_kind === "synthetic_demo" || (meta?.dataset_kind === "intraday_artifact" && state.ctx.overview?.meta?.dataset_kind === "synthetic_demo");
}
function capturedAt(meta) {
  if (!meta) return null;
  if (meta.snapshot?.captured_at) return meta.snapshot.captured_at;
  const times = (meta.sources || []).map(s => s.collected_at).filter(v => toDate(v)).sort((a, b) => toDate(b) - toDate(a));
  return times[0] || null;
}
// The headline date follows the dataset the pages show; the legacy daily_bars table never sets it.
function primaryDate(meta) {
  const p = meta?.primary_source;
  if (p?.date) return p.dataset === "raw_active_generation" ? `${p.date} · active raw 第 ${fmt(p.generation, 0)} 代` : `${p.date} · 日快照`;
  return (meta?.sources || []).find(s => s.date && s.dataset !== "daily_bars")?.date || "未知";
}
function srcSpans(meta = state.meta, extra = []) {
  if (!meta) return `<div class="src"><span>来源 不可用</span><span>数据日 未知</span><span>采集 未知</span></div>`;
  const kind = isDemo(meta) ? "合成数据" : meta.dataset_kind === "intraday_artifact" ? "盘中报价文件" : "本地只读快照";
  return `<div class="src"><span>来源 ${esc(kind)}</span><span>数据日 ${esc(primaryDate(meta))}</span><span>采集 ${esc(bj(capturedAt(meta)))}</span>${extra.map(x => `<span>${esc(x)}</span>`).join("")}</div>`;
}
const DATASET_LABEL = {daily_bars:"daily_bars（旧日线表 · 兼容）", raw_active_generation:"active raw 日线", daily_snapshot_meta:"日快照元数据"};
function snapshotAgeText(s) {
  const c = s.check || {};
  if (s.status === "stale") {
    if (c.status === "recent" && c.result === "unchanged") return `源数据未变化，最后检查 ${bj(c.checked_at)}`;
    if (c.status === "failed") return `最近一次检查失败 ${bj(c.checked_at)}`;
    return c.checked_at ? `快照超过两小时，最后检查 ${bj(c.checked_at)}` : "快照超过两小时，没有检查记录";
  }
  return ["unknown", "unavailable"].includes(s.status) ? "快照采集时间未知" : `快照按 ${s.refresh_interval_seconds || 3600} 秒发布`;
}
const pageHead = (title, sub = "", extra = []) => `<div class="page-head"><div><h1>${esc(title)}</h1>${sub ? `<p>${esc(sub)}</p>` : ""}</div>${srcSpans(state.meta, extra)}</div>`;
function metaDisplay(meta) {
  state.meta = meta;
  const banner = $("#source-banner"), demo = isDemo(meta);
  banner.className = `source-banner ${demo ? "demo" : ""}`;
  banner.textContent = demo ? "合成数据 · DEMO 标的与所有数值均非真实行情 / 非真实业绩" : meta.dataset_kind === "intraday_artifact" ? "盘中报价文件 · 只读" : "本地数据库快照 · 只读";
  $("#database-name").textContent = meta.database || "本地 SQLite";
  const captured = meta.snapshot?.captured_at;
  if (meta.data_revision) state.dataRevision = meta.data_revision;
  if ("snapshot_revision" in meta) state.snapshotRevision = meta.snapshot_revision || null;
  if ("artifact_revision" in meta) state.artifactRevision = meta.artifact_revision || null;
  $("#updated").textContent = captured ? `快照 ${bj(captured)}` : `读取 ${bj(meta.at)}`;
  if (meta.snapshot) {
    const s = meta.snapshot;
    banner.textContent += ` · 每 ${s.poll_interval_seconds || 5} 秒检查更新 · ${snapshotAgeText(s)} · 非实时行情`;
  }
  $("#connection-state").textContent = meta.status === "partial" ? "部分数据不可用" : "快照可读";
  $(".conn-dot").classList.remove("bad");
  const lines = (meta.sources || []).map(s => `<div class="source-line"><strong>${esc(DATASET_LABEL[s.dataset] || s.dataset)}</strong><span>${esc(s.source || "来源未知")}</span><span>业务 ${esc(s.date || "未知")}</span><span>采集 ${esc(bj(s.collected_at))}</span><span>${fmt(s.count, 0)} 条</span>${badge(s.status)}</div>`);
  const notes = (meta.notices || []).map(n => `<div class="source-line"><strong>提示</strong><span class="mono">${esc(n)}</span></div>`);
  $("#source-details-content").innerHTML = [...lines, ...notes].join("") || "来源不可用";
}
function finalize() {
  document.querySelectorAll("table[data-min-width]").forEach(el => {el.style.minWidth = `${el.dataset.minWidth}px`;});
  document.querySelectorAll("[data-width]").forEach(el => {el.style.width = `${Math.max(0, Math.min(100, Number(el.dataset.width)))}%`;});
  hydrateIcons();
}

// Status strip. Rate-limit rows carry deadlines but no success telemetry; an expired deadline is not a pause.
const blockedNow = p => p.blocked_active === true || (p.blocked_active === undefined && toDate(p.blocked_until) > new Date());
const circuitNow = p => p.circuit_active === true;
const rateOnly = p => p.telemetry === "api_rate_limit_state";
function providerKind(p) {
  if (blockedNow(p) || circuitNow(p)) return "warn";
  if (rateOnly(p)) return Number(p.failure_streak) > 0 || p.error ? "warn" : "unk";
  if (p.stale) return "unk";
  const err = toDate(p.error_at), ok = toDate(p.success_at);
  if (err && (!ok || err > ok)) return "warn";
  if (!ok) return "unk";
  if (p.quality && !["good", "ok"].includes(p.quality)) return "warn";
  return "ok";
}
function providerText(p) {
  if (blockedNow(p)) return `${p.name} 接口限流暂停到 ${bj(p.blocked_until)}`;
  if (circuitNow(p)) return `${p.name} 接口熔断到 ${bj(p.circuit_open_until)}`;
  if (rateOnly(p)) return Number(p.failure_streak) > 0 || p.error ? `${p.name} 接口最近连续失败 ${fmt(p.failure_streak, 0)} 次（状态更新于 ${bj(p.state_updated_at)}）` : `${p.name} 只有限流状态，未采集成功记录`;
  if (p.stale) return `${p.name} 旧版遥测，最后记录 ${bj(p.last_activity_at)}，当前插件已不再更新`;
  if (!toDate(p.success_at)) return `${p.name} 接口没有成功记录`;
  return toDate(p.error_at) > toDate(p.success_at) ? `${p.name} 接口最近一次请求失败` : `${p.name} 接口数据质量未达正常`;
}
const SESSION_HINT = {closed_day:"休市", pre_open:"未开盘", call_auction:"集合竞价", trading:"交易中", lunch_break:"午间休市", after_close:"已收盘", unknown:"交易日历未知"};
function sessionBand(live, session = state.ctx.overview?.data?.session || {}) {
  if (live?.status === "available") return "";
  const inSession = session.calendar === "open" && ["trading", "call_auction"].includes(session.phase);
  const [title, detail] = session.calendar === "closed" ? ["休市日，没有实时行情", "交易日历确认今天休市"]
    : inSession ? ["交易时段，但没有可用的盘中行情", "盘中报价未取得或已过期，不是休市"]
    : session.calendar === "open" ? [`${SESSION_HINT[session.phase] || "非交易时段"}，没有实时行情`, "今天是交易日，当前不在连续竞价时段"]
    : ["交易日历未知，无法判断是否休市", "没有今天的已确认交易日历记录"];
  return `<div class="closed-band ${inSession ? "warn" : ""}">${ic("clock")}<div><b>${esc(title)}</b>${esc(detail)} · 原因代码 ${esc(live?.reason || "live_market_missing")}</div></div>`;
}
function dataState() {
  const o = state.ctx.overview, h = state.ctx.health;
  if (!o?.data || o.meta?.status === "unavailable") return {kind:"crit", text:"不可用", reasons:[{kind:"crit", label:"不可用", text:"总览数据读取失败", code:o?.meta?.reason || "overview_unavailable"}]};
  const batches = o.data.batches || [];
  if (!batches.length) return {kind:"unk", text:"未知", reasons:[{kind:"unk", label:"未知", text:"没有原始数据批次记录", code:"raw_batches_missing"}]};
  if (!batches.some(b => b.state === "published")) return {kind:"crit", text:"不可用", reasons:[{kind:"crit", label:"不可用", text:"没有已发布的原始数据批次", code:"raw_generation_unpublished"}]};
  const reasons = [];
  const providers = h?.data?.providers;
  if (!providers) reasons.push({kind:"warn", label:"部分可用", text:"数据接口状态读取失败", code:h?.meta?.reason || "health_unavailable"});
  else providers.forEach(p => {
    const kind = providerKind(p);
    if (kind === "ok" || (kind === "unk" && (rateOnly(p) || p.stale))) return;
    const code = blockedNow(p) ? "rate_limited" : circuitNow(p) ? "breaker_open" : p.error || p.quality || "provider_unverified";
    reasons.push(kind === "warn" ? {kind:"warn", label:"部分可用", text:providerText(p), code} : {kind:"unk", label:"未知", text:providerText(p), code});
  });
  if (o.data.quality && o.data.quality !== "good") reasons.push({kind:"warn", label:"部分可用", text:"最新快照质量未达正常", code:o.data.quality});
  if (o.data.complete === false) reasons.push({kind:"warn", label:"部分可用", text:"快照完整性未确认", code:"snapshot_incomplete"});
  if (reasons.some(r => r.kind === "warn")) return {kind:"warn", text:"部分可用", reasons};
  return reasons.length ? {kind:"unk", text:"未知", reasons} : {kind:"ok", text:"正常", reasons:[{kind:"ok", label:"正常", text:`原始批次已发布 · 数据日 ${o.data.data_date || "未知"}`, code:"published"}]};
}
function chainState() {
  const o = state.ctx.overview?.data, a = o?.acceptance;
  if (!a || !a.status || a.status === "unknown") return {kind:"unk", text:"未知", reasons:[{kind:"unk", label:"未知", text:"尚无每日链路验收记录", code:"daily_acceptance_missing"}]};
  if (a.trade_date && o.data_date && a.trade_date !== o.data_date) return {kind:"unk", text:"未知", reasons:[{kind:"unk", label:"未知", text:`最近验收是 ${a.trade_date}，与数据日 ${o.data_date} 不一致`, code:"daily_acceptance_stale"}]};
  if (a.status === "healthy") return {kind:"ok", text:"正常", reasons:[{kind:"ok", label:"正常", text:`${a.trade_date || "最近交易日"} 链路验收通过`, code:"healthy"}]};
  const findings = a.findings || [], seen = new Set(), reasons = [];
  findings.forEach(f => {
    const code = f.code || f.severity || "unknown";
    if (seen.has(code)) return; seen.add(code);
    const kind = f.severity === "critical" ? "crit" : f.severity === "warning" ? "warn" : "unk";
    reasons.push({kind, label:kind === "crit" ? "严重" : kind === "warn" ? "警告" : "未知", text:FINDINGS[f.code] || f.message || "验收发现异常", code});
  });
  const count = severity => new Set(findings.filter(f => f.severity === severity).map(f => f.code || f.message)).size;
  if (a.status === "critical" || a.status === "warning") {
    const kind = a.status === "critical" ? "crit" : "warn", label = kind === "crit" ? "严重" : "警告", n = count(a.status);
    if (!reasons.length) reasons.push({kind, label, text:a.summary || "链路验收发现异常", code:a.status});
    return {kind, text:n ? `${label} · ${n} 类` : label, reasons};
  }
  return {kind:"unk", text:"未知", reasons:[{kind:"unk", label:"未知", text:"链路验收状态无法识别", code:a.status}]};
}
function setChip(id, label, kind, text, title) {
  const el = $(id);
  if (!el) return;
  el.className = `chip ${kind}`;
  el.title = title;
  el.innerHTML = `${ic(KIND_ICON[kind])}<span class="k">${esc(label)}</span><b>${esc(text)}</b>`;
}
function updateStrip() {
  const data = dataState(), chain = chainState();
  setChip("#chip-data", "数据", data.kind, data.text, data.reasons.map(r => r.code).join(", "));
  setChip("#chip-chain", "链路", chain.kind, chain.text, chain.reasons.map(r => r.code).join(", "));
  setChip("#chip-strategy", "策略", "lab", "未验证", "unverified");
  const live = state.ctx.overview?.data?.live_market, session = state.ctx.overview?.data?.session;
  $("#market-hint").textContent = live?.status === "available" ? `盘中 · ${LIVE_REGIME[live.regime] || "未知"}` : `${SESSION_HINT[session?.phase] || "交易日历未知"} · 无实时行情`;
}
function liveMarketCard(live = {}) {
  const status = live?.status || "unknown", available = status === "available";
  const coverage = finite(live?.coverage) ? `${fmt(Number(live.coverage) * 100, 1)}%` : "—";
  const phase = SESSION_HINT[state.ctx.overview?.data?.session?.phase] || "交易日历未知";
  const sub = available ? `${bj(live.source_timestamp)} · ${live.source || "来源未知"} · ${fmt(live.sample_size, 0)}/${fmt(live.expected_size, 0)} · 覆盖 ${coverage}` : `${status === "pending" ? "盘中状态待确认" : "没有可用的盘中行情"} · ${phase} · 原因代码 ${live?.reason || "live_market_missing"}`;
  const value = available ? esc(LIVE_REGIME[live.regime] || "未知") : status === "pending" ? kindBadge("info", "待确认", "pending") : badge("live_market_missing");
  return `<div class="live-card" id="live-market-card"><div class="lbl">盘中市场状态</div><div class="val">${value}</div><div class="sub">${esc(sub)}</div></div>`;
}
async function refreshOverviewLiveMarket() {
  const payload = await api("overview");
  if (state.view !== "overview") return false;
  state.ctx.overview = payload;
  metaDisplay(payload.meta);
  updateStrip();
  const card = $("#live-market-card");
  if (card) card.outerHTML = liveMarketCard(payload.data.live_market);
  return true;
}
function renderOverview(d) {
  if (d.candidates.length) state.selectedCode ||= d.candidates[0].code;
  const data = dataState(), chain = chainState(), visible = Number(d.candidate_count) > 0;
  const can = chain.kind === "ok" && data.kind !== "crit" && visible;
  const reasons = [...chain.reasons.map(r => ({...r, src:"链路"})), ...data.reasons.map(r => ({...r, src:"数据"}))];
  reasons.push(visible ? {kind:"ok", label:"正常", src:"候选", text:`当前有效批次 ${fmt(d.candidate_count, 0)} 只候选`, code:"candidates_visible"} : {kind:"unk", label:"未知", src:"候选", text:"当前有效批次没有可见候选", code:"no_candidates"});
  reasons.push({kind:"lab", label:"未验证", src:"策略", text:"尚未通过留出期与前瞻检验，名单只用于研究", code:"unverified"});
  reasons.sort((a, b) => KIND_ORDER[a.kind] - KIND_ORDER[b.kind]);
  const verdict = `<section class="verdict ${can ? "ok" : ""}"><span class="vi">${ic(can ? "circle-check" : "ban")}</span><div><h2>${can ? "今天可以生成候选名单" : "今天不生成候选名单"}</h2><ul>${reasons.map(r => `<li>${kindBadge(r.kind, r.label, r.code)}<span>${esc(r.src)}：${esc(r.text)}</span></li>`).join("")}</ul></div><div class="act"><a class="btn primary" href="#health">${ic("triangle-alert")}查看异常</a><a class="btn" href="#research">${ic("flask-conical")}研究进度</a></div></section>`;
  const m = d.market || {}, count = (m.advancing || 0) + (m.declining || 0) + (m.flat || 0);
  const mk = d.coverage_breakdown?.market || {};
  const kpis = `<div class="kpis">${kpi("数据日期", `${esc(d.data_date || "未知")}`, `${weekday(d.data_date)} · 请求日 ${esc(d.requested_date || "未知")}`)}${kpi("行情覆盖", finite(mk.count) ? `${fmt(mk.count, 0)}<small>只</small>` : "—", `${d.complete ? "完整快照标记已记录" : "快照完整性未确认"} · 不代表风险已核实`)}${kpi("涨 / 跌 / 平", count ? `<span class="up">${fmt(m.advancing, 0)}</span><small>/</small><span class="down">${fmt(m.declining, 0)}</span><small>/</small><span class="flat">${fmt(m.flat, 0)}</span>` : "—", `最近收盘样本 ${fmt(count, 0)} 只`, {cls:"span-m"})}${kpi("涨跌幅中位数", `<span class="${tone(m.median_return)}">${pct(m.median_return)}</span>`, "最近收盘 · 未复权")}${kpi("成交额", finite(m.total_amount) ? `${fmt(m.total_amount / 1e8)}<small>亿元</small>` : "—", `收盘记录 ${esc(d.market_date || "未知")}`)}</div>`;
  const w = v => percentWidth(count ? Number(v || 0) / count * 100 : 0);
  const current = Object.keys(CLOSE_REGIME).includes(m.regime) ? m.regime : "unknown";
  const states = Object.entries(CLOSE_REGIME).map(([k, t]) => `<div class="state ${current === k ? "on" : ""}" title="${k}"><b>${t}</b>${current === k ? "当前" : "&nbsp;"}</div>`).join("");
  const market = panel("市场宽度与状态", pb(`<div class="breadth">${count ? `<i class="rise" data-width="${w(m.advancing)}"></i><i class="fall" data-width="${w(m.declining)}"></i><i class="flatbar" data-width="${w(m.flat)}"></i>` : ""}</div><div class="legend"><span>涨 <b>${fmt(m.advancing, 0)}</b></span><span>跌 <b>${fmt(m.declining, 0)}</b></span><span>平 <b>${fmt(m.flat, 0)}</b></span><span>涨跌分档数据未提供，不显示分布柱</span></div><div class="states">${states}</div><div class="regime-note">收盘市场状态 · ${esc(d.market_date || "未知")} · 市场状态只调阈值，不覆盖链路异常</div>${liveMarketCard(d.live_market)}`));
  const top = (d.candidates || []).slice(0, 5).map((c, i) => `<a href="${stockHref(c.code)}" class="mini-item"><span class="mini-rank">${String(i + 1).padStart(2, "0")}</span><span>${esc(c.name)}<small>${esc(c.code)} · ${esc(c.industry || "行业未知")}</small></span><span class="mini-score">${fmt(c.score, 0)}<small>/ ${fmt(c.score_max, 0)}</small></span></a>`).join("");
  const topPanel = panel("排名前列", top ? `<div class="mini-list">${top}</div>` : empty("没有可见候选", "no_candidates"), `<a class="more" href="#candidates">全部候选</a>`);
  const rs = state.ctx.research;
  const research = panel("研究进度", !rs || ["unavailable", "missing"].includes(rs.status) ? empty("研究进度不可用", rs?.reason || "research_pool_schema_unavailable") : `<div class="status-rows"><div class="status-row"><span>冻结状态</span>${badge(rs.status)}</div><div class="status-row"><span>业务日</span><span>${esc(rs.trade_date || "未知")}</span></div><div class="status-row"><span>冻结时间</span><span>${esc(bj(rs.frozen_at))}</span></div><div class="status-row"><span>发布时间</span><span>${esc(bj(rs.published_at))}</span></div></div>`, `<a class="more" href="#research">研究观察池</a>`);
  return pageHead("今日总览") + verdict + kpis + `<div class="grid g-2-1"><div class="col">${coveragePanel(d.coverage_breakdown)}${market}${pipeline(d, chain)}</div><div class="col">${topPanel}${research}</div></div>`;
}
// Each measure keeps its own denominator: complete prices and unknown risk can both be true.
function coveragePanel(cb = {}) {
  const m = cb.market || {}, r = cb.risk || {}, i = cb.indicator || {}, f = cb.formal || {};
  const ratio = (a, b) => !finite(a) || !finite(b) ? "未记录" : Number(b) > 0 ? `${fmt(a, 0)} / ${fmt(b, 0)}（${fmt(a / b * 100, 1)}%）` : `${fmt(a, 0)} / ${fmt(b, 0)}`;
  const indicator = i.status === "not_applicable" ? "不适用：没有股票通过风险门槛" : i.status === "measured" ? ratio(i.enriched, i.targets)
    : i.status === "denominator_unrecorded" && finite(i.coverage) ? `${fmt(Number(i.coverage) * 100, 1)}%（分母未记录）` : "未记录";
  const rows = [
    ["行情", finite(m.count) ? `${fmt(m.count, 0)} 只 · ${m.complete ? "完整快照" : "完整性未确认"}` : "未记录", `数据日 ${m.raw_date || m.date || "未知"}${m.raw_batch_id ? ` · 批次 ${m.raw_batch_id}` : ""}`],
    ["风险四项已知", ratio(r.known, r.total), "停牌、涨停、跌停、ST 四项都明确才算已知；未知不当作安全"],
    ["指标可计算", indicator, "只对通过风险门槛的股票计算技术指标"],
    ["正式通过", finite(f.candidates) ? `${fmt(f.candidates, 0)} 只` : "未记录", `风险门槛通过 ${finite(f.risk_confirmed) ? fmt(f.risk_confirmed, 0) : "未记录"} · 当前可见 ${fmt(f.visible, 0)}`]];
  return panel(`覆盖与门槛 · 最新筛选 ${esc(cb.run_date || "无记录")}`, `<div class="status-rows">${rows.map(([k, v, s]) => `<div class="status-row"><span>${esc(k)}<small>${esc(s)}</small></span><span class="num">${esc(v)}</span></div>`).join("")}</div>`);
}
function pipeline(d, chain) {
  const step = (kind, title, status, desc, time) => `<div class="step"><span class="dot ${kind}"></span><div><div class="t">${esc(title)} ${status}</div><div class="d">${esc(desc)}</div></div><time>${esc(time)}</time></div>`;
  const health = state.ctx.health, h = health?.data, providers = h?.providers;
  const steps = [];
  if (!providers) steps.push(step("unk", "抓取", badge("unknown"), `接口状态读取失败 · 原因代码 ${health?.meta?.reason || "health_unavailable"}`, "—"));
  else {
    const kinds = providers.map(providerKind), bad = kinds.filter(k => k === "warn").length, unknown = kinds.filter(k => k === "unk").length;
    const latest = providers.filter(p => !p.stale).map(p => p.success_at).filter(v => toDate(v)).sort((a, b) => toDate(b) - toDate(a))[0];
    const kind = bad ? "warn" : unknown === providers.length ? "unk" : "ok";
    steps.push(step(kind, "抓取", badge(bad ? "partial" : kind === "unk" ? "unknown" : "ok"), `${providers.length} 个接口/来源 · ${bad ? `${bad} 个暂停中或最近失败` : "没有生效中的暂停或最近失败"}${unknown ? ` · ${unknown} 个没有成功遥测` : ""}`, latest ? bj(latest) : "未知"));
  }
  const pub = (d.batches || []).find(b => b.state === "published");
  const pubAt = (h?.batches || []).find(b => b.state === "published")?.published_at;
  steps.push(pub ? step("ok", "发布", badge("published"), `原始批次 ${pub.date} · 第 ${fmt(pub.generation, 0)} 代 · ${fmt(pub.rows, 0)} 行`, pubAt ? bj(pubAt) : pub.date) : step("crit", "发布", badge("unavailable"), "没有已发布的原始数据批次 · 原因代码 raw_generation_unpublished", "—"));
  const a = d.acceptance || {};
  steps.push(step(chain.kind, "验收", kindBadge(chain.kind, chain.text, a.status || "unknown"), a.summary || chain.reasons[0]?.text || "", a.checked_at ? bj(a.checked_at) : "—"));
  const run = (d.runs || [])[0];
  const JOBS = {automatic_close:"自动收盘筛选"};
  const indicator = d.coverage_breakdown?.indicator?.status === "not_applicable" ? "不适用（无股票通过风险门槛）" : finite(run?.coverage) ? `${fmt(run.coverage * 100, 1)}%` : "未知";
  steps.push(run ? step(kindOf(run.status), "筛选", badge(run.status), `${JOBS[run.job] || "筛选任务"} · ${run.date} · ${fmt(run.count, 0)} 只 · 指标覆盖 ${indicator}${run.error ? ` · 原因代码 ${run.error}` : ""}`, run.date) : step("unk", "筛选", badge("unknown"), "没有筛选任务记录 · 原因代码 screen_run_missing", "—"));
  const outbox = h?.automatic_outbox;
  const total = outbox ? Object.values(outbox).reduce((s, n) => s + Number(n || 0), 0) : 0;
  if (!outbox || !total) steps.push(step("unk", "推送", badge("unknown"), outbox ? "没有自动收盘推送记录" : `推送状态读取失败 · 原因代码 ${health?.meta?.reason || "health_unavailable"}`, "—"));
  else {
    const kind = outbox.failed ? "crit" : outbox.unknown_delivery ? "warn" : (outbox.pending || outbox.sending) ? "info" : "ok";
    const text = {crit:"失败", warn:"投递不明", info:"待发送", ok:"已发送"}[kind];
    steps.push(step(kind, "推送", kindBadge(kind, text), Object.entries(outbox).map(([k, n]) => `${(DELIVERY[k] || ["其他状态"])[0]} ${n}`).join(" · "), "—"));
  }
  return panel("处理链路", pb(`<div class="pipe">${steps.join("")}</div>`));
}
const JOB_NAMES = {automatic_close:"自动收盘筛选"};

// Watch: stored signals and intraday quote artifact.
function watchHead(view) {
  const live = state.ctx.overview?.data?.live_market;
  const seg = `<div class="toolbar bare"><div class="seg" role="group" aria-label="盯盘视图"><a href="#watch/signals" class="${view === "signals" ? "active" : ""}">信号记录</a><a href="#watch/intraday" class="${view === "intraday" ? "active" : ""}">盘中报价</a></div></div>`;
  return seg + sessionBand(live);
}
function signalRows(rows) {
  if (!rows.length) return empty("暂无可见信号", "no_signal_records");
  return `<div class="signal-list">${rows.slice(0, LIMITS.events).map(e => `<article class="signal-row"><div class="signal-name">${link(e.code, e.name)}<small>${esc(SIGNAL[e.signal] || "其他信号")}</small></div><div><div class="signal-values"><span><small>最新已存报价</small>${fmt(e.last_stored_price)} ${badge(e.freshness)}</span><span><small>历史价相对风险区间</small>${pct(e.distance_pct)}</span><span><small>风险区间 / 失效位</small>${fmt(e.attention_low)}–${fmt(e.attention_high)} / ${fmt(e.invalidation)}</span></div><div class="signal-tail">报价证据 ${esc(bj(e.quote_at))} · 市场证据 ${esc(bj(e.market_at))} · 计划 ${esc(e.plan_version || "未知")}</div></div><div class="signal-status">${badge(e.state, DELIVERY)}<span class="mono">状态代码 ${esc(e.state || "unknown")}</span><time>记录 ${esc(bj(e.created_at))}</time>${e.sent_at ? `<time>发送 ${esc(bj(e.sent_at))}</time>` : ""}</div></article>`).join("")}</div>`;
}
function renderSignals(d) {
  state.signals = (d.items || []).slice(0, LIMITS.events);
  return pageHead("盯盘", "", [`信号 ${state.signals.length} / ${LIMITS.events}`]) + watchHead("signals") +
    (!d.origin_configured ? notice("", "lock", "未配置授权 origin，私有盯盘记录不可见。") : "") +
    panel("已存信号与投递状态", `<div class="toolbar"><select id="signal-filter" aria-label="投递状态筛选"><option value="">全部投递状态</option>${Object.entries(DELIVERY).map(([k, [t]]) => `<option value="${k}">${t}</option>`).join("")}</select><span class="count">记录时间不等于当前行情时间</span></div><div id="signal-rows">${signalRows(state.signals)}</div>`);
}
function renderIntraday(d) {
  const quotes = (d.quotes || []).slice(0, LIMITS.candidates);
  const rows = quotes.map(q => `<tr><td>${link(q.code, q.name)}</td><td class="r num">${fmt(q.price)}</td><td class="r num ${tone(q.pct_change)}">${pct(q.pct_change)}</td><td>${esc(bj(q.provider_ts))}</td><td>${esc(bj(q.collected_at))}</td></tr>`);
  return pageHead("盯盘", "", [`报价 ${quotes.length} / ${LIMITS.candidates}`]) + watchHead("intraday") +
    `<div class="kpis k4">${kpi("目标", fmt(d.target_count, 0), "自选、有效候选与重点股并集")}${kpi("返回", fmt(d.returned_count, 0), "本轮报价返回")}${kpi("缺失", fmt(d.missing_count, 0), d.reason ? `原因代码 ${esc(d.reason)}` : "—")}${kpi("文件年龄", finite(d.age_seconds) ? `${fmt(d.age_seconds, 0)}<small>秒</small>` : "未知", `有效期 ${fmt(d.valid_for_seconds, 0)} 秒`)}</div>` +
    notice(kindOf(d.status) === "ok" ? "info" : "", "clock", `报价文件 ${esc(d.artifact_path || "未配置")} · ${badge(d.status)}${d.reason ? ` · 原因代码 ${esc(d.reason)}` : ""}<br>来源 ${esc(d.source && d.source !== "unknown" ? d.source : "未知")} · 报价时间 ${esc((d.provider_ts || []).map(bj).join("、") || "未知")} · 采集 ${esc(bj(d.collected_at))} · 发布 ${esc(bj(d.published_at))}；过期或不确定报价不会标为实时。`) +
    panel("盘中报价", table(["标的", {t:"现价", cls:"r"}, {t:"涨跌", cls:"r"}, "报价时间", "采集时间"], rows, 680, {empty:empty("没有盘中报价", d.reason || "artifact_missing")}));
}

// Candidates.
const boardOf = code => /^(60|00)/.test(code) ? "main" : /^30/.test(code) ? "gem" : /^68/.test(code) ? "star" : "other";
const priceCell = c => finite(c.close) ? `<span title="收盘日 ${esc(c.close_date || "未知")}">${fmt(c.close)}</span>` : finite(c.reference) ? `${fmt(c.reference)}<small>参考价</small>` : "—";
function applyFilter() {
  const f = state.filter, q = f.query.trim().toLowerCase(), dir = f.sort === "asc" ? 1 : -1;
  const rows = state.candidates.map((c, i) => ({c, i})).filter(({c}) =>
    (!f.risk || c.risk_level === f.risk) && (f.board === "all" || boardOf(String(c.code)) === f.board) &&
    (!f.cheap || (finite(c.close) && Number(c.close) > 0 && Number(c.close) <= 50)) && (!f.noSt || !/ST/i.test(String(c.name || ""))) &&
    `${c.code} ${c.name} ${c.industry} ${c.ai_review?.decision || ""}`.toLowerCase().includes(q));
  rows.sort((a, b) => {
    const x = Number(a.c.score), y = Number(b.c.score);
    const diff = Number.isFinite(x) && Number.isFinite(y) ? (x - y) * dir : Number.isFinite(x) ? -1 : Number.isFinite(y) ? 1 : 0;
    return diff || a.i - b.i;
  });
  return rows.map(r => r.c);
}
function d3Cell(c) {
  const recs = state.ctx.perf3?.records;
  if (!recs) return `<span class="muted" title="performance_unavailable">不可用</span>`;
  const r = recs.find(x => x.code === c.code && x.date === c.date);
  if (!r) return `<span class="muted" title="无对应推荐">—</span>`;
  return r.status === "complete" && finite(r.return_pct) ? `<span class="num ${tone(r.return_pct)}">${pct(r.return_pct)}</span>` : badge(r.maturity === "pending" ? "pending" : r.status);
}
function candidateEmpty() {
  if (state.candidates.length) return empty("筛选后没有候选", "filters_excluded_all", ["放宽搜索词或板块", "关闭股价或 ST 筛选", "把风险改回全部"]);
  const chain = chainState();
  return empty("今天没有可见候选", chain.kind !== "ok" ? chain.reasons[0]?.code || "no_candidates" : "no_candidates", ["到系统健康查看链路验收与数据批次", "等待下一次收盘筛选完成后刷新"]);
}
function candidateTable(rows) {
  if (!rows.length) return candidateEmpty();
  const body = rows.map(c => `<tr class="click"><td class="m-name">${link(c.code, c.name)}</td><td class="m-price r num">${priceCell(c)}</td><td class="m-pct r num ${tone(c.pct_change)}">${pct(c.pct_change)}</td><td class="m-score"><span class="score"><i><b data-width="${percentWidth(finite(c.score) && Number(c.score_max) > 0 ? c.score / c.score_max * 100 : 0)}"></b></i><span class="num"><strong>${fmt(c.score, 0)}</strong><small>/ ${fmt(c.score_max, 0)}</small></span></span></td><td class="m-risk">${badge(c.risk_level)}</td><td class="m-hide wrap">${esc(c.industry || "行业未知")}<small>技术 ${fmt(c.technical_score, 0)} · 行业 ${fmt(c.industry_score, 0)} · 基本面 ${fmt(c.fundamental_score, 0)}</small><small>风险区间 ${fmt(c.attention_low)}–${fmt(c.attention_high)} · 建议买入价 ${fmt(c.confirmation)}</small></td><td class="m-hide">${d3Cell(c)}</td></tr>`);
  return table(["股票", {t:"收盘价", cls:"r"}, {t:"涨跌幅", cls:"r"}, "评分", "风险", "理由", "D3 结果"], body, 900, {cls:"mlist"});
}
const sortText = sort => `评分${sort === "asc" ? "从低到高" : "从高到低"}`;
function renderCandidates(d) {
  state.candidates = (d.items || []).slice(0, LIMITS.candidates);
  const f = state.filter, chain = chainState(), rows = applyFilter();
  const opt = (value, text, cur) => `<option value="${value}" ${cur === value ? "selected" : ""}>${text}</option>`;
  const boards = [["all", "全部"], ["main", "主板"], ["gem", "创业板"], ["star", "科创板"]];
  const toolbar = `<div class="toolbar"><label class="search">${ic("search")}<input id="candidate-search" value="${esc(f.query)}" placeholder="代码、名称或行业" aria-label="筛选候选"></label><div class="seg" role="group" aria-label="板块">${boards.map(([k, t]) => `<button type="button" data-board="${k}" class="${f.board === k ? "active" : ""}" aria-pressed="${f.board === k}">${t}</button>`).join("")}</div><button type="button" class="fchip ${f.cheap ? "on" : ""}" data-chip="cheap" aria-pressed="${f.cheap}">股价 0-50</button><button type="button" class="fchip ${f.noSt ? "on" : ""}" data-chip="nost" aria-pressed="${f.noSt}">排除 ST</button><button type="button" class="fchip on" data-sort="score" aria-label="切换评分排序">${ic("arrow-up-down")}<span id="sort-label">${sortText(f.sort)}</span></button><select id="candidate-risk" aria-label="风险筛选">${opt("", "全部风险", f.risk)}${opt("eligible", "可跟踪", f.risk)}${opt("watch_only", "需复核", f.risk)}${opt("blocked", "已拦截", f.risk)}</select><select id="candidate-sort" aria-label="候选排序">${opt("desc", "评分从高到低", f.sort)}${opt("asc", "评分从低到高", f.sort)}</select><span class="count" id="candidate-count">${rows.length} / ${LIMITS.candidates}</span></div>`;
  const warn = chain.kind === "ok" ? "" : notice(chain.kind === "crit" ? "crit" : "", "triangle-alert", `链路${esc(chain.text)}：名单只用于排查，不作为当日候选。原因代码 ${esc(chain.reasons.map(r => r.code).join("、"))}`);
  return pageHead("候选池", "", [`上限 ${LIMITS.candidates}`]) + warn + panel("正式候选 · 当前有效批次", toolbar + `<div id="candidate-table">${candidateTable(rows)}</div>`);
}
function filterCandidates() {
  const f = state.filter;
  f.query = $("#candidate-search")?.value || ""; f.risk = $("#candidate-risk")?.value || ""; f.sort = $("#candidate-sort")?.value || "desc";
  const rows = applyFilter();
  $("#candidate-table").innerHTML = candidateTable(rows);
  $("#candidate-count").textContent = `${rows.length} / ${LIMITS.candidates}`;
  $("#sort-label").textContent = sortText(f.sort);
  finalize();
}

// Research pool (unverified).
function researchRows(rows) {
  const gapText = value => value === "risk_and_intraday_confirmation_not_verified" ? "风险及盘中确认未核验" : value || "—";
  const planText = r => !r.entry_range ? "未验证入场区间" :
    `A ${fmt(r.entry_range.A?.[0])}–${fmt(r.entry_range.A?.[1])}（未观察）<small>B 确认≥${fmt(r.entry_range.B_confirmation)} · 限价≤${fmt(r.entry_range.B_limit)} · 09:35–14:57</small><small>${esc(r.target_condition || "目标未验证")}</small><small>${esc(r.invalidation_condition || "失效条件未验证")}</small>`;
  const entryText = r => r.fill_status === "simulated_fill" && r.paper_status === "unknown"
    ? "模拟入场记录证据待核<small>金额与估值暂不展示</small>"
    : r.fill_status === "simulated_fill" && r.accounting_version === "mark-to-close-entry-cash-v1"
    ? `模拟入场 ${fmt(r.simulated_entry_price)}<small>${esc(r.simulated_entry_date)} · ${fmt(r.simulated_quantity, 0)} 股 · 买入费用 ${fmt(r.entry_fees_cny)} 元 · 投入 ${fmt(r.total_cost_cny)} 元</small><small>仅合成证据；收盘估值不是已卖出或实盘盈亏</small>`
    : r.fill_status === "simulated_fill" ? `旧版模拟入场 ${fmt(r.simulated_entry_price)}<small>${esc(r.simulated_entry_date)} · 旧版往返费用假设 ${fmt(r.round_trip_fee_pct)}%；不与新版收盘估值混算</small>`
    : r.fill_status === "unfilled" ? "模拟未成交<small>无持仓估值</small>" : "未入场<small>缺合格风险、分钟或执行证据</small>";
  const markText = r => [1, 3, 5].map(h => {const m = r.valuation?.[String(h)]; return `<small>D${h} ${badge(m?.status)}${m?.status === "complete" && finite(m.return_pct) ? ` · 收盘估值 ${fmt(m.return_pct, 6)}%` : ""}${m?.reason ? ` · ${esc(m.reason)}` : ""}</small>`;}).join("");
  return table(["研究序号 / 日期", "标的 / 记录", "评分 / 风险", "冻结参考价 / 计划", "资格 / 确认", "模拟状态 / 入场", "D1 / D3 / D5", "版本 / 缺口"],
    rows.map(r => `<tr><td>${esc(r.rank)}<small>${esc(r.data_date || "—")}</small></td><td>${esc(r.name)}<small>${esc(r.code)} · ${esc(r.record_id)}</small></td><td>${fmt(r.score, 0)} · ${badge(r.risk_level)}</td><td class="wrap">${fmt(r.observation_reference_close)}<small>未复权冻结收盘</small>${planText(r)}</td><td>${badge(r.eligibility)}<small>${r.confirmation === "not_assessed" ? "B 未确认" : "B 两根完成柱确认（模拟）"}</small><small>A：未观察，不计未触发</small>${r.entry_qualification_version && r.entry_qualification_version !== r.qualification_version ? "<small>模拟记录绑定成交时资格；当前风险另列</small>" : ""}</td><td class="wrap">${badge(r.paper_status)}<small>${entryText(r)}</small></td><td>${markText(r)}</td><td>${esc(r.protocol_version || "—")}<small>${esc(r.accounting_version || "尚无核算")}</small><small>${esc(gapText(r.missing_reason))}</small></td></tr>`), 1480);
}
function paperHistoryRows(rows) {
  const mark = (r, h) => {const m = r.marks?.[h]; return `<small>D${h} ${badge(m?.status)}${m?.status === "complete" ? ` · ${m.return_kind === "mark_to_close" ? "收盘估值" : "旧版收益"} ${fmt(m.return_kind === "mark_to_close" ? m.return_pct : m.net_return_pct, 6)}%` : ""}</small>`;};
  return table(["记录 / 日期", "模拟状态", "当前资格 / 成交时版本", "费用口径", "估值与缺口"],
    rows.map(r => `<tr><td>${esc(r.record_id)}<small>${esc(r.entry_date || bj(r.confirmed_at))}</small></td><td>${badge(r.fill_status)}<small>仅模拟记录，非真实持仓</small></td><td>${badge(r.current_qualification_state)}<small>${esc(r.entry_qualification_version || "绑定资格未知")}</small></td><td>${esc(r.accounting_version || "旧版未标明")}</td><td>${[1, 3, 5].map(h => mark(r, h)).join("")}</td></tr>`), 900);
}
const GATES = ["超额均值 > 0 且 t ≥ 2", "36 个月中 ≥ 24 个月为正", "超额胜率 > 0", "绝对收益均值 > 0", "2021、2022、2023 每年超额 > 0"];
// Offline studies frozen in docs/research: each list stays on its own, never merged into formal candidates.
function catalogSection(payload = state.ctx.catalog) {
  const data = payload?.data;
  if (!data) return panel("独立研究成果 · 离线冻结文件", empty("研究成果目录不可用", payload?.meta?.reason || "research_catalog_unavailable"));
  const entry = e => {
    const rows = (e.items || []).map(r => `<tr><td>${esc(r.rank ?? "—")}</td><td>${r.code ? link(r.code, r.name) : esc(r.name || "—")}</td><td>${esc(r.sector || "—")}</td><td class="r num">${fmt(r.close)}</td><td class="r num ${tone(r.return5)}">${finite(r.return5) ? pct(Number(r.return5) * 100) : "—"}</td><td class="r num">${finite(r.amount20) ? `${fmt(Number(r.amount20) / 1e8)}<small>亿</small>` : "—"}</td></tr>`);
    const meta = `<div class="status-rows"><div class="status-row"><span>冻结时间 / 输入日</span><span>${esc(bj(e.frozen_at))} · ${esc(e.input_as_of || "未知")}</span></div><div class="status-row"><span>注册提交 / 文件哈希</span><span class="mono">${esc((e.registration_commit || "未知").slice(0, 12))} · ${esc((e.file_sha256 || "").slice(0, 16))}</span></div><div class="status-row"><span>历史检验</span><span>${esc(e.historical_evaluation || e.status || "未记录")}</span></div>${e.next_observation ? `<div class="status-row"><span>下一次观察</span><span>${esc(e.next_observation)}</span></div>` : ""}<div class="status-row"><span>风险口径</span><span>${esc(e.risk_basis || "未记录")}</span></div></div>`;
    const title = `${esc(e.id)} · ${(e.stages || []).map(s => badge(s)).join("")} ${badge("research_only")}`;
    return panel(title, `<div class="lab-band">${ic("flask-conical")}<div><b>${esc(e.label || "研究观察")}</b><br>独立冻结名单，不是正式推荐，未接入插件自动策略，不写入正式候选或推荐表。</div></div>` + meta +
      table(["排名", "标的", "板块", {t:"冻结收盘", cls:"r"}, {t:"5日涨跌", cls:"r"}, {t:"20日成交额", cls:"r"}], rows, 720), `<span class="src">${esc(e.file)}</span>`);
  };
  const missing = (data.unavailable || []).map(u => notice("", "triangle-alert", `${esc(u.file)} 不可用 · 原因代码 ${esc(u.reason)}`)).join("");
  return `<h2 class="section-title">独立研究成果 · 离线冻结文件（只读）</h2>` + missing + (data.entries || []).map(entry).join("");
}
function renderResearch() {
  const rs = state.ctx.research || {status:"unavailable", reason:"research_pool_schema_unavailable"};
  const band = `<div class="lab-band">${ic("flask-conical")}<div><b>未验证 · 不是买入建议</b><br>研究记录独立冻结，不进入历史表现统计。</div></div>`;
  const primary = rs.primary || [], radar = rs.radar || [], history = rs.paper_history || [];
  if (rs.status === "empty") {
    return pageHead("研究观察池") + band + panel("研究观察池", empty("暂无研究冻结", rs.reason || "no_research_freeze", ["收盘后由插件 /观察选股 或自动收盘冻结", "冻结后等待下一次只读快照发布"])) + catalogSection();
  }
  if (["unavailable", "missing"].includes(rs.status) || (!primary.length && !radar.length && !history.length && !rs.trade_date)) {
    const steps = rs.reason === "research_not_exposed" ? ["部署与 main 对应的 Web 后端（含研究池接口）", "部署后到系统健康页核对 Web 版本能力列表"] : ["确认研究冻结任务已在插件侧运行", "确认研究池表已同步到只读快照"];
    return pageHead("研究观察池") + band + panel("研究观察池", empty("不可用", rs.reason || "research_pool_schema_unavailable", steps)) + catalogSection();
  }
  const rule = (k, v) => `<div class="rule"><div class="k">${k}</div><div class="v">${v}</div></div>`;
  const rp = rs.parameters;
  const params = rp ? `深筛前 ${fmt(rp.deep_limit, 0)} · 重点 ${fmt(rp.primary_limit, 0)} · 警戒 ${fmt(rp.radar_limit, 0)} · 价格 ${fmt(rp.price_min)}–${fmt(rp.price_max)} 元<br><small>研究独立固定参数，不读取正式筛选的 ${esc((rs.independent_of || []).join(" / ") || "价格与深筛设置")}</small>` : `<span class="muted">冻结记录未包含参数（旧版本冻结）</span>`;
  const rules = `<section class="panel"><div class="rules">${rule("冻结状态", badge(rs.status))}${rule("业务日", esc(rs.trade_date || "未知"))}${rule("冻结 / 发布", `${esc(bj(rs.frozen_at))}<br>${esc(bj(rs.published_at))}`)}${rule("批次 / 来源", `${esc(rs.batch_id || rs.run_id || "未知")} · ${esc(rs.source || "未知")}`)}</div></section>` +
    (rs.reason ? notice("", "triangle-alert", `${esc(reasonText(rs.reason))} · 原因代码 ${esc(rs.reason)}`) : "") +
    panel(`选股参数 · ${esc(rs.selection_policy || "策略版本未记录")}`, pb(params));
  const gates = panel("留出期检验", `<div class="gates">${GATES.map(g => `<div class="gate"><span>${esc(g)}</span><span>H3 ${badge("unknown")}</span><span>H5 ${badge("unknown")}</span></div>`).join("")}</div>`, `<span class="src">原因代码 holdout_source_unavailable</span>`);
  let left = LIMITS.recommendations;
  const take = arr => {const out = arr.slice(0, Math.max(0, left)); left -= out.length; return out;};
  const p = take(primary), r = take(radar), h = take(history);
  return pageHead("研究观察池", "", [`记录上限 ${LIMITS.recommendations}`]) + band + rules + gates +
    panel(`观察池 · ${p.length}`, researchRows(p)) + panel(`警戒池 · ${r.length}`, researchRows(r)) + panel(`历史模拟记录 · ${h.length}`, paperHistoryRows(h)) + catalogSection();
}

// Stock detail.
const EVIDENCE_FIELDS = {roe:"ROE", profit_growth:"净利润同比", cash_quality:"现金质量", pe:"PE", pb:"PB", st_flag:"ST", audit_flag:"审计风险", suspended:"停牌", delisting_risk:"退市风险"};
function renderAnnouncements(data) {
  const items = (data?.items || []).slice(0, LIMITS.announcements);
  if (!items.length) return empty("没有可验证的公告", data?.status === "unavailable" ? "announcements_unavailable" : "no_announcements");
  return items.map(r => `<article class="announcement"><strong>${esc(r.title || "公告")}</strong><p>${esc(r.quote)}</p><small>${esc(r.source || "来源未知")} · 业务 ${esc(r.business_date || "未知")} · 公告 ${esc(r.announcement_date || "未知")} · 采集 ${esc(bj(r.collected_at))}</small></article>`).join("");
}
// Any code or name can be opened from the daily index; formal candidates are not required.
const stockSearchForm = (value = "") => `<form class="toolbar" id="stock-search-form" role="search"><label class="search">${ic("search")}<input id="stock-search" value="${esc(value)}" maxlength="16" placeholder="代码或名称，如 600857 / 宁波中百" aria-label="查找个股"></label><button type="submit" class="btn">查找</button><span class="count">无正式候选也可查看已存价格与研究记录</span></form><div id="stock-search-results"></div>`;
function renderStockSearch() {
  return pageHead("个股详情") + panel("查找个股", stockSearchForm());
}
async function searchStocks() {
  const input = $("#stock-search"), out = $("#stock-search-results"), q = (input?.value || "").trim();
  if (!out) return;
  if (/^(?:\d{6}|DEMO\d{2})$/i.test(q)) {location.hash = `#stock/${encodeURIComponent(q.toUpperCase())}`; return;}
  try {
    const payload = await api(`search?q=${encodeURIComponent(q)}`);
    const items = payload.data?.items || [];
    const SOURCE = {stock_symbols:"名称索引", formal_candidate:"正式候选", research_pool:"研究池", active_raw:"active raw 日线"};
    out.innerHTML = items.length ? `<div class="search-results">${items.map(r => `<a href="${stockHref(r.code)}"><span>${esc(r.name || "名称未知")} <small class="mono">${esc(r.code)}</small></span><small>${esc(SOURCE[r.source] || "来源未知")}</small></a>`).join("")}</div>` : empty("没有匹配的股票", "no_records");
  } catch (error) {
    out.innerHTML = empty("查找失败", error.message);
  }
  finalize();
}
function stockStatusNotice(d) {
  const s = d.bar_source || {};
  const source = s.kind === "active_raw" ? `active raw 第 ${fmt(s.generation, 0)} 代 · 批次 ${s.batch_id || "未知"} · 截至 ${s.trade_date || "未知"}` : s.kind === "legacy_daily_bars" ? `旧日线表（兼容路径）· 截至 ${s.trade_date || "未知"}` : "没有已存日线";
  const r = d.research;
  const research = r ? `研究池：${r.pool === "primary" ? "重点观察" : "警戒"} 第 ${esc(r.rank)} 名（${esc(r.trade_date)} 冻结，仅研究，不是正式推荐）` : "研究池：最新冻结批次中没有这只股票";
  return notice("info", "database", `行情来源 ${esc(source)}<br>正式身份：${d.formal_status === "formal_candidate" ? "当前有效正式候选" : "不是当前正式候选"} · 推荐记录：${d.recommendation_status === "recorded" ? "有" : "无"}<br>${research}`);
}
function renderStock(d) {
  const bars = (d.bars || []).slice(-LIMITS.bars);
  state.stock = {...d, bars}; state.selectedCode = d.code;
  const c = d.candidate || {}, last = bars[bars.length - 1], prev = bars[bars.length - 2];
  const chg = last && prev && finite(prev.close) && Number(prev.close) !== 0 ? (last.close - prev.close) / prev.close * 100 : null;
  const head = `<div class="stock-head"><a class="icon-btn tip-left" href="#candidates" aria-label="返回候选池" data-tip="返回候选池">${ic("arrow-left")}</a><div><h1>${esc(d.name || d.code)}</h1><small class="muted">${esc(d.code)} · ${esc(c.industry || "行业未知")}</small></div><span class="px">${fmt(d.last_close)}</span><span class="num ${tone(chg)}">${pct(chg)}</span>${badge(c.risk_level)}${srcSpans(state.meta, [`收盘 ${d.bar_date || "未知"}`])}</div>`;
  const ev = d.data_evidence || {};
  const coverage = `<div class="evidence-coverage"><span>技术历史 ${d.technical_history?.bars ?? 0} 根 · ${d.technical_history?.status === "available" ? "可用" : "不足 20 根"}</span><span>财务因子 ${ev.financial_known ?? 0}/${ev.financial_total ?? 5} 已证实</span><span>风险证据 ${ev.risk_known ?? 0}/${ev.risk_total ?? 4} 已证实</span></div>`;
  const tools = `<div class="toolbar"><div class="seg" role="group" aria-label="周期">${[30, 60, 120].map(n => `<button type="button" data-period="${n}" class="${state.period === n ? "active" : ""}">${n} 日</button>`).join("")}</div><label class="checkbox"><input type="checkbox" id="ma-toggle" ${state.ma ? "checked" : ""}>MA5 / MA20</label><span class="count">最多 ${LIMITS.bars} 根 · 未复权</span></div>`;
  const chart = bars.length ? `<div class="chart-wrap"><canvas class="price-chart" role="img" aria-label="已存未复权日K线与成交量"></canvas><div class="chart-tip"></div></div><div class="chart-caption"><span>日 K · 未复权 · 空心红涨 / 实心绿跌</span><span><span class="ma5">MA5</span> · <span class="ma20">MA20</span> · 成交量</span></div>` : empty("K 线不可用", "bars_unavailable");
  const q = d.data_quality || {};
  const quality = notice(kindOf(q.status) === "ok" ? "info" : "", "database", `行情数据 ${badge(q.status)} · 来源时间 ${esc(bj(q.source_timestamp))}${q.missing_reason ? ` · 原因代码 ${esc(q.missing_reason)}` : ""} · 未知不以旧数据代替`);
  const kv = (k, v) => `<div>${k}</div><div>${v}</div>`;
  const keyData = panel("关键数据", `<div class="kv">${kv("综合评分", `${fmt(c.score, 0)} / ${fmt(c.score_max, 0)}`)}${kv("技术 / 行业 / 基本面", `${fmt(c.technical_score, 0)} / ${fmt(c.industry_score, 0)} / ${fmt(c.fundamental_score, 0)}`)}${kv("市场修正", fmt(c.market_adjustment, 0))}${kv("风险区间", `${fmt(c.attention_low)} – ${fmt(c.attention_high)}`)}${kv("建议买入价", fmt(c.confirmation))}${kv("失效", fmt(c.invalidation))}${kv("目标区", `${fmt(c.target_low)} – ${fmt(c.target_high)}`)}${kv("涨跌停价", "未知")}${kv("计划验证", badge(c.plan_validated ? "validated" : "unknown"))}${kv("覆盖率", finite(c.coverage) ? `${fmt(c.coverage * 100, 1)}%` : "—")}${Object.entries(d.indicators || {}).map(([k, v]) => kv(esc(k), fmt(v))).join("")}</div>`);
  const evRows = Object.entries({...(ev.financial || {}), ...(ev.risk || {})}).map(([k, v]) => `<tr><td>${esc(EVIDENCE_FIELDS[k] || k)}</td><td class="num">${v?.value === null || v?.value === undefined ? "未知" : typeof v.value === "boolean" ? (v.value ? "是" : "否") : fmt(v.value)}</td><td>${badge(v?.quality)}</td><td class="wrap">${esc(v?.reason || "—")}</td></tr>`);
  const recs = (d.recommendations || []).slice(0, LIMITS.recommendations).map(r => `<tr><td>${esc(r.date)}</td><td>${badge(r.plan_status)}</td><td>${badge(r.comparability)}</td><td class="mono">${esc(r.plan_version || "—")}</td></tr>`);
  return panel("查找个股", stockSearchForm()) + `<section class="panel">${head}${coverage}${tools}${chart}</section>` + stockStatusNotice(d) + quality +
    `<div class="grid g-2-1"><div class="col">${panel("推荐历史", table(["推荐日期", "计划状态", "可比性", "计划版本"], recs, 520), `<span class="src">最多 ${LIMITS.recommendations} 条</span>`)}${panel("信号历史", signalRows(d.signals || []), `<span class="src">最多 ${LIMITS.events} 条</span>`)}${panel("财务与风险证据", table(["字段", "值", "质量", "缺口"], evRows, 520))}</div><div class="col">${keyData}${panel("相关公告", renderAnnouncements(d.announcements), `<span class="src">最多 ${LIMITS.announcements} 条</span>`)}</div></div>`;
}
function movingAverage(bars, n) {
  const out = []; let sum = 0;
  bars.forEach((b, i) => {sum += Number(b.close); if (i >= n) sum -= Number(bars[i - n].close); out.push(i >= n - 1 ? sum / n : NaN);});
  return out;
}
function drawChart(bars, canvas, ma = true, period = 60) {
  state.chartCleanup?.(); state.chartCleanup = null;
  if (!canvas || !bars.length) return;
  const start = Math.max(0, bars.length - period), rows = bars.slice(start), tip = $(".chart-tip", canvas.parentElement);
  const ma5 = movingAverage(bars, 5).slice(start), ma20 = movingAverage(bars, 20).slice(start);
  const left = 8, right = 52, top = 12, volH = 46, gap = 18, axis = 18;
  let hover = -1;
  function draw() {
    const width = canvas.clientWidth, height = canvas.clientHeight, ratio = devicePixelRatio || 1;
    canvas.width = Math.round(width * ratio); canvas.height = Math.round(height * ratio);
    const ctx = canvas.getContext("2d"); ctx.setTransform(ratio, 0, 0, ratio, 0, 0); ctx.clearRect(0, 0, width, height);
    const plotW = width - left - right, plotH = height - top - volH - gap - axis;
    if (plotW <= 0 || plotH <= 0) return;
    const values = rows.flatMap(r => [r.low, r.high]).concat(ma ? [...ma5, ...ma20].filter(Number.isFinite) : []);
    const low = Math.min(...values), high = Math.max(...values), pad = Math.max((high - low) * .08, .05), min = low - pad, max = high + pad;
    const y = v => top + (max - v) / (max - min) * plotH, step = plotW / rows.length, x = i => left + step * (i + .5);
    const volTop = top + plotH + gap, maxVol = Math.max(...rows.map(r => Number(r.volume) || 0), 1), bw = Math.max(1, Math.min(12, step * .6));
    ctx.font = '10px "Segoe UI",sans-serif'; ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {const v = min + (max - min) * i / 4, yy = Math.round(y(v)) + .5; ctx.strokeStyle = "#eef1f5"; ctx.beginPath(); ctx.moveTo(left, yy); ctx.lineTo(width - right, yy); ctx.stroke(); ctx.fillStyle = "#6a7584"; ctx.textAlign = "left"; ctx.fillText(v.toFixed(2), width - right + 6, yy + 3);}
    rows.forEach((r, i) => {
      const up = r.close >= r.open, color = up ? "#d0393b" : "#17875a", cx = Math.round(x(i)) + .5;
      const bodyTop = Math.min(y(r.open), y(r.close)), bodyH = Math.max(1, Math.abs(y(r.open) - y(r.close)));
      ctx.strokeStyle = color; ctx.fillStyle = color;
      ctx.beginPath(); ctx.moveTo(cx, y(r.high)); ctx.lineTo(cx, y(r.low)); ctx.stroke();
      if (up) {ctx.fillStyle = "#fff"; ctx.fillRect(cx - bw / 2, bodyTop, bw, bodyH); ctx.strokeRect(cx - bw / 2, bodyTop, bw, bodyH);}
      else ctx.fillRect(cx - bw / 2, bodyTop, bw, bodyH);
      const vh = (Number(r.volume) || 0) / maxVol * volH;
      ctx.globalAlpha = .45; ctx.fillStyle = color; ctx.fillRect(cx - bw / 2, volTop + volH - vh, bw, vh); ctx.globalAlpha = 1;
    });
    if (ma) [[ma5, "#2f6bd8"], [ma20, "#c9861a"]].forEach(([series, color]) => {
      ctx.strokeStyle = color; ctx.lineWidth = 1.4; ctx.beginPath(); let started = false;
      series.forEach((v, i) => {if (!Number.isFinite(v)) {started = false; return;} if (!started) {ctx.moveTo(x(i), y(v)); started = true;} else ctx.lineTo(x(i), y(v));});
      ctx.stroke(); ctx.lineWidth = 1;
    });
    ctx.fillStyle = "#6a7584";
    [0, Math.floor(rows.length / 2), rows.length - 1].forEach((i, j) => {ctx.textAlign = j === 0 ? "left" : j === 2 ? "right" : "center"; ctx.fillText(String(rows[i].date).slice(5), j === 0 ? left : j === 2 ? width - right : x(i), height - 4);});
    if (hover >= 0) {ctx.strokeStyle = "#9aa3af"; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(x(hover), top); ctx.lineTo(x(hover), volTop + volH); ctx.stroke(); ctx.setLineDash([]);}
  }
  const move = event => {
    const rect = canvas.getBoundingClientRect(), step = (rect.width - left - right) / rows.length;
    hover = Math.max(0, Math.min(rows.length - 1, Math.floor((event.clientX - rect.left - left) / step)));
    const r = rows[hover];
    tip.style.display = "block";
    tip.innerHTML = `${esc(r.date)}<br>开 ${fmt(r.open)}　高 ${fmt(r.high)}<br>低 ${fmt(r.low)}　收 ${fmt(r.close)}${ma ? `<br>MA5 ${fmt(ma5[hover])}　MA20 ${fmt(ma20[hover])}` : ""}`;
    draw();
  };
  const leave = () => {hover = -1; tip.style.display = "none"; draw();};
  canvas.addEventListener("pointermove", move); canvas.addEventListener("pointerleave", leave);
  const observer = new ResizeObserver(draw); observer.observe(canvas); draw();
  state.chartCleanup = () => {observer.disconnect(); canvas.removeEventListener("pointermove", move); canvas.removeEventListener("pointerleave", leave);};
}

// Performance: mature, pending and unknown stay separate; ratios need enough mature samples.
function renderPerformance(d) {
  state.performance = d;
  const all = d.records || [], recs = all.slice(0, LIMITS.recommendations);
  const pending = all.filter(r => r.maturity === "pending").length, unknown = all.filter(r => !["pending", "mature"].includes(r.maturity)).length;
  const enough = n => Number(n) >= MIN_SAMPLE, gated = (n, html) => enough(n) ? html : `<span class="muted">样本不足</span>`;
  const seg = `<div class="toolbar bare"><div class="seg" role="group" aria-label="持有期">${[1, 3, 5, 10].map(n => `<button type="button" data-horizon="${n}" class="${state.horizon === n ? "active" : ""}">T+${n}</button>`).join("")}</div><span class="count">交易日窗口 · 未复权收盘到收盘 · 不含交易成本</span></div>`;
  const counts = `<div class="kpis k4">${kpi("已到期", fmt(d.mature_count, 0), `价格可评 ${fmt(d.price_evaluable_count, 0)} · 路径可证 ${fmt(d.order_evaluable_count, 0)}`)}${kpi("待到期", fmt(pending, 0), "未到期不进入统计")}${kpi("未知", fmt(unknown, 0), "缺数据或交易日窗口未核验")}${kpi("样本", fmt(d.sample_count, 0), `T+${esc(d.horizon ?? state.horizon)}`)}</div>`;
  const stats = `<div class="kpis k4">${kpi("中位收益", gated(d.price_evaluable_count, `<span class="${tone(d.median_return_pct)}">${pct(d.median_return_pct)}</span>`), `已到期价格完备 ${fmt(d.price_evaluable_count, 0)} 例`)}${kpi("正收益比例", gated(d.price_evaluable_count, pct(d.positive_return_rate, true)), "只用已到期样本")}${kpi("目标触及率", gated(d.target_denominator, pct(d.target_hit_rate, true)), `顺序可证分母 ${fmt(d.target_denominator, 0)}`)}${kpi("基准对比", badge(d.benchmark?.status || "unavailable"), `原因代码 ${esc(d.benchmark?.reason || "no_verified_same_window_benchmark")}`, {val:"sm"})}</div>`;
  const legend = "待到期 = 还没到评估日；未知 = 缺价格、交易日历或可比性证据；顺序未知 = 同一日线同时触及目标和失效，无法判断先后。";
  const gateNote = !Number(d.sample_count) ? notice("info", "inbox", `暂无正式推荐样本，无法计算收益、正收益比例或触及率；这不等于 0%。正式名单为空时不会产生样本。<br>${legend}`)
    : enough(d.price_evaluable_count) ? notice("info", "clock", legend) : notice("info", "clock", `已到期可评样本少于 ${MIN_SAMPLE} 例，比例与收益显示为样本不足；${MIN_SAMPLE} 是展示门槛，不是统计检验。<br>${legend}`);
  const AI_LABEL = {all:"规则全集", keep:"AI 保留", watch:"AI 观察", veto:"AI 否决"}, ai = d.ai_groups || {};
  const aiRows = Object.entries(AI_LABEL).map(([k, t]) => {const g = ai[k] || {}, ok = enough(g.price_evaluable_count); return `<tr><td>${t}</td><td class="r num">${fmt(g.sample_count, 0)}</td><td class="r num">${fmt(g.price_evaluable_count, 0)}</td><td class="r num">${ok ? pct(g.positive_return_rate, true) : "样本不足"}</td><td class="r num">${ok ? pct(g.median_return_pct) : "样本不足"}</td></tr>`;});
  const statusBar = `<div class="toolbar">${Object.entries(d.status_counts || {}).map(([s, n]) => `${badge(s)}<span class="muted num">${fmt(n, 0)}</span>`).join("") || `<span class="muted">没有状态计数</span>`}<span class="count">失效率 ${enough(d.invalidation_denominator) ? pct(d.invalidation_rate, true) : "样本不足"} · 分母 ${fmt(d.invalidation_denominator, 0)}</span></div>`;
  const rows = recs.map(r => `<tr><td>${link(r.code, r.name)}<small>${esc(r.date)}</small></td><td>${badge(r.maturity)}</td><td>${badge(r.status)}</td><td>${badge(r.ai_decision, AI_DECISION)}</td><td class="r num ${tone(r.return_pct)}">${pct(r.return_pct)}</td><td class="r num">${pct(r.max_gain_pct)}</td><td class="r num ${tone(r.drawdown_pct)}">${pct(r.drawdown_pct)}</td><td class="mono">${esc(r.reason || "—")}</td></tr>`);
  return pageHead("历史表现", "", [`记录上限 ${LIMITS.recommendations}`]) + seg + counts + stats + gateNote +
    panel("AI 影子评审分组", table(["分组", {t:"样本", cls:"r"}, {t:"价格可评", cls:"r"}, {t:"正收益比例", cls:"r"}, {t:"中位收益", cls:"r"}], aiRows, 560)) +
    panel(`推荐记录 · ${recs.length}`, statusBar + table(["推荐日期 / 标的", "到期", "评估状态", "AI 评审", {t:"收益", cls:"r"}, {t:"最大浮盈", cls:"r"}, {t:"收盘回撤", cls:"r"}, "原因代码"], rows, 1000));
}

function renderHealth(d) {
  const loop = isLoopback(), chain = chainState(), live = state.ctx.overview?.data?.live_market, acc = (d.daily_acceptance || [])[0];
  const sec = `<div class="secnote ${loop ? "ok" : ""}">${ic(loop ? "lock" : "shield-alert")}<div><b>${esc(accessText())}</b><br>${loop ? "仅本机回环可访问；远程查看请走 SSH 隧道。" : "当前地址可被局域网访问且没有鉴权，建议改为回环绑定并通过 SSH 隧道访问。"}</div></div>`;
  const cell = (k, v, s) => `<div><div class="k">${k}</div><div class="v">${v}</div><div class="s">${s}</div></div>`;
  const check = state.meta?.snapshot?.check || {}, CHECK = {published:"已发布新快照", unchanged:"源未变化", failed:"检查失败"};
  const checkText = check.checked_at ? `快照检查 ${bj(check.checked_at)} · ${CHECK[check.result] || "结果未知"}` : "快照检查 无记录";
  const v = state.ctx.version?.data, build = v?.build || {};
  const webValue = !v ? kindBadge("unk", "未知", "version_unavailable") : build.status === "recorded" ? kindBadge("ok", build.revision || build.release, build.release || "") : kindBadge("unk", "构建未记录", "build_info_missing");
  const webSub = !v ? "后端没有版本接口（旧版） · 原因代码 version_unavailable"
    : `API v${esc(v.api_version)} · ${fmt((v.capabilities || []).length, 0)} 项能力 · 库 schema ${esc(v.database_schema_version ?? "未知")}${(v.capabilities || []).includes("research_pools") ? "" : " · 缺研究池能力"}`;
  const session = state.ctx.overview?.data?.session || {};
  const svc = `<section class="panel"><div class="svc">${cell("数据库", `${badge(d.database)}${badge(d.integrity)}`, `${fmt(d.tables, 0)} 张表 · ${esc(checkText)}`)}${cell("Web 版本", webValue, webSub)}${cell("链路验收", kindBadge(chain.kind, chain.text), esc(acc ? `${acc.date} · 检查 ${bj(acc.checked_at)}` : chain.reasons[0]?.text || ""))}${cell("盘中行情", live?.status === "available" ? badge("available") : badge("live_market_missing"), `${esc(SESSION_HINT[session.phase] || "交易日历未知")} · 原因代码 ${esc(live?.reason || "live_market_missing")}`)}</div></section>`;
  const PK = {ok:"正常", warn:"部分可用", unk:"未知"}, TELEMETRY = {provider_health:"成功/失败遥测", api_rate_limit_state:"限流状态"};
  const deadline = p => p.blocked_until ? `${bj(p.blocked_until)}${blockedNow(p) ? "（生效中）" : "（已过期）"}` : p.circuit_open_until ? `熔断 ${bj(p.circuit_open_until)}${circuitNow(p) ? "（生效中）" : "（已过期）"}` : "—";
  const prov = (d.providers || []).map(p => {const k = providerKind(p), rate = rateOnly(p); return `<tr><td>${esc(p.name)}<small>${esc(TELEMETRY[p.telemetry] || "来源未知")}${p.stale ? esc(`（旧版，最后记录 ${bj(p.last_activity_at)}，已停止更新）`) : ""}</small></td><td>${kindBadge(k, PK[k], p.quality || "")}</td><td>${rate ? `<span class="muted">未采集</span>` : esc(bj(p.success_at))}</td><td>${esc(p.error_at ? bj(p.error_at) : "—")}</td><td>${rate ? `<span class="muted">不适用</span>` : badge(p.quality)}</td><td class="mono">${esc(p.error || (Number(p.failure_streak) > 0 ? `连续失败 ${p.failure_streak}` : "—"))}</td><td>${esc(deadline(p))}</td><td>${esc(p.state_updated_at ? bj(p.state_updated_at) : "—")}</td></tr>`;});
  const batches = (d.batches || []).map(b => `<tr><td class="mono">${esc(b.id)}</td><td>${esc(b.date)}</td><td class="r num">${fmt(b.generation, 0)}</td><td>${badge(b.state)}</td><td class="r num">${fmt(b.rows, 0)}</td><td>${esc(b.basis === "unadjusted" ? "未复权" : b.basis || "未知")}</td><td>${esc(bj(b.published_at))}</td></tr>`);
  // Older records stay visible but folded, so a past failure is not read as today's outage.
  const current = d.data_date || acc?.date;
  const split = (rows, key) => {const now = rows.filter(r => !current || !r[key] || r[key] >= current); return [now, rows.filter(r => !now.includes(r))];};
  const history = (heads, rows, min) => rows.length ? `<details class="history"><summary>更早记录 ${rows.length} 条 · 保留原始证据，不代表当前问题</summary>${table(heads, rows, min)}</details>` : "";
  const accHeads = ["交易日", "检查时间", "状态", "摘要"], jobHeads = ["日期", "任务", "状态", "错误代码"];
  const accRow = r => `<tr><td>${esc(r.date)}</td><td>${esc(bj(r.checked_at))}</td><td>${badge(r.status)}</td><td class="wrap">${esc(r.summary || "—")}</td></tr>`;
  const jobRow = r => `<tr><td>${esc(r.date)}</td><td>${esc(JOB_NAMES[r.name] || r.name)}</td><td>${badge(r.state)}</td><td class="mono">${esc(r.error || "—")}</td></tr>`;
  const [accNow, accOld] = split(d.daily_acceptance || [], "date"), [jobsNow, jobsOld] = split(d.jobs || [], "date");
  const fails = (d.failures || []).map(r => `<tr><td>${esc(r.code)}</td><td>${badge(r.state)}</td><td>${badge(r.risk)}</td><td>${esc(bj(r.at))}</td></tr>`);
  const outbox = (title, o) => panel(title, Object.keys(o || {}).length ? `<div class="status-rows">${Object.entries(o).map(([s, n]) => `<div class="status-row">${badge(s, DELIVERY)}<span class="num">${fmt(n, 0)}</span></div>`).join("")}</div>` : empty("没有投递记录", "no_records"));
  return pageHead("系统健康", "", ["持久状态快照，不代表远端服务探活"]) + sec + svc +
    panel("数据源接口", table(["接口 / 遥测", "状态", "最近成功", "最近错误", "质量记录", "错误", "暂停 / 熔断到", "状态更新"], prov, 980)) +
    panel("原始数据批次", table(["批次", "数据日期", {t:"代", cls:"r"}, "状态", {t:"行数", cls:"r"}, "口径", "发布时间"], batches, 760)) +
    panel(`每日链路验收 · 当前数据日 ${esc(current || "未知")}`, table(accHeads, accNow.map(accRow), 640, {empty:empty("当前数据日没有验收记录", "daily_acceptance_missing")}) + history(accHeads, accOld.map(accRow), 640)) +
    `<div class="grid g-1-1"><div class="col">${panel(`任务 · 当前数据日 ${esc(current || "未知")}`, table(jobHeads, jobsNow.map(jobRow), 520, {empty:empty("当前数据日没有任务记录", "no_records")}) + history(jobHeads, jobsOld.map(jobRow), 520))}${panel("失败与风险标的", table(["标的", "状态", "风险", "时间"], fails, 480))}</div><div class="col">${outbox("盘中信号投递", d.outbox)}${outbox("自动收盘推送", d.automatic_outbox)}${outbox("每日验收推送", d.daily_acceptance_outbox)}</div></div>`;
}
function renderSettings(d) {
  const names = {min_score:"最低技术分", price_min:"最低价格", price_max:"最高价格", deep_screen_limit:"技术深筛上限", factor_screen_limit:"因子筛选上限", screen_min_indicator_coverage:"最低指标覆盖", intraday_confirmation_periods:"连续确认次数", intraday_cooldown_seconds:"信号冷却（秒）", intraday_min_amount:"最低成交额", market_comparison_enabled:"量价对照", market_comparison_benchmark:"指定基准指数", paper_trading_only:"仅研究 / 模拟", price_plan_close_tolerance_pct:"收盘计划偏差容限", official_evidence_enabled:"官方证据核验", official_evidence_candidate_limit:"官方证据候选上限", official_evidence_cache_seconds:"官方证据缓存（秒）"};
  const value = v => typeof v === "boolean" ? (v ? "开启" : "关闭") : v === null || v === undefined || v === "" ? `<span class="muted">未知</span>` : esc(v);
  const rows = (d.items || []).map(r => `<tr><td>${esc(names[r.key] || r.key)}<small class="mono">${esc(r.key)}</small></td><td class="num">${value(r.effective)}</td><td class="num">${value(r.default)}</td><td class="wrap">${esc(r.label || "—")}<small>${r.source === "explicit_public_snapshot" ? "显式公开配置快照" : "有效配置未提供"}</small></td></tr>`);
  return pageHead("策略设置", "", ["只读"]) + panel("参数", table(["参数", "当前值", "默认值", "说明"], rows, 720)) + notice("info", "lock", "敏感字段未暴露。默认值不代表正在运行的插件配置。");
}

// Navigation.
const VIEWS = ["overview", "candidates", "stock", "research", "performance", "health", "settings"];
function route() {
  const parts = location.hash.slice(1).split("/"), head = parts[0] || "overview";
  if (head === "signals" || head === "intraday") {history.replaceState(null, "", `#watch/${head}`); return {view:head, parts:["watch", head]};}
  if (head === "watch") {const seg = parts[1] === "intraday" ? "intraday" : "signals"; if (parts[1] !== seg) history.replaceState(null, "", `#watch/${seg}`); return {view:seg, parts:["watch", seg]};}
  return {view:VIEWS.includes(head) ? head : "overview", parts};
}
const MOBILE = matchMedia("(max-width:820px)");
function syncNav(view) {
  if (view === "signals" || view === "intraday") state.watchSeg = view;
  document.querySelectorAll("[data-watch]").forEach(a => {a.dataset.view = state.watchSeg; a.setAttribute("href", `#watch/${state.watchSeg}`);});
  $("#tab-more").dataset.view = ["stock", "performance", "health", "settings"].includes(view) ? view : "more";
  document.querySelectorAll(".side-nav a[data-view], .tabbar a[data-view]").forEach(a => {a.classList.remove("active"); a.removeAttribute("aria-current");});
  document.querySelectorAll(`${MOBILE.matches ? ".tabbar" : ".side-nav"} a[data-view]`).forEach(a => {if (a.dataset.view === view) {a.classList.add("active"); a.setAttribute("aria-current", "page");}});
}
function openSheet() {$("#sheet").classList.add("open"); $("#sheet-mask").classList.add("open"); $("#sheet-close").focus();}
function closeSheet() {$("#sheet").classList.remove("open"); $("#sheet-mask").classList.remove("open");}
async function contextJobs(view, signal) {
  const jobs = {};
  if (view !== "overview") jobs.overview = soft("overview", signal);
  if (view !== "health") jobs.health = soft("health", signal);
  if (view === "health") jobs.version = soft("version", signal);
  if (view === "research") jobs.catalog = soft("research_catalog", signal);
  if (view === "overview") jobs.candidates = soft("candidates", signal);
  if (view === "candidates") jobs.perf3 = soft("performance?horizon=3", signal);
  const keys = Object.keys(jobs), values = await Promise.all(Object.values(jobs));
  return Object.fromEntries(keys.map((k, i) => [k, values[i]]));
}
function applyContext(view, payload, ctx) {
  const researchOf = p => p?.data && "research" in p.data ? p.data.research : p?.data ? {status:"unavailable", reason:"research_not_exposed"} : {status:"unavailable", reason:p?.meta?.reason || "candidates_unavailable"};
  if (view === "overview" && payload) state.ctx.overview = payload; else if (ctx.overview) state.ctx.overview = ctx.overview;
  if (view === "health" && payload) state.ctx.health = payload; else if (ctx.health) state.ctx.health = ctx.health;
  if ((view === "candidates" || view === "research") && payload) state.ctx.research = researchOf(payload);
  else if (ctx.candidates) state.ctx.research = researchOf(ctx.candidates);
  if (ctx.perf3) state.ctx.perf3 = ctx.perf3.data || null;
  if (ctx.version) state.ctx.version = ctx.version.data ? ctx.version : null;
  if (ctx.catalog) state.ctx.catalog = ctx.catalog;
}

async function load({silent=false} = {}) {
  const {view, parts} = route();
  state.view=view; const request=++state.request;
  state.controller?.abort();state.controller=new AbortController();const signal=state.controller.signal;
  syncNav(view); closeSheet();
  $("#breadcrumb").textContent=titleMap[view];document.title=`${titleMap[view]} · Stock Watch`;
  if(!silent){
    state.chartCleanup?.();state.chartCleanup=null;
    if(view==="candidates")state.filter=defaultFilter();
    $("#refresh").disabled=true;$("#content").innerHTML=`<div class="loading">正在读取快照…</div>`;
  }
  const ctxJob=contextJobs(view,signal);
  try {
    let path=view==="research"?"candidates":view;
    if(view==="performance")path+=`?horizon=${state.horizon}`;
    if(view==="stock"){
      let code=parts[1] || state.selectedCode;
      if(!code){const choices=await soft("candidates",signal);code=choices.data?.items?.[0]?.code;}
      if(!code){
        const ctx=await ctxJob;
        if(request!==state.request)return false;
        applyContext(view,null,ctx);
        if(ctx.overview?.meta && ctx.overview.meta.status!=="unavailable")metaDisplay(ctx.overview.meta);
        updateStrip();
        $("#content").innerHTML=renderStockSearch();finalize();
        return true;
      }
      path=`stocks/${encodeURIComponent(code)}`;
    }
    const payload=await api(path,signal);
    if(request!==state.request)return false;
    const ctx=await ctxJob;
    if(request!==state.request)return false;
    applyContext(view,payload,ctx);
    metaDisplay(payload.meta);
    updateStrip();
    const renderers={overview:renderOverview,candidates:renderCandidates,signals:renderSignals,intraday:renderIntraday,stock:renderStock,research:renderResearch,performance:renderPerformance,health:renderHealth,settings:renderSettings};
    state.chartCleanup?.();state.chartCleanup=null;
    $("#content").innerHTML=renderers[view](payload.data);finalize();
    if(view==="stock")drawChart(state.stock.bars,$(".price-chart"),state.ma,state.period);
    return true;
  } catch(error) {
    if(error.name==="AbortError" || request!==state.request)return false;
    if(silent)return false;
    const ctx=await ctxJob;
    if(request!==state.request)return false;
    const failed={meta:{status:"unavailable",reason:error.message},data:null};
    applyContext(view,null,ctx);
    if(view==="overview")state.ctx.overview=failed;
    if(view==="health")state.ctx.health=failed;
    state.meta=null;
    updateStrip();
    $("#content").innerHTML=empty("数据不可用",error.message);
    $("#content").insertAdjacentHTML("afterbegin",pageHead(titleMap[view]));
    $("#source-details-content").textContent="来源不可用";
    $("#source-banner").className="source-banner unavailable";$("#source-banner").textContent="数据不可用 · 未显示旧快照或推断结果";
    $("#connection-state").textContent="数据不可用";$(".conn-dot").classList.add("bad");
    $("#updated").textContent="快照 未知";
    return false;
  } finally {if(!silent && request===state.request)$("#refresh").disabled=false;}
}
document.addEventListener("input",event=>{if(event.target.id==="candidate-search")filterCandidates();});
document.addEventListener("submit",event=>{if(event.target.id==="stock-search-form"){event.preventDefault();searchStocks();}});
document.addEventListener("change",event=>{
  if(["candidate-risk","candidate-sort"].includes(event.target.id))filterCandidates();
  if(event.target.id==="signal-filter")$("#signal-rows").innerHTML=signalRows(state.signals.filter(r=>!event.target.value || r.state===event.target.value));
  if(event.target.id==="ma-toggle" && state.stock){state.ma=event.target.checked;drawChart(state.stock.bars,$(".price-chart"),state.ma,state.period);}
});
document.addEventListener("click",event=>{
  const t=event.target;
  const horizon=t.closest("[data-horizon]"), period=t.closest("[data-period]"), sort=t.closest("[data-sort]"), board=t.closest("[data-board]"), chip=t.closest("[data-chip]"), row=t.closest("tr.click");
  if(horizon){state.horizon=Number(horizon.dataset.horizon);load();return;}
  if(period && state.stock){state.period=Number(period.dataset.period);document.querySelectorAll("[data-period]").forEach(b=>b.classList.toggle("active",Number(b.dataset.period)===state.period));drawChart(state.stock.bars,$(".price-chart"),state.ma,state.period);return;}
  if(sort){const s=$("#candidate-sort");s.value=s.value==="asc"?"desc":"asc";filterCandidates();return;}
  if(board){state.filter.board=board.dataset.board;document.querySelectorAll("[data-board]").forEach(b=>{b.classList.toggle("active",b===board);b.setAttribute("aria-pressed",String(b===board));});filterCandidates();return;}
  if(chip){const key=chip.dataset.chip==="cheap"?"cheap":"noSt";state.filter[key]=!state.filter[key];chip.classList.toggle("on",state.filter[key]);chip.setAttribute("aria-pressed",String(state.filter[key]));filterCandidates();return;}
  if(row && !t.closest("a")){const a=row.querySelector("a[href]");if(a)location.hash=a.getAttribute("href");}
});
hydrateIcons();
$("#access-hint").innerHTML=`${ic(isLoopback()?"lock":"shield-alert")}<span>${esc(accessText())}</span>`;
$("#tab-more").addEventListener("click",event=>{event.preventDefault();openSheet();});
$("#sheet-mask").addEventListener("click",closeSheet);
$("#sheet-close").addEventListener("click",closeSheet);
$("#sheet").addEventListener("click",event=>{if(event.target.closest("a"))closeSheet();});
document.addEventListener("keydown",event=>{if(event.key==="Escape")closeSheet();});
MOBILE.addEventListener("change",()=>syncNav(state.view));
$("#refresh").addEventListener("click",load);
window.addEventListener("hashchange",load);
setInterval(async()=>{
  if(document.hidden || $("#refresh").disabled || state.polling) return;
  state.polling = true;
  try {
    const payload = await api("revision");
    const revision = payload.data || {};
    const dataRevision = revision.data_revision || null;
    const snapshotRevision = revision.snapshot_revision || null;
    const artifactRevision = revision.artifact_revision || null;
    const snapshotChanged = snapshotRevision !== state.snapshotRevision && (snapshotRevision !== null || state.snapshotRevision !== null);
    const artifactChanged = artifactRevision !== state.artifactRevision && (artifactRevision !== null || state.artifactRevision !== null);
    const dataChanged = dataRevision !== state.dataRevision && (dataRevision !== null || state.dataRevision !== null);
    const shouldReloadIntraday = artifactChanged && state.view === "intraday";
    if(snapshotChanged || shouldReloadIntraday || (dataChanged && state.view === "intraday")) {
      if(await load({silent:true})) {
        state.dataRevision = dataRevision || state.dataRevision;
        state.snapshotRevision = snapshotRevision;
        state.artifactRevision = artifactRevision;
      }
    } else if ((artifactChanged || dataChanged) && state.view === "overview") {
      if(await refreshOverviewLiveMarket()) {
        state.dataRevision = dataRevision || state.dataRevision;
        state.snapshotRevision = snapshotRevision;
        state.artifactRevision = artifactRevision;
      }
    }
    else {
      state.dataRevision = dataRevision || state.dataRevision;
      state.snapshotRevision = snapshotRevision;
      state.artifactRevision = artifactRevision;
    }
  } catch (_) {
    // Keep the last rendered state when the lightweight probe is unavailable.
  } finally { state.polling = false; }
},5000);
load();
