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
  "circle-plus": '<circle cx="12" cy="12" r="10"/><path d="M8 12h8"/><path d="M12 8v8"/>',
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
  critical:["严重","crit"], unavailable:["不可用","crit"], missing:["不可用","crit"], failed:["失败","crit"], blocked:["已拦截","crit"], missed:["已终止","crit"],
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
const reasonText = code => REASONS[code] || FINDINGS[code] || (/[\u4e00-\u9fa5]/.test(String(code || "")) ? String(code) : "读取失败，详见原因代码");
// What a daily-acceptance finding means for the user and where to look next.
const FINDING_HELP = {
  calendar_unverified:["无法确认当天是否交易日，当天验收无法判定", "查看系统健康里的交易日历接口是否限流或失败"],
  daily_snapshot_missing:["当天没有完整收盘快照，筛选和验收缺少输入", "查看原始数据批次是否已发布、日线接口是否失败"],
  candidate_freeze_missing:["当天没有冻结正式候选，正式名单为空", "查看任务里自动收盘筛选的错误代码；四项风险证据未通过验收时属于预期结果"],
  ai_review_missing:["AI 影子评审没有生成，只影响研究对照，不影响规则候选", "检查模型配置和每日请求上限"],
  ai_review_terminal_problem:["AI 影子评审异常结束，只影响研究对照", "查看插件日志里的模型请求错误"],
  ai_review_stale_pending:["AI 影子评审长时间未完成，只影响研究对照", "检查模型接口是否超时"],
  recommendation_outcomes_blocked:["推荐结果评估被阻断，历史表现无法更新", "检查复权因子与交易日历证据"],
  recommendation_outcomes_overdue:["推荐结果评估逾期，历史表现无法更新", "检查到期日行情是否已采集"]
};
function findingHelp(findings = []) {
  const codes = [...new Set(findings.map(f => f.code).filter(code => FINDING_HELP[code]))];
  return codes.length ? `<div class="status-rows">${codes.map(code => `<div class="status-row"><span>${esc(FINDINGS[code] || code)}<small>影响：${esc(FINDING_HELP[code][0])}</small><small>下一步：${esc(FINDING_HELP[code][1])}</small></span><span class="mono">${esc(code)}</span></div>`).join("")}</div>` : "";
}

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
const state = {view:"overview", watchSeg:"signals", selectedCode:null, horizon:5, period:60, ma:true, candidates:[], signals:[], performance:null, stock:null, request:0, chartCleanup:null, controller:null, dataRevision:null, snapshotRevision:null, artifactRevision:null, polling:false, meta:null, ctx:{overview:null, health:null, research:null, perf3:null, version:null, catalog:null, signals:null}, filter:defaultFilter()};

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
  const top = (d.candidates || []).slice(0, 5).map((c, i) => `<a href="${stockHref(c.code)}" class="mini-item"><span class="mini-rank">${String(i + 1).padStart(2, "0")}</span><span>${esc(c.name)}${hotBadge(c.code)}<small>${esc(c.code)} · ${esc(c.industry || "行业未知")}</small></span><span class="mini-score">${fmt(c.score, 0)}<small>/ ${fmt(c.score_max, 0)}</small></span></a>`).join("");
  const topPanel = panel("排名前列", top ? `<div class="mini-list">${top}</div>` : empty("没有可见候选", "no_candidates"), `<a class="more" href="#candidates">全部候选</a>`);
  const rs = state.ctx.research;
  const research = panel("研究进度", !rs || ["unavailable", "missing"].includes(rs.status) ? empty("研究进度不可用", rs?.reason || "research_pool_schema_unavailable") : `<div class="status-rows"><div class="status-row"><span>冻结状态</span>${badge(rs.status)}</div><div class="status-row"><span>业务日</span><span>${esc(rs.trade_date || "未知")}</span></div><div class="status-row"><span>冻结时间</span><span>${esc(bj(rs.frozen_at))}</span></div><div class="status-row"><span>发布时间</span><span>${esc(bj(rs.published_at))}</span></div></div>`, `<a class="more" href="#research">研究观察池</a>`);
  return pageHead("今日总览") + verdict + kpis + `<div class="grid g-2-1"><div class="col">${coveragePanel(d.coverage_breakdown)}${market}${pipeline(d, chain)}</div><div class="col">${topPanel}${research}${indexTrendPanel()}</div></div>`;
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
const JOB_NAMES = {automatic_close:"自动收盘筛选", daily_screen:"收盘筛选"};

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
  const chain = chainState(), rs = state.ctx.research;
  const research = rs && (rs.primary || []).length + (rs.radar || []).length ? `<p><a class="btn" href="#research">${ic("flask-conical")}查看研究观察池（重点 ${(rs.primary || []).length} / 警戒 ${(rs.radar || []).length}，仅研究）</a></p>` : "";
  return empty("今天没有可见候选", chain.kind !== "ok" ? chain.reasons[0]?.code || "no_candidates" : "no_candidates", ["到系统健康查看链路验收与数据批次", "等待下一次收盘筛选完成后刷新"], research);
}
function candidateTable(rows) {
  if (!rows.length) return candidateEmpty();
  const body = rows.map(c => `<tr class="click"><td class="m-name">${link(c.code, c.name)}${hotBadge(c.code)}</td><td class="m-price r num">${priceCell(c)}</td><td class="m-pct r num ${tone(c.pct_change)}">${pct(c.pct_change)}</td><td class="m-score"><span class="score"><i><b data-width="${percentWidth(finite(c.score) && Number(c.score_max) > 0 ? c.score / c.score_max * 100 : 0)}"></b></i><span class="num"><strong>${fmt(c.score, 0)}</strong><small>/ ${fmt(c.score_max, 0)}</small></span></span></td><td class="m-risk">${badge(c.risk_level)}</td><td class="m-hide wrap">${esc(c.industry || "行业未知")}<small>技术 ${fmt(c.technical_score, 0)} · 行业 ${fmt(c.industry_score, 0)} · 基本面 ${fmt(c.fundamental_score, 0)}</small><small>风险区间 ${fmt(c.attention_low)}–${fmt(c.attention_high)} · 建议买入价 ${fmt(c.confirmation)}</small></td><td class="m-hide">${d3Cell(c)}</td></tr>`);
  return table(["股票", {t:"收盘价", cls:"r"}, {t:"涨跌幅", cls:"r"}, "评分", "风险", "理由", "D3 结果"], body, 900, {cls:"mlist"});
}
const sortText = sort => `评分${sort === "asc" ? "从低到高" : "从高到低"}`;
// Screening funnel (S04) and per-stock audit rows (S03) from the plugin's latest recorded screen; display only.
const FUNNEL_STAGE = {input:"原池（当日行情）", price:"价格区间", risk_state:"停牌 / 涨跌停 / ST 已核验为否", deep_screen:"深筛名额（按成交额）",
  indicators:"指标可算", risk_review:"风险复核", min_score:"达到最低分", candidates:"候选（数量上限）"};
const FUNNEL_REASON = {price_out_of_range:"价格不在区间", suspended:"停牌", limit_up:"涨停", limit_down:"跌停", st:"ST", risk_state_unknown:"风险状态未核验",
  beyond_deep_limit:"超出深筛名额", history_failed:"历史行情不可用", failed:"指标获取失败", not_computed:"未计算", risk_blocked:"风险阻断",
  technical_incomplete:"技术数据不完整", factor_not_checked:"不在因子名额内，没查 ST / 审计", st_audit_unknown:"ST / 审计状态未知",
  risk_unknown:"风险未知", below_min_score:"低于最低分", beyond_candidate_limit:"超出候选上限"};
const AUDIT_STATUS = {candidate:["ok", "入选"], fallback_candidate:["warn", "观察候选（未达最低分）"], beyond_candidate_limit:["info", "达标，超出上限"],
  risk_blocked:["crit", "风险阻断"], technical_incomplete:["unk", "技术数据不完整"], factor_not_checked:["unk", "没查 ST / 审计"],
  st_audit_unknown:["unk", "ST / 审计未知"], risk_unknown:["unk", "风险未知"], below_min_score:["unk", "低于最低分"], not_selected:["unk", "未入选"], unknown:["unk", "未知"]};
const INDICATOR_OK = ["network", "memory_cache", "persistent_cache", "raw_batch"];
const reasonList = excluded => Object.entries(excluded || {}).sort((a, b) => b[1] - a[1]).map(([k, n]) => `${esc(FUNNEL_REASON[k] || k)} ${fmt(n, 0)}`).join(" · ");
function funnelVerdict(stages) {
  if (!stages.length) return "";
  if (stages[0].count === 0) return "当日行情为空，后面各步都没有数据。";
  const gates = stages.filter(s => s.gate !== false), last = gates[gates.length - 1];
  const stop = gates.find((s, i) => i > 0 && s.count === 0 && gates[i - 1].count > 0);
  if (!stop) return "";
  if (stop.key === "min_score" && last?.fallback) return `没有股票达到最低分，列出 ${fmt(last.fallback, 0)} 只未达最低分的观察候选。`;
  const top = Object.entries(stop.excluded || {}).sort((a, b) => b[1] - a[1])[0];
  return `在「${FUNNEL_STAGE[stop.key] || stop.key}」这一步全部被拦下${top ? `，主要原因：${FUNNEL_REASON[top[0]] || top[0]}` : ""}。`;
}
function funnelPanel(screen) {
  if (screen?.status !== "available") {
    const recorded = screen?.status === "not_recorded";
    return panel("筛选漏斗", empty(recorded ? "最近的筛选没有记录漏斗" : "筛选漏斗不可用", screen?.status || "screen_audit_unavailable", recorded ? ["插件更新到带筛选漏斗的版本后，下一次筛选开始记录"] : []), `<span class="src">S04</span>`);
  }
  const stages = screen.funnel?.stages || [], first = stages[0]?.count;
  const rows = stages.map(s => {
    const share = finite(first) && first > 0 ? `占原池 ${fmt(s.count / first * 100, 1)}%` : "";
    const extra = s.key === "price" ? `${fmt(s.price_min)}–${fmt(s.price_max)} 元` : s.key === "deep_screen" ? `上限 ${fmt(s.limit, 0)}` : s.key === "min_score" ? `最低分 ${fmt(s.min_score, 0)}` : s.key === "candidates" ? `上限 ${fmt(s.limit, 0)}${s.fallback ? ` · 观察候选 ${fmt(s.fallback, 0)}` : ""}` : "";
    const why = Object.keys(s.excluded || {}).length ? `<small>排除：${reasonList(s.excluded)}</small>` : "";
    const aside = s.gate === false ? `<small>只统计、不单独拦截；缺指标的股票在风险复核里出局</small>` : "";
    return `<div class="status-row"><span>${esc(FUNNEL_STAGE[s.key] || s.key)}${aside}${why}</span><span class="num">${fmt(s.count, 0)}<small>${[share, extra].filter(Boolean).join(" · ")}</small></span></div>`;
  }).join("");
  const verdict = funnelVerdict(stages);
  const older = screen.is_latest ? "" : notice("", "triangle-alert", `最新一次筛选（${esc(screen.latest_run_id || "未知")}）没有记录漏斗，下面是更早的一次`);
  return panel("筛选漏斗", older + (verdict ? notice("info", "list-filter", esc(verdict)) : "") + `<div class="status-rows">${rows}</div>`, `<span class="src">${esc(screen.date || "日期未知")} · ${esc(screen.job_name || "筛选")} · ${badge(screen.run_status)}</span>`);
}
function auditPanel(screen) {
  if (screen?.status !== "available") return "";
  const rows = (screen.audit || []).map(r => {
    const [kind, text] = AUDIT_STATUS[r.status] || AUDIT_STATUS.unknown, ind = r.indicators || {}, missing = r.missing_inputs || [];
    const why = [...(r.reasons || []), ...(r.risk_flags || []).map(f => `风险：${f}`)].map(esc).join("；") || "—";
    const comp = r.comparable ? "同口径" : `缺 ${fmt(missing.length, 0)} 项${INDICATOR_OK.includes(r.indicator_status) ? "" : " · 指标不可用"}`;
    return `<tr><td class="num">${fmt(r.rank, 0)}</td><td>${link(r.code, r.name)}</td><td>${kindBadge(kind, text, r.status)}</td><td class="num">${fmt(r.base_score, 0)}${finite(r.score) && r.score !== r.base_score ? `<small>综合 ${fmt(r.score, 0)}</small>` : ""}</td><td>${badge(r.risk_level)}</td><td class="wrap">${why}</td><td class="num">${fmt(ind.rsi6, 1)}<small>5 日 ${pct(ind.momentum5)} · 20 日 ${pct(ind.momentum20)}</small></td><td title="${esc(missing.join(", "))}">${comp}</td></tr>`;
  });
  return panel(`深筛明细 · 前 ${fmt(rows.length, 0)} / 共 ${fmt(screen.audit_total, 0)}`, `<p class="regime-note">技术分只用来排序，不是胜率或上涨概率；同分时按风险、数据完整度和成交额排。缺评分输入的股票，缺的那项按 0 分算，和数据齐全的不是同一口径。</p>` + table(["排名", "股票", "去向", "技术分", "风险", "加减分与风险标记", "RSI6 / 动量", "可比性"], rows, 980), `<span class="src">S03 · 全部最终候选 + 排名靠前的其他股票</span>`);
}
function stockScreenLine(d) {
  const s = d.screen_audit;
  if (s?.status === "not_listed") return `最近一次筛选（${esc(s.date || "未知")}）：不在深筛明细前 ${fmt(s.audit_shown, 0)} 名里（共 ${fmt(s.audit_total, 0)} 只进入深筛，其余原因看候选页漏斗）`;
  if (s?.status !== "listed" || !s.row) return "";
  const r = s.row, [, text] = AUDIT_STATUS[r.status] || AUDIT_STATUS.unknown;
  return `最近一次筛选（${esc(s.date || "未知")}）：深筛第 ${fmt(r.rank, 0)} / ${fmt(s.audit_total, 0)} 名 · ${esc(text)} · 技术分 ${fmt(r.base_score, 0)}（只用于排序）${(r.reasons || []).length ? ` · ${r.reasons.map(esc).join("；")}` : ""}`;
}
function renderCandidates(d) {
  state.candidates = (d.items || []).slice(0, LIMITS.candidates);
  const f = state.filter, chain = chainState(), rows = applyFilter();
  const opt = (value, text, cur) => `<option value="${value}" ${cur === value ? "selected" : ""}>${text}</option>`;
  const boards = [["all", "全部"], ["main", "主板"], ["gem", "创业板"], ["star", "科创板"]];
  const toolbar = `<div class="toolbar"><label class="search">${ic("search")}<input id="candidate-search" value="${esc(f.query)}" placeholder="代码、名称或行业" aria-label="筛选候选"></label><div class="seg" role="group" aria-label="板块">${boards.map(([k, t]) => `<button type="button" data-board="${k}" class="${f.board === k ? "active" : ""}" aria-pressed="${f.board === k}">${t}</button>`).join("")}</div><button type="button" class="fchip ${f.cheap ? "on" : ""}" data-chip="cheap" aria-pressed="${f.cheap}">股价 0-50</button><button type="button" class="fchip ${f.noSt ? "on" : ""}" data-chip="nost" aria-pressed="${f.noSt}">排除 ST</button><button type="button" class="fchip on" data-sort="score" aria-label="切换评分排序">${ic("arrow-up-down")}<span id="sort-label">${sortText(f.sort)}</span></button><select id="candidate-risk" aria-label="风险筛选">${opt("", "全部风险", f.risk)}${opt("eligible", "可跟踪", f.risk)}${opt("watch_only", "需复核", f.risk)}${opt("blocked", "已拦截", f.risk)}</select><select id="candidate-sort" aria-label="候选排序">${opt("desc", "评分从高到低", f.sort)}${opt("asc", "评分从低到高", f.sort)}</select><span class="count" id="candidate-count">${rows.length} / ${LIMITS.candidates}</span></div>`;
  const warn = chain.kind === "ok" ? "" : notice(chain.kind === "crit" ? "crit" : "", "triangle-alert", `链路${esc(chain.text)}：名单只用于排查，不作为当日候选。原因代码 ${esc(chain.reasons.map(r => r.code).join("、"))}`);
  return pageHead("候选池", "", [`上限 ${LIMITS.candidates}`]) + warn + panel("正式候选 · 当前有效批次", toolbar + `<div id="candidate-table">${candidateTable(rows)}</div>`) + funnelPanel(d.screen) + auditPanel(d.screen);
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
    rows.map(r => `<tr class="click"><td class="m-hide">${esc(r.rank)}<small>${esc(r.data_date || "—")}</small></td><td class="m-name"><span class="show-m">${esc(r.rank)}. </span>${link(r.code, r.name)}${hotBadge(r.code)}<small class="hide-m">${esc(r.record_id)}</small></td><td class="m-score">${fmt(r.score, 0)} · ${badge(r.risk_level)}</td><td class="m-price wrap">${fmt(r.observation_reference_close)}<small>未复权冻结收盘</small><span class="hide-m">${planText(r)}</span></td><td class="m-risk">${badge(r.eligibility)}<span class="hide-m"><small>${r.confirmation === "not_assessed" ? "B 未确认" : "B 两根完成柱确认（模拟）"}</small><small>A：未观察，不计未触发</small>${r.entry_qualification_version && r.entry_qualification_version !== r.qualification_version ? "<small>模拟记录绑定成交时资格；当前风险另列</small>" : ""}</span></td><td class="m-pct wrap">${badge(r.paper_status)}<small class="hide-m">${entryText(r)}</small></td><td class="m-hide">${markText(r)}</td><td class="m-hide">${esc(r.protocol_version || "—")}<small>${esc(r.accounting_version || "尚无核算")}</small><small>${esc(gapText(r.missing_reason))}</small></td></tr>`), 1480, {cls:"mlist"});
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
    const rows = (e.items || []).map(r => `<tr><td>${esc(r.rank ?? "—")}</td><td>${r.code ? link(r.code, r.name) + hotBadge(r.code) : esc(r.name || "—")}</td><td>${esc(r.sector || "—")}</td><td class="r num">${fmt(r.close)}</td><td class="r num ${tone(r.return5)}">${finite(r.return5) ? pct(Number(r.return5) * 100) : "—"}</td><td class="r num">${finite(r.amount20) ? `${fmt(Number(r.amount20) / 1e8)}<small>亿</small>` : "—"}</td></tr>`);
    const meta = `<div class="status-rows"><div class="status-row"><span>冻结时间 / 输入日</span><span>${esc(bj(e.frozen_at))} · ${esc(e.input_as_of || "未知")}</span></div><div class="status-row"><span>注册提交 / 文件哈希</span><span class="mono">${esc((e.registration_commit || "未知").slice(0, 12))} · ${esc((e.file_sha256 || "").slice(0, 16))}</span></div><div class="status-row"><span>历史检验</span><span>${esc(e.historical_evaluation || e.status || "未记录")}</span></div>${e.next_observation ? `<div class="status-row"><span>下一次观察</span><span>${esc(e.next_observation)}</span></div>` : ""}<div class="status-row"><span>风险口径</span><span>${esc(e.risk_basis || "未记录")}</span></div></div>`;
    const title = `${esc(e.id)} · ${(e.stages || []).map(s => badge(s)).join("")} ${badge("research_only")}`;
    return panel(title, `<div class="lab-band">${ic("flask-conical")}<div><b>${esc(e.label || "研究观察")}</b><br>独立冻结名单，不是正式推荐，未接入插件自动策略，不写入正式候选或推荐表。</div></div>` + meta +
      table(["排名", "标的", "板块", {t:"冻结收盘", cls:"r"}, {t:"5日涨跌", cls:"r"}, {t:"20日成交额", cls:"r"}], rows, 720), `<span class="src">${esc(e.file)}</span>`);
  };
  const missing = (data.unavailable || []).map(u => notice("", "triangle-alert", `${esc(u.file)} 不可用 · 原因代码 ${esc(u.reason)}`)).join("");
  return `<h2 class="section-title">独立研究成果 · 离线冻结文件（只读）</h2>` + missing + (data.entries || []).map(entry).join("");
}
// Evening research job (schemes F and D): display-only labels, never part of the formal gates.
const EXCLUSION_TEXT = {not_main_or_chinext:"不在主板或创业板", st_name:"ST 股票", no_bar_today:"当日没有行情", history_lt_60:"近 60 日行情不完整",
  corporate_action_60d:"近 60 日有除权除息", price_above_50:"收盘价高于 50 元", amount_below_20m:"成交额低于 2000 万元", indicator_missing:"指标无法计算"};
const FACTOR_TEXT = {R20:"20 日涨幅", C_MA20:"偏离 20 日均线", IVOL20:"20 日特质波动", VOLR5_60:"5 日 / 60 日均量", MAX20:"20 日最大 3 日涨幅均值",
  TURN5_20:"5 日 / 20 日换手（用量代替）", ABTURN:"20 日 / 60 日换手（用量代替）", RSI14:"RSI14"};
const SIGNAL_STATUS_TEXT = {not_configured:"没有配置研究信号文件", missing:"研究信号还没有生成", unreadable:"研究信号文件读取失败",
  invalid:"研究信号文件格式不对", job_error:"研究信号任务这次没算出来", pool_lt_30:"研究股票池不足 30 只", history_lt_60:"行情不足 60 个交易日"};
const signalsData = (payload = state.ctx.signals) => payload?.data || null;
function hotMap(payload = state.ctx.signals) {
  const d = signalsData(payload), oh = d?.overheat;
  if (!d || !["available", "stale"].includes(d.status) || oh?.status !== "available") return new Map();
  return new Map((oh.hot || []).map(item => [item.code, item]));
}
const riskCache = new WeakMap();
function riskMaps(payload = state.ctx.signals) {
  const d = signalsData(payload);
  if (!d || !["available", "stale"].includes(d.status)) return {unlock: new Map(), margin: new Map()};
  if (!riskCache.has(d)) riskCache.set(d, {
    unlock: new Map(d.unlock?.status === "available" ? (d.unlock.heavy || []).map(x => [x.code, x]) : []),
    margin: new Map(d.margin?.status === "available" ? (d.margin.crowded || []).map(x => [x.code, x]) : [])});
  return riskCache.get(d);
}
// Overheat, heavy lockup expiry and margin crowding: research reminders only, never gates.
function hotBadge(code, payload = state.ctx.signals) {
  const item = hotMap(payload).get(code), risk = riskMaps(payload), unlock = risk.unlock.get(code), margin = risk.margin.get(code);
  const oh = signalsData(payload)?.overheat;
  return (item ? ` ${kindBadge("warn", "过热", `研究标签 · ${oh.trade_date || "日期未知"} 过热综合分位 ${fmt(Number(item.pct) * 100, 1)}%，属于研究股票池里最热的 10%。只做提醒，不改正式门槛。`)}` : "")
    + (unlock ? ` ${kindBadge("warn", "解禁", `${unlock.date} 起 30 天内解禁合计约占流通市值 ${fmt(Number(unlock.ratio) * 100, 1)}%（≥5% 才标）。研究显示解禁比例越大前后收益越差；只做提醒，未验证。`)}` : "")
    + (margin ? ` ${kindBadge("warn", "两融拥挤", `融资买入占当日成交额 ${fmt(Number(margin.buy_share) * 100, 1)}%，排在两融标的前 10%。研究显示融资买入强度高之后短期收益偏低；只做提醒，未验证。`)}` : "");
}
function riskReminderPanel(payload = state.ctx.signals) {
  const d = signalsData(payload);
  if (!d || ["not_configured", "missing", "unreadable", "invalid"].includes(d.status)) return signalsUnavailable("解禁与两融", d?.status || payload?.meta?.reason || "research_signals_unavailable");
  const u = d.risk?.unlock, m = d.risk?.margin, uw = d.unlock, mw = d.margin;
  const row = (title, sub, right) => `<div class="status-row"><span>${title}${sub ? `<small>${sub}</small>` : ""}</span><span>${right}</span></div>`;
  const missing = section => kindBadge("unk", "这次没取到", section?.reason || section?.status || "unavailable");
  const unlockRow = uw?.status !== "available" ? row("解禁", "", missing(uw))
    : !u?.events?.length ? row("解禁", `${esc(uw.window?.[0] || "")} — ${esc(uw.window?.[1] || "")}`, kindBadge("ok", "30 天内没有解禁"))
    : row("解禁", u.events.map(e => `${esc(e.date)} ${esc(e.type || "")} 占流通 ${fmt(Number(e.ratio) * 100, 2)}%`).join("<br>"),
      `${u.heavy ? kindBadge("warn", "解禁压力大", "30 天内合计 ≥ 5% 流通市值") : kindBadge("info", "有解禁", "30 天内合计低于 5%")}<small>合计 ${fmt(Number(u.ratio_total) * 100, 2)}%</small>`);
  const marginRow = mw?.status !== "available" ? row("两融", "", missing(mw))
    : m?.status !== "listed" ? row("两融", esc(mw.trade_date || ""), kindBadge("unk", "不在两融标的里或当天无成交"))
    : row("两融", `${esc(mw.trade_date || "")} · 融资余额 ${fmt(Number(m.balance) / 1e8)} 亿 · 融资买入 ${fmt(Number(m.buy) / 1e8, 3)} 亿`,
      `${m.crowded ? kindBadge("warn", "两融拥挤", "融资买入占成交额排在两融标的前 10%") : kindBadge("ok", "不拥挤")}<small>买入占成交 ${fmt(Number(m.buy_share) * 100, 1)}% · 分位 ${fmt(Number(m.pct) * 100, 0)}%</small>`);
  const stale = d.status === "stale" ? notice("", "triangle-alert", `研究信号超过 3 天没有更新（生成于 ${esc(bj(d.generated_at))}）`) : "";
  return panel("解禁与两融", stale + `<div class="status-rows">${unlockRow}${marginRow}</div><p class="regime-note">风险提醒，只显示、未验证：A 股研究显示解禁占流通比例越大，前后收益越差；融资买入强度高的股票之后短期收益偏低。阈值（30 天内解禁合计 ≥ 5%、融资买入占成交额前 10%）只是展示用，不改正式门槛。</p>`, `<span class="src">研究参考 · 只显示</span>`);
}
function signalsUnavailable(title, code) {
  return panel(title, empty(SIGNAL_STATUS_TEXT[code] || `${title}暂不可用`, code), `<span class="src">研究参考</span>`);
}
function indexTrendPanel(payload = state.ctx.signals) {
  const d = signalsData(payload), it = d?.index_trend;
  if (!d || ["not_configured", "missing", "unreadable", "invalid"].includes(d.status)) return signalsUnavailable("大盘温度计", d?.status || payload?.meta?.reason || "research_signals_unavailable");
  if (!Object.keys(it?.indices || {}).length) return signalsUnavailable("大盘温度计", it?.status || "index_trend_unavailable");
  const rows = [["000905", "中证500"], ["000852", "中证1000"]].map(([c, name]) => {
    const x = it.indices[c];
    if (!x) return `<div class="status-row"><span>${name}</span><span class="muted">这次没有取到</span></div>`;
    const side = x.above_ma200 ? kindBadge("ok", "200 日均线上方", "研究口径：次日持有") : kindBadge("warn", "200 日均线下方", "研究口径：次日空仓");
    return `<div class="status-row"><span>${esc(x.name || name)}<small>${esc(x.date || "—")} · 收盘 ${fmt(x.close)} · 200 日均线 ${fmt(x.ma200)}</small></span><span>${side}<small>偏离 ${pct(x.distance_ma200, true)} · 已持续 ${fmt(x.sessions_on_side, 0)} 个交易日 · 120 日均线${x.above_ma120 ? "上方" : "下方"}</small></span></div>`;
  }).join("");
  const stale = d.status === "stale" ? notice("", "triangle-alert", `研究信号超过 3 天没有更新（生成于 ${esc(bj(d.generated_at))}）`) : "";
  return panel("大盘温度计", stale + `<div class="status-rows">${rows}</div><p class="regime-note">2021—2026 回看：只在指数高于 200 日均线时持有，中证 500 和中证 1000 的最大回撤都减半左右；换成 120 日均线，中证 500 反而更差，所以只作仓位参考，不是交易规则。</p>`, `<span class="src">研究参考 · 只显示</span>`);
}
function factorValue(key, value) {
  if (!finite(value)) return "—";
  if (["R20", "C_MA20", "MAX20"].includes(key)) return pct(value, true);
  if (key === "IVOL20") return `${fmt(Number(value) * 100)}%`;
  if (key === "RSI14") return fmt(value, 1);
  return `${fmt(value)} 倍`;
}
function overheatStockPanel(code, payload = state.ctx.signals) {
  const d = signalsData(payload), oh = d?.overheat, s = d?.stock;
  if (!d || ["not_configured", "missing", "unreadable", "invalid"].includes(d.status)) return signalsUnavailable("过热标签", d?.status || payload?.meta?.reason || "research_signals_unavailable");
  if (oh?.status !== "available") return signalsUnavailable("过热标签", oh?.reason || "overheat_unavailable");
  const head = `<span class="src">研究参考 · ${esc(oh.trade_date || "日期未知")}</span>`;
  const stale = d.status === "stale" ? notice("", "triangle-alert", `研究信号超过 3 天没有更新（生成于 ${esc(bj(d.generated_at))}）`) : "";
  if (!s || s.code !== code || s.status !== "evaluated") {
    const why = s?.code === code && s.reason ? EXCLUSION_TEXT[s.reason] || s.reason : "当天不在研究股票池里";
    return panel("过热标签", stale + `<div class="status-rows"><div class="status-row"><span>状态</span><span>${kindBadge("unk", "不在评估范围", s?.reason || "not_evaluated")}</span></div><div class="status-row"><span>原因</span><span>${esc(why)}</span></div></div>`, head);
  }
  const label = s.hot ? kindBadge("warn", "过热", "研究股票池里最热的 10%") : kindBadge("ok", "未过热", "不在研究股票池最热的 10% 里");
  const rows = Object.entries(s.indicators || {}).map(([k, v]) => `<div>${esc(FACTOR_TEXT[k] || k)}</div><div class="num">${factorValue(k, v)}</div>`).join("");
  return panel("过热标签", stale + `<div class="status-rows"><div class="status-row"><span>状态</span><span>${label}</span></div><div class="status-row"><span>过热综合分位</span><span>${fmt(Number(s.pct) * 100, 1)}%<small>最热的 10% 标为过热 · ${fmt(oh.evaluated, 0)} 只参与排名</small></span></div></div><div class="kv">${rows}</div><p class="regime-note">2021—2023 回看：最热的 10% 之后 5 日平均跑输股票池 0.55%、20 日跑输 1.43%，三年都为负。只做提醒，不改正式门槛；换手比值用成交量代替，ST 按当前名称判断。</p>`, head);
}
const MACD_TEXT = {no_active_raw:"没有 active raw 日线，旧日线表缺前收盘，算不了连续价格", insufficient_history:"行情不足 60 个交易日"};
function macdStockPanel(d) {
  const m = d?.macd;
  if (m?.status !== "available") return panel("MACD 状态", empty(MACD_TEXT[m?.reason] || MACD_TEXT[m?.status] || "MACD 暂不可用", m?.reason || m?.status || "macd_unavailable"), `<span class="src">研究参考 · 未验证</span>`);
  const golden = m.state === "golden", side = golden ? "上穿" : "下穿";
  const since = m.cross_date ? `${esc(m.cross_date)} DIF ${side} DEA · ${m.sessions_since_cross === 0 ? "当天" : `${fmt(m.sessions_since_cross, 0)} 个交易日前`}` : `本批次内 DIF 一直在 DEA ${golden ? "上方" : "下方"}`;
  const gap = m.last_bar_date && m.last_bar_date !== m.trade_date ? notice("", "triangle-alert", `最近一根日线是 ${esc(m.last_bar_date)}，之后的交易日按价格不变计算`) : "";
  const rows = `<div class="status-rows"><div class="status-row"><span>状态</span><span><b class="num ${golden ? "up" : "down"}">${golden ? "金叉" : "死叉"}</b> ${kindBadge("lab", "未验证", "方案 I 从 10-08 起前瞻检验，2027 年 4 月终点判定")}<small>${since}</small></span></div><div class="status-row"><span>近 3 日新金叉<small>方案 I 的金叉条件</small></span><span>${m.recent_golden_cross ? "是" : "否"}</span></div></div>`;
  const values = `<div class="kv"><div>DIF</div><div class="num">${fmt(m.dif, 3)}</div><div>DEA</div><div class="num">${fmt(m.dea, 3)}</div><div>MACD 柱（2 × (DIF − DEA)）</div><div class="num ${tone(m.histogram)}">${fmt(m.histogram, 3)}</div></div>`;
  return panel("MACD 状态", gap + rows + values + `<p class="regime-note">MACD(12, 26, 9)，用前收盘连乘的连续价格计算，除权不会变成假死叉；DIF / DEA 换算到最新收盘价，和软件前复权的数值接近。用本批次 ${fmt(m.sessions, 0)} 个交易日（${esc(m.first_session || "—")} 起）递推。方案 H 回看：金叉 + 不过热持有 5 天比同日股票池少赚 0.09 个百分点，不显著；方案 I 从 10-08 起前瞻检验它在 T+2 / T+3 上的增量。只显示，不改正式门槛。</p>`, `<span class="src">研究参考 · 截至 ${esc(m.trade_date || "日期未知")}</span>`);
}
// W14: browse the pools by purpose; the frozen rank stays in every row whatever the order or filter.
const RESEARCH_FILTERS = [["all", "全部"], ["primary", "只看重点"], ["radar", "只看警戒"], ["hot", "过热"], ["risk_unknown", "风险未知"]];
const RESEARCH_COLUMNS = [[3, "冻结参考价 / 计划"], [4, "资格 / 确认"], [5, "模拟状态 / 入场"], [6, "D1 / D3 / D5"], [7, "版本 / 缺口"]];
const researchHidden = () => new Set(saved("stock-watch.research-hide", []).filter(Number.isInteger));
function researchPick(primary, radar) {
  const f = state.researchFilter || "all", hot = hotMap(state.ctx.signals);
  const unknownRisk = r => !r.risk_level || /unknown|missing|未知/.test(String(r.risk_level));
  const keep = rows => f === "hot" ? rows.filter(r => hot.has(r.code)) : f === "risk_unknown" ? rows.filter(unknownRisk) : rows;
  const order = rows => state.researchSort === "score" ? [...rows].sort((a, b) => (Number(b.score) || -Infinity) - (Number(a.score) || -Infinity) || Number(a.rank) - Number(b.rank)) : rows;
  return [f === "radar" ? [] : order(keep(primary)), f === "primary" ? [] : order(keep(radar))];
}
function researchTools() {
  const f = state.researchFilter || "all", hidden = researchHidden();
  return `<div class="toolbar research-tools"><div class="seg" role="group" aria-label="研究池筛选">${RESEARCH_FILTERS.map(([id, label]) => `<button type="button" data-research-filter="${id}" class="${f === id ? "active" : ""}" aria-pressed="${f === id}">${label}</button>`).join("")}</div>` +
    `<select id="research-sort" aria-label="研究池排序"><option value="rank"${state.researchSort !== "score" ? " selected" : ""}>按冻结排名</option><option value="score"${state.researchSort === "score" ? " selected" : ""}>按评分（原排名仍显示）</option></select>` +
    `<details class="col-toggle"><summary>显示列</summary>${RESEARCH_COLUMNS.map(([i, label]) => `<label><input type="checkbox" data-research-col="${i}"${hidden.has(i) ? "" : " checked"}> ${label}</label>`).join("")}</details></div>`;
}
const researchPoolsClass = () => ["research-pools", ...[...researchHidden()].map(i => `hide-c${i}`)].join(" ");
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
  const p0 = take(primary), r0 = take(radar), h = take(history), [p, r] = researchPick(p0, r0);
  const shown = (rows, all) => rows.length === all.length ? `${rows.length}` : `${rows.length} / ${all.length}`;
  return pageHead("研究观察池", "", [`记录上限 ${LIMITS.recommendations}`]) + band + rules + gates + researchTools() +
    `<div class="${researchPoolsClass()}">${panel(`观察池 · ${shown(p, p0)}`, researchRows(p))}${panel(`警戒池 · ${shown(r, r0)}`, researchRows(r))}</div>` + panel(`历史模拟记录 · ${h.length}`, paperHistoryRows(h)) + catalogSection();
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
    const [payload, w] = await Promise.all([api(`search?q=${encodeURIComponent(q)}`), watchStatus()]);
    const items = payload.data?.items || [];
    const SOURCE = {stock_symbols:"名称索引", formal_candidate:"正式候选", research_pool:"研究池", active_raw:"active raw 日线"};
    out.innerHTML = items.length ? `<div class="search-results">${items.map(r => `<div class="search-row"><a href="${stockHref(r.code)}"><span>${esc(r.name || "名称未知")} <small class="mono">${esc(r.code)}</small></span><small>${esc(SOURCE[r.source] || "来源未知")}</small></a>${watchButton(r.code, w, true)}</div>`).join("")}</div>${w.configured ? `<p class="watch-scope">${esc(watchNote(w))}</p>` : ""}` : empty("没有匹配的股票", "no_records");
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
  const offline = (state.ctx.catalog?.data?.entries || []).flatMap(e => (e.items || []).filter(i => i.code === d.code)
    .map(i => `${esc(e.id)} 第 ${esc(i.rank)} 名（${esc(e.label || "研究观察")}）`));
  const screen = stockScreenLine(d);
  return notice("info", "database", `行情来源 ${esc(source)}<br>正式身份：${d.formal_status === "formal_candidate" ? "当前有效正式候选" : "不是当前正式候选"} · 推荐记录：${d.recommendation_status === "recorded" ? "有" : "无"}<br>${research}${offline.length ? `<br>离线研究名单：${offline.join("；")}` : ""}${screen ? `<br>${screen}` : ""}`);
}
function limitText(d) {
  const l = d.limits;
  if (!l || !finite(l.up) || !finite(l.down)) return "未知";
  const board = {growth:"创业板 / 科创板", beijing:"北交所", main:l.st ? "主板 ST" : "主板"}[l.board] || "板块未知";
  return `<span class="num up">${fmt(l.up)}</span> / <span class="num down">${fmt(l.down)}</span><small>按 ${esc(d.bar_date || "最近")} 收盘和${board} ±${fmt(Number(l.rate) * 100, 0)}% 推算，未核验；新股上市初期、退市整理期等特殊情况不适用</small>`;
}
// The plugin's intraday quote file, beside (never instead of) the stored daily close.
function liveQuoteHtml(live) {
  if (!live || !["available", "partial"].includes(live.status)) return "";
  const q = live.quote;
  if (!q) return `<small class="muted">盘中：这只股票不在插件盯盘名单（自选股 / 候选 / 关注）</small>`;
  return `<span class="live-quote">${kindBadge("info", "盘中", "插件盯盘报价，只读")}<b class="num">${fmt(q.price)}</b><span class="num ${tone(q.pct_change)}">${pct(q.pct_change)}</span><small>${esc(bj(q.provider_ts))} · ${esc(q.source || live.source || "来源未知")}</small></span>`;
}
async function refreshStockLive() {
  const code = state.selectedCode, el = $("#live-quote");
  if (!code || !el) return false;
  try {
    const d = (await api("intraday")).data || {};
    el.innerHTML = liveQuoteHtml({status: d.status, source: d.source, quote: (d.quotes || []).find(q => q.code === code) || null});
    return true;
  } catch {
    return false;
  }
}
// Add to watchlist: the page only queues a request; the plugin applies it to one chat session and reports back.
const WATCH_RESULT = {added:"已加入自选", exists:"已在自选中", limit_reached:"自选已满：请先在聊天里 /自选 删除 几只", scope_unresolved:"插件不确定加到哪个会话：请在插件设置 web_watch_scope 指定", disabled:"插件已关闭网页加自选（web_watch_enabled）", invalid_request:"请求无效，插件没有处理", error:"插件处理出错，请看插件日志"};
const WATCH_REJECT = {invalid_code:"代码无效", unknown_code:"快照里没有这只股票", invalid_body:"请求无效", inbox_missing:"插件收件箱目录不存在（部署时创建）", inbox_full:"待处理的请求太多，请稍后再试", rate_limited:"操作太频繁，请稍后再试", inbox_unwritable:"写不进插件收件箱", inbox_unreadable:"读不了插件收件箱"};
async function watchStatus(id = null) {
  if (!id && state.watch && Date.now() - state.watch.at < 15000) return state.watch.data;
  try {
    const response = await fetch(`/api/watch${id ? `?id=${encodeURIComponent(id)}` : ""}`, {cache:"no-store"});
    const data = response.ok ? (await response.json()).data || {configured:false} : {configured:false};
    state.watch = {at:Date.now(), data};
    return data;
  } catch {
    return {configured:false};
  }
}
function watchNote(w) {
  if (!w?.configured) return "";
  if (w.inbox !== "ready") return WATCH_REJECT.inbox_missing;
  if (!w.plugin) return "插件还没有写出网页自选状态：需要在 AstrBot 重载新版插件";
  if (!w.plugin.fresh) return `插件已 ${w.plugin.age_seconds ?? "?"} 秒没有更新网页自选状态，可能没在运行新版本`;
  if (!w.plugin.enabled) return WATCH_RESULT.disabled;
  const s = w.scope || {};
  if (!s.label) return s.status === "ambiguous" ? "有多个会话可选，插件不猜：请在插件设置 web_watch_scope 指定" : "还没有可写入的会话：先在聊天里用一次 /自选 添加，或在插件设置 web_watch_scope 指定";
  return `加到 ${s.label}（已有 ${s.count ?? "?"} / ${s.limit ?? "?"} 只；成本价不在网页显示）`;
}
const watchReady = w => !!(w?.configured && w.inbox === "ready" && w.plugin?.fresh && w.plugin.enabled && w.scope?.label);
function watchButton(code, w, compact = false) {
  if (!w?.configured || !/^\d{6}$/.test(code || "")) return "";
  if ((w.codes || []).includes(code)) return `<span class="watch-state">${ic("circle-check")}已在自选</span>`;
  return `<button type="button" class="btn${compact ? " sm" : ""} watch-add" data-watch-add="${esc(code)}" title="${esc(watchNote(w))}"${watchReady(w) ? "" : " disabled"}>${ic("circle-plus")}加入自选</button>`;
}
async function refreshWatchSlot(code) {
  const slot = $("#watch-slot");
  if (!slot) return;
  const w = await watchStatus();
  if (state.selectedCode !== code || !$("#watch-slot")) return;
  slot.innerHTML = w.configured ? `${watchButton(code, w)}<small class="watch-note">${esc(watchNote(w))}</small>` : "";
}
async function addToWatch(button) {
  const code = button.dataset.watchAdd, holder = button.parentElement;
  const say = text => { let el = holder.querySelector(".watch-note"); if (!el) { el = document.createElement("small"); el.className = "watch-note"; holder.appendChild(el); } el.textContent = text; };
  const reset = () => { button.disabled = false; button.textContent = "加入自选"; };
  button.disabled = true; button.textContent = "提交中…";
  try {
    const response = await fetch("/api/watch/add", {method:"POST", cache:"no-store", headers:{"Content-Type":"application/json", "X-Stock-Watch":"add"}, body:JSON.stringify({code})});
    const payload = await response.json().catch(() => ({}));
    if (response.status !== 202) { reset(); say(WATCH_REJECT[payload.reason] || `提交失败（HTTP ${response.status}）`); return; }
    button.textContent = "等待插件确认…";
    let last = "pending";
    for (let i = 0; i < 20; i++) {
      await new Promise(resolve => setTimeout(resolve, 2000));
      const r = (await watchStatus(payload.request_id)).request || {};
      last = r.state || "unknown";
      if (r.state === "done") {
        say(WATCH_RESULT[r.status] || "插件已处理");
        state.watch = null;
        if (["added", "exists"].includes(r.status)) button.outerHTML = `<span class="watch-state">${ic("circle-check")}已在自选</span>`;
        else reset();
        return;
      }
      // The plugin removes the request file a moment before it writes the result.
      if (last === "unknown" && i >= 2) break;
    }
    reset();
    say(last === "pending" ? "插件还没有处理这条请求：插件可能没在运行新版本，请求会留在收件箱里等插件处理" : "收件箱和结果里都找不到这条请求，请刷新页面查看");
  } catch {
    reset();
    say("连接不可用，请求可能没有提交");
  }
}
function renderStock(d) {
  const bars = (d.bars || []).slice(-LIMITS.bars);
  state.stock = {...d, bars}; state.selectedCode = d.code;
  const c = d.candidate || {}, last = bars[bars.length - 1], prev = bars[bars.length - 2];
  const chg = last && prev && finite(prev.close) && Number(prev.close) !== 0 ? (last.close - prev.close) / prev.close * 100 : null;
  const head = `<div class="stock-head"><a class="icon-btn tip-left" href="#candidates" aria-label="返回候选池" data-tip="返回候选池">${ic("arrow-left")}</a><div><h1>${esc(d.name || d.code)}</h1><small class="muted">${esc(d.code)} · ${esc(c.industry || "行业未知")}</small></div><span class="px">${fmt(d.last_close)}</span><span class="num ${tone(chg)}">${pct(chg)}</span><span id="live-quote">${liveQuoteHtml(d.live)}</span><span id="watch-slot" class="watch-slot"></span>${badge(c.risk_level)}${srcSpans(state.meta, [`收盘 ${d.bar_date || "未知"}`])}</div>`;
  const ev = d.data_evidence || {};
  const coverage = `<div class="evidence-coverage"><span>技术历史 ${d.technical_history?.bars ?? 0} 根 · ${d.technical_history?.status === "available" ? "可用" : "不足 20 根"}</span><span>财务因子 ${ev.financial_known ?? 0}/${ev.financial_total ?? 5} 已证实</span><span>风险证据 ${ev.risk_known ?? 0}/${ev.risk_total ?? 4} 已证实</span></div>`;
  const tools = chartTools();
  const chart = bars.length ? `<div class="chart-wrap"><canvas class="price-chart" role="img" aria-label="已存日K线、均线、成交量与 MACD"></canvas><div class="chart-tip"></div></div><div class="chart-caption" id="chart-caption">${chartCaption(chartSeries(bars))}</div>` : empty("K 线不可用", "bars_unavailable");
  const q = d.data_quality || {};
  const quality = notice(kindOf(q.status) === "ok" ? "info" : "", "database", `行情数据 ${badge(q.status)} · 来源时间 ${esc(bj(q.source_timestamp))}${q.missing_reason ? ` · 原因代码 ${esc(q.missing_reason)}` : ""} · 未知不以旧数据代替`);
  const kv = (k, v) => `<div>${k}</div><div>${v}</div>`;
  const keyData = panel("关键数据", `<div class="kv">${kv("综合评分", `${fmt(c.score, 0)} / ${fmt(c.score_max, 0)}`)}${kv("技术 / 行业 / 基本面", `${fmt(c.technical_score, 0)} / ${fmt(c.industry_score, 0)} / ${fmt(c.fundamental_score, 0)}`)}${kv("市场修正", fmt(c.market_adjustment, 0))}${kv("风险区间", `${fmt(c.attention_low)} – ${fmt(c.attention_high)}`)}${kv("建议买入价", fmt(c.confirmation))}${kv("失效", fmt(c.invalidation))}${kv("目标区", `${fmt(c.target_low)} – ${fmt(c.target_high)}`)}${kv("下一交易日涨跌停价", limitText(d))}${kv("计划验证", badge(c.plan_validated ? "validated" : "unknown"))}${kv("覆盖率", finite(c.coverage) ? `${fmt(c.coverage * 100, 1)}%` : "—")}${Object.entries(d.indicators || {}).map(([k, v]) => kv(esc(k), fmt(v))).join("")}</div>`);
  const evRows = Object.entries({...(ev.financial || {}), ...(ev.risk || {})}).map(([k, v]) => `<tr><td>${esc(EVIDENCE_FIELDS[k] || k)}</td><td class="num">${v?.value === null || v?.value === undefined ? "未知" : typeof v.value === "boolean" ? (v.value ? "是" : "否") : fmt(v.value)}</td><td>${badge(v?.quality)}</td><td class="wrap">${esc(v?.reason || "—")}</td></tr>`);
  const recs = (d.recommendations || []).slice(0, LIMITS.recommendations).map(r => `<tr><td>${esc(r.date)}</td><td>${badge(r.plan_status)}</td><td>${badge(r.comparability)}</td><td class="mono">${esc(r.plan_version || "—")}</td></tr>`);
  return panel("查找个股", stockSearchForm()) + `<section class="panel">${head}${coverage}${tools}${chart}</section>` + stockStatusNotice(d) + quality +
    `<div class="grid g-2-1"><div class="col">${panel("推荐历史", table(["推荐日期", "计划状态", "可比性", "计划版本"], recs, 520), limitNote((d.recommendations || []).length, LIMITS.recommendations))}${panel("信号历史", signalRows(d.signals || []), limitNote((d.signals || []).length, LIMITS.events))}${panel("财务与风险证据", table(["字段", "值", "质量", "缺口"], evRows, 520))}</div><div class="col">${overheatStockPanel(d.code)}${riskReminderPanel()}${macdStockPanel(d)}${keyData}${panel("相关公告", renderAnnouncements(d.announcements), limitNote((d.announcements?.items || []).length, LIMITS.announcements))}</div></div>`;
}
// Chart settings live in this browser only (localStorage); moving averages are computed from the bars shown.
const MA_DEFAULT = [5, 13, 34, 55, 120], MA_LIMIT = 8;
const MA_COLORS = ["#2f6bd8", "#c9861a", "#8e44ad", "#16a085", "#d35400", "#7f8c8d", "#c0392b", "#2c3e50"];
const saved = (key, fallback) => {try {const value = JSON.parse(localStorage.getItem(key)); return value ?? fallback;} catch {return fallback;}};
const save = (key, value) => {try {localStorage.setItem(key, JSON.stringify(value));} catch {/* private mode: settings last for this page only */}};
function maSet(value = saved("stock-watch.ma", null)) {
  if (!Array.isArray(value)) return MA_DEFAULT.map(n => ({n, on: true}));
  return value.filter((m, i, all) => Number.isInteger(m?.n) && m.n >= 2 && m.n <= 250 && all.findIndex(x => x?.n === m.n) === i)
    .slice(0, MA_LIMIT).map(m => ({n: m.n, on: m.on !== false}));
}
function chartPrefs() {
  if (!state.maList) Object.assign(state, {maList: maSet(), macdOn: saved("stock-watch.macd", true) !== false, adjusted: saved("stock-watch.adjust", false) === true});
}
const maColor = n => MA_COLORS[Math.max(0, state.maList.findIndex(m => m.n === n)) % MA_COLORS.length];
function maBar() {
  chartPrefs();
  const chips = state.maList.map(m => `<span class="ma-chip ${m.on ? "on" : ""}"><button type="button" data-ma="${m.n}" aria-pressed="${m.on}"><i data-bg="${maColor(m.n)}"></i>MA${m.n}</button><button type="button" class="ma-x" data-ma-remove="${m.n}" aria-label="删除 MA${m.n}">×</button></span>`).join("");
  const add = state.maList.length < MA_LIMIT ? `<form id="ma-add-form" class="ma-add"><input id="ma-add" type="number" min="2" max="250" step="1" placeholder="周期" aria-label="添加均线周期"><button type="submit" class="btn">添加均线</button></form>` : `<span class="count">最多 ${MA_LIMIT} 条均线</span>`;
  return chips + add + `<button type="button" class="btn" data-ma-reset>恢复默认</button>`;
}
function chartTools() {
  chartPrefs();
  const check = (id, on, text) => `<label class="checkbox"><input type="checkbox" id="${id}" ${on ? "checked" : ""}>${text}</label>`;
  return `<div class="toolbar"><div class="seg" role="group" aria-label="周期">${[30, 60, 120].map(n => `<button type="button" data-period="${n}" class="${state.period === n ? "active" : ""}">${n} 日</button>`).join("")}</div>${check("ma-toggle", state.ma, "均线")}${check("macd-toggle", state.macdOn, "MACD")}${check("adjust-toggle", state.adjusted, "前复权")}<span class="count">最多 ${LIMITS.bars} 根</span></div><div class="toolbar ma-bar" id="ma-bar">${maBar()}</div>`;
}
function chartSeries(bars) {
  chartPrefs();
  const macd = state.stock?.macd, byDate = new Map((macd?.status === "available" ? macd.series || [] : []).map(x => [x.date, x]));
  const factors = bars.map(b => Number(byDate.get(b.date)?.adj));
  const adjusted = state.adjusted && bars.length > 0 && factors.every(f => Number.isFinite(f) && f > 0);
  const rows = adjusted ? bars.map((b, i) => ({...b, open: b.open * factors[i], high: b.high * factors[i], low: b.low * factors[i], close: b.close * factors[i]})) : bars;
  return {rows, adjusted, adjustMissing: state.adjusted && !adjusted, macd: state.macdOn && byDate.size ? bars.map(b => byDate.get(b.date) || null) : null};
}
function chartCaption(s) {
  const count = s.rows.length, on = state.ma ? state.maList.filter(m => m.on) : [];
  const mas = on.length ? on.map(m => `<span data-fg="${maColor(m.n)}">MA${m.n}</span>`).join(" · ") : "均线已隐藏";
  const start = Math.max(0, count - state.period), shown = count - start;
  const short = on.filter(m => m.n > count).map(m => `MA${m.n}`);
  const partial = on.filter(m => m.n <= count && (Math.max(start, m.n - 1) - start) / shown > .25).map(m => `MA${m.n}`);
  const notes = [s.adjustMissing ? "前复权需要 60 个交易日以上的连续价格，这只股票不够，先按未复权显示" : "",
    short.length ? `${short.join("、")} 需要比现有 ${fmt(count, 0)} 根更长的历史，画不出来` : "",
    partial.length ? `${partial.join("、")} 只画得出后半段（前面历史不够）` : ""].filter(Boolean);
  return `<span>日 K · ${s.adjusted ? "前复权（按前收盘连乘）" : "未复权"} · 空心红涨 / 实心绿跌</span><span>${mas} · 成交量${s.macd ? " · MACD(12, 26, 9) 未验证" : ""}</span>${notes.length ? `<span>${notes.join("；")}</span>` : ""}`;
}
function redrawChart() {
  if (!state.stock) return;
  const s = chartSeries(state.stock.bars), cap = $("#chart-caption");
  drawChart(s.rows, $(".price-chart"), {period: state.period, macd: s.macd,
    mas: state.ma ? state.maList.filter(m => m.on).map(m => ({n: m.n, color: maColor(m.n)})) : []});
  if (cap) cap.innerHTML = chartCaption(s);
  // The page CSP blocks inline style attributes; colours go through the CSSOM instead.
  document.querySelectorAll("[data-bg]").forEach(el => {el.style.background = el.dataset.bg;});
  document.querySelectorAll("[data-fg]").forEach(el => {el.style.color = el.dataset.fg;});
}
function setMaList(list) {
  state.maList = maSet(list); save("stock-watch.ma", state.maList);
  const bar = $("#ma-bar"); if (bar) bar.innerHTML = maBar();
  redrawChart();
}
function movingAverage(bars, n) {
  const out = []; let sum = 0;
  bars.forEach((b, i) => {sum += Number(b.close); if (i >= n) sum -= Number(bars[i - n].close); out.push(i >= n - 1 ? sum / n : NaN);});
  return out;
}
function drawChart(bars, canvas, opts = {}) {
  state.chartCleanup?.(); state.chartCleanup = null;
  if (!canvas || !bars.length) return;
  const period = opts.period || 60, start = Math.max(0, bars.length - period), rows = bars.slice(start), tip = $(".chart-tip", canvas.parentElement);
  const lines = (opts.mas || []).map(m => ({...m, values: movingAverage(bars, m.n).slice(start)}));
  const macd = opts.macd ? opts.macd.slice(start) : null;
  canvas.classList.toggle("with-macd", Boolean(macd));
  const left = 8, right = 52, top = 12, volH = 46, gap = 18, axis = 18, macdGap = macd ? 16 : 0, macdH = macd ? 80 : 0;
  let hover = -1;
  function draw() {
    const width = canvas.clientWidth, height = canvas.clientHeight, ratio = devicePixelRatio || 1;
    canvas.width = Math.round(width * ratio); canvas.height = Math.round(height * ratio);
    const ctx = canvas.getContext("2d"); ctx.setTransform(ratio, 0, 0, ratio, 0, 0); ctx.clearRect(0, 0, width, height);
    const plotW = width - left - right, plotH = height - top - volH - gap - macdGap - macdH - axis;
    if (plotW <= 0 || plotH <= 0) return;
    const values = rows.flatMap(r => [r.low, r.high]).concat(lines.flatMap(l => l.values.filter(Number.isFinite)));
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
    const path = (series, scaleY, color, width = 1.4) => {
      ctx.strokeStyle = color; ctx.lineWidth = width; ctx.beginPath(); let started = false;
      series.forEach((v, i) => {if (!Number.isFinite(v)) {started = false; return;} if (!started) {ctx.moveTo(x(i), scaleY(v)); started = true;} else ctx.lineTo(x(i), scaleY(v));});
      ctx.stroke(); ctx.lineWidth = 1;
    };
    lines.forEach(l => path(l.values, y, l.color));
    const macdTop = volTop + volH + macdGap, bottom = macd ? macdTop + macdH : volTop + volH;
    if (macd) {
      const pick = key => macd.map(m => Number.isFinite(Number(m?.[key])) ? Number(m[key]) : NaN);
      const dif = pick("dif"), dea = pick("dea"), hist = pick("hist");
      const span = Math.max(...[...dif, ...dea, ...hist].filter(Number.isFinite).map(Math.abs), 1e-6);
      const my = v => macdTop + macdH / 2 - v / span * (macdH / 2);
      ctx.strokeStyle = "#e3e7ed"; ctx.beginPath(); ctx.moveTo(left, Math.round(my(0)) + .5); ctx.lineTo(width - right, Math.round(my(0)) + .5); ctx.stroke();
      hist.forEach((v, i) => {if (!Number.isFinite(v)) return; ctx.fillStyle = v >= 0 ? "#d0393b" : "#17875a"; ctx.globalAlpha = .6; ctx.fillRect(Math.round(x(i)) - 1, Math.min(my(0), my(v)), 2, Math.abs(my(v) - my(0))); ctx.globalAlpha = 1;});
      path(dif, my, "#1f2937", 1.2); path(dea, my, "#d4a017", 1.2);
      ctx.fillStyle = "#6a7584"; ctx.textAlign = "left"; ctx.fillText("MACD", width - right + 6, macdTop + 10);
    }
    ctx.fillStyle = "#6a7584";
    [0, Math.floor(rows.length / 2), rows.length - 1].forEach((i, j) => {ctx.textAlign = j === 0 ? "left" : j === 2 ? "right" : "center"; ctx.fillText(String(rows[i].date).slice(5), j === 0 ? left : j === 2 ? width - right : x(i), height - 4);});
    if (hover >= 0) {ctx.strokeStyle = "#9aa3af"; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(x(hover), top); ctx.lineTo(x(hover), bottom); ctx.stroke(); ctx.setLineDash([]);}
  }
  const move = event => {
    const rect = canvas.getBoundingClientRect(), step = (rect.width - left - right) / rows.length;
    hover = Math.max(0, Math.min(rows.length - 1, Math.floor((event.clientX - rect.left - left) / step)));
    const r = rows[hover], m = macd?.[hover];
    const maText = lines.map(l => `MA${l.n} ${fmt(l.values[hover])}`).join("　");
    tip.style.display = "block";
    tip.innerHTML = `${esc(r.date)}<br>开 ${fmt(r.open)}　高 ${fmt(r.high)}<br>低 ${fmt(r.low)}　收 ${fmt(r.close)}${maText ? `<br>${maText}` : ""}${m ? `<br>DIF ${fmt(m.dif, 3)}　DEA ${fmt(m.dea, 3)}　柱 ${fmt(m.hist, 3)}` : ""}`;
    draw();
  };
  const leave = () => {hover = -1; tip.style.display = "none"; draw();};
  canvas.addEventListener("pointermove", move); canvas.addEventListener("pointerleave", leave);
  const observer = new ResizeObserver(draw); observer.observe(canvas); draw();
  state.chartCleanup = () => {observer.disconnect(); canvas.removeEventListener("pointermove", move); canvas.removeEventListener("pointerleave", leave);};
}

// Performance: mature, pending and unknown stay separate; ratios need enough mature samples.
// W16: page through what was read, and say so when the read limit cut the list.
const PAGE_SIZE = 50;
function pager(key, rows, limit) {
  const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  const page = Math.min(Math.max(1, state.pages?.[key] || 1), pages);
  const cut = limit && rows.length >= limit ? ` · 已到读取上限 ${fmt(limit, 0)} 条，更早的记录没有读取` : "";
  const nav = pages > 1 ? `<button type="button" class="btn sm" data-page="${key}:${page - 1}"${page <= 1 ? " disabled" : ""}>上一页</button><button type="button" class="btn sm" data-page="${key}:${page + 1}"${page >= pages ? " disabled" : ""}>下一页</button>` : "";
  return {slice: rows.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE),
    controls: `<div class="toolbar pager"><span class="count">共 ${fmt(rows.length, 0)} 条 · 第 ${page} / ${pages} 页（每页 ${PAGE_SIZE} 条）${cut}</span>${nav}</div>`};
}
const limitNote = (count, limit, what = "条") => Number(count) >= limit ? `<span class="src warn-text">已到读取上限 ${fmt(limit, 0)} ${what}，更早的没有读取</span>` : `<span class="src">最多 ${fmt(limit, 0)} ${what}</span>`;
function renderPerformance(d) {
  state.performance = d;
  const all = d.records || [], read = all.slice(0, LIMITS.recommendations);
  const range = state.perfRange || {}, inRange = r => (!range.from || String(r.date) >= range.from) && (!range.to || String(r.date) <= range.to);
  const filtered = read.filter(inRange), {slice: recs, controls} = pager("performance", filtered, all.length >= LIMITS.recommendations ? LIMITS.recommendations : 0);
  const dateBar = `<div class="toolbar"><label>从 <input type="date" id="perf-from" value="${esc(range.from || "")}"></label><label>到 <input type="date" id="perf-to" value="${esc(range.to || "")}"></label><span class="count">${range.from || range.to ? `日期范围内 ${fmt(filtered.length, 0)} / 已读 ${fmt(read.length, 0)} 条` : "日期范围只在已读取的记录里筛"}</span></div>`;
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
    panel(`推荐记录 · ${filtered.length}`, statusBar + dateBar + controls + table(["推荐日期 / 标的", "到期", "评估状态", "AI 评审", {t:"收益", cls:"r"}, {t:"最大浮盈", cls:"r"}, {t:"收盘回撤", cls:"r"}, "原因代码"], rows, 1000));
}

// A job row says which attempt this was, why it stopped, and what the screen saw.
const GATE_TEXT = {risk_evidence_missing:"四项风险证据不足", indicator_coverage:"指标覆盖不足", snapshot_incomplete:"收盘快照不完整",
  market_stats_unconfirmed:"市场统计未确认", waiting_snapshot:"当日完整收盘数据未就绪", report_gate:"报告版本门控未通过", exception:"扫描异常"};
const FUNNEL = [["input", "行情"], ["risk_tuple_complete", "风险四项已知"], ["tradable", "可交易"], ["indicator_targets", "指标目标"], ["enriched", "指标可算"], ["candidate_count", "候选"]];
function jobBadge(r) {
  if (r.state !== "missed") return r.state === "failed" && r.next_retry_at ? kindBadge("info", "等待重试", "failed") : badge(r.state);
  if (r.stop === "gate_unpassable") return kindBadge("warn", "门槛不可通过·已停止", r.stop);
  return kindBadge("crit", {retry_exhausted:"重试用完·已终止", crossed_trading_date:"跨日终止"}[r.stop] || "已终止", r.stop || "missed");
}
function jobExplain(r, limits = {}) {
  const codes = (r.failure_codes || []).map(c => GATE_TEXT[c] || c).join("、"), max = limits?.max_attempts;
  const tries = r.attempts ? `第 ${r.attempts}${max ? ` / ${max}` : ""} 次尝试` : "", after = tries ? `${tries}后` : "";
  const window = finite(limits?.retry_window_seconds) ? `${fmt(limits.retry_window_seconds / 3600, 1)} 小时` : "";
  const bound = [max ? `${max} 次` : "", window].filter(Boolean).join(" / ");
  const main = r.stop === "gate_unpassable" ? `${after}停止：${codes || "正式风险门槛"}在当前代码下不可能通过（四项风险字段还没有获验收的来源），重复同一筛选不会改变结果；风险证据通过验收前每天都会这样`
    : r.stop === "retry_exhausted" ? `${after}停止：重试次数或时间窗用完${bound ? `（上限 ${bound}）` : ""}${codes ? `；最后一次失败：${codes}` : ""}`
    : r.stop === "crossed_trading_date" ? "到下一个交易日仍未完成，按规则终止，避免隔日误发"
    : r.stop ? `${after}终止，终止原因没有记录`
    : r.state === "failed" ? `${tries || "本次"}失败${codes ? `：${codes}` : ""}${r.next_retry_at ? `；下次尝试 ${bj(r.next_retry_at)}` : ""}`
    : r.state === "completed" ? "已完成" : r.state === "running" ? "运行中" : "—";
  const g = r.gate, counts = g?.counts || {};
  const funnel = FUNNEL.filter(([k]) => counts[k] !== undefined).map(([k, t]) => `${t} ${fmt(counts[k], 0)}`).join(" → ");
  const watch = counts.observation_universe_targets ? `观察口径：成交额前 ${fmt(counts.observation_universe_targets, 0)} 只（含风险未核验），指标可用 ${fmt(counts.observation_universe_enriched, 0)}` : "";
  const record = g ? [g.attempt ? `第 ${fmt(g.attempt, 0)} 次尝试的门控记录` : "门控记录", g.generation ? `输入第 ${fmt(g.generation, 0)} 代` : "", g.phase ? `阶段 ${g.phase}` : "", g.recorded_at ? bj(g.recorded_at) : ""].filter(Boolean).join(" · ") : "";
  return esc(main) + [funnel && `漏斗：${funnel}`, watch, record].filter(Boolean).map(t => `<small>${esc(t)}</small>`).join("") + (r.error && !(r.failure_codes || []).length ? `<small class="mono">${esc(r.error)}</small>` : "");
}
// Later runs for an accepted date are listed next to the original verdict; they never rewrite it.
const REVIEW = {
  late_publication:"最新状态复核：验收之后该日才有自动收盘正式发布；原验收记录的是当时状态，保留不改",
  late_screen_not_formal:"最新状态复核：验收之后才有该日完成的筛选，但不是自动收盘正式发布；补跑不算正式冻结，原验收结论保留",
  late_screen:"最新状态复核：验收之后该日又有完成的筛选；原验收结论保留",
  no_later_evidence:"最新状态复核：验收之后没有该日新的筛选或发布"
};
function acceptanceFollowUp(r) {
  const runs = (r.runs || []).map(x => `${JOB_NAMES[x.job] || x.job} ${(STATUS[x.status] || [x.status || "未知"])[0]}${finite(x.candidates) ? ` · ${fmt(x.candidates, 0)} 只` : ""}${Number(x.report_version) > 0 ? ` · 报告 v${x.report_version}` : ""} · ${bj(x.finished_at)}${x.after_check ? "（验收之后）" : ""}`);
  const lines = [...(runs.length ? runs : ["该日没有筛选运行记录"]), r.publication ? `自动收盘正式发布 ${bj(r.publication.created_at)}` : "没有自动收盘正式发布"];
  if (r.review) lines.push(REVIEW[r.review] || r.review);
  return lines.map(t => `<small>${esc(t)}</small>`).join("");
}

// O09: a request failing and stored data still being usable can both be true, so each layer gets its own column.
function sourceLayers(d) {
  const live = state.ctx.overview?.data?.live_market, session = state.ctx.overview?.data?.session || {};
  const active = (d.batches || []).find(b => b.state === "active") || (d.batches || [])[0];
  const providerRow = p => {
    const paused = blockedNow(p) || circuitNow(p), streak = Number(p.failure_streak) || 0;
    const can = p.stale ? kindBadge("unk", "旧遥测", "stale_telemetry") : paused ? kindBadge("warn", "暂停中", "paused") : streak ? kindBadge("warn", `最近连续失败 ${streak} 次`, "failing") : kindBadge("ok", "可请求", "requestable");
    const until = p.blocked_until ? `${bj(p.blocked_until)}${blockedNow(p) ? "（生效中）" : "（已过期）"}` : p.circuit_open_until ? `${bj(p.circuit_open_until)}${circuitNow(p) ? "（生效中）" : "（已过期）"}` : "—";
    return [esc(p.name), can, esc(until), rateOnly(p) ? `<span class="muted">未采集</span>` : esc(bj(p.success_at)), rateOnly(p) ? `<span class="muted">不适用</span>` : badge(p.quality),
      p.stale ? "不代表当前：插件已不再更新这类遥测" : "看下面的数据行：接口失败时已存数据照样能读"];
  };
  const rows = [
    ["日线（插件 raw 批次）", `<span class="muted">按计划请求，见任务表</span>`, "—", esc(active?.date || "未知"), active ? badge(active.state) : badge("unknown"),
      active ? `可用：读已发布的第 ${esc(active.generation ?? "?")} 代批次（${fmt(active.rows, 0)} 行）` : "不可用：没有已发布批次"],
    ["盘中报价（插件盯盘文件）", live?.status === "available" ? kindBadge("ok", "在更新", "available") : kindBadge("unk", "未在更新", live?.reason || "live_market_missing"), "—",
      esc(live?.at ? bj(live.at) : "—"), badge(live?.status || "unknown"), esc(SESSION_HINT[session.phase] || "交易日历未知")],
    ...(d.providers || []).map(providerRow),
  ];
  return panel("数据源分层（O09）", table(["来源", "能不能请求", "暂停 / 熔断到", "最近成功 / 数据日", "质量", "业务能不能用"], rows.map(r => `<tr>${r.map(c => `<td class="wrap">${c}</td>`).join("")}</tr>`), 980),
    `<span class="src">请求能力、限流、最近成功、质量、业务可用分开看</span>`);
}
// W18: a plain-text summary of what the page shows; it holds no keys, tokens or chat ids because the API never sends them.
function healthSummaryText(d) {
  const plain = html => String(html ?? "").replace(/<br\s*\/?>/g, "；").replace(/<[^>]+>/g, "").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, "&").trim();
  const acc = d.daily_acceptance || [], findings = state.ctx.overview?.data?.acceptance?.findings || [];
  const lines = [`# Stock Watch 健康摘要`, `导出时间：${bj(new Date().toISOString())}`, `数据日：${d.data_date || "未知"}`, `Web：${state.ctx.version?.data?.build?.revision || "未知"}`, "",
    "## 最近验收", ...acc.slice(0, 5).map(r => `- ${r.date} ${r.status}：${r.summary || "—"}（检查 ${bj(r.checked_at)}）`), "",
    "## 当前发现", ...(findings.length ? findings.map(f => `- ${typeof f === "string" ? f : f.code || f.reason || JSON.stringify(f)}：${plain(reasonText(typeof f === "string" ? f : f.code || ""))}`) : ["- 无"]), "",
    "## 任务", ...(d.jobs || []).slice(0, 15).map(r => `- ${r.date} ${JOB_NAMES[r.name] || r.name} ${r.status}：${plain(jobExplain(r, d.automatic_close_limits))}`), "",
    "## 数据源", ...(d.providers || []).map(p => `- ${p.name}：${providerKind(p)}；${plain(providerText(p))}`), "",
    "## 原始数据批次", ...(d.batches || []).slice(0, 3).map(b => `- ${b.date} 第 ${b.generation} 代 ${b.state} ${b.rows} 行`)];
  return lines.join("\n") + "\n";
}
function exportHealth() {
  const d = state.health;
  if (!d) return;
  const url = URL.createObjectURL(new Blob([healthSummaryText(d)], {type: "text/markdown;charset=utf-8"}));
  const a = Object.assign(document.createElement("a"), {href: url, download: `stock-watch-health-${d.data_date || "unknown"}.md`});
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function renderHealth(d) {
  state.health = d;
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
  const accHeads = ["交易日", "检查时间", "状态", "摘要", "同日运行与复核"], jobHeads = ["日期", "任务", "状态", "说明"];
  const accRow = r => `<tr><td>${esc(r.date)}</td><td>${esc(bj(r.checked_at))}</td><td>${badge(r.status)}</td><td class="wrap">${esc(r.summary || "—")}</td><td class="wrap">${acceptanceFollowUp(r)}</td></tr>`;
  const jobRow = r => `<tr><td>${esc(r.date)}</td><td>${esc(JOB_NAMES[r.name] || r.name)}</td><td>${jobBadge(r)}</td><td class="wrap">${jobExplain(r, d.automatic_close_limits)}</td></tr>`;
  const [accNow, accOld] = split(d.daily_acceptance || [], "date"), [jobsNow, jobsOld] = split(d.jobs || [], "date");
  const fails = (d.failures || []).map(r => `<tr><td>${esc(r.code)}</td><td>${badge(r.state)}</td><td>${badge(r.risk)}</td><td>${esc(bj(r.at))}</td></tr>`);
  const outbox = (title, o) => panel(title, Object.keys(o || {}).length ? `<div class="status-rows">${Object.entries(o).map(([s, n]) => `<div class="status-row">${badge(s, DELIVERY)}<span class="num">${fmt(n, 0)}</span></div>`).join("")}</div>` : empty("没有投递记录", "no_records"));
  return pageHead("系统健康", "", ["持久状态快照，不代表远端服务探活"]) + sec + svc +
    `<div class="toolbar"><button type="button" class="btn" data-health-export>${ic("database")}导出摘要（不含密钥 / 会话号）</button><span class="count">把验收、当前发现、任务、数据源整理成一份文本，方便发给别人看</span></div>` +
    sourceLayers(d) +
    panel("数据源接口", table(["接口 / 遥测", "状态", "最近成功", "最近错误", "质量记录", "错误", "暂停 / 熔断到", "状态更新"], prov, 980), `<span class="src">每类最多 30 条</span>`) +
    panel("原始数据批次", table(["批次", "数据日期", {t:"代", cls:"r"}, "状态", {t:"行数", cls:"r"}, "口径", "发布时间"], batches, 760), `<span class="src">最近 10 批</span>`) +
    panel(`每日链路验收 · 当前数据日 ${esc(current || "未知")}`, table(accHeads, accNow.map(accRow), 900, {empty:empty("当前数据日没有验收记录", "daily_acceptance_missing")}) + findingHelp(state.ctx.overview?.data?.acceptance?.findings) + history(accHeads, accOld.map(accRow), 900), `<span class="src">最近 10 次 · 原验收记录不改写</span>`) +
    panel(`任务 · 当前数据日 ${esc(current || "未知")}`, table(jobHeads, jobsNow.map(jobRow), 760, {empty:empty("当前数据日没有任务记录", "no_records")}) + history(jobHeads, jobsOld.map(jobRow), 760), `<span class="src">最近 15 条</span>`) +
    `<div class="grid g-1-1"><div class="col">${panel("失败与风险标的", table(["标的", "状态", "风险", "时间"], fails, 480), `<span class="src">最近 20 条</span>`)}</div><div class="col">${outbox("盘中信号投递", d.outbox)}${outbox("自动收盘推送", d.automatic_outbox)}${outbox("每日验收推送", d.daily_acceptance_outbox)}</div></div>`;
}
// C10: how the screen's caps chain, in the order the plugin applies them (values from the load-time snapshot).
const CAP_CHAIN = [
  ["price_min", "price_max", "价格区间", "价格不在区间的股票不进入后面任何一步"],
  ["deep_screen_limit", null, "深筛名额", "停牌 / 涨跌停 / ST 都核验为否的股票里，按成交额取前 N 只算指标和技术分"],
  ["factor_screen_limit", null, "因子名额", "深筛里技术分前 N 只才去取 ST / 审计和基本面字段；其余拿不到，会被算成“风险未知”，进不了正式候选"],
  ["official_evidence_candidate_limit", null, "官方证据名额", "因子名额里前 N 只再查官方公告证据"],
  ["min_score", null, "最低技术分", "风险复核通过且技术分不低于它的才算达标"],
  ["candidate_limit", null, "候选上限", "达标的按排名取前 N 只写成正式候选"],
  ["fallback_limit", null, "观察候选上限", "一只都没达标时，最多列出 N 只未达最低分的观察候选"],
  ["report_candidate_limit", null, "报告展示上限", "推送报告里最多展示 N 只"],
];
function capsPanel(items) {
  const byKey = new Map(items.map(r => [r.key, r]));
  const show = key => {const r = byKey.get(key); if (!r) return `<span class="muted">未知</span>`; return r.state === "shown" && r.effective !== null && r.effective !== undefined ? `${esc(r.effective)}${r.differs ? `<small>默认 ${esc(r.default)}</small>` : ""}` : `<span class="muted">未知</span><small>默认 ${esc(r.default ?? "—")}</small>`;};
  const rows = CAP_CHAIN.map(([key, second, label, help], i) => `<div class="status-row"><span>${i + 1}. ${label}<small>${help}</small></span><span class="num">${second ? `${show(key)} – ${show(second)}` : show(key)}<small class="mono">${esc(second ? `${key} / ${second}` : key)}</small></span></div>`).join("");
  return panel("筛选上限怎么串起来（C10）", `<div class="status-rows">${rows}</div><p class="regime-note">顺序就是插件实际执行的顺序；每一步只在上一步留下的股票里做。候选页的“筛选漏斗”显示每次筛选在每一步还剩多少只。</p>`, `<span class="src">只读 · 来自插件加载时的配置快照</span>`);
}
// C06: groups by what the setting is for; the first matching group wins, so the order matters.
const SETTING_GROUPS = [
  ["push", "启停与推送", /^(enabled|paper_trading_only|push_|allow_self_whitelist|automatic_delivery_|daily_acceptance_alert|web_watch_)/],
  ["source", "数据源与快照", /^(tushare_|daily_cache_|daily_snapshot_|daily_market_url|market_min_snapshot_size|calendar_|realtime_backup_|request_timeout|max_concurrency|universe_codes|factor_source|factor_data_url)/],
  ["screen", "选股与收盘任务", /^(price_|min_score|deep_screen_limit|factor_screen_limit|screen_|candidate_|report_candidate_limit|fallback_limit|degraded_watch_|daily_scan_time|daily_acceptance|automatic_close_|factor_mode)/],
  ["research", "研究与证据", /^(official_evidence_|market_comparison_|research_)/],
  ["intraday", "盘中盯盘", /^(intraday_|quote_|minute_|watchlist_limit|auto_watch_candidates|confirmation_|cost_)/],
  ["model", "模型与新闻", /^(llm_|news_)/],
];
const settingGroup = key => (SETTING_GROUPS.find(([, , re]) => re.test(key)) || ["ops"])[0];
function settingGroupsHtml(items, heads, row) {
  return [...SETTING_GROUPS, ["ops", "其它 / 运维"]].map(([id, label]) => {
    const rows = items.filter(r => settingGroup(r.key) === id);
    if (!rows.length) return "";
    const changed = rows.filter(r => r.differs).length;
    return `<details class="history setting-group" data-group="${id}"><summary>${esc(label)} · ${fmt(rows.length, 0)} 项${changed ? ` · 已改 ${fmt(changed, 0)}` : ""}</summary>${table(heads, rows.map(row), 760)}</details>`;
  }).join("");
}
const settingValue = (items, key) => { const r = items.find(x => x.key === key); return r && r.state === "shown" ? r.effective : null; };
// C08: orders of magnitude from the effective settings, not measurements.
function budgetPanel(items) {
  const v = key => settingValue(items, key), n = (x, d = 0) => x === null || x === undefined ? "未知" : fmt(x, d);
  const per = (seconds, span) => finite(seconds) && seconds > 0 ? Math.floor(span / seconds) : null;
  const rows = [
    ["盘中报价", `每 ${n(v("quote_interval_seconds"))} 秒一轮，4 小时交易时段约 ${n(per(v("quote_interval_seconds"), 14400))} 轮；每轮把盯盘名单（自选 + 候选 + 重点关注，重点最多 ${n(v("intraday_focus_limit"))} 只）分批请求新浪`, "quote_interval_seconds"],
    ["全市场盘中环境", `每 ${n(v("intraday_market_refresh_seconds"))} 秒一次，交易时段约 ${n(per(v("intraday_market_refresh_seconds"), 14400))} 次`, "intraday_market_refresh_seconds"],
    ["Tushare 日线", `保留 ${n(v("tushare_raw_session_count"))} 个交易日；每个交易日的全市场日线按每页 ${n(v("tushare_bulk_page_size"))} 条分页（约 5,500 只 → ${n(finite(v("tushare_bulk_page_size")) && v("tushare_bulk_page_size") > 0 ? Math.ceil(5500 / v("tushare_bulk_page_size")) : null)} 页），失败重试 ${n(v("tushare_retry_attempts"))} 次；平时每天只新增 1 个交易日`, "tushare_raw_session_count"],
    ["官方证据", v("official_evidence_enabled") === false ? "已关闭" : `每次收盘最多核验 ${n(v("official_evidence_candidate_limit"))} 只候选，结果缓存 ${n(v("official_evidence_cache_seconds"))} 秒`, "official_evidence_enabled"],
    ["模型", v("llm_enabled") === false ? "已关闭" : `每天最多 ${n(v("llm_daily_request_limit"))} 次，两次间隔 ≥ ${n(v("llm_min_interval_seconds"))} 秒；盘中解释${v("llm_annotation_enabled") === false ? "已关闭" : `每 ${n(v("llm_annotation_interval_seconds"))} 秒最多 ${n(v("llm_annotation_limit"))} 条`}`, "llm_daily_request_limit"],
    ["新闻", `每 ${n(v("news_interval_seconds"))} 秒拉一次 RSS，每天约 ${n(per(v("news_interval_seconds"), 86400))} 次`, "news_interval_seconds"],
  ];
  return panel("请求量与额度预估（C08）", `<div class="status-rows">${rows.map(([label, text, key]) => `<div class="status-row"><span>${label}<small class="mono">${esc(key)}</small></span><span class="wrap">${esc(text)}</span></div>`).join("")}</div><p class="regime-note">按当前生效配置推算的上限和量级，不是实测。“5 秒”是轮询间隔，不保证 5 秒内拿到行情：请求本身要时间，失败会退避。改动这些参数前先看这里会多出多少请求。</p>`, `<span class="src">只读 · 估算</span>`);
}
// C09: a clock time is when a job starts, not when the data is complete.
function arrivalPanel(items) {
  const v = key => settingValue(items, key), n = x => x === null || x === undefined ? "未知" : esc(String(x));
  const rows = [
    ["盘后扫描开始", n(v("daily_scan_time")), "插件到点开始取当天全市场日线并筛选；数据没齐会按下面的重试窗口再试"],
    ["当日验收", v("daily_acceptance_enabled") === false ? "已关闭" : n(v("daily_acceptance_time")), "到点核对当天任务的实际运行状态（完成 / 仍在重试 / 超时），不是到点就算失败"],
    ["自动收盘重试", `${n(v("automatic_close_max_attempts"))} 次 · 每 ${n(v("automatic_close_retry_seconds"))} 秒 · 最长 ${n(v("automatic_close_retry_window_seconds"))} 秒`, "风险门槛在当前代码下不可能通过时首轮就停（O04），只有临时取数失败才重试"],
    ["Tushare 日线", "交易日 15:00—16:00 入库", "Tushare 文档说明的入库时间，个别日子更晚；扫描时间晚于它才不必等"],
    ["两融明细", "沪市当晚 · 深市常到下一交易日", "所以“两融拥挤”用最近一个沪深都已发布的交易日"],
    ["解禁事件", "东财晚间陆续补登", "10-07 实测 18:13 / 18:41 / 19:12 三次各多出新登记的事件；研究信号 21:40 那次可以补上"],
  ];
  return panel("数据什么时候到（C09）", `<div class="status-rows">${rows.map(([label, value, help]) => `<div class="status-row"><span>${label}<small>${esc(help)}</small></span><span class="num">${value}</span></div>`).join("")}</div>`, `<span class="src">时间为北京时间；外部数据时间是经验值，不保证</span>`);
}
// C11: only whether a credential is filled in, never its content.
const CREDENTIALS = [["tushare_token", "Tushare 日线与交易日历", "只需要日线、交易日历的读取权限"], ["llm_api_key", "模型解释与研究摘要（llm_*）", "只需要对话补全；建议单独开一个低额度的 key"]];
function credentialPanel(items) {
  const rows = CREDENTIALS.map(([key, use, scope]) => {
    const r = items.find(x => x.key === key), filled = r && r.state === "custom";
    const state = !r || r.state === "unknown" ? kindBadge("unk", "未知", "snapshot_missing") : filled ? kindBadge("info", "已填写 · 未验证", "configured_unverified") : kindBadge("warn", "未填写", "empty");
    return `<div class="status-row"><span>${esc(use)}<small class="mono">${esc(key)}</small></span><span>${state}<small>${esc(scope)}</small></span></div>`;
  }).join("");
  return panel("凭据状态（C11）", `<div class="status-rows">${rows}</div><p class="regime-note">只显示有没有填写，不显示内容，也不代表能用：能不能用看健康页的接口状态。默认关闭的功能保持关闭。</p>`, `<span class="src">只读</span>`);
}
// C12: one line per plugin load, newest first; only key names, never values.
function settingsHistoryPanel(h) {
  const items = h?.items || [];
  const groupName = key => (SETTING_GROUPS.find(([id]) => id === settingGroup(key)) || [null, "其它 / 运维"])[1];
  const row = r => `<tr><td>${esc(bj(r.written_at))}</td><td>${esc(r.plugin_version || "—")}<small class="mono">${esc((r.code_sha256 || "").slice(0, 12) || "—")}</small></td><td class="wrap">${r.first ? "第一次记录（更早的修改无从查起）" : r.changed.length ? esc(r.changed.join("、")) : "与上次加载相同"}</td><td class="wrap">${r.first || !r.changed.length ? "—" : esc([...new Set(r.changed.map(groupName))].join("、"))}</td></tr>`;
  const body = items.length ? table(["加载时间", "插件版本 / main.py", "改了哪些设置", "影响哪类功能"], items.map(row), 760)
    : empty(h?.status === "missing" ? "还没有加载记录：插件更新到带这个功能的版本并重载后开始记录" : "没有加载记录", h?.status || "settings_history_unavailable");
  return panel("配置变更记录（C12）", body + `<p class="regime-note">每次插件加载记一条：时间、版本、和上次加载比改了哪些键（只记键名；不公开的字符串只看得出“未填写 / 默认 / 已修改”之间的变化）。在 AstrBot 面板改配置要重载插件才生效，这里也是重载后才多一条。兼容保护项 tushare_raw_require_universe_evidence 设成 false 也不会关掉全市场完整性保护（代码里固定开启）。</p>`, `<span class="src">只读 · 最近 20 次加载</span>`);
}
function renderSettings(d) {
  const names = {min_score:"最低技术分", price_min:"最低价格", price_max:"最高价格", deep_screen_limit:"技术深筛上限", factor_screen_limit:"因子筛选上限", screen_min_indicator_coverage:"最低指标覆盖", intraday_confirmation_periods:"连续确认次数", intraday_cooldown_seconds:"信号冷却（秒）", intraday_min_amount:"最低成交额", market_comparison_enabled:"量价对照", market_comparison_benchmark:"指定基准指数", paper_trading_only:"仅研究 / 模拟", price_plan_close_tolerance_pct:"收盘计划偏差容限", official_evidence_enabled:"官方证据核验", official_evidence_candidate_limit:"官方证据候选上限", official_evidence_cache_seconds:"官方证据缓存（秒）"};
  const s = d.snapshot || {}, items = d.items || [];
  const STATE = {empty:"未填写", default:"与默认相同（值不公开）", custom:"已修改（值不公开）", invalid_type:"类型不符（值不公开）", unknown:"未知"};
  const SOURCE = {plugin_snapshot:"来源：插件加载时的配置快照", explicit_public_snapshot:"来源：显式公开配置快照", effective_unknown:"来源：没有读到插件配置"};
  const value = v => typeof v === "boolean" ? (v ? "开启" : "关闭") : v === null || v === undefined || v === "" ? `<span class="muted">空</span>` : esc(v);
  const current = r => r.state === "shown" ? value(r.effective) : `<span class="muted">${esc(STATE[r.state] || "未知")}</span>`;
  const row = r => `<tr><td>${esc(names[r.key] || r.key)}<small class="mono">${esc(r.key)}</small></td><td class="num">${current(r)}${r.differs ? ` ${kindBadge("warn", "已改", "differs_from_default")}` : ""}</td><td class="num">${value(r.default)}</td><td class="wrap">${esc(r.label || "—")}<small>${esc(SOURCE[r.source] || "来源未知")}</small></td></tr>`;
  const heads = ["参数", "当前值", "默认值", "说明 / 来源"];
  const common = items.filter(r => r.group === "common"), others = items.filter(r => r.group !== "common"), changed = items.filter(r => r.differs);
  const SNAP = {missing:"没有读到插件配置快照：插件还没加载带这个功能的版本，或 Web 读不到插件数据目录。", unreadable:"插件配置快照无法读取。",
    invalid:"插件配置快照格式不对。", not_configured:"Web 没有配置插件快照的位置。"};
  const build = s.matches_web_build === true ? "与当前 Web 构建是同一提交" : s.matches_web_build === false ? "与当前 Web 构建的提交不同" : "无法与 Web 构建比对";
  const head = s.status === "plugin_snapshot"
    ? notice("info", "clock", `当前值来自插件加载时写出的配置快照：加载于 ${esc(bj(s.written_at))} · 插件 ${esc(s.plugin_version || "版本未知")} · main.py ${esc((s.code_sha256 || "").slice(0, 12) || "未知")}（${esc(build)}）。面板改配置后，插件重新加载才会更新这里。`)
    : s.status === "explicit_values" ? notice("info", "lock", "当前值来自显式指定的公开配置快照，不是插件自己写出的。")
    : notice("warn", "triangle-alert", `${esc(SNAP[s.status] || "插件配置快照状态未知。")}当前值显示为未知；默认值不代表正在运行的配置。`);
  const deprecated = (s.deprecated_settings || []).length ? notice("warn", "triangle-alert", `这些旧配置项设成了非默认值，但当前代码不读取：${esc(s.deprecated_settings.join("、"))}。在面板改回默认可以消掉加载告警，实际行为不变。`) : "";
  const issues = (s.setting_issues || []).map(i => notice(i.level === "error" ? "crit" : "warn", "triangle-alert",
    `配置检查：${esc(i.message)}。${esc(i.effect)}。<small class="mono">${esc((i.keys || []).join(" · "))}</small>`)).join("");
  return pageHead("策略设置", "", ["只读"]) + head + deprecated + issues + capsPanel(items) +
    `<div class="grid g-1-1">${budgetPanel(items)}${arrivalPanel(items)}</div>` + credentialPanel(items) + settingsHistoryPanel(d.history) +
    (changed.length ? panel(`与默认值不同 · ${fmt(changed.length, 0)} 项`, table(heads, changed.map(row), 760)) : "") +
    panel("常用参数", table(heads, common.map(row), 760)) +
    panel(`全部参数 · ${fmt(items.length, 0)} 项`, settingGroupsHtml(others, heads, row), `<span class="src">按用途分组（C06）；常用参数已在上面单列</span>`) +
    notice("info", "lock", "令牌、密钥、地址、推送白名单、路径等字段只显示“未填写 / 与默认相同 / 已修改”，不显示内容。");
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
async function contextJobs(view, signal, parts = []) {
  const jobs = {};
  if (["overview", "candidates", "research", "stock"].includes(view)) {
    const code = view === "stock" ? parts[1] || state.selectedCode : null;
    jobs.signals = soft(/^\d{6}$/.test(code || "") ? `research_signals?code=${encodeURIComponent(code)}` : "research_signals", signal);
  }
  if (view !== "overview") jobs.overview = soft("overview", signal);
  if (view !== "health") jobs.health = soft("health", signal);
  if (view === "health") jobs.version = soft("version", signal);
  if (view === "research" || view === "stock") jobs.catalog = soft("research_catalog", signal);
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
  if (ctx.signals) state.ctx.signals = ctx.signals;
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
  const ctxJob=contextJobs(view,signal,parts);
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
    if(view==="stock"){redrawChart();refreshWatchSlot(payload.data?.code);}
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
document.addEventListener("submit",event=>{
  if(event.target.id==="stock-search-form"){event.preventDefault();searchStocks();}
  if(event.target.id==="ma-add-form"){event.preventDefault();const n=Number($("#ma-add")?.value);if(Number.isInteger(n)&&n>=2&&n<=250&&!state.maList.some(m=>m.n===n))setMaList([...state.maList,{n,on:true}].sort((a,b)=>a.n-b.n));}
});
document.addEventListener("change",event=>{
  if(["candidate-risk","candidate-sort"].includes(event.target.id))filterCandidates();
  if(event.target.id==="signal-filter")$("#signal-rows").innerHTML=signalRows(state.signals.filter(r=>!event.target.value || r.state===event.target.value));
  if(event.target.id==="ma-toggle" && state.stock){state.ma=event.target.checked;redrawChart();}
  if(event.target.id==="macd-toggle" && state.stock){state.macdOn=event.target.checked;save("stock-watch.macd",state.macdOn);redrawChart();}
  if(event.target.id==="research-sort"){state.researchSort=event.target.value;$("#content").innerHTML=renderResearch();finalize();}
  if(event.target.dataset?.researchCol){const i=Number(event.target.dataset.researchCol),hidden=researchHidden();event.target.checked?hidden.delete(i):hidden.add(i);save("stock-watch.research-hide",[...hidden]);const box=$(".research-pools");if(box)box.className=researchPoolsClass();}
  if(["perf-from","perf-to"].includes(event.target.id) && state.performance){state.perfRange={...(state.perfRange||{}),[event.target.id==="perf-from"?"from":"to"]:event.target.value};state.pages={...(state.pages||{}),performance:1};$("#content").innerHTML=renderPerformance(state.performance);finalize();}
  if(event.target.id==="adjust-toggle" && state.stock){state.adjusted=event.target.checked;save("stock-watch.adjust",state.adjusted);redrawChart();}
});
document.addEventListener("click",event=>{
  const t=event.target;
  const watchAdd=t.closest("[data-watch-add]");
  if(watchAdd){if(!watchAdd.disabled)addToWatch(watchAdd);return;}
  if(t.closest("[data-health-export]")){exportHealth();return;}
  const researchFilter=t.closest("[data-research-filter]");
  if(researchFilter){state.researchFilter=researchFilter.dataset.researchFilter;$("#content").innerHTML=renderResearch();finalize();return;}
  const pageBtn=t.closest("[data-page]");
  if(pageBtn && !pageBtn.disabled && state.performance){const [key,page]=pageBtn.dataset.page.split(":");state.pages={...(state.pages||{}),[key]:Number(page)};$("#content").innerHTML=renderPerformance(state.performance);finalize();return;}
  const horizon=t.closest("[data-horizon]"), period=t.closest("[data-period]"), sort=t.closest("[data-sort]"), board=t.closest("[data-board]"), chip=t.closest("[data-chip]"), row=t.closest("tr.click");
  if(horizon){state.horizon=Number(horizon.dataset.horizon);load();return;}
  if(period && state.stock){state.period=Number(period.dataset.period);document.querySelectorAll("[data-period]").forEach(b=>b.classList.toggle("active",Number(b.dataset.period)===state.period));redrawChart();return;}
  const maChip=t.closest("[data-ma]"), maRemove=t.closest("[data-ma-remove]");
  if(maChip && state.stock){const n=Number(maChip.dataset.ma);setMaList(state.maList.map(m=>m.n===n?{...m,on:!m.on}:m));return;}
  if(maRemove && state.stock){const n=Number(maRemove.dataset.maRemove);setMaList(state.maList.filter(m=>m.n!==n));return;}
  if(t.closest("[data-ma-reset]") && state.stock){setMaList(MA_DEFAULT.map(n=>({n,on:true})));return;}
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
    } else if (artifactChanged && state.view === "stock") {
      if(await refreshStockLive()) state.artifactRevision = artifactRevision;
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
