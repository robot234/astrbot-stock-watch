"""Read-only projections over existing plugin tables; no StockStore lifecycle."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import statistics
import time
from data_evidence import resolve as resolve_evidence, stamp as evidence_stamp
import paper_forward as forward
from paper_review import review_persisted


ROOT = Path(__file__).resolve().parents[1]
CHINA = timezone(timedelta(hours=8))
TABLES = {
    "web_demo_metadata", "screen_runs", "screen_candidates", "active_candidate_runs",
    "daily_quotes", "daily_bars", "market_contexts", "daily_snapshot_meta", "batches", "datasets", "active_generations",
    "provider_health", "provider_api_state", "job_runs", "screen_gate_diagnostics", "intraday_event_outbox",
    "intraday_signal_states", "automatic_close_deliveries", "automatic_close_publications", "recommendation_records",
    "recommendation_outcomes", "recommendation_ai_review_batches", "recommendation_ai_reviews",
    "daily_acceptance_runs", "daily_acceptance_events", "daily_acceptance_alerts",
    "trading_calendar", "corporate_action_factors",
    "risk_events", "market_comparison_observations",
    "data_evidence_records", "factor_snapshots", "evidence_fetch_cache",
    "research_pool_runs", "research_pool_picks", "paper_qualification_events", "paper_simulated_entries",
    "paper_freeze_contracts", "paper_entry_terms", "paper_no_action_evidence",
    "partition_bars", "day_partitions", "batch_days", "stock_symbols", "schema_meta",
}
API_VERSION = 2
CAPABILITIES = (
    "overview", "signals", "candidates", "research_pools", "research_parameters", "stock_detail",
    "stock_active_raw_bars", "stock_search", "performance", "health", "provider_state_v2",
    "session_calendar", "coverage_breakdown", "settings", "intraday", "revision",
    "snapshot_check_status", "version", "research_catalog", "research_signals",
)
# Written by webapp/research_signals.py each evening; kept as a literal so the server never imports numpy or pandas.
SIGNALS_SCHEMA = "stock_watch_research_signals/v1"
SIGNALS_MAX_AGE_SECONDS = 3 * 86400
SIGNALS_MAX_BYTES = 8 * 1024 * 1024
SIGNAL_FACTORS = ("R20", "C_MA20", "IVOL20", "VOLR5_60", "MAX20", "TURN5_20", "ABTURN", "RSI14")
SIGNAL_EXCLUSIONS = frozenset({"not_main_or_chinext", "st_name", "no_bar_today", "history_lt_60",
                               "corporate_action_60d", "price_above_50", "amount_below_20m", "indicator_missing"})
# Stock-page MACD uses the research schemes H / I definition and stays unverified until scheme I's endpoint.
MACD_SPANS, MACD_CROSS_LOOKBACK, MACD_MIN_SESSIONS = (12, 26, 9), 3, 60
SIGNAL_APPROXIMATIONS = frozenset({"turnover_ratio_from_volume", "st_from_current_name"})
SIGNAL_INDICES = ("000905", "000852")
# Offline research freezes shown read-only; stages are curated from each study's own delivery documents.
RESEARCH_CATALOG = (
    {"file": "ULTRASHORT_REVERSAL_V1_FROZEN.json", "stages": ("not_passed", "forward_pending"),
     "documents": ("ULTRASHORT_REVERSAL_V1_DELIVERY.md", "ULTRASHORT_REVERSAL_V1_EVALUATION.md",
                   "ULTRASHORT_REVERSAL_V1_FORWARD_PLAN.md")},
    {"file": "LLM_SECTOR_FIRST_EXP_V0_FROZEN.json", "stages": ("exploration",),
     "documents": ("LLM_SECTOR_FIRST_EXP_V0.md", "LLM_SECTOR_FIRST_EXP_V0_REVIEW.md")},
)
RESEARCH_STAGES = ("not_passed", "exploration", "forward_pending", "passed")
TRADING_PHASES = ((9 * 60 + 15, "pre_open"), (9 * 60 + 30, "call_auction"), (11 * 60 + 30, "trading"),
                  (13 * 60, "lunch_break"), (15 * 60, "trading"))
COMMON_SETTINGS = {
    "min_score", "price_min", "price_max", "deep_screen_limit", "factor_screen_limit",
    "screen_min_indicator_coverage", "intraday_confirmation_periods",
    "intraday_cooldown_seconds", "intraday_min_amount", "market_comparison_enabled",
    "market_comparison_benchmark", "paper_trading_only", "price_plan_close_tolerance_pct",
    "official_evidence_enabled", "official_evidence_candidate_limit", "official_evidence_cache_seconds",
}
# Kept equal to the plugin's PUBLIC_STRING_SETTINGS; any other string is shown only as a configured state.
PUBLIC_STRING_SETTINGS = frozenset({
    "factor_mode", "factor_source", "tushare_bj_calendar_policy", "tushare_raw_dataset_key", "tushare_universe_statuses",
    "realtime_backup_mode", "daily_scan_time", "daily_acceptance_time", "llm_model", "llm_shadow_prompt_version",
    "market_comparison_benchmark", "tushare_raw_universe_version",
})
CONFIGURED_STATES = {"empty", "default", "custom", "invalid_type"}


def obj(value):
    try:
        result = json.loads(value) if isinstance(value, str) else value
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def arr(value):
    try:
        result = json.loads(value) if isinstance(value, str) else value
        return result if isinstance(result, list) else []
    except (ValueError, TypeError):
        return []


def stable_data_revision(snapshot_revision, artifact_revision):
    """One stable revision for client polling; component revisions stay separate."""
    payload = json.dumps({"snapshot": snapshot_revision or None, "artifact": artifact_revision or None}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def stable_artifact_state_revision(revision, status, reason, market):
    """Expose a client revision when artifact validity changes without new rows."""
    payload = json.dumps({"artifact": revision or None, "status": status or "unknown",
                          "reason": reason or "", "market": market or {}},
                         sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def instant(value):
    try:
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def epoch_instant(value):
    """Provider rate-limit deadlines are stored as Unix seconds; 0 means no deadline."""
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value.strip()):
        return None
    seconds = number(value)
    if seconds is None:
        return instant(value)
    if seconds <= 0:
        return None
    try:
        return datetime.fromtimestamp(seconds, timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def trading_phase(local):
    minute = local.hour * 60 + local.minute
    for end, phase in TRADING_PHASES:
        if minute < end:
            return phase
    return "after_close"


def error_category(value):
    text = str(value or "").lower()
    for token, label in (("permission", "permission_denied"), ("rate", "rate_limited"),
                         ("timeout", "timeout"), ("locked", "database_busy"), ("calendar", "calendar_unknown")):
        if token in text:
            return label
    return "error_recorded" if text else None


GATE_FAILURES = ("risk_evidence_missing", "indicator_coverage", "snapshot_incomplete", "market_stats_unconfirmed")
JOB_ERROR_PHRASES = (("当日完整收盘数据尚未就绪", "waiting_snapshot"), ("报告版本门控未通过", "report_gate"),
                     ("扫描异常", "exception"))
GATE_COUNTS = ("input", "risk_tuple_complete", "tradable", "indicator_targets", "enriched", "candidate_count",
               "observation_universe_targets", "observation_universe_enriched")


def job_failure_codes(value):
    """Only the plugin's own failure codes; free error text can carry paths or provider messages."""
    text = str(value or "")
    return [code for code in GATE_FAILURES if code in text] + [code for phrase, code in JOB_ERROR_PHRASES if phrase in text]


def job_stop(row):
    """Why a terminal automatic job stopped retrying."""
    if str(row.get("status") or "") != "missed":
        return None
    reason = str(row.get("automatic_terminal_reason") or row.get("error") or "")
    for marker, code in (("formal_gate_unpassable", "gate_unpassable"), ("retry bounds exhausted", "retry_exhausted"),
                         ("trading-date boundary", "crossed_trading_date")):
        if marker in reason:
            return code
    return "terminal_unrecorded"


def safe_text(value, limit=120):
    return re.sub(r"[\x00-\x1f\x7f]", "", str(value or ""))[:limit]


def setting_issue_rows(value):
    """The plugin's load-time setting checks, trimmed to display text."""
    return [{"code": safe_text(item.get("code"), 40), "level": item["level"],
             "keys": [safe_text(key, 60) for key in arr(item.get("keys")) if isinstance(key, str)][:4],
             "message": safe_text(item.get("message"), 200), "effect": safe_text(item.get("effect"), 200)}
            for item in arr(value)[:30] if isinstance(item, dict) and item.get("level") in ("error", "warning")]


def sha256_text(value):
    return value if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) else None


def flag(value):
    return value if isinstance(value, bool) else None


def signal_stock(code, overheat):
    """One code's overheat entry, or why it was outside the research pool on that session."""
    entry = obj(obj(overheat.get("stocks")).get(code))
    if entry:
        indicators = obj(entry.get("indicators"))
        return {"code": code, "status": "evaluated", "hot": entry.get("hot") is True, "score": number(entry.get("score")),
                "pct": number(entry.get("pct")), "reason": None,
                "indicators": {key: number(indicators.get(key)) for key in SIGNAL_FACTORS}}
    reason = obj(overheat.get("excluded")).get(code)
    return {"code": code, "status": "excluded" if reason in SIGNAL_EXCLUSIONS else "not_evaluated", "hot": False,
            "score": None, "pct": None, "reason": reason if reason in SIGNAL_EXCLUSIONS else None, "indicators": {}}


def signal_index(item):
    item = obj(item)
    values = {key: number(item.get(key)) for key in ("close", "ma200", "distance_ma200", "ma120", "distance_ma120",
                                                     "sessions_on_side")}
    return {"name": safe_text(item.get("name"), 20) or None, "date": safe_text(item.get("date"), 10) or None, **values,
            "above_ma200": flag(item.get("above_ma200")), "above_ma120": flag(item.get("above_ma120"))}


def catalog_entry(directory, spec):
    """Project one frozen research file; the list stays separate from formal candidates."""
    path = directory / spec["file"]
    if path.stat().st_size > 262144:
        raise ValueError("frozen_file_too_large")
    raw = path.read_bytes()
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("frozen_file_not_object")
    items = []
    for row in arr(data.get("list"))[:20]:
        if not isinstance(row, dict):
            continue
        daily = row.get("source_daily_risk") if isinstance(row.get("source_daily_risk"), dict) else {}
        code = re.sub(r"^(?:sh|sz|bj)\.", "", str(row.get("code") or ""))
        items.append({"rank": int(number(row.get("rank")) or 0) or None,
                      "code": code if re.fullmatch(r"\d{6}", code) else None,
                      "name": safe_text(row.get("name"), 40), "sector": safe_text(row.get("sector_id"), 40) or None,
                      "close": number(row.get("close") if row.get("close") is not None else daily.get("close")),
                      "return5": number(row.get("five_day_continuous_return", row.get("return5"))),
                      "amount20": number(row.get("amount20")),
                      "raw_sha256": safe_text(row.get("raw_sha256") or row.get("raw_file_sha256"), 64) or None})
    risk_basis = data.get("risk_basis") or next((row.get("risk_basis") for row in arr(data.get("list")) if isinstance(row, dict)), "")
    return {"id": safe_text(data.get("rule") or data.get("mode"), 60) or spec["file"], "file": spec["file"],
            "file_sha256": hashlib.sha256(raw).hexdigest(), "label": safe_text(data.get("label"), 80) or None,
            "frozen_at": safe_text(data.get("frozen_at"), 40) or None, "input_as_of": safe_text(data.get("input_as_of"), 20) or None,
            "registration_commit": safe_text(data.get("registration_commit"), 40) or None,
            "execution_commit": safe_text(data.get("execution_commit"), 40) or None,
            "status": safe_text(data.get("status") or data.get("latest_capture_status"), 60) or None,
            "historical_evaluation": safe_text(data.get("historical_evaluation_status"), 160) or None,
            "next_observation": safe_text(data.get("next_observation"), 160) or None,
            "selected_sectors": [safe_text(value, 40) for value in arr(data.get("selected_sectors"))[:10]],
            "risk_basis": safe_text(risk_basis, 160) or None, "price_basis": safe_text(data.get("price_basis"), 60) or None,
            "stages": [stage for stage in spec["stages"] if stage in RESEARCH_STAGES],
            "eligibility": "research_only", "plugin_integrated": False,
            "documents": ["docs/research/" + name for name in spec["documents"]],
            "items": items}


def ohlc(row, basis_key="price_basis"):
    """Positive, internally consistent unadjusted OHLC, else None."""
    values = [number(row.get(key)) for key in ("open", "high", "low", "close")]
    if (any(v is None or v <= 0 for v in values) or values[1] < max(values[0], values[3])
            or values[2] > min(values[0], values[3]) or row.get(basis_key) != "unadjusted"):
        return None
    return values


def macd_state(days, rows):
    """MACD(12, 26, 9) side on the last session, from a price chained by close / pre_close.

    Chaining keeps ex-rights gaps from looking like crosses; sessions after the first usable bar that have
    no usable bar keep the price unchanged. DIF and DEA are rescaled to the latest close, so they read like
    a forward-adjusted chart.
    """
    by_date = {row.get("trade_date"): row for row in rows}
    series, level, last_bar = [], 1.0, None
    for day in days:
        row = by_date.get(day)
        values = ohlc(row, "basis") if row else None
        previous = number(row.get("pre_close")) if values else None
        if values and series and previous and previous > 0:
            level *= values[3] / previous
        if values:
            last_bar = (day, values[3])
        if values or series:
            series.append((day, level))
    if len(series) < MACD_MIN_SESSIONS:
        return {"status": "insufficient_history", "sessions": len(series), "min_sessions": MACD_MIN_SESSIONS}
    alphas = [2 / (span + 1) for span in MACD_SPANS]
    fast = slow = series[0][1]
    dea, above = None, []
    for _, price in series:
        fast += alphas[0] * (price - fast)
        slow += alphas[1] * (price - slow)
        dif = fast - slow
        dea = dif if dea is None else dea + alphas[2] * (dif - dea)
        above.append(dif > dea)
    t = k = len(series) - 1
    while k > 0 and above[k - 1] == above[t]:
        k -= 1
    recent = above[t] and any(above[j] and not above[j - 1]
                              for j in range(max(1, t - MACD_CROSS_LOOKBACK + 1), t + 1))
    scale = last_bar[1] / series[t][1]
    return {"status": "available", "rule": "MACD_12_26_9_CHAINED", "state": "golden" if above[t] else "dead",
            "trade_date": series[t][0], "last_bar_date": last_bar[0], "sessions": len(series),
            "first_session": series[0][0], "cross_date": series[k][0] if k else None,
            "sessions_since_cross": t - k if k else None, "recent_golden_cross": bool(recent),
            "dif": round(dif * scale, 4), "dea": round(dea * scale, 4), "histogram": round(2 * (dif - dea) * scale, 4)}


class Unavailable(RuntimeError):
    pass


class Snapshot:
    def __init__(self, db, origin, now):
        self.db, self.origin, self.now = db, origin, now
        self.notices = set()
        self.tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self._columns = {}
        self.demo = False
        if "web_demo_metadata" in self.tables:
            row = db.execute("SELECT value FROM web_demo_metadata WHERE key='kind'").fetchone()
            self.demo = bool(row and row[0] == "synthetic_demo")

    def snapshot_metadata(self):
        if "web_snapshot_metadata" not in self.tables:
            self.notices.add("web_snapshot_metadata:unavailable")
            return {"status": "unavailable", "reason": "snapshot_metadata_missing",
                    "captured_at": None, "revision": None, "refresh_interval_seconds": None,
                    "poll_interval_seconds": 5}
        try:
            values = dict(self.db.execute("SELECT key,value FROM web_snapshot_metadata"))
        except sqlite3.Error:
            self.notices.add("web_snapshot_metadata:unavailable")
            return {"status": "unavailable", "reason": "snapshot_metadata_unreadable",
                    "captured_at": None, "revision": None, "refresh_interval_seconds": None,
                    "poll_interval_seconds": 5}
        captured = instant(values.get("captured_at"))
        age = (self.now - captured).total_seconds() if captured else None
        fingerprint = values.get("source_fingerprint")
        try:
            parsed_fingerprint = json.loads(fingerprint) if isinstance(fingerprint, str) else fingerprint
            canonical_fingerprint = json.dumps(parsed_fingerprint, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            revision = hashlib.sha256(canonical_fingerprint.encode("utf-8")).hexdigest()[:16] if fingerprint else None
        except (TypeError, ValueError, json.JSONDecodeError):
            revision = None
        incomplete = captured is None or not fingerprint or revision is None
        if incomplete:
            self.notices.add("web_snapshot_metadata:partial")
        return {"captured_at": captured.isoformat() if captured else None,
                "age_seconds": max(0, int(age)) if age is not None else None,
                "status": "partial" if incomplete else "unknown" if age is None or age < 0 else "stale" if age > 7200 else "recent",
                "method": values.get("method"), "integrity": values.get("integrity"),
                "refresh_interval_seconds": 3600, "revision": revision,
                "poll_interval_seconds": 5}

    def columns(self, table):
        if table not in TABLES:
            raise ValueError("table not allowed")
        if table not in self.tables:
            self.notices.add(table + ":unavailable")
            return set()
        if table not in self._columns:
            self._columns[table] = {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}
        return self._columns[table]

    def rows(self, table, *, required=(), where="", params=(), order="", limit=500):
        columns = self.columns(table)
        if not columns or not set(required).issubset(columns):
            self.notices.add(table + ":schema_unavailable")
            return []
        sql = f"SELECT * FROM {table}" + (" WHERE " + where if where else "") + (" ORDER BY " + order if order else "")
        return [dict(r) for r in self.db.execute(sql + " LIMIT ?", (*params, min(max(int(limit), 1), 10000)))]

    def scoped(self, table, **kwargs):
        return self.rows(table, required=("origin", *kwargs.pop("required", ())),
                         where="origin=?", params=(self.origin,), **kwargs) if self.origin else []

    def recommendations(self, code=None):
        where, params = ("(visibility='public' OR origin=?)", [self.origin]) if self.origin else ("visibility='public'", [])
        if code:
            where += " AND code=?"
            params.append(code)
        return self.rows("recommendation_records", required=("visibility", "origin", "recommended_date", "code"),
                         where=where, params=params, order="recommended_date DESC,code", limit=1000)

    def active_raw(self):
        """The published active daily generation as recorded; display-only, not revalidated here."""
        if hasattr(self, "_active_raw"):
            return self._active_raw
        self._active_raw = None
        if not {"batches", "active_generations", "datasets", "partition_bars", "batch_days"}.issubset(self.tables):
            return None
        batch_columns, generation_columns = self.columns("batches"), self.columns("active_generations")
        dataset_columns, bar_columns = self.columns("datasets"), self.columns("partition_bars")
        if not ({"batch_id", "dataset_id", "actual_trade_date", "status", "source", "published_at"}.issubset(batch_columns)
                and {"dataset_id", "active_batch_id", "generation"}.issubset(generation_columns)
                and {"dataset_id", "dataset_key"}.issubset(dataset_columns)
                and {"partition_id", "trade_date", "code", "open", "high", "low", "close"}.issubset(bar_columns)
                and {"batch_id", "trade_date", "partition_id"}.issubset(self.columns("batch_days"))):
            return None
        where = ["b.status='published'", "d.dataset_key='tushare_daily'"]
        if "shadow" in batch_columns:
            where.append("b.shadow=0")
        if "publication_mode" in batch_columns:
            where.append("b.publication_mode='active'")
        quality = ",b.quality" if "quality" in batch_columns else ",NULL AS quality"
        row = self.db.execute(
            "SELECT b.batch_id,b.actual_trade_date,b.source,b.published_at,a.generation" + quality +
            " FROM active_generations a JOIN batches b ON b.batch_id=a.active_batch_id"
            " JOIN datasets d ON d.dataset_id=a.dataset_id WHERE " + " AND ".join(where) + " LIMIT 1"
        ).fetchone()
        if row and row["actual_trade_date"]:
            self._active_raw = {"batch_id": safe_text(row["batch_id"]), "trade_date": str(row["actual_trade_date"]),
                                "source": safe_text(row["source"]) or "unknown", "published_at": row["published_at"],
                                "generation": number(row["generation"]), "quality": safe_text(row["quality"]) or "unknown",
                                "dataset_key": "tushare_daily"}
        return self._active_raw

    def active_raw_rows(self, code, limit=120):
        active = self.active_raw()
        if not active:
            return []
        today = self.now.astimezone(CHINA).date().isoformat()
        return [dict(r) for r in self.db.execute(
            "SELECT pb.* FROM partition_bars pb JOIN batch_days bd"
            " ON bd.batch_id=? AND bd.trade_date=pb.trade_date AND bd.partition_id=pb.partition_id"
            " WHERE pb.code=? AND pb.trade_date<=? ORDER BY pb.trade_date DESC LIMIT ?",
            (active["batch_id"], code, today, min(max(int(limit), 1), 500)))]

    def stock_macd(self, code):
        """Display-only MACD state over the active raw generation's sessions (at most 500)."""
        active = self.active_raw()
        if not active:
            return {"status": "unavailable", "reason": "no_active_raw"}
        today = self.now.astimezone(CHINA).date().isoformat()
        days = [row[0] for row in self.db.execute(
            "SELECT DISTINCT trade_date FROM batch_days WHERE batch_id=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 500",
            (active["batch_id"], today))][::-1]
        return {**macd_state(days, self.active_raw_rows(code, limit=500)), "verified": False, "display_only": True}

    def raw_evidence(self, row, code):
        active = self.active_raw() or {}
        published = instant(active.get("published_at"))
        return {"code": code, "business_date": row.get("trade_date"), "announcement_date": None,
                "collected_at": published.isoformat() if published else None,
                "source": safe_text(row.get("source")) or active.get("source") or "unknown",
                "evidence": f"sqlite:partition_bars:{active.get('batch_id') or 'unknown'}:{code}:{row.get('trade_date') or 'unknown'}",
                "quality": "active_published_raw"}

    def primary_source(self):
        """The dataset that the main pages actually show, so legacy tables cannot set the headline date."""
        active = self.active_raw()
        if active:
            return {"dataset": "raw_active_generation", "date": active["trade_date"], "batch_id": active["batch_id"],
                    "generation": active["generation"], "source": active["source"], "collected_at": active["published_at"]}
        snapshots = self.rows("daily_snapshot_meta", required=("trade_date",), order="trade_date DESC", limit=1)
        if snapshots:
            row = snapshots[0]
            return {"dataset": "daily_snapshot_meta", "date": row.get("trade_date"), "batch_id": None, "generation": None,
                    "source": safe_text(row.get("source")) or "unknown", "collected_at": row.get("fetched_at")}
        return {"dataset": None, "date": None, "batch_id": None, "generation": None, "source": None, "collected_at": None}

    def session(self):
        """Closed only when the calendar says so; a missing intraday artifact is not a holiday."""
        local = self.now.astimezone(CHINA)
        today = local.date().isoformat()
        calendar = "unknown"
        rows = self.rows("trading_calendar", required=("trade_date", "is_open"), where="trade_date=?", params=(today,), limit=1)
        if rows:
            row = rows[0]
            status = str(row.get("status") or ("open" if row.get("is_open") else "closed")).lower()
            if status in ("open", "closed") and bool(row.get("is_open")) == (status == "open"):
                calendar = status
        phase = "closed_day" if calendar == "closed" else trading_phase(local) if calendar == "open" else "unknown"
        return {"date": today, "calendar": calendar, "phase": phase, "local_time": local.strftime("%H:%M")}

    def research_pools(self):
        """Show one immutable research freeze without joining it to formal candidates."""
        empty = {"status": "unavailable", "reason": "research_pool_schema_unavailable", "run_id": None,
                 "trade_date": None, "frozen_at": None, "published_at": None,
                 "primary": [], "radar": [], "paper_history": []}
        required_run = ("run_id", "trade_date", "batch_id", "source", "basis", "published_at", "frozen_at", "status")
        required_pick = ("run_id", "pool", "code", "name", "rank", "score", "close", "risk_level", "risk_flags", "reasons")
        if not set(required_run).issubset(self.columns("research_pool_runs")) or not set(required_pick).issubset(self.columns("research_pool_picks")):
            return empty
        runs = self.rows("research_pool_runs", required=required_run,
                         order="trade_date DESC,frozen_at DESC", limit=1)
        if not runs:
            return {**empty, "status": "empty", "reason": "no_research_freeze"}
        run = runs[0]
        frozen = instant(run.get("frozen_at"))
        published = instant(run.get("published_at"))
        trade_date = str(run.get("trade_date") or "")
        try:
            business_day = date.fromisoformat(trade_date)
        except ValueError:
            business_day = None
        current_day = self.now.astimezone(CHINA).date()
        latest_dates = self.rows("daily_snapshot_meta", required=("trade_date",),
                                 order="trade_date DESC", limit=1)
        latest_day = str(latest_dates[0].get("trade_date") or "") if latest_dates else ""
        reason = None
        if (run.get("status") != "research_only" or run.get("basis") != "unadjusted"
                or not business_day or business_day > current_day or not frozen or not published
                or frozen > self.now or published > frozen):
            reason = "freeze_identity_or_time_unverified"
        elif latest_day and trade_date < latest_day:
            reason = "superseded_by_newer_market_snapshot"
        elif (current_day - business_day).days > 7:
            reason = "old_business_date"
        status = "unknown" if reason == "freeze_identity_or_time_unverified" else "stale" if reason else "research_only"
        picks = self.rows("research_pool_picks", required=required_pick, where="run_id=?",
                          params=(run["run_id"],), order="pool,rank", limit=500)
        freeze_rows = self.rows("paper_freeze_contracts", required=("run_id", "protocol_version", "rules_sha256"),
                                where="run_id=?", params=(run["run_id"],), limit=1)
        freeze_rule = freeze_rows[0] if freeze_rows else {}
        review_records = {}
        if "paper_entry_terms" in self.tables:
            db_path = self.db.execute("PRAGMA database_list").fetchone()[2]
            if db_path:
                as_of = self.now.astimezone(CHINA).date().isoformat()
                sessions = [row["trade_date"] for row in self.rows(
                    "trading_calendar", required=("trade_date", "status", "is_open"),
                    where="trade_date<=? AND status='open' AND is_open=1",
                    params=(as_of,), order="trade_date", limit=10000)]
                try:
                    reviewed = review_persisted(db_path, sessions, as_of=as_of, cutoff_at=self.now)
                    review_records = {item["record_id"]: item for item in reviewed["records"]}
                except (ValueError, sqlite3.Error):
                    self.notices.add("paper_review:unavailable")
        pools = {"primary": [], "radar": []}
        for row in picks:
            pool = row.get("pool")
            if pool not in pools:
                continue
            close = number(row.get("close"))
            record_id = f"{run['run_id']}:{row['code']}"
            qualification = {}
            entry = {}
            if pool == "primary":
                candidates = self.rows(
                    "paper_qualification_events", required=("record_id", "version", "state", "first_decided_at"),
                    where="record_id=? AND first_decided_at<=?",
                    params=(record_id, self.now.astimezone(timezone.utc).isoformat()),
                    order="first_decided_at DESC,version DESC", limit=1)
                qualification = candidates[0] if candidates and instant(candidates[0]["first_decided_at"]) else {}
                entries = self.rows(
                    "paper_simulated_entries", required=("record_id", "qualification_version", "fill_status", "confirmed_at"),
                    where="record_id=?", params=(record_id,), limit=1)
                if entries and instant(entries[0].get("confirmed_at")) and instant(entries[0]["confirmed_at"]) <= self.now:
                    entry = entries[0]
            item = {"record_id": record_id,
                                "run_id": run["run_id"], "batch_id": safe_text(run["batch_id"]),
                                "pool": pool, "rank": row.get("rank"),
                                "code": safe_text(row["code"]), "name": safe_text(row["name"]),
                                "score": number(row.get("score")), "risk_level": safe_text(row.get("risk_level")) or "unknown",
                                "risk_flags": [safe_text(item) for item in arr(row.get("risk_flags"))],
                                "reasons": [safe_text(item) for item in arr(row.get("reasons"))],
                                "observation_reference_close": close if close and close > 0 else None,
                                "eligibility": "research_only", "confirmation": "not_assessed",
                                "entry_range": None, "exit_range": None,
                                "protocol_version": safe_text(freeze_rule.get("protocol_version")) or "legacy-unversioned",
                                "accounting_version": (forward.ACCOUNTING_VERSION
                                                       if freeze_rule.get("protocol_version") == forward.PROTOCOL_VERSION
                                                       else "unknown-accounting-version" if freeze_rule.get("protocol_version")
                                                       else "legacy-percent-fee-v1"),
                                "freeze_rules_sha256": safe_text(freeze_rule.get("rules_sha256")) or None,
                                "arm_A": {"state": "unknown", "reason": "A_observations_not_collected"},
                                "arm_B": {"state": "unknown", "reason": "risk_or_minute_unverified"},
                                "paper_status": "not_entered", "data_date": trade_date,
                                "missing_reason": "risk_and_intraday_confirmation_not_verified"}
            if pool == "primary" and freeze_rule.get("protocol_version") == forward.PROTOCOL_VERSION and close and close > 0:
                confirmation_price = close * 1.005
                item["entry_range"] = {"A": [round(close * 0.995, 4), round(close * 1.005, 4)],
                                       "B_confirmation": round(confirmation_price, 4),
                                       "B_limit": float(forward.b_limit_price(close)),
                                       "B_chase_ceiling": round(close * 1.03, 4),
                                       "basis": "unadjusted_frozen_reference"}
                item["target_condition"] = "D1/D3/D5收盘估值；没有卖出目标或已平仓证据"
                item["invalidation_condition"] = "风险未核、追价超限、B窗口14:57结束或次交易日计划失效"
            if qualification:
                item.update(qualification_version=safe_text(qualification.get("version")),
                            qualification_at=safe_text(qualification.get("first_decided_at")),
                            eligibility=("trading_flags_clear_only" if qualification["state"] == "eligible"
                                         else safe_text(qualification["state"])),
                            missing_reason=safe_text(qualification.get("reason")))
            bound_rows = self.rows(
                "paper_qualification_events", required=("record_id", "version", "state", "first_decided_at"),
                where="record_id=? AND version=?", params=(record_id, entry["qualification_version"]), limit=1,
            ) if entry else []
            bound = bound_rows[0] if bound_rows else None
            confirmed = instant(entry.get("confirmed_at")) if entry else None
            decided = instant(bound.get("first_decided_at")) if bound else None
            if entry and bound and bound["state"] == "eligible" and decided and confirmed and decided <= confirmed:
                term_rows = self.rows("paper_entry_terms", required=("record_id", "accounting_version", "quantity", "entry_fees_cny", "total_cost_cny"),
                                      where="record_id=?", params=(record_id,), limit=1)
                terms = term_rows[0] if term_rows else None
                item.update(confirmation="two_completed_bars", fill_status=safe_text(entry.get("fill_status")),
                            entry_qualification_version=safe_text(entry.get("qualification_version")),
                            entry_qualification_at=safe_text(bound.get("first_decided_at")),
                            simulated_entry_date=safe_text(entry.get("entry_date")),
                            simulated_entry_price=number(entry.get("entry_price")),
                            round_trip_fee_pct=number(entry.get("round_trip_fee_pct")),
                            entry_slippage_pct=number(entry.get("entry_slippage_pct")),
                            accounting_version=safe_text(entry.get("accounting_version")) or "legacy-percent-fee-v1",
                            simulated_quantity=int(terms["quantity"]) if terms else None,
                            entry_fees_cny=number(terms.get("entry_fees_cny")) if terms else None,
                            total_cost_cny=number(terms.get("total_cost_cny")) if terms else None)
                item["arm_B"] = {"state": "entered" if entry.get("fill_status") == "simulated_fill" else "not_filled",
                                 "reason": safe_text(entry.get("reason")), "confirmed_at": safe_text(entry.get("confirmed_at"))}
                item["paper_status"] = "valuation_in_progress" if entry.get("fill_status") == "simulated_fill" else "not_executable"
                record_review = review_records.get(record_id)
                if (entry.get("fill_status") == "simulated_fill" and
                        entry.get("accounting_version") == forward.ACCOUNTING_VERSION and
                        (not record_review or record_review.get("entry_price") is None)):
                    item.update(simulated_quantity=None, entry_fees_cny=None, total_cost_cny=None,
                                paper_status="unknown", missing_reason="entry_evidence_unverified")
                if record_review and record_review.get("accounting_version") == forward.ACCOUNTING_VERSION:
                    item["valuation"] = {str(h): record_review["marks"][h] for h in (1, 3, 5)}
                    if all(record_review["marks"][h]["status"] == "complete" for h in (1, 3, 5)):
                        item["paper_status"] = "valuation_complete"
                        item["missing_reason"] = "close_marks_complete_no_sale"
                    elif any(record_review["marks"][h]["status"] == "unknown" for h in (1, 3, 5)):
                        item["missing_reason"] = "holding_period_evidence_unverified"
                if (not qualification or qualification.get("state") == "eligible") and item["missing_reason"] not in {
                        "close_marks_complete_no_sale", "holding_period_evidence_unverified", "entry_evidence_unverified"}:
                    item["missing_reason"] = "paper_review_pending"
            pools[pool].append(item)
        paper_history = sorted(
            (item for item in review_records.values()
             if item.get("state") == "confirmed" and
                item.get("fill_status") in {"simulated_fill", "unfilled"}),
            key=lambda item: (item.get("confirmed_at") or "", item["record_id"]), reverse=True,
        )[:50]
        diagnostics = obj(run.get("diagnostics"))
        recorded = obj(diagnostics.get("parameters"))
        parameters = {key: number(recorded.get(key)) for key in ("deep_limit", "primary_limit", "radar_limit", "price_min", "price_max")
                      if number(recorded.get(key)) is not None} or None
        return {"status": status, "reason": reason, "run_id": safe_text(run["run_id"]),
                "batch_id": safe_text(run["batch_id"]), "trade_date": trade_date,
                "source": safe_text(run["source"]), "basis": safe_text(run["basis"]),
                "frozen_at": frozen.isoformat() if frozen else None,
                "published_at": published.isoformat() if published else None,
                "selection_policy": safe_text(diagnostics.get("selection_policy")) or None,
                "parameters": parameters,
                "independent_of": [safe_text(v, 60) for v in arr(recorded.get("independent_of"))[:8]],
                "paper_history": paper_history, **pools}

    def ai_review_maps(self, run_ids):
        run_ids = [str(value) for value in dict.fromkeys(run_ids or []) if str(value)]
        if not run_ids:
            return {}, {}
        batches = self.rows(
            "recommendation_ai_review_batches",
            required=("review_batch_id", "run_id", "status", "model_requested", "prompt_version", "requested_at"),
            where="run_id IN (" + ",".join("?" for _ in run_ids) + ")",
            params=run_ids,
            order="requested_at DESC",
            limit=2000,
        )
        latest = {}
        for batch in batches:
            latest.setdefault(str(batch["run_id"]), batch)
        batch_ids = [str(batch["review_batch_id"]) for batch in latest.values()]
        if not batch_ids:
            return latest, {}
        reviews = self.rows(
            "recommendation_ai_reviews",
            required=("review_batch_id", "recommendation_id", "code", "decision"),
            where="review_batch_id IN (" + ",".join("?" for _ in batch_ids) + ")",
            params=batch_ids,
            limit=10000,
        )
        return latest, {(str(row["review_batch_id"]), str(row["recommendation_id"])): row for row in reviews}

    def candidates(self):
        refs = self.rows("active_candidate_runs", required=("scope", "run_id"),
                         where="scope IN (?,?)", params=("global", self.origin or "global"), limit=10)
        runs = self.rows("screen_runs", required=("run_id", "actual_trade_date"),
                         order="actual_trade_date DESC,finished_at DESC", limit=100)
        run_map = {r["run_id"]: r for r in runs}
        allowed = [r["run_id"] for r in refs]
        if not allowed:
            self.notices.add("candidate_visibility_or_active_run:unavailable")
            return []
        raw = self.rows("screen_candidates", required=("run_id", "code", "price_plan"),
                        where="run_id IN (" + ",".join("?" for _ in allowed) + ")",
                        params=allowed, order="score DESC,code", limit=500)
        recommendations = self.rows(
            "recommendation_records",
            required=("recommendation_id", "run_id", "code"),
            where="run_id IN (" + ",".join("?" for _ in allowed) + ")",
            params=allowed,
            limit=1000,
        )
        recommendation_map = {(str(row["run_id"]), str(row["code"])): row for row in recommendations}
        ai_batches, ai_reviews = self.ai_review_maps(allowed)
        result, seen = [], set()
        for row in raw:
            if row["code"] in seen:
                continue
            seen.add(row["code"])
            plan, factors = obj(row.get("price_plan")), obj(row.get("factor_payload"))
            run = run_map.get(row["run_id"], {})
            diagnostics = obj(run.get("diagnostics"))
            provenance = obj(plan.get("provenance"))
            total, adjustment = number(row.get("score")), number(factors.get("market_adjustment"))
            components = [number(factors.get(k)) for k in ("industry_score", "fundamental_score", "market_adjustment")]
            base = total if row.get("score_max") == 50 else (
                total - max(-20, min(20, int(round(sum(components[:2]))) + components[2]))
                if total is not None and all(v is not None for v in components) else None)
            coverage_value = number(run.get("coverage"))
            recommendation = recommendation_map.get((str(row["run_id"]), str(row["code"])), {})
            ai_batch = ai_batches.get(str(row["run_id"]), {})
            ai_review = ai_reviews.get((str(ai_batch.get("review_batch_id") or ""), str(recommendation.get("recommendation_id") or "")), {})
            result.append({
                "rank": len(result) + 1, "code": safe_text(row["code"]), "name": safe_text(row.get("name")),
                "score": total, "score_max": number(row.get("score_max")), "technical_score": base,
                "industry_score": components[0], "fundamental_score": components[1], "market_adjustment": adjustment,
                "industry": safe_text(factors.get("industry_name") or factors.get("current_industry_name")) or "unavailable",
                "risk_level": safe_text(row.get("risk_level")) or "unknown",
                "date": run.get("actual_trade_date"), "run_id": row["run_id"],
                "plan_version": row["run_id"] + ":" + hashlib.sha256(str(row.get("price_plan") or "").encode()).hexdigest()[:16],
                "plan_validated": plan.get("validated") is True and provenance.get("basis") == "unadjusted",
                "attention_low": number(plan.get("attention_low")), "attention_high": number(plan.get("attention_high")),
                "target_low": number(plan.get("sell_low")), "target_high": number(plan.get("sell_high")),
                "invalidation": number(plan.get("invalidation")), "confirmation": number(plan.get("confirmation")),
                "reference": number(plan.get("reference_price")), "atr": number(plan.get("atr")),
                "confidence": "plan_only / low" if plan.get("validated") is True else "unknown",
                "coverage": coverage_value, "fundamental_coverage": number(factors.get("fundamental_coverage")),
                "coverage_status": "unknown" if coverage_value is None else "available" if coverage_value >= 1 else "partial",
                "missing_reason": safe_text(diagnostics.get("degraded_reason") or diagnostics.get("reason")) or None,
                "source_timestamp": safe_text(diagnostics.get("source_timestamp") or diagnostics.get("quote_timestamp_max")) or None,
                "ai_review": {
                    "status": safe_text(ai_batch.get("status")) or "unavailable",
                    "decision": safe_text(ai_review.get("decision")) or "unknown",
                    "score_adjustment": number(ai_review.get("score_adjustment")),
                    "reason": safe_text(ai_review.get("reason"), 400) or None,
                    "risk_tags": [safe_text(value, 80) for value in arr(ai_review.get("risk_tags_json"))[:8]],
                    "model": safe_text(ai_batch.get("model_returned") or ai_batch.get("model_requested")) or "unknown",
                    "prompt_version": safe_text(ai_batch.get("prompt_version")) or "unknown",
                },
            })
        return result

    def signals(self, code=None):
        events = self.scoped("intraday_event_outbox", required=("code", "created_at"), order="created_at DESC", limit=200)
        result = []
        for event in events:
            if code and event["code"] != code:
                continue
            original = self.rows("screen_candidates", required=("run_id", "code", "price_plan"),
                                 where="run_id=? AND code=?", params=(event.get("run_id"), event["code"]), limit=1)
            plan = {}
            if original:
                encoded = str(original[0].get("price_plan") or "")
                version = str(event.get("run_id")) + ":" + hashlib.sha256(encoded.encode()).hexdigest()[:16]
                candidate_plan = obj(encoded)
                if (version == event.get("plan_version") and candidate_plan.get("validated") is True
                        and obj(candidate_plan.get("provenance")).get("basis") == "unadjusted"):
                    plan = candidate_plan
            quote = self.rows("daily_quotes", required=("code", "trade_date"),
                              where="code=? AND trade_date<=?",
                              params=(event["code"], self.now.astimezone(CHINA).date().isoformat()),
                              order="trade_date DESC", limit=1)
            quote = quote[0] if quote else {}
            fetched = quote.get("provider_ts") or quote.get("fetched_at")
            stamp, received = instant(fetched), instant(quote.get("fetched_at"))
            future = any(s is not None and s > self.now for s in (stamp, received))
            price = None if future else number(quote.get("price"))
            if price is not None and price <= 0:
                price = None
            low, high = number(plan.get("attention_low")), number(plan.get("attention_high"))
            distance = None
            if price is not None and price > 0 and low and high and low <= high:
                distance = ((price / low - 1) if price < low else (price / high - 1) if price > high else 0) * 100
            freshness = "unknown" if stamp is None or future else "fresh" if 0 <= (self.now - stamp).total_seconds() <= 180 else "stale"
            result.append({
                "code": safe_text(event["code"]), "name": safe_text(event.get("name")), "signal": safe_text(event.get("signal")),
                "state": safe_text(event.get("state")) or "unknown_delivery", "created_at": event.get("created_at"),
                "sent_at": event.get("sent_at"), "plan_version": safe_text(event.get("plan_version"), 180),
                "quote_at": event.get("quote_fetched_at") or None, "market_at": event.get("market_snapshot_at") or None,
                "last_stored_price": price, "last_stored_quote_at": fetched, "freshness": freshness,
                "price_evidence": self.price_evidence(quote, event["code"], "daily_quotes"),
                "distance_pct": distance, "attention_low": low, "attention_high": high,
                "invalidation": number(plan.get("invalidation")), "risk_event": bool(event.get("risk_event")),
                "error": error_category(event.get("last_error")),
            })
        return result

    @staticmethod
    def price_evidence(row, code, table):
        collected = instant(row.get("fetched_at"))
        return {"code": code, "business_date": row.get("trade_date"), "announcement_date": None,
                "collected_at": collected.isoformat() if collected else None,
                "source": safe_text(row.get("source")) or "unknown",
                "evidence": f"sqlite:{table}:{code}:{row.get('trade_date') or 'unknown'}",
                "quality": "stored_observation" if collected and row.get("source") else "unknown"}

    def overview(self):
        snapshots = self.rows("daily_snapshot_meta", required=("trade_date",), order="trade_date DESC", limit=1)
        runs = self.rows("screen_runs", required=("actual_trade_date",), order="actual_trade_date DESC,finished_at DESC", limit=8)
        contexts = self.rows("market_contexts", required=("as_of", "payload"), order="as_of DESC", limit=1)
        market = obj(contexts[0]["payload"]) if contexts else {}
        latest = runs[0] if runs else {}
        snapshot = snapshots[0] if snapshots else {}
        candidates = self.candidates()
        acceptance_rows = self.rows("daily_acceptance_runs", required=("trade_date", "status", "checked_at"), order="trade_date DESC,checked_at DESC", limit=1)
        acceptance = acceptance_rows[0] if acceptance_rows else {}
        acceptance_findings = arr(acceptance.get("findings_json")) if acceptance else []
        return {
            "data_date": snapshot.get("trade_date") or latest.get("actual_trade_date"),
            "requested_date": snapshot.get("requested_date"), "quality": snapshot.get("quality", "unknown"),
            "complete": snapshot.get("complete") == 1, "coverage": number(latest.get("coverage")),
            "coverage_breakdown": self.coverage_breakdown(snapshot, latest, market, contexts[0]["as_of"] if contexts else None, len(candidates)),
            "session": self.session(),
            "market": {key: market.get(key) for key in ("regime", "breadth", "advancing", "declining", "flat", "median_return", "total_amount")},
            "market_date": contexts[0]["as_of"] if contexts else None,
            "candidate_count": len(candidates), "candidates": candidates[:5],
            "acceptance": {"trade_date": acceptance.get("trade_date"), "checked_at": acceptance.get("checked_at"),
                           "status": acceptance.get("status", "unknown"), "summary": acceptance.get("summary") or "尚无每日验收记录",
                           "findings": acceptance_findings},
            "signal_counts": dict(Counter(e["state"] for e in self.signals())),
            "batches": [{"date": r.get("actual_trade_date"), "generation": number(r.get("generation")),
                         "state": r.get("status", "unknown"), "rows": number(r.get("row_count"))}
                        for r in self.rows("batches", required=("actual_trade_date", "created_at"),
                                           order="actual_trade_date DESC,created_at DESC", limit=5)],
            "runs": [{"run_id": r.get("run_id"), "job": r.get("job_name"), "date": r.get("actual_trade_date"),
                      "status": r.get("status", "unknown"), "quality": r.get("quality", "unknown"),
                      "count": r.get("quote_count"), "coverage": number(r.get("coverage")),
                      "error": error_category(r.get("error"))} for r in runs],
        }

    def coverage_breakdown(self, snapshot, run, market, market_date, visible_candidates):
        """Each measure keeps its own denominator; complete prices and unknown risk can both be true."""
        diagnostics = obj(run.get("diagnostics"))
        count = lambda key: int(number(diagnostics.get(key))) if number(diagnostics.get(key)) is not None else None
        targets, enriched = count("indicator_targets"), count("enriched")
        stored = number(run.get("coverage"))
        if targets is None:
            indicator = {"status": "denominator_unrecorded" if stored is not None else "unknown", "coverage": stored}
        elif targets == 0:
            indicator = {"status": "not_applicable", "coverage": None}
        else:
            indicator = {"status": "measured", "coverage": enriched / targets if enriched is not None else stored}
        sides = [number(market.get(key)) for key in ("advancing", "declining", "flat")]
        active = self.active_raw()
        confirmed = count("risk_confirmed")
        return {
            "market": {"date": snapshot.get("trade_date"), "complete": snapshot.get("complete") == 1 if snapshot else None,
                       "quality": snapshot.get("quality") or "unknown",
                       "count": int(sum(sides)) if all(v is not None for v in sides) and sum(sides) > 0 else None,
                       "count_date": market_date, "raw_batch_id": active["batch_id"] if active else None,
                       "raw_date": active["trade_date"] if active else None},
            "risk": {"known": count("risk_tuple_complete"), "total": count("input")},
            "indicator": {**indicator, "enriched": enriched, "targets": targets},
            "formal": {"risk_confirmed": confirmed if confirmed is not None else count("tradable"),
                       "candidates": int(number(run.get("candidate_count"))) if number(run.get("candidate_count")) is not None else None,
                       "visible": visible_candidates},
            "run_id": safe_text(run.get("run_id")) or None, "run_date": run.get("actual_trade_date"),
            "run_status": safe_text(run.get("status")) or None,
        }

    def active_raw_bars(self, code):
        active, result = self.active_raw(), []
        for row in reversed(self.active_raw_rows(code)):
            values = ohlc(row, "basis")
            if values is None:
                self.notices.add("partition_bars:unusable_rows_excluded")
                continue
            result.append({"date": row["trade_date"], "open": values[0], "high": values[1], "low": values[2], "close": values[3],
                           "volume": number(row.get("volume")), "source": safe_text(row.get("source")) or active["source"],
                           "data_evidence": self.raw_evidence(row, code)})
        return result

    def bars(self, code):
        """Prefer the published active raw generation; the old daily_bars table is a labelled fallback."""
        raw = self.active_raw_bars(code)
        if raw:
            active = self.active_raw()
            return raw, {"kind": "active_raw", "batch_id": active["batch_id"], "generation": active["generation"],
                         "trade_date": active["trade_date"], "source": active["source"]}
        rows = self.rows("daily_bars", required=("code", "trade_date", "open", "high", "low", "close"),
                         where="code=? AND trade_date<=?", params=(code, self.now.astimezone(CHINA).date().isoformat()),
                         order="trade_date DESC", limit=120)
        result = []
        for row in reversed(rows):
            values = ohlc(row)
            if values is None:
                self.notices.add("daily_bars:unusable_rows_excluded")
                continue
            result.append({"date": row["trade_date"], "open": values[0], "high": values[1], "low": values[2], "close": values[3],
                           "volume": number(row.get("volume")), "source": row.get("source") or "unknown",
                           "data_evidence": self.price_evidence(row, code, "daily_bars")})
        kind = "legacy_daily_bars" if result else "none"
        return result, {"kind": kind, "batch_id": None, "generation": None,
                        "trade_date": result[-1]["date"] if result else None, "source": result[-1]["source"] if result else None}

    def attach_latest_closes(self, items):
        """Read-only display fields: latest valid unadjusted close per candidate, active raw first."""
        today = self.now.astimezone(CHINA).date().isoformat()
        for item in items:
            item.update({"close": None, "close_date": None, "prev_close": None, "pct_change": None, "close_source": None})
            raw = next((row for row in self.active_raw_rows(item["code"], limit=3) if ohlc(row, "basis")), None)
            if raw:
                previous = number(raw.get("pre_close"))
                item.update(close=number(raw["close"]), close_date=raw["trade_date"], close_source="active_raw",
                            prev_close=previous if previous and previous > 0 else None,
                            pct_change=number(raw.get("pct_change")))
                continue
            rows = self.rows("daily_bars", required=("code", "trade_date", "open", "high", "low", "close"),
                             where="code=? AND trade_date<=?", params=(item["code"], today),
                             order="trade_date DESC", limit=6)
            valid = []
            for row in rows:
                values = ohlc(row)
                if values is None:
                    continue
                valid.append((row["trade_date"], values[3]))
                if len(valid) == 2:
                    break
            if valid:
                item["close_date"], item["close"] = valid[0]
                item["close_source"] = "legacy_daily_bars"
            if len(valid) == 2 and valid[1][1]:
                item["prev_close"] = valid[1][1]
                item["pct_change"] = round((valid[0][1] / valid[1][1] - 1) * 100, 4)
        return items

    def symbol(self, code):
        if "stock_symbols" not in self.tables:
            return None
        rows = self.rows("stock_symbols", required=("code", "name"), where="code=?", params=(code,), limit=1)
        return {"name": safe_text(rows[0]["name"], 40), "source": safe_text(rows[0].get("source"))} if rows and rows[0].get("name") else None

    def latest_research_picks(self):
        if not {"research_pool_runs", "research_pool_picks"}.issubset(self.tables):
            return None, []
        runs = self.rows("research_pool_runs", required=("run_id", "trade_date", "frozen_at"),
                         order="trade_date DESC,frozen_at DESC", limit=1)
        if not runs:
            return None, []
        return runs[0], self.rows("research_pool_picks", required=("run_id", "pool", "code", "rank"), where="run_id=?",
                                  params=(runs[0]["run_id"],), order="pool,rank", limit=500)

    def research_membership(self, code):
        """Latest research freeze membership only; never a formal candidate or recommendation."""
        run, picks = self.latest_research_picks()
        pick = next((row for row in picks if row["code"] == code), None)
        if not pick:
            return None
        return {"run_id": safe_text(run["run_id"]), "trade_date": run["trade_date"], "pool": safe_text(pick["pool"]),
                "rank": pick.get("rank"), "name": safe_text(pick.get("name"), 40), "score": number(pick.get("score")),
                "risk_level": safe_text(pick.get("risk_level")) or "unknown", "eligibility": "research_only"}

    def search(self, query):
        text = re.sub(r"[\x00-\x1f\x7f%_\\]", "", str(query or "")).strip()[:16]
        if not text:
            raise Unavailable("search_query_invalid")
        found = {}
        def add(code, name, source):
            code = safe_text(code, 12)
            if re.fullmatch(r"(?:\d{6}|DEMO\d{2})", code) and code not in found and len(found) < 20:
                found[code] = {"code": code, "name": safe_text(name, 40) or None, "source": source}
        if "stock_symbols" in self.tables and {"code", "name"}.issubset(self.columns("stock_symbols")):
            for row in self.db.execute("SELECT code,name FROM stock_symbols WHERE code LIKE ? OR name LIKE ?"
                                       " ORDER BY (code=?) DESC,(name=?) DESC,code LIMIT 20",
                                       (text + "%", "%" + text + "%", text, text)):
                add(row["code"], row["name"], "stock_symbols")
        lowered = text.lower()
        for row in self.candidates():
            if row["code"].lower().startswith(lowered) or lowered in str(row.get("name") or "").lower():
                add(row["code"], row.get("name"), "formal_candidate")
        for row in self.latest_research_picks()[1]:
            if str(row["code"]).startswith(text) or text in str(row.get("name") or ""):
                add(row["code"], row.get("name"), "research_pool")
        active = self.active_raw()
        if active and re.fullmatch(r"\d{1,6}", text):
            # A code range (not LIKE) keeps the lookup on the (batch_id, trade_date) and (partition_id, code) keys.
            for row in self.db.execute(
                    "SELECT DISTINCT pb.code FROM batch_days bd JOIN partition_bars pb ON pb.partition_id=bd.partition_id"
                    " WHERE bd.batch_id=? AND bd.trade_date=? AND pb.code>=? AND pb.code<? ORDER BY pb.code LIMIT 20",
                    (active["batch_id"], active["trade_date"], text, text[:-1] + chr(ord(text[-1]) + 1))):
                add(row["code"], (self.symbol(row["code"]) or {}).get("name"), "active_raw")
        return {"query": text, "items": list(found.values()), "limit": 20}

    def stock(self, code):
        if not re.fullmatch(r"(?:\d{6}|DEMO\d{2})", code):
            raise Unavailable("invalid_stock_code")
        candidate = next((r for r in self.candidates() if r["code"] == code), None)
        bars, bar_source = self.bars(code)
        recommendations = self.recommendations(code)
        research, symbol = self.research_membership(code), self.symbol(code)
        if not candidate and not bars and not recommendations and not research and not symbol:
            raise Unavailable("stock_unavailable")
        closes = [r["close"] for r in bars]
        indicators = {"MA" + str(n): statistics.fmean(closes[-n:]) if len(closes) >= n else None for n in (5, 10, 20)}
        cutoff = (candidate or {}).get("date") or (bars[-1]["date"] if bars else self.now.astimezone(CHINA).date().isoformat())
        evidence = self.stock_evidence(code, cutoff)
        bar_collected = [instant(row.get("data_evidence", {}).get("collected_at")) for row in bars]
        source_timestamp = max((value for value in bar_collected if value), default=None)
        if bars:
            data_status, missing_reason = ("available", None) if len(bars) >= 20 else ("pending", "insufficient_history")
        else:
            data_status, missing_reason = "unknown", "daily_bars_not_collected"
        name = ((candidate or {}).get("name") or (research or {}).get("name") or (symbol or {}).get("name")
                or (recommendations[0].get("name") if recommendations else None) or code)
        return {
            "code": code, "name": name, "candidate": candidate, "research": research,
            "formal_status": "formal_candidate" if candidate else "no_formal_candidate",
            "recommendation_status": "recorded" if recommendations else "no_recommendation",
            "bars": bars, "bar_source": bar_source, "indicators": indicators,
            "last_close": closes[-1] if closes else None, "bar_date": bars[-1]["date"] if bars else None,
            "signals": self.signals(code),
            "recommendations": [{"date": r.get("recommended_date"), "plan_version": r.get("plan_version"),
                                 "plan_status": r.get("plan_status", "unknown"),
                                 "comparability": r.get("comparability_status", "unknown")} for r in recommendations[:30]],
            "announcements": evidence["announcements"], "data_evidence": evidence,
            "technical_history": {"bars": len(bars), "status": "available" if len(bars) >= 20 else "insufficient"},
            "macd": (self.stock_macd(code) if bar_source["kind"] == "active_raw"
                     else {"status": "unavailable", "reason": "no_active_raw", "verified": False, "display_only": True}),
            "data_quality": {"status": data_status, "missing_reason": missing_reason,
                             "source_timestamp": source_timestamp.isoformat() if source_timestamp else None},
        }

    def stock_evidence(self, code, cutoff):
        records, documents = [], []
        for row in self.rows("data_evidence_records",
                             required=("code", "business_date", "collected_at", "kind", "payload"),
                             where="code=? AND business_date<=?", params=(code, cutoff),
                             order="collected_at DESC", limit=200):
            payload = obj(row["payload"])
            collected = evidence_stamp(payload.get("collected_at"))
            announced = payload.get("announcement_date")
            if not collected or collected > self.now or not announced or announced > cutoff:
                continue
            if row["kind"] == "assertion":
                if payload.get("business_date") == cutoff:
                    records.append(payload)
            elif row["kind"] == "announcement" and payload.get("code") == code and payload.get("quality") == "readable":
                documents.append({key: safe_text(payload.get(key), 500 if key == "quote" else 160)
                                  for key in ("title", "source", "business_date", "announcement_date", "collected_at", "quote")})
        factors = self.rows("factor_snapshots", required=("code", "as_of", "payload", "source"),
                            where="code=? AND as_of=?", params=(code, cutoff), limit=20)
        for factor in factors:
            nested = obj(factor["payload"]).get("evidence_records", [])
            if isinstance(nested, list):
                records.extend(nested)
        # Deduplicate identical persisted observations, not contradictory ones.
        records = list({json.dumps(r, sort_keys=True): r for r in records if isinstance(r, dict)}.values())
        financial = {kind: resolve_evidence(records, code, cutoff, self.now.isoformat(), kind,
                                            exact_date=kind in ("pe", "pb"))
                     for kind in ("roe", "profit_growth", "cash_quality", "pe", "pb")}
        risks = {kind: resolve_evidence(records, code, cutoff, self.now.isoformat(), kind)
                 for kind in ("st_flag", "audit_flag", "suspended", "delisting_risk")}
        return {
            "as_of": cutoff,
            "financial": financial, "risk": risks,
            "financial_known": sum(v["quality"] == "verified" for v in financial.values()),
            "financial_total": len(financial),
            "risk_known": sum(v["quality"] == "verified" for v in risks.values()),
            "risk_total": len(risks),
            "sources": [{"source": safe_text(r.get("source")), "business_date": r.get("business_date"),
                         "announcement_date": r.get("announcement_date"), "collected_at": r.get("collected_at"),
                         "quality": safe_text(r.get("quality"))} for r in records[:30]],
            "announcements": {"status": "available" if documents else "unavailable", "items": documents},
        }

    def performance(self, horizon):
        if horizon not in (1, 3, 5, 10):
            raise Unavailable("unsupported_horizon")
        records = self.recommendations()
        ai_batches, ai_reviews = self.ai_review_maps([record.get("run_id") for record in records])
        outcomes = self.rows("recommendation_outcomes", required=("recommendation_id", "horizon"),
                             where="horizon=?", params=(horizon,), limit=2000)
        saved = {r["recommendation_id"]: r for r in outcomes}
        calendar = {r["trade_date"]: r for r in self.rows("trading_calendar", required=("trade_date", "status", "is_open"), limit=10000)}
        evaluations = []
        for record in records:
            old = saved.get(record["recommendation_id"], {})
            ai_batch = ai_batches.get(str(record.get("run_id") or ""), {})
            ai_review = ai_reviews.get((str(ai_batch.get("review_batch_id") or ""), str(record["recommendation_id"])), {})
            row = {"code": record["code"], "name": safe_text(record.get("name")), "date": record["recommended_date"],
                   "strategy": safe_text(record.get("strategy_version")) or "unknown",
                   "stored_status": old.get("status") or "pending", "status": "pending",
                   "maturity": "pending", "return_pct": None, "max_gain_pct": None, "drawdown_pct": None,
                   "target_hit": None, "invalidation_hit": None, "reason": "window_or_calendar_incomplete",
                   "ai_status": safe_text(ai_batch.get("status")) or "unavailable",
                   "ai_decision": safe_text(ai_review.get("decision")) or "unknown",
                   "ai_adjustment": number(ai_review.get("score_adjustment")),
                   "ai_reason": safe_text(ai_review.get("reason"), 400) or None,
                   "ai_model": safe_text(ai_batch.get("model_returned") or ai_batch.get("model_requested")) or "unknown"}
            try:
                cursor = date.fromisoformat(record["recommended_date"]) + timedelta(days=1)
            except (TypeError, ValueError):
                row.update(status="unknown", reason="recommendation_date_invalid")
                evaluations.append(row)
                continue
            days, calendar_unknown = [], False
            while cursor <= self.now.astimezone(CHINA).date() and len(days) < horizon:
                day = calendar.get(cursor.isoformat(), {})
                if not day.get("source") or day.get("status") not in ("open", "closed") or bool(day.get("is_open")) != (day["status"] == "open"):
                    calendar_unknown = True
                    break
                if day["is_open"]:
                    if cursor == self.now.astimezone(CHINA).date() and self.now.astimezone(CHINA).hour < 15:
                        break
                    days.append(cursor.isoformat())
                cursor += timedelta(days=1)
            if len(days) < horizon:
                if calendar_unknown:
                    row.update(status="unknown", maturity="unknown", reason="calendar_window_unverified")
                evaluations.append(row)
                continue
            row["maturity"] = "mature"
            base = number(record.get("confirmation_price") if record.get("confirmation_price") is not None else record.get("candidate_price"))
            factor = number(record.get("corporate_action_factor"))
            raw = self.rows("daily_bars", required=("code", "trade_date", "corporate_action_factor", "corporate_action_evidence"),
                            where="code=? AND trade_date IN (" + ",".join("?" for _ in days) + ")",
                            params=(record["code"], *days), order="trade_date", limit=10)
            snapshots = self.rows("daily_snapshot_meta", required=("trade_date", "complete"),
                                  where="trade_date IN (" + ",".join("?" for _ in days) + ")", params=days, limit=10)
            usable = (record.get("plan_status") == "validated" and record.get("comparability_status") == "comparable"
                      and record.get("price_basis") == "unadjusted" and base is not None and base > 0
                      and factor is not None and factor > 0 and len(raw) == horizon
                      and len(snapshots) == horizon and all(r["complete"] == 1 for r in snapshots))
            if not self.demo:
                factor_days = [record["recommended_date"], *days]
                factors = self.rows("corporate_action_factors",
                                    required=("code", "trade_date", "adj_factor", "source", "evidence"),
                                    where="code=? AND trade_date IN (" + ",".join("?" for _ in factor_days) + ")",
                                    params=(record["code"], *factor_days), limit=11)
                code = str(record["code"])
                exchange = "SH" if code.startswith("6") else "SZ" if code.startswith(("0", "3")) else "BJ" if code.startswith(("4", "8", "92")) else None
                expected = lambda day: f"tushare:adj_factor:{day}:{code}.{exchange}"
                usable = bool(usable and exchange and len(factors) == len(factor_days)
                              and all(f.get("source") == "tushare_adj_factor"
                                      and f.get("evidence") == expected(f["trade_date"])
                                      and number(f.get("adj_factor")) is not None
                                      and math.isclose(float(f["adj_factor"]), factor, rel_tol=1e-10)
                                      for f in factors)
                              and all(b.get("corporate_action_evidence") == expected(b["trade_date"]) for b in raw))
            for bar in raw:
                nums = [number(bar.get(k)) for k in ("open", "high", "low", "close", "volume", "amount")]
                f = number(bar.get("corporate_action_factor"))
                usable = bool(usable and all(v is not None and v > 0 for v in nums)
                              and nums[1] >= max(nums[0], nums[3]) and nums[2] <= min(nums[0], nums[3])
                              and f is not None and math.isclose(f, factor, rel_tol=1e-10)
                              and bar.get("price_basis") == "unadjusted" and bar.get("corporate_action_evidence"))
            if not usable:
                row.update(status="unknown", reason="price_calendar_or_comparability_missing")
                evaluations.append(row)
                continue
            closes = [float(r["close"]) for r in raw]
            peak, drawdown = base, 0.0
            for close in closes:
                peak = max(peak, close)
                drawdown = min(drawdown, (close / peak - 1) * 100)
            target, invalidation = number(record.get("target_low")), number(record.get("invalidation_price"))
            t = next((i for i, r in enumerate(raw) if target and r["high"] >= target), None)
            inv = next((i for i, r in enumerate(raw) if invalidation and r["low"] <= invalidation), None)
            unknown_order = old.get("status") == "unknown_order" or (t is not None and t == inv)
            row.update(status="unknown_order" if unknown_order else "complete", reason="daily_order_unprovable" if unknown_order else "",
                       return_pct=(closes[-1] / base - 1) * 100,
                       max_gain_pct=max((r["high"] / base - 1) * 100 for r in raw), drawdown_pct=drawdown)
            if not unknown_order:
                row["target_hit"] = t is not None if target else None
                row["invalidation_hit"] = inv is not None if invalidation else None
            evaluations.append(row)
        complete = [r for r in evaluations if r["return_pct"] is not None]
        paths = [r for r in evaluations if r["status"] == "complete"]
        targets = [r["target_hit"] for r in paths if r["target_hit"] is not None]
        invalidations = [r["invalidation_hit"] for r in paths if r["invalidation_hit"] is not None]
        returns = [r["return_pct"] for r in complete]
        def ai_group(decision):
            rows = evaluations if decision == "all" else [row for row in evaluations if row["ai_decision"] == decision]
            values = [row["return_pct"] for row in rows if row["return_pct"] is not None]
            return {
                "sample_count": len(rows),
                "price_evaluable_count": len(values),
                "positive_return_rate": sum(value > 0 for value in values) / len(values) if values else None,
                "median_return_pct": statistics.median(values) if values else None,
                "mean_return_pct": statistics.fmean(values) if values else None,
            }
        return {
            "horizon": horizon, "sample_count": len(evaluations), "mature_count": sum(r["maturity"] == "mature" for r in evaluations),
            "price_evaluable_count": len(complete), "order_evaluable_count": len(paths),
            "status_counts": dict(Counter(r["status"] for r in evaluations)),
            "median_return_pct": statistics.median(returns) if returns else None,
            "positive_return_rate": sum(v > 0 for v in returns) / len(returns) if returns else None,
            "target_hit_rate": sum(targets) / len(targets) if targets else None,
            "target_denominator": len(targets), "invalidation_rate": sum(invalidations) / len(invalidations) if invalidations else None,
            "invalidation_denominator": len(invalidations),
            "max_drawdown_pct": min((r["drawdown_pct"] for r in complete), default=None),
            "benchmark": {"status": "unavailable", "reason": "no_verified_same_window_benchmark", "return_pct": None},
            "ai_groups": {key: ai_group(key) for key in ("all", "keep", "watch", "veto")},
            "ai_batch_status_counts": dict(Counter(str(batch.get("status") or "unknown") for batch in ai_batches.values())),
            "records": evaluations, "basis": "gross_unadjusted_close_to_close",
        }

    def providers(self):
        """Provider success telemetry and per-API rate-limit state are different tables; never mix their meanings."""
        result = []
        for row in self.rows("provider_health", required=("provider",), order="provider", limit=30):
            success, failure = instant(row.get("last_success_at")), instant(row.get("last_error_at"))
            # The current plugin no longer writes provider_health; week-old rows are history, not today's state.
            activity = max((value for value in (success, failure) if value), default=None)
            result.append({"name": safe_text(row.get("provider")), "telemetry": "provider_health",
                           "last_activity_at": activity.isoformat() if activity else None,
                           "stale": activity is None or (self.now - activity) > timedelta(days=7),
                           "success_at": success.isoformat() if success else None,
                           "error_at": failure.isoformat() if failure else None,
                           "quality": safe_text(row.get("last_quality")) or "unknown",
                           "error": error_category(row.get("last_error")),
                           "success_count": number(row.get("success_count")), "error_count": number(row.get("error_count")),
                           "blocked_until": None, "blocked_active": False, "circuit_open_until": None, "circuit_active": False,
                           "failure_streak": None, "state_updated_at": None})
        for row in self.rows("provider_api_state", required=("api_name",), order="api_name", limit=30):
            blocked, circuit = epoch_instant(row.get("blocked_until")), epoch_instant(row.get("circuit_open_until"))
            updated = instant(row.get("updated_at"))
            streak = number(row.get("failure_streak"))
            result.append({"name": safe_text(row.get("api_name")), "telemetry": "api_rate_limit_state",
                           "last_activity_at": updated.isoformat() if updated else None, "stale": False,
                           "success_at": None, "error_at": None, "quality": None,
                           "error": error_category(row.get("last_error")),
                           "success_count": None, "error_count": None,
                           "blocked_until": blocked.isoformat() if blocked else None,
                           "blocked_active": bool(blocked and blocked > self.now),
                           "circuit_open_until": circuit.isoformat() if circuit else None,
                           "circuit_active": bool(circuit and circuit > self.now),
                           "failure_streak": int(streak) if streak is not None else None,
                           "state_updated_at": updated.isoformat() if updated else None})
        return result

    def job_records(self, jobs):
        """Each job with its attempt count, next retry, stop reason and the latest recorded gate attempt."""
        gates = {}
        keys = [str(r["job_key"]) for r in jobs if r.get("job_key")]
        required = ("job_key", "attempt", "phase", "diagnostics_json", "recorded_at")
        if keys and "screen_gate_diagnostics" in self.tables:
            for row in self.rows("screen_gate_diagnostics", required=required, where=f"job_key IN ({','.join('?' * len(keys))})",
                                 params=keys, order="job_key,attempt DESC", limit=500):
                gates.setdefault(row["job_key"], row)
        result = []
        for row in jobs:
            state, attempts = row.get("status", "unknown"), number(row.get("automatic_attempts"))
            retry_at = epoch_instant(row.get("automatic_next_retry_at"))
            record = {"key": row.get("job_key"), "name": row.get("job_name"), "date": row.get("trade_date"), "state": state,
                      "error": error_category(row.get("error")), "failure_codes": job_failure_codes(row.get("error")),
                      "started_at": row.get("started_at"), "finished_at": row.get("finished_at"),
                      "attempts": int(attempts) if attempts else None,
                      "next_retry_at": retry_at.isoformat() if retry_at and state == "failed" else None,
                      "stop": job_stop(row), "terminal_reason": safe_text(row.get("automatic_terminal_reason"), 120) or None,
                      "gate": None}
            gate = gates.get(row.get("job_key"))
            if gate:
                values = obj(gate.get("diagnostics_json"))
                codes = lambda key: [safe_text(item, 40) for item in arr(values.get(key)) if isinstance(item, str)][:8]
                record["gate"] = {"attempt": number(gate.get("attempt")), "phase": safe_text(gate.get("phase"), 120),
                                  "recorded_at": gate.get("recorded_at"), "generation": number(values.get("raw_generation")),
                                  "batch_id": safe_text(values.get("raw_batch_id"), 80) or None,
                                  "counts": {key: int(number(values[key])) for key in GATE_COUNTS if number(values.get(key)) is not None},
                                  "retryable": codes("gate_retryable"), "unpassable": codes("gate_unpassable"),
                                  "dependency": safe_text(values.get("gate_dependency"), 60) or None,
                                  "unlicensed_risk_fields": codes("gate_unlicensed_risk_fields")}
            result.append(record)
        return result

    def acceptance_records(self, rows):
        """Acceptance rows stay as recorded; later runs and publications for the date sit beside them, never merged in."""
        dates = sorted({str(r["trade_date"]) for r in rows if r.get("trade_date")})
        runs, publications = {}, {}
        if dates:
            marks = ",".join("?" * len(dates))
            for run in self.rows("screen_runs", required=("run_id", "job_name", "actual_trade_date", "status", "finished_at"),
                                 where=f"actual_trade_date IN ({marks})", params=dates, order="finished_at", limit=200):
                runs.setdefault(run["actual_trade_date"], []).append(run)
            if "automatic_close_publications" in self.tables:
                for item in self.rows("automatic_close_publications", required=("actual_trade_date", "run_id", "created_at"),
                                      where=f"actual_trade_date IN ({marks})", params=dates, limit=50):
                    publications[item["actual_trade_date"]] = item
        result = []
        for row in rows:
            day, checked = row.get("trade_date"), instant(row.get("checked_at"))
            linked = []
            for run in runs.get(day, [])[-5:]:
                finished = instant(run.get("finished_at"))
                linked.append({"run_id": safe_text(run.get("run_id"), 40), "job": safe_text(run.get("job_name"), 40),
                               "status": safe_text(run.get("status"), 20), "quality": safe_text(run.get("quality"), 20),
                               "candidates": number(run.get("candidate_count")), "report_version": number(run.get("report_version")),
                               "finished_at": finished.isoformat() if finished else None,
                               "after_check": bool(finished and checked and finished > checked)})
            publication = publications.get(day)
            published = instant(publication.get("created_at")) if publication else None
            late = [run for run in linked if run["after_check"] and run["status"] == "completed"]
            review = ("late_publication" if published and checked and published > checked
                      else "late_screen_not_formal" if late and not publication
                      else "late_screen" if late else "no_later_evidence")
            result.append({"date": day, "checked_at": row.get("checked_at"), "status": row.get("status", "unknown"),
                           "summary": row.get("summary") or "",
                           "findings": [safe_text(item.get("code"), 60) for item in arr(row.get("findings_json"))
                                        if isinstance(item, dict) and item.get("code")][:12],
                           "runs": linked, "review": review,
                           "publication": {"run_id": safe_text(publication.get("run_id"), 40),
                                           "created_at": published.isoformat() if published else None} if publication else None})
        return result

    def health(self):
        providers = self.providers()
        batches = self.rows("batches", required=("actual_trade_date",), order="actual_trade_date DESC,created_at DESC", limit=10)
        jobs = self.rows("job_runs", required=("job_key",), order="started_at DESC", limit=15)
        failures = self.rows("risk_events", required=("code",), order="event_at DESC", limit=20)
        outbox = self.signals()
        deliveries = self.scoped("automatic_close_deliveries", order="created_at DESC", limit=100)
        acceptance = self.rows("daily_acceptance_runs", required=("trade_date", "status", "checked_at"), order="trade_date DESC,checked_at DESC", limit=10)
        acceptance_alerts = self.rows("daily_acceptance_alerts", required=("state",), order="created_at DESC", limit=100)
        return {
            "database": "readable", "integrity": "not_checked", "tables": len(self.tables),
            "data_date": self.primary_source().get("date"),
            "providers": providers,
            "batches": [{"id": r.get("batch_id"), "date": r.get("actual_trade_date"), "generation": r.get("generation"),
                         "state": r.get("status", "unknown"), "rows": r.get("row_count"), "basis": r.get("basis", "unknown"),
                         "published_at": r.get("published_at")} for r in batches],
            "jobs": self.job_records(jobs),
            "failures": [{"code": r.get("code"), "state": r.get("state"), "risk": r.get("risk_level"), "at": r.get("event_at")} for r in failures],
            "outbox": dict(Counter(r["state"] for r in outbox)),
            "automatic_outbox": dict(Counter(r.get("state", "unknown_delivery") for r in deliveries)),
            "daily_acceptance": self.acceptance_records(acceptance),
            "daily_acceptance_outbox": dict(Counter(r.get("state", "unknown_delivery") for r in acceptance_alerts)),
            "evidence": self.source_summary(),
        }

    def source_summary(self):
        sources = []
        for table, date_field, time_field in (("daily_bars", "trade_date", "fetched_at"),
                                            ("factor_snapshots", "as_of", "fetched_at"),
                                            ("data_evidence_records", "business_date", "collected_at")):
            if table not in self.tables:
                sources.append({"dataset": table, "status": "not_collected", "count": None})
                continue
            columns = self.columns(table)
            if not {"source", date_field, time_field}.issubset(columns):
                sources.append({"dataset": table, "status": "unavailable", "count": None})
                continue
            rows = self.db.execute(f"SELECT source,COUNT(*) AS n,MAX({date_field}) AS date,MAX({time_field}) AS collected FROM {table} GROUP BY source LIMIT 30")
            rows = list(rows)
            if not rows:
                sources.append({"dataset": table, "status": "not_collected", "count": 0})
            for row in rows:
                sources.append({"dataset": table, "source": safe_text(row["source"]), "count": row["n"],
                                "date": row["date"], "collected_at": row["collected"], "status": "stored_not_live"})
        batch_columns = self.columns("batches")
        generation_columns = self.columns("active_generations")
        dataset_columns = self.columns("datasets")
        required_batch = {"batch_id", "dataset_id", "actual_trade_date", "status", "quality", "source", "row_count", "published_at"}
        if required_batch.issubset(batch_columns) and {"dataset_id", "active_batch_id", "generation"}.issubset(generation_columns) and {"dataset_id", "dataset_key"}.issubset(dataset_columns):
            rows = self.db.execute(
                "SELECT d.dataset_key,b.source,b.actual_trade_date,b.quality,b.row_count,b.published_at,a.generation "
                "FROM active_generations a JOIN batches b ON b.batch_id=a.active_batch_id "
                "JOIN datasets d ON d.dataset_id=a.dataset_id "
                "WHERE b.status='published' ORDER BY d.dataset_key LIMIT 30"
            )
            for row in rows:
                sources.append({"dataset": "raw_active_generation", "dataset_key": row["dataset_key"],
                                "source": safe_text(row["source"]), "count": row["row_count"],
                                "date": row["actual_trade_date"], "collected_at": row["published_at"],
                                "generation": row["generation"], "quality": row["quality"],
                                "status": "active_published_snapshot"})
        elif batch_columns or generation_columns or dataset_columns:
            sources.append({"dataset": "raw_active_generation", "status": "unavailable", "count": None})
        else:
            sources.append({"dataset": "raw_active_generation", "status": "not_collected", "count": None})
        snapshot_columns = self.columns("daily_snapshot_meta")
        required_snapshot = {"trade_date", "requested_date", "source", "quality", "complete", "fetched_at"}
        if required_snapshot.issubset(snapshot_columns):
            row = self.db.execute(
                "SELECT trade_date,requested_date,source,quality,complete,fetched_at "
                "FROM daily_snapshot_meta ORDER BY trade_date DESC,fetched_at DESC LIMIT 1"
            ).fetchone()
            if row:
                sources.append({"dataset": "daily_snapshot_meta", "source": safe_text(row["source"]),
                                "count": None, "date": row["trade_date"], "requested_date": row["requested_date"],
                                "collected_at": row["fetched_at"], "quality": row["quality"],
                                "complete": row["complete"] == 1,
                                "status": "complete_stored_snapshot" if row["complete"] == 1 else "incomplete_snapshot"})
            else:
                sources.append({"dataset": "daily_snapshot_meta", "status": "not_collected", "count": 0})
        elif snapshot_columns:
            sources.append({"dataset": "daily_snapshot_meta", "status": "unavailable", "count": None})
        else:
            sources.append({"dataset": "daily_snapshot_meta", "status": "not_collected", "count": None})
        return sources

class Dashboard:
    def __init__(self, database: Path, *, origin="", settings=None, artifact_path=None, snapshot_status_path=None,
                 signals_path=None, now=None):
        self.database = Path(database).absolute()
        self.origin, self.settings = origin, settings
        self.signals_path = Path(signals_path).absolute() if signals_path else None
        self._signals_cache = (None, None)
        self.artifact_configured = artifact_path is not None
        self.artifact_path = Path(artifact_path).absolute() if artifact_path else self.database.with_name("intraday_quotes.json")
        self.snapshot_status_path = (Path(snapshot_status_path).absolute() if snapshot_status_path
                                     else self.database.with_name("snapshot_status.json"))
        self.build_info_path = ROOT / "webapp" / "build_info.json"
        self.catalog_dir = ROOT / "docs" / "research"
        self.clock = now or (lambda: datetime.now(timezone.utc))

    def research_catalog(self):
        entries, unavailable = [], []
        for spec in RESEARCH_CATALOG:
            try:
                entries.append(catalog_entry(self.catalog_dir, spec))
            except FileNotFoundError:
                unavailable.append({"file": spec["file"], "reason": "frozen_file_missing"})
            except (OSError, UnicodeError, ValueError, TypeError):
                unavailable.append({"file": spec["file"], "reason": "frozen_file_unreadable"})
        return {"entries": entries, "unavailable": unavailable, "stage_vocabulary": list(RESEARCH_STAGES),
                "formal_tables_written": False}

    def _signals_file(self):
        stat = self.signals_path.stat()
        if stat.st_size > SIGNALS_MAX_BYTES:
            raise ValueError("signals_file_too_large")
        key = (stat.st_mtime_ns, stat.st_size)
        if self._signals_cache[0] != key:
            self._signals_cache = (key, json.loads(self.signals_path.read_text(encoding="utf-8")))
        return self._signals_cache[1]

    def research_signals(self, code=None):
        """Evening research job output (scheme F overheat label, scheme D index trend); display only."""
        empty = {"status": "not_configured", "generated_at": None, "age_seconds": None, "inputs": None,
                 "overheat": None, "index_trend": None, "stock": None, "display_only": True}
        if self.signals_path is None:
            return empty
        try:
            data = self._signals_file()
        except FileNotFoundError:
            return {**empty, "status": "missing"}
        except (OSError, UnicodeError, ValueError):
            return {**empty, "status": "unreadable"}
        if not isinstance(data, dict) or data.get("schema") != SIGNALS_SCHEMA:
            return {**empty, "status": "invalid"}
        generated = instant(data.get("generated_at"))
        age = (self.clock() - generated).total_seconds() if generated else None
        hot = obj(data.get("overheat"))
        hot_items = []
        for item in arr(hot.get("hot"))[:2000]:
            item = obj(item)
            item_code = safe_text(item.get("code"), 6)
            if re.fullmatch(r"\d{6}", item_code):
                hot_items.append({"code": item_code, "name": safe_text(item.get("name"), 40) or None,
                                  "score": number(item.get("score")), "pct": number(item.get("pct"))})
        overheat = {"status": safe_text(hot.get("status"), 20) or "unknown",
                    "reason": "pool_lt_30" if hot.get("reason") == "pool_lt_30" else
                              "history_lt_60" if hot.get("reason") == "history_lt_60" else
                              "job_error" if hot.get("reason") else None,
                    "rule": safe_text(hot.get("rule"), 20) or None, "trade_date": safe_text(hot.get("trade_date"), 10) or None,
                    "pool_size": number(hot.get("pool_size")), "evaluated": number(hot.get("evaluated")),
                    "quantile": number(hot.get("quantile")), "threshold": number(hot.get("threshold")),
                    "hot_count": len(hot_items), "hot": hot_items,
                    "approximations": [a for a in arr(hot.get("approximations")) if a in SIGNAL_APPROXIMATIONS],
                    "excluded_counts": {key: number(value) for key, value in obj(hot.get("excluded_counts")).items()
                                        if key in SIGNAL_EXCLUSIONS}}
        trend = obj(data.get("index_trend"))
        index_trend = {"status": safe_text(trend.get("status"), 20) or "unknown", "rule": safe_text(trend.get("rule"), 20) or None,
                       "source": safe_text(trend.get("source"), 80) or None,
                       "indices": {code_: signal_index(item) for code_, item in obj(trend.get("indices")).items()
                                   if code_ in SIGNAL_INDICES},
                       "failed": sorted(code_ for code_ in obj(trend.get("errors")) if code_ in SIGNAL_INDICES)}
        inputs = obj(data.get("inputs"))
        return {"status": "available" if age is not None and 0 <= age <= SIGNALS_MAX_AGE_SECONDS else "stale",
                "generated_at": generated.isoformat() if generated else None,
                "age_seconds": int(age) if age is not None and age >= 0 else None,
                "inputs": {"trade_date": safe_text(inputs.get("trade_date"), 10) or None,
                           "generation": number(inputs.get("generation")), "sessions": number(inputs.get("sessions")),
                           "snapshot_revision": safe_text(inputs.get("snapshot_revision"), 16) or None},
                "overheat": overheat, "index_trend": index_trend,
                "stock": signal_stock(code, hot) if code else None, "display_only": True}

    def snapshot_check(self):
        """The refresh timer's last check; it can be recent while an unchanged copy stays old."""
        unknown = {"status": "unknown", "checked_at": None, "result": None, "age_seconds": None,
                   "last_published_at": None, "failure_category": None}
        try:
            values = json.loads(self.snapshot_status_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {**unknown, "reason": "snapshot_status_missing"}
        except (OSError, UnicodeError, ValueError):
            return {**unknown, "reason": "snapshot_status_unreadable"}
        if not isinstance(values, dict):
            return {**unknown, "reason": "snapshot_status_unreadable"}
        checked, published = instant(values.get("checked_at")), instant(values.get("last_published_at"))
        result = values.get("result") if values.get("result") in ("published", "unchanged", "failed") else None
        age = (self.clock() - checked).total_seconds() if checked else None
        if checked is None or result is None or age < 0:
            return {**unknown, "reason": "snapshot_status_invalid"}
        return {"status": "failed" if result == "failed" else "stale" if age > 7200 else "recent",
                "reason": None, "checked_at": checked.isoformat(), "result": result, "age_seconds": int(age),
                "last_published_at": published.isoformat() if published else None,
                "failure_category": safe_text(values.get("category"), 60) or None}

    def build_info(self):
        try:
            recorded = obj(self.build_info_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError):
            recorded = {}
        build = {key: safe_text(recorded.get(key), 80) or None for key in ("release", "revision", "built_at")}
        build["plugin_main_sha256"] = sha256_text(recorded.get("plugin_main_sha256"))
        build["status"] = "recorded" if build["release"] or build["revision"] else "unknown"
        return build

    def version(self):
        """Which features this deployed backend actually contains, independent of plugin version numbers."""
        build = self.build_info()
        schema_version = None
        try:
            with self.snapshot() as snapshot:
                if "schema_meta" in snapshot.tables:
                    row = snapshot.db.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
                    schema_version = int(row[0]) if row and str(row[0]).isdigit() else None
        except (Unavailable, sqlite3.Error, OSError, ValueError):
            schema_version = None
        return {"api_version": API_VERSION, "capabilities": list(CAPABILITIES), "build": build,
                "database_schema_version": schema_version}

    @contextmanager
    def snapshot(self):
        if not self.database.is_file():
            raise Unavailable("database_missing")
        db = sqlite3.connect(self.database.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.3)
        db.row_factory = sqlite3.Row
        deadline = time.monotonic() + 3
        try:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            db.set_progress_handler(lambda: int(time.monotonic() > deadline), 5000)
            yield Snapshot(db, self.origin, self.clock())
        finally:
            db.rollback()
            db.close()

    def settings_snapshot(self):
        """The plugin's load-time settings file (``--settings``, else next to the intraday artifact)."""
        path = (Path(self.settings) if self.settings is not None
                else self.artifact_path.with_name("public_settings.json") if self.artifact_configured else None)
        unknown = {"status": "not_configured", "values": {}, "configured": {}, "written_at": None, "plugin_version": None,
                   "code_sha256": None, "schema_sha256": None, "deprecated_settings": [], "setting_issues": []}
        if path is None:
            return unknown
        try:
            if path.stat().st_size > 262144:
                return {**unknown, "status": "invalid"}
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {**unknown, "status": "missing"}
        except (OSError, UnicodeError, ValueError):
            return {**unknown, "status": "unreadable"}
        if not isinstance(data, dict) or not isinstance(data.get("values"), dict):
            return {**unknown, "status": "invalid"}
        written = instant(data.get("written_at"))
        return {"status": "plugin_snapshot" if data.get("schema_version") == 1 else "explicit_values",
                "values": data["values"], "configured": obj(data.get("configured")),
                "written_at": written.isoformat() if written else None,
                "plugin_version": safe_text(data.get("plugin_version"), 40) or None,
                "code_sha256": sha256_text(data.get("code_sha256")), "schema_sha256": sha256_text(data.get("schema_sha256")),
                "deprecated_settings": [safe_text(key, 60) for key in arr(data.get("deprecated_settings")) if isinstance(key, str)][:20],
                "setting_issues": setting_issue_rows(data.get("setting_issues"))}

    def public_settings(self):
        """Schema default next to the plugin's load-time value; strings outside the allowlist show only a state."""
        schema = obj((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))
        snapshot = self.settings_snapshot()
        values, configured = snapshot["values"], snapshot["configured"]
        source = "explicit_public_snapshot" if snapshot["status"] == "explicit_values" else "plugin_snapshot"
        result = []
        for key, spec in schema.items():
            if not isinstance(spec, dict):
                continue
            kind, default, value = spec.get("type"), spec.get("default"), values.get(key)
            shown = ((kind == "bool" and isinstance(value, bool))
                     or (kind in ("int", "float") and not isinstance(value, bool) and number(value) is not None)
                     or (kind == "string" and key in PUBLIC_STRING_SETTINGS and isinstance(value, str)
                         and re.fullmatch(r"[A-Za-z0-9_.:,\-]{0,40}", value) is not None))
            if shown:
                state, differs = "shown", value != default
            else:
                state = configured.get(key) if configured.get(key) in CONFIGURED_STATES else "unknown"
                differs = {"custom": True, "invalid_type": True, "default": False,
                           "empty": str(default or "").strip() not in ("", "[]", "{}")}.get(state)
            result.append({"key": key, "label": spec.get("description", key), "type": kind,
                           "group": "common" if key in COMMON_SETTINGS else "advanced",
                           "default": default, "effective": value if shown else None, "state": state, "differs": differs,
                           "source": "effective_unknown" if state == "unknown" else source})
        result.sort(key=lambda item: (item["group"] != "common", item["key"]))
        plugin_sha = snapshot["code_sha256"]
        build_sha = self.build_info()["plugin_main_sha256"] if plugin_sha else None
        meta = {key: snapshot[key] for key in ("status", "written_at", "plugin_version", "code_sha256", "schema_sha256",
                                               "deprecated_settings", "setting_issues")}
        meta["matches_web_build"] = (plugin_sha == build_sha) if plugin_sha and build_sha else None
        return {"items": result, "snapshot": meta, "read_only": True, "sensitive_fields": "not_exposed"}

    def intraday(self):
        try:
            artifact = json.loads(self.artifact_path.read_text(encoding="utf-8"))
            if not isinstance(artifact, dict) or artifact.get("schema_version") != 1:
                raise ValueError("artifact_schema_invalid")
        except FileNotFoundError:
            artifact = {"schema_version": 1, "status": "unknown", "reason": "artifact_missing", "quotes": []}
        except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
            artifact = {"schema_version": 1, "status": "unknown", "reason": "artifact_parse_failed", "quotes": []}
        now = self.clock()
        published = instant(artifact.get("published_at"))
        age = (now - published).total_seconds() if published else None
        status, reason = str(artifact.get("status") or "unknown"), str(artifact.get("reason") or "")
        valid_for = number(artifact.get("valid_for_seconds"))
        valid_for_seconds = max(30, min(int(valid_for), 900)) if valid_for is not None else 30
        if published is None:
            status, reason = "unknown", reason or "published_at_missing"
        elif age < 0:
            status, reason = "unknown", "published_at_future"
        elif age > valid_for_seconds:
            status, reason = "unknown", "artifact_expired"
        rows, clean, times = artifact.get("quotes"), [], []
        if not isinstance(rows, list):
            rows = []
            status, reason = "unknown", "artifact_quotes_invalid"
        for row in rows:
            if not isinstance(row, dict):
                status, reason = "unknown", reason or "quote_parse_failed"
                continue
            provider, collected = instant(row.get("provider_ts")), instant(row.get("collected_at"))
            if provider is None or collected is None or provider > now or collected > now:
                status, reason = "unknown", reason or "quote_timestamp_invalid"
                continue
            times.append(provider)
            clean.append({key: row.get(key) for key in ("code", "name", "price", "pct_change", "amount", "volume", "provider_ts", "collected_at", "source")})
        if times and (max(times) - min(times)).total_seconds() > 90:
            status, reason = "unknown", "mixed_provider_timestamps"
        market = self._artifact_live_market(artifact, now, status, reason)
        effective_revision = stable_artifact_state_revision(artifact.get("revision"), status, reason, market)
        return {"artifact_path": self.artifact_path.name, "artifact_path_configured": self.artifact_configured, "revision": artifact.get("revision"),
                "source": artifact.get("source") or "unknown", "status": status, "reason": reason,
                "target_count": int(artifact.get("target_count") or 0), "returned_count": int(artifact.get("returned_count") or len(clean)),
                "missing_count": int(artifact.get("missing_count") or 0), "provider_ts": artifact.get("provider_ts") or [],
                "collected_at": artifact.get("collected_at"), "published_at": artifact.get("published_at"),
                "age_seconds": max(0, int(age)) if age is not None and age >= 0 else None, "valid_for_seconds": valid_for_seconds,
                "market": market, "effective_revision": effective_revision, "quotes": clean}

    @staticmethod
    def _unknown_live_market(reason, *, pending=False):
        return {"status": "pending" if pending else "unknown", "regime": "unknown", "quality": "unknown",
                "reason": str(reason or "live_market_evidence_unavailable"), "source": "",
                "source_timestamp": None, "updated_at": None, "sample_size": 0, "expected_size": 0,
                "coverage": None, "opportunity_allowed": False}

    def _artifact_live_market(self, artifact, now, artifact_status, artifact_reason):
        if artifact_reason in {"artifact_missing", "artifact_parse_failed", "published_at_missing", "published_at_future", "artifact_expired"}:
            return self._unknown_live_market(artifact_reason or "artifact_unavailable")
        values = artifact.get("market") if isinstance(artifact, dict) else None
        if not isinstance(values, dict):
            return self._unknown_live_market("live_market_missing")
        regime = str(values.get("regime") or "unknown").strip().lower()
        pending_regime = str(values.get("pending_regime") or "unknown").strip().lower()
        quality = str(values.get("quality") or "unknown").strip().lower()
        reason = str(values.get("reason") or "live_market_evidence_unavailable").strip()
        source = str(values.get("source") or "").strip()
        source_timestamp, updated_at = instant(values.get("source_timestamp")), instant(values.get("updated_at"))
        if regime == "unknown":
            return self._unknown_live_market(reason, pending=pending_regime in {"strong", "neutral", "weak", "risk_off"})
        if regime not in {"strong", "neutral", "weak", "risk_off"} or quality != "good" or reason != "confirmed" or source != "sina":
            return self._unknown_live_market(reason or "live_market_unconfirmed")
        if source_timestamp is None or updated_at is None:
            return self._unknown_live_market("live_market_timestamp_missing")
        local_now = now.astimezone(CHINA)
        if source_timestamp > now or updated_at > now:
            return self._unknown_live_market("live_market_timestamp_future")
        if source_timestamp.astimezone(CHINA).date() != local_now.date() or updated_at.astimezone(CHINA).date() != local_now.date():
            return self._unknown_live_market("live_market_not_today")
        if (now - source_timestamp).total_seconds() > 150 or (now - updated_at).total_seconds() > 150:
            return self._unknown_live_market("live_market_expired")
        sample, expected, coverage = number(values.get("sample_size")), number(values.get("expected_size")), number(values.get("coverage"))
        if sample is None or expected is None or coverage is None or sample < 1 or expected < 1 or sample > expected or not 0 <= coverage <= 1:
            return self._unknown_live_market("live_market_coverage_invalid")
        return {"status": "available", "regime": regime, "quality": quality, "reason": reason, "source": source,
                "source_timestamp": source_timestamp.isoformat(), "updated_at": updated_at.isoformat(),
                "sample_size": int(sample), "expected_size": int(expected), "coverage": coverage,
                "opportunity_allowed": regime != "risk_off"}

    def query(self, route, params=None):
        params = params or {}
        if route == "intraday":
            try:
                data = self.intraday()
                return {"meta": {"status": "partial" if data.get("status") != "available" else "available", "read_only": True,
                                 "dataset_kind": "intraday_artifact", "database": self.database.name,
                                 "artifact_path": self.artifact_path.name, "at": self.clock().isoformat(),
                                 "sources": [{"dataset": "intraday_artifact", "status": data.get("status"), "count": data.get("returned_count")}], "notices": []}, "data": data}
            except Exception:
                return {"meta": {"status": "unavailable", "reason": "artifact_unavailable", "read_only": True,
                                 "dataset_kind": "intraday_artifact", "at": self.clock().isoformat()}, "data": None}
        if route == "research_catalog":
            data = self.research_catalog()
            return {"meta": {"status": "partial" if data["unavailable"] else "available", "read_only": True,
                             "dataset_kind": "research_files", "database": self.database.name, "at": self.clock().isoformat(),
                             "sources": [{"dataset": "research_catalog", "status": "frozen_files", "count": len(data["entries"])}],
                             "notices": [item["file"] + ":" + item["reason"] for item in data["unavailable"]]},
                    "data": data}
        if route == "research_signals":
            code = params.get("code")
            if code is not None and not re.fullmatch(r"\d{6}", str(code)):
                return {"meta": {"status": "unavailable", "reason": "invalid_stock_code", "read_only": True,
                                 "dataset_kind": "research_job_file", "at": self.clock().isoformat()}, "data": None}
            data = self.research_signals(code)
            return {"meta": {"status": "available" if data["status"] == "available" else "partial", "read_only": True,
                             "dataset_kind": "research_job_file", "database": self.database.name, "at": self.clock().isoformat(),
                             "sources": [{"dataset": "research_signals", "status": data["status"],
                                          "count": (data["overheat"] or {}).get("hot_count", 0)}],
                             "notices": [] if data["status"] == "available" else ["research_signals:" + data["status"]]},
                    "data": data}
        if route == "version":
            data = self.version()
            return {"meta": {"status": "available" if data["build"]["status"] == "recorded" else "partial", "read_only": True,
                             "dataset_kind": "web_build", "database": self.database.name, "at": self.clock().isoformat(),
                             "sources": [], "notices": [] if data["build"]["status"] == "recorded" else ["build_info:unavailable"]},
                    "data": data}
        try:
            with self.snapshot() as snapshot:
                artifact = self.intraday()
                if artifact.get("status") != "available":
                    snapshot.notices.add("intraday_artifact:" + safe_text(artifact.get("reason") or artifact.get("status")))
                if route == "overview":
                    data = snapshot.overview()
                    data["live_market"] = artifact.get("market")
                elif route == "revision":
                    metadata = snapshot.snapshot_metadata()
                    snapshot_revision = metadata.get("revision")
                    artifact_revision = artifact.get("revision")
                    data = {"data_revision": stable_data_revision(snapshot_revision, artifact.get("effective_revision")),
                            "snapshot_revision": snapshot_revision, "artifact_revision": artifact_revision,
                            "captured_at": metadata.get("captured_at"),
                            "status": metadata.get("status", "unknown"),
                            "refresh_interval_seconds": metadata.get("refresh_interval_seconds"),
                            "poll_interval_seconds": metadata.get("poll_interval_seconds", 5)}
                elif route == "signals":
                    data = {"items": snapshot.signals(), "origin_configured": bool(self.origin)}
                elif route == "candidates":
                    data = {"items": snapshot.attach_latest_closes(snapshot.candidates()), "research": snapshot.research_pools()}
                elif route.startswith("stocks/"):
                    data = snapshot.stock(route.split("/", 1)[1])
                elif route == "search":
                    data = snapshot.search(params.get("q", ""))
                elif route == "performance":
                    data = snapshot.performance(int(params.get("horizon", "5")))
                elif route == "health":
                    data = snapshot.health()
                    values = self.settings_snapshot()["values"]
                    data["automatic_close_limits"] = {
                        key: values[name] if isinstance(values.get(name), int) and not isinstance(values.get(name), bool) else None
                        for key, name in (("max_attempts", "automatic_close_max_attempts"),
                                          ("retry_seconds", "automatic_close_retry_seconds"),
                                          ("retry_window_seconds", "automatic_close_retry_window_seconds"))}
                elif route == "settings":
                    data = self.public_settings()
                elif route == "intraday":
                    data = self.intraday()
                else:
                    raise Unavailable("route_not_found")
                if route == "revision":
                    return {"meta": {"status": "partial" if snapshot.notices else "available", "read_only": True,
                                     "dataset_kind": "synthetic_demo" if snapshot.demo else "local_database",
                                     "database": self.database.name, "at": self.clock().isoformat(),
                                     "data_revision": data.get("data_revision"),
                                     "artifact_revision": data.get("artifact_revision"),
                                     "snapshot_revision": data.get("snapshot_revision"),
                                     "snapshot": {**snapshot.snapshot_metadata(), "check": self.snapshot_check()},
                                     "sources": [], "notices": sorted(snapshot.notices)},
                            "data": data}
                sources = snapshot.source_summary()
                primary_source = snapshot.primary_source()
                snapshot_metadata = {**snapshot.snapshot_metadata(), "check": self.snapshot_check()}
                artifact_revision = artifact.get("revision")
                data_revision = stable_data_revision(snapshot_metadata.get("revision"), artifact.get("effective_revision"))
                return {"meta": {"status": "partial" if snapshot.notices else "available", "read_only": True,
                                 "dataset_kind": "synthetic_demo" if snapshot.demo else "local_database",
                                 "database": self.database.name, "at": self.clock().isoformat(),
                                 "data_revision": data_revision, "artifact_revision": artifact_revision,
                                 "snapshot_revision": snapshot_metadata.get("revision"),
                                 "snapshot": snapshot_metadata,
                                 "primary_source": primary_source,
                                 "sources": sources,
                                 "notices": sorted(snapshot.notices)}, "data": data}
        except (sqlite3.Error, OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError, Unavailable) as exc:
            reason = str(exc) if isinstance(exc, Unavailable) else "database_busy_or_schema_unavailable" if isinstance(exc, sqlite3.Error) else "input_or_source_unavailable"
            return {"meta": {"status": "unavailable", "reason": reason, "read_only": True,
                             "dataset_kind": "unknown", "at": self.clock().isoformat()}, "data": None}
