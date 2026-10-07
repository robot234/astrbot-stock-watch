"""S03 / S04: the screening funnel and per-stock audit rows are recorded, consistent and display-only."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import importlib
import itertools
import json
import sqlite3

from test_v0163_degraded_watch import _raw_main, core, main_module, policy
from webapp.data import SCREEN_STAGES, Dashboard, screen_audit_row, screen_funnel

audit = importlib.import_module("astrbot_stock_watch.screen_audit")
NOW = datetime(2026, 10, 7, 9, 30, tzinfo=timezone.utc)


def stage(funnel, key):
    return next(item for item in funnel["stages"] if item["key"] == key)


def test_quote_gate_matches_is_screenable_for_every_state():
    values = (True, False, None)
    for price, states in itertools.product((0.0, 1.0, 10.0, 99.0, float("nan")), itertools.product(values, repeat=4)):
        quote = core.Quote("600001", "x", price, suspended=states[0], limit_up=states[1], limit_down=states[2], st=states[3])
        gate = audit.quote_gate(quote, 2, 80)
        assert (gate is None) == core.is_screenable(quote, 2, 80)
        if gate not in (None, "price_out_of_range", "risk_state_unknown"):
            assert getattr(quote, gate) is True


def candidate(code, base, risk="eligible", flags=()):
    item = core.Candidate(core.Quote(code, code, 10.0, history_days=30), base, [f"规则{base:+d}"], base_score=base)
    item.risk_level, item.risk_flags = risk, list(flags)
    return item


def test_statuses_and_rows_follow_the_real_selection():
    scored = [candidate("600001", 30), candidate("600002", 25), candidate("600003", 20, "watch_only"),
              candidate("600004", 5), candidate("600005", 40, "blocked", ["停牌"]),
              candidate("600006", 35, "unknown", ["技术数据不完整"]), candidate("600007", 35, "unknown", ["ST/审计状态未知"]),
              candidate("600008", 35, "unknown", ["ST/审计状态未知"])]
    qualified = [item for item in scored if item.base_score >= 10 and item.risk_level not in {"blocked", "unknown"}]
    final = qualified[:2]
    statuses = audit.scored_statuses(scored, minimum=10, factor_codes={"600001", "600002", "600003", "600007"},
                                     qualified=qualified, final=final, fallback=[])
    assert [statuses[id(item)] for item in scored] == [
        "candidate", "candidate", "beyond_candidate_limit", "below_min_score", "risk_blocked",
        "technical_incomplete", "st_audit_unknown", "factor_not_checked"]
    rows = audit.audit_rows(scored, statuses, {"600001": "raw_batch"}, final=final, limit=3)
    assert [row["rank"] for row in rows] == [1, 2, 3] and rows[0]["comparable"] is False
    assert rows[0]["missing_inputs"] == list(audit.SCORE_INPUTS) and rows[1]["indicator_status"] == "not_computed"
    late = audit.audit_rows(scored, statuses, {}, final=[scored[6]], limit=2)
    assert [row["code"] for row in late] == ["600001", "600007"]

    fallback = [scored[3]]
    statuses = audit.scored_statuses(scored, minimum=50, factor_codes=set(), qualified=[], final=fallback, fallback=fallback)
    assert statuses[id(scored[3])] == "fallback_candidate" and statuses[id(scored[0])] == "below_min_score"


def test_normal_close_screen_records_a_consistent_funnel_and_rows(tmp_path, monkeypatch):
    as_of = datetime.now(core.CHINA_TZ).date().isoformat()
    monkeypatch.setattr(policy, "accepted_negative", lambda source, version, field: field in policy.RISK_FIELDS)
    monkeypatch.setattr(main_module, "safe_factor_row", lambda row, code, as_of, known_at: dict(row))
    main, quotes = _raw_main(tmp_path, as_of)

    result = asyncio.run(main._score_quotes_result(quotes, 10, as_of, context="daily_close", requested_date=as_of))

    diagnostics = result.diagnostics
    funnel, rows = diagnostics["screen_funnel"], diagnostics["screen_audit"]
    json.dumps({key: diagnostics[key] for key in ("screen_funnel", "screen_audit", "screen_audit_total")},
               ensure_ascii=False, allow_nan=False)
    assert [item["key"] for item in funnel["stages"]] == list(SCREEN_STAGES)
    assert [item["key"] for item in screen_funnel(funnel)["stages"]] == list(SCREEN_STAGES)
    assert stage(funnel, "input")["count"] == diagnostics["input"] == 4
    assert stage(funnel, "risk_state")["count"] == diagnostics["tradable"] == 2
    assert stage(funnel, "risk_state")["excluded"] == {"limit_up": 1, "risk_state_unknown": 1}
    assert stage(funnel, "deep_screen")["count"] == diagnostics["indicator_targets"] == 2
    assert stage(funnel, "indicators")["count"] == diagnostics["enriched"] and stage(funnel, "indicators")["gate"] is False
    assert stage(funnel, "min_score")["count"] == diagnostics["qualified"]
    assert stage(funnel, "candidates")["count"] == diagnostics["candidate_count"] == len(result.candidates)
    assert diagnostics["screen_audit_total"] == 2 and len(rows) == 2
    assert {row["code"] for row in rows if row["status"] in {"candidate", "fallback_candidate"}} == {
        item.quote.code for item in result.candidates}
    assert all(row["comparable"] for row in rows if row["indicator_status"] == "raw_batch" and not row["missing_inputs"])


def test_unverified_risk_funnel_says_where_everything_stopped(tmp_path, monkeypatch):
    as_of = datetime.now(core.CHINA_TZ).date().isoformat()
    monkeypatch.setattr(policy, "accepted_negative", lambda source, version, field: False)
    main, quotes = _raw_main(tmp_path, as_of)

    result = asyncio.run(main._score_quotes_result(quotes, 10, as_of, include_factors=False,
                                                   context="daily_close", requested_date=as_of))

    funnel = result.diagnostics["screen_funnel"]
    assert stage(funnel, "price")["count"] == 4
    assert stage(funnel, "risk_state")["count"] == 0
    assert stage(funnel, "risk_state")["excluded"] == {"limit_up": 1, "risk_state_unknown": 3}
    assert stage(funnel, "candidates")["count"] == 0 and result.diagnostics["screen_audit"] == []


def test_degraded_watch_copy_leaves_out_the_audit_rows(tmp_path):
    as_of = datetime.now(core.CHINA_TZ).date().isoformat()
    main, _quotes = _raw_main(tmp_path, as_of)
    saved = {}

    def saver(job_key, trade_date, reason, missing, diagnostics, items):
        saved.update(diagnostics)
        return 1

    main.store.save_degraded_watch_list = saver
    diagnostics = {"screen_audit": [{"code": "600001"}], "screen_funnel": {"version": 1}, "input": 4}
    result = asyncio.run(main._save_and_push_degraded_watch("job", as_of, "risk_evidence_missing", [], diagnostics, [],
                                                            terminal=False))
    assert result == {"state": "saved", "pushed": 0}
    assert saved == {"screen_funnel": {"version": 1}, "input": 4}


def run_row(db, run_id, date, finished, diagnostics):
    db.execute("INSERT INTO screen_runs(run_id,job_name,requested_date,actual_trade_date,source,started_at,finished_at,"
               "quote_count,candidate_count,status,quality,diagnostics) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
               (run_id, "daily_screen", date, date, "tushare", finished, finished, 4, 1, "completed", "good",
                json.dumps(diagnostics, ensure_ascii=False)))


def test_web_reads_the_newest_recorded_funnel_and_sanitizes_it(tmp_path):
    from test_v0139_intraday_m2 import _imports
    _, _, Store = _imports()
    database = tmp_path / "plugin.sqlite3"
    Store(database)
    funnel = {"version": 1, "stages": [
        {"key": "input", "count": 4}, {"key": "price", "count": 4, "excluded": {}, "price_min": 2, "price_max": 80},
        {"key": "risk_state", "count": 2, "excluded": {"limit_up": 1, "risk_state_unknown": 1, "<b>x</b>": 3, "st": True}},
        {"key": "evil", "count": 9}, {"key": "candidates", "count": 1, "excluded": {}, "limit": 10, "fallback": 0}]}
    rows = [{"code": "600001", "name": "<i>合成</i>", "rank": 1, "status": "candidate", "base_score": 30, "score": 30,
             "score_max": 50, "risk_level": "eligible", "risk_flags": [], "reasons": ["均线多头趋势+12", 7],
             "indicators": {"rsi6": 55.0, "evil": 1}, "history_days": 40, "indicator_status": "raw_batch",
             "comparable": True, "missing_inputs": ["volume_ratio", "evil"]},
            {"code": "../etc", "rank": 2, "status": "candidate"}, {"code": "600002", "rank": 3, "status": "hacked"}]
    db = sqlite3.connect(database)
    try:
        run_row(db, "run-old", "2026-09-29", "2026-09-29T08:00:00Z", {"screen_funnel": funnel, "screen_audit": rows,
                                                                       "screen_audit_total": 2})
        run_row(db, "run-new", "2026-09-30", "2026-09-30T08:00:00Z", {"indicator_targets": 0})
        db.commit()
        db.execute("PRAGMA journal_mode=DELETE")
    finally:
        db.close()
    before = hashlib.sha256(database.read_bytes()).digest()
    dashboard = Dashboard(database, now=lambda: NOW)
    screen = dashboard.query("candidates")["data"]["screen"]
    assert screen["status"] == "available" and screen["run_id"] == "run-old" and screen["is_latest"] is False
    assert screen["latest_run_id"] == "run-new" and screen["audit_total"] == 2
    assert [item["key"] for item in screen["funnel"]["stages"]] == ["input", "price", "risk_state", "candidates"]
    assert stage(screen["funnel"], "risk_state")["excluded"] == {"limit_up": 1, "risk_state_unknown": 1}
    assert [row["code"] for row in screen["audit"]] == ["600001", "600002"]
    first, second = screen["audit"]
    assert first["reasons"] == ["均线多头趋势+12"] and first["indicators"] == {"rsi6": 55.0}
    assert first["missing_inputs"] == ["volume_ratio"] and first["name"] == "<i>合成</i>" and second["status"] == "unknown"
    assert hashlib.sha256(database.read_bytes()).digest() == before
    assert screen_funnel({"version": 2, "stages": funnel["stages"]}) is None and screen_audit_row({"code": "600001"}) is None


def test_stock_page_gets_its_row_from_the_latest_recorded_screen(tmp_path):
    from test_v0139_intraday_m2 import _imports
    _, _, Store = _imports()
    database = tmp_path / "plugin.sqlite3"
    Store(database)
    db = sqlite3.connect(database)
    try:
        db.executemany("INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?)",
                       [("600001", "甲", "甲", "stock_basic", "2026-09-30T08:00:00"),
                        ("600009", "乙", "乙", "stock_basic", "2026-09-30T08:00:00")])
        run_row(db, "run-1", "2026-09-30", "2026-09-30T08:00:00Z", {
            "screen_funnel": {"version": 1, "stages": [{"key": "input", "count": 2}]}, "screen_audit_total": 300,
            "screen_audit": [{"code": "600001", "rank": 7, "status": "below_min_score", "base_score": 8, "reasons": ["5日动量转强+8"]}]})
        db.commit()
    finally:
        db.close()
    dashboard = Dashboard(database, now=lambda: NOW)
    listed = dashboard.query("stocks/600001")["data"]["screen_audit"]
    assert listed["status"] == "listed" and listed["row"]["rank"] == 7 and listed["audit_total"] == 300
    assert dashboard.query("stocks/600009")["data"]["screen_audit"] == {
        "status": "not_listed", "run_id": "run-1", "date": "2026-09-30", "is_latest": True, "audit_total": 300,
        "audit_shown": 1, "row": None}
