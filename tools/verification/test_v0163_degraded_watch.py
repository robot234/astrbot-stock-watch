"""Indicator coverage, gate diagnostics and the separate degraded watch list."""
from __future__ import annotations

import asyncio
import importlib
import json
import types
from datetime import date, datetime, timedelta

from test_v0139_intraday_m2 import _imports, _main
from test_v0138_automatic_close import _automatic_reconciliation_main, _candidate, _imports as _imports138

Main, core, StockStore = _imports()
main_module = importlib.import_module("astrbot_stock_watch.main")
policy = importlib.import_module("astrbot_stock_watch.formal_source_policy")


def _bars(code: str, as_of: str, days: int = 40, start: float = 10.0) -> list[dict]:
    """Synthetic unadjusted Tushare-shaped daily rows ending on ``as_of``."""
    end = date.fromisoformat(as_of)
    rows, previous = [], start
    for index in range(days):
        close = round(previous * 1.003, 4)
        rows.append({
            "code": code, "trade_date": (end - timedelta(days=days - 1 - index)).isoformat(),
            "open": previous, "high": round(close * 1.01, 4), "low": round(previous * 0.99, 4),
            "close": close, "pre_close": previous, "pct_change": (close / previous - 1) * 100,
            "volume": 1_000_000 + index, "amount": 10_000_000.0 + index,
            "price_basis": "unadjusted", "source": "tushare",
        })
        previous = close
    return rows


def _quote(code: str, close: float, amount: float, **risk) -> core.Quote:
    return core.Quote(code, f"合成{code}", close, round(close / 1.003, 4), amount, 0.3, 1_000_000.0,
                      source="tushare", **risk)


def _raw_main(tmp_path, as_of: str):
    store = StockStore(tmp_path / "coverage.sqlite3")
    store.factor_snapshots = lambda _as_of: {code: {"st_flag": False, "audit_flag": False}
                                             for code in ("600001", "600002", "600003")}
    store.save_factor_snapshots = lambda *_args, **_kwargs: None
    main = _main(Main, store, deep_screen_limit=300, fallback_limit=5, degraded_watch_limit=10)
    main.quotes = types.SimpleNamespace(tushare_token="")
    main.raw_dataset_key = "tushare_daily"
    main._tushare_mode = lambda: True
    main._raw_screen_provenance = {}
    history = {code: _bars(code, as_of) for code in ("600001", "600002", "600003", "600004")}
    closes = {code: rows[-1]["close"] for code, rows in history.items()}

    async def no_names(_quotes, _as_of):
        return None

    async def fresh_history(codes, _before, **_kwargs):
        return {code: history[code] for code in codes if code in history}, {"batch_id": "batch-1", "generation": 3}, ""

    main._fill_quote_names = no_names
    main._read_fresh_raw_history_async = fresh_history
    clean = dict(suspended=False, limit_up=False, limit_down=False, st=False)
    quotes = [
        _quote("600001", closes["600001"], 9e8, **clean),
        _quote("600002", closes["600002"], 8e8, **clean),
        # Price-eligible but its risk tuple is not established.
        _quote("600003", closes["600003"], 7e8),
        # An explicit limit-up block never appears anywhere.
        _quote("600004", closes["600004"], 6e8, suspended=False, limit_up=True, limit_down=False, st=False),
    ]
    return main, quotes


def test_normal_close_screen_keeps_full_coverage_and_formal_list(tmp_path, monkeypatch):
    as_of = datetime.now(core.CHINA_TZ).date().isoformat()
    monkeypatch.setattr(policy, "accepted_negative", lambda source, version, field: field in policy.RISK_FIELDS)
    monkeypatch.setattr(main_module, "safe_factor_row", lambda row, code, as_of, known_at: dict(row))
    main, quotes = _raw_main(tmp_path, as_of)

    result = asyncio.run(main._score_quotes_result(quotes, 10, as_of, context="daily_close", requested_date=as_of))

    diagnostics = result.diagnostics
    assert diagnostics["risk_confirmed"] == 2
    assert diagnostics["indicator_targets"] == 2 and diagnostics["indicator_raw_batch"] == 2
    assert diagnostics["indicator_coverage"] == 1.0
    assert diagnostics["observation_universe_targets"] == 3
    assert diagnostics["observation_universe_coverage"] == 1.0
    assert diagnostics["indicator_source_counts"] == {"tushare": 3}
    assert diagnostics["indicator_price_basis_counts"] == {"unadjusted": 3}
    formal = {item.quote.code for item in result.candidates}
    assert formal and formal <= {"600001", "600002"}
    assert all(item.risk_level not in {"blocked", "unknown"} for item in result.candidates)
    assert "600004" not in {item.quote.code for item in result.observations}


def test_unverified_risk_yields_empty_formal_list_and_labelled_watch_list(tmp_path, monkeypatch):
    as_of = datetime.now(core.CHINA_TZ).date().isoformat()
    monkeypatch.setattr(policy, "accepted_negative", lambda source, version, field: False)
    main, quotes = _raw_main(tmp_path, as_of)

    result = asyncio.run(main._score_quotes_result(quotes, 10, as_of, include_factors=False,
                                                   context="daily_close", requested_date=as_of))

    diagnostics = result.diagnostics
    assert result.candidates == ()
    assert diagnostics["risk_confirmed"] == 0 and diagnostics["indicator_targets"] == 0
    assert diagnostics["indicator_targets_risk_unknown"] == 3
    assert diagnostics["observation_universe_enriched"] == 3
    assert diagnostics["observation_universe_coverage"] == 1.0
    codes = [item.quote.code for item in result.observations]
    assert sorted(codes) == ["600001", "600002", "600003"]
    assert all(item.risk_level == "unknown" for item in result.observations)

    reason, missing = main._degraded_watch_details(
        {**diagnostics, "screen_min_indicator_coverage": 0.8}, {"complete": True, "quality": "good"},
        ["risk_evidence_missing"])
    assert reason == "risk_evidence_missing"
    assert any("风险字段已核验 0/4" in text for text in missing)
    text = main._degraded_watch_text(as_of, missing, [
        {"code": item.quote.code, "name": item.quote.name, "close": item.quote.price, "score": item.score,
         "history_days": item.quote.history_days, "risk_level": item.risk_level} for item in result.observations])
    assert "数据不完整" in text and "未验证" in text
    assert "推荐" not in text and "建议" not in text and "买入价" not in text


def test_automatic_close_failure_persists_diagnostics_and_separate_watch_list(tmp_path):
    Main138, ScreenScoreResult, core138, Store138 = _imports138()
    store = Store138(tmp_path / "automatic-degraded.sqlite3")
    main, _reads = _automatic_reconciliation_main(Main138, ScreenScoreResult, core138, store)
    main.config.update({"automatic_close_max_attempts": 1})
    with store._connect() as db:
        db.execute("UPDATE job_runs SET automatic_attempts=1 WHERE job_key=?", ("automatic_close:2026-09-09",))
    store.set_subscription("qq:group:1", True)
    sent = []

    async def send_message(origin, chain):
        sent.append((origin, "".join(chain) if isinstance(chain, list) else str(chain)))

    main.context = types.SimpleNamespace(send_message=send_message)
    fixture_score = main._score_quotes_result
    watch = _candidate(core138, "600000")
    watch.risk_level = "unknown"

    async def score(quotes, limit, as_of="", **kwargs):
        scored = await fixture_score(quotes, limit, as_of, **kwargs)
        diagnostics = {**scored.diagnostics, "risk_tuple_complete": 0, "tradable": 0, "indicator_targets": 0,
                       "indicator_raw_batch": 0, "indicator_coverage": 0.0, "enriched": 0, "risk_confirmed": 0,
                       "observation_universe_targets": 1, "observation_universe_enriched": 1,
                       "observation_universe_coverage": 1.0, "indicator_source_counts": {"tushare": 1}}
        return ScreenScoreResult.build([], diagnostics, [watch])

    main._score_quotes_result = score

    result = asyncio.run(main._run_automatic_close_job("2026-09-09", "automatic_close:2026-09-09"))

    assert result["state"] == "fail_closed" and "risk_evidence_missing" in result["reasons"]
    with store._connect() as db:
        gate = db.execute("SELECT phase,diagnostics_json FROM screen_gate_diagnostics").fetchone()
        job = db.execute("SELECT status FROM job_runs WHERE job_key=?", ("automatic_close:2026-09-09",)).fetchone()
        watch_rows = db.execute("SELECT trade_date,reason,items_json FROM degraded_watch_lists").fetchall()
        formal = db.execute("SELECT COUNT(*) FROM screen_candidates").fetchone()[0]
        tracked = db.execute("SELECT COUNT(*) FROM recommendation_records").fetchone()[0]
    assert job["status"] == "missed"
    assert gate["phase"].startswith("fail_closed:") and "risk_evidence_missing" in gate["phase"]
    recorded = json.loads(gate["diagnostics_json"])
    for key in ("indicator_targets", "enriched", "indicator_failed", "indicator_coverage", "degraded_reason",
                "indicator_source_counts", "observation_universe_coverage"):
        assert key in recorded
    assert recorded["degraded_reason"] == "risk_evidence_missing"
    assert len(watch_rows) == 1 and watch_rows[0]["trade_date"] == "2026-09-09"
    assert [item["code"] for item in json.loads(watch_rows[0]["items_json"])] == ["600000"]
    assert formal == 0 and tracked == 0
    assert len(sent) == 1 and sent[0][0] == "qq:group:1"
    assert "降级观察名单" in sent[0][1] and "推荐" not in sent[0][1]
    latest = store.latest_degraded_watch_list()
    assert latest["items"][0]["code"] == "600000" and latest["missing"]

    # A later terminal save for the same trade date never pushes twice.
    again = asyncio.run(main._save_and_push_degraded_watch(
        "automatic_close:2026-09-09", "2026-09-09", "risk_evidence_missing", ["x"], {}, [watch], terminal=True))
    assert again["pushed"] == 0 and len(sent) == 1
