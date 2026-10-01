from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import MappingProxyType, SimpleNamespace

import pytest

from test_v0139_intraday_m2 import _imports, _main


PREVIOUS, CURRENT = "2026-09-10", "2026-09-11"
OBSERVED = "2026-09-12T01:00:00+00:00"


class Gateway:
    def __init__(self, defect=""):
        self.defect, self.calls = defect, []

    async def request_json(self, client, payload, **kwargs):
        from astrbot_stock_watch.providers import TusharePermissionError

        api, params = payload["api_name"], payload["params"]
        self.calls.append((api, dict(params)))
        if self.defect == "permission" and api == "index_daily":
            raise TusharePermissionError("fixture permission denied")
        fields = payload["fields"].split(",")
        day = params.get("trade_date", params.get("start_date"))
        current = day == "20260911"
        if params.get("offset", 0):
            rows = []
        elif api == "trade_cal":
            rows = [{"exchange": "SSE", "cal_date": day, "is_open": 1,
                     "pretrade_date": "20260909" if not current else "20260910"}]
        elif api == "index_daily":
            rows = [{"ts_code": "000300.SH" if self.defect != "wrong_index" else "000001.SH",
                     "trade_date": day if self.defect != "wrong_date" else "20260909",
                     "close": 102 if current else 100, "pre_close": 100}]
        elif api == "daily":
            rows = []
            for code, before, after, amount in (("600000.SH", 10, 11, 1000), ("000001.SZ", 20, 18, 2000)):
                close = after if current else before
                rows.append({"ts_code": code, "trade_date": day, "open": close, "high": close,
                             "low": close, "close": close, "pre_close": before,
                             "pct_chg": (close / before - 1) * 100, "vol": 100,
                             "amount": amount * (1.5 if current else 1)})
            if self.defect == "missing_stock" and current:
                rows.pop()
        elif api == "adj_factor":
            rows = [{"ts_code": code, "trade_date": day, "adj_factor": 2 if self.defect == "factor_change" and current else 1}
                    for code in ("600000.SH", "000001.SZ")]
        else:
            pytest.fail("unexpected endpoint: " + api)
        return {"code": 0, "data": {"fields": fields, "items": [[r[f] for f in fields] for r in rows]}}


def _setup(tmp_path, monkeypatch, defect=""):
    Main, core, Store = _imports()
    from astrbot_stock_watch import market_comparison as comparison
    from astrbot_stock_watch.providers import TushareBulkDailyProvider

    clock = [OBSERVED]
    monkeypatch.setattr(comparison, "utc_now", lambda: clock[0])
    store = Store(tmp_path / "comparison.sqlite3")
    gateway = Gateway(defect)
    provider = TushareBulkDailyProvider("fixture", gateway=gateway, min_snapshot_size=2)
    packet = asyncio.run(provider.fetch_market_comparison(
        CURRENT, PREVIOUS, "000300.SH", ["600000", "000001"], "raw:fixture", client=object()))
    return Main, core, store, comparison, clock, provider, gateway, packet


def test_provider_storage_report_exact_index_units_and_point_in_time(tmp_path, monkeypatch):
    _, _, store, comparison, clock, _, gateway, packet = _setup(tmp_path, monkeypatch)
    assert packet["status"] == "available"
    assert packet["stock_days"][CURRENT][0]["amount"] == 1_500_000
    assert all(params["ts_code"] == "000300.SH" for api, params in gateway.calls if api == "index_daily")
    ident = store.save_market_comparison(packet)
    assert store.save_market_comparison(packet) == ident
    query = (CURRENT, "000300.SH", "raw:fixture")
    assert store.market_comparison_report(*query, as_of="2026-09-11T23:59:00+00:00")["status"] == "unknown"
    good = store.market_comparison_report(*query, as_of=OBSERVED)
    assert good["turnover_change_pct"] == pytest.approx(50)
    assert good["market_return_pct"] == pytest.approx(0)
    assert good["index_return_pct"] == pytest.approx(2)
    assert good["divergence_pct"] == pytest.approx(-2)
    assert "000300.SH" in comparison.render(good) and "个百分点" in comparison.render(good)
    before = store.path.read_bytes()
    completed = subprocess.run(
        [sys.executable, str(Path(comparison.__file__)), "--database", str(store.path),
         "--trade-date", CURRENT, "--benchmark-code", "000300.SH", "--universe-ref", "raw:fixture", "--as-of", OBSERVED],
        check=True, capture_output=True, text=True, timeout=10)
    cli = json.loads(completed.stdout)
    assert cli["read_only"] is True and cli["divergence_pct"] == pytest.approx(-2)
    assert store.path.read_bytes() == before
    clock[0] = "2026-09-12T01:01:00+00:00"
    failed = {**packet, "status": "unknown", "reason": "permission_denied", "available_at": clock[0]}
    store.save_market_comparison(failed)
    assert store.market_comparison_report(*query, as_of=clock[0])["reason"] == "permission_denied"
    assert store.market_comparison_report(*query, as_of=OBSERVED)["status"] == "available"
    assert store.market_comparison_report(CURRENT, "000001.SH", "raw:fixture", as_of=clock[0])["status"] == "unknown"


@pytest.mark.parametrize("defect", ["permission", "wrong_index", "wrong_date", "missing_stock", "factor_change"])
def test_key_provider_failures_persist_unknown_without_alternate_index(tmp_path, monkeypatch, defect):
    _, _, store, _, _, _, gateway, packet = _setup(tmp_path, monkeypatch, defect)
    assert packet["status"] == "unknown"
    if defect == "permission":
        assert packet["reason"] == "permission_denied"
    store.save_market_comparison(packet)
    result = store.market_comparison_report(CURRENT, "000300.SH", "raw:fixture", as_of=OBSERVED)
    assert result["status"] == "unknown"
    assert result["turnover_change_pct"] is result["divergence_pct"] is None
    assert all(params["ts_code"] == "000300.SH" for api, params in gateway.calls if api == "index_daily")
    assert not any(api not in {"trade_cal", "daily", "adj_factor", "index_daily"} for api, _ in gateway.calls)


def test_units_calendar_and_immutable_payload_fail_closed(tmp_path, monkeypatch):
    _, _, store, comparison, _, _, _, packet = _setup(tmp_path, monkeypatch)
    changed = deepcopy(packet)
    changed["amount_unit"] = "unknown"
    assert comparison.evaluate(changed, as_of=OBSERVED)["reason"] == "source_or_unit_unknown"
    changed = deepcopy(packet)
    changed["calendar"][1]["pretrade_date"] = "2026-09-09"
    assert comparison.evaluate(changed, as_of=OBSERVED)["reason"] == "not_adjacent_sessions"
    store.save_market_comparison(packet)
    with store._connect() as db:
        db.execute("UPDATE market_comparison_observations SET payload_json='{}'")
    assert store.market_comparison_report(CURRENT, "000300.SH", "raw:fixture", as_of=OBSERVED)["reason"] == "evidence_integrity_failed"


def test_collection_and_read_only_report_integration_default_off(tmp_path, monkeypatch):
    Main, core, store, _, _, provider, _, packet = _setup(tmp_path, monkeypatch)
    main = _main(Main, store, market_comparison_benchmark="000300.SH")
    called = []
    async def helper(*args):
        called.append(args)
        return packet
    provider.fetch_market_comparison = helper
    main.quotes = SimpleNamespace(bulk_provider=provider)
    result = {"quotes": [core.Quote("600000", "a", 11), core.Quote("000001", "b", 18)],
              "bars": {"600000": [{"trade_date": PREVIOUS}, {"trade_date": CURRENT}]}}
    assert asyncio.run(main._collect_market_comparison(result, CURRENT, "raw:fixture"))["reason"] == "disabled"
    assert called == []
    main.config["market_comparison_enabled"] = True
    assert asyncio.run(main._collect_market_comparison(result, CURRENT, "raw:fixture"))["status"] == "available"
    assert called == [(CURRENT, PREVIOUS, "000300.SH", ["600000", "000001"], "raw:fixture")]
    main._validate_report_diagnostics = lambda values, **kwargs: MappingProxyType({**values, "market_stats_confirmed": True})
    diagnostics = asyncio.run(main._report_diagnostics_for_send({"raw_batch_id": "raw:fixture"},
        requested_date=CURRENT, actual_date=CURRENT, quote_count=2))
    text = "\n".join(main._market_report_lines(CURRENT, CURRENT, result["quotes"], [],
                                            {"quality": "good", "complete": True}, diagnostics))
    assert "成交额变化 +50.00%" in text and "相对指数差 -2.00" in text
    assert len(called) == 1


def test_future_intraday_review_predicate_requires_natural_identity_plan_and_freshness():
    from audit_runtime_evidence import intraday_review_check

    plan = '{"validated":true}'
    version = "run:" + hashlib.sha256(plan.encode()).hexdigest()[:16]
    row = {
        "origin": "fixture", "code": "600000", "signal": "attention_entry", "plan_version": version,
        "event_sequence": 1, "run_id": "run", "risk_event": False, "state": "sent",
        "created_at": "2026-09-11T01:31:00", "sent_at": "2026-09-11T01:31:01",
        "quote_fetched_at": "2026-09-11T09:30:59+08:00", "market_snapshot_at": "2026-09-11T09:30:58+08:00",
    }
    row["event_key"] = "intraday:" + hashlib.sha256("\0".join(["fixture", "600000", "attention_entry", version, "1"]).encode()).hexdigest()
    kwargs = {"fsm_linked": True, "candidate_plan": plan, "quote_max_age_seconds": 60, "market_max_age_seconds": 60}
    assert intraday_review_check(row, **kwargs)["ready_for_review"]
    assert not intraday_review_check({**row, "risk_event": True}, **kwargs)["ready_for_review"]
    assert not intraday_review_check({**row, "quote_fetched_at": "2026-09-11T09:20:00+08:00"}, **kwargs)["ready_for_review"]
    assert not intraday_review_check(row, fsm_linked=True, candidate_plan=plan)["ready_for_review"]


def test_mature_window_review_requires_actual_factor_rows_and_keeps_other_windows_pending(tmp_path):
    from audit_runtime_evidence import audit_database
    from test_v0140_recommendation_tracking import _record, _factor_row

    _, _, Store = _imports()
    path = tmp_path / "maturity.sqlite3"
    store = Store(path)
    _record(store, "fixture")
    with store._connect() as db:
        db.execute("UPDATE recommendation_records SET recommended_date='2026-09-09',created_at='2026-09-09T08:00:00'")
        store._upsert_recommendation_outcome(
            db, "fixture", 1, "2026-09-10", "unknown_order", return_pct=0,
            max_gain_pct=2, max_drawdown_pct=-2, session_complete=True, price_basis="unadjusted")
        db.execute("UPDATE recommendation_outcomes SET updated_at='2026-09-10T08:00:00'")
    for day, opened in (("2026-09-10", True), ("2026-09-11", True), ("2026-09-12", False)):
        store.save_calendar(day, opened, "fixture", ttl_seconds=999999)
    before = audit_database(path, as_of="2026-09-12")["recommendations"]
    assert not before["groups"][0]["horizons"]["1"]["ready_for_review"]
    base = _factor_row(store, "600000", "2026-09-09")
    bar_factor = _factor_row(store, "600000", "2026-09-10")
    with store._connect() as db:
        db.execute("UPDATE recommendation_records SET corporate_action_evidence=?,corporate_action_observed_at=?,corporate_action_response_sha256=? WHERE recommendation_id='fixture'",
                   (base["corporate_action_evidence"], base["corporate_action_observed_at"], base["corporate_action_response_sha256"]))
    store.save_daily_bars("600000", [{
        "trade_date": "2026-09-10", "open": 10, "close": 10, "high": 10.2, "low": 9.8,
        "volume": 100, "amount": 1000, **bar_factor,
    }], "tushare", "unadjusted")
    with store._connect() as db:
        db.execute("UPDATE daily_bars SET fetched_at='2026-09-10T07:00:00'")
    after = audit_database(path, as_of="2026-09-12")["recommendations"]
    horizons = after["groups"][0]["horizons"]
    assert horizons["1"]["ready_for_review"] and horizons["1"]["stored_status_counts"] == {"unknown_order": 1}
    assert horizons["3"]["maturity"] == "pending" and after["ready_for_review"] is False
    with store._connect() as db:
        db.execute("UPDATE corporate_action_factors SET observed_at='2026-09-10T06:00:00+00:00',fetched_at='2026-09-09T00:00:00' WHERE trade_date='2026-09-09'")
        db.execute("UPDATE recommendation_records SET corporate_action_observed_at='2026-09-10T06:00:00+00:00' WHERE recommendation_id='fixture'")
    tampered = audit_database(path, as_of="2026-09-12")["recommendations"]
    assert not tampered["groups"][0]["horizons"]["1"]["ready_for_review"]
