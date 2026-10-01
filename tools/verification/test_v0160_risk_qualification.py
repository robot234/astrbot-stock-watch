"""Point-in-time risk evidence and legitimate empty-screen regressions."""
from __future__ import annotations

import asyncio
import importlib
import json
import types
from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from test_v0139_intraday_m2 import _imports, _main
from test_v0138_automatic_close import _candidate, _run_args
from paper_review import review_persisted
from webapp.data import Dashboard

Main, core, StockStore = _imports()
risk = importlib.import_module("astrbot_stock_watch.risk_qualification")
storage_module = importlib.import_module("astrbot_stock_watch.storage")
forward = importlib.import_module("astrbot_stock_watch.paper_forward")
policy = importlib.import_module("astrbot_stock_watch.formal_source_policy")


@pytest.fixture
def synthetic_formal_source_license(monkeypatch):
    """Explicitly authorize synthetic negatives for these local flow tests only."""
    monkeypatch.setattr(policy, "accepted_negative", lambda source, version, field: field in policy.RISK_FIELDS)


pytestmark = pytest.mark.usefixtures("synthetic_formal_source_license")


def _observe(store, *, bar_start, quote_at, now, **kwargs):
    if bar_start is not None:
        end = forward.aware(bar_start) + __import__("datetime").timedelta(minutes=1)
        bar_evidence = forward.synthetic_bar_evidence(
            bar_start=bar_start, bar_close=kwargs["bar_close"], received_at=end)
        execution_evidence = forward.synthetic_execution_evidence(
            record_id=kwargs["record_id"], quote_at=quote_at,
            quote_price=kwargs["quote_price"], received_at=now)
    else:
        bar_evidence = execution_evidence = None
    return store.observe_paper_completed_bar(
        bar_start=bar_start, quote_at=quote_at, now=now,
        bar_evidence=bar_evidence, execution_evidence=execution_evidence, **kwargs)


def _seed_forward_action_window(store, through):
    all_days = ("2026-09-25", "2026-09-26", "2026-09-27", "2026-09-28", "2026-09-29", "2026-09-30")
    with store._connect() as db:
        for day in all_days:
            if day > through:
                break
            open_day = day not in {"2026-09-26", "2026-09-27"}
            db.execute("INSERT OR IGNORE INTO trading_calendar(trade_date,is_open,status,source,fetched_at) VALUES(?,?,?,?,?)",
                       (day, int(open_day), "open" if open_day else "closed", "synthetic",
                        "2026-09-24T10:00:00+00:00"))
    for day in all_days:
        if day > through:
            break
        if day not in {"2026-09-26", "2026-09-27"}:
            store.record_paper_action_evidence("600000", day, "none",
                observed_at=day + "T15:30:00+08:00", received_at=day + "T15:31:00+08:00")


def _store(tmp_path):
    store = StockStore(tmp_path / "risk.sqlite3")
    store.save_daily_quotes("2026-09-24", [core.Quote("600000", "Synthetic", 10.0)])
    with store._connect() as db:
        db.execute("INSERT INTO datasets(dataset_id,dataset_key,provider,basis,created_at) VALUES(?,?,?,?,?)",
                   ("dataset-1", "tushare_daily", "tushare", "unadjusted", "2026-09-24T10:00:00"))
        for batch in ("batch-a", "batch-b"):
            db.execute("INSERT INTO batches(batch_id,dataset_id,requested_date,actual_trade_date,status,manifest_hash,created_at) VALUES(?,?,?,?,?,?,?)",
                       (batch, "dataset-1", "2026-09-24", "2026-09-24", "published", "fixture", "2026-09-24T10:00:00"))
        db.execute("INSERT INTO trading_calendar(trade_date,is_open,status,source,fetched_at) VALUES(?,?,?,?,?)",
                   ("2026-09-25", 1, "open", "synthetic", "2026-09-24T10:00:00+00:00"))
    return store


def _observation(field="limit_up", value=False, *, source="eastmoney:companion", observed="2026-09-24T10:30:00+00:00"):
    return {"trade_date": "2026-09-24", "batch_id": "batch-a", "code": "600000",
            "source": source, "first_observed_at": observed,
            "source_timestamp": "2026-09-24T07:00:00+00:00",
            "reference_close": 10.0, "fields": {field: value}}


def _clock_at(value):
    fixed = datetime.fromisoformat(value)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)

    return FixedDatetime


def test_risk_evidence_is_dated_batch_bound_and_conflict_closed(tmp_path, monkeypatch):
    store = _store(tmp_path)
    quote = core.Quote("600000", "Synthetic", 10.0)
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "datetime", _clock_at("2026-09-24T10:31:00+00:00"))
        store.record_daily_risk_observations({"trade_date": "2026-09-24", "batch_id": "batch-a",
                                              "observations": [_observation()]})
    with store._connect() as db:
        # Rows written by the previous UTC-naive writer remain readable as UTC.
        db.execute("UPDATE daily_risk_observations SET recorded_at=? WHERE value=0",
                   ("2026-09-24T10:31:00",))
    early = store.daily_risk_evidence_for_quotes("2026-09-24", "batch-a", [quote],
                                                  "2026-09-24T10:00:00+00:00")
    late = store.daily_risk_evidence_for_quotes("2026-09-24", "batch-a", [quote],
                                                 "2026-09-24T11:00:00+00:00")
    assert early == {}
    assert store.daily_risk_evidence_for_quotes("2026-09-24", "batch-a", [quote],
                                                "2026-09-24T10:30:59+00:00") == {}
    assert late["600000"] == {"suspended": None, "limit_up": False, "limit_down": None, "st": None}
    assert store.daily_risk_evidence_for_quotes("2026-09-24", "batch-b", [quote],
                                                "2026-09-24T11:00:00+00:00") == {}
    assert store.daily_risk_evidence_for_quotes("2026-09-24", "batch-a",
                                                [core.Quote("600000", "Synthetic", 10.02)],
                                                "2026-09-24T11:00:00+00:00") == {}
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "datetime", _clock_at("2026-09-24T10:46:00+00:00"))
        store.record_daily_risk_observations({"trade_date": "2026-09-24", "batch_id": "batch-a",
                                              "observations": [_observation(value=True, observed="2026-09-24T10:45:00+00:00")]})
    conflicting = store.daily_risk_evidence_for_quotes("2026-09-24", "batch-a", [quote],
                                                         "2026-09-24T11:00:00+00:00")
    assert conflicting["600000"]["limit_up"] is None


def test_historical_sidecar_imported_today_cannot_rewrite_old_qualification(tmp_path, monkeypatch):
    store = _store(tmp_path)
    snapshot = {"trade_date": "2026-09-24", "batch_id": "batch-a", "source": "tushare",
                "basis": "unadjusted", "published_at": "2026-09-24T08:30:00+00:00"}
    primary = [{"code": "600000", "name": "Synthetic", "score": 40, "close": 10.0,
                "amount": 1e8, "risk_level": "unknown", "risk_flags": [], "reasons": []}]
    with monkeypatch.context() as frozen_clock:
        frozen_clock.setattr(storage_module, "_utcnow_naive", lambda: datetime(2026, 9, 24, 9, 30))
        store.save_research_pools(snapshot, primary, [], {})
    before = store.qualify_research_pick("research:batch-a", "600000", "2026-09-24T11:00:00+00:00")
    observations = [_observation("suspended", False, source="baostock:daily:unadjusted"),
                    _observation("st", False, source="baostock:daily:unadjusted"),
                    _observation("limit_up", False), _observation("limit_down", False)]
    store.record_daily_risk_observations({"trade_date": "2026-09-24", "batch_id": "batch-a",
                                          "observations": observations})
    quote = core.Quote("600000", "Synthetic", 10.0)
    assert store.daily_risk_evidence_for_quotes("2026-09-24", "batch-a", [quote],
                                                "2026-09-24T11:00:00+00:00") == {}
    historical = store.qualify_research_pick("research:batch-a", "600000", "2026-09-24T11:00:00+00:00")
    assert historical["state"] == "unknown" and historical["version"] == before["version"]
    later = store.qualify_research_pick("research:batch-a", "600000", datetime.now(timezone.utc))
    assert later["state"] == "eligible" and later["version"] != before["version"]


def test_sidecar_pool_absence_cannot_prove_negative_limit():
    sidecar = {"version": 1, "trade_date": "2026-09-24", "batch_id": "batch-a",
               "sources": {"trading": "baostock:daily:unadjusted", "limit": "akshare:dated-pools"},
               "failed_codes": [], "captured_at": "2026-09-24T11:00:00+00:00",
               "source_captured_at": {key: "2026-09-24T10:30:00+00:00"
                                      for key in ("trading", "limit_up", "limit_down")},
               "rows": {"600000": {"close": 10.0, "suspended": False,
                                     "limit_up": None, "limit_down": None}}}
    bundle = risk.observations_from_research_sidecar(sidecar)
    assert len(bundle["observations"]) == 1
    assert bundle["observations"][0]["fields"] == {"suspended": False, "st": None}
    sidecar["rows"]["600000"]["limit_up"] = False
    with pytest.raises(ValueError, match="negative_limit_unverified"):
        risk.observations_from_research_sidecar(sidecar)


def test_legitimate_empty_requires_complete_risk_and_zero_tradable():
    snapshot = {"complete": True, "quality": "good"}
    base = {"input": 2, "risk_tuple_complete": 2, "tradable": 0,
            "indicator_targets": 0, "candidate_count": 0, "valid_empty": True,
            "indicator_coverage": 0.0, "screen_min_indicator_coverage": 0.8,
            "market_stats_confirmed": True}
    assert Main._automatic_report_failures(base, snapshot) == []
    assert "risk_evidence_missing" in Main._automatic_report_failures(
        {**base, "risk_tuple_complete": 1}, snapshot)
    assert "indicator_coverage" in Main._automatic_report_failures(
        {**base, "tradable": 1, "indicator_targets": 1, "valid_empty": False}, snapshot)


def test_legitimate_empty_clears_old_active_candidate_pointer(tmp_path):
    store = StockStore(tmp_path / "screen.sqlite3")
    store.save_screen_bundle_atomic(_run_args("earlier"), [_candidate(core, "600000")],
                                    diagnostics={"indicator_coverage": 1.0}, coverage=1.0,
                                    report_key="earlier", valid_until="2026-10-01T00:00:00+08:00")
    assert store.latest_screen_candidates()
    empty = {"input": 5000, "risk_tuple_complete": 5000, "tradable": 0,
             "indicator_targets": 0, "candidate_count": 0, "valid_empty": True,
             "indicator_coverage": 0.0}
    args = list(_run_args("empty"))
    args[8] = 0
    store.save_screen_bundle_atomic(tuple(args), [], diagnostics=empty, coverage=0.0,
                                    report_key="later")
    assert store.active_candidate_runs() == []
    assert store.latest_screen_candidates() == []


def test_freeze_to_later_qualification_two_completed_bars_and_restart_fill(tmp_path, monkeypatch):
    store = _store(tmp_path)
    snapshot = {"trade_date": "2026-09-24", "batch_id": "batch-a", "source": "tushare",
                "basis": "unadjusted", "published_at": "2026-09-24T08:30:00+00:00"}
    primary = [{"code": "600000", "name": "Synthetic", "score": 40, "close": 10.0,
                "amount": 1e8, "risk_level": "unknown", "risk_flags": [], "reasons": []}]
    with monkeypatch.context() as frozen_clock:
        frozen_clock.setattr(storage_module, "_utcnow_naive", lambda: datetime(2026, 9, 24, 9, 30))
        frozen = store.save_research_pools(snapshot, primary, [], {})
    assert frozen["run_id"] == "research:batch-a"
    unknown = store.qualify_research_pick("research:batch-a", "600000", "2026-09-24T10:00:00+00:00")
    assert unknown["state"] == "unknown"
    observations = [
        _observation("suspended", False, source="baostock:daily:unadjusted"),
        _observation("st", False, source="baostock:daily:unadjusted"),
        _observation("limit_up", False), _observation("limit_down", False),
    ]
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "datetime", _clock_at("2026-09-24T10:46:00+00:00"))
        store.record_daily_risk_observations({"trade_date": "2026-09-24", "batch_id": "batch-a",
                                              "observations": observations})
    before_recorded = store.qualify_research_pick("research:batch-a", "600000", "2026-09-24T10:40:00+00:00")
    assert before_recorded["state"] == "unknown" and before_recorded["version"] == unknown["version"]
    eligible = store.qualify_research_pick("research:batch-a", "600000", "2026-09-24T11:00:00+00:00")
    assert eligible["state"] == "eligible" and eligible["version"] != unknown["version"]
    common = {"record_id": eligible["record_id"], "version": eligible["version"],
              "bar_close": 10.08, "quote_price": 10.06,
              "suspended": False, "limit_up": False, "limit_down": False,
              "st": False,
              "source_quality": "validated_minute"}
    unverified = _observe(store,
        **{**common, "source_quality": "aggregated_quotes_unverified"},
        bar_start="2026-09-25T09:35:00+08:00",
        quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    assert unverified["state"] == "unknown"
    first = _observe(store,
        **common, bar_start="2026-09-25T09:35:00+08:00",
        quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    assert first["state"] == "pending" and first["consecutive_count"] == 1
    replay = _observe(store,
        **common, bar_start="2026-09-25T09:35:00+08:00",
        quote_at="2026-09-25T09:36:40+08:00", now="2026-09-25T09:36:41+08:00")
    assert replay["reason"] == "duplicate_or_out_of_order_observation"
    restarted = StockStore(tmp_path / "risk.sqlite3")
    fill = _observe(restarted,
        **common, bar_start="2026-09-25T09:36:00+08:00",
        quote_at="2026-09-25T09:37:10+08:00", now="2026-09-25T09:37:11+08:00")
    assert fill["fill_status"] == "simulated_fill" and fill["entry_date"] == "2026-09-25"
    assert fill["entry_price"] == 10.0701 and fill["round_trip_fee_pct"] == 0.30
    assert fill["entry_slippage_pct"] == fill["exit_slippage_pct"] == 0.10
    assert _observe(restarted,
        **common, bar_start="2026-09-25T09:36:00+08:00",
        quote_at="2026-09-25T09:37:10+08:00", now="2026-09-25T09:37:11+08:00")["entry_price"] == fill["entry_price"]
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_simulated_entries").fetchone()[0] == 1
        for day in ("2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30"):
            db.execute("INSERT INTO corporate_action_factors(code,trade_date,adj_factor,source,evidence,fetched_at,observed_at,response_sha256) VALUES(?,?,?,?,?,?,?,?)",
                       ("600000", day, 1.0, "tushare_adj_factor", "synthetic factor", day + "T16:00:00+08:00",
                        day + "T16:00:00+08:00", "fixture-digest"))
        for day, close in (("2026-09-28", 10.3), ("2026-09-29", 10.2), ("2026-09-30", 10.5)):
            db.execute("INSERT INTO daily_bars(code,trade_date,open,high,low,close,source,fetched_at,price_basis,corporate_action_factor,corporate_action_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                       ("600000", day, close, close, close, close, "fixture", day + "T16:00:00+08:00",
                        "unadjusted", 1.0, "synthetic factor"))
    _seed_forward_action_window(store, "2026-09-30")
    dashboard = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 30, 10, tzinfo=timezone.utc))
    webpage = dashboard.query("candidates")["data"]["research"]["primary"][0]
    assert webpage["record_id"] == eligible["record_id"]
    assert webpage["fill_status"] == "simulated_fill" and webpage["simulated_entry_price"] == 10.0701
    assert webpage["entry_range"]["B_limit"] == webpage["simulated_entry_price"]
    assert webpage["accounting_version"] == forward.ACCOUNTING_VERSION
    assert webpage["valuation"]["1"]["status"] == "complete"
    assert webpage["valuation"]["3"]["status"] == "complete"
    assert webpage["valuation"]["5"]["status"] == "pending"
    reviewed = review_persisted(tmp_path / "risk.sqlite3",
                                ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30"],
                                as_of="2026-09-30")
    assert reviewed["records"][0]["record_id"] == webpage["record_id"]
    assert reviewed["records"][0]["marks"][1]["status"] == "complete"
    mark = reviewed["records"][0]["marks"][1]
    assert mark["return_kind"] == "mark_to_close" and mark["net_return_pct"] is None
    assert mark["return_pct"] == float(forward.close_mark(10.3, quantity=900, total_cost_cny="9068.18"))
    assert reviewed["records"][0]["marks"][3]["status"] == "complete"
    assert reviewed["records"][0]["marks"][5]["status"] == "pending"
    with store._connect() as db:
        db.execute("DELETE FROM paper_no_action_evidence WHERE code='600000' AND trade_date='2026-09-29'")
    gap = review_persisted(tmp_path / "risk.sqlite3",
                           ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30"],
                           as_of="2026-09-30")["records"][0]
    assert gap["marks"][1]["status"] == "complete"
    assert gap["marks"][3]["status"] == "unknown" and gap["marks"][3]["return_pct"] is None


def _eligible_paper_pick(tmp_path, monkeypatch, *, extra_primary=()):
    store = _store(tmp_path)
    snapshot = {"trade_date": "2026-09-24", "batch_id": "batch-a", "source": "tushare",
                "basis": "unadjusted", "published_at": "2026-09-24T08:30:00+00:00"}
    primary = [{"code": "600000", "name": "Synthetic", "score": 40, "close": 10.0,
                "amount": 1e8, "risk_level": "unknown", "risk_flags": [], "reasons": []}]
    primary.extend(extra_primary)
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "_utcnow_naive", lambda: datetime(2026, 9, 24, 9, 30))
        store.save_research_pools(snapshot, primary, [], {})
    observations = [_observation("suspended", False, source="baostock:daily:unadjusted"),
                    _observation("st", False, source="baostock:daily:unadjusted"),
                    _observation("limit_up", False), _observation("limit_down", False)]
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "datetime", _clock_at("2026-09-24T10:46:00+00:00"))
        store.record_daily_risk_observations({"trade_date": "2026-09-24", "batch_id": "batch-a",
                                              "observations": observations})
    qualified = store.qualify_research_pick("research:batch-a", "600000", "2026-09-24T11:00:00+00:00")
    assert qualified["state"] == "eligible"
    common = {"record_id": qualified["record_id"], "version": qualified["version"],
              "bar_close": 10.08, "quote_price": 10.06, "suspended": False,
              "limit_up": False, "limit_down": False, "st": False,
              "source_quality": "validated_minute"}
    return store, qualified, common


def test_entry_survives_qualification_updates_and_review_as_of(tmp_path, monkeypatch):
    store, qualified, common = _eligible_paper_pick(tmp_path, monkeypatch)
    first = _observe(store,
        **common, bar_start="2026-09-25T09:35:00+08:00",
        quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    assert first["state"] == "pending"
    fill = _observe(store,
        **common, bar_start="2026-09-25T09:36:00+08:00",
        quote_at="2026-09-25T09:37:10+08:00", now="2026-09-25T09:37:11+08:00")
    assert fill["fill_status"] == "simulated_fill"
    pre_confirmation = Dashboard(tmp_path / "risk.sqlite3",
        now=lambda: datetime(2026, 9, 25, 1, 36, 30, tzinfo=timezone.utc))
    assert pre_confirmation.query("candidates")["data"]["research"]["primary"][0].get("fill_status") is None
    with store._connect() as db:
        for day in ("2026-09-25", "2026-09-28"):
            db.execute("INSERT INTO corporate_action_factors(code,trade_date,adj_factor,source,evidence,fetched_at,observed_at,response_sha256) VALUES(?,?,?,?,?,?,?,?)",
                       ("600000", day, 1.0, "tushare_adj_factor", "synthetic factor",
                        day + "T16:00:00+08:00", day + "T16:00:00+08:00", "fixture-digest"))
        db.execute("INSERT INTO daily_bars(code,trade_date,open,high,low,close,source,fetched_at,price_basis,corporate_action_factor,corporate_action_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   ("600000", "2026-09-28", 10.3, 10.3, 10.3, 10.3, "fixture",
                    "2026-09-28T16:00:00+08:00", "unadjusted", 1.0, "synthetic factor"))
    _seed_forward_action_window(store, "2026-09-28")
    def snapshot(now):
        web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: now).query("candidates")["data"]["research"]["primary"][0]
        review = review_persisted(tmp_path / "risk.sqlite3", ["2026-09-25", "2026-09-28"],
                                  as_of="2026-09-28")["records"][0]
        return web, review
    before_web, before_review = snapshot(datetime(2026, 9, 25, 2, tzinfo=timezone.utc))
    assert before_web["fill_status"] == "simulated_fill"
    assert before_review["marks"][1]["status"] == "complete"
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "datetime", _clock_at("2026-09-25T03:00:00+00:00"))
        store.record_daily_risk_observations({"trade_date": "2026-09-24", "batch_id": "batch-a",
            "observations": [_observation("limit_up", False, observed="2026-09-25T02:59:00+00:00")]})
    refreshed = store.qualify_research_pick("research:batch-a", "600000", "2026-09-25T03:01:00+00:00")
    assert refreshed["state"] == "eligible" and refreshed["version"] != qualified["version"]
    for state in ("eligible", "unknown", "blocked"):
        if state == "unknown":
            with monkeypatch.context() as clock:
                clock.setattr(storage_module, "datetime", _clock_at("2026-09-25T03:03:00+00:00"))
                store.record_daily_risk_observations({"trade_date": "2026-09-24", "batch_id": "batch-a",
                    "observations": [_observation("limit_up", True, observed="2026-09-25T03:02:00+00:00")]})
            unknown = store.qualify_research_pick("research:batch-a", "600000", "2026-09-25T03:04:00+00:00")
            assert unknown["state"] == "unknown"
        elif state == "blocked":
            # Synthetic future status, while the durable entry stays bound to its original eligible row.
            with store._connect() as db:
                db.execute("INSERT INTO paper_qualification_events(record_id,version,run_id,code,trade_date,batch_id,state,reason,risk_json,first_decided_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (qualified["record_id"], "risk:synthetic-blocked", "research:batch-a", "600000",
                     "2026-09-24", "batch-a", "blocked", "explicit_trading_risk", "{}",
                     "2026-09-25T03:05:00+00:00"))
        web, review = snapshot(datetime(2026, 9, 28, 10, tzinfo=timezone.utc))
        assert web["record_id"] == review["record_id"] == qualified["record_id"]
        assert web["fill_status"] == review["fill_status"] == "simulated_fill"
        assert web["entry_qualification_version"] == review["entry_qualification_version"] == qualified["version"]
        assert review["current_qualification_state"] == state
        assert review["marks"][1]["status"] == "complete"
        assert review["marks"][1]["return_pct"] == before_review["marks"][1]["return_pct"]
        assert web["eligibility"] == ("trading_flags_clear_only" if state == "eligible" else state)
    historical_web, _ = snapshot(datetime(2026, 9, 25, 2, tzinfo=timezone.utc))
    assert historical_web["qualification_version"] == qualified["version"]
    assert historical_web["fill_status"] == "simulated_fill"
    reopened = StockStore(tmp_path / "risk.sqlite3")
    assert reopened.latest_paper_qualification(qualified["record_id"])["state"] == "blocked"
    with reopened._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_simulated_entries").fetchone()[0] == 1


@pytest.mark.parametrize("bar_start,quote_at,now", [
    ("2026-09-25T11:02:00+08:00", "2026-09-25T14:00:00+08:00", "2026-09-25T14:00:01+08:00"),
    ("2026-09-25T11:29:00+08:00", "2026-09-25T13:00:10+08:00", "2026-09-25T13:00:11+08:00"),
    ("2026-09-25T14:59:00+08:00", "2026-09-28T09:30:10+08:00", "2026-09-28T09:30:11+08:00"),
    ("2026-09-25T09:35:00+08:00", "2026-09-25T09:36:10+08:00", "2026-09-25T14:00:01+08:00"),
    ("2026-09-26T09:35:00+08:00", "2026-09-26T09:36:10+08:00", "2026-09-26T09:36:11+08:00"),
    ("2026-09-28T09:35:00+08:00", "2026-09-28T09:36:10+08:00", "2026-09-28T09:36:11+08:00"),
])
def test_paper_rejects_stale_break_or_expired_plan(tmp_path, monkeypatch, bar_start, quote_at, now):
    store, qualified, common = _eligible_paper_pick(tmp_path, monkeypatch)
    if bar_start.startswith("2026-09-28"):
        with store._connect() as db:
            db.execute("INSERT INTO trading_calendar(trade_date,is_open,status,source,fetched_at) VALUES(?,?,?,?,?)",
                       ("2026-09-28", 1, "open", "synthetic", "2026-09-25T10:00:00+00:00"))
    missing = _observe(store,**common, bar_start=None,
                                                 quote_at=quote_at, now=now)
    assert missing["state"] == "unknown"
    outcome = _observe(store,**common, bar_start=bar_start,
                                                 quote_at=quote_at, now=now)
    assert outcome["state"] == "unknown"
    if bar_start == "2026-09-25T11:02:00+08:00":
        second = _observe(store,
            **common, bar_start="2026-09-25T11:03:00+08:00",
            quote_at="2026-09-25T14:00:30+08:00", now="2026-09-25T14:00:31+08:00")
        assert second["state"] == "unknown" and second.get("fill_status") is None
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_simulated_entries").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM intraday_signal_states WHERE origin='paper'").fetchone()[0] == 0


def test_automatic_close_collects_then_reuses_exact_batch_evidence(tmp_path):
    store = _store(tmp_path)
    sidecars = tmp_path / "research_risk_evidence"
    sidecars.mkdir()
    sidecar = {"version": 1, "trade_date": "2026-09-24", "batch_id": "batch-a",
               "sources": {"trading": "baostock:daily:unadjusted", "limit": "akshare:dated-pools"},
               "failed_codes": [], "captured_at": "2026-09-24T10:30:00+00:00",
               "source_captured_at": {field: "2026-09-24T10:30:00+00:00"
                                      for field in ("trading", "limit_up", "limit_down")},
               "rows": {"600000": {"close": 10.0, "suspended": False,
                                     "st": False, "limit_up": None, "limit_down": None}}}
    (sidecars / "2026-09-24.json").write_text(json.dumps(sidecar), encoding="utf-8")
    calls = []

    async def companion(quotes, *, collect_observations):
        calls.append([quote.code for quote in quotes])
        assert collect_observations is True
        for quote in quotes:
            quote.suspended = quote.limit_up = quote.limit_down = False
        return {"requested": 1, "complete": 1, "observations": [{
            "code": "600000", "reference_close": 10.0,
            "first_observed_at": "2026-09-24T10:45:00+00:00",
            "source_timestamp": "2026-09-24T07:00:00+00:00",
            "fields": {"suspended": False, "limit_up": False, "limit_down": False},
        }]}

    main = _main(Main, store)
    main.quotes = types.SimpleNamespace(enrich_daily_risk_fields=companion)
    fresh = asyncio.run(main._collect_automatic_close_risk_evidence(
        "2026-09-24", "batch-a", [core.Quote("600000", "Synthetic", 10.0)]))
    assert [fresh[0].suspended, fresh[0].limit_up, fresh[0].limit_down] == [False] * 3
    assert calls == [["600000"]]
    state = store.daily_risk_evidence_for_quotes(
        "2026-09-24", "batch-a", fresh, "2026-09-24T10:35:00+00:00")
    assert state == {}  # The sidecar was imported after this historical decision.
    state_now = store.daily_risk_evidence_for_quotes(
        "2026-09-24", "batch-a", fresh, datetime.now(timezone.utc))
    assert state_now["600000"]["suspended"] is False
    assert state_now["600000"]["st"] is False
    assert state_now["600000"]["limit_up"] is False
    main.quotes = types.SimpleNamespace(enrich_daily_risk_fields=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("cache should avoid provider")))
    replay = asyncio.run(main._collect_automatic_close_risk_evidence(
        "2026-09-24", "batch-a", [core.Quote("600000", "Synthetic", 10.0)]))
    assert replay[0].limit_up is False and replay[0].limit_down is False
    assert calls == [["600000"]]
    other_batch = asyncio.run(main._apply_cached_daily_risk_evidence(
        "2026-09-24", "batch-b", [core.Quote("600000", "Synthetic", 10.0)]))
    assert other_batch[0].limit_up is None

def test_automatic_gate_diagnostics_are_one_row_per_attempt(tmp_path):
    store = StockStore(tmp_path / "gates.sqlite3")
    job_key = "automatic_close:2026-09-24"
    first_clock = datetime.fromisoformat("2026-09-24T18:30:00+08:00")
    claim = store.claim_automatic_close_job(job_key, "2026-09-24", now=first_clock)
    assert claim["acquired"] and claim["automatic_attempts"] == 1
    gate = {"input": 5000, "risk_tuple_complete": 0, "indicator_targets": 0,
            "indicator_coverage": 0.0, "screen_min_indicator_coverage": 0.8}
    store.record_screen_gate_diagnostics(job_key, "2026-09-24", "fail_closed:risk_evidence_missing", gate)
    store.record_screen_gate_diagnostics(job_key, "2026-09-24", "completed", {"input": 1})
    store.finish_automatic_close_job(job_key, status="failed", error="risk_evidence_missing",
                                     retry_after_seconds=30, now=first_clock)
    second = store.claim_automatic_close_job(job_key, "2026-09-24", now=datetime.fromisoformat("2026-09-24T18:31:00+08:00"))
    assert second["acquired"] and second["automatic_attempts"] == 2
    store.record_screen_gate_diagnostics(job_key, "2026-09-24", "fail_closed:indicator_coverage",
                                         {**gate, "risk_tuple_complete": 5000, "indicator_targets": 100,
                                          "indicator_failed": 100})
    with store._connect() as db:
        rows = list(db.execute("SELECT attempt,phase,diagnostics_json FROM screen_gate_diagnostics ORDER BY attempt"))
    assert [(row["attempt"], row["phase"]) for row in rows] == [
        (1, "fail_closed:risk_evidence_missing"), (2, "fail_closed:indicator_coverage")]
    assert json.loads(rows[1]["diagnostics_json"])["indicator_failed"] == 100
