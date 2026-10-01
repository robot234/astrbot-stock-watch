from __future__ import annotations

import json
import hashlib
import asyncio
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from astrbot_stock_watch.core import Candidate, PricePlan, Quote, build_price_plan, format_compact_candidate, price_plan_is_validated
from astrbot_stock_watch.storage import StockStore
from astrbot_stock_watch.providers import TushareBulkDailyProvider


def _factor_row(store: StockStore, code: str, day: str, factor: float = 1.0) -> dict:
    digest = hashlib.sha256(f"synthetic:{code}:{day}:{factor}".encode()).hexdigest()
    observed = f"{day}T06:00:00+00:00"
    row = {"code": code, "trade_date": day, "adj_factor": factor,
           "source": "tushare_adj_factor", "evidence": f"tushare:adj_factor:{day}:{code}.SH",
           "observed_at": observed, "response_sha256": digest}
    assert store.save_corporate_action_factors([row]) == 1
    return {"corporate_action_factor": factor, "corporate_action_evidence": row["evidence"],
            "corporate_action_observed_at": observed, "corporate_action_response_sha256": digest}


def _record(store: StockStore, ident: str, code: str = "600000", version: str = "v1", *, comparable: str = "comparable", factor: float = 1.0) -> None:
    evidence = _factor_row(store, code, "2026-01-02", factor) if comparable == "comparable" else {}
    with store._connect() as db:
        db.execute(
            "INSERT INTO recommendation_records(recommendation_id,run_id,recommended_date,code,name,candidate_price,confirmation_price,invalidation_price,confirmation_level,target_low,plan_version,market_regime,data_timestamp,freshness,source,caller_identity,price_basis,prediction_json,created_at,strategy_version,plan_status,comparability_status,corporate_action_factor,corporate_action_evidence,corporate_action_observed_at,corporate_action_response_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ident, "run-" + version, "2026-01-02", code, code, 10.0, 10.0, 9.0, 11.0, 12.0, version, "neutral", "2026-01-02", "verified_close", "fixture", "fixture", "unadjusted", json.dumps({}), "2026-01-02T16:00:00", version, "validated", comparable, factor if comparable == "comparable" else None, evidence.get("corporate_action_evidence", ""), evidence.get("corporate_action_observed_at", ""), evidence.get("corporate_action_response_sha256", "")),
        )


def _calendar(store: StockStore) -> None:
    day = date(2026, 1, 3)
    while day <= date(2026, 1, 12):
        open_ = day.weekday() < 5
        store.save_calendar(day.isoformat(), open_, "fixture", ttl_seconds=999999999)
        if open_:
            store.save_snapshot_meta(day.isoformat(), "fixture", "good", True, day.isoformat())
        day += timedelta(days=1)


def _bars(store: StockStore, code: str, rows) -> None:
    store.save_daily_bars(code, [{"trade_date": day, "open": close, "high": high, "low": low, "close": close, "volume": 100, "amount": 1000, "price_basis": "unadjusted", **_factor_row(store, code, day)} for day, close, high, low in rows], "fixture", "unadjusted")


def _validated_plan() -> PricePlan:
    quote = Quote("600099", "fixture", 10, atr14=1, support20=10, resistance20=11, history_days=20, indicator_last_date="2026-09-09", indicator_last_close=10, indicator_price_basis="unadjusted", indicator_source="tushare")
    plan = build_price_plan(quote, context="daily_close", actual_date="2026-09-09")
    plan.provenance["corporate_action_evidence"] = {"comparable": True, "factor": 1}
    assert price_plan_is_validated(plan)
    return plan


def test_missing_corporate_action_evidence_fails_closed_without_invalidating_plan(tmp_path):
    store = StockStore(tmp_path / "comparability.sqlite3")
    _record(store, "missing", comparable="unknown")
    _calendar(store)
    _bars(store, "600000", [("2026-01-05", 11, 11, 11)])
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["unknown"] == 1
    with store._connect() as db:
        row = dict(db.execute("SELECT status,reason FROM recommendation_outcomes WHERE recommendation_id='missing' AND horizon=1").fetchone())
    assert row["status"] == "unknown" and row["reason"] == "corporate_action_evidence_missing"


def test_completed_snapshot_advances_outcomes_once_without_successful_screen(tmp_path):
    from test_v0138_automatic_close import _imports

    Main, _, _, _ = _imports()
    store = StockStore(tmp_path / "checkpoint.sqlite3")
    _record(store, "old", comparable="unknown")
    store.save_calendar("2026-01-05", True, "fixture", ttl_seconds=999999999)
    store.save_snapshot_meta("2026-01-05", "fixture", "partial", False, "2026-01-05")
    main = Main.__new__(Main)
    main.store = store
    now = datetime(2026, 1, 5, 16, 0, tzinfo=timezone(timedelta(hours=8)))
    assert asyncio.run(main._recommendation_outcome_checkpoint_tick(now))["state"] == "not_due"
    store.save_snapshot_meta("2026-01-05", "fixture", "good", True, "2026-01-05")
    assert store.recommendation_checkpoint_due("2026-01-05") == "2026-01-05"
    assert asyncio.run(main._recommendation_outcome_checkpoint_tick(now))["state"] == "evaluated"
    with store._connect() as db:
        rows = [dict(row) for row in db.execute("SELECT status,reason,updated_at FROM recommendation_outcomes")]
    assert len(rows) == 4
    assert all(row["status"] == "pending" or (row["status"] == "unknown" and row["reason"] == "corporate_action_evidence_missing") for row in rows)
    assert store.recommendation_checkpoint_due("2026-01-05") is None
    assert asyncio.run(main._recommendation_outcome_checkpoint_tick(now))["state"] == "not_due"
    with store._connect() as db:
        assert [row[0] for row in db.execute("SELECT updated_at FROM recommendation_outcomes")] == [row["updated_at"] for row in rows]


def test_mature_windows_no_future_leak_and_version_aggregation(tmp_path):
    store = StockStore(tmp_path / "recommendations.sqlite3")
    _record(store, "r1", version="v1")
    _record(store, "r2", code="600001", version="v2")
    _calendar(store)
    _bars(store, "600000", [("2026-01-05", 10.5, 10.8, 10.1), ("2026-01-06", 11.2, 11.5, 10.9), ("2026-01-07", 11.4, 11.8, 11.0), ("2026-01-08", 11.8, 12.2, 11.3), ("2026-01-09", 12.0, 12.3, 11.6), ("2026-01-12", 99.0, 99.0, 99.0)])
    _bars(store, "600001", [("2026-01-05", 9.5, 9.8, 9.2)])
    outcome = store.evaluate_recommendation_outcomes(as_of="2026-01-09")
    assert outcome["complete"] == 4
    rows = store.recommendation_reviews()
    first = next(row for row in rows if row["recommendation_id"] == "r1")
    assert first["status"] == "complete" and first["return_pct"] == pytest.approx(20.0)
    performance = {row["strategy_version"]: row for row in store.recommendation_performance(5)}
    assert performance["v1"]["mature_count"] == 1
    assert performance["v2"]["mature_count"] == 0


def test_missing_bars_pending_and_same_day_target_invalidation_is_unknown_order(tmp_path):
    store = StockStore(tmp_path / "orders.sqlite3")
    _record(store, "r1")
    _calendar(store)
    _bars(store, "600000", [("2026-01-05", 10.0, 12.2, 8.9), ("2026-01-06", 10.1, 10.2, 9.8), ("2026-01-07", 10.2, 10.3, 9.9), ("2026-01-08", 10.3, 10.4, 10.0), ("2026-01-09", 10.4, 10.5, 10.1)])
    store.evaluate_recommendation_outcomes(as_of="2026-01-09")
    row = next(row for row in store.recommendation_reviews() if row["recommendation_id"] == "r1")
    assert row["status"] == "unknown_order"
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-06")["pending"] >= 1


def test_calendar_gap_intraday_and_unusable_prices_fail_closed(tmp_path):
    store = StockStore(tmp_path / "gates.sqlite3")
    _record(store, "r1")
    _calendar(store)
    with store._connect() as db:
        db.execute("DELETE FROM trading_calendar WHERE trade_date='2026-01-06'")
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-09")["pending"] == 3
    _calendar(store)
    _bars(store, "600000", [("2026-01-05", 10, 10.1, 9.9), ("2026-01-06", 10, 10.1, 9.9), ("2026-01-07", 10, 10.1, 9.9), ("2026-01-08", 10, 10.1, 9.9), ("2026-01-09", 10, 10.1, 9.9)])
    with store._connect() as db:
        db.execute("UPDATE daily_bars SET volume=0 WHERE code='600000' AND trade_date='2026-01-05'")
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-09")["unknown"] >= 1


def test_cross_day_event_order_visibility_and_immutable_snapshot(tmp_path):
    store = StockStore(tmp_path / "immutable.sqlite3")
    _record(store, "r1", code="600000")
    _record(store, "r2", code="600001")
    _calendar(store)
    _bars(store, "600000", [("2026-01-05", 10, 12.2, 9.8), ("2026-01-06", 10, 10.2, 8.9), ("2026-01-07", 10, 10.2, 9.8), ("2026-01-08", 10, 10.2, 9.8), ("2026-01-09", 10, 10.2, 9.8)])
    _bars(store, "600001", [("2026-01-05", 10, 10.2, 8.9), ("2026-01-06", 10, 12.2, 9.8), ("2026-01-07", 10, 10.2, 9.8), ("2026-01-08", 10, 10.2, 9.8), ("2026-01-09", 10, 10.2, 9.8)])
    store.evaluate_recommendation_outcomes(as_of="2026-01-09")
    review = {row["recommendation_id"]: row for row in store.recommendation_reviews()}
    assert review["r1"]["event_order"] == "target_before_invalidation"
    assert review["r2"]["event_order"] == "invalidation_before_target"
    with store._connect() as db:
        db.execute("UPDATE recommendation_records SET visibility='private',origin='only-a' WHERE recommendation_id='r2'")
    assert {row["recommendation_id"] for row in store.recommendation_reviews("only-b")} == {"r1"}
    assert {row["recommendation_id"] for row in store.recommendation_reviews("only-a")} == {"r1", "r2"}
    plan = PricePlan("ready", 10, 1, 9, 11, 9.8, 10.2, 11, 12, 12.5, 9, "good", provenance={"basis": "unadjusted", "last_date": "2026-01-02", "tolerance_pct": 1.0, "corporate_action_evidence": {"comparable": True, "factor": 1}}, validated=True)
    candidate = Candidate(Quote("600002", "x", 10), 10, [], price_plan=plan)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        store._save_recommendations_in_tx(db, "same-run", "2026-01-02", "fixture", [candidate])
        candidate.quote.price = 99
        store._save_recommendations_in_tx(db, "same-run", "2026-01-02", "fixture", [candidate])
    with store._connect() as db:
        assert db.execute("SELECT candidate_price FROM recommendation_records WHERE run_id='same-run'").fetchone()[0] == 10


def test_normal_source_split_without_explicit_factor_is_unknown(tmp_path):
    store = StockStore(tmp_path / "split.sqlite3")
    _record(store, "r1")
    _calendar(store)
    store.save_daily_bars("600000", [{"trade_date": "2026-01-05", "open": 5, "high": 5, "low": 5, "close": 5, "volume": 100, "amount": 500, "price_basis": "unadjusted"}], "tushare", "unadjusted")
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["unknown"] == 1


def test_non_unit_adj_factor_is_comparable_until_factor_changes(tmp_path):
    store = StockStore(tmp_path / "factor-change.sqlite3")
    _record(store, "factor", comparable="comparable", factor=7.25)
    _calendar(store)
    rows = [
        {"trade_date": "2026-01-05", "open": 10, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 100, "amount": 1000, "price_basis": "unadjusted", **_factor_row(store, "600000", "2026-01-05", 7.25)},
    ]
    store.save_daily_bars("600000", rows, "tushare_adj_factor", "unadjusted")
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["complete"] == 1
    rows[0]["corporate_action_factor"] = 7.5
    store.save_daily_bars("600000", rows, "tushare_adj_factor", "unadjusted")
    outcome = store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))
    assert outcome["unknown"] == 1


def test_adj_factor_provider_parses_and_persists_explicit_evidence(tmp_path):
    class Gateway:
        async def request_json(self, _client, payload, **kwargs):
            assert payload["api_name"] == kwargs["api_name"] == "adj_factor"
            assert payload["params"]["trade_date"] == "20260105"
            return {"data": {"fields": ["ts_code", "trade_date", "adj_factor"], "items": [["600000.SH", "20260105", 7.25]]}}

    provider = TushareBulkDailyProvider.__new__(TushareBulkDailyProvider)
    provider.page_size = 6000
    provider.token = "fixture"
    provider.gateway = Gateway()
    row = __import__("asyncio").run(provider.fetch_adj_factor_page("2026-01-05", client=object()))[0]
    assert {key: row[key] for key in ("code", "ts_code", "trade_date", "adj_factor", "source", "evidence")} == {
        "code": "600000", "ts_code": "600000.SH", "trade_date": "2026-01-05", "adj_factor": 7.25,
        "source": "tushare_adj_factor", "evidence": "tushare:adj_factor:2026-01-05:600000.SH",
    }
    assert row["observed_at"].endswith("+00:00") and len(row["response_sha256"]) == 64
    store = StockStore(tmp_path / "factor-store.sqlite3")
    assert store.save_corporate_action_factors([row]) == 1
    assert store.save_corporate_action_factors([{**row, "evidence": ""}]) == 0
    assert store.save_corporate_action_factors([{**row, "evidence": "fixture"}]) == 0
    saved = store.corporate_action_factors(["600000"], ["2026-01-05"])["600000:2026-01-05"]
    assert saved["adj_factor"] == pytest.approx(7.25) and saved["evidence"] == row["evidence"]


def test_adj_factor_provider_ignores_unvalidated_paging_rows():
    provider = TushareBulkDailyProvider.__new__(TushareBulkDailyProvider)
    provider.page_size = 6000

    async def pages(*args, **kwargs):
        yield 0, [{
            "code": "600000",
            "ts_code": "600000.SH",
            "trade_date": "2026-01-05",
            "close": 10.0,
        }]

    provider._iter_pages = pages
    rows = __import__("asyncio").run(provider.fetch_adj_factor_date("2026-01-05"))
    assert rows == []


def test_factor_first_capture_survives_refetch_and_revision_fails_closed(tmp_path):
    store = StockStore(tmp_path / "factor-revision.sqlite3")
    _record(store, "r1")
    _calendar(store)
    _bars(store, "600000", [("2026-01-05", 10.5, 10.8, 10.1)])
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["complete"] == 1
    original = store.corporate_action_factors(["600000"], ["2026-01-02"])["600000:2026-01-02"]
    replay = {**original, "observed_at": "2026-01-03T06:00:00+00:00", "response_sha256": "f" * 64}
    assert store.save_corporate_action_factors([replay]) == 1
    unchanged = store.corporate_action_factors(["600000"], ["2026-01-02"])["600000:2026-01-02"]
    assert unchanged["observed_at"] == original["observed_at"]
    assert unchanged["response_sha256"] == original["response_sha256"]
    assert unchanged["conflicted"] == 0
    assert store.save_corporate_action_factors([{**replay, "adj_factor": 1.1}]) == 1
    conflicted = store.corporate_action_factors(["600000"], ["2026-01-02"])["600000:2026-01-02"]
    assert conflicted["conflicted"] == 1 and conflicted["adj_factor"] == 1
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["unknown"] == 1


def test_legacy_and_after_cutoff_factor_cannot_upgrade_old_recommendation(tmp_path):
    store = StockStore(tmp_path / "late-factor.sqlite3")
    _record(store, "r1", comparable="unknown")
    _calendar(store)
    with store._connect() as db:
        db.execute("INSERT INTO corporate_action_factors(code,trade_date,adj_factor,source,evidence,fetched_at) VALUES(?,?,?,?,?,?)",
                   ("600000", "2026-01-02", 1, "tushare_adj_factor", "tushare:adj_factor:2026-01-02:600000.SH", "2026-01-02T06:00:00"))
    legacy = store.corporate_action_factors(["600000"], ["2026-01-02"])["600000:2026-01-02"]
    assert not legacy["observed_at"] and not legacy["response_sha256"]
    late = {"code": "600000", "trade_date": "2026-01-02", "adj_factor": 1,
            "source": "tushare_adj_factor", "evidence": legacy["evidence"],
            "observed_at": "2026-01-03T06:00:00+00:00", "response_sha256": "a" * 64}
    assert store.save_corporate_action_factors([late]) == 1
    with store._connect() as db:
        db.execute("UPDATE recommendation_records SET comparability_status='comparable',corporate_action_factor=1,corporate_action_evidence=?,corporate_action_observed_at=?,corporate_action_response_sha256=? WHERE recommendation_id='r1'",
                   (late["evidence"], late["observed_at"], late["response_sha256"]))
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["unknown"] == 1
    assert store.save_corporate_action_factors([{**late, "observed_at": "2999-01-01T00:00:00+00:00"}]) == 0


def test_bar_factor_observed_after_evaluation_cutoff_is_unknown(tmp_path):
    store = StockStore(tmp_path / "late-bar.sqlite3")
    _record(store, "r1")
    _calendar(store)
    factor = _factor_row(store, "600000", "2026-01-05")
    late = {**factor, "corporate_action_observed_at": "2026-01-06T06:00:00+00:00"}
    # A later API response is a distinct first observation on this test database.
    with store._connect() as db:
        db.execute("UPDATE corporate_action_factors SET observed_at=? WHERE code='600000' AND trade_date='2026-01-05'",
                   (late["corporate_action_observed_at"],))
    store.save_daily_bars("600000", [{"trade_date": "2026-01-05", "open": 10, "high": 10.5,
                                      "low": 9.8, "close": 10.2, "volume": 100, "amount": 1000,
                                      "price_basis": "unadjusted", **late}], "fixture", "unadjusted")
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["unknown"] == 1


def test_unmatured_window_stays_pending_without_factor_capture(tmp_path):
    store = StockStore(tmp_path / "pending-factor.sqlite3")
    _record(store, "r1", comparable="unknown")
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-02", horizons=(1,))["pending"] == 1


def test_candidate_freeze_requires_matching_persisted_capture(tmp_path):
    store = StockStore(tmp_path / "frozen-capture.sqlite3")
    evidence = _factor_row(store, "600099", "2026-09-09", 7.25)
    plan = _validated_plan()
    plan.provenance["corporate_action_evidence"] = {
        "comparable": True, "factor": 7.25, "source": "tushare_adj_factor",
        "trade_date": "2026-09-09", "evidence": evidence["corporate_action_evidence"],
        "observed_at": evidence["corporate_action_observed_at"],
        "response_sha256": evidence["corporate_action_response_sha256"],
    }
    with store._connect() as db:
        store._save_recommendations_in_tx(db, "capture-run", "2026-09-09", "fixture",
                                          [Candidate(Quote("600099", "capture", 10), 10, [], price_plan=plan),
                                           Candidate(Quote("600098", "missing", 10), 10, [], price_plan=plan)])
    with store._connect() as db:
        statuses = {row["code"]: row["comparability_status"] for row in db.execute(
            "SELECT code,comparability_status FROM recommendation_records WHERE run_id='capture-run'")}
    assert statuses == {"600099": "comparable", "600098": "unknown"}


def test_legacy_factor_cache_without_capture_does_not_attach(tmp_path):
    import asyncio
    store = StockStore(tmp_path / "legacy-factor-cache.sqlite3")
    with store._connect() as db:
        db.execute("INSERT INTO corporate_action_factors(code,trade_date,adj_factor,source,evidence,fetched_at) VALUES(?,?,?,?,?,?)",
                   ("600000", "2026-01-05", 1, "tushare_adj_factor",
                    "tushare:adj_factor:2026-01-05:600000.SH", "2026-01-05T07:00:00"))
    provider = TushareBulkDailyProvider.__new__(TushareBulkDailyProvider)
    provider.storage = store
    rows = {"2026-01-05": [{"code": "600000", "trade_date": "2026-01-05"}]}
    diagnostics = {}
    asyncio.run(provider._apply_corporate_action_factors(rows, ["2026-01-05"], diagnostics, allow_network=False))
    assert "corporate_action_factor" not in rows["2026-01-05"][0]
    assert diagnostics["corporate_action_evidence"] == "unavailable"


def test_unpersisted_factor_response_does_not_attach():
    import asyncio
    from contextlib import asynccontextmanager

    class Storage:
        def corporate_action_factors(self, *_args):
            return {}

        def save_corporate_action_factors(self, *_args):
            return 0

    class HTTP:
        @asynccontextmanager
        async def slot(self):
            yield object()

    provider = TushareBulkDailyProvider.__new__(TushareBulkDailyProvider)
    provider.storage, provider.http = Storage(), HTTP()

    async def fetched(*_args, **_kwargs):
        return [{"code": "600000", "ts_code": "600000.SH", "trade_date": "2026-01-05",
                 "adj_factor": 1, "source": "tushare_adj_factor",
                 "evidence": "tushare:adj_factor:2026-01-05:600000.SH",
                 "observed_at": "2026-01-05T06:00:00+00:00", "response_sha256": "a" * 64}]

    provider.fetch_adj_factor_date = fetched
    rows = {"2026-01-05": [{"code": "600000", "trade_date": "2026-01-05"}]}
    diagnostics = {}
    asyncio.run(provider._apply_corporate_action_factors(rows, ["2026-01-05"], diagnostics))
    assert diagnostics["corporate_action_evidence"] == "unavailable"
    assert "corporate_action_factor" not in rows["2026-01-05"][0]


def test_unknown_order_uses_price_denominator_not_path_denominator(tmp_path):
    store = StockStore(tmp_path / "performance.sqlite3")
    _record(store, "complete")
    _record(store, "unknown-order", "600021")
    with store._connect() as db:
        store._upsert_recommendation_outcome(db, "complete", 5, "2026-01-09", "complete", return_pct=10, max_gain_pct=15, max_drawdown_pct=-3, invalidation_order="touched", sample_complete=True, session_complete=True)
        store._upsert_recommendation_outcome(db, "unknown-order", 5, "2026-01-09", "unknown_order", return_pct=-2, max_gain_pct=20, max_drawdown_pct=-4, target_order="touched", invalidation_order="touched", session_complete=True)
    row = store.recommendation_performance(5)[0]
    assert row["price_evaluable_count"] == 2 and row["order_evaluable_count"] == 1
    assert row["median_return_pct"] == pytest.approx(4.0)
    assert row["median_max_gain_pct"] == pytest.approx(17.5)
    assert row["max_drawdown_pct"] == pytest.approx(-4.0)
    assert row["invalidation_count"] == row["invalidation_eligible_count"] == 1


def test_private_bundle_scenario_renderer_and_rollback(tmp_path):
    store = StockStore(tmp_path / "bundle.sqlite3")
    candidate = Candidate(Quote("600003", "private", 10), 10, [], price_plan=_validated_plan())
    assert "情景涨幅区间" in format_compact_candidate(candidate, 1)
    args = ("private-run", "manual_screen", "2026-09-09", "2026-09-09", "fixture", "2026-09-09T16:00:00Z", "2026-09-09T16:00:00Z", 1, 1, "completed", "good", None)
    store.save_screen_bundle_atomic(args, [candidate], diagnostics={}, coverage=1, report_key="private", scope="origin-a")
    assert store.recommendation_reviews("origin-a") and not store.recommendation_reviews("origin-b")
    original = store._save_recommendations_in_tx
    store._save_recommendations_in_tx = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture"))
    with pytest.raises(RuntimeError, match="fixture"):
        store.save_screen_bundle_atomic(("rollback", *args[1:]), [candidate], diagnostics={}, coverage=1, report_key="rollback")
    store._save_recommendations_in_tx = original
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM screen_runs WHERE run_id='rollback'").fetchone()[0] == 0


def test_real_bundle_producer_is_private_for_non_global_scope(tmp_path):
    store = StockStore(tmp_path / "scope.sqlite3")
    plan = _validated_plan()
    candidate = Candidate(Quote("600003", "private", 10), 10, [], price_plan=plan)
    args = ("private-run", "manual_screen", "2026-09-09", "2026-09-09", "fixture", "2026-09-09T16:00:00Z", "2026-09-09T16:00:00Z", 1, 1, "completed", "good", None)
    store.save_screen_bundle_atomic(args, [candidate], diagnostics={}, coverage=1, report_key="private", scope="origin-a")
    assert store.recommendation_reviews("origin-a")
    assert not store.recommendation_reviews("origin-b")
    with store._connect() as db:
        row = db.execute("SELECT visibility,origin,caller_identity,strategy_version,plan_version FROM recommendation_records WHERE run_id='private-run'").fetchone()
    assert row[0:3] == ("private", "origin-a", "screen:origin-a")
    assert row[3] != row[4]


def test_aggregation_keeps_unknown_out_of_return_denominator_and_even_median(tmp_path):
    store = StockStore(tmp_path / "aggregate.sqlite3")
    _record(store, "r1", code="600010", version="same")
    _record(store, "r2", code="600011", version="same")
    _record(store, "r3", code="600012", version="same")
    _calendar(store)
    for code, close in (("600010", 11), ("600011", 13)):
        _bars(store, code, [("2026-01-05", close, close, close), ("2026-01-06", close, close, close), ("2026-01-07", close, close, close), ("2026-01-08", close, close, close), ("2026-01-09", close, close, close)])
    store.evaluate_recommendation_outcomes(as_of="2026-01-09")
    row = store.recommendation_performance(5)[0]
    assert row["sample_count"] == 3 and row["evaluable_count"] == 2 and row["unknown_count"] == 1
    assert row["median_return_pct"] == pytest.approx(20.0)


def test_performance_includes_unknown_order_prices_but_excludes_it_from_path_rates(tmp_path):
    store = StockStore(tmp_path / "mixed-performance.sqlite3")
    _record(store, "complete", version="mixed")
    _record(store, "unknown-order", code="600021", version="mixed")
    with store._connect() as db:
        store._upsert_recommendation_outcome(db, "complete", 5, "2026-01-09", "complete", close_price=11, return_pct=10, max_gain_pct=15, max_drawdown_pct=-3, target_order="not_touched", invalidation_order="touched", sample_complete=True, session_complete=True)
        store._upsert_recommendation_outcome(db, "unknown-order", 5, "2026-01-09", "unknown_order", close_price=9.8, return_pct=-2, max_gain_pct=20, max_drawdown_pct=-4, target_order="touched", invalidation_order="touched", reason="daily_bar_order_unprovable", session_complete=True)
    row = store.recommendation_performance(5)[0]
    assert row["price_evaluable_count"] == row["evaluable_count"] == 2
    assert row["order_evaluable_count"] == 1
    assert row["median_return_pct"] == pytest.approx(4.0)
    assert row["median_max_gain_pct"] == pytest.approx(17.5)
    assert row["max_drawdown_pct"] == pytest.approx(-4.0)
    assert row["invalidation_count"] == 1 and row["invalidation_eligible_count"] == 1
    assert row["invalidation_rate"] == pytest.approx(1.0)


def test_scenario_range_renderer_and_recommendation_failure_rollback(tmp_path):
    store = StockStore(tmp_path / "rollback-source.sqlite3")
    candidate = Candidate(Quote("600020", "scenario", 10), 10, [], price_plan=_validated_plan())
    assert "情景涨幅区间" in format_compact_candidate(candidate, 1)
    args = ("rollback-run", "manual_screen", "2026-09-09", "2026-09-09", "fixture", "2026-09-09T16:00:00Z", "2026-09-09T16:00:00Z", 1, 1, "completed", "good", None)
    original = store._save_recommendations_in_tx
    store._save_recommendations_in_tx = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture"))
    with pytest.raises(RuntimeError, match="fixture"):
        store.save_screen_bundle_atomic(args, [candidate], diagnostics={}, coverage=1, report_key="rollback")
    store._save_recommendations_in_tx = original
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM screen_runs WHERE run_id='rollback-run'").fetchone()[0] == 0
