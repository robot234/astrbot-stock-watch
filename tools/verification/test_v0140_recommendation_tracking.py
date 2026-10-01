from __future__ import annotations

import json
from datetime import date, timedelta

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from astrbot_stock_watch.storage import StockStore
from astrbot_stock_watch.core import Candidate, PricePlan, Quote, build_price_plan, format_compact_candidate, price_plan_is_validated
import pytest


def _record(store: StockStore, recommendation_id: str, code: str = "600000", version: str = "v1") -> None:
    with store._connect() as db:
        db.execute(
            "INSERT INTO recommendation_records(recommendation_id,run_id,recommended_date,code,name,candidate_price,confirmation_price,invalidation_price,confirmation_level,target_low,plan_version,market_regime,data_timestamp,freshness,source,caller_identity,price_basis,prediction_json,created_at,strategy_version,plan_status,comparability_status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (recommendation_id, "run-" + version, "2026-01-02", code, code, 10.0, 10.0, 9.0, 11.0, 12.0, version, "neutral", "2026-01-02", "verified_close", "fixture", "fixture", "unadjusted", json.dumps({"status": "available"}), "2026-01-02T16:00:00", version, "validated", "comparable"),
        )


def _calendar(store: StockStore, *open_days: str) -> None:
    start, end = date(2026, 1, 3), date(2026, 1, 12)
    open_set = set(open_days)
    while start <= end:
        store.save_calendar(start.isoformat(), start.isoformat() in open_set, "fixture", ttl_seconds=999999999)
        start += timedelta(days=1)
    for day in open_days:
        store.save_snapshot_meta(day, "fixture", "good", True, day)


def _bars(store: StockStore, code: str, values: list[tuple[str, float, float, float]]) -> None:
    store.save_daily_bars(code, [{"trade_date": day, "open": close, "high": high, "low": low, "close": close, "volume": 100, "amount": 1000, "price_basis": "unadjusted", "corporate_action_factor": 1, "corporate_action_evidence": "fixture:no_action"} for day, close, high, low in values], "fixture", "unadjusted")


def _validated_daily_close_plan() -> PricePlan:
    quote = Quote(
        "600099", "fixture", 10,
        atr14=1, support20=10, resistance20=11, history_days=20,
        indicator_last_date="2026-09-09", indicator_last_close=10,
        indicator_price_basis="unadjusted", indicator_source="tushare",
    )
    plan = build_price_plan(quote, context="daily_close", actual_date="2026-09-09")
    plan.provenance["corporate_action_evidence"] = {"comparable": True, "factor": 1}
    assert price_plan_is_validated(plan)
    return plan


def test_mature_windows_no_future_leak_and_version_aggregation(tmp_path):
    store = StockStore(tmp_path / "recommendations.sqlite3")
    _record(store, "r1", version="v1")
    _record(store, "r2", code="600001", version="v2")
    _calendar(store, "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09")
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
    _calendar(store, "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09")
    _bars(store, "600000", [("2026-01-05", 10.0, 12.2, 8.9), ("2026-01-06", 10.1, 10.2, 9.8), ("2026-01-07", 10.2, 10.3, 9.9), ("2026-01-08", 10.3, 10.4, 10.0), ("2026-01-09", 10.4, 10.5, 10.1)])
    store.evaluate_recommendation_outcomes(as_of="2026-01-09")
    row = next(row for row in store.recommendation_reviews() if row["recommendation_id"] == "r1")
    assert row["status"] == "unknown_order"
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-06")["pending"] >= 1


def test_calendar_gap_intraday_and_unusable_prices_fail_closed(tmp_path):
    store = StockStore(tmp_path / "gates.sqlite3")
    _record(store, "r1")
    _calendar(store, "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09")
    with store._connect() as db:
        db.execute("DELETE FROM trading_calendar WHERE trade_date='2026-01-06'")
    # The T+1 window ends before the later missing calendar row; only later
    # horizons remain pending rather than retroactively degrading maturity.
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-09")["pending"] == 3
    _calendar(store, "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09")
    _bars(store, "600000", [("2026-01-05", 10, 10.1, 9.9), ("2026-01-06", 10, 10.1, 9.9), ("2026-01-07", 10, 10.1, 9.9), ("2026-01-08", 10, 10.1, 9.9), ("2026-01-09", 10, 10.1, 9.9)])
    with store._connect() as db:
        db.execute("UPDATE daily_bars SET volume=0 WHERE code='600000' AND trade_date='2026-01-05'")
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-09")["unknown"] >= 1


def test_cross_day_event_order_visibility_and_immutable_snapshot(tmp_path):
    store = StockStore(tmp_path / "immutable.sqlite3")
    _record(store, "r1", code="600000")
    _record(store, "r2", code="600001")
    _calendar(store, "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09")
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
    _calendar(store, "2026-01-05")
    store.save_daily_bars("600000", [{"trade_date": "2026-01-05", "open": 5, "high": 5, "low": 5, "close": 5, "volume": 100, "amount": 500, "price_basis": "unadjusted"}], "tushare", "unadjusted")
    result = store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))
    assert result["unknown"] == 1


def test_real_bundle_producer_is_private_for_non_global_scope(tmp_path):
    store = StockStore(tmp_path / "scope.sqlite3")
    plan = _validated_daily_close_plan()
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
    _calendar(store, "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09")
    for code, close in (("600010", 11), ("600011", 13)):
        _bars(store, code, [("2026-01-05", close, close, close), ("2026-01-06", close, close, close), ("2026-01-07", close, close, close), ("2026-01-08", close, close, close), ("2026-01-09", close, close, close)])
    # Third record has an explicit calendar/session window but no comparable bar.
    store.evaluate_recommendation_outcomes(as_of="2026-01-09")
    row = store.recommendation_performance(5)[0]
    assert row["sample_count"] == 3 and row["evaluable_count"] == 2 and row["unknown_count"] == 1
    assert row["median_return_pct"] == pytest.approx(20.0)


def test_performance_includes_unknown_order_prices_but_excludes_it_from_path_rates(tmp_path):
    store = StockStore(tmp_path / "mixed-performance.sqlite3")
    _record(store, "complete", version="mixed")
    _record(store, "unknown-order", code="600021", version="mixed")
    with store._connect() as db:
        store._upsert_recommendation_outcome(
            db, "complete", 5, "2026-01-09", "complete", close_price=11, return_pct=10,
            max_gain_pct=15, max_drawdown_pct=-3, target_order="not_touched",
            invalidation_order="touched", sample_complete=True, session_complete=True,
        )
        store._upsert_recommendation_outcome(
            db, "unknown-order", 5, "2026-01-09", "unknown_order", close_price=9.8, return_pct=-2,
            max_gain_pct=20, max_drawdown_pct=-4, target_order="touched",
            invalidation_order="touched", reason="daily_bar_order_unprovable", session_complete=True,
        )
    row = store.recommendation_performance(5)[0]
    assert row["price_evaluable_count"] == row["evaluable_count"] == 2
    assert row["order_evaluable_count"] == 1
    assert row["median_return_pct"] == pytest.approx(4.0)
    assert row["median_max_gain_pct"] == pytest.approx(17.5)
    assert row["max_drawdown_pct"] == pytest.approx(-4.0)
    assert row["invalidation_count"] == 1 and row["invalidation_eligible_count"] == 1
    assert row["invalidation_rate"] == pytest.approx(1.0)


def test_scenario_range_renderer_and_recommendation_failure_rollback(tmp_path):
    store = StockStore(tmp_path / "rollback.sqlite3")
    plan = _validated_daily_close_plan()
    candidate = Candidate(Quote("600020", "scenario", 10), 10, [], price_plan=plan)
    assert "情景涨幅区间" in format_compact_candidate(candidate, 1)
    args = ("rollback-run", "manual_screen", "2026-09-09", "2026-09-09", "fixture", "2026-09-09T16:00:00Z", "2026-09-09T16:00:00Z", 1, 1, "completed", "good", None)
    original = store._save_recommendations_in_tx
    store._save_recommendations_in_tx = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture"))
    with pytest.raises(RuntimeError, match="fixture"):
        store.save_screen_bundle_atomic(args, [candidate], diagnostics={}, coverage=1, report_key="rollback")
    store._save_recommendations_in_tx = original
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM screen_runs WHERE run_id='rollback-run'").fetchone()[0] == 0
