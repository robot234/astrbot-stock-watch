from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
import json
from types import SimpleNamespace
from threading import Barrier

import pytest

from test_v0139_intraday_m2 import _collect, _imports, _main
from test_v0140_recommendation_tracking import _bars, _calendar, _record, _validated_plan


def _runtime(tmp_path):
    Main, core, Store = _imports()
    store = Store(tmp_path / "m3.sqlite3")
    main = _main(Main, store)
    main._raw_screen_provenance = {}
    main._tushare_mode = lambda: False
    return main, core, store


def _event(origin):
    return SimpleNamespace(unified_msg_origin=origin, plain_result=lambda text: text)


@pytest.mark.parametrize("evidence", [
    "tushare:adj_factor:2026-09-08:600099.SH",
    "tushare:adj_factor:2026-09-09:600000.SH",
    "tushare:adj_factor:2026-09-09:600099.SZ",
    "tushare:adj_factor:",
    "provider_declaration",
])
def test_close_producer_rejects_misbound_factor_evidence(tmp_path, evidence):
    main, core, store = _runtime(tmp_path)
    quote = core.Quote(
        "600099", "fixture", 10, atr14=1, support20=10, resistance20=11,
        history_days=20, indicator_last_date="2026-09-09",
        indicator_last_close=10, indicator_price_basis="unadjusted",
        indicator_source="tushare", corporate_action_factor=7.25,
        corporate_action_evidence=evidence,
    )
    plan = main._daily_close_plan(quote, "2026-09-09")
    assert core.price_plan_is_validated(plan)
    assert "corporate_action_evidence" not in plan.provenance
    with store._connect() as db:
        store._save_recommendations_in_tx(
            db, "run", "2026-09-09", "fixture",
            [core.Candidate(quote, 10, [], price_plan=plan)],
        )
        row = db.execute("SELECT comparability_status,corporate_action_factor FROM recommendation_records").fetchone()
    assert tuple(row) == ("unknown", None)


@pytest.mark.parametrize("factor", [None, 0, -1, "bad", float("nan"), float("inf")])
def test_close_producer_rejects_unusable_factors(tmp_path, factor):
    main, core, _store = _runtime(tmp_path)
    quote = core.Quote(
        "600099", "fixture", 10, corporate_action_factor=factor,
        corporate_action_evidence="tushare:adj_factor:2026-09-09:600099.SH",
    )
    main._raw_screen_provenance = {
        "corporate_action_evidence": {"comparable": True, "factor": factor},
    }
    assert "corporate_action_evidence" not in main._daily_close_plan(quote, "2026-09-09").provenance


@pytest.mark.parametrize("damage", ["missing", "unvalidated", "basis", "stale", "anchor", "quality"])
def test_snapshot_and_render_share_plan_validation(tmp_path, damage):
    _main_obj, core, store = _runtime(tmp_path)
    plan = _validated_plan()
    if damage == "missing":
        plan = None
    elif damage == "unvalidated":
        plan.validated = False
    elif damage == "basis":
        plan.provenance["basis"] = "qfq"
    elif damage == "stale":
        plan.provenance["last_date"] = "2026-09-08"
    elif damage == "anchor":
        plan.provenance["anchor_price"] = 99
    else:
        plan.quality = "insufficient"
    candidate = core.Candidate(core.Quote("600099", "fixture", 10), 10, [], price_plan=plan)
    assert not core.price_plan_is_validated(plan)
    rendered = core.format_compact_candidate(candidate, 1)
    assert "情景涨幅区间" not in rendered and "数据不足" in rendered
    with store._connect() as db:
        store._save_recommendations_in_tx(db, "run", "2026-09-09", "fixture", [candidate])
        row = db.execute("SELECT plan_status,prediction_json FROM recommendation_records").fetchone()
    assert row["plan_status"] == "unknown"
    assert json.loads(row["prediction_json"])["status"] == "insufficient_data"


@pytest.mark.parametrize("updates,reason", [
    ({"atr": "bad"}, "price_plan_invalid"),
    ({"atr": 0}, "atr_or_reference_missing"),
    ({"sell_high": float("inf")}, "atr_or_reference_missing"),
    ({"invalidation": 10}, "risk_reward_inadequate"),
    ({"invalidation": 1}, "risk_reward_inadequate"),
])
def test_prediction_rejects_insufficient_evidence(tmp_path, updates, reason):
    _main_obj, _core, store = _runtime(tmp_path)
    plan = {**asdict(_validated_plan()), **updates}
    assert store._recommendation_prediction(plan, candidate_price=10) == {
        "status": "insufficient_data", "reason": reason,
    }


@pytest.mark.parametrize("gate,status,reason", [
    ("intraday", "pending", "session_not_complete"),
    ("snapshot", "pending", "completed_session_evidence_missing"),
    ("missing_bar", "unknown", "missing_suspended_or_price_not_comparable"),
    ("base_factor", "unknown", "corporate_action_base_factor_missing"),
    ("reference", "unknown", "reference_price_invalid"),
])
def test_outcome_gates_clear_previously_computed_metrics(tmp_path, gate, status, reason):
    _main_obj, _core, store = _runtime(tmp_path)
    _record(store, "record")
    _calendar(store)
    _bars(store, "600000", [("2026-01-05", 10.5, 10.8, 10.1)])
    assert store.evaluate_recommendation_outcomes(as_of="2026-01-05", horizons=(1,))["complete"] == 1
    cutoff = "2026-01-05"
    with store._connect() as db:
        if gate == "intraday":
            cutoff = datetime(2026, 1, 5, 14, 59)
        elif gate == "snapshot":
            db.execute("UPDATE daily_snapshot_meta SET complete=0")
        elif gate == "missing_bar":
            db.execute("DELETE FROM daily_bars")
        elif gate == "base_factor":
            db.execute("UPDATE recommendation_records SET corporate_action_factor=0")
        else:
            db.execute("UPDATE recommendation_records SET confirmation_price=-1")
    assert store.evaluate_recommendation_outcomes(as_of=cutoff, horizons=(1,))[status] == 1
    with store._connect() as db:
        row = db.execute("SELECT * FROM recommendation_outcomes").fetchone()
    assert (row["status"], row["reason"]) == (status, reason)
    assert all(row[key] is None for key in ("return_pct", "max_gain_pct", "max_drawdown_pct"))
    assert row["sample_complete"] == row["session_complete"] == 0


@pytest.mark.parametrize("horizon", [-1, 0, 2, 11, "bad", None])
def test_command_rejects_unsupported_horizon_without_read(tmp_path, horizon):
    main, _core, store = _runtime(tmp_path)
    def unexpected_read(*args):
        pytest.fail("Unsupported horizon must not query another window")
    store.recommendation_performance = unexpected_read
    text = asyncio.run(_collect(main.strategy_performance(_event("a"), horizon)))[0]
    assert "仅支持" in text


@pytest.mark.parametrize("metric", [None, float("nan"), float("inf")])
def test_review_and_performance_do_not_render_invalid_metrics(tmp_path, metric):
    main, _core, store = _runtime(tmp_path)
    store.recommendation_reviews = lambda *_args: [{
        "status": "complete", "code": "600099", "recommended_date": "2026-09-09",
        "return_pct": metric, "max_gain_pct": metric, "max_drawdown_pct": metric,
    }]
    store.recommendation_performance = lambda *_args: [{
        "mature_count": 1, "sample_count": 1, "positive_return_rate": metric,
        "median_return_pct": metric, "median_max_gain_pct": metric,
        "max_drawdown_pct": metric, "return_distribution": {"min": metric, "max": metric},
    }]
    for command in (main.recommendation_review, main.strategy_performance):
        text = asyncio.run(_collect(command(_event("a"))))[0]
        assert "数据不足" in text
        assert "nan" not in text.lower() and "inf" not in text.lower()


@pytest.mark.parametrize("command_name", ["recommendation_review", "strategy_performance"])
def test_command_missing_store_and_empty_store(tmp_path, command_name):
    main, _core, _store = _runtime(tmp_path)
    command = getattr(main, command_name)
    assert "暂无" in asyncio.run(_collect(command(_event("a"))))[0]
    main.store = SimpleNamespace()
    assert "暂不可用" in asyncio.run(_collect(command(_event("a"))))[0]


def test_simultaneous_commands_keep_private_results_and_diagnostics_isolated(tmp_path):
    main, _core, store = _runtime(tmp_path)
    for ident, code in (("private-a", "600001"), ("private-b", "600002"), ("public", "600003")):
        _record(store, ident, code, version=ident)
    with store._connect() as db:
        db.execute("UPDATE recommendation_records SET visibility='private',origin='origin-a' WHERE recommendation_id='private-a'")
        db.execute("UPDATE recommendation_records SET visibility='private',origin='origin-b' WHERE recommendation_id='private-b'")
        before = list(db.iterdump())
    main._last_screen_diagnostics = {"diagnostics_invocation_id": "unchanged", "input": 5550}
    original = main._store_call

    async def scenario():
        entered = 0
        all_entered = asyncio.Event()
        async def gated(method, *args, **kwargs):
            nonlocal entered
            entered += 1
            if entered == 6:
                all_entered.set()
            await asyncio.wait_for(all_entered.wait(), timeout=5)
            return await original(method, *args, **kwargs)
        main._store_call = gated
        return await asyncio.gather(*[
            _collect(command(_event(origin)))
            for origin in ("origin-a", "origin-b", "")
            for command in (main.recommendation_review, main.strategy_performance)
        ])

    results = asyncio.run(scenario())
    for index, output in enumerate(results):
        text = output[0]
        if index % 2 == 0:
            assert "600003" in text
            assert ("600001" in text) == (index == 0)
            assert ("600002" in text) == (index == 2)
        else:
            assert "public" in text
            assert ("private-a" in text) == (index == 1)
            assert ("private-b" in text) == (index == 3)
    with store._connect() as db:
        assert list(db.iterdump()) == before
    assert main._last_screen_diagnostics == {"diagnostics_invocation_id": "unchanged", "input": 5550}


@pytest.mark.parametrize("horizon", [1, 3, 5, 10])
def test_supported_horizons_pass_exact_origin_and_window(tmp_path, horizon):
    main, _core, store = _runtime(tmp_path)
    calls = []
    def reader(*args):
        calls.append(args)
        return []
    store.recommendation_performance = reader
    asyncio.run(_collect(main.strategy_performance(_event("origin-a"), horizon)))
    assert calls == [(horizon, "origin-a")]


def test_review_renders_all_statuses_without_inventing_returns(tmp_path):
    main, _core, store = _runtime(tmp_path)
    rows = [
        {"status": status, "code": str(index), "recommended_date": "2026-01-02",
         "return_pct": 12, "max_gain_pct": 15, "max_drawdown_pct": -2}
        for index, status in enumerate(("complete", "unknown_order", "pending", "unknown", None))
    ]
    calls = []
    def reader(*args):
        calls.append(args)
        return rows
    store.recommendation_reviews = reader
    text = asyncio.run(_collect(main.recommendation_review(_event("a"), 999)))[0]
    assert calls == [("a", 100)]
    assert text.count("收益+12.00%") == 1
    assert text.count("pending") == 2 and "unknown：" in text
    assert "顺序未知" in text and "区间高+15.00%" in text and "回撤-2.00%" in text


def test_persist_bars_limits_to_tracked_code_date_and_explicit_evidence(tmp_path):
    main, _core, store = _runtime(tmp_path)
    _record(store, "tracked")
    valid = {
        "trade_date": "2026-01-05", "open": 10, "high": 11, "low": 9.5,
        "close": 10.5, "volume": 100, "amount": 1000, "price_basis": "unadjusted",
        "corporate_action_factor": 1,
        "corporate_action_evidence": "tushare:adj_factor:2026-01-05:600000.SH",
    }
    result = {"bars": {
        "600000": [None, valid, {**valid, "trade_date": "2026-01-06"},
                   {**valid, "corporate_action_factor": None},
                   {**valid, "corporate_action_evidence": ""}],
        "600001": [valid],
    }}
    assert asyncio.run(main._persist_recommendation_daily_bars(result, "2026-01-05")) == 1
    with store._connect() as db:
        rows = db.execute("SELECT code,trade_date,corporate_action_factor FROM daily_bars").fetchall()
    assert [tuple(row) for row in rows] == [("600000", "2026-01-05", 1)]
    assert asyncio.run(main._persist_recommendation_daily_bars({"bars": []}, "2026-01-05")) == 0
    main.store = SimpleNamespace()
    assert asyncio.run(main._persist_recommendation_daily_bars(result, "2026-01-05")) == 0


def test_concurrent_snapshot_producers_keep_one_immutable_valid_record(tmp_path):
    _main_obj, core, store = _runtime(tmp_path)
    start = Barrier(2)
    def produce(price):
        candidate = core.Candidate(core.Quote("600099", "fixture", price), 10, [], price_plan=_validated_plan())
        start.wait(timeout=5)
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            store._save_recommendations_in_tx(db, "same-run", "2026-09-09", "fixture", [candidate])
            return db.execute("SELECT candidate_price FROM recommendation_records").fetchone()[0]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(produce, price) for price in (10, 11)]
        observed = [future.result(timeout=10) for future in futures]
    assert observed[0] == observed[1] and observed[0] in (10, 11)
    with store._connect() as db:
        rows = db.execute("SELECT plan_status,prediction_json FROM recommendation_records").fetchall()
    assert len(rows) == 1 and rows[0]["plan_status"] == "validated"
    assert json.loads(rows[0]["prediction_json"])["status"] == "available"


def test_snapshot_rejects_valid_plan_from_another_recommendation_date(tmp_path):
    _main_obj, core, store = _runtime(tmp_path)
    candidate = core.Candidate(core.Quote("600099", "fixture", 10), 10, [], price_plan=_validated_plan())
    with store._connect() as db:
        store._save_recommendations_in_tx(db, "stale-run", "2026-09-10", "fixture", [candidate])
        row = db.execute("SELECT plan_status,prediction_json FROM recommendation_records").fetchone()
    assert row["plan_status"] == "unknown"
    assert json.loads(row["prediction_json"])["status"] == "insufficient_data"
