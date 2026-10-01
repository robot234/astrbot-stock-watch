from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import date, timedelta
import json
import sqlite3
from types import SimpleNamespace

import pytest

from audit_point_in_time import audit_dataset, WalkForwardInputError
from audit_runtime_evidence import audit_database, source_capabilities
from test_v0138_automatic_close import _imports, _delivery_main, _save_publication
from test_v0139_intraday_m2 import _main, _queued
from test_v0140_recommendation_tracking import _record


def _dataset():
    start = date(2026, 1, 1)
    days = [(start + timedelta(days=i)).isoformat() for i in range(90)]
    sessions = [d for d in days if date.fromisoformat(d).weekday() < 5]
    candidates = []
    for day in (sessions[0], sessions[5], sessions[10], sessions[20], sessions[21]):
        for rank, code in enumerate(("600000", "600001"), 1):
            candidates.append({
                "date": day, "code": code, "rank": rank, "market_regime": "neutral",
                "decision_at": day + "T16:01:00+08:00",
                "recorded_at": day + "T16:00:00+08:00",
                "features_available_at": day + "T15:30:00+08:00",
                "source_ref": "fixture:" + day + ":" + code,
            })
    return {
        "manifest": {
            "kind": "fixture", "source": "deterministic_test", "universe_provenance": "fixture",
            "ranking_version": "fixture-v1", "benchmark_code": "000300.SH",
            "folds": [{"train_start": sessions[0], "train_end": sessions[10],
                       "test_start": sessions[20], "test_end": sessions[21]}],
        },
        "candidates": candidates,
        "calendar": [{"date": d, "is_open": d in sessions, "verified": True,
                      "source": "fixture", "available_at": days[0] + "T00:00:00+08:00"} for d in days],
        "universes": [{"date": d, "codes": ["600000", "600001"], "complete": True,
                       "source": "fixture", "available_at": d + "T09:00:00+08:00"} for d in sessions],
        "bars": [{"date": d, "code": code, "close": 100 + i * direction,
                  "price_basis": "unadjusted", "factor": 1, "factor_evidence": "fixture",
                  "source": "fixture", "available_at": d + "T15:30:00+08:00"}
                 for i, d in enumerate(sessions) for code, direction in (("600000", 1), ("600001", -0.2))],
        "benchmark": [{"date": d, "code": "000300.SH", "close": 100 + i * 0.1,
                       "price_basis": "index_close", "source": "fixture",
                       "available_at": d + "T15:30:00+08:00"} for i, d in enumerate(sessions)],
    }


def test_audited_folds_purge_labels_and_report_baseline_strata_without_quality_claims():
    data = _dataset()
    result = audit_dataset(data, as_of="2026-03-31")
    assert result["status"] == "insufficient_data"
    assert {"not_attested_real_data", "decision_history_too_short"} <= set(result["blockers"])
    assert result["folds"][0]["purged_training_labels"] > 0
    test = result["folds"][0]["test"]["5"]
    assert test["price_coverage"] == test["benchmark_coverage"] == 1
    assert test["mean_excess_pct"] is not None
    assert test["mean_daily_rank_correlation"] == -1
    assert result["market_regimes"]["neutral"]["5"] == result["rank_bands"]["top10"]["5"]
    assert result["production_tuning_allowed"] is result["strategy_quality_established"] is False
    assert result["input_sha256"] == audit_dataset(deepcopy(data), as_of="2026-03-31")["input_sha256"]


@pytest.mark.parametrize("field", ["recorded_at", "features_available_at"])
def test_audited_ranking_rejects_hindsight_availability(field):
    data = _dataset()
    data["candidates"][0][field] = "2026-03-31T16:00:00+08:00"
    with pytest.raises(WalkForwardInputError, match="post-decision"):
        audit_dataset(data, as_of="2026-03-31")


def test_audited_inputs_reject_universe_leakage_outcome_columns_and_overlap():
    for mutation, match in (
        (lambda d: d["universes"][0].update(available_at="2026-03-31T00:00:00+08:00"), "universe"),
        (lambda d: d["candidates"][0].update(return_pct=99), "input contract"),
        (lambda d: d["manifest"]["folds"][0].update(test_start="2026-01-01"), "folds"),
    ):
        data = _dataset()
        mutation(data)
        with pytest.raises(WalkForwardInputError, match=match):
            audit_dataset(data, as_of="2026-03-31")


def test_audited_missing_session_does_not_shift_to_next_bar_and_factor_change_is_unknown():
    data = _dataset()
    data["calendar"] = [r for r in data["calendar"] if r["date"] != "2026-01-02"]
    result = audit_dataset(data, as_of="2026-03-31")
    row = next(r for r in result["records"] if r["date"] == "2026-01-01" and r["horizon"] == 1)
    assert row["status"] == "pending" and row["reason"] == "calendar_unverified"
    assert row["return_pct"] is None
    data = _dataset()
    next(r for r in data["bars"] if r["date"] == "2026-01-02" and r["code"] == "600000")["factor"] = 2
    row = next(r for r in audit_dataset(data, as_of="2026-03-31")["records"]
               if r["date"] == "2026-01-01" and r["code"] == "600000" and r["horizon"] == 1)
    assert row["status"] == "unknown" and row["return_pct"] is None


def test_audited_benchmark_never_substitutes_other_index():
    data = _dataset()
    data["manifest"]["benchmark_code"] = "000001.SH"
    result = audit_dataset(data, as_of="2026-03-31")
    assert all(r["excess_pct"] is None for r in result["records"])
    assert result["folds"][0]["test"]["1"]["benchmark_coverage"] == 0
    assert "T+1:benchmark_coverage_insufficient" in result["blockers"]


def test_readonly_audit_reports_maturity_separately_from_stored_unknown(tmp_path):
    _, _, _, Store = _imports()
    path = tmp_path / "readonly.sqlite3"
    store = Store(path)
    _record(store, "real-shape", comparable="unknown")
    with store._connect() as db:
        db.execute("UPDATE recommendation_records SET recommended_date='2026-09-11',created_at='2026-09-11T08:00:00'")
        for horizon in (1, 3, 5, 10):
            store._upsert_recommendation_outcome(db, "real-shape", horizon, "2026-09-11", "unknown", reason="corporate_action_evidence_missing")
        db.execute("UPDATE recommendation_outcomes SET updated_at='2026-09-11T08:01:00'")
    store.save_calendar("2026-09-12", False, "fixture", ttl_seconds=999999)
    with store._connect() as db:
        before = list(db.iterdump())
    report = audit_database(path, as_of="2026-09-12")
    group = report["recommendations"]["groups"][0]
    assert all(r["maturity"] == "pending" and r["stored_status_counts"] == {"unknown": 1}
               for r in group["horizons"].values())
    assert report["market_inputs"]["index_divergence"]["status"] == "unknown"
    assert report["market_inputs"]["turnover_change"]["status"] == "unknown"
    assert report["intraday"]["status"] == "pending"
    with store._connect() as db:
        assert list(db.iterdump()) == before
    with pytest.raises(FileNotFoundError):
        audit_database(tmp_path / "missing.sqlite3", as_of="2026-09-12")
    assert not (tmp_path / "missing.sqlite3").exists()


def test_source_audit_records_actual_signature_without_importing_runtime(tmp_path):
    path = tmp_path / "astrbot/core/star/context.py"
    path.parent.mkdir(parents=True)
    path.write_text("raise RuntimeError('must not import')\nclass Context:\n    async def send_message(self, session, message_chain):\n        return False\n")
    evidence = source_capabilities(tmp_path)["astrbot/core/star/context.py"]
    assert evidence["methods"] == {"send_message": ["self", "session", "message_chain"]}
    assert len(evidence["sha256"]) == 64


@pytest.mark.parametrize("kind", ["automatic_close", "intraday"])
def test_false_send_result_is_unknown_and_capability_evidence_survives_reload(tmp_path, kind):
    Main, _, core, Store = _imports()
    path = tmp_path / "delivery.sqlite3"
    store = Store(path)
    store.set_subscription("origin-a", True)
    calls = []
    class Context:
        async def send_message(self, origin, payload):
            calls.append(origin)
            with store._connect() as db:
                assert db.execute("SELECT COUNT(*) FROM delivery_capability_evidence").fetchone()[0] == 1
            return False
    if kind == "automatic_close":
        _save_publication(store, core, origins=("origin-a",))
        row = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
        main = _delivery_main(Main, store, Context())
        dispatch = main._dispatch_automatic_delivery
    else:
        row = _queued(store)
        main = _main(Main, store, Context())
        dispatch = main._dispatch_intraday_delivery
    assert asyncio.run(dispatch(row)) == "unknown_delivery"
    assert asyncio.run(dispatch(row)) == "unknown_delivery"
    assert calls == ["origin-a"]
    reopened = Store(path)
    with reopened._connect() as db:
        saved = db.execute("SELECT * FROM delivery_capability_evidence").fetchone()
    evidence = json.loads(saved["evidence_json"])
    assert saved["delivery_kind"] == kind
    assert evidence["idempotency"] == evidence["receipt_lookup"] == "not_exposed"
    assert evidence["unknown_delivery_retry_allowed"] is False


def test_capability_advertisement_is_not_proof_and_defaults_are_not_recorded():
    _imports()
    from astrbot_stock_watch.delivery_capabilities import delivery_capabilities
    class Context:
        async def send_message(self, origin, payload, idempotency_key="secret-default"):
            pytest.fail("capability inspection must not send")
        async def get_delivery_receipt(self, key):
            pytest.fail("capability inspection must not query")
    evidence = delivery_capabilities(Context())
    assert evidence["idempotency"] == evidence["receipt_lookup"] == "unverified"
    assert "secret-default" not in json.dumps(evidence)
    assert delivery_capabilities(SimpleNamespace())["sender_available"] is False


def test_capability_record_failure_stops_before_send_and_wrong_fence_cannot_record(tmp_path):
    Main, _, core, Store = _imports()
    store = Store(tmp_path / "lease.sqlite3")
    store.set_subscription("origin-a", True)
    _save_publication(store, core, origins=("origin-a",))
    row = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    class Context:
        async def send_message(self, origin, payload):
            pytest.fail("must not send without durable capability evidence")
    main = _delivery_main(Main, store, Context())
    assert not store.record_delivery_capabilities("automatic_close", row["delivery_id"], "wrong", 999, {})
    def fail(*args):
        raise sqlite3.OperationalError("fixture disk error")
    store.record_delivery_capabilities = fail
    assert asyncio.run(main._dispatch_automatic_delivery(row)) == "failed"
    assert store.automatic_close_delivery_summary()["failed"] == 1
