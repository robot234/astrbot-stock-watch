"""Synthetic end-to-end checks for the versioned B paper chain."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import pytest

from test_v0160_risk_qualification import (
    _eligible_paper_pick, _observe, _seed_forward_action_window, StockStore, Dashboard,
    forward, review_persisted, synthetic_formal_source_license,
)

pytestmark = pytest.mark.usefixtures("synthetic_formal_source_license")


def _filled(tmp_path, monkeypatch, *, quote_price=10.06):
    store, qualified, common = _eligible_paper_pick(tmp_path, monkeypatch)
    common = {**common, "quote_price": quote_price}
    first = _observe(store, **common, bar_start="2026-09-25T09:35:00+08:00",
                     quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    assert first["state"] == "pending"
    second = _observe(store, **common, bar_start="2026-09-25T09:36:00+08:00",
                      quote_at="2026-09-25T09:37:10+08:00", now="2026-09-25T09:37:11+08:00")
    return store, qualified, common, second


def test_manual_window_and_decimal_examples():
    assert forward.in_entry_window("2026-09-25T01:35:00+00:00", "A")
    assert forward.in_entry_window("2026-09-25T10:00:00+08:00", "A")
    assert not forward.in_entry_window("2026-09-25T10:00:00.000001+08:00", "A")
    assert forward.in_entry_window("2026-09-25T14:57:00+08:00", "B")
    assert not forward.in_entry_window("2026-09-25T14:57:00.000001+08:00", "B")
    assert not forward.in_entry_window("2026-09-25T12:00:00+08:00", "B")
    assert forward.close_mark(11, quantity=100, total_cost_cny="1005.01") == Decimal("9.451647")
    assert forward.close_mark(9, quantity=100, total_cost_cny="1005.01") == Decimal("-10.448652")
    assert forward.money(Decimal("0.015")) == Decimal("0.02")
    terms = forward.entry_terms("10.00", capacity=Decimal("1010"))
    assert terms["quantity"] == 100 and terms["filled_price"] == Decimal("10.0100")
    assert terms["commission_cny"] == Decimal("5.00") and terms["transfer_fee_cny"] == Decimal("0.01")
    assert terms["total_cost_cny"] == Decimal("1006.0100")
    assert forward.b_limit_price("10") == Decimal("10.0701")
    assert forward.entry_terms("10.06")["filled_price"] == forward.b_limit_price("10")
    assert forward.entry_terms("10.07")["filled_price"] > forward.b_limit_price("10")


def test_missing_execution_and_risk_do_not_create_entry(tmp_path, monkeypatch):
    store, qualified, common = _eligible_paper_pick(tmp_path, monkeypatch)
    missing = store.observe_paper_completed_bar(
        **common, bar_start="2026-09-25T09:35:00+08:00",
        quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    assert missing == {"state": "unknown", "reason": "paper_source_evidence_unverified"}
    unclear = _observe(store, **{**common, "limit_up": None},
        bar_start="2026-09-25T09:35:00+08:00",
        quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    assert unclear["state"] == "unknown"
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_simulated_entries").fetchone()[0] == 0
    web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 25, 2, tzinfo=timezone.utc)).query("candidates")["data"]["research"]["primary"][0]
    assert web["arm_A"]["state"] == "unknown" and web["paper_status"] == "not_entered"


def test_rehashed_incomplete_minute_evidence_cannot_confirm(tmp_path, monkeypatch):
    import hashlib
    import json
    store, qualified, common = _eligible_paper_pick(tmp_path, monkeypatch)
    start = "2026-09-25T09:35:00+08:00"
    quote_at = "2026-09-25T09:36:10+08:00"
    now = "2026-09-25T09:36:11+08:00"
    bar = forward.synthetic_bar_evidence(
        bar_start=start, bar_close=common["bar_close"], received_at="2026-09-25T09:36:00+08:00")
    bar["bar_high"] = "9.00"
    content = {key: value for key, value in bar.items() if key != "sha256"}
    bar["sha256"] = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    execution = forward.synthetic_execution_evidence(
        record_id=qualified["record_id"], quote_at=quote_at,
        quote_price=common["quote_price"], received_at=now)
    rejected = store.observe_paper_completed_bar(
        **common, bar_start=start, quote_at=quote_at, now=now,
        bar_evidence=bar, execution_evidence=execution)
    assert rejected == {"state": "unknown", "reason": "paper_source_evidence_unverified"}
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_bar_observations").fetchone()[0] == 0


def test_b_limit_unfilled_does_not_create_cost_terms(tmp_path, monkeypatch):
    for label, price in (("above", 10.20), ("below", 9.90), ("slippage_exceeds", 10.07)):
        store, _, _, result = _filled(tmp_path / label, monkeypatch, quote_price=price)
        assert result["fill_status"] == "unfilled"
        with store._connect() as db:
            assert db.execute("SELECT COUNT(*) FROM paper_entry_terms").fetchone()[0] == 0
        if label == "slippage_exceeds":
            reviewed = review_persisted(tmp_path / label / "risk.sqlite3", ["2026-09-25"], as_of="2026-09-25")
            assert reviewed["records"][0]["marks"][1]["status"] == "unfilled"
            web = Dashboard(tmp_path / label / "risk.sqlite3",
                            now=lambda: datetime(2026, 9, 25, 2, tzinfo=timezone.utc))
            item = web.query("candidates")["data"]["research"]["primary"][0]
            assert item["fill_status"] == "unfilled"
            assert all(mark["status"] == "unfilled" and mark["return_pct"] is None
                       for mark in item["valuation"].values())


def test_prior_over_limit_fill_remains_recorded_but_has_no_numeric_mark(tmp_path, monkeypatch):
    store, qualified, _, fill = _filled(tmp_path, monkeypatch)
    assert fill["fill_status"] == "simulated_fill"
    _seed_forward_action_window(store, "2026-09-28")
    with store._connect() as db:
        db.execute("INSERT INTO daily_bars(code,trade_date,open,high,low,close,source,fetched_at,price_basis,corporate_action_factor,corporate_action_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   ("600000", "2026-09-28", 11, 11, 11, 11, "synthetic_fixture",
                    "2026-09-28T16:00:00+08:00", "unadjusted", 1, "fixture"))
    sessions = ["2026-09-25", "2026-09-28"]
    assert review_persisted(tmp_path / "risk.sqlite3", sessions, as_of="2026-09-28")["records"][0]["marks"][1]["status"] == "complete"
    with store._connect() as db:
        old = db.execute("SELECT quote_at FROM paper_entry_terms WHERE record_id=?", (qualified["record_id"],)).fetchone()
        evidence = forward.synthetic_execution_evidence(
            record_id=qualified["record_id"], quote_at=old["quote_at"], quote_price="10.07",
            received_at="2026-09-25T09:37:11+08:00")
        terms = forward.entry_terms("10.07")
        import json
        db.execute("UPDATE paper_simulated_entries SET entry_price=? WHERE record_id=?",
                   (str(terms["filled_price"]), qualified["record_id"]))
        db.execute("UPDATE paper_entry_terms SET entry_notional_cny=?,commission_cny=?,transfer_fee_cny=?,entry_fees_cny=?,total_cost_cny=?,execution_evidence_hash=?,execution_evidence_json=? WHERE record_id=?",
                   (str(terms["entry_notional_cny"]), str(terms["commission_cny"]),
                    str(terms["transfer_fee_cny"]), str(terms["entry_fees_cny"]),
                    str(terms["total_cost_cny"]), evidence["sha256"],
                    json.dumps(evidence), qualified["record_id"]))
    reviewed = review_persisted(tmp_path / "risk.sqlite3", sessions, as_of="2026-09-28")["records"][0]
    assert reviewed["fill_status"] == "simulated_fill"
    assert reviewed["marks"][1]["status"] == "unknown" and reviewed["marks"][1]["return_pct"] is None
    web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 28, 10, tzinfo=timezone.utc))
    item = web.query("candidates")["data"]["research"]["primary"][0]
    assert item["paper_status"] == "unknown" and item["entry_fees_cny"] is None


def test_new_rule_denominator_includes_unentered_across_batches_and_legacy_stays_separate(tmp_path, monkeypatch):
    from test_v0160_risk_qualification import storage_module
    extra = [{"code": "600001", "name": "Unobserved", "score": 39, "close": 10.0,
              "amount": 1e8, "risk_level": "unknown", "risk_flags": [], "reasons": []}]
    store, _, common = _eligible_paper_pick(tmp_path, monkeypatch, extra_primary=extra)
    _observe(store, **common, bar_start="2026-09-25T09:35:00+08:00",
             quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    fill = _observe(store, **common, bar_start="2026-09-25T09:36:00+08:00",
                    quote_at="2026-09-25T09:37:10+08:00", now="2026-09-25T09:37:11+08:00")
    assert fill["fill_status"] == "simulated_fill"
    first = review_persisted(tmp_path / "risk.sqlite3", ["2026-09-25"], as_of="2026-09-25")
    assert first["summary"]["recommended"] == 2
    assert first["summary"]["horizons"][1]["mark_to_close"]["original_candidate_count"] == 2
    assert {r["accounting_version"] for r in first["records"]} == {forward.ACCOUNTING_VERSION}
    assert next(r for r in first["records"] if r["record_id"].endswith(":600001"))["state"] == "unknown"
    web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 25, 2, tzinfo=timezone.utc))
    items = web.query("candidates")["data"]["research"]["primary"]
    assert len(items) == 2 and {item["accounting_version"] for item in items} == {forward.ACCOUNTING_VERSION}
    assert next(item for item in items if item["code"] == "600001")["paper_status"] == "not_entered"

    snapshot = {"trade_date": "2026-09-25", "batch_id": "batch-b", "source": "tushare",
                "basis": "unadjusted", "published_at": "2026-09-25T08:30:00+00:00"}
    later = [{"code": "600002", "name": "Later", "score": 38, "close": 10.0,
              "amount": 1e8, "risk_level": "unknown", "risk_flags": [], "reasons": []}]
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "_utcnow_naive", lambda: datetime(2026, 9, 25, 9, 30))
        store.save_research_pools(snapshot, later, [], {})
    with store._connect() as db:
        db.execute("INSERT INTO research_pool_runs(run_id,trade_date,batch_id,source,basis,published_at,frozen_at,diagnostics,status) VALUES(?,?,?,?,?,?,?,?,?)",
                   ("research:legacy", "2026-09-23", "legacy", "synthetic_fixture", "unadjusted",
                    "2026-09-23T08:30:00+00:00", "2026-09-23T09:30:00+00:00", "{}", "research_only"))
        db.execute("INSERT INTO research_pool_picks(run_id,pool,code,name,rank,score,close,amount,risk_level,risk_flags,reasons) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   ("research:legacy", "primary", "600003", "Legacy", 1, 37, 10.0, 1e8, "unknown", "[]", "[]"))
    mixed = review_persisted(tmp_path / "risk.sqlite3", ["2026-09-25"], as_of="2026-09-25")
    assert mixed["summary"]["recommended"] == 4
    assert mixed["summary"]["horizons"][1]["mark_to_close"]["original_candidate_count"] == 3
    assert next(r for r in mixed["records"] if r["record_id"].endswith(":600003"))["accounting_version"] == "legacy-percent-fee-v1"
    latest = web.query("candidates")["data"]["research"]
    assert latest["batch_id"] == "batch-b" and latest["primary"][0]["accounting_version"] == forward.ACCOUNTING_VERSION
    assert any(r["record_id"].endswith(":600000") for r in latest["paper_history"])


def test_holding_period_evidence_and_late_conflict(tmp_path, monkeypatch):
    store, qualified, _, result = _filled(tmp_path, monkeypatch)
    assert result["fill_status"] == "simulated_fill"
    _seed_forward_action_window(store, "2026-09-28")
    with store._connect() as db:
        db.execute("INSERT INTO daily_bars(code,trade_date,open,high,low,close,source,fetched_at,price_basis,corporate_action_factor,corporate_action_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   ("600000", "2026-09-28", 11, 11, 11, 11, "synthetic", "2026-09-28T15:30:00+08:00",
                    "unadjusted", 1.0, "fixture"))
    sessions = ["2026-09-25", "2026-09-28"]
    normal = review_persisted(tmp_path / "risk.sqlite3", sessions, as_of="2026-09-28")["records"][0]
    assert normal["marks"][1]["status"] == "complete"
    assert normal["marks"][1]["return_kind"] == "mark_to_close" and normal["marks"][1]["net_return_pct"] is None
    assert normal["marks"][3]["status"] == "pending" and normal["marks"][3]["return_pct"] is None
    with store._connect() as db:
        db.execute("DELETE FROM paper_no_action_evidence WHERE trade_date='2026-09-25'")
    missing = review_persisted(tmp_path / "risk.sqlite3", sessions, as_of="2026-09-28")["records"][0]
    assert missing["marks"][1]["status"] == "unknown" and missing["marks"][1]["return_pct"] is None
    store.record_paper_action_evidence("600000", "2026-09-25", "none",
        observed_at="2026-09-25T15:30:00+08:00", received_at="2026-09-25T15:31:00+08:00")
    store.record_paper_action_evidence("600000", "2026-09-28", "action",
        observed_at="2026-09-28T15:35:00+08:00", received_at="2026-09-28T15:36:00+08:00")
    conflict = review_persisted(tmp_path / "risk.sqlite3", sessions, as_of="2026-09-28")["records"][0]
    assert conflict["marks"][1]["status"] == "unknown"
    prior = review_persisted(tmp_path / "risk.sqlite3", sessions, as_of="2026-09-28",
        cutoff_at="2026-09-28T07:34:00+00:00")["records"][0]
    assert prior["marks"][1]["status"] == "complete"
    with store._connect() as db:
        db.execute("UPDATE paper_bar_observations SET evidence_sha256='bad' WHERE bar_start=?",
                   ("2026-09-25T01:35:00+00:00",))
    tampered = review_persisted(tmp_path / "risk.sqlite3", sessions, as_of="2026-09-28")["records"][0]
    assert tampered["marks"][1]["status"] == "unknown"
    assert tampered["fill_status"] == "simulated_fill"  # Keep historical event, invalidate numeric mark.
    web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 28, 10, tzinfo=timezone.utc))
    item = web.query("candidates")["data"]["research"]["primary"][0]
    assert item["paper_status"] == "unknown" and item["entry_fees_cny"] is None


def test_B_cutoff_inclusive_utc_and_one_microsecond_late(tmp_path, monkeypatch):
    store, qualified, common = _eligible_paper_pick(tmp_path / "on_time", monkeypatch)
    first = _observe(store, **common, bar_start="2026-09-25T14:54:00+08:00",
                     quote_at="2026-09-25T14:55:10+08:00", now="2026-09-25T14:55:11+08:00")
    assert first["state"] == "pending"
    at_end = _observe(store, **common, bar_start="2026-09-25T14:55:00+08:00",
                      quote_at="2026-09-25T14:56:59+08:00", now="2026-09-25T06:57:00+00:00")
    assert at_end["fill_status"] == "simulated_fill"
    late_store, _, late_common = _eligible_paper_pick(tmp_path / "too_late", monkeypatch)
    _observe(late_store, **late_common, bar_start="2026-09-25T14:54:00+08:00",
             quote_at="2026-09-25T14:55:10+08:00", now="2026-09-25T14:55:11+08:00")
    late = _observe(late_store, **late_common, bar_start="2026-09-25T14:55:00+08:00",
                    quote_at="2026-09-25T14:56:59+08:00", now="2026-09-25T14:57:00.000001+08:00")
    assert late["state"] == "unknown" and late["reason"] == "paper_entry_window_closed"
    with late_store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_simulated_entries").fetchone()[0] == 0


def test_empty_freeze_stays_empty_and_A_not_inferred(tmp_path, monkeypatch):
    from test_v0160_risk_qualification import _store, storage_module
    store = _store(tmp_path)
    snapshot = {"trade_date": "2026-09-24", "batch_id": "batch-a", "source": "tushare",
                "basis": "unadjusted", "published_at": "2026-09-24T08:30:00+00:00"}
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "_utcnow_naive", lambda: datetime(2026, 9, 24, 9, 30))
        store.save_research_pools(snapshot, [], [], {})
    web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 25, 2, tzinfo=timezone.utc))
    data = web.query("candidates")["data"]["research"]
    assert data["primary"] == [] and data["radar"] == []
    assert review_persisted(tmp_path / "risk.sqlite3", ["2026-09-25"], as_of="2026-09-25")["records"] == []


def test_legacy_percent_fee_record_remains_separate(tmp_path, monkeypatch):
    store, qualified, common = _eligible_paper_pick(tmp_path, monkeypatch)
    with store._connect() as db:
        db.execute("INSERT INTO paper_simulated_entries(record_id,qualification_version,first_bar_at,second_bar_at,confirmed_at,fill_status,entry_date,entry_price,round_trip_fee_pct,entry_slippage_pct,exit_slippage_pct,reason,source_quality,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (qualified["record_id"], qualified["version"], "2026-09-25T01:35:00+00:00",
             "2026-09-25T01:36:00+00:00", "2026-09-25T01:37:11+00:00", "simulated_fill",
             "2026-09-25", 10.10, 0.30, 0.10, 0.10, "legacy_fixture", "validated_minute",
             "2026-09-25T01:37:11+00:00"))
        for day in ("2026-09-25", "2026-09-28"):
            db.execute("INSERT INTO corporate_action_factors(code,trade_date,adj_factor,source,evidence,fetched_at,observed_at,response_sha256) VALUES(?,?,?,?,?,?,?,?)",
                ("600000", day, 1, "tushare_adj_factor", "fixture", day + "T16:00:00+08:00",
                 day + "T16:00:00+08:00", "fixture-digest"))
        db.execute("INSERT INTO daily_bars(code,trade_date,open,high,low,close,source,fetched_at,price_basis,corporate_action_factor,corporate_action_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            ("600000", "2026-09-28", 10.3, 10.3, 10.3, 10.3, "fixture",
             "2026-09-28T16:00:00+08:00", "unadjusted", 1, "fixture"))
    reviewed = review_persisted(tmp_path / "risk.sqlite3", ["2026-09-25", "2026-09-28"], as_of="2026-09-28")["records"][0]
    assert reviewed["accounting_version"] == "legacy-percent-fee-v1"
    assert reviewed["marks"][1]["return_kind"] == "legacy_percent_fee"
    assert reviewed["marks"][1]["return_pct"] is None
    assert reviewed["marks"][1]["net_return_pct"] is not None
    web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 28, 10, tzinfo=timezone.utc))
    item = web.query("candidates")["data"]["research"]["primary"][0]
    assert item["accounting_version"] == "legacy-percent-fee-v1" and item.get("valuation") is None


def test_new_freeze_does_not_hide_older_simulated_mark(tmp_path, monkeypatch):
    from test_v0160_risk_qualification import storage_module
    store, qualified, _, fill = _filled(tmp_path, monkeypatch)
    assert fill["fill_status"] == "simulated_fill"
    _seed_forward_action_window(store, "2026-09-28")
    with store._connect() as db:
        db.execute("INSERT INTO daily_bars(code,trade_date,open,high,low,close,source,fetched_at,price_basis,corporate_action_factor,corporate_action_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            ("600000", "2026-09-28", 11, 11, 11, 11, "synthetic_fixture",
             "2026-09-28T16:00:00+08:00", "unadjusted", 1, "fixture"))
    snapshot = {"trade_date": "2026-09-25", "batch_id": "batch-b", "source": "tushare",
                "basis": "unadjusted", "published_at": "2026-09-25T08:30:00+00:00"}
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "_utcnow_naive", lambda: datetime(2026, 9, 25, 9, 30))
        store.save_research_pools(snapshot, [], [], {})
    web = Dashboard(tmp_path / "risk.sqlite3", now=lambda: datetime(2026, 9, 28, 10, tzinfo=timezone.utc))
    research = web.query("candidates")["data"]["research"]
    assert research["batch_id"] == "batch-b" and research["primary"] == []
    assert len(research["paper_history"]) == 1
    historical = research["paper_history"][0]
    assert historical["record_id"] == qualified["record_id"]
    assert historical["marks"][1]["status"] == "complete"
    assert historical["accounting_version"] == forward.ACCOUNTING_VERSION
