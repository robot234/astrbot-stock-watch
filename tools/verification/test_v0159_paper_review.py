"""Synthetic end-to-end identity and day-zero review checks."""
from datetime import datetime, timezone
import sqlite3

from paper_review import review
from webapp.data import Dashboard
from webapp.demo import create_demo


def test_freeze_identity_through_web_and_simulated_day_zero(tmp_path):
    db, settings = tmp_path / "synthetic.sqlite3", tmp_path / "settings.json"
    create_demo(db, settings)
    with sqlite3.connect(db) as conn:
        conn.executescript("""
        CREATE TABLE research_pool_runs(run_id TEXT,trade_date TEXT,batch_id TEXT,source TEXT,basis TEXT,
            published_at TEXT,frozen_at TEXT,diagnostics TEXT,status TEXT);
        CREATE TABLE research_pool_picks(run_id TEXT,pool TEXT,code TEXT,name TEXT,rank INTEGER,
            score INTEGER,close REAL,amount REAL,risk_level TEXT,risk_flags TEXT,reasons TEXT);
        """)
        conn.execute("INSERT INTO research_pool_runs VALUES(?,?,?,?,?,?,?,?,?)",
                     ("research:batch-1", "2026-09-10", "batch-1", "fixture", "unadjusted",
                      "2026-09-10T16:00:00+08:00", "2026-09-10T18:15:00+08:00", "{}", "research_only"))
        conn.execute("INSERT INTO research_pool_picks VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                     ("research:batch-1", "primary", "300001", "合成甲", 1, 40, 10, 1e8, "unknown", "[]", '["技术观察"]'))
    dashboard = Dashboard(db, settings=settings, origin="demo",
                          now=lambda: datetime(2026, 9, 18, 8, tzinfo=timezone.utc))
    candidate_data = dashboard.query("candidates")["data"]
    item = candidate_data["research"]["primary"][0]
    assert item["eligibility"] == "research_only" and item["entry_range"] is None
    assert "300001" not in [formal["code"] for formal in candidate_data["items"]]
    sessions = ["2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"]
    result = review([
        {"record_id": item["record_id"], "state": "confirmed", "fill_status": "simulated_fill",
         "entry_date": "2026-09-11", "entry_price": 10.2, "round_trip_fee_pct": 0.3,
         "bars": [{"date": day, "close": close} for day, close in zip(sessions[1:], [10.5, 10.0, 9.9, 10.8, 10.7])]},
        {"record_id": "research:batch-1:600002", "state": "not_triggered"},
        {"record_id": "research:batch-1:600003", "state": "confirmed", "fill_status": "unfilled"},
        {"record_id": "research:batch-1:600004", "state": "unknown"},
    ], sessions, as_of="2026-09-18")
    assert result["records"][0]["record_id"] == item["record_id"]
    assert result["records"][0]["marks"][1]["status"] == "complete"
    assert result["records"][0]["marks"][3]["technical_return_pct"] < 0
    assert result["records"][0]["marks"][3]["net_return_pct"] < result["records"][0]["marks"][3]["technical_return_pct"]
    assert result["summary"]["recommended"] == 4
    assert result["summary"]["triggered"] == 2
    assert result["summary"]["executable_simulated"] == 1
    assert result["summary"]["horizons"][3]["mature_evaluable"] == 1
    assert result["summary"]["horizons"][3]["all_sample_net_positive_rate"] == 0


def test_immaturity_missing_price_and_unknown_fill_remain_visible():
    samples = [{"record_id": "a", "state": "confirmed", "fill_status": "simulated_fill",
                "entry_date": "2026-09-11", "entry_price": 10, "round_trip_fee_pct": 0.2,
                "bars": [{"date": "2026-09-14", "close": 9}]},
               {"record_id": "b", "state": "confirmed", "fill_status": "unknown"}]
    result = review(samples, ["2026-09-11", "2026-09-14", "2026-09-15"], as_of="2026-09-15")
    assert result["records"][0]["marks"][1]["net_return_pct"] < 0
    assert result["records"][0]["marks"][3]["status"] == "pending"
    assert result["records"][0]["marks"][5]["status"] == "pending"
    assert result["records"][0]["marks"][1]["mark_date"] == "2026-09-14"
    assert result["records"][1]["marks"][1]["status"] == "unknown"


def test_conflicting_or_invalid_same_day_close_is_order_independent():
    sessions = ["2026-09-11", "2026-09-14"]
    base = {"record_id": "research:batch:300001", "state": "confirmed",
            "fill_status": "simulated_fill", "entry_date": "2026-09-11",
            "entry_price": 10, "round_trip_fee_pct": 0.2}
    for closes, reason in (([9, 11], "conflicting_close_observations"),
                           ([9, None], "invalid_close_observation"),
                           ([9, -1], "invalid_close_observation")):
        for ordered in (closes, list(reversed(closes))):
            bars = [{"date": "2026-09-14", "close": close} for close in ordered]
            result = review([{**base, "bars": bars}], sessions, as_of="2026-09-14")
            mark = result["records"][0]["marks"][1]
            assert mark["status"] == "unknown" and mark["reason"] == reason
            assert mark["technical_return_pct"] is None and mark["net_return_pct"] is None
            assert result["summary"]["horizons"][1]["mature_evaluable"] == 0
            assert result["summary"]["horizons"][1]["net_positive_rate_evaluable"] is None
    duplicate = review([{**base, "bars": [{"date": "2026-09-14", "close": 9}] * 2}],
                       sessions, as_of="2026-09-14")
    assert duplicate["records"][0]["marks"][1]["status"] == "complete"
