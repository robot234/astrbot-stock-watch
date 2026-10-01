"""Synthetic, read-only checks for the independent research pool projection."""
from datetime import datetime, timezone
import hashlib
import json
import sqlite3

from webapp.data import Dashboard
from webapp.demo import create_demo


NOW = datetime(2026, 9, 12, 8, tzinfo=timezone.utc)


def _dashboard(tmp_path):
    database, settings = tmp_path / "synthetic.sqlite3", tmp_path / "settings.json"
    create_demo(database, settings)
    return Dashboard(database, settings=settings, origin="demo", now=lambda: NOW)


def _add_research(database):
    with sqlite3.connect(database) as db:
        db.executescript("""
        CREATE TABLE research_pool_runs(run_id TEXT,trade_date TEXT,batch_id TEXT,source TEXT,basis TEXT,
            published_at TEXT,frozen_at TEXT,diagnostics TEXT,status TEXT);
        CREATE TABLE research_pool_picks(run_id TEXT,pool TEXT,code TEXT,name TEXT,rank INTEGER,
            score INTEGER,close REAL,amount REAL,risk_level TEXT,risk_flags TEXT,reasons TEXT);
        """)
        db.execute("INSERT INTO research_pool_runs VALUES(?,?,?,?,?,?,?,?,?)",
                   ("research:old", "2026-09-11", "old", "fixture", "unadjusted",
                    "2026-09-11T14:00:00+08:00", "2026-09-11T18:15:00+08:00", "{}", "research_only"))
        db.execute("INSERT INTO research_pool_runs VALUES(?,?,?,?,?,?,?,?,?)",
                   ("research:new", "2026-09-12", "new", "fixture", "unadjusted",
                    "2026-09-12T14:00:00+08:00", "2026-09-12T15:00:00+08:00", "{}", "research_only"))
        for run_id, pool, code in (("research:old", "primary", "600000"),
                                   ("research:new", "primary", "300001"),
                                   ("research:new", "radar", "600002")):
            db.execute("INSERT INTO research_pool_picks VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                       (run_id, pool, code, "合成样本", 1, 35, 12.5, 1e8,
                        "unknown", "[]", json.dumps(["synthetic reason"])))


def test_research_pools_keep_batch_identity_and_formal_separation(tmp_path):
    dashboard = _dashboard(tmp_path)
    _add_research(dashboard.database)
    before = hashlib.sha256(dashboard.database.read_bytes()).digest()
    data = dashboard.query("candidates")["data"]
    research = data["research"]
    assert research["run_id"] == "research:new"
    assert research["status"] == "research_only"
    assert [row["code"] for row in research["primary"]] == ["300001"]
    assert [row["code"] for row in research["radar"]] == ["600002"]
    assert all(row["run_id"] == "research:new" and row["entry_range"] is None
               and row["confirmation"] == "not_assessed"
               for row in research["primary"] + research["radar"])
    assert all(row["code"].startswith("DEMO") for row in data["items"])
    assert hashlib.sha256(dashboard.database.read_bytes()).digest() == before


def test_missing_and_old_research_data_degrade_without_formal_promotion(tmp_path):
    dashboard = _dashboard(tmp_path)
    assert dashboard.query("candidates")["data"]["research"]["status"] == "unavailable"
    _add_research(dashboard.database)
    with sqlite3.connect(dashboard.database) as db:
        db.execute("UPDATE research_pool_runs SET trade_date='2026-09-01'")
    assert dashboard.query("candidates")["data"]["research"]["status"] == "stale"
    with sqlite3.connect(dashboard.database) as db:
        db.execute("DELETE FROM research_pool_picks")
        db.execute("DELETE FROM research_pool_runs")
    assert dashboard.query("candidates")["data"]["research"]["status"] == "empty"
