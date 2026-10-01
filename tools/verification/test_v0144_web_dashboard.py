from datetime import datetime, timezone
import hashlib
import http.client
import json
from pathlib import Path
import sqlite3
import threading

import pytest

from webapp.data import Dashboard
from webapp.demo import create_demo
from webapp.server import create_server


NOW = datetime(2026, 9, 12, 3, tzinfo=timezone.utc)


@pytest.fixture
def dashboard(tmp_path):
    database, settings = tmp_path / "demo.sqlite3", tmp_path / "settings.json"
    create_demo(database, settings)
    return Dashboard(database, settings=settings, origin="demo", now=lambda: NOW)


def mutate(dashboard, sql, params=()):
    with sqlite3.connect(dashboard.database) as db:
        db.execute(sql, params)


@pytest.mark.parametrize("route", ["overview", "signals", "candidates", "stocks/DEMO01",
                                    "performance", "health", "settings"])
def test_views_are_read_only(dashboard, route):
    before = hashlib.sha256(dashboard.database.read_bytes()).digest()
    files = set(dashboard.database.parent.iterdir())
    response = dashboard.query(route)
    assert response["meta"]["status"] != "unavailable"
    assert response["meta"]["dataset_kind"] == "synthetic_demo"
    assert response["meta"]["read_only"] is True
    assert response["data"]
    assert hashlib.sha256(dashboard.database.read_bytes()).digest() == before
    assert set(dashboard.database.parent.iterdir()) == files
    with dashboard.snapshot() as snapshot:
        with pytest.raises(sqlite3.OperationalError):
            snapshot.db.execute("DELETE FROM screen_runs")


def test_missing_legacy_and_locked_database(tmp_path, dashboard):
    missing = tmp_path / "absent.sqlite3"
    assert Dashboard(missing).query("overview")["meta"]["reason"] == "database_missing"
    assert not missing.exists()
    legacy = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(legacy) as db:
        db.execute("CREATE TABLE screen_runs(run_id TEXT)")
    assert Dashboard(legacy).query("overview")["meta"]["status"] in {"partial", "unavailable"}
    db = sqlite3.connect(dashboard.database)
    try:
        db.execute("BEGIN EXCLUSIVE")
        assert dashboard.query("overview")["meta"]["status"] == "unavailable"
    finally:
        db.rollback()
        db.close()


def test_origin_and_settings_privacy(dashboard):
    mutate(dashboard, "UPDATE recommendation_records SET visibility='private',origin='other' WHERE code='DEMO01'")
    mutate(dashboard, "UPDATE intraday_event_outbox SET origin='other' WHERE code='DEMO01'")
    assert "DEMO01" not in [r["code"] for r in dashboard.query("performance")["data"]["records"]]
    assert "DEMO01" not in [r["code"] for r in dashboard.query("signals")["data"]["items"]]
    assert Dashboard(dashboard.database).query("signals")["data"]["items"] == []
    mutate(dashboard, "UPDATE recommendation_records SET origin='' WHERE code='DEMO01'")
    assert "DEMO01" not in [r["code"] for r in Dashboard(dashboard.database, now=lambda: NOW).query("performance")["data"]["records"]]
    dashboard.settings.write_text(json.dumps({"values": {
        "token": "TOP_SECRET", "origin": "TOP_SECRET", "min_score": 25,
        "market_comparison_benchmark": "https://TOP_SECRET"}}), encoding="utf-8")
    settings = dashboard.query("settings")
    assert "TOP_SECRET" not in json.dumps(settings)
    assert next(r for r in settings["data"]["items"] if r["key"] == "min_score")["effective"] == 25


def test_exact_signal_plan_and_future_quote(dashboard):
    first = dashboard.query("signals")["data"]["items"][0]
    assert first["attention_low"] is not None
    mutate(dashboard, "UPDATE intraday_event_outbox SET plan_version='wrong' WHERE code=?", (first["code"],))
    changed = dashboard.query("signals")["data"]["items"][0]
    assert changed["attention_low"] is None and changed["distance_pct"] is None
    mutate(dashboard, "UPDATE daily_quotes SET fetched_at='2099-01-01T00:00:00Z'")
    assert all(r["last_stored_price"] is None and r["freshness"] == "unknown"
               for r in dashboard.query("signals")["data"]["items"])


def test_historical_plan_not_replaced_by_active_candidate(dashboard):
    mutate(dashboard, "UPDATE active_candidate_runs SET run_id='another-run'")
    assert dashboard.query("candidates")["data"]["items"] == []
    assert dashboard.query("signals")["data"]["items"][0]["attention_low"] is not None


@pytest.mark.parametrize("horizon", [1, 3, 5, 10])
def test_performance_denominators(dashboard, horizon):
    data = dashboard.query("performance", {"horizon": str(horizon)})["data"]
    assert data["sample_count"] == 8 and data["mature_count"] == 5
    assert data["price_evaluable_count"] == 5
    assert data["order_evaluable_count"] < data["price_evaluable_count"]
    assert data["status_counts"]["pending"] == 3
    for row in data["records"]:
        if row["status"] == "pending":
            assert row["return_pct"] is None
        if row["status"] == "unknown_order":
            assert row["return_pct"] is not None and row["target_hit"] is None
    assert data["benchmark"]["return_pct"] is None


@pytest.mark.parametrize("sql", [
    "UPDATE daily_bars SET corporate_action_factor=2 WHERE code='DEMO01'",
    "UPDATE daily_bars SET corporate_action_evidence='' WHERE code='DEMO01'",
    "UPDATE daily_bars SET high=0 WHERE code='DEMO01'",
    "UPDATE recommendation_records SET comparability_status='unknown' WHERE code='DEMO01'",
    "DELETE FROM daily_bars WHERE code='DEMO01' AND trade_date='2026-08-17'",
])
def test_uncertain_prices_are_excluded(dashboard, sql):
    mutate(dashboard, sql)
    data = dashboard.query("performance")["data"]
    row = next(r for r in data["records"] if r["code"] == "DEMO01")
    assert row["status"] == "unknown" and row["return_pct"] is None
    assert data["price_evaluable_count"] == 4


def test_calendar_unknown_is_not_immaturity(dashboard):
    mutate(dashboard, "DELETE FROM trading_calendar WHERE trade_date='2026-08-17'")
    data = dashboard.query("performance")["data"]
    assert data["status_counts"]["unknown"] == 5
    assert data["price_evaluable_count"] == 0


def test_real_data_requires_bound_factor_evidence(dashboard):
    mutate(dashboard, "DELETE FROM web_demo_metadata")
    assert dashboard.query("performance")["data"]["price_evaluable_count"] == 0
    with sqlite3.connect(dashboard.database) as db:
        db.execute("CREATE TABLE corporate_action_factors(code TEXT,trade_date TEXT,adj_factor REAL,source TEXT,evidence TEXT)")
        for table in ("recommendation_records", "daily_bars"):
            db.execute(f"UPDATE {table} SET code='600000' WHERE code='DEMO01'")
        for (day,) in db.execute("SELECT trade_date FROM daily_bars WHERE code='600000'").fetchall():
            evidence = f"tushare:adj_factor:{day}:600000.SH"
            db.execute("INSERT INTO corporate_action_factors VALUES(?,?,1,'tushare_adj_factor',?)", ("600000", day, evidence))
            db.execute("UPDATE daily_bars SET corporate_action_evidence=? WHERE code='600000' AND trade_date=?", (evidence, day))
    assert dashboard.query("performance")["data"]["price_evaluable_count"] == 1
    mutate(dashboard, "UPDATE corporate_action_factors SET evidence='wrong-code'")
    assert dashboard.query("performance")["data"]["price_evaluable_count"] == 0


def test_bars_and_malformed_plan(dashboard):
    bars = dashboard.query("stocks/DEMO01")["data"]["bars"]
    assert len(bars) > 50 and bars == sorted(bars, key=lambda r: r["date"])
    mutate(dashboard, "UPDATE daily_bars SET price_basis='qfq' WHERE code='DEMO01'")
    assert dashboard.query("stocks/DEMO01")["data"]["bars"] == []
    mutate(dashboard, "UPDATE screen_candidates SET price_plan=?", (json.dumps({"provenance": ["malformed"]}),))
    assert dashboard.query("candidates")["meta"]["status"] != "unavailable"
    assert dashboard.query("stocks/../../secret")["meta"]["status"] == "unavailable"
    assert dashboard.query("performance", {"horizon": "2"})["meta"]["status"] == "unavailable"


def test_demo_repeatability_and_no_overwrite(tmp_path, dashboard):
    other = create_demo(tmp_path / "other.sqlite3")
    assert other.read_bytes() == dashboard.database.read_bytes()
    with pytest.raises(FileExistsError):
        create_demo(dashboard.database)


def test_current_plugin_schema_without_web_migration(tmp_path):
    from test_v0139_intraday_m2 import _imports
    _, _, Store = _imports()
    database = tmp_path / "plugin.sqlite3"
    Store(database)
    before = database.read_bytes()
    dashboard = Dashboard(database, now=lambda: NOW)
    for route in ("overview", "signals", "candidates", "performance", "health", "settings"):
        result = dashboard.query(route)
        assert result["meta"]["status"] != "unavailable", (route, result)
        assert result["meta"]["dataset_kind"] == "local_database"
    assert database.read_bytes() == before


def test_health_separates_active_raw_and_stored_snapshot_provenance(tmp_path):
    database = tmp_path / "provenance.sqlite3"
    with sqlite3.connect(database) as db:
        db.executescript("""
            CREATE TABLE datasets(dataset_id TEXT,dataset_key TEXT);
            CREATE TABLE batches(batch_id TEXT,dataset_id TEXT,actual_trade_date TEXT,status TEXT,
                quality TEXT,source TEXT,row_count INTEGER,published_at TEXT,created_at TEXT);
            CREATE TABLE active_generations(dataset_id TEXT,active_batch_id TEXT,generation INTEGER);
            CREATE TABLE daily_snapshot_meta(trade_date TEXT,requested_date TEXT,source TEXT,
                quality TEXT,complete INTEGER,fetched_at TEXT);
            CREATE TABLE daily_bars(code TEXT,trade_date TEXT,source TEXT,fetched_at TEXT);
        """)
        db.execute("INSERT INTO datasets VALUES('daily','tushare_daily')")
        db.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?)", (
            "batch-9", "daily", "2026-09-11", "published", "good", "tushare", 661611,
            "2026-09-11T08:00:00+00:00", "2026-09-11T07:00:00+00:00"))
        db.execute("INSERT INTO active_generations VALUES('daily','batch-9',9)")
        db.execute("INSERT INTO daily_snapshot_meta VALUES(?,?,?,?,?,?)", (
            "2026-09-11", "2026-09-14", "tushare", "good", 1, "2026-09-11T08:00:00+00:00"))
        db.execute("INSERT INTO daily_bars VALUES(?,?,?,?)", (
            "600000", "2026-08-28", "eastmoney", "2026-08-28T08:00:00+00:00"))
    evidence = Dashboard(database, now=lambda: NOW).query("health")["data"]["evidence"]
    active = [item for item in evidence if item["dataset"] == "raw_active_generation"]
    assert active == [{"dataset": "raw_active_generation", "dataset_key": "tushare_daily", "source": "tushare",
                       "count": 661611, "date": "2026-09-11", "collected_at": "2026-09-11T08:00:00+00:00",
                       "generation": 9, "quality": "good", "status": "active_published_snapshot"}]
    latest = next(item for item in evidence if item["dataset"] == "daily_snapshot_meta")
    assert latest["status"] == "complete_stored_snapshot"
    assert latest["complete"] is True
    assert latest["date"] == "2026-09-11"
    missing_evidence = next(item for item in evidence if item["dataset"] == "data_evidence_records")
    assert missing_evidence == {"dataset": "data_evidence_records", "status": "not_collected", "count": None}
    assert all(item["status"] == "stored_not_live" for item in evidence if item["dataset"] == "daily_bars")


def test_http_read_only_security_and_static(dashboard):
    server = create_server(dashboard, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    def request(path, method="GET", headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        try:
            conn.request(method, path, headers=headers or {})
            response = conn.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            conn.close()
    try:
        status, headers, body = request("/")
        assert status == 200 and b"app.js" in body
        status, _headers, script = request("/app.js")
        assert status == 200
        assert "风险区间" in script.decode()
        assert "建议买入价" in script.decode()
        assert "关注区" not in script.decode()
        assert "frame-ancestors 'none'" in headers["Content-Security-Policy"]
        assert request("/api/candidates")[0] == 200
        assert request("/icons/activity.svg")[0] == 200
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            assert request("/api/settings", method)[0] == 405
        assert request("/api/settings", headers={"Origin": "https://evil.example"})[0] == 403
        assert request("/", headers={"Host": "evil.example"})[0] == 403
        for path in ("/../data.py", "/%2e%2e/data.py", "/icons/../../data.py"):
            assert request(path)[0] == 404
        assert "TOP_SECRET" not in request("/api/health")[2].decode()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
