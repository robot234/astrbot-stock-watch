"""Focused local regression coverage for the 2026-09-22 audit corrections."""
from __future__ import annotations

import http.client
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

# Keep this focused module runnable in isolation; do not rely on a previous
# verification module having happened to add the package parent first.
sys.path.insert(0, str(Path(__file__).resolve().parents[2].parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))

from astrbot_stock_watch.providers import SinaQuoteProvider, TushareRequestGateway
from astrbot_stock_watch.storage import StockStore
from webapp.data import Dashboard
from webapp.demo import create_demo
from webapp.server import create_server


def test_web_metadata_and_missing_artifact_are_explicitly_partial(tmp_path):
    database = tmp_path / "demo.sqlite3"
    create_demo(database, tmp_path / "settings.json")
    dashboard = Dashboard(database, artifact_path=tmp_path / "absent-artifact.json")
    result = dashboard.query("overview")
    assert result["meta"]["status"] == "partial"
    assert "web_snapshot_metadata:unavailable" in result["meta"]["notices"]
    assert "intraday_artifact:artifact_missing" in result["meta"]["notices"]
    assert result["data"]["live_market"]["status"] == "unknown"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE web_snapshot_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.execute("INSERT INTO web_snapshot_metadata VALUES('captured_at', 'malformed')")
    result = dashboard.query("settings")
    assert result["meta"]["status"] == "partial"
    assert result["meta"]["snapshot"]["status"] == "partial"


def test_head_matches_get_headers_without_a_body(tmp_path):
    database = tmp_path / "demo.sqlite3"
    create_demo(database, tmp_path / "settings.json")
    server = create_server(Dashboard(database), port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        connection.request("HEAD", "/api/overview")
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Content-Type").startswith("application/json")
        assert int(response.getheader("Content-Length")) > 0
        assert response.read() == b""
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=3)


def test_bounded_retention_and_trade_date_indexes(tmp_path):
    store = StockStore(tmp_path / "store.sqlite3")
    with store._connect() as db:
        indexes = {row[1] for row in db.execute("PRAGMA index_list(minute_bars)")}
        assert {"idx_minute_bars_trade_date_start", "idx_minute_bars_trade_date_code_start"} <= indexes
        db.executemany(
            "INSERT INTO minute_bars VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [(f"600{i:03d}", f"2020-01-01T00:{i:02d}:00+00:00", "2020-01-01", 1, 1, 1, 1, 0, 0, "fixture", "2020-01-01T00:00:00") for i in range(3)],
        )
    assert store.cleanup_minute_bars(before="2020-01-02", limit=2) == 2
    assert len(store.minute_bars(trade_date="2020-01-01", limit=10)) == 1
    for index in range(3):
        store.save_provider_cache("daily", f"digest-{index}", {"value": index}, ttl_seconds=1, now=1)
    assert store.cleanup_provider_cache(now=2, limit=2) == 2
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM provider_cache").fetchone()[0] == 1


def test_bounded_intraday_terminal_retention_preserves_recovery_states(tmp_path):
    store = StockStore(tmp_path / "intraday-retention.sqlite3")
    states = ("sent", "cancelled", "sent", "pending", "sending", "failed", "unknown_delivery")
    with store._connect() as db:
        indexes = {row[1] for row in db.execute("PRAGMA index_list(intraday_event_outbox)")}
        assert {"idx_intraday_outbox_recovery", "idx_intraday_outbox_terminal_cleanup"} <= indexes
        for sequence, state in enumerate(states, 1):
            db.execute(
                "INSERT INTO intraday_event_outbox(event_key,origin,code,signal,plan_version,event_sequence,invocation_id,payload,payload_hash,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"fixture:{sequence}", "origin", f"60000{sequence}", "attention_entry", "plan", sequence,
                 "invoke", "fixture", "digest", state, "2020-01-01T00:00:00", "2020-01-01T00:00:00"),
            )
    # The explicit limit leaves one additional safe terminal row behind, and
    # no recovery/manual-review state is eligible for deletion.
    assert store.cleanup_intraday_terminal_outbox("2021-01-01T00:00:00", limit=2) == 2
    with store._connect() as db:
        remaining = {row["event_key"]: row["state"] for row in db.execute("SELECT event_key,state FROM intraday_event_outbox")}
    assert len(remaining) == 5
    assert {"pending", "sending", "failed", "unknown_delivery"} <= set(remaining.values())
    assert list(remaining.values()).count("sent") + list(remaining.values()).count("cancelled") == 1
    assert store.cleanup_intraday_terminal_outbox("2021-01-01T00:00:00", limit=10) == 1
    with pytest.raises(ValueError, match="cutoff"):
        store.cleanup_intraday_terminal_outbox("bad-cutoff")


def test_provider_repair_rolls_back_if_rebuild_fails(tmp_path, monkeypatch):
    path = tmp_path / "repair.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE provider_api_state(api_name TEXT)")
        db.execute("INSERT INTO provider_api_state VALUES('daily')")
    def fail_after_drop(cls, db, *args):
        db.execute("DROP TABLE provider_api_state")
        raise RuntimeError("fixture rebuild failure")

    monkeypatch.setattr(StockStore, "_repair_v14_provider_constraints_in_tx", classmethod(fail_after_drop))
    with sqlite3.connect(path) as db, pytest.raises(RuntimeError):
        StockStore._repair_v14_provider_constraints(db)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT api_name FROM provider_api_state").fetchone()[0] == "daily"


def test_provider_buckets_raw_code_timestamp_and_pagination_dedup():
    gateway = TushareRequestGateway()
    assert gateway._bucket("daily_basic") == (50, 60.0)
    assert gateway._bucket("fina_indicator") == (1, 60.0)
    assert SinaQuoteProvider._snapshot_row({"f12": "12", "f2": 10}) is None
    row = SinaQuoteProvider._snapshot_row({"f12": "600000", "f2": 10, "f3": 1, "f5": 1, "f6": 1})
    assert row and row.provider_ts is None and row.fetched_at is not None

    class Response:
        def raise_for_status(self): pass
        def json(self): return {"data": {"diff": [{"f12": "600000", "f2": 10, "f3": 1, "f5": 1, "f6": 1}, {"f12": "600000", "f2": 10, "f3": 1, "f5": 1, "f6": 1}], "total": 2}}
    class Slot:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False
        async def get(self, *args, **kwargs): return Response()
    provider = SinaQuoteProvider()
    provider.http.slot = lambda: Slot()
    async def no_risk(_quotes, **_kwargs): return None
    provider.enrich_daily_risk_fields = no_risk
    import asyncio
    assert len(asyncio.run(provider._fetch_eastmoney_snapshot())) == 1


def test_packaged_service_explicitly_wires_artifact_and_dynamic_links_are_safe():
    root = Path(__file__).resolve().parents[2]
    service = (root / "webapp/deploy/stock-watch-web.service").read_text(encoding="utf-8")
    installer = (root / "webapp/deploy/install.py").read_text(encoding="utf-8")
    app = (root / "webapp/static/app.js").read_text(encoding="utf-8")
    assert "--artifact /home/pi/astrbot/data/plugin_data/astrbot_stock_watch/intraday_quotes.json" in service
    assert "artifact_wiring_missing" in installer
    assert "const stockHref" in app and "const percentWidth" in app
    assert 'href="#stock/${c.code}"' not in app
    assert '"roe": None' in (root / "providers.py").read_text(encoding="utf-8")
