from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2].parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))

from astrbot_stock_watch.core import CHINA_TZ, Quote
from astrbot_stock_watch.providers import SinaQuoteProvider
from test_v0133_concurrency import _main_class
from webapp.data import Dashboard
from webapp.demo import create_demo


def quote(at: datetime) -> Quote:
    return Quote("600000", "Test", 10.0, 9.9, 100.0, 1.0, 1000.0, source="sina", provider_ts=at, fetched_at=at)


def test_risk_fields_need_explicit_authoritative_values():
    item = quote(datetime.now(CHINA_TZ))
    SinaQuoteProvider._apply_authoritative_risk_fields([item], [{"f12": "600000", "f43": 1000, "f51": 1000, "f52": 900, "suspended": "0"}])
    assert (item.suspended, item.limit_up, item.limit_down) == (None, True, None)

    unknown = quote(datetime.now(CHINA_TZ))
    SinaQuoteProvider._apply_authoritative_risk_fields([unknown], [{"f12": "600000", "f43": 1000}])
    assert (unknown.suspended, unknown.limit_up, unknown.limit_down) == (None, None, None)

    conflicting = quote(datetime.now(CHINA_TZ))
    SinaQuoteProvider._apply_authoritative_risk_fields(
        [conflicting],
        [
            {"f12": "600000", "f43": 1000, "f51": 1000, "f52": 900, "suspended": "0"},
            {"f12": "600000", "f43": 1000, "f51": 1100, "f52": 900, "suspended": "0"},
        ],
    )
    assert (conflicting.suspended, conflicting.limit_up, conflicting.limit_down) == (None, None, None)

    ambiguous = quote(datetime.now(CHINA_TZ))
    SinaQuoteProvider._apply_authoritative_risk_fields(
        [ambiguous],
        [
            {"f12": "600000", "f43": 1000, "f51": 1000, "f52": 900, "suspended": "0"},
            {"f12": "600000", "f43": 1000, "f51": 1000, "f52": 900, "suspended": "0"},
        ],
    )
    assert (ambiguous.suspended, ambiguous.limit_up, ambiguous.limit_down) == (None, None, None)


def test_line_b_accepts_bounded_future_time_and_rejects_boundaries():
    Main = _main_class()
    item = Main.__new__(Main)
    item._source_health = {"sina": {}}
    item._int = lambda key, default, minimum, maximum: {"quote_clock_skew_seconds": 120}.get(key, default)
    now = datetime(2026, 9, 15, 10, 0, tzinfo=CHINA_TZ)
    assert Main._fresh_quotes(item, [quote(now + timedelta(seconds=120))], now=now)
    assert Main._fresh_quotes(item, [quote(now + timedelta(seconds=121))], now=now) == []
    assert item._source_health["sina"]["last_rejections"] == {"future_fetched_at": 1}
    assert Main._fresh_quotes(item, [quote(now - timedelta(seconds=61))], now=now) == []
    assert item._source_health["sina"]["last_rejections"] == {"stale_fetched_at": 1}

    signal_quote = quote(now + timedelta(seconds=120))
    signal_quote.suspended = signal_quote.limit_up = signal_quote.limit_down = signal_quote.st = False
    item._float = lambda _key, default, _minimum, _maximum: default
    specs, reasons = Main._intraday_signal_specs(item, signal_quote, {}, live_regime="strong", now=now)
    assert specs and "stale_quote" not in reasons


def test_regime_guard_is_reentrant_and_releases_after_failure():
    Main = _main_class()
    item = Main.__new__(Main)
    started = asyncio.Event()
    release = asyncio.Event()

    class Store:
        def intraday_market_regime_state(self):
            return None

        def active_raw_universe_codes(self, *_args, **_kwargs):
            return ["600000"], {"batch_id": "fixture"}

        def save_intraday_market_regime_state(self, state):
            return dict(state)

    class Quotes:
        async def fetch_intraday_market_snapshot(self, _codes):
            started.set()
            await release.wait()
            raise RuntimeError("fixture provider failure")

    async def store_call(method, *args, **kwargs):
        await asyncio.sleep(0)
        return method(*args, **kwargs)

    item.store, item.quotes = Store(), Quotes()
    item._store_call = store_call
    item._raw_dataset = lambda: "tushare_daily"
    item._raw_stale_days = lambda: 2
    item._int = lambda _key, default, _minimum, _maximum: default
    item._float = lambda _key, default, _minimum, _maximum: default
    item._intraday_market_refresh_active = False
    now = datetime(2026, 9, 15, 10, 0, tzinfo=CHINA_TZ)

    async def run():
        first = asyncio.create_task(Main._resolve_intraday_market_context(item, now))
        await started.wait()
        second = await Main._resolve_intraday_market_context(item, now)
        release.set()
        first_result = await first
        return first_result, second

    first, second = asyncio.run(run())
    assert first["regime"] == "unknown" and first["reason"] == "intraday_market_provider_error"
    assert second == {"regime": "unknown", "quality": "unknown", "reason": "intraday_market_refresh_pending", "source": ""}
    assert item._intraday_market_refresh_active is False


def test_stale_regime_refreshes_to_current_state_instead_of_freezing_opportunities():
    Main = _main_class()
    item = Main.__new__(Main)
    now = datetime(2026, 9, 16, 10, 0, tzinfo=CHINA_TZ)
    current_quote = quote(now)

    class Store:
        def intraday_market_regime_state(self):
            return {"regime": "risk_off", "quality": "good", "source": "sina", "source_timestamp": (now - timedelta(minutes=5)).isoformat(), "quote_timestamp_min": (now - timedelta(minutes=5)).isoformat(), "quote_timestamp_max": (now - timedelta(minutes=5)).isoformat(), "sample_size": 1, "expected_size": 1, "coverage": 1.0}

        def active_raw_universe_codes(self, *_args, **_kwargs):
            return ["600000"], {"batch_id": "fixture"}

        def save_intraday_market_regime_state(self, state):
            return dict(state)

    class Quotes:
        async def fetch_intraday_market_snapshot(self, _codes):
            return SimpleNamespace(quotes=[current_quote], source="sina", failed_batches=0)

    async def store_call(method, *args, **kwargs):
        return method(*args, **kwargs)

    item.store, item.quotes, item._store_call = Store(), Quotes(), store_call
    item._raw_dataset = lambda: "tushare_daily"
    item._raw_stale_days = lambda: 2
    item._int = lambda key, default, _minimum, _maximum: {"market_min_snapshot_size": 1, "intraday_market_regime_confirmations": 1}.get(key, default)
    item._float = lambda key, default, _minimum, _maximum: 0.5 if key == "intraday_market_min_coverage" else default
    item._intraday_market_refresh_active = False
    item._intraday_market_evaluation_clock = lambda _started: now
    state = asyncio.run(Main._refresh_intraday_market_regime_state(item, now))
    assert state["quality"] == "good" and state["regime"] in {"strong", "neutral"}


def test_revision_has_distinct_components_and_one_stable_client_field(tmp_path):
    database, artifact = tmp_path / "demo.sqlite3", tmp_path / "intraday_quotes.json"
    create_demo(database, tmp_path / "settings.json")
    artifact.write_text(json.dumps({"schema_version": 1, "revision": "artifact-a", "status": "unknown", "quotes": []}), encoding="utf-8")
    now = datetime(2026, 9, 15, 3, tzinfo=timezone.utc)
    dashboard = Dashboard(database, artifact_path=artifact, now=lambda: now)
    first = dashboard.query("revision")["data"]
    second = dashboard.query("revision")["data"]
    assert first["data_revision"] == second["data_revision"]
    assert first["artifact_revision"] == "artifact-a"
    assert "snapshot_revision" in first and first["data_revision"] not in {first["artifact_revision"], first["snapshot_revision"]}
    app = (Path(__file__).resolve().parents[2] / "webapp" / "static" / "app.js").read_text(encoding="utf-8")
    assert "const revision = payload.data || {};" in app
    assert "const dataRevision = revision.data_revision || null;" in app
    assert "snapshotRevision !== state.snapshotRevision" in app
    assert "artifactRevision !== state.artifactRevision" in app
    assert 'artifactChanged && state.view === "intraday"' in app
    assert "if(await load({silent:true})) {" in app
    assert "if(silent)return false;" in app
    assert "if(request!==state.request)return false;" in app
    assert "return true;" in app
    assert "if(!silent){" in app
    assert app.index("if(silent)return false;") < app.index('$("#content").innerHTML=empty("数据不可用",error.message);')
    refresh_start = app.index("if(await load({silent:true})) {")
    refresh_end = app.index("\n    else {", refresh_start)
    assert "state.snapshotRevision = snapshotRevision;" in app[refresh_start:refresh_end]
    assert "state.artifactRevision = artifactRevision;" in app[refresh_start:refresh_end]
    assert "payload.data?.revision" not in app

    import sqlite3
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE web_snapshot_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.execute("INSERT INTO web_snapshot_metadata VALUES('source_fingerprint', ?)", ('{"b":2,"a":1}',))
    first = dashboard.query("revision")["data"]["snapshot_revision"]
    with sqlite3.connect(database) as db:
        db.execute("UPDATE web_snapshot_metadata SET value=? WHERE key='source_fingerprint'", ('{ "a": 1, "b": 2 }',))
    assert dashboard.query("revision")["data"]["snapshot_revision"] == first
