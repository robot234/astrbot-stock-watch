from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import types

from intraday_artifact import publish
from webapp.data import Dashboard
from webapp.demo import create_demo


UTC = timezone.utc
NOW = datetime(2026, 9, 16, 5, 34, tzinfo=UTC)


def quote():
    return types.SimpleNamespace(code="600000", name="fixture", price=10.0, pct_change=1.2,
                                 amount=1000.0, volume=20.0, source="sina",
                                 provider_ts=NOW - timedelta(seconds=2), fetched_at=NOW - timedelta(seconds=1))


def market(**updates):
    value = {
        "regime": "strong", "pending_regime": "unknown", "quality": "good", "reason": "confirmed",
        "source": "sina", "source_timestamp": (NOW - timedelta(seconds=3)).isoformat(),
        "updated_at": (NOW - timedelta(seconds=1)).isoformat(), "sample_size": 5548,
        "expected_size": 5548, "coverage": 1.0, "opportunity_allowed": True,
    }
    value.update(updates)
    return value


def dashboard_with_closing_context(tmp_path, *, artifact_market=None, published_at=NOW, artifact_status="available", artifact_reason="", valid_for_seconds=None, clock=NOW):
    tmp_path.mkdir(parents=True, exist_ok=True)
    database, artifact = tmp_path / "demo.sqlite3", tmp_path / "intraday.json"
    create_demo(database, tmp_path / "settings.json")
    with sqlite3.connect(database) as db:
        db.execute("DELETE FROM market_contexts")
        db.execute("INSERT INTO market_contexts(as_of,payload,source,quality) VALUES(?,?,?,?)",
                   ("2026-09-14", '{"regime":"risk_on","advancing":1,"declining":0,"flat":0,"median_return":1}',
                    "daily_snapshot", "good"))
    extra = {"market": artifact_market or market()}
    if valid_for_seconds is not None:
        extra["valid_for_seconds"] = valid_for_seconds
    publish(artifact, target_codes=["600000"], quotes=[quote()], collected_at=NOW,
            published_at=published_at, status=artifact_status, reason=artifact_reason, extra=extra)
    return Dashboard(database, artifact_path=artifact, now=lambda: clock)


def test_overview_separates_older_close_from_confirmed_today_live_market(tmp_path):
    payload = dashboard_with_closing_context(tmp_path).query("overview")
    assert payload["data"]["market_date"] == "2026-09-14"
    assert payload["data"]["market"]["regime"] == "risk_on"
    live = payload["data"]["live_market"]
    assert live["status"] == "available"
    assert live["regime"] == "strong"
    assert live["sample_size"] == live["expected_size"] == 5548
    assert live["opportunity_allowed"] is True


def test_live_market_never_falls_back_to_closing_state_when_expired_or_unconfirmed(tmp_path):
    expired_dir, pending_dir = tmp_path / "expired", tmp_path / "pending"
    expired_dir.mkdir()
    pending_dir.mkdir()
    expired = dashboard_with_closing_context(expired_dir, published_at=NOW - timedelta(seconds=31)).query("overview")
    assert expired["data"]["live_market"] == Dashboard._unknown_live_market("artifact_expired")
    pending = dashboard_with_closing_context(pending_dir, artifact_status="unknown", artifact_reason="awaiting_regime_confirmation:1/2", artifact_market=market(regime="unknown", pending_regime="strong", reason="awaiting_regime_confirmation:1/2")).query("overview")
    assert pending["data"]["live_market"]["status"] == "pending"
    assert pending["data"]["live_market"]["opportunity_allowed"] is False


def test_artifact_validity_window_uses_published_interval_without_weakening_legacy_default(tmp_path):
    legacy = dashboard_with_closing_context(tmp_path / "legacy", published_at=NOW - timedelta(seconds=31), clock=NOW).intraday()
    assert legacy["status"] == "unknown" and legacy["reason"] == "artifact_expired"
    extended_dir = tmp_path / "extended"
    extended_dir.mkdir()
    extended = dashboard_with_closing_context(extended_dir, published_at=NOW - timedelta(seconds=31), valid_for_seconds=60, clock=NOW).intraday()
    assert extended["status"] == "available" and extended["valid_for_seconds"] == 60


def test_artifact_and_data_revisions_ignore_publication_clock_but_track_live_market_change(tmp_path):
    path = tmp_path / "intraday.json"
    first = publish(path, target_codes=["600000"], quotes=[quote()], collected_at=NOW,
                    published_at=NOW, extra={"market": market()})
    repeated = publish(path, target_codes=["600000"], quotes=[quote()], collected_at=NOW + timedelta(seconds=5),
                       published_at=NOW + timedelta(seconds=5), extra={"market": market()})
    changed = publish(path, target_codes=["600000"], quotes=[quote()], collected_at=NOW + timedelta(seconds=5),
                      published_at=NOW + timedelta(seconds=5), extra={"market": market(regime="weak", opportunity_allowed=True)})
    assert first["revision"] == repeated["revision"]
    assert changed["revision"] != repeated["revision"]

    dashboard = dashboard_with_closing_context(tmp_path)
    initial = dashboard.query("revision")["data"]["data_revision"]
    publish(dashboard.artifact_path, target_codes=["600000"], quotes=[quote()], collected_at=NOW,
            published_at=NOW, extra={"market": market(regime="weak", opportunity_allowed=True)})
    assert dashboard.query("revision")["data"]["data_revision"] != initial


def test_silent_overview_refresh_has_a_dedicated_path_without_stock_detail_reload():
    source = (Path(__file__).parents[2] / "webapp" / "static" / "app.js").read_text(encoding="utf-8")
    start = source.index("async function refreshOverviewLiveMarket")
    end = source.index("function renderOverview", start)
    assert "api(\"overview\")" in source[start:end]
    assert "stocks/" not in source[start:end]
