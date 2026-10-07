"""Settings load records (C12): which keys changed between plugin loads, keys only, newest first on the Web."""
from __future__ import annotations

from datetime import datetime, timezone
import importlib
import json

from test_v0138_automatic_close import _imports
from webapp.data import Dashboard

Main, _ScreenScoreResult, _core, StockStore = _imports()
main_module = importlib.import_module("astrbot_stock_watch.main")
DEFAULTS = dict(main_module._SCHEMA_DEFAULTS)
NOW = datetime(2026, 10, 7, 11, 30, tzinfo=timezone.utc)


def _plugin(**config):
    plugin = Main.__new__(Main)
    plugin.config = {**DEFAULTS, **config}
    plugin.deprecated_settings, plugin.setting_issues = [], []
    return plugin


def _history(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_each_load_records_changed_keys_only(tmp_path):
    snapshot = tmp_path / "public_settings.json"
    history = tmp_path / "public_settings_history.jsonl"
    first_value, second_value = "q7" * 8, "z9" * 8
    Main._write_public_settings(_plugin(), snapshot)
    Main._write_public_settings(_plugin(min_score=15, **{"tushare_token": first_value}), snapshot)
    Main._write_public_settings(_plugin(min_score=15, **{"tushare_token": second_value}), snapshot)
    rows = _history(history)
    assert [row["changed"] for row in rows] == [None, ["min_score", "tushare_token"], []]
    assert rows[1]["fingerprint"]["min_score"] == ["value", 15] and rows[1]["fingerprint"]["tushare_token"] == ["state", "custom"]
    text = history.read_text(encoding="utf-8")
    assert first_value not in text and second_value not in text
    assert json.loads(snapshot.read_text(encoding="utf-8"))["values"]["min_score"] == 15


def test_history_is_capped_and_its_failure_keeps_the_snapshot(tmp_path, monkeypatch):
    snapshot = tmp_path / "public_settings.json"
    history = tmp_path / "public_settings_history.jsonl"
    history.write_text("".join(json.dumps({"n": i}) + "\n" for i in range(80)) + "not json\n", encoding="utf-8")
    Main._write_public_settings(_plugin(), snapshot)
    rows = history.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 50 and json.loads(rows[-1])["changed"] is None and rows[-2] == "not json"
    monkeypatch.setattr(Main, "_append_settings_history", lambda *args, **kwargs: 1 / 0)
    snapshot.unlink()
    Main._write_public_settings(_plugin(), snapshot)
    assert snapshot.exists()


def test_web_shows_recent_loads_newest_first_with_known_keys(tmp_path):
    database = tmp_path / "plugin.sqlite3"
    StockStore(database)
    artifact = tmp_path / "plugin_data" / "intraday_quotes.json"
    artifact.parent.mkdir()
    dashboard = Dashboard(database, artifact_path=artifact, now=lambda: NOW)
    assert dashboard.query("settings")["data"]["history"] == {"status": "missing", "items": []}
    lines = [{"written_at": "2026-10-07T09:54:24+00:00", "plugin_version": "0.13.3", "code_sha256": "e" * 64, "changed": None},
             "junk", {"written_at": "2026-10-07T11:00:00+00:00", "plugin_version": "0.13.3\x07", "code_sha256": "bad",
                      "changed": ["min_score", "not_a_setting", 3]}]
    (artifact.parent / "public_settings_history.jsonl").write_text(
        "".join((json.dumps(line) if not isinstance(line, str) else line) + "\n" for line in lines) + "{broken\n", encoding="utf-8")
    history = dashboard.query("settings")["data"]["history"]
    assert history["status"] == "available"
    assert history["items"] == [
        {"written_at": "2026-10-07T11:00:00+00:00", "plugin_version": "0.13.3", "code_sha256": None, "first": False, "changed": ["min_score"]},
        {"written_at": "2026-10-07T09:54:24+00:00", "plugin_version": "0.13.3", "code_sha256": "e" * 64, "first": True, "changed": []}]
    explicit = Dashboard(database, settings=tmp_path / "x.json", artifact_path=artifact, now=lambda: NOW)
    assert explicit.query("settings")["data"]["history"]["status"] == "not_configured"
