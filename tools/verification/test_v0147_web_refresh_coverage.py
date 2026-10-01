from datetime import datetime, timezone
import json
import sqlite3

from webapp.data import Dashboard
from webapp.demo import create_demo


NOW = datetime(2026, 9, 15, 3, tzinfo=timezone.utc)


def test_revision_probe_is_lightweight_and_exposes_snapshot_cadence(tmp_path):
    database = tmp_path / "demo.sqlite3"
    create_demo(database, tmp_path / "settings.json")
    with sqlite3.connect(database) as db:
        db.executescript(
            """
            CREATE TABLE web_snapshot_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO web_snapshot_metadata VALUES
              ('captured_at','2026-09-15T02:00:00+00:00'),
              ('source_fingerprint','{\"main\":[1,2,3]}'),
              ('method','sqlite_online_backup'),
              ('integrity','ok'),
              ('refresh_interval_seconds','3600');
            """
        )
    payload = Dashboard(database, now=lambda: NOW).query("revision")
    # The snapshot cadence remains readable, but no independent intraday
    # artifact was supplied.  Do not report the combined Web metadata as
    # wholly available or fabricate live-artifact access.
    assert payload["meta"]["status"] == "partial"
    assert "intraday_artifact:artifact_missing" in payload["meta"]["notices"]
    assert payload["data"]["data_revision"]
    assert payload["data"]["poll_interval_seconds"] == 5
    assert payload["data"]["refresh_interval_seconds"] == 3600


def test_missing_optional_evidence_is_not_reported_as_schema_unavailable(tmp_path):
    database = tmp_path / "demo.sqlite3"
    create_demo(database, tmp_path / "settings.json")
    payload = Dashboard(database, now=lambda: NOW).query("health")
    missing = next(item for item in payload["data"]["evidence"] if item["dataset"] == "data_evidence_records")
    assert missing["status"] == "not_collected"
    assert "data_evidence_records:unavailable" not in payload["meta"]["notices"]


def test_stock_missing_bars_exposes_unknown_reason_without_fallback(tmp_path):
    database = tmp_path / "demo.sqlite3"
    create_demo(database, tmp_path / "settings.json")
    with sqlite3.connect(database) as db:
        db.execute("DELETE FROM daily_bars WHERE code='DEMO01'")
    payload = Dashboard(database, origin="demo", now=lambda: NOW).query("stocks/DEMO01")
    quality = payload["data"]["data_quality"]
    assert quality == {"status": "unknown", "missing_reason": "daily_bars_not_collected", "source_timestamp": None}
    assert payload["data"]["last_close"] is None


def test_candidate_contains_run_coverage_provenance(tmp_path):
    database = tmp_path / "demo.sqlite3"
    create_demo(database, tmp_path / "settings.json")
    with sqlite3.connect(database) as db:
        db.execute("ALTER TABLE screen_runs ADD COLUMN diagnostics TEXT NOT NULL DEFAULT '{}'")
        db.execute(
            "UPDATE screen_runs SET coverage=0.5, diagnostics=? WHERE run_id=(SELECT run_id FROM active_candidate_runs WHERE scope='global')",
            (json.dumps({"indicator_coverage": 0.5, "degraded_reason": "history_coverage", "quote_timestamp_max": "2026-09-14T08:00:00Z"}),),
        )
    candidate = Dashboard(database, now=lambda: NOW).query("candidates")["data"]["items"][0]
    assert candidate["coverage_status"] == "partial"
    assert candidate["missing_reason"] == "history_coverage"
    assert candidate["source_timestamp"] == "2026-09-14T08:00:00Z"
