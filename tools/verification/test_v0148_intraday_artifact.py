import json
import os
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
import types

from intraday_artifact import publish, read
from webapp.data import Dashboard


UTC = timezone.utc


def quote(code="600000", ts=None):
    return types.SimpleNamespace(code=code, name="fixture", price=10.0, pct_change=1.2,
                                 amount=1000.0, volume=20.0, source="sina",
                                 provider_ts=ts or datetime(2026, 9, 15, 1, 30, tzinfo=UTC),
                                 fetched_at=datetime(2026, 9, 15, 1, 30, tzinfo=UTC))


def test_atomic_artifact_roundtrip_and_missing(tmp_path):
    path = tmp_path / "intraday.json"
    payload = publish(path, target_codes=["600000", "000001"], quotes=[quote()],
                      collected_at=datetime(2026, 9, 15, 1, 30, tzinfo=UTC),
                      published_at=datetime(2026, 9, 15, 1, 30, tzinfo=UTC))
    assert path.is_file() and payload["returned_count"] == 1 and payload["missing_count"] == 1
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o644
    assert read(path)["revision"] == payload["revision"]


def test_web_intraday_marks_expired_and_does_not_expose_origin(tmp_path):
    db = tmp_path / "db.sqlite3"
    db.touch()
    path = tmp_path / "intraday.json"
    publish(path, target_codes=["600000"], quotes=[quote()],
            collected_at=datetime(2026, 9, 15, 1, 30, tzinfo=UTC),
            published_at=datetime(2026, 9, 15, 1, 30, tzinfo=UTC))
    dashboard = Dashboard(db, origin="private-origin", artifact_path=path,
                          now=lambda: datetime(2026, 9, 15, 1, 31, tzinfo=UTC))
    data = dashboard.intraday()
    assert data["status"] == "unknown" and data["reason"] == "artifact_expired"
    assert "origin" not in json.dumps(data)


def test_web_intraday_rejects_future_and_mixed_provider_times(tmp_path):
    db, path = tmp_path / "db.sqlite3", tmp_path / "intraday.json"
    db.touch()
    now = datetime(2026, 9, 15, 1, 30, tzinfo=UTC)
    publish(path, target_codes=["600000", "000001"], quotes=[quote("600000", now), quote("000001", now + timedelta(seconds=100))],
            collected_at=now, published_at=now)
    data = Dashboard(db, artifact_path=path, now=lambda: now + timedelta(seconds=1)).intraday()
    assert data["status"] == "unknown" and data["reason"] in {"quote_timestamp_invalid", "mixed_provider_timestamps"}
