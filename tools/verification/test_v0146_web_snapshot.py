from datetime import datetime, timedelta, timezone
import hashlib
import sqlite3

import pytest

from webapp.data import Dashboard
from webapp.demo import create_demo
from webapp.deploy.snapshot import publish


def test_online_backup_includes_uncheckpointed_wal_and_is_read_only(tmp_path):
    source, target = tmp_path / "source.sqlite3", tmp_path / "snapshot.sqlite3"
    create_demo(source, tmp_path / "settings.json")
    with sqlite3.connect(source) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE snapshot_test(value TEXT)")
        writer.execute("INSERT INTO snapshot_test VALUES('committed_wal')")
        writer.commit()
        before = hashlib.sha256(source.read_bytes()).hexdigest()
        result = publish(source, target)
        assert result["status"] == "published"
        assert hashlib.sha256(source.read_bytes()).hexdigest() == before
        assert not writer.execute("SELECT name FROM sqlite_master WHERE name='web_snapshot_metadata'").fetchone()
        with sqlite3.connect(target.as_uri() + "?mode=ro", uri=True) as db:
            assert db.execute("SELECT value FROM snapshot_test").fetchone()[0] == "committed_wal"
            assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
            assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
            with pytest.raises(sqlite3.OperationalError):
                db.execute("DELETE FROM snapshot_test")
        assert publish(source, target)["status"] == "unchanged"
        payload = Dashboard(target).query("settings")
        assert payload["meta"]["snapshot"]["captured_at"] == result["captured_at"]
        assert payload["meta"]["snapshot"]["status"] == "recent"
        later = datetime.now(timezone.utc) + timedelta(hours=3)
        assert Dashboard(target, now=lambda: later).query("settings")["meta"]["snapshot"]["status"] == "stale"


def test_failed_refresh_keeps_previous_snapshot(tmp_path):
    source, target = tmp_path / "source.sqlite3", tmp_path / "snapshot.sqlite3"
    create_demo(source, tmp_path / "settings.json")
    publish(source, target)
    before = target.read_bytes()
    with sqlite3.connect(source) as db:
        db.execute("CREATE TABLE new_generation(value)")
    with pytest.raises(TimeoutError):
        publish(source, target, budget=-1)
    assert target.read_bytes() == before
    assert not list(tmp_path.glob(".snapshot-*"))


def test_missing_source_does_not_create_database(tmp_path):
    source, target = tmp_path / "absent.sqlite3", tmp_path / "snapshot.sqlite3"
    with pytest.raises(FileNotFoundError):
        publish(source, target)
    assert not source.exists() and not target.exists()
