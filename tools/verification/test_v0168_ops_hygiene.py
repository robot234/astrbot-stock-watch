"""Snapshot staging cleanup, the Web updater's pure helpers and unit/installer consistency."""
from pathlib import Path
import sqlite3
import types

import pytest

from webapp.demo import create_demo
from webapp.deploy import snapshot
from webapp.deploy.update import failures, server_arguments


ROOT = Path(__file__).resolve().parents[2]


def test_orphaned_staging_files_are_removed_but_the_snapshot_is_kept(tmp_path):
    source, target = tmp_path / "source.sqlite3", tmp_path / "snapshot.sqlite3"
    create_demo(source, tmp_path / "settings.json")
    snapshot.publish(source, target)
    snapshot.record_check(target, {"status": "published", "captured_at": "2026-10-07T02:00:00+00:00"})
    for name in (".snapshot-old.sqlite3", ".snapshot-old.sqlite3-journal", ".status-old.json"):
        (tmp_path / name).write_bytes(b"x")
    assert snapshot.remove_orphans(target) == [".snapshot-old.sqlite3", ".snapshot-old.sqlite3-journal", ".status-old.json"]
    assert target.exists() and (tmp_path / "snapshot_status.json").exists()
    assert snapshot.remove_orphans(target) == []


def test_termination_during_backup_still_removes_the_staging_copy(tmp_path, monkeypatch):
    source, target = tmp_path / "source.sqlite3", tmp_path / "snapshot.sqlite3"
    create_demo(source, tmp_path / "settings.json")
    snapshot.publish(source, target)
    before = target.read_bytes()
    with sqlite3.connect(source) as db:
        db.execute("CREATE TABLE changed(value)")
    calls = iter(range(10 ** 6))

    def monotonic():
        if next(calls) > 0:
            snapshot._terminate(15, None)
        return 0.0

    monkeypatch.setattr(snapshot, "time", types.SimpleNamespace(monotonic=monotonic))
    with pytest.raises(snapshot.Terminated):
        snapshot.publish(source, target)
    assert target.read_bytes() == before
    assert not list(tmp_path.glob(".snapshot-*"))


def test_updater_reads_server_arguments_and_rejects_bad_probes():
    unit = (ROOT / "webapp/deploy/stock-watch-web.service").read_text(encoding="utf-8")
    assert server_arguments(unit) == {"host": "192.168.124.6", "database": "/var/lib/stock-watch-web-snapshot/stock_watch.sqlite3",
                                      "port": "8767", "artifact": "/home/pi/astrbot/data/plugin_data/astrbot_stock_watch/intraday_quotes.json",
                                      "signals": "/home/pi/apps/stock-watch-web/state/research_signals.json",
                                      "watch-inbox": "/home/pi/astrbot/data/plugin_data/astrbot_stock_watch/web_watch_inbox"}
    good = {"version": {"http": 200, "status": "available", "revision": "abc1234"},
            "candidates": {"http": 200, "status": "partial", "research_status": "research_only"}}
    assert failures(good, "abc1234") == []
    assert failures(good, "other") == ["version:revision_mismatch"]
    assert failures({**good, "overview": {"error": "URLError"}}, "abc1234") == ["overview"]
    assert failures({**good, "candidates": {"http": 200, "status": "partial", "research_status": "unavailable"}}) == ["candidates:research_missing"]


def test_installer_expects_the_artifact_wiring_used_in_production():
    unit = (ROOT / "webapp/deploy/stock-watch-web.service").read_text(encoding="utf-8")
    installer = (ROOT / "webapp/deploy/install.py").read_text(encoding="utf-8")
    wiring = "--artifact " + server_arguments(unit)["artifact"]
    assert wiring in installer
