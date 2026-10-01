"""Privileged, bounded SQLite online backup; never run the HTTP server as root."""
from contextlib import closing
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time


SOURCE = Path("/home/pi/astrbot/data/plugin_data/astrbot_stock_watch/stock_watch.sqlite3")
TARGET = Path("/var/lib/stock-watch-web-snapshot/stock_watch.sqlite3")


def fingerprint(path):
    result = {}
    for suffix in ("", "-wal"):
        item = Path(str(path) + suffix)
        try:
            stat = item.stat()
            result[suffix or "main"] = [stat.st_ino, stat.st_size, stat.st_mtime_ns]
        except FileNotFoundError:
            result[suffix or "main"] = None
    return result


def publish(source, target, budget=240):
    source, target = Path(source), Path(target)
    if source.is_symlink() or target.is_symlink() or source.resolve() == target.resolve():
        raise ValueError("unsafe_snapshot_path")
    if not source.is_file():
        raise FileNotFoundError("source_missing")
    before = fingerprint(source)
    if target.exists():
        with closing(sqlite3.connect(target.as_uri() + "?mode=ro", uri=True)) as old:
            old.execute("PRAGMA query_only=ON")
            row = old.execute("SELECT value FROM web_snapshot_metadata WHERE key='source_fingerprint'").fetchone()
            if row and json.loads(row[0]) == before:
                return {"status": "unchanged"}
    deadline = time.monotonic() + budget

    def check(*_):
        if time.monotonic() > deadline:
            raise TimeoutError("snapshot_budget_exceeded")

    fd, temporary = tempfile.mkstemp(prefix=".snapshot-", suffix=".sqlite3", dir=target.parent)
    os.close(fd)
    staging = Path(temporary)
    try:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=1)) as src:
            src.execute("PRAGMA query_only=ON")
            src.execute("BEGIN")
            captured = datetime.now(timezone.utc).isoformat()
            src.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            with closing(sqlite3.connect(staging)) as dst:
                src.backup(dst, pages=1024, progress=check, sleep=0.05)
                src.rollback()
                dst.execute("PRAGMA journal_mode=DELETE")
                dst.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
                if dst.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                    raise ValueError("snapshot_integrity_failed")
                dst.execute("DROP TABLE IF EXISTS web_snapshot_metadata")
                dst.execute("CREATE TABLE web_snapshot_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
                dst.executemany("INSERT INTO web_snapshot_metadata VALUES(?,?)", [
                    ("captured_at", captured),
                    ("source_fingerprint", json.dumps(before, sort_keys=True)),
                    ("method", "sqlite_online_backup"),
                    ("integrity", "ok"),
                    ("refresh_interval_seconds", "3600"),
                ])
                dst.commit()
        check()
        with staging.open("r+b") as handle:
            os.fsync(handle.fileno())
        if os.name == "posix":
            os.chmod(staging, 0o440)
        os.replace(staging, target)
        if os.name == "posix":
            directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return {"status": "published", "captured_at": captured, "bytes": target.stat().st_size}
    finally:
        for suffix in ("", "-wal", "-shm", "-journal"):
            item = Path(str(staging) + suffix)
            if item.exists():
                item.unlink()


def main():
    import fcntl

    with (TARGET.parent / ".refresh.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            print(json.dumps(publish(SOURCE, TARGET)), flush=True)
        except Exception as exc:
            print(json.dumps({"status": "failed", "category": type(exc).__name__}), flush=True)
            raise SystemExit(1) from None


if __name__ == "__main__":
    main()
