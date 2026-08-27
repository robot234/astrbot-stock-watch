from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


class StockStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self):
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self):
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS watchlist(scope TEXT NOT NULL, code TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(scope, code));
                CREATE TABLE IF NOT EXISTS subscriptions(origin TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS whitelist(origin TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS seen_news(fingerprint TEXT PRIMARY KEY, created_at TEXT NOT NULL);
            """)

    def add_watch(self, scope: str, code: str, limit: int) -> bool:
        with self._connect() as db:
            count = db.execute("SELECT COUNT(*) FROM watchlist WHERE scope=?", (scope,)).fetchone()[0]
            if count >= limit:
                return False
            db.execute("INSERT OR IGNORE INTO watchlist VALUES (?, ?, ?)", (scope, code, datetime.utcnow().isoformat()))
            return True

    def remove_watch(self, scope: str, code: str) -> bool:
        with self._connect() as db:
            return db.execute("DELETE FROM watchlist WHERE scope=? AND code=?", (scope, code)).rowcount > 0

    def list_watch(self, scope: str) -> list[str]:
        with self._connect() as db:
            return [str(row[0]) for row in db.execute("SELECT code FROM watchlist WHERE scope=? ORDER BY code", (scope,))]

    def all_watch(self) -> dict[str, list[str]]:
        with self._connect() as db:
            result: dict[str, list[str]] = {}
            for row in db.execute("SELECT scope, code FROM watchlist ORDER BY scope, code"):
                result.setdefault(str(row[0]), []).append(str(row[1]))
            return result

    def set_subscription(self, origin: str, enabled: bool) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO subscriptions VALUES (?, ?, ?) ON CONFLICT(origin) DO UPDATE SET enabled=excluded.enabled, updated_at=excluded.updated_at", (origin, int(enabled), datetime.utcnow().isoformat()))

    def is_subscribed(self, origin: str) -> bool:
        with self._connect() as db:
            row = db.execute("SELECT enabled FROM subscriptions WHERE origin=?", (origin,)).fetchone()
            return bool(row and row[0])

    def subscriptions(self) -> list[str]:
        with self._connect() as db:
            return [str(row[0]) for row in db.execute("SELECT origin FROM subscriptions WHERE enabled=1")]

    def set_whitelist(self, origin: str, enabled: bool) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO whitelist VALUES (?, ?, ?) ON CONFLICT(origin) DO UPDATE SET enabled=excluded.enabled, updated_at=excluded.updated_at",
                (origin, int(enabled), datetime.utcnow().isoformat()),
            )

    def is_whitelisted(self, origin: str) -> bool:
        with self._connect() as db:
            row = db.execute("SELECT enabled FROM whitelist WHERE origin=?", (origin,)).fetchone()
            return bool(row and row[0])

    def whitelist(self) -> list[str]:
        with self._connect() as db:
            return [str(row[0]) for row in db.execute("SELECT origin FROM whitelist WHERE enabled=1 ORDER BY origin")]

    def mark_news_seen(self, fingerprint: str, keep_days: int = 14) -> bool:
        cutoff = (datetime.utcnow() - timedelta(days=keep_days)).isoformat()
        with self._connect() as db:
            db.execute("DELETE FROM seen_news WHERE created_at < ?", (cutoff,))
            if db.execute("SELECT 1 FROM seen_news WHERE fingerprint=?", (fingerprint,)).fetchone():
                return False
            db.execute("INSERT INTO seen_news VALUES (?, ?)", (fingerprint, datetime.utcnow().isoformat()))
            return True
