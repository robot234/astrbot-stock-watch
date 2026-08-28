from __future__ import annotations

import sqlite3
import math
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


class StockStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _init_db(self):
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS watchlist(scope TEXT NOT NULL, code TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(scope, code));
                CREATE TABLE IF NOT EXISTS subscriptions(origin TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS whitelist(origin TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS daily_quotes(
                    trade_date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    price REAL NOT NULL,
                    prev_close REAL NOT NULL DEFAULT 0,
                    amount REAL NOT NULL DEFAULT 0,
                    pct_change REAL NOT NULL DEFAULT 0,
                    volume REAL NOT NULL DEFAULT 0,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY(trade_date, code)
                );
                CREATE TABLE IF NOT EXISTS seen_news(fingerprint TEXT PRIMARY KEY, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS signal_events(
                    origin TEXT NOT NULL,
                    code TEXT NOT NULL,
                    last_sent_at TEXT NOT NULL,
                    PRIMARY KEY(origin, code)
                );
                CREATE TABLE IF NOT EXISTS confirmation_events(
                    origin TEXT NOT NULL,
                    code TEXT NOT NULL,
                    consecutive_count INTEGER NOT NULL,
                    last_observed_at TEXT NOT NULL,
                    PRIMARY KEY(origin, code)
                );
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(watchlist)")}
            if "cost_price" not in columns:
                db.execute("ALTER TABLE watchlist ADD COLUMN cost_price REAL NULL")

    def add_watch(self, scope: str, code: str, limit: int, cost_price: float | None = None) -> bool:
        if cost_price is not None and (not math.isfinite(cost_price) or cost_price <= 0):
            cost_price = None
        with self._connect() as db:
            exists = db.execute("SELECT 1 FROM watchlist WHERE scope=? AND code=?", (scope, code)).fetchone()
            count = db.execute("SELECT COUNT(*) FROM watchlist WHERE scope=?", (scope,)).fetchone()[0]
            if not exists and count >= limit:
                return False
            db.execute(
                "INSERT INTO watchlist(scope, code, created_at, cost_price) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(scope, code) DO UPDATE SET cost_price=COALESCE(excluded.cost_price, watchlist.cost_price)",
                (scope, code, datetime.utcnow().isoformat(), cost_price),
            )
            return True

    def remove_watch(self, scope: str, code: str) -> bool:
        with self._connect() as db:
            return db.execute("DELETE FROM watchlist WHERE scope=? AND code=?", (scope, code)).rowcount > 0

    def watch_cost(self, scope: str, code: str) -> float | None:
        with self._connect() as db:
            row = db.execute("SELECT cost_price FROM watchlist WHERE scope=? AND code=?", (scope, code)).fetchone()
            try:
                value = float(row[0]) if row and row[0] is not None else 0.0
                return value if math.isfinite(value) and value > 0 else None
            except (TypeError, ValueError):
                return None

    def list_watch_details(self, scope: str) -> list[tuple[str, float | None]]:
        with self._connect() as db:
            rows = db.execute("SELECT code, cost_price FROM watchlist WHERE scope=? ORDER BY code", (scope,))
            result = []
            for code, cost in rows:
                try:
                    value = float(cost) if cost is not None and math.isfinite(float(cost)) and float(cost) > 0 else None
                except (TypeError, ValueError):
                    value = None
                result.append((str(code), value))
            return result

    def list_watch(self, scope: str) -> list[str]:
        with self._connect() as db:
            return [str(row[0]) for row in db.execute("SELECT code FROM watchlist WHERE scope=? ORDER BY code", (scope,))]

    def all_watch(self) -> dict[str, list[str]]:
        with self._connect() as db:
            result: dict[str, list[str]] = {}
            for row in db.execute("SELECT scope, code FROM watchlist ORDER BY scope, code"):
                result.setdefault(str(row[0]), []).append(str(row[1]))
            return result

    def all_watch_details(self) -> dict[str, dict[str, float | None]]:
        with self._connect() as db:
            result: dict[str, dict[str, float | None]] = {}
            for scope, code, cost in db.execute("SELECT scope, code, cost_price FROM watchlist ORDER BY scope, code"):
                try:
                    value = float(cost) if cost is not None and float(cost) > 0 else None
                except (TypeError, ValueError):
                    value = None
                result.setdefault(str(scope), {})[str(code)] = value
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

    def save_daily_quotes(self, trade_date: str, quotes, keep_days: int = 180) -> int:
        rows = [
            (trade_date, quote.code, quote.name, quote.price, quote.prev_close, quote.amount,
             quote.pct_change, quote.volume, quote.fetched_at.isoformat())
            for quote in quotes
        ]
        if not rows:
            return 0
        cutoff = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        with self._connect() as db:
            db.execute("DELETE FROM daily_quotes WHERE trade_date=?", (trade_date,))
            db.executemany("INSERT INTO daily_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            db.execute("DELETE FROM daily_quotes WHERE trade_date < ?", (cutoff,))
        return len(rows)

    def daily_quotes(self, trade_date: str) -> list:
        from .core import Quote

        with self._connect() as db:
            rows = db.execute(
                "SELECT code, name, price, prev_close, amount, pct_change, volume, fetched_at FROM daily_quotes WHERE trade_date=? ORDER BY code",
                (trade_date,),
            )
            result = []
            for row in rows:
                try:
                    fetched_at = datetime.fromisoformat(str(row[7]))
                except ValueError:
                    fetched_at = datetime.now()
                result.append(Quote(str(row[0]), str(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]), float(row[6]), fetched_at=fetched_at))
            return result

    def mark_news_seen(self, fingerprint: str, keep_days: int = 14) -> bool:
        cutoff = (datetime.utcnow() - timedelta(days=keep_days)).isoformat()
        with self._connect() as db:
            db.execute("DELETE FROM seen_news WHERE created_at < ?", (cutoff,))
            if db.execute("SELECT 1 FROM seen_news WHERE fingerprint=?", (fingerprint,)).fetchone():
                return False
            db.execute("INSERT INTO seen_news VALUES (?, ?)", (fingerprint, datetime.utcnow().isoformat()))
            return True

    def claim_signal(self, origin: str, code: str, cooldown_seconds: int = 600, now: datetime | None = None) -> bool:
        """Atomically claim a signal slot so cooldown survives restarts and concurrent loops."""
        current = now or datetime.utcnow()
        if current.tzinfo is not None:
            current = current.astimezone(timezone.utc).replace(tzinfo=None)
        current_iso = current.isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT last_sent_at FROM signal_events WHERE origin=? AND code=?",
                (origin, code),
            ).fetchone()
            if row:
                try:
                    previous = datetime.fromisoformat(str(row[0]))
                    if previous.tzinfo is not None:
                        previous = previous.astimezone(timezone.utc).replace(tzinfo=None)
                    if (current - previous).total_seconds() < max(0, cooldown_seconds):
                        return False
                except ValueError:
                    pass
            db.execute(
                "INSERT INTO signal_events(origin, code, last_sent_at) VALUES (?, ?, ?) "
                "ON CONFLICT(origin, code) DO UPDATE SET last_sent_at=excluded.last_sent_at",
                (origin, code, current_iso),
            )
            return True

    def release_signal(self, origin: str, code: str, claimed_at: datetime | None = None) -> None:
        with self._connect() as db:
            if claimed_at is None:
                db.execute("DELETE FROM signal_events WHERE origin=? AND code=?", (origin, code))
                return
            current = claimed_at
            if current.tzinfo is not None:
                current = current.astimezone(timezone.utc).replace(tzinfo=None)
            db.execute(
                "DELETE FROM signal_events WHERE origin=? AND code=? AND last_sent_at=?",
                (origin, code, current.isoformat()),
            )

    def reset_confirmation(self, origin: str, code: str) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM confirmation_events WHERE origin=? AND code=?", (origin, code))

    def observe_confirmation(
        self,
        origin: str,
        code: str,
        required: int,
        max_gap_seconds: int,
        qualifies: bool = True,
        now: datetime | None = None,
    ) -> bool:
        """Atomically record one qualifying observation and report confirmation."""
        try:
            required = max(1, int(required))
        except (TypeError, ValueError):
            required = 1
        try:
            max_gap_seconds = max(0, int(max_gap_seconds))
        except (TypeError, ValueError):
            max_gap_seconds = 0
        current = now or datetime.utcnow()
        if current.tzinfo is not None:
            current = current.astimezone(timezone.utc).replace(tzinfo=None)
        current_iso = current.isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if not qualifies:
                db.execute("DELETE FROM confirmation_events WHERE origin=? AND code=?", (origin, code))
                return False
            row = db.execute(
                "SELECT consecutive_count, last_observed_at FROM confirmation_events WHERE origin=? AND code=?",
                (origin, code),
            ).fetchone()
            count = 1
            if row:
                try:
                    previous = datetime.fromisoformat(str(row[1]))
                    if previous.tzinfo is not None:
                        previous = previous.astimezone(timezone.utc).replace(tzinfo=None)
                    if (current - previous).total_seconds() <= max_gap_seconds:
                        count = max(0, int(row[0])) + 1
                except (TypeError, ValueError):
                    count = 1
            if count >= required:
                db.execute("DELETE FROM confirmation_events WHERE origin=? AND code=?", (origin, code))
                return True
            db.execute(
                "INSERT INTO confirmation_events(origin, code, consecutive_count, last_observed_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(origin, code) DO UPDATE SET consecutive_count=excluded.consecutive_count, last_observed_at=excluded.last_observed_at",
                (origin, code, count, current_iso),
            )
            return False
