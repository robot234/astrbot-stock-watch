from __future__ import annotations

import sqlite3
import time
import math
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

LATEST_SCHEMA_VERSION = 12


class StockStore:
    @staticmethod
    def _date_norm(value: str) -> str:
        digits = str(value or "").replace("-", "")
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}" if len(digits) == 8 else str(value or "")
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(5):
            try:
                self._init_db()
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 4:
                    raise
                time.sleep(0.1 * (attempt + 1))

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def schema_version(self) -> int:
        with self._connect() as db:
            row = db.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            try:
                return int(row[0]) if row else 0
            except (TypeError, ValueError):
                return 0

    def _init_db(self):
        """Create the v7 baseline and apply each schema migration exactly once.

        The plugin has shipped several SQLite layouts already.  Keeping the
        migrations small and ordered makes a restart harmless and, more
        importantly, prevents a newer binary from silently rewriting data
        owned by a future schema.
        """
        with self._connect() as db:
            meta_exists = self._table_exists(db, "schema_meta")
            current = 0
            if meta_exists:
                row = db.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
                if row:
                    try:
                        current = int(row[0])
                    except (TypeError, ValueError):
                        raise RuntimeError("invalid stock watch schema version")
            if current > LATEST_SCHEMA_VERSION:
                raise RuntimeError(
                    f"stock watch database schema {current} is newer than supported {LATEST_SCHEMA_VERSION}"
                )

            # v7 is the last pre-migration layout.  CREATE IF NOT EXISTS keeps
            # empty databases and databases made by v0.10 on the same path.
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
                CREATE TABLE IF NOT EXISTS schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS screen_runs(
                    run_id TEXT PRIMARY KEY, job_name TEXT NOT NULL, requested_date TEXT NOT NULL,
                    actual_trade_date TEXT, source TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL,
                    finished_at TEXT, quote_count INTEGER NOT NULL DEFAULT 0, candidate_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'running', quality TEXT NOT NULL DEFAULT 'unknown', error TEXT
                );
                CREATE TABLE IF NOT EXISTS screen_candidates(
                    run_id TEXT NOT NULL, code TEXT NOT NULL, name TEXT NOT NULL, score INTEGER NOT NULL,
                    score_max INTEGER NOT NULL, risk_level TEXT NOT NULL, risk_flags TEXT NOT NULL,
                    price_plan TEXT NOT NULL, reasons TEXT NOT NULL, PRIMARY KEY(run_id, code)
                );
                CREATE TABLE IF NOT EXISTS provider_health(
                    provider TEXT PRIMARY KEY, last_success_at TEXT, last_error_at TEXT,
                    success_count INTEGER NOT NULL DEFAULT 0, error_count INTEGER NOT NULL DEFAULT 0,
                    last_quality TEXT NOT NULL DEFAULT 'unknown'
                );
                CREATE TABLE IF NOT EXISTS trading_calendar(
                    trade_date TEXT PRIMARY KEY, is_open INTEGER NOT NULL,
                    source TEXT NOT NULL DEFAULT '', fetched_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS job_runs(
                    job_key TEXT PRIMARY KEY, job_name TEXT NOT NULL, trade_date TEXT NOT NULL,
                    started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS risk_events(
                    event_id TEXT PRIMARY KEY, run_id TEXT, code TEXT NOT NULL,
                    state TEXT NOT NULL, risk_level TEXT NOT NULL, event_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS result_evaluations(
                    evaluation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, code TEXT NOT NULL,
                    as_of TEXT NOT NULL, horizon INTEGER NOT NULL, status TEXT NOT NULL,
                    close REAL, return_pct REAL, mfe_pct REAL, mae_pct REAL,
                    first_touch TEXT, sample_complete INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS daily_bars(
                    code TEXT NOT NULL, trade_date TEXT NOT NULL, open REAL NOT NULL,
                    high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
                    volume REAL NOT NULL DEFAULT 0, amount REAL NOT NULL DEFAULT 0,
                    source TEXT NOT NULL DEFAULT '', fetched_at TEXT NOT NULL,
                    PRIMARY KEY(code, trade_date)
                );
                CREATE TABLE IF NOT EXISTS factor_snapshots(
                    as_of TEXT NOT NULL, code TEXT NOT NULL, payload TEXT NOT NULL,
                    source TEXT NOT NULL, quality TEXT NOT NULL, fetched_at TEXT NOT NULL,
                    PRIMARY KEY(as_of, code, source)
                );
                CREATE TABLE IF NOT EXISTS market_contexts(
                    as_of TEXT PRIMARY KEY, payload TEXT NOT NULL, source TEXT NOT NULL,
                    quality TEXT NOT NULL, fetched_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS price_states(
                    origin TEXT NOT NULL, code TEXT NOT NULL, state TEXT NOT NULL,
                    updated_at TEXT NOT NULL, PRIMARY KEY(origin, code)
                );
                CREATE TABLE IF NOT EXISTS daily_snapshot_meta(
                    trade_date TEXT PRIMARY KEY, source TEXT NOT NULL, quality TEXT NOT NULL,
                    complete INTEGER NOT NULL DEFAULT 0, requested_date TEXT NOT NULL,
                    fetched_at TEXT NOT NULL, note TEXT NOT NULL DEFAULT ''
                );
            """)
            # Databases predating schema_meta are treated as the v7 layout,
            # while a truly empty database starts at v7 after the baseline is
            # materialised.  We only promise compatibility for v7 and empty
            # stores; malformed older layouts fail loudly instead of guessing.
            if not meta_exists:
                current = 7
            elif current == 0:
                current = 7
            self._ensure_column(db, "watchlist", "cost_price", "REAL NULL")
            self._ensure_column(db, "watchlist", "name", "TEXT NULL")
            self._ensure_column(db, "daily_quotes", "source", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(db, "daily_quotes", "provider_ts", "TEXT NULL")
            self._ensure_column(db, "screen_candidates", "factor_payload", "TEXT NOT NULL DEFAULT '{}'")
            self._set_schema_version(db, max(current, 7))

            migrations = (
                (8, self._migrate_v8_calendar),
                (9, self._migrate_v9_snapshots),
                (10, self._migrate_v10_screen_runs),
                (11, self._migrate_v11_run_scoped_events),
                (12, self._migrate_v12_minute_bars),
            )
            for version, migration in migrations:
                if current < version:
                    migration(db)
                    self._set_schema_version(db, version)
                    current = version
            db.execute(
                """CREATE TABLE IF NOT EXISTS report_versions(
                    report_key TEXT PRIMARY KEY,
                    report_version INTEGER NOT NULL,
                    run_id TEXT NOT NULL,
                    quality TEXT NOT NULL DEFAULT 'unknown',
                    updated_at TEXT NOT NULL
                )"""
            )

            # Normalize dates written by pre-v4 releases so lexical range
            # queries cannot mistake legacy rows for future data.
            legacy = db.execute(
                "SELECT code, trade_date, open, high, low, close, volume, amount, source, fetched_at "
                "FROM daily_bars WHERE length(trade_date)=8"
            ).fetchall()
            for row in legacy:
                normalized = self._date_norm(str(row[1]))
                db.execute(
                    "INSERT OR REPLACE INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (row[0], normalized, row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]),
                )
                db.execute("DELETE FROM daily_bars WHERE code=? AND trade_date=?", (row[0], row[1]))

    @staticmethod
    def _table_exists(db, table: str) -> bool:
        row = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return bool(row)

    @staticmethod
    def _columns(db, table: str) -> set[str]:
        return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}

    @classmethod
    def _ensure_column(cls, db, table: str, column: str, definition: str) -> None:
        if column not in cls._columns(db, table):
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @staticmethod
    def _set_schema_version(db, version: int) -> None:
        db.execute(
            "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(int(version)),),
        )

    @staticmethod
    def _migrate_v8_calendar(db) -> None:
        StockStore._ensure_column(db, "trading_calendar", "status", "TEXT NOT NULL DEFAULT 'unknown'")
        StockStore._ensure_column(db, "trading_calendar", "expires_at", "TEXT NULL")
        db.execute(
            "UPDATE trading_calendar SET status=CASE WHEN is_open=1 THEN 'open' ELSE 'closed' END "
            "WHERE status IS NULL OR status='' OR status='unknown'"
        )

    @staticmethod
    def _migrate_v9_snapshots(db) -> None:
        for column, definition in (
            ("snapshot_version", "INTEGER NOT NULL DEFAULT 1"),
            ("state", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("attempts", "INTEGER NOT NULL DEFAULT 0"),
            ("last_error", "TEXT NULL"),
            ("next_retry_at", "TEXT NULL"),
            ("terminal", "INTEGER NOT NULL DEFAULT 0"),
            ("updated_at", "TEXT NULL"),
        ):
            StockStore._ensure_column(db, "daily_snapshot_meta", column, definition)
        db.execute(
            "UPDATE daily_snapshot_meta SET state=CASE WHEN complete=1 THEN 'complete' "
            "WHEN quality IN ('partial','degraded') THEN 'partial' ELSE 'unknown' END "
            "WHERE state IS NULL OR state='' OR state='unknown'"
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS snapshot_requests(
                request_id TEXT PRIMARY KEY,
                requested_date TEXT NOT NULL,
                actual_trade_date TEXT,
                state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT '',
                quality TEXT NOT NULL DEFAULT 'unknown',
                last_error TEXT,
                next_retry_at TEXT,
                terminal INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )

    @staticmethod
    def _migrate_v10_screen_runs(db) -> None:
        for column, definition in (
            ("outcome", "TEXT NOT NULL DEFAULT 'running'"),
            ("diagnostics", "TEXT NOT NULL DEFAULT '{}'"),
            ("coverage", "REAL NOT NULL DEFAULT 0"),
            ("deep_screen_count", "INTEGER NOT NULL DEFAULT 0"),
            ("factor_screen_count", "INTEGER NOT NULL DEFAULT 0"),
            ("report_key", "TEXT NOT NULL DEFAULT ''"),
            ("report_version", "INTEGER NOT NULL DEFAULT 0"),
            ("candidate_run_id", "TEXT NULL"),
        ):
            StockStore._ensure_column(db, "screen_runs", column, definition)
        db.execute(
            "UPDATE screen_runs SET outcome=CASE WHEN status='completed' THEN 'completed' "
            "WHEN status='failed' THEN 'failed' WHEN status='degraded' THEN 'degraded' ELSE status END "
            "WHERE outcome IS NULL OR outcome='running'"
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS active_candidate_runs(
                scope TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                requested_date TEXT NOT NULL,
                actual_trade_date TEXT,
                valid_until TEXT,
                status TEXT NOT NULL,
                quality TEXT NOT NULL DEFAULT 'unknown',
                coverage REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS report_versions(
                report_key TEXT PRIMARY KEY,
                report_version INTEGER NOT NULL,
                run_id TEXT NOT NULL,
                quality TEXT NOT NULL DEFAULT 'unknown',
                updated_at TEXT NOT NULL
            )"""
        )

    @staticmethod
    def _migrate_v11_run_scoped_events(db) -> None:
        # Rebuild the three state tables so the run is part of the durable key.
        # Legacy rows remain available under the explicit "legacy" run scope.
        db.executescript("""
            CREATE TABLE IF NOT EXISTS signal_events_v11(
                origin TEXT NOT NULL, code TEXT NOT NULL, run_id TEXT NOT NULL DEFAULT 'legacy',
                last_sent_at TEXT NOT NULL, PRIMARY KEY(origin, code, run_id)
            );
            INSERT OR IGNORE INTO signal_events_v11(origin,code,run_id,last_sent_at)
                SELECT origin,code,'legacy',last_sent_at FROM signal_events;
            DROP TABLE signal_events;
            ALTER TABLE signal_events_v11 RENAME TO signal_events;

            CREATE TABLE IF NOT EXISTS confirmation_events_v11(
                origin TEXT NOT NULL, code TEXT NOT NULL, run_id TEXT NOT NULL DEFAULT 'legacy',
                consecutive_count INTEGER NOT NULL, last_observed_at TEXT NOT NULL,
                PRIMARY KEY(origin, code, run_id)
            );
            INSERT OR IGNORE INTO confirmation_events_v11(origin,code,run_id,consecutive_count,last_observed_at)
                SELECT origin,code,'legacy',consecutive_count,last_observed_at FROM confirmation_events;
            DROP TABLE confirmation_events;
            ALTER TABLE confirmation_events_v11 RENAME TO confirmation_events;

            CREATE TABLE IF NOT EXISTS price_states_v11(
                origin TEXT NOT NULL, code TEXT NOT NULL, run_id TEXT NOT NULL DEFAULT 'legacy',
                state TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(origin, code, run_id)
            );
            INSERT OR IGNORE INTO price_states_v11(origin,code,run_id,state,updated_at)
                SELECT origin,code,'legacy',state,updated_at FROM price_states;
            DROP TABLE price_states;
            ALTER TABLE price_states_v11 RENAME TO price_states;
        """)
        StockStore._ensure_column(db, "risk_events", "run_id", "TEXT")
        db.execute("UPDATE risk_events SET run_id='legacy' WHERE run_id IS NULL OR run_id=''")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_risk_events_run_code_state ON risk_events(run_id,code,state)")

    @staticmethod
    def _migrate_v12_minute_bars(db) -> None:
        db.execute(
            """CREATE TABLE IF NOT EXISTS minute_bars(
                code TEXT NOT NULL,
                start_at TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL DEFAULT 0,
                amount REAL NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT '',
                completed_at TEXT NOT NULL,
                PRIMARY KEY(code,start_at)
            )"""
        )

    def save_factor_snapshots(self, as_of: str, rows: dict[str, dict], source: str, quality: str) -> None:
        import json
        if not as_of or not rows:
            return
        now = datetime.utcnow().isoformat()
        values = [(as_of, code, json.dumps(payload, ensure_ascii=False, default=str), source, str(payload.get("quality") or quality), now) for code, payload in rows.items()]
        with self._connect() as db:
            db.executemany("INSERT OR REPLACE INTO factor_snapshots(as_of,code,payload,source,quality,fetched_at) VALUES(?,?,?,?,?,?)", values)

    def save_market_context(self, as_of: str, payload: dict, source: str, quality: str) -> None:
        import json
        if not as_of:
            return
        with self._connect() as db:
            db.execute("INSERT OR REPLACE INTO market_contexts(as_of,payload,source,quality,fetched_at) VALUES(?,?,?,?,?)", (as_of, json.dumps(payload, ensure_ascii=False), source, quality, datetime.utcnow().isoformat()))

    def transition_price_state(self, origin: str, code: str, state: str, run_id: str | None = None) -> bool:
        now = datetime.utcnow().isoformat()
        run_id = str(run_id or "legacy")
        with self._connect() as db:
            row = db.execute("SELECT state FROM price_states WHERE origin=? AND code=? AND run_id=?", (origin, code, run_id)).fetchone()
            changed = not row or str(row[0]) != state
            db.execute("INSERT INTO price_states(origin,code,run_id,state,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(origin,code,run_id) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at", (origin, code, run_id, state, now))
            return changed

    def price_state(self, origin: str, code: str, run_id: str | None = None) -> str | None:
        run_id = str(run_id or "legacy")
        with self._connect() as db:
            row = db.execute("SELECT state FROM price_states WHERE origin=? AND code=? AND run_id=?", (origin, code, run_id)).fetchone()
            return str(row[0]) if row else None

    def set_price_state(self, origin: str, code: str, state: str, run_id: str | None = None) -> None:
        now = datetime.utcnow().isoformat()
        run_id = str(run_id or "legacy")
        with self._connect() as db:
            db.execute("INSERT INTO price_states(origin,code,run_id,state,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(origin,code,run_id) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at", (origin, code, run_id, state, now))

    def price_state_for_run(self, run_id: str, origin: str, code: str) -> str | None:
        return self.price_state(origin, code, run_id=run_id)

    def set_price_state_for_run(self, run_id: str, origin: str, code: str, state: str) -> None:
        self.set_price_state(origin, code, state, run_id=run_id)

    def factor_snapshots(self, as_of: str) -> dict[str, dict]:
        import json
        with self._connect() as db:
            rows = db.execute("SELECT code,payload FROM factor_snapshots WHERE as_of=? ORDER BY fetched_at DESC", (as_of,)).fetchall()
        result: dict[str, dict] = {}
        for row in rows:
            if row[0] in result:
                continue
            try:
                payload = json.loads(row[1])
                if isinstance(payload, dict):
                    result[str(row[0])] = payload
            except (TypeError, ValueError):
                continue
        return result

    def factor_snapshot_meta(self, as_of: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT source,quality,fetched_at,COUNT(*) AS row_count FROM factor_snapshots WHERE as_of=? GROUP BY source,quality,fetched_at ORDER BY fetched_at DESC LIMIT 1", (as_of,)).fetchone()
            return dict(row) if row else None

    def market_context(self, as_of: str) -> dict | None:
        import json
        with self._connect() as db:
            row = db.execute("SELECT payload FROM market_contexts WHERE as_of=?", (as_of,)).fetchone()
        try:
            return json.loads(row[0]) if row else None
        except (TypeError, ValueError):
            return None

    def market_context_meta(self, as_of: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT source,quality,fetched_at FROM market_contexts WHERE as_of=?", (as_of,)).fetchone()
            return dict(row) if row else None

    def add_watch(self, scope: str, code: str, limit: int, cost_price: float | None = None, name: str | None = None) -> bool:
        if cost_price is not None and (not math.isfinite(cost_price) or cost_price <= 0):
            cost_price = None
        clean_name = str(name or "").strip().replace("\n", " ").replace("\r", " ") or None
        with self._connect() as db:
            exists = db.execute("SELECT 1 FROM watchlist WHERE scope=? AND code=?", (scope, code)).fetchone()
            count = db.execute("SELECT COUNT(*) FROM watchlist WHERE scope=?", (scope,)).fetchone()[0]
            if not exists and count >= limit:
                return False
            db.execute(
                "INSERT INTO watchlist(scope, code, created_at, cost_price, name) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(scope, code) DO UPDATE SET cost_price=COALESCE(excluded.cost_price, watchlist.cost_price), "
                "name=COALESCE(excluded.name, watchlist.name)",
                (scope, code, datetime.utcnow().isoformat(), cost_price, clean_name),
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

    def list_watch_details_with_names(self, scope: str) -> list[tuple[str, str | None, float | None]]:
        with self._connect() as db:
            rows = db.execute("SELECT code, name, cost_price FROM watchlist WHERE scope=? ORDER BY code", (scope,))
            result = []
            for code, name, cost in rows:
                try:
                    value = float(cost) if cost is not None and math.isfinite(float(cost)) and float(cost) > 0 else None
                except (TypeError, ValueError):
                    value = None
                clean_name = str(name or "").strip().replace("\n", " ").replace("\r", " ") or None
                result.append((str(code), clean_name, value))
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
             quote.pct_change, quote.volume, quote.fetched_at.isoformat(), getattr(quote, "source", ""), quote.provider_ts.isoformat() if getattr(quote, "provider_ts", None) else None)
            for quote in quotes
        ]
        if not rows:
            return 0
        cutoff = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        with self._connect() as db:
            # A partial retry may contain fewer symbols and must not erase a
            # usable old quote from the same trading date.
            db.executemany(
                "INSERT INTO daily_quotes(trade_date,code,name,price,prev_close,amount,pct_change,volume,fetched_at,source,provider_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(trade_date,code) DO UPDATE SET name=excluded.name,price=excluded.price,"
                "prev_close=excluded.prev_close,amount=excluded.amount,pct_change=excluded.pct_change,"
                "volume=excluded.volume,fetched_at=excluded.fetched_at,source=excluded.source,provider_ts=excluded.provider_ts",
                rows,
            )
            db.execute("DELETE FROM daily_quotes WHERE trade_date < ?", (cutoff,))
        return len(rows)

    @staticmethod
    def _quality_rank(value: str) -> int:
        return {"unknown": 0, "cached": 1, "degraded": 2, "partial": 3, "good": 4}.get(str(value or "").lower(), 0)

    def save_snapshot_meta(
        self,
        trade_date: str,
        source: str,
        quality: str,
        complete: bool,
        requested_date: str,
        note: str = "",
        *,
        snapshot_version: int | None = None,
        state: str | None = None,
        attempts: int | None = None,
        last_error: str | None = None,
        next_retry_at: str | None = None,
        terminal: bool | None = None,
    ) -> None:
        if not trade_date:
            return
        source = str(source or "unknown")
        quality = str(quality or "unknown").lower()
        now = datetime.utcnow().isoformat()
        with self._connect() as db:
            current = db.execute("SELECT * FROM daily_snapshot_meta WHERE trade_date=?", (trade_date,)).fetchone()
            # A later, smaller response is a retry state, not a replacement
            # for a good snapshot. Keep the good payload and only advance
            # durable retry fields.
            downgrade = bool(current) and self._quality_rank(quality) < self._quality_rank(str(current["quality"]))
            requested_attempts = max(0, int(attempts or 0))
            current_attempts = max(0, int(current["attempts"] or 0)) if current else 0
            resolved_attempts = max(current_attempts, requested_attempts)
            if downgrade and str(current["quality"]).lower() == "good":
                # ``attempts`` is an absolute observation from the request
                # state machine.  Never add it again while preserving a good
                # snapshot, even when that good row was not marked complete.
                db.execute(
                    "UPDATE daily_snapshot_meta SET attempts=?,last_error=COALESCE(?,last_error),"
                    "next_retry_at=?,updated_at=? WHERE trade_date=?",
                    (resolved_attempts, last_error, next_retry_at, now, trade_date),
                )
                return
            if current:
                version = max(1, int(snapshot_version or current["snapshot_version"] or 1))
                if not downgrade and snapshot_version is None:
                    version = max(version, int(current["snapshot_version"] or 1) + 1)
            else:
                version = max(1, int(snapshot_version or 1))
            resolved_state = str(state or ("complete" if complete else ("partial" if quality in {"partial", "degraded"} else "unknown")))
            db.execute(
                "INSERT INTO daily_snapshot_meta(trade_date,source,quality,complete,requested_date,fetched_at,note,snapshot_version,state,attempts,last_error,next_retry_at,terminal,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(trade_date) DO UPDATE SET source=excluded.source,quality=excluded.quality,complete=excluded.complete,"
                "requested_date=excluded.requested_date,fetched_at=excluded.fetched_at,note=excluded.note,snapshot_version=excluded.snapshot_version,"
                "state=excluded.state,attempts=excluded.attempts,last_error=excluded.last_error,next_retry_at=excluded.next_retry_at,"
                "terminal=excluded.terminal,updated_at=excluded.updated_at",
                (
                    trade_date,
                    source,
                    quality,
                    int(bool(complete)),
                    requested_date or trade_date,
                    now,
                    note or "",
                    version,
                    resolved_state,
                    resolved_attempts,
                    last_error,
                    next_retry_at,
                    int(bool(terminal)) if terminal is not None else int(bool(current["terminal"])) if current else 0,
                    now,
                ),
            )

    def snapshot_meta(self, trade_date: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM daily_snapshot_meta WHERE trade_date=?", (trade_date,)).fetchone()
            return dict(row) if row else None

    def latest_snapshot_meta(self, before_or_equal: str = "") -> dict | None:
        with self._connect() as db:
            if before_or_equal:
                row = db.execute("SELECT * FROM daily_snapshot_meta WHERE trade_date<=? ORDER BY trade_date DESC LIMIT 1", (before_or_equal,)).fetchone()
            else:
                row = db.execute("SELECT * FROM daily_snapshot_meta ORDER BY trade_date DESC LIMIT 1").fetchone()
            return dict(row) if row else None

    def save_snapshot_request(
        self,
        request_id: str,
        requested_date: str,
        *,
        actual_trade_date: str | None = None,
        state: str = "pending",
        attempts: int = 0,
        source: str = "",
        quality: str = "unknown",
        last_error: str | None = None,
        next_retry_at: str | None = None,
        terminal: bool = False,
    ) -> None:
        if not request_id or not requested_date:
            return
        now = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute(
                "INSERT INTO snapshot_requests(request_id,requested_date,actual_trade_date,state,attempts,source,quality,last_error,next_retry_at,terminal,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(request_id) DO UPDATE SET actual_trade_date=excluded.actual_trade_date,"
                "state=excluded.state,attempts=MAX(snapshot_requests.attempts,excluded.attempts),source=excluded.source,quality=excluded.quality,last_error=excluded.last_error,"
                "next_retry_at=excluded.next_retry_at,terminal=excluded.terminal,updated_at=excluded.updated_at",
                (request_id, requested_date, actual_trade_date, state, max(0, int(attempts)), source or "", quality or "unknown", last_error, next_retry_at, int(bool(terminal)), now, now),
            )

    def snapshot_request(self, request_id: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_id,)).fetchone()
            return dict(row) if row else None

    def pending_snapshot_requests(self, limit: int = 20) -> list[dict]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT * FROM snapshot_requests WHERE terminal=0 AND state NOT IN ('complete','terminal') "
                "ORDER BY updated_at LIMIT ?",
                (max(1, min(int(limit), 100)),),
            )
            return [dict(row) for row in rows]

    def save_daily_bars(self, code: str, bars, source: str = "eastmoney") -> int:
        rows = [(str(code), self._date_norm(str(item.get("trade_date"))), float(item.get("open") or 0), float(item.get("high") or 0), float(item.get("low") or 0), float(item.get("close") or 0), float(item.get("volume") or 0), float(item.get("amount") or 0), source, datetime.utcnow().isoformat()) for item in bars if item.get("trade_date")]
        if not rows:
            return 0
        with self._connect() as db:
            db.executemany("INSERT OR REPLACE INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
        return len(rows)

    def save_minute_bars(self, bars, keep_days: int = 7, source: str = "sina") -> int:
        """Batch-persist completed minute bars and prune old dates in one transaction."""
        import dataclasses
        now = datetime.utcnow().isoformat()
        rows = []
        for bar in bars or []:
            try:
                if dataclasses.is_dataclass(bar):
                    code, start = str(bar.code), bar.start
                    values = (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.amount)
                else:
                    code, start = str(bar.get("code") or ""), bar.get("start") or bar.get("start_at")
                    values = tuple(bar.get(name) for name in ("open", "high", "low", "close", "volume", "amount"))
                if not code or not isinstance(start, datetime):
                    continue
                start = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
                start_at = start.isoformat()
                trade_date = start.astimezone(timezone(timedelta(hours=8))).date().isoformat()
                numbers = tuple(float(value or 0) for value in values)
                if not all(math.isfinite(value) for value in numbers):
                    continue
                rows.append((code, start_at, trade_date, *numbers, source or "", now))
            except (AttributeError, TypeError, ValueError):
                continue
        if not rows:
            return 0
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=max(0, int(keep_days)))).isoformat()
        with self._connect() as db:
            db.executemany(
                "INSERT INTO minute_bars(code,start_at,trade_date,open,high,low,close,volume,amount,source,completed_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(code,start_at) DO UPDATE SET high=excluded.high,low=excluded.low,"
                "close=excluded.close,volume=excluded.volume,amount=excluded.amount,source=excluded.source,completed_at=excluded.completed_at",
                rows,
            )
            db.execute("DELETE FROM minute_bars WHERE trade_date < ?", (cutoff,))
        return len(rows)

    def minute_bars(self, code: str | None = None, trade_date: str | None = None, limit: int = 120) -> list[dict]:
        with self._connect() as db:
            sql = "SELECT * FROM minute_bars WHERE 1=1"
            args: list[object] = []
            if code:
                sql += " AND code=?"
                args.append(str(code))
            if trade_date:
                sql += " AND trade_date=?"
                args.append(self._date_norm(str(trade_date)))
            sql += " ORDER BY start_at DESC LIMIT ?"
            args.append(max(1, min(int(limit), 5000)))
            rows = [dict(row) for row in db.execute(sql, args)]
        return list(reversed(rows))

    def restore_minute_bars(self, trade_date: str, codes=None, limit: int = 120) -> list[dict]:
        values = list(dict.fromkeys(str(code) for code in (codes or []) if str(code)))
        per_code_limit = max(1, min(int(limit), 5000))
        with self._connect() as db:
            sql = "SELECT * FROM minute_bars WHERE trade_date=?"
            args: list[object] = [self._date_norm(str(trade_date))]
            if values:
                rows: list[dict] = []
                for code in values[:900]:
                    code_rows = db.execute(
                        "SELECT * FROM minute_bars WHERE trade_date=? AND code=? ORDER BY start_at DESC LIMIT ?",
                        (self._date_norm(str(trade_date)), code, per_code_limit),
                    ).fetchall()
                    rows.extend(dict(row) for row in reversed(code_rows))
                return rows
            sql += " ORDER BY start_at LIMIT ?"
            args.append(per_code_limit)
            return [dict(row) for row in db.execute(sql, args)]

    def cleanup_minute_bars(self, keep_days: int = 7, before: str | None = None) -> int:
        cutoff = self._date_norm(before) if before else (datetime.now(timezone.utc).date() - timedelta(days=max(0, int(keep_days)))).isoformat()
        with self._connect() as db:
            return db.execute("DELETE FROM minute_bars WHERE trade_date < ?", (cutoff,)).rowcount

    def daily_bars(self, code: str, after: str = "", before_or_equal: str = "") -> list[dict]:
        after = self._date_norm(after) if after else ""
        before_or_equal = self._date_norm(before_or_equal) if before_or_equal else ""
        with self._connect() as db:
            sql = "SELECT * FROM daily_bars WHERE code=?"; args = [code]
            if after:
                sql += " AND trade_date>?"; args.append(after)
            if before_or_equal:
                sql += " AND trade_date<=?"; args.append(before_or_equal)
            sql += " ORDER BY trade_date"
            return [dict(row) for row in db.execute(sql, args)]

    def latest_daily_bars(self, codes, before_or_equal: str = "", limit: int = 60) -> dict[str, list[dict]]:
        """Read bounded, point-in-time-safe daily bars for many codes without a giant IN clause."""
        cutoff = self._date_norm(before_or_equal) if before_or_equal else ""
        size = max(20, min(int(limit), 240))
        values = list(dict.fromkeys(str(code).strip() for code in codes if str(code).strip()))
        result: dict[str, list[dict]] = {}
        with self._connect() as db:
            for code in values:
                sql = "SELECT * FROM daily_bars WHERE code=?"
                args = [code]
                if cutoff:
                    sql += " AND trade_date<=?"
                    args.append(cutoff)
                sql += " ORDER BY trade_date DESC LIMIT ?"
                args.append(size)
                rows = [dict(row) for row in db.execute(sql, args)]
                if rows:
                    result[code] = list(reversed(rows))
        return result

    def daily_quotes(self, trade_date: str) -> list:
        from .core import Quote

        with self._connect() as db:
            rows = db.execute(
                "SELECT code, name, price, prev_close, amount, pct_change, volume, fetched_at, source, provider_ts FROM daily_quotes WHERE trade_date=? ORDER BY code",
                (trade_date,),
            )
            result = []
            for row in rows:
                try:
                    fetched_at = datetime.fromisoformat(str(row[7]))
                except ValueError:
                    fetched_at = datetime.now()
                provider_ts = None
                try:
                    provider_ts = datetime.fromisoformat(str(row[9])) if row[9] else None
                except ValueError:
                    provider_ts = None
                result.append(Quote(str(row[0]), str(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]), float(row[6]), source=str(row[8] or ""), provider_ts=provider_ts, fetched_at=fetched_at))
            return result

    def latest_quote_names(self, codes) -> dict[str, str]:
        """Return the newest usable cached display name for each requested code."""
        values = list(dict.fromkeys(str(code).zfill(6) for code in codes if str(code).zfill(6).isdigit() and len(str(code).zfill(6)) == 6))
        if not values:
            return {}
        result: dict[str, str] = {}
        with self._connect() as db:
            for start in range(0, len(values), 900):
                chunk = values[start:start + 900]
                placeholders = ",".join("?" for _ in chunk)
                rows = db.execute(
                    f"SELECT code,name FROM daily_quotes WHERE code IN ({placeholders}) AND name<>'' AND name<>code ORDER BY trade_date DESC",
                    chunk,
                ).fetchall()
                for row in rows:
                    code, name = str(row[0]), str(row[1]).strip()
                    if code not in result and name and name != code:
                        result[code] = name
        return result

    def latest_daily_trade_date(self, before_or_equal: str) -> str | None:
        """Return the newest locally cached trade date not later than the requested date."""
        with self._connect() as db:
            row = db.execute(
                "SELECT MAX(trade_date) FROM daily_quotes WHERE trade_date<=?",
                (before_or_equal,),
            ).fetchone()
            value = str(row[0] or "").strip() if row else ""
            return value or None

    def daily_trade_dates(self, after: str, limit: int = 30) -> list[str]:
        with self._connect() as db:
            rows = db.execute("SELECT DISTINCT trade_date FROM daily_quotes WHERE trade_date>? ORDER BY trade_date LIMIT ?", (after, max(1, min(int(limit), 200))))
            return [str(row[0]) for row in rows]

    def save_screen_run(
        self,
        run_id: str,
        job_name: str,
        requested_date: str,
        actual_trade_date: str | None,
        source: str,
        started_at: str,
        finished_at: str | None,
        quote_count: int,
        candidate_count: int,
        status: str,
        quality: str,
        error: str | None = None,
        *,
        outcome: str | None = None,
        diagnostics: dict | str | None = None,
        coverage: float = 0.0,
        deep_screen_count: int = 0,
        factor_screen_count: int = 0,
        report_key: str = "",
        report_version: int = 0,
        candidate_run_id: str | None = None,
        coverage_floor: float = 0.8,
    ) -> None:
        import json
        outcome = str(outcome or status or "running")
        if isinstance(diagnostics, dict):
            diagnostics = json.dumps(diagnostics, ensure_ascii=False, default=str)
        diagnostics = str(diagnostics or "{}")
        with self._connect() as db:
            db.execute(
                """INSERT INTO screen_runs(
                    run_id,job_name,requested_date,actual_trade_date,source,started_at,finished_at,
                    quote_count,candidate_count,status,quality,error,outcome,diagnostics,coverage,
                    deep_screen_count,factor_screen_count,report_key,report_version,candidate_run_id
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id) DO UPDATE SET finished_at=excluded.finished_at,quote_count=excluded.quote_count,
                    candidate_count=excluded.candidate_count,status=excluded.status,quality=excluded.quality,
                    error=excluded.error,actual_trade_date=excluded.actual_trade_date,source=excluded.source,
                    outcome=excluded.outcome,diagnostics=excluded.diagnostics,coverage=excluded.coverage,
                    deep_screen_count=excluded.deep_screen_count,factor_screen_count=excluded.factor_screen_count,
                    report_key=excluded.report_key,report_version=excluded.report_version,
                    candidate_run_id=excluded.candidate_run_id""",
                (
                    run_id, job_name, requested_date, actual_trade_date, source, started_at, finished_at,
                    int(quote_count), int(candidate_count), status, quality, error, outcome, diagnostics,
                    max(0.0, min(1.0, float(coverage))), int(deep_screen_count), int(factor_screen_count),
                    report_key or "", int(report_version), candidate_run_id or run_id,
                ),
            )
            if int(candidate_count) == 0 and str(status) == "completed" and float(coverage) >= max(0.0, min(1.0, float(coverage_floor))):
                db.execute("DELETE FROM active_candidate_runs WHERE scope='global'")

    @staticmethod
    def _candidate_valid_until(actual_trade_date: str | None, valid_days: int = 10) -> str | None:
        try:
            value = str(actual_trade_date or "")
            start = datetime.strptime(value, "%Y-%m-%d").date()
            if start.isoformat() != value:
                return None
            valid_days = max(1, min(int(valid_days), 60))
        except (TypeError, ValueError, OverflowError):
            return None
        # This is only a loose integrity bound.  Verified calendar open-count
        # is the sole business validity decision, so weekday arithmetic here
        # must not expire a candidate during an extended market closure.
        safety_days = min(366, max(30, valid_days * 7))
        return f"{(start + timedelta(days=safety_days)).isoformat()}T23:59:59+08:00"

    def save_screen_candidates(self, run_id: str, candidates, *, valid_days: int = 10) -> int:
        import json
        rows = []
        for candidate in candidates:
            plan = candidate.price_plan
            plan_data = {key: getattr(plan, key) for key in plan.__dataclass_fields__} if plan else {}
            overlay = candidate.factor_overlay
            overlay_data = {key: getattr(overlay, key) for key in overlay.__dataclass_fields__} if overlay else {}
            rows.append((run_id, candidate.quote.code, candidate.quote.name, candidate.score, candidate.score_max,
                         candidate.risk_level, json.dumps(candidate.risk_flags, ensure_ascii=False),
                         json.dumps(plan_data, ensure_ascii=False, default=str), json.dumps(candidate.reasons, ensure_ascii=False), json.dumps(overlay_data, ensure_ascii=False, default=str)))
        if not rows:
            return 0
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.executemany("INSERT OR REPLACE INTO screen_candidates(run_id,code,name,score,score_max,risk_level,risk_flags,price_plan,reasons,factor_payload) VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
            run = db.execute("SELECT status,quality,requested_date,actual_trade_date,coverage,job_name,report_key,report_version FROM screen_runs WHERE run_id=?", (run_id,)).fetchone()
            if run and str(run["status"]) in {"completed", "degraded"}:
                valid_until = self._candidate_valid_until(run["actual_trade_date"] or run["requested_date"], valid_days)
                report_key = str(run["report_key"] or "") or f"{run['job_name']}:{run['actual_trade_date'] or run['requested_date']}"
                claimed, allocated = self._claim_report_version_in_tx(db, report_key, run_id, int(run["report_version"] or 0), str(run["quality"] or "unknown"))
                if claimed:
                    db.execute("UPDATE screen_runs SET report_key=?,report_version=? WHERE run_id=?", (report_key, allocated, run_id))
                    db.execute(
                        "INSERT INTO active_candidate_runs(scope,run_id,requested_date,actual_trade_date,valid_until,status,quality,coverage,updated_at) "
                        "VALUES('global',?,?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET run_id=excluded.run_id,requested_date=excluded.requested_date,"
                        "actual_trade_date=excluded.actual_trade_date,valid_until=excluded.valid_until,status=excluded.status,quality=excluded.quality,coverage=excluded.coverage,updated_at=excluded.updated_at",
                        (run_id, run["requested_date"], run["actual_trade_date"], valid_until, run["status"], run["quality"], float(run["coverage"] or 0), datetime.utcnow().isoformat()),
                    )
        return len(rows)

    def _claim_report_version_in_tx(self, db, report_key: str, run_id: str, requested_version: int, quality: str) -> tuple[bool, int]:
        """Allocate and claim a report version while the caller holds its transaction."""
        if not report_key or not run_id:
            return True, max(0, int(requested_version or 0))
        row = db.execute("SELECT report_version,quality FROM report_versions WHERE report_key=?", (report_key,)).fetchone()
        incoming_rank = self._quality_rank(quality)
        if row:
            current_version = max(0, int(row["report_version"] or 0))
            if incoming_rank < self._quality_rank(str(row["quality"])):
                return False, current_version
            allocated = max(current_version + 1, int(requested_version or 0), 1)
        else:
            allocated = max(1, int(requested_version or 0))
        db.execute(
            "INSERT INTO report_versions(report_key,report_version,run_id,quality,updated_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(report_key) DO UPDATE SET report_version=excluded.report_version,run_id=excluded.run_id,"
            "quality=excluded.quality,updated_at=excluded.updated_at",
            (report_key, allocated, run_id, quality or "unknown", datetime.utcnow().isoformat()),
        )
        return True, allocated

    def save_screen_bundle_atomic(
        self,
        run_args: tuple,
        candidates,
        *,
        diagnostics: dict | str | None = None,
        coverage: float = 0.0,
        deep_screen_count: int = 0,
        factor_screen_count: int = 0,
        report_key: str = "",
        report_version: int = 0,
        scope: str = "global",
        valid_until: str | None = None,
        coverage_floor: float = 0.8,
    ) -> dict:
        run_id = str(run_args[0])
        import json
        values = list(run_args)
        if len(values) < 12:
            raise ValueError("screen run arguments require the v7 twelve-field tuple")
        status, quality, error = str(values[9]), str(values[10]), values[11]
        coverage_value = max(0.0, min(1.0, float(coverage)))
        if isinstance(diagnostics, dict):
            diagnostics = json.dumps(diagnostics, ensure_ascii=False, default=str)
        diagnostics = str(diagnostics or "{}")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            report_claimed, allocated_version = self._claim_report_version_in_tx(
                db, report_key, run_id, report_version, quality
            )
            db.execute(
                "INSERT INTO screen_runs(run_id,job_name,requested_date,actual_trade_date,source,started_at,finished_at,quote_count,candidate_count,status,quality,error,outcome,diagnostics,coverage,deep_screen_count,factor_screen_count,report_key,report_version,candidate_run_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(values[:12]) + (
                    status, diagnostics, coverage_value, int(deep_screen_count),
                    int(factor_screen_count), report_key or "", allocated_version, run_id,
                ),
            )
            rows=[]
            for c in candidates:
                plan=c.price_plan; pdata={k:getattr(plan,k) for k in plan.__dataclass_fields__} if plan else {}
                overlay=c.factor_overlay; odata={k:getattr(overlay,k) for k in overlay.__dataclass_fields__} if overlay else {}
                rows.append((run_id,c.quote.code,c.quote.name,c.score,c.score_max,c.risk_level,json.dumps(c.risk_flags,ensure_ascii=False),json.dumps(pdata,ensure_ascii=False,default=str),json.dumps(c.reasons,ensure_ascii=False),json.dumps(odata,ensure_ascii=False,default=str)))
            if rows:
                db.executemany("INSERT INTO screen_candidates(run_id,code,name,score,score_max,risk_level,risk_flags,price_plan,reasons,factor_payload) VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
            # A completed non-empty run becomes the active candidate source.
            # Empty degraded/failed runs intentionally leave the previous
            # source untouched so a transient outage cannot empty monitoring.
            # A report may become active only after its version claim wins in
            # this same transaction.  A lower-quality retry is retained for
            # diagnostics but cannot replace the active pool.
            if report_claimed and status in {"completed", "degraded"} and rows:
                db.execute(
                    "INSERT INTO active_candidate_runs(scope,run_id,requested_date,actual_trade_date,valid_until,status,quality,coverage,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET run_id=excluded.run_id,requested_date=excluded.requested_date,"
                    "actual_trade_date=excluded.actual_trade_date,valid_until=excluded.valid_until,status=excluded.status,quality=excluded.quality,"
                    "coverage=excluded.coverage,updated_at=excluded.updated_at",
                    (scope or "global", run_id, values[2], values[3], valid_until, status, quality, coverage_value, datetime.utcnow().isoformat()),
                )
            elif report_claimed and status == "completed" and not rows and coverage_value >= max(0.0, min(1.0, float(coverage_floor))):
                db.execute("DELETE FROM active_candidate_runs WHERE scope=?", (scope or "global",))
        return {"run_id": run_id, "report_claimed": report_claimed, "report_version": allocated_version}

    def save_screen_bundle(
        self,
        run_args: tuple,
        candidates,
        **kwargs,
    ) -> str:
        """Persist a screen bundle and return its run id (compatibility API)."""
        return str(self.save_screen_bundle_atomic(run_args, candidates, **kwargs)["run_id"])

    def update_provider_health(self, provider: str, success: bool, quality: str, error: str | None = None) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("INSERT INTO provider_health(provider,last_success_at,last_error_at,success_count,error_count,last_quality) VALUES(?,?,?,?,?,?) ON CONFLICT(provider) DO UPDATE SET last_success_at=CASE WHEN ? THEN excluded.last_success_at ELSE provider_health.last_success_at END,last_error_at=CASE WHEN ? THEN provider_health.last_error_at ELSE excluded.last_error_at END,success_count=provider_health.success_count+CASE WHEN ? THEN 1 ELSE 0 END,error_count=provider_health.error_count+CASE WHEN ? THEN 0 ELSE 1 END,last_quality=excluded.last_quality", (provider, now if success else None, None if success else now, int(success), int(not success), quality, int(success), int(success), int(success), int(success)))

    def provider_health_rows(self) -> list[dict]:
        with self._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM provider_health ORDER BY provider")]

    def recent_screen_runs(self, limit: int = 10) -> list[dict]:
        with self._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM screen_runs ORDER BY started_at DESC LIMIT ?", (max(1, min(int(limit), 100)),))]

    def set_active_candidate_run(
        self,
        run_id: str,
        *,
        scope: str = "global",
        requested_date: str = "",
        actual_trade_date: str | None = None,
        valid_until: str | None = None,
        status: str = "completed",
        quality: str = "good",
        coverage: float = 1.0,
    ) -> None:
        if not run_id:
            return
        with self._connect() as db:
            db.execute(
                "INSERT INTO active_candidate_runs(scope,run_id,requested_date,actual_trade_date,valid_until,status,quality,coverage,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET run_id=excluded.run_id,requested_date=excluded.requested_date,"
                "actual_trade_date=excluded.actual_trade_date,valid_until=excluded.valid_until,status=excluded.status,quality=excluded.quality,"
                "coverage=excluded.coverage,updated_at=excluded.updated_at",
                (scope or "global", run_id, requested_date, actual_trade_date, valid_until, status, quality, max(0.0, min(1.0, float(coverage))), datetime.utcnow().isoformat()),
            )

    def clear_active_candidate_run(self, scope: str = "global") -> None:
        with self._connect() as db:
            db.execute("DELETE FROM active_candidate_runs WHERE scope=?", (scope or "global",))

    @staticmethod
    def _valid_until_is_current(value: str | None, *, now: datetime | None = None) -> bool:
        if not value:
            return False
        try:
            expiry = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if expiry.tzinfo is None:
                return False
            current = now or datetime.now(timezone.utc)
            current = current.astimezone(timezone.utc)
            return current <= expiry.astimezone(timezone.utc)
        except (TypeError, ValueError):
            return False

    def active_candidate_run(self, scope: str = "global", *, now: datetime | None = None) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM active_candidate_runs WHERE scope=?", (scope or "global",)).fetchone()
        if not row:
            return None
        result = dict(row)
        if not self._valid_until_is_current(result.get("valid_until"), now=now):
            return None
        return result

    def active_candidate_runs(self) -> list[dict]:
        with self._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM active_candidate_runs ORDER BY scope")]

    def claim_report_version(self, report_key: str, run_id: str, version: int, quality: str = "unknown") -> bool:
        """Accept only a report version newer than the key's current version."""
        if not report_key or not run_id:
            return False
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            claimed, _allocated = self._claim_report_version_in_tx(db, report_key, run_id, version, quality)
            return claimed

    def report_version(self, report_key: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM report_versions WHERE report_key=?", (report_key,)).fetchone()
            return dict(row) if row else None

    def latest_screen_candidates(self, limit: int = 30, scope: str = "global") -> list[dict]:
        with self._connect() as db:
            active = db.execute("SELECT * FROM active_candidate_runs WHERE scope=?", (scope or "global",)).fetchone()
            if active:
                # An active pointer without a valid, timezone-aware expiry is
                # not safe for automatic monitoring or user-facing pool reads.
                if not self._valid_until_is_current(active["valid_until"]):
                    return []
                rows = db.execute(
                    "SELECT c.*,r.actual_trade_date,r.source,r.quality,r.status,r.coverage,a.valid_until FROM screen_candidates c "
                    "JOIN screen_runs r ON r.run_id=c.run_id JOIN active_candidate_runs a ON a.run_id=c.run_id AND a.scope=? "
                    "WHERE c.run_id=? ORDER BY c.score DESC LIMIT ?",
                    (scope or "global", str(active["run_id"]), max(1, min(int(limit), 100))),
                ).fetchall()
                return [dict(row) for row in rows]
            # A verified completed-empty run is an explicit empty pool. Do
            # not fall back to an older non-empty run after it cleared the
            # active pointer. Legacy rows have coverage=0 and keep the v0.10
            # compatibility fallback below.
            empty = db.execute(
                "SELECT started_at FROM screen_runs WHERE status='completed' AND candidate_count=0 AND coverage>=0.8 "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if empty:
                newer_candidate = db.execute(
                    "SELECT 1 FROM screen_runs WHERE candidate_count>0 AND status IN ('completed','degraded') AND started_at>? LIMIT 1",
                    (empty[0],),
                ).fetchone()
                if not newer_candidate:
                    return []
            # Compatibility fallback for stores whose active pointer was not
            # written by an older command implementation.
            return [dict(row) for row in db.execute(
                "SELECT c.*, r.actual_trade_date, r.source, r.quality, r.status, r.coverage FROM screen_candidates c "
                "JOIN screen_runs r ON r.run_id=c.run_id WHERE r.run_id=(SELECT candidate_run.run_id FROM screen_runs candidate_run "
                "WHERE candidate_run.status IN ('completed','degraded') AND EXISTS (SELECT 1 FROM screen_candidates candidate_row WHERE candidate_row.run_id=candidate_run.run_id) "
                "ORDER BY candidate_run.started_at DESC LIMIT 1) ORDER BY c.score DESC LIMIT ?",
                (max(1, min(int(limit), 100)),
            ))]

    def begin_job(self, job_key: str, job_name: str, trade_date: str, lease_seconds: int = 900) -> bool:
        now = datetime.utcnow().isoformat()
        with self._connect() as db:
            try:
                db.execute("INSERT INTO job_runs(job_key,job_name,trade_date,started_at,status) VALUES(?,?,?,?,?)", (job_key, job_name, trade_date, now, "running"))
                return True
            except sqlite3.IntegrityError:
                row = db.execute("SELECT status,started_at FROM job_runs WHERE job_key=?", (job_key,)).fetchone()
                stale = False
                if row and str(row[0]) == "running":
                    try:
                        stale = (datetime.utcnow() - datetime.fromisoformat(str(row[1]))).total_seconds() >= max(60, int(lease_seconds))
                    except (TypeError, ValueError):
                        stale = True
                if row and (str(row[0]) == "failed" or stale):
                    db.execute("UPDATE job_runs SET started_at=?,finished_at=NULL,status='running',error=NULL WHERE job_key=?", (now, job_key))
                    return True
                return False

    def finish_job(self, job_key: str, status: str = "completed", error: str | None = None) -> None:
        with self._connect() as db:
            db.execute("UPDATE job_runs SET finished_at=?, status=?, error=? WHERE job_key=?", (datetime.utcnow().isoformat(), status, error, job_key))

    def save_calendar(
        self,
        trade_date: str,
        is_open: bool | None,
        source: str = "",
        ttl_seconds: int = 86400,
        *,
        status: str | None = None,
        expires_at: str | None = None,
    ) -> None:
        """Persist an open/closed/unknown calendar answer with a bounded TTL."""
        if not trade_date:
            return
        if status is None:
            if isinstance(is_open, str) and is_open.lower() in {"open", "closed", "unknown"}:
                status = is_open.lower()
            else:
                status = "open" if is_open is True else "closed" if is_open is False else "unknown"
        status = str(status).lower()
        if status not in {"open", "closed", "unknown"}:
            status = "unknown"
        is_open_value = 1 if status == "open" else 0
        if expires_at is None:
            expires_at = (datetime.utcnow() + timedelta(seconds=max(0, int(ttl_seconds)))).isoformat()
        now = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute(
                "INSERT INTO trading_calendar(trade_date,is_open,status,source,fetched_at,expires_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(trade_date) DO UPDATE SET is_open=excluded.is_open,status=excluded.status,source=excluded.source,"
                "fetched_at=excluded.fetched_at,expires_at=excluded.expires_at",
                (trade_date, is_open_value, status, source or "", now, expires_at),
            )

    @staticmethod
    def _calendar_status(row, now: datetime) -> tuple[str, str]:
        """Return (status, freshness) without collapsing unknown states."""
        expiry = str(row["expires_at"] or "")
        if not expiry:
            return "unknown", "expired"
        try:
            when = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            if when.tzinfo is not None:
                when = when.astimezone(timezone.utc).replace(tzinfo=None)
            if now > when:
                return "unknown", "expired"
        except (TypeError, ValueError):
            return "unknown", "expired"
        status = str(row["status"] or "").lower()
        if status not in {"open", "closed", "unknown"}:
            status = "open" if row["is_open"] == 1 else "closed"
        return status, "fresh"

    def calendar_lookup(self, trade_date: str, *, now: datetime | None = None) -> dict:
        """Describe a cached calendar answer, preserving fresh unknowns.

        ``calendar_status`` intentionally keeps its historical bool/None API.
        Callers that decide whether a network retry is allowed must use this
        detail API so a fresh unknown is not mistaken for a cache miss.
        """
        with self._connect() as db:
            row = db.execute("SELECT * FROM trading_calendar WHERE trade_date=?", (trade_date,)).fetchone()
        current = now or datetime.utcnow()
        if current.tzinfo is not None:
            current = current.astimezone(timezone.utc).replace(tzinfo=None)
        if not row:
            return {"state": "missing", "status": "unknown", "freshness": "missing", "source": "", "expires_at": None}
        status, freshness = self._calendar_status(row, current)
        state = status if freshness == "fresh" and status != "unknown" else "fresh-unknown" if freshness == "fresh" else freshness
        return {
            "state": state,
            "status": status,
            "freshness": freshness,
            "source": str(row["source"] or ""),
            "fetched_at": row["fetched_at"],
            "expires_at": row["expires_at"],
        }

    def calendar_states(self, start_date: str, end_date: str, *, now: datetime | None = None) -> dict[str, str]:
        """Return fresh open/closed/unknown states for an inclusive date range."""
        start, end = self._date_norm(start_date), self._date_norm(end_date)
        if not start or not end or start > end:
            return {}
        current = now or datetime.utcnow()
        if current.tzinfo is not None:
            current = current.astimezone(timezone.utc).replace(tzinfo=None)
        with self._connect() as db:
            rows = db.execute(
                "SELECT * FROM trading_calendar WHERE trade_date>=? AND trade_date<=?",
                (start, end),
            ).fetchall()
        result: dict[str, str] = {}
        for row in rows:
            status, _freshness = self._calendar_status(row, current)
            result[str(row["trade_date"])] = status
        return result

    def calendar_state(self, trade_date: str, *, now: datetime | None = None) -> str:
        return str(self.calendar_lookup(trade_date, now=now)["status"])

    def calendar_status(self, trade_date: str, now: datetime | None = None) -> bool | None:
        state = self.calendar_state(trade_date, now=now)
        return True if state == "open" else False if state == "closed" else None

    def save_risk_event(self, event_id: str, run_id: str | None, code: str, state: str, risk_level: str, payload: str, event_at: str) -> bool:
        run_id = str(run_id or "legacy")
        with self._connect() as db:
            try:
                db.execute("INSERT INTO risk_events(event_id,run_id,code,state,risk_level,event_at,payload) VALUES(?,?,?,?,?,?,?)", (event_id, run_id, code, state, risk_level, event_at, payload))
                return True
            except sqlite3.IntegrityError:
                return False

    def risk_events(self, run_id: str | None = None, code: str | None = None, limit: int = 100) -> list[dict]:
        with self._connect() as db:
            sql = "SELECT * FROM risk_events WHERE 1=1"
            args: list[object] = []
            if run_id:
                sql += " AND run_id=?"
                args.append(run_id)
            if code:
                sql += " AND code=?"
                args.append(code)
            sql += " ORDER BY event_at DESC LIMIT ?"
            args.append(max(1, min(int(limit), 500)))
            return [dict(row) for row in db.execute(sql, args)]

    def save_result_evaluation(self, evaluation_id: str, run_id: str, code: str, as_of: str, horizon: int, status: str, close: float | None, return_pct: float | None, mfe_pct: float | None, mae_pct: float | None, first_touch: str | None, sample_complete: bool) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO result_evaluations(evaluation_id,run_id,code,as_of,horizon,status,close,return_pct,mfe_pct,mae_pct,first_touch,sample_complete,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(evaluation_id) DO UPDATE SET status=excluded.status,close=excluded.close,return_pct=excluded.return_pct,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,first_touch=excluded.first_touch,sample_complete=excluded.sample_complete,created_at=excluded.created_at", (evaluation_id, run_id, code, as_of, int(horizon), status, close, return_pct, mfe_pct, mae_pct, first_touch, int(sample_complete), datetime.utcnow().isoformat()))

    def evaluations(self, run_id: str | None = None, limit: int = 50) -> list[dict]:
        with self._connect() as db:
            if run_id:
                rows = db.execute("SELECT * FROM result_evaluations WHERE run_id=? ORDER BY created_at DESC LIMIT ?", (run_id, max(1, min(int(limit), 200))))
            else:
                rows = db.execute("SELECT * FROM result_evaluations ORDER BY created_at DESC LIMIT ?", (max(1, min(int(limit), 200)),))
            return [dict(row) for row in rows]

    def mark_news_seen(self, fingerprint: str, keep_days: int = 14) -> bool:
        cutoff = (datetime.utcnow() - timedelta(days=keep_days)).isoformat()
        with self._connect() as db:
            db.execute("DELETE FROM seen_news WHERE created_at < ?", (cutoff,))
            if db.execute("SELECT 1 FROM seen_news WHERE fingerprint=?", (fingerprint,)).fetchone():
                return False
            db.execute("INSERT INTO seen_news VALUES (?, ?)", (fingerprint, datetime.utcnow().isoformat()))
            return True

    def claim_signal(
        self,
        origin: str,
        code: str,
        cooldown_seconds: int | str = 600,
        now: datetime | None = None,
        run_id: str | None = None,
    ) -> bool:
        """Atomically claim a signal slot so cooldown survives restarts and concurrent loops."""
        # Accept ``claim_signal(origin, code, run_id)`` as a convenient
        # run-scoped shorthand while preserving the v0.10 signature.
        if isinstance(cooldown_seconds, str):
            # Also accept the natural ``(run_id, origin, code)`` ordering.
            if str(cooldown_seconds).isdigit() and len(str(cooldown_seconds)) == 6 and not (str(code).isdigit() and len(str(code)) == 6):
                run_id, origin, code, cooldown_seconds = origin, code, cooldown_seconds, 600
            else:
                run_id, cooldown_seconds = cooldown_seconds, 600
        run_id = str(run_id or "legacy")
        current = now or datetime.utcnow()
        if current.tzinfo is not None:
            current = current.astimezone(timezone.utc).replace(tzinfo=None)
        current_iso = current.isoformat()
        try:
            cooldown_seconds = max(0, int(cooldown_seconds))
        except (TypeError, ValueError):
            cooldown_seconds = 600
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT last_sent_at FROM signal_events WHERE origin=? AND code=? AND run_id=?",
                (origin, code, run_id),
            ).fetchone()
            if row:
                try:
                    previous = datetime.fromisoformat(str(row[0]))
                    if previous.tzinfo is not None:
                        previous = previous.astimezone(timezone.utc).replace(tzinfo=None)
                    if (current - previous).total_seconds() < cooldown_seconds:
                        return False
                except ValueError:
                    pass
            db.execute(
                "INSERT INTO signal_events(origin, code, run_id, last_sent_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(origin, code, run_id) DO UPDATE SET last_sent_at=excluded.last_sent_at",
                (origin, code, run_id, current_iso),
            )
            return True

    def claim_signal_for_run(self, run_id: str, origin: str, code: str, cooldown_seconds: int = 600, now: datetime | None = None) -> bool:
        return self.claim_signal(origin, code, cooldown_seconds, now=now, run_id=run_id)

    def release_signal(self, origin: str, code: str, claimed_at: datetime | None = None, run_id: str | None = None) -> None:
        run_id = str(run_id or "legacy")
        with self._connect() as db:
            if claimed_at is None:
                db.execute("DELETE FROM signal_events WHERE origin=? AND code=? AND run_id=?", (origin, code, run_id))
                return
            current = claimed_at
            if current.tzinfo is not None:
                current = current.astimezone(timezone.utc).replace(tzinfo=None)
            db.execute(
                "DELETE FROM signal_events WHERE origin=? AND code=? AND run_id=? AND last_sent_at=?",
                (origin, code, run_id, current.isoformat()),
            )

    def release_signal_for_run(self, run_id: str, origin: str, code: str, claimed_at: datetime | None = None) -> None:
        self.release_signal(origin, code, claimed_at=claimed_at, run_id=run_id)

    def reset_confirmation(self, origin: str, code: str, run_id: str | None = None) -> None:
        run_id = str(run_id or "legacy")
        with self._connect() as db:
            db.execute("DELETE FROM confirmation_events WHERE origin=? AND code=? AND run_id=?", (origin, code, run_id))

    def reset_confirmation_for_run(self, run_id: str, origin: str, code: str) -> None:
        self.reset_confirmation(origin, code, run_id=run_id)

    def observe_confirmation(
        self,
        origin: str,
        code: str,
        required: int,
        max_gap_seconds: int,
        qualifies: bool = True,
        now: datetime | None = None,
        run_id: str | None = None,
    ) -> bool:
        """Atomically record one qualifying observation and report confirmation."""
        run_id = str(run_id or "legacy")
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
                db.execute("DELETE FROM confirmation_events WHERE origin=? AND code=? AND run_id=?", (origin, code, run_id))
                return False
            row = db.execute(
                "SELECT consecutive_count, last_observed_at FROM confirmation_events WHERE origin=? AND code=? AND run_id=?",
                (origin, code, run_id),
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
                db.execute("DELETE FROM confirmation_events WHERE origin=? AND code=? AND run_id=?", (origin, code, run_id))
                return True
            db.execute(
                "INSERT INTO confirmation_events(origin, code, run_id, consecutive_count, last_observed_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(origin, code, run_id) DO UPDATE SET consecutive_count=excluded.consecutive_count, last_observed_at=excluded.last_observed_at",
                (origin, code, run_id, count, current_iso),
            )
            return False

    def observe_confirmation_for_run(
        self,
        run_id: str,
        origin: str,
        code: str,
        required: int,
        max_gap_seconds: int,
        qualifies: bool = True,
        now: datetime | None = None,
    ) -> bool:
        return self.observe_confirmation(origin, code, required, max_gap_seconds, qualifies, now, run_id=run_id)
