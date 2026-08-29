from __future__ import annotations

import sqlite3
import time
import math
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


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
            columns = {row[1] for row in db.execute("PRAGMA table_info(watchlist)")}
            if "cost_price" not in columns:
                try:
                    db.execute("ALTER TABLE watchlist ADD COLUMN cost_price REAL NULL")
                except sqlite3.OperationalError as exc:
                    if "duplicate column" not in str(exc).lower():
                        raise
            if "name" not in columns:
                try:
                    db.execute("ALTER TABLE watchlist ADD COLUMN name TEXT NULL")
                except sqlite3.OperationalError as exc:
                    if "duplicate column" not in str(exc).lower():
                        raise
            quote_columns = {row[1] for row in db.execute("PRAGMA table_info(daily_quotes)")}
            if "source" not in quote_columns:
                try:
                    db.execute("ALTER TABLE daily_quotes ADD COLUMN source TEXT NOT NULL DEFAULT ''")
                except sqlite3.OperationalError as exc:
                    if "duplicate column" not in str(exc).lower():
                        raise
            if "provider_ts" not in quote_columns:
                try:
                    db.execute("ALTER TABLE daily_quotes ADD COLUMN provider_ts TEXT NULL")
                except sqlite3.OperationalError as exc:
                    if "duplicate column" not in str(exc).lower():
                        raise
            candidate_columns = {row[1] for row in db.execute("PRAGMA table_info(screen_candidates)")}
            if "factor_payload" not in candidate_columns:
                db.execute("ALTER TABLE screen_candidates ADD COLUMN factor_payload TEXT NOT NULL DEFAULT '{}'")
            db.execute("INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', '7')")
            # Normalize dates written by pre-v4 releases (YYYYMMDD) so range
            # queries cannot mistake legacy rows for future data.
            legacy = db.execute("SELECT code, trade_date, open, high, low, close, volume, amount, source, fetched_at FROM daily_bars WHERE length(trade_date)=8").fetchall()
            for row in legacy:
                normalized = self._date_norm(str(row[1]))
                db.execute("INSERT OR REPLACE INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?)", (row[0], normalized, row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
                db.execute("DELETE FROM daily_bars WHERE code=? AND trade_date=?", (row[0], row[1]))

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

    def transition_price_state(self, origin: str, code: str, state: str) -> bool:
        now = datetime.utcnow().isoformat()
        with self._connect() as db:
            row = db.execute("SELECT state FROM price_states WHERE origin=? AND code=?", (origin, code)).fetchone()
            changed = not row or str(row[0]) != state
            db.execute("INSERT INTO price_states(origin,code,state,updated_at) VALUES(?,?,?,?) ON CONFLICT(origin,code) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at", (origin, code, state, now))
            return changed

    def price_state(self, origin: str, code: str) -> str | None:
        with self._connect() as db:
            row = db.execute("SELECT state FROM price_states WHERE origin=? AND code=?", (origin, code)).fetchone()
            return str(row[0]) if row else None

    def set_price_state(self, origin: str, code: str, state: str) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("INSERT INTO price_states(origin,code,state,updated_at) VALUES(?,?,?,?) ON CONFLICT(origin,code) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at", (origin, code, state, now))

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
            db.execute("DELETE FROM daily_quotes WHERE trade_date=?", (trade_date,))
            db.executemany("INSERT INTO daily_quotes(trade_date,code,name,price,prev_close,amount,pct_change,volume,fetched_at,source,provider_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            db.execute("DELETE FROM daily_quotes WHERE trade_date < ?", (cutoff,))
        return len(rows)

    def save_snapshot_meta(self, trade_date: str, source: str, quality: str, complete: bool, requested_date: str, note: str = "") -> None:
        if not trade_date:
            return
        with self._connect() as db:
            db.execute(
                "INSERT INTO daily_snapshot_meta(trade_date,source,quality,complete,requested_date,fetched_at,note) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(trade_date) DO UPDATE SET source=excluded.source,quality=excluded.quality,complete=excluded.complete,requested_date=excluded.requested_date,fetched_at=excluded.fetched_at,note=excluded.note",
                (trade_date, source or "unknown", quality or "unknown", int(complete), requested_date or trade_date, datetime.utcnow().isoformat(), note or ""),
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

    def save_daily_bars(self, code: str, bars, source: str = "eastmoney") -> int:
        rows = [(str(code), self._date_norm(str(item.get("trade_date"))), float(item.get("open") or 0), float(item.get("high") or 0), float(item.get("low") or 0), float(item.get("close") or 0), float(item.get("volume") or 0), float(item.get("amount") or 0), source, datetime.utcnow().isoformat()) for item in bars if item.get("trade_date")]
        if not rows:
            return 0
        with self._connect() as db:
            db.executemany("INSERT OR REPLACE INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
        return len(rows)

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

    def save_screen_run(self, run_id: str, job_name: str, requested_date: str, actual_trade_date: str | None,
                        source: str, started_at: str, finished_at: str | None, quote_count: int,
                        candidate_count: int, status: str, quality: str, error: str | None = None) -> None:
        with self._connect() as db:
            db.execute("""INSERT INTO screen_runs(run_id,job_name,requested_date,actual_trade_date,source,started_at,finished_at,quote_count,candidate_count,status,quality,error)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET finished_at=excluded.finished_at, quote_count=excluded.quote_count,
                candidate_count=excluded.candidate_count, status=excluded.status, quality=excluded.quality, error=excluded.error,
                actual_trade_date=excluded.actual_trade_date, source=excluded.source""",
                (run_id, job_name, requested_date, actual_trade_date, source, started_at, finished_at, int(quote_count), int(candidate_count), status, quality, error))

    def save_screen_candidates(self, run_id: str, candidates) -> int:
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
            db.executemany("INSERT OR REPLACE INTO screen_candidates(run_id,code,name,score,score_max,risk_level,risk_flags,price_plan,reasons,factor_payload) VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
        return len(rows)

    def save_screen_bundle(self, run_args: tuple, candidates) -> str:
        run_id = str(run_args[0])
        with self._connect() as db:
            db.execute("INSERT INTO screen_runs(run_id,job_name,requested_date,actual_trade_date,source,started_at,finished_at,quote_count,candidate_count,status,quality,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", run_args)
            import json
            rows=[]
            for c in candidates:
                plan=c.price_plan; pdata={k:getattr(plan,k) for k in plan.__dataclass_fields__} if plan else {}
                overlay=c.factor_overlay; odata={k:getattr(overlay,k) for k in overlay.__dataclass_fields__} if overlay else {}
                rows.append((run_id,c.quote.code,c.quote.name,c.score,c.score_max,c.risk_level,json.dumps(c.risk_flags,ensure_ascii=False),json.dumps(pdata,ensure_ascii=False,default=str),json.dumps(c.reasons,ensure_ascii=False),json.dumps(odata,ensure_ascii=False,default=str)))
            if rows:
                db.executemany("INSERT INTO screen_candidates(run_id,code,name,score,score_max,risk_level,risk_flags,price_plan,reasons,factor_payload) VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
        return run_id

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

    def latest_screen_candidates(self, limit: int = 30) -> list[dict]:
        with self._connect() as db:
            return [dict(row) for row in db.execute("SELECT c.*, r.actual_trade_date, r.source FROM screen_candidates c JOIN screen_runs r ON r.run_id=c.run_id WHERE r.run_id=(SELECT run_id FROM screen_runs WHERE status='completed' ORDER BY started_at DESC LIMIT 1) ORDER BY c.score DESC LIMIT ?", (max(1, min(int(limit), 100)),))]

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

    def save_calendar(self, trade_date: str, is_open: bool, source: str = "") -> None:
        with self._connect() as db:
            db.execute("INSERT OR REPLACE INTO trading_calendar(trade_date,is_open,source,fetched_at) VALUES(?,?,?,?)", (trade_date, int(is_open), source, datetime.utcnow().isoformat()))

    def calendar_status(self, trade_date: str) -> bool | None:
        with self._connect() as db:
            row = db.execute("SELECT is_open FROM trading_calendar WHERE trade_date=?", (trade_date,)).fetchone()
            return bool(row[0]) if row else None

    def save_risk_event(self, event_id: str, run_id: str | None, code: str, state: str, risk_level: str, payload: str, event_at: str) -> bool:
        with self._connect() as db:
            try:
                db.execute("INSERT INTO risk_events(event_id,run_id,code,state,risk_level,event_at,payload) VALUES(?,?,?,?,?,?,?)", (event_id, run_id, code, state, risk_level, event_at, payload))
                return True
            except sqlite3.IntegrityError:
                return False

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
