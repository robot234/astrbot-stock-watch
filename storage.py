from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
import math
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

LATEST_SCHEMA_VERSION = 14
DEFAULT_RAW_MIN_ROW_COUNT = 4000
DEFAULT_RAW_REQUIRE_UNIVERSE_EVIDENCE = True
DEFAULT_PROVIDER_BUCKETS = {
    "trade_cal": (1, 60),
    "stock_basic": (1, 60),
    "daily": (30, 60),
}


class SnapshotLeaseError(RuntimeError):
    """A durable snapshot lease operation could not be completed safely."""


class SnapshotLeaseCapabilityError(SnapshotLeaseError):
    """The configured storage cannot safely provide the lease contract."""


class SnapshotLeaseLostError(SnapshotLeaseError):
    """A snapshot owner attempted to mutate a lease it no longer owns."""


class RawBatchRead:
    """A point-in-time reader pinned to one published raw generation."""

    def __init__(self, connection, read_id: str, batch_id: str, dataset_id: str, generation: int, basis: str, source: str):
        self._db = connection
        self.read_id = read_id
        self.batch_id = batch_id
        self.dataset_id = dataset_id
        self.generation = int(generation)
        self.basis = basis
        self.source = source

    def _rows(self, codes=None, before_or_equal: str = "", after: str = "") -> list[dict]:
        values: list[str] = []
        for code in codes or []:
            text = str(code).strip()
            if not text:
                continue
            canonical = StockStore._canonical_raw_code(text)
            values.append(canonical[0] if canonical else text)
        values = list(dict.fromkeys(values))
        base_sql = (
            "SELECT pb.* FROM batch_days bd "
            "JOIN day_partitions dp ON dp.partition_id=bd.partition_id "
            "JOIN partition_bars pb ON pb.partition_id=dp.partition_id "
            "WHERE bd.batch_id=?"
        )
        base_args: list[object] = [self.batch_id]
        if before_or_equal:
            base_sql += " AND pb.trade_date<=?"
            base_args.append(StockStore._date_norm(before_or_equal))
        if after:
            base_sql += " AND pb.trade_date>?"
            base_args.append(StockStore._date_norm(after))
        # SQLite deployments commonly cap one statement at 999 bind
        # parameters.  Query code filters in bounded chunks instead of
        # silently dropping symbols beyond that limit.
        chunks = [values[index:index + 900] for index in range(0, len(values), 900)] if values else [None]
        rows: list[dict] = []
        for chunk in chunks:
            sql, args = base_sql, list(base_args)
            if chunk:
                placeholders = ",".join("?" for _ in chunk)
                sql += f" AND pb.code IN ({placeholders})"
                args.extend(chunk)
            rows.extend(dict(row) for row in self._db.execute(sql, args))
        rows.sort(key=lambda row: (str(row.get("trade_date") or ""), str(row.get("code") or "")))
        for row in rows:
            if str(row.get("basis") or "").strip().lower() != str(self.basis or "").strip().lower():
                raise RuntimeError("mixed raw batch basis")
            if str(row.get("source") or "") != self.source:
                raise RuntimeError("mixed raw batch source")
            # ``calculate_daily_indicators`` consumes the historical name
            # while raw storage keeps the shorter schema-level ``basis``.
            row["price_basis"] = row.get("basis")
        return rows

    def rows(self, codes=None, before_or_equal: str = "", after: str = "") -> list[dict]:
        """Return rows from this batch only, in deterministic date/code order."""
        return self._rows(codes, before_or_equal, after)

    def bars(self, codes=None, before_or_equal: str = "", after: str = "") -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        for row in self._rows(codes, before_or_equal, after):
            result.setdefault(str(row["code"]), []).append(row)
        return result

    def iter_rows(self, codes=None, before_or_equal: str = "", after: str = ""):
        yield from self._rows(codes, before_or_equal, after)


class StockStore:
    # Main uses this marker together with method signatures to select the
    # durable path before invoking any lease operation.
    SNAPSHOT_LEASE_CAPABILITY = "fenced-v1"

    @staticmethod
    def _date_norm(value: str) -> str:
        digits = str(value or "").replace("-", "")
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}" if len(digits) == 8 else str(value or "")
    def __init__(self, path: Path):
        self.path = path
        self._raw_batch_validation_cache: dict[str, dict] = {}
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
        connection.execute("PRAGMA foreign_keys=ON")
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
                (13, self._migrate_v13_provenance_and_symbols),
                (14, self._migrate_v14_raw_dataset_generations),
            )
            pending = [(version, migration) for version, migration in migrations if current < version]
            if pending:
                # Baseline creation/repair is deliberately committed before
                # migrations.  Every pending migration and its version marker
                # then share one explicit transaction, so a failure at any
                # point leaves the database at the last committed schema.
                db.commit()
                db.execute("BEGIN IMMEDIATE")
                try:
                    for version, migration in pending:
                        migration(db)
                        self._set_schema_version(db, version)
                        current = version
                    # Keep partially-created v13 stores repairable, but keep
                    # these repairs inside the same 7->13 transaction.
                    self._ensure_column(db, "daily_bars", "price_basis", "TEXT NOT NULL DEFAULT 'unknown'")
                    self._ensure_column(db, "result_evaluations", "price_basis", "TEXT NOT NULL DEFAULT 'unknown'")
                    self._ensure_column(db, "result_evaluations", "plan_validated", "INTEGER NOT NULL DEFAULT 0")
                    db.execute(
                        """CREATE TABLE IF NOT EXISTS stock_symbols(
                            code TEXT PRIMARY KEY,
                            name TEXT NOT NULL DEFAULT '',
                            normalized_name TEXT NOT NULL DEFAULT '',
                            source TEXT NOT NULL DEFAULT '',
                            updated_at TEXT NOT NULL
                        )"""
                    )
                    db.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbols_normalized_name ON stock_symbols(normalized_name)")
                    db.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbols_code_prefix ON stock_symbols(code)")
                    db.execute(
                        """CREATE TABLE IF NOT EXISTS report_versions(
                            report_key TEXT PRIMARY KEY,
                            report_version INTEGER NOT NULL,
                            run_id TEXT NOT NULL,
                            quality TEXT NOT NULL DEFAULT 'unknown',
                            updated_at TEXT NOT NULL
                        )"""
                    )
                    self._normalize_legacy_daily_bar_dates(db)
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
            # Keep partially-created v13 stores repairable and make the
            # legacy date normalization below safe after an interrupted DDL.
            if not pending:
                self._ensure_column(db, "daily_bars", "price_basis", "TEXT NOT NULL DEFAULT 'unknown'")
                self._ensure_column(db, "result_evaluations", "price_basis", "TEXT NOT NULL DEFAULT 'unknown'")
                self._ensure_column(db, "result_evaluations", "plan_validated", "INTEGER NOT NULL DEFAULT 0")
                db.execute(
                    """CREATE TABLE IF NOT EXISTS stock_symbols(
                        code TEXT PRIMARY KEY,
                        name TEXT NOT NULL DEFAULT '',
                        normalized_name TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT '',
                        updated_at TEXT NOT NULL
                    )"""
                )
                db.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbols_normalized_name ON stock_symbols(normalized_name)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbols_code_prefix ON stock_symbols(code)")
                db.execute(
                    """CREATE TABLE IF NOT EXISTS report_versions(
                        report_key TEXT PRIMARY KEY,
                        report_version INTEGER NOT NULL,
                        run_id TEXT NOT NULL,
                        quality TEXT NOT NULL DEFAULT 'unknown',
                        updated_at TEXT NOT NULL
                    )"""
                )
                self._normalize_legacy_daily_bar_dates(db)

            # v14 is still unreleased, but local stores may already carry the
            # first draft of its raw tables.  Add the manifest/coverage fields
            # in place so those databases fail closed until republished.
            self._ensure_v14_raw_columns(db)
            # Provider throttling/cache state is also draft-v14.  Keep this
            # repair additive so opening an existing v14 store is harmless.
            self._ensure_v14_provider_tables(db)
            # Snapshot request ownership is also additive within schema 14.
            # The lease fence is deliberately durable so an expired owner can
            # never publish or release after a later owner takes over.
            self._ensure_v14_snapshot_lease_columns(db)
            # Universe evidence uses durable per-status staging so a process
            # restart can resume L/D/P without activating a partial set.
            self._ensure_v14_universe_tables(db)
            # Automatic close publication and delivery state are additive v14
            # durability records. Existing v14 databases receive them without
            # rewriting raw-market tables or changing the schema marker.
            self._ensure_v14_automatic_close_tables(db)
            # Intraday signal state and delivery records are additive as well.
            # They intentionally share the v14 marker so existing production
            # stores can adopt durable cooldown/outbox behavior in place.
            self._ensure_v14_intraday_tables(db)
            # Recommendation snapshots and their strictly point-in-time
            # outcome records are additive. They never alter an historical
            # candidate or infer a missing calendar/price observation.
            self._ensure_v14_recommendation_tables(db)

    @staticmethod
    def _table_exists(db, table: str) -> bool:
        row = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return bool(row)

    @staticmethod
    def _columns(db, table: str) -> set[str]:
        return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}

    @staticmethod
    def _ensure_v14_automatic_close_tables(db) -> None:
        job_columns = {str(row[1]) for row in db.execute("PRAGMA table_info(job_runs)")}
        for column, definition in (
            ("automatic_attempts", "INTEGER NOT NULL DEFAULT 0"),
            ("automatic_first_started_at", "TEXT"),
            ("automatic_next_retry_at", "REAL NOT NULL DEFAULT 0"),
            ("automatic_terminal_reason", "TEXT NOT NULL DEFAULT ''"),
        ):
            if column not in job_columns:
                db.execute(f"ALTER TABLE job_runs ADD COLUMN {column} {definition}")
        db.execute(
            """CREATE TABLE IF NOT EXISTS automatic_close_publications(
                publication_key TEXT PRIMARY KEY,
                actual_trade_date TEXT NOT NULL UNIQUE,
                requested_date TEXT NOT NULL,
                run_id TEXT NOT NULL UNIQUE,
                invocation_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                origins_json TEXT NOT NULL DEFAULT '[]',
                outbox_prepared INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
        publication_columns = {str(row[1]) for row in db.execute("PRAGMA table_info(automatic_close_publications)")}
        if "outbox_prepared" not in publication_columns:
            db.execute("ALTER TABLE automatic_close_publications ADD COLUMN outbox_prepared INTEGER NOT NULL DEFAULT 0")
        db.execute(
            """CREATE TABLE IF NOT EXISTS automatic_close_deliveries(
                delivery_id TEXT PRIMARY KEY,
                publication_key TEXT NOT NULL REFERENCES automatic_close_publications(publication_key) ON DELETE CASCADE,
                actual_trade_date TEXT NOT NULL,
                origin TEXT NOT NULL,
                run_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','sending','sent','failed','unknown_delivery','cancelled')),
                attempts INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_fence INTEGER NOT NULL DEFAULT 0,
                lease_expires_at REAL NOT NULL DEFAULT 0,
                next_retry_at REAL NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                sent_at TEXT,
                UNIQUE(publication_key,origin)
            )"""
        )
        db.execute("CREATE INDEX IF NOT EXISTS idx_automatic_close_delivery_recovery ON automatic_close_deliveries(state,next_retry_at,lease_expires_at)")

    @classmethod
    def _ensure_v14_intraday_tables(cls, db) -> None:
        db.execute(
            """CREATE TABLE IF NOT EXISTS intraday_origin_preferences(
                origin TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            )"""
        )

        db.execute(
            """CREATE TABLE IF NOT EXISTS intraday_signal_states(
                origin TEXT NOT NULL,
                code TEXT NOT NULL,
                signal TEXT NOT NULL,
                plan_version TEXT NOT NULL,
                armed INTEGER NOT NULL DEFAULT 1,
                consecutive_count INTEGER NOT NULL DEFAULT 0,
                last_condition INTEGER NOT NULL DEFAULT 0,
                last_observed_at REAL NOT NULL DEFAULT 0,
                last_triggered_at REAL NOT NULL DEFAULT 0,
                trigger_count INTEGER NOT NULL DEFAULT 0,
                last_reason TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(origin,code,signal,plan_version)
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS intraday_event_outbox(
                event_key TEXT PRIMARY KEY,
                origin TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                signal TEXT NOT NULL,
                plan_version TEXT NOT NULL,
                event_sequence INTEGER NOT NULL,
                run_id TEXT NOT NULL DEFAULT '',
                invocation_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                quote_fetched_at TEXT NOT NULL DEFAULT '',
                candidate_valid_until TEXT NOT NULL DEFAULT '',
                market_regime TEXT NOT NULL DEFAULT '',
                market_snapshot_at TEXT NOT NULL DEFAULT '',
                risk_event INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','sending','sent','failed','unknown_delivery','cancelled')),
                attempts INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_fence INTEGER NOT NULL DEFAULT 0,
                lease_expires_at REAL NOT NULL DEFAULT 0,
                next_retry_at REAL NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                sent_at TEXT,
                UNIQUE(origin,code,signal,plan_version,event_sequence)
            )"""
        )
        for column, definition in (
            ("quote_fetched_at", "TEXT NOT NULL DEFAULT ''"),
            ("candidate_valid_until", "TEXT NOT NULL DEFAULT ''"),
            ("market_regime", "TEXT NOT NULL DEFAULT ''"),
            ("market_snapshot_at", "TEXT NOT NULL DEFAULT ''"),
            ("risk_event", "INTEGER NOT NULL DEFAULT 0"),
        ):
            cls._ensure_column(db, "intraday_event_outbox", column, definition)
        db.execute("CREATE INDEX IF NOT EXISTS idx_intraday_outbox_recovery ON intraday_event_outbox(state,next_retry_at,lease_expires_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_intraday_outbox_origin ON intraday_event_outbox(origin,created_at)")
        db.execute(
            """CREATE TABLE IF NOT EXISTS intraday_market_regime_state(
                scope TEXT PRIMARY KEY,
                regime TEXT NOT NULL DEFAULT 'unknown',
                pending_regime TEXT NOT NULL DEFAULT 'unknown',
                pending_count INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT '',
                source_timestamp TEXT NOT NULL DEFAULT '',
                sample_size INTEGER NOT NULL DEFAULT 0,
                expected_size INTEGER NOT NULL DEFAULT 0,
                coverage REAL NOT NULL DEFAULT 0,
                breadth REAL,
                advancing INTEGER NOT NULL DEFAULT 0,
                declining INTEGER NOT NULL DEFAULT 0,
                flat INTEGER NOT NULL DEFAULT 0,
                median_return REAL,
                quote_timestamp_min TEXT NOT NULL DEFAULT '',
                quote_timestamp_max TEXT NOT NULL DEFAULT '',
                quality TEXT NOT NULL DEFAULT 'unknown',
                reason TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )"""
        )

    @staticmethod
    def _ensure_v14_recommendation_tables(db) -> None:
        db.execute("""CREATE TABLE IF NOT EXISTS recommendation_records(
            recommendation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, recommended_date TEXT NOT NULL,
            code TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', candidate_price REAL, confirmation_price REAL,
            attention_low REAL, attention_high REAL, invalidation_price REAL, confirmation_level REAL,
            target_low REAL, target_high REAL, plan_version TEXT NOT NULL, market_regime TEXT NOT NULL DEFAULT 'unknown',
            data_timestamp TEXT NOT NULL DEFAULT '', freshness TEXT NOT NULL DEFAULT 'unknown', source TEXT NOT NULL DEFAULT '',
            caller_identity TEXT NOT NULL DEFAULT '', price_basis TEXT NOT NULL DEFAULT 'unknown', prediction_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL, UNIQUE(run_id,code))""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_recommendation_records_date ON recommendation_records(recommended_date,code)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_recommendation_records_version ON recommendation_records(plan_version,market_regime)")
        db.execute("""CREATE TABLE IF NOT EXISTS recommendation_outcomes(
            recommendation_id TEXT NOT NULL REFERENCES recommendation_records(recommendation_id) ON DELETE CASCADE,
            horizon INTEGER NOT NULL CHECK(horizon IN (1,3,5,10)), evaluated_through TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL CHECK(status IN ('pending','complete','unknown','unknown_order')), close_price REAL, return_pct REAL,
            max_gain_pct REAL, max_drawdown_pct REAL, confirmation_order TEXT NOT NULL DEFAULT 'not_touched',
            target_order TEXT NOT NULL DEFAULT 'not_touched', invalidation_order TEXT NOT NULL DEFAULT 'not_touched',
            first_touch TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', sample_complete INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL, PRIMARY KEY(recommendation_id,horizon))""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_recommendation_outcomes_status ON recommendation_outcomes(horizon,status,updated_at)")
        for table, column, definition in (
            ("recommendation_records", "origin", "TEXT NOT NULL DEFAULT 'global'"),
            ("recommendation_records", "visibility", "TEXT NOT NULL DEFAULT 'public'"),
            ("recommendation_records", "strategy_version", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("recommendation_records", "plan_status", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("recommendation_records", "plan_reason", "TEXT NOT NULL DEFAULT ''"),
            ("recommendation_records", "comparability_status", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("recommendation_outcomes", "session_complete", "INTEGER NOT NULL DEFAULT 0"),
            ("recommendation_outcomes", "price_basis", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("recommendation_outcomes", "event_order", "TEXT NOT NULL DEFAULT 'not_touched'"),
            ("daily_bars", "corporate_action_factor", "REAL"),
            ("daily_bars", "corporate_action_evidence", "TEXT NOT NULL DEFAULT ''"),
        ):
            StockStore._ensure_column(db, table, column, definition)

    @classmethod
    def _ensure_column(cls, db, table: str, column: str, definition: str) -> None:
        if column not in cls._columns(db, table):
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @classmethod
    def _ensure_v14_raw_columns(cls, db) -> None:
        if not cls._table_exists(db, "batches"):
            return
        for column, definition in (
            ("manifest_version", "INTEGER NOT NULL DEFAULT 1"),
            ("shadow", "INTEGER NOT NULL DEFAULT 0"),
            ("publication_mode", "TEXT NOT NULL DEFAULT 'active'"),
            ("manifest_hash", "TEXT NOT NULL DEFAULT ''"),
            ("expected_dates_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("coverage_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("universe_version", "TEXT NOT NULL DEFAULT ''"),
            ("universe_counts_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("require_universe_evidence", "INTEGER NOT NULL DEFAULT 1"),
            ("bj_calendar_policy", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("min_row_count", "INTEGER NOT NULL DEFAULT 4000"),
            ("min_overall_coverage", "REAL NOT NULL DEFAULT 0.97"),
            ("min_market_coverage", "REAL NOT NULL DEFAULT 0.95"),
            ("min_market_median_ratio", "REAL NOT NULL DEFAULT 0.95"),
            ("page_size", "INTEGER NOT NULL DEFAULT 6000"),
        ):
            cls._ensure_column(db, "batches", column, definition)
        for column, definition in (
            ("market_counts_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("digest_version", "INTEGER NOT NULL DEFAULT 1"),
        ):
            cls._ensure_column(db, "day_partitions", column, definition)
        # Keep unfiltered server-page metadata separate from filtered bar
        # partitions.  A policy filter can turn a full page into an empty
        # stored partition, while pagination must still resume from the raw
        # server count and terminal bit.
        db.execute(
            """CREATE TABLE IF NOT EXISTS raw_page_metadata(
                batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
                trade_date TEXT NOT NULL,
                partition_no INTEGER NOT NULL CHECK(partition_no >= 0),
                server_row_count INTEGER NOT NULL CHECK(server_row_count >= 0),
                server_terminal INTEGER NOT NULL DEFAULT 0 CHECK(server_terminal IN (0,1)),
                server_page_digest TEXT NOT NULL CHECK(length(trim(server_page_digest)) > 0),
                filter_policy TEXT NOT NULL DEFAULT 'include_all' CHECK(length(trim(filter_policy)) > 0),
                server_rows_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                PRIMARY KEY(batch_id,trade_date,partition_no)
            )"""
        )
        if cls._table_exists(db, "raw_page_metadata"):
            for column, definition in (
                ("trade_date", "TEXT NOT NULL DEFAULT ''"),
                ("partition_no", "INTEGER NOT NULL DEFAULT 0"),
                ("server_row_count", "INTEGER NOT NULL DEFAULT 0"),
                ("server_terminal", "INTEGER NOT NULL DEFAULT 0"),
                ("server_page_digest", "TEXT NOT NULL DEFAULT ''"),
                ("filter_policy", "TEXT NOT NULL DEFAULT 'include_all'"),
                ("server_rows_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ):
                cls._ensure_column(db, "raw_page_metadata", column, definition)
        db.execute("CREATE INDEX IF NOT EXISTS idx_raw_page_metadata_lookup ON raw_page_metadata(batch_id,trade_date,partition_no)")
        if cls._table_exists(db, "result_evaluations"):
            for column, definition in (
                ("evaluation_dataset_id", "TEXT NULL"),
                ("evaluation_batch_id", "TEXT NULL"),
                ("evaluation_generation", "INTEGER NULL"),
            ):
                cls._ensure_column(db, "result_evaluations", column, definition)
        db.execute("CREATE INDEX IF NOT EXISTS idx_batches_manifest ON batches(dataset_id,status,manifest_hash)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_partition_digest ON day_partitions(dataset_id,trade_date,content_hash)")

    @classmethod
    def _ensure_v14_provider_tables(cls, db) -> None:
        """Create/repair provider state without rewriting legacy tables.

        This is intentionally additive.  A v14 database may have been
        interrupted between individual DDL statements, so every column and
        index is repaired idempotently on every open.  The foreign key and
        checks live on the fresh draft-v14 tables; old application tables are
        never rebuilt just to add constraints.
        """
        db.execute(
            """CREATE TABLE IF NOT EXISTS provider_api_state(
                api_name TEXT PRIMARY KEY CHECK(length(trim(api_name)) > 0),
                bucket_limit INTEGER NOT NULL DEFAULT 1 CHECK(bucket_limit > 0),
                window_seconds INTEGER NOT NULL DEFAULT 60 CHECK(window_seconds > 0),
                window_started_at REAL NOT NULL DEFAULT 0 CHECK(window_started_at >= 0),
                request_count INTEGER NOT NULL DEFAULT 0 CHECK(request_count >= 0),
                blocked_until REAL NOT NULL DEFAULT 0 CHECK(blocked_until >= 0),
                retry_after REAL NOT NULL DEFAULT 0 CHECK(retry_after >= 0),
                rate_limit_failures INTEGER NOT NULL DEFAULT 0 CHECK(rate_limit_failures >= 0),
                failure_streak INTEGER NOT NULL DEFAULT 0 CHECK(failure_streak >= 0),
                circuit_open_until REAL NOT NULL DEFAULT 0 CHECK(circuit_open_until >= 0),
                last_error TEXT NOT NULL DEFAULT '',
                state_digest TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS provider_cache(
                cache_key TEXT PRIMARY KEY CHECK(length(trim(cache_key)) > 0),
                api_name TEXT NOT NULL REFERENCES provider_api_state(api_name) ON DELETE CASCADE,
                request_digest TEXT NOT NULL CHECK(length(trim(request_digest)) > 0),
                payload_digest TEXT NOT NULL CHECK(length(trim(payload_digest)) > 0),
                response_digest TEXT NOT NULL CHECK(length(trim(response_digest)) > 0),
                body_json TEXT NOT NULL,
                cache_version INTEGER NOT NULL DEFAULT 1 CHECK(cache_version > 0),
                created_at TEXT NOT NULL,
                expires_at REAL NOT NULL CHECK(expires_at >= 0),
                status TEXT NOT NULL DEFAULT 'valid' CHECK(status IN ('valid','stale','invalid')),
                UNIQUE(api_name, request_digest)
            )"""
        )
        # Draft-v14 repair for databases where a table was created before a
        # later process crash.  ALTER TABLE is additive and therefore keeps
        # all legacy schema layouts untouched.
        if cls._table_exists(db, "provider_api_state"):
            for column, definition in (
                ("bucket_limit", "INTEGER NOT NULL DEFAULT 1"),
                ("window_seconds", "INTEGER NOT NULL DEFAULT 60"),
                ("window_started_at", "REAL NOT NULL DEFAULT 0"),
                ("request_count", "INTEGER NOT NULL DEFAULT 0"),
                ("blocked_until", "REAL NOT NULL DEFAULT 0"),
                ("retry_after", "REAL NOT NULL DEFAULT 0"),
                ("rate_limit_failures", "INTEGER NOT NULL DEFAULT 0"),
                ("failure_streak", "INTEGER NOT NULL DEFAULT 0"),
                ("circuit_open_until", "REAL NOT NULL DEFAULT 0"),
                ("last_error", "TEXT NOT NULL DEFAULT ''"),
                ("state_digest", "TEXT NOT NULL DEFAULT ''"),
                ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ):
                cls._ensure_column(db, "provider_api_state", column, definition)
        if cls._table_exists(db, "provider_cache"):
            for column, definition in (
                ("api_name", "TEXT NOT NULL DEFAULT 'daily'"),
                ("request_digest", "TEXT NOT NULL DEFAULT 'legacy'"),
                ("payload_digest", "TEXT NOT NULL DEFAULT 'legacy'"),
                ("response_digest", "TEXT NOT NULL DEFAULT 'legacy'"),
                ("body_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("cache_version", "INTEGER NOT NULL DEFAULT 1"),
                ("created_at", "TEXT NOT NULL DEFAULT ''"),
                ("expires_at", "REAL NOT NULL DEFAULT 0"),
                ("status", "TEXT NOT NULL DEFAULT 'valid'"),
            ):
                cls._ensure_column(db, "provider_cache", column, definition)
        db.execute("CREATE INDEX IF NOT EXISTS idx_provider_api_updated ON provider_api_state(updated_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_provider_cache_expiry ON provider_cache(api_name,expires_at,status)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_provider_cache_digest ON provider_cache(api_name,request_digest,response_digest)")

    @classmethod
    def _ensure_v14_snapshot_lease_columns(cls, db) -> None:
        """Add the schema-14 request lease/fence fields idempotently."""
        if not cls._table_exists(db, "snapshot_requests"):
            return
        for column, definition in (
            ("lease_owner", "TEXT NOT NULL DEFAULT ''"),
            ("lease_fence", "INTEGER NOT NULL DEFAULT 0"),
            ("lease_expires_at", "REAL NOT NULL DEFAULT 0"),
            ("lease_updated_at", "TEXT NOT NULL DEFAULT ''"),
            ("failure_kind", "TEXT NOT NULL DEFAULT ''"),
            ("calendar_evidence_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("provenance_json", "TEXT NOT NULL DEFAULT '{}'"),
        ):
            cls._ensure_column(db, "snapshot_requests", column, definition)
        db.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_requests_lease ON snapshot_requests(lease_expires_at,state,terminal)")

    @classmethod
    def _ensure_v14_universe_tables(cls, db) -> None:
        """Create the durable L/D/P evidence staging and activation tables."""
        db.execute(
            """CREATE TABLE IF NOT EXISTS universe_evidence_batches(
                evidence_batch_id TEXT PRIMARY KEY CHECK(length(trim(evidence_batch_id)) > 0),
                evidence_key TEXT NOT NULL UNIQUE CHECK(length(trim(evidence_key)) > 0),
                cycle_digest TEXT NOT NULL DEFAULT '',
                provider TEXT NOT NULL DEFAULT 'tushare' CHECK(length(trim(provider)) > 0),
                effective_date TEXT NOT NULL,
                bj_calendar_policy TEXT NOT NULL DEFAULT 'sse_fallback',
                universe_version TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'staging' CHECK(status IN ('staging','published','failed')),
                expected_statuses_json TEXT NOT NULL DEFAULT '["D","L","P"]',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                evidence_digest TEXT NOT NULL DEFAULT '',
                recovery_of TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS universe_evidence_statuses(
                evidence_batch_id TEXT NOT NULL REFERENCES universe_evidence_batches(evidence_batch_id) ON DELETE CASCADE,
                list_status TEXT NOT NULL CHECK(list_status IN ('L','D','P')),
                cycle_digest TEXT NOT NULL DEFAULT '',
                rows_json TEXT NOT NULL DEFAULT '[]',
                row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count >= 0),
                content_hash TEXT NOT NULL DEFAULT '',
                request_digest TEXT NOT NULL DEFAULT '',
                payload_digest TEXT NOT NULL DEFAULT '',
                response_digest TEXT NOT NULL DEFAULT '',
                integrity_digest TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'validated' CHECK(status IN ('staging','validated','rejected')),
                updated_at TEXT NOT NULL,
                PRIMARY KEY(evidence_batch_id,list_status)
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS active_universe_evidence(
                evidence_key TEXT PRIMARY KEY REFERENCES universe_evidence_batches(evidence_key) ON DELETE RESTRICT,
                evidence_batch_id TEXT NOT NULL REFERENCES universe_evidence_batches(evidence_batch_id) ON DELETE RESTRICT,
                provider TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                bj_calendar_policy TEXT NOT NULL,
                universe_version TEXT NOT NULL DEFAULT '',
                evidence_digest TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(provider,effective_date,bj_calendar_policy,universe_version)
            )"""
        )
        # Complete only interrupted additive DDL; constrained key columns are
        # repaired by the normal v14 migration on a fresh database.
        if cls._table_exists(db, "universe_evidence_batches"):
            for column, definition in (
                ("evidence_key", "TEXT NOT NULL DEFAULT 'legacy-evidence'"),
                ("cycle_digest", "TEXT NOT NULL DEFAULT ''"),
                ("provider", "TEXT NOT NULL DEFAULT 'tushare'"),
                ("effective_date", "TEXT NOT NULL DEFAULT ''"),
                ("bj_calendar_policy", "TEXT NOT NULL DEFAULT 'sse_fallback'"),
                ("universe_version", "TEXT NOT NULL DEFAULT ''"),
                ("status", "TEXT NOT NULL DEFAULT 'staging'"),
                ("expected_statuses_json", "TEXT NOT NULL DEFAULT '[\"D\",\"L\",\"P\"]'"),
                ("evidence_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("evidence_digest", "TEXT NOT NULL DEFAULT ''"),
                ("recovery_of", "TEXT NULL"),
                ("created_at", "TEXT NOT NULL DEFAULT ''"),
                ("updated_at", "TEXT NOT NULL DEFAULT ''"),
                ("error", "TEXT"),
            ):
                cls._ensure_column(db, "universe_evidence_batches", column, definition)
        if cls._table_exists(db, "universe_evidence_statuses"):
            for column, definition in (
                ("list_status", "TEXT NOT NULL DEFAULT 'L'"),
                ("cycle_digest", "TEXT NOT NULL DEFAULT ''"),
                ("rows_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("row_count", "INTEGER NOT NULL DEFAULT 0"),
                ("content_hash", "TEXT NOT NULL DEFAULT ''"),
                ("request_digest", "TEXT NOT NULL DEFAULT ''"),
                ("payload_digest", "TEXT NOT NULL DEFAULT ''"),
                ("response_digest", "TEXT NOT NULL DEFAULT ''"),
                ("integrity_digest", "TEXT NOT NULL DEFAULT ''"),
                ("status", "TEXT NOT NULL DEFAULT 'validated'"),
                ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ):
                cls._ensure_column(db, "universe_evidence_statuses", column, definition)
        if cls._table_exists(db, "active_universe_evidence"):
            for column, definition in (
                ("evidence_batch_id", "TEXT NOT NULL DEFAULT ''"),
                ("provider", "TEXT NOT NULL DEFAULT 'tushare'"),
                ("effective_date", "TEXT NOT NULL DEFAULT ''"),
                ("bj_calendar_policy", "TEXT NOT NULL DEFAULT 'sse_fallback'"),
                ("universe_version", "TEXT NOT NULL DEFAULT ''"),
                ("evidence_digest", "TEXT NOT NULL DEFAULT ''"),
                ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ):
                cls._ensure_column(db, "active_universe_evidence", column, definition)
        db.execute("CREATE INDEX IF NOT EXISTS idx_universe_evidence_lookup ON universe_evidence_batches(provider,effective_date,bj_calendar_policy,status)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_universe_evidence_statuses ON universe_evidence_statuses(evidence_batch_id,status)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_active_universe_evidence_date ON active_universe_evidence(provider,effective_date)")
        # A status row is reusable only when it belongs to the same durable
        # fetch cycle as the batch.  Backfill interrupted draft rows so old
        # stores remain readable without silently mixing cycles.
        batch_cycles: dict[str, str] = {}
        for row in db.execute("SELECT evidence_batch_id, evidence_key, cycle_digest FROM universe_evidence_batches"):
            batch_id = str(row[0] or "")
            evidence_key = str(row[1] or "")
            cycle_digest = str(row[2] or "").strip().lower()
            if not cycle_digest:
                cycle_digest = cls._universe_evidence_cycle_digest(evidence_key, batch_id)
                db.execute(
                    "UPDATE universe_evidence_batches SET cycle_digest=? WHERE evidence_batch_id=? AND (cycle_digest IS NULL OR cycle_digest='')",
                    (cycle_digest, batch_id),
                )
            batch_cycles[batch_id] = cycle_digest
        for batch_id, cycle_digest in batch_cycles.items():
            db.execute(
                "UPDATE universe_evidence_statuses SET cycle_digest=? WHERE evidence_batch_id=? AND (cycle_digest IS NULL OR cycle_digest='')",
                (cycle_digest, batch_id),
            )
        # Complete interrupted status rows without blessing malformed data.
        # A missing response digest from an early draft is deterministically
        # derived from its canonical rows; an existing value remains part of
        # the integrity commitment and is never silently rewritten.
        for row in db.execute(
            "SELECT evidence_batch_id,list_status,cycle_digest,rows_json,row_count,content_hash,request_digest,payload_digest,response_digest,integrity_digest FROM universe_evidence_statuses"
        ):
            if str(row[9] or "").strip():
                continue
            try:
                parsed = json.loads(str(row[3] or "[]"))
                normalized = cls._normalize_universe_status_rows(str(row[1] or ""), parsed)
                rows_text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                computed_content = hashlib.sha256(rows_text.encode("utf-8")).hexdigest()
                content_hash = str(row[5] or "").strip().lower() or computed_content
                response_digest = str(row[8] or "").strip().lower() or computed_content
                integrity = cls._universe_status_integrity_digest(
                    str(row[2] or ""), str(row[1] or ""), content_hash,
                    str(row[6] or ""), str(row[7] or ""), response_digest,
                )
            except (TypeError, ValueError, KeyError, OverflowError, json.JSONDecodeError):
                continue
            db.execute(
                "UPDATE universe_evidence_statuses SET row_count=?,content_hash=?,response_digest=?,integrity_digest=? WHERE evidence_batch_id=? AND list_status=? AND (integrity_digest IS NULL OR integrity_digest='')",
                (len(normalized), content_hash, response_digest, integrity, str(row[0]), str(row[1])),
            )
        for alias, table in (
            ("raw_universe_evidence_batches", "universe_evidence_batches"),
            ("raw_universe_evidence_statuses", "universe_evidence_statuses"),
            ("raw_active_universe_evidence", "active_universe_evidence"),
        ):
            db.execute(f"CREATE VIEW IF NOT EXISTS {alias} AS SELECT * FROM {table}")
        cls._repair_v14_provider_constraints(db)

    @staticmethod
    def _provider_contract_sql(db, table: str) -> str:
        row = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        return str(row[0] or "").lower() if row else ""

    @classmethod
    def _provider_state_contract_ok(cls, db) -> bool:
        if not cls._table_exists(db, "provider_api_state"):
            return False
        info = {str(row[1]): int(row[5] or 0) for row in db.execute("PRAGMA table_info(provider_api_state)")}
        sql = cls._provider_contract_sql(db, "provider_api_state")
        return info.get("api_name") == 1 and "check" in sql

    @classmethod
    def _provider_cache_contract_ok(cls, db) -> bool:
        if not cls._table_exists(db, "provider_cache"):
            return False
        info = {str(row[1]): int(row[5] or 0) for row in db.execute("PRAGMA table_info(provider_cache)")}
        sql = cls._provider_contract_sql(db, "provider_cache")
        foreign_keys = {
            str(row[3]): str(row[2])
            for row in db.execute("PRAGMA foreign_key_list(provider_cache)")
        }
        return (
            info.get("cache_key") == 1
            and foreign_keys.get("api_name") == "provider_api_state"
            and "check" in sql
        )

    @staticmethod
    def _provider_int(value, default: int, minimum: int = 0, maximum: int | None = None) -> int:
        try:
            if isinstance(value, bool):
                raise ValueError
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            parsed = default
        parsed = max(minimum, parsed)
        if maximum is not None:
            parsed = min(maximum, parsed)
        return parsed

    @staticmethod
    def _provider_float(value, default: float, minimum: float = 0.0) -> float:
        try:
            parsed = float(value)
            if not math.isfinite(parsed):
                raise ValueError
        except (TypeError, ValueError, OverflowError):
            parsed = default
        return max(minimum, parsed)

    @classmethod
    def _repair_v14_provider_constraints(cls, db) -> None:
        """Rebuild interrupted draft-v14 provider tables with real constraints.

        ``ALTER TABLE`` can add columns but cannot add a primary key or a
        foreign key.  A process crash between draft-v14 DDL statements could
        therefore leave a table that looked complete to ``CREATE IF NOT
        EXISTS`` while allowing duplicate state/cache rows.  Rebuild only
        when the contract is actually missing and copy sanitised rows into
        the canonical tables; ordinary v14 opens remain additive/idempotent.
        """
        state_exists = cls._table_exists(db, "provider_api_state")
        cache_exists = cls._table_exists(db, "provider_cache")
        state_rebuild = state_exists and not cls._provider_state_contract_ok(db)
        cache_rebuild = cache_exists and not cls._provider_cache_contract_ok(db)
        if not state_rebuild and not cache_rebuild:
            return

        state_rows = []
        cache_rows = []
        if state_exists:
            try:
                state_rows = [dict(row) for row in db.execute("SELECT * FROM provider_api_state")]
            except sqlite3.Error:
                state_rows = []
        if cache_exists:
            try:
                cache_rows = [dict(row) for row in db.execute("SELECT * FROM provider_cache")]
            except sqlite3.Error:
                cache_rows = []

        # A child table must be removed before its parent can be replaced when
        # foreign_keys=ON.  Cache rows are restored after state rows.
        if cache_rebuild or state_rebuild:
            if cache_exists:
                db.execute("DROP TABLE provider_cache")
        if state_rebuild and state_exists:
            db.execute("DROP TABLE provider_api_state")

        db.execute(
            """CREATE TABLE IF NOT EXISTS provider_api_state(
                api_name TEXT PRIMARY KEY CHECK(length(trim(api_name)) > 0),
                bucket_limit INTEGER NOT NULL DEFAULT 1 CHECK(bucket_limit > 0),
                window_seconds INTEGER NOT NULL DEFAULT 60 CHECK(window_seconds > 0),
                window_started_at REAL NOT NULL DEFAULT 0 CHECK(window_started_at >= 0),
                request_count INTEGER NOT NULL DEFAULT 0 CHECK(request_count >= 0),
                blocked_until REAL NOT NULL DEFAULT 0 CHECK(blocked_until >= 0),
                retry_after REAL NOT NULL DEFAULT 0 CHECK(retry_after >= 0),
                rate_limit_failures INTEGER NOT NULL DEFAULT 0 CHECK(rate_limit_failures >= 0),
                failure_streak INTEGER NOT NULL DEFAULT 0 CHECK(failure_streak >= 0),
                circuit_open_until REAL NOT NULL DEFAULT 0 CHECK(circuit_open_until >= 0),
                last_error TEXT NOT NULL DEFAULT '',
                state_digest TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )"""
        )

        restored_names: set[str] = set()
        for row in state_rows:
            name = str(row.get("api_name") or "").strip().lower()
            if not name or name in restored_names:
                continue
            restored_names.add(name)
            state = {
                "api_name": name,
                "bucket_limit": cls._provider_int(row.get("bucket_limit"), 1, 1),
                "window_seconds": cls._provider_int(row.get("window_seconds"), 60, 1),
                "window_started_at": cls._provider_float(row.get("window_started_at"), 0.0),
                "request_count": cls._provider_int(row.get("request_count"), 0),
                "blocked_until": cls._provider_float(row.get("blocked_until"), 0.0),
                "retry_after": cls._provider_float(row.get("retry_after"), 0.0),
                "rate_limit_failures": cls._provider_int(row.get("rate_limit_failures"), 0),
                "failure_streak": cls._provider_int(row.get("failure_streak"), 0),
                "circuit_open_until": cls._provider_float(row.get("circuit_open_until"), 0.0),
                "last_error": str(row.get("last_error") or "")[:240],
                "updated_at": str(row.get("updated_at") or "") or cls._provider_now_text(),
            }
            state["state_digest"] = cls._provider_state_digest(state)
            db.execute(
                "INSERT INTO provider_api_state(api_name,bucket_limit,window_seconds,window_started_at,request_count,blocked_until,retry_after,rate_limit_failures,failure_streak,circuit_open_until,last_error,state_digest,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(state[key] for key in (
                    "api_name", "bucket_limit", "window_seconds", "window_started_at", "request_count",
                    "blocked_until", "retry_after", "rate_limit_failures", "failure_streak", "circuit_open_until",
                    "last_error", "state_digest", "updated_at",
                )),
            )

        # If only the cache table was malformed, existing valid state rows are
        # still present.  Restore every cache row under a unique key and create
        # a default state row for orphaned draft entries.
        if cache_rebuild or state_rebuild or not cache_exists:
            db.execute(
                """CREATE TABLE IF NOT EXISTS provider_cache(
                    cache_key TEXT PRIMARY KEY CHECK(length(trim(cache_key)) > 0),
                    api_name TEXT NOT NULL REFERENCES provider_api_state(api_name) ON DELETE CASCADE,
                    request_digest TEXT NOT NULL CHECK(length(trim(request_digest)) > 0),
                    payload_digest TEXT NOT NULL CHECK(length(trim(payload_digest)) > 0),
                    response_digest TEXT NOT NULL CHECK(length(trim(response_digest)) > 0),
                    body_json TEXT NOT NULL,
                    cache_version INTEGER NOT NULL DEFAULT 1 CHECK(cache_version > 0),
                    created_at TEXT NOT NULL,
                    expires_at REAL NOT NULL CHECK(expires_at >= 0),
                    status TEXT NOT NULL DEFAULT 'valid' CHECK(status IN ('valid','stale','invalid')),
                    UNIQUE(api_name, request_digest)
                )"""
            )
            used_keys: set[str] = set()
            for row in cache_rows:
                api_name = str(row.get("api_name") or "").strip().lower() or "daily"
                if api_name not in restored_names:
                    state = {
                        "api_name": api_name, "bucket_limit": 1, "window_seconds": 60,
                        "window_started_at": 0.0, "request_count": 0, "blocked_until": 0.0,
                        "retry_after": 0.0, "rate_limit_failures": 0, "failure_streak": 0,
                        "circuit_open_until": 0.0, "last_error": "", "updated_at": cls._provider_now_text(),
                    }
                    state["state_digest"] = cls._provider_state_digest(state)
                    db.execute(
                        "INSERT OR IGNORE INTO provider_api_state(api_name,bucket_limit,window_seconds,window_started_at,request_count,blocked_until,retry_after,rate_limit_failures,failure_streak,circuit_open_until,last_error,state_digest,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        tuple(state[key] for key in (
                            "api_name", "bucket_limit", "window_seconds", "window_started_at", "request_count",
                            "blocked_until", "retry_after", "rate_limit_failures", "failure_streak", "circuit_open_until",
                            "last_error", "state_digest", "updated_at",
                        )),
                    )
                    restored_names.add(api_name)
                body_json = str(row.get("body_json") or "{}")
                try:
                    parsed_body = json.loads(body_json)
                    if not isinstance(parsed_body, dict):
                        raise ValueError
                except (TypeError, ValueError, json.JSONDecodeError):
                    parsed_body = {}
                body_json = json.dumps(parsed_body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                response_digest = hashlib.sha256(body_json.encode("utf-8")).hexdigest()
                request_digest = str(row.get("request_digest") or "").strip() or hashlib.sha256((api_name + body_json).encode("utf-8")).hexdigest()
                payload_digest = str(row.get("payload_digest") or "").strip() or request_digest
                key = str(row.get("cache_key") or "").strip() or f"{api_name}:{request_digest}"
                base_key = key
                suffix = 1
                while key in used_keys:
                    suffix += 1
                    key = f"{base_key}:{suffix}"
                used_keys.add(key)
                db.execute(
                    "INSERT OR IGNORE INTO provider_cache(cache_key,api_name,request_digest,payload_digest,response_digest,body_json,cache_version,created_at,expires_at,status) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        key, api_name, request_digest, payload_digest, response_digest, body_json,
                        cls._provider_int(row.get("cache_version"), 1, 1),
                        str(row.get("created_at") or "") or cls._provider_now_text(),
                        cls._provider_float(row.get("expires_at"), 0.0),
                        str(row.get("status") or "valid") if str(row.get("status") or "valid") in {"valid", "stale", "invalid"} else "invalid",
                    ),
                )

        db.execute("CREATE INDEX IF NOT EXISTS idx_provider_api_updated ON provider_api_state(updated_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_provider_cache_expiry ON provider_cache(api_name,expires_at,status)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_provider_cache_digest ON provider_cache(api_name,request_digest,response_digest)")

    @staticmethod
    def _provider_now_text() -> str:
        return datetime.utcnow().isoformat()

    @classmethod
    def _provider_state_digest(cls, state: dict) -> str:
        payload = {
            key: state.get(key)
            for key in (
                "api_name", "bucket_limit", "window_seconds", "window_started_at",
                "request_count", "blocked_until", "retry_after", "rate_limit_failures",
                "failure_streak", "circuit_open_until", "last_error",
            )
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()

    @classmethod
    def _provider_state_row(cls, db, api_name: str, bucket_limit: int = 1, window_seconds: int = 60):
        name = str(api_name or "").strip().lower()
        if not name:
            raise ValueError("provider api name is required")
        try:
            limit = max(1, int(bucket_limit))
            window = max(1, int(window_seconds))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("provider bucket settings are invalid") from exc
        now_text = cls._provider_now_text()
        db.execute(
            "INSERT INTO provider_api_state(api_name,bucket_limit,window_seconds,updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(api_name) DO NOTHING",
            (name, limit, window, now_text),
        )
        row = db.execute("SELECT * FROM provider_api_state WHERE api_name=?", (name,)).fetchone()
        if not row:
            raise RuntimeError("provider api state could not be created")
        try:
            current_limit = int(row["bucket_limit"] or 0)
            current_window = int(row["window_seconds"] or 0)
        except (TypeError, ValueError, OverflowError):
            current_limit, current_window = 0, 0
        if current_limit != limit or current_window != window:
            state = dict(row)
            state.update({"bucket_limit": limit, "window_seconds": window})
            state["state_digest"] = cls._provider_state_digest(state)
            db.execute(
                "UPDATE provider_api_state SET bucket_limit=?,window_seconds=?,state_digest=?,updated_at=? WHERE api_name=?",
                (limit, window, state["state_digest"], now_text, name),
            )
            row = db.execute("SELECT * FROM provider_api_state WHERE api_name=?", (name,)).fetchone()
        return row

    def provider_api_state(self, api_name: str, *, bucket_limit: int = 1, window_seconds: int = 60) -> dict:
        """Return persisted state for one API bucket, creating it if needed."""
        with self._connect() as db:
            row = self._provider_state_row(db, api_name, bucket_limit, window_seconds)
            return dict(row)

    get_provider_api_state = provider_api_state

    def reserve_provider_api_request(
        self,
        api_name: str,
        *,
        bucket_limit: int = 1,
        window_seconds: int = 60,
        now: float | None = None,
    ) -> dict:
        """Atomically reserve one short request slot.

        The caller must perform any waiting after this method returns.  The
        SQLite write transaction is deliberately kept free of network and
        sleep operations, which prevents a rate-limited task from holding the
        database lock while other APIs try to make progress.
        """
        current = float(time.time() if now is None else now)
        if not math.isfinite(current) or current < 0:
            raise ValueError("provider clock value is invalid")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._provider_state_row(db, api_name, bucket_limit, window_seconds)
            state = dict(row)
            name = str(state["api_name"])
            blocked = max(float(state.get("blocked_until") or 0), float(state.get("circuit_open_until") or 0))
            if blocked > current:
                state["wait_seconds"] = blocked - current
                state["allowed"] = False
                return state
            limit = max(1, int(state.get("bucket_limit") or bucket_limit))
            window = max(1, int(state.get("window_seconds") or window_seconds))
            started = float(state.get("window_started_at") or 0)
            count = max(0, int(state.get("request_count") or 0))
            if (started <= 0 and count <= 0) or current - started >= window:
                started, count = current, 0
            if count >= limit:
                wait = max(0.0, started + window - current)
                state["wait_seconds"] = wait
                state["allowed"] = False
                db.execute(
                    "UPDATE provider_api_state SET window_started_at=?,request_count=?,updated_at=? WHERE api_name=?",
                    (started, count, self._provider_now_text(), name),
                )
                return state
            count += 1
            state.update({"window_started_at": started, "request_count": count, "allowed": True, "wait_seconds": 0.0})
            state["state_digest"] = self._provider_state_digest(state)
            db.execute(
                "UPDATE provider_api_state SET window_started_at=?,request_count=?,state_digest=?,updated_at=? WHERE api_name=?",
                (started, count, state["state_digest"], self._provider_now_text(), name),
            )
            return state

    reserve_provider_request = reserve_provider_api_request

    def update_provider_api_state(
        self,
        api_name: str,
        *,
        now: float | None = None,
        blocked_until: float | None = None,
        retry_after: float | None = None,
        rate_limited: bool = False,
        success: bool = False,
        error: str = "",
        open_circuit: bool = False,
        circuit_seconds: float = 65.0,
    ) -> dict:
        """Persist a provider outcome without coupling APIs together."""
        current = float(time.time() if now is None else now)
        if not math.isfinite(current) or current < 0:
            raise ValueError("provider clock value is invalid")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._provider_state_row(db, api_name)
            state = dict(row)
            name = str(state["api_name"])
            if success:
                state.update({"failure_streak": 0, "rate_limit_failures": 0, "blocked_until": 0.0, "retry_after": 0.0, "circuit_open_until": 0.0, "last_error": ""})
            if rate_limited:
                state["rate_limit_failures"] = max(0, int(state.get("rate_limit_failures") or 0)) + 1
                state["failure_streak"] = max(0, int(state.get("failure_streak") or 0)) + 1
                until = float(blocked_until if blocked_until is not None else current + 65.0)
                state["blocked_until"] = max(float(state.get("blocked_until") or 0), until)
                state["retry_after"] = max(float(state.get("retry_after") or 0), until)
                if open_circuit:
                    state["circuit_open_until"] = max(float(state.get("circuit_open_until") or 0), current + max(65.0, float(circuit_seconds)))
            elif error and not success:
                state["failure_streak"] = max(0, int(state.get("failure_streak") or 0)) + 1
            if error:
                state["last_error"] = str(error)[:240]
            state["state_digest"] = self._provider_state_digest(state)
            db.execute(
                "UPDATE provider_api_state SET blocked_until=?,retry_after=?,rate_limit_failures=?,failure_streak=?,circuit_open_until=?,last_error=?,state_digest=?,updated_at=? WHERE api_name=?",
                (float(state.get("blocked_until") or 0), float(state.get("retry_after") or 0), int(state.get("rate_limit_failures") or 0), int(state.get("failure_streak") or 0), float(state.get("circuit_open_until") or 0), str(state.get("last_error") or "")[:240], state["state_digest"], self._provider_now_text(), name),
            )
            refreshed = db.execute("SELECT * FROM provider_api_state WHERE api_name=?", (name,)).fetchone()
            return dict(refreshed) if refreshed else state

    record_provider_api_result = update_provider_api_state

    def save_provider_api_state(self, api_name: str, **values) -> dict:
        """Compatibility entry point for persisted gateway state updates."""
        return self.update_provider_api_state(api_name, **values)

    def provider_api_states(self) -> list[dict]:
        with self._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM provider_api_state ORDER BY api_name")]

    @staticmethod
    def _provider_request_digest(payload: dict) -> str:
        safe = dict(payload or {})
        safe.pop("token", None)
        text = json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def get_provider_cache(self, api_name: str, request_digest: str, *, now: float | None = None, cache_key: str | None = None) -> dict | None:
        current = float(time.time() if now is None else now)
        with self._connect() as db:
            if cache_key:
                row = db.execute(
                    "SELECT * FROM provider_cache WHERE api_name=? AND cache_key=? AND request_digest=? AND status='valid' AND expires_at>?",
                    (str(api_name or "").strip().lower(), str(cache_key), str(request_digest or ""), current),
                ).fetchone()
            else:
                row = db.execute(
                    "SELECT * FROM provider_cache WHERE api_name=? AND request_digest=? AND status='valid' AND expires_at>?",
                    (str(api_name or "").strip().lower(), str(request_digest or ""), current),
                ).fetchone()
            if not row:
                return None
            try:
                body = json.loads(str(row["body_json"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                return None
            if not isinstance(body, dict):
                return None
            try:
                response_digest = hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
            except (TypeError, ValueError, OverflowError):
                return None
            if response_digest != str(row["response_digest"] or "") or str(row["request_digest"] or "") != str(request_digest or ""):
                try:
                    db.execute("UPDATE provider_cache SET status='invalid' WHERE cache_key=?", (str(row["cache_key"]),))
                except sqlite3.Error:
                    pass
                return None
            return {**dict(row), "body": body}

    provider_cache = get_provider_cache
    get_cached_provider_response = get_provider_cache

    def save_provider_cache(
        self,
        api_name: str,
        request_digest: str,
        body: dict,
        *,
        ttl_seconds: float = 86400.0,
        payload_digest: str | None = None,
        now: float | None = None,
        cache_key: str | None = None,
    ) -> str:
        current = float(time.time() if now is None else now)
        if not math.isfinite(current) or current < 0:
            raise ValueError("provider clock value is invalid")
        if not isinstance(body, dict):
            raise ValueError("provider cache body must be an object")
        digest = str(request_digest or "").strip()
        if not digest:
            raise ValueError("provider cache request digest is required")
        text = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        response_digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        payload_value = str(payload_digest or digest).strip() or digest
        key = str(cache_key or f"{str(api_name or '').strip().lower()}:{digest}").strip()
        if not key:
            raise ValueError("provider cache key is required")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            api_value = str(api_name or "").strip().lower()
            self._provider_state_row(db, api_value)
            db.execute("DELETE FROM provider_cache WHERE api_name=? AND request_digest=? AND cache_key<>?", (api_value, digest, key))
            db.execute(
                "INSERT INTO provider_cache(cache_key,api_name,request_digest,payload_digest,response_digest,body_json,cache_version,created_at,expires_at,status) VALUES(?,?,?,?,?,?,?,?,?,'valid') "
                "ON CONFLICT(cache_key) DO UPDATE SET api_name=excluded.api_name,request_digest=excluded.request_digest,payload_digest=excluded.payload_digest,response_digest=excluded.response_digest,body_json=excluded.body_json,cache_version=excluded.cache_version,created_at=excluded.created_at,expires_at=excluded.expires_at,status='valid'",
                (key, api_value, digest, payload_value, response_digest, text, 1, self._provider_now_text(), current + max(0.0, float(ttl_seconds))),
            )
        return key

    put_provider_cache = save_provider_cache

    def delete_provider_cache(self, api_name: str | None = None) -> int:
        with self._connect() as db:
            if api_name:
                cursor = db.execute("DELETE FROM provider_cache WHERE api_name=?", (str(api_name).strip().lower(),))
            else:
                cursor = db.execute("DELETE FROM provider_cache")
            return int(cursor.rowcount or 0)

    @staticmethod
    def _universe_evidence_key(provider: str, effective_date: str, bj_calendar_policy: str, universe_version: str) -> str:
        value = "|".join((str(provider or "tushare").strip().lower(), str(effective_date or ""), str(bj_calendar_policy or "").strip().lower(), str(universe_version or "").strip()))
        return "evidence-" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:40]

    @staticmethod
    def _universe_evidence_cycle_digest(evidence_key: str, batch_id: str) -> str:
        """Return the stable integrity token for one durable fetch cycle."""
        value = "|".join((str(evidence_key or "").strip(), str(batch_id or "").strip()))
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @classmethod
    def _universe_status_response_digest(cls, list_status: str, normalized_rows: list[dict]) -> str:
        """Digest the canonical response payload persisted for one status."""
        # The normalized row list is the response payload retained by the
        # evidence table.  Keeping this commitment identical to ``content_hash``
        # preserves the v0.13 storage contract while making response-digest
        # tampering detectable after a restart.
        text = json.dumps(normalized_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _universe_status_integrity_digest(
        cycle_digest: str,
        list_status: str,
        content_hash: str,
        request_digest: str,
        payload_digest: str,
        response_digest: str,
    ) -> str:
        payload = {
            "cycle_digest": str(cycle_digest or "").strip().lower(),
            "list_status": str(list_status or "").strip().upper(),
            "content_hash": str(content_hash or "").strip().lower(),
            "request_digest": str(request_digest or "").strip(),
            "payload_digest": str(payload_digest or "").strip(),
            "response_digest": str(response_digest or "").strip().lower(),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()

    def get_or_create_universe_evidence_batch(
        self,
        effective_date: str,
        *,
        provider: str = "tushare",
        bj_calendar_policy: str = "sse_fallback",
        universe_version: str = "",
        universe_statuses: str = "L,D,P",
        batch_id: str | None = None,
    ) -> dict:
        """Return a durable evidence cycle, creating a repair cycle if needed."""
        effective = self._canonical_raw_date(effective_date)
        if not effective:
            raise ValueError("universe evidence effective_date must be canonical")
        provider_value = str(provider or "tushare").strip().lower()[:80]
        policy_value = str(bj_calendar_policy or "sse_fallback").strip().lower()[:32]
        version_value = str(universe_version or "").strip()[:160]
        status_parts = [part.strip().upper() for part in str(universe_statuses or "L,D,P").replace(";", ",").replace(" ", ",").split(",") if part.strip()]
        expected_statuses = [status for status in ("L", "D", "P") if status in status_parts]
        if "L" not in expected_statuses:
            expected_statuses.insert(0, "L")
        if not provider_value or policy_value not in {"require_bse", "sse_fallback", "exclude"}:
            raise ValueError("invalid universe evidence batch identity")
        evidence_key = self._universe_evidence_key(provider_value, effective, policy_value, version_value)
        now = self._provider_now_text()
        with self._connect() as db:
            identity_sql = "SELECT * FROM universe_evidence_batches WHERE provider=? AND effective_date=? AND bj_calendar_policy=?"
            identity_args: list[object] = [provider_value, effective, policy_value]
            if version_value:
                identity_sql += " AND universe_version=?"
                identity_args.append(version_value)
            identity_sql += " ORDER BY CASE status WHEN 'staging' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END,updated_at DESC,created_at DESC"
            rows = db.execute(identity_sql, identity_args).fetchall()

            def published_cycle_is_valid(candidate) -> bool:
                if str(candidate["status"] or "") != "published":
                    return False
                try:
                    evidence = self._normalize_universe_counts(json.loads(str(candidate["evidence_json"] or "{}")))
                except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                    return False
                digest = str(candidate["evidence_digest"] or "").strip().lower()
                if not digest or str(evidence.get("digest") or "").strip().lower() != digest:
                    return False
                try:
                    if self._validate_universe_evidence(
                        evidence,
                        requested_date=str(candidate["effective_date"] or ""),
                        actual_trade_date=str(candidate["effective_date"] or ""),
                        bj_calendar_policy=str(candidate["bj_calendar_policy"] or ""),
                        universe_version=str(candidate["universe_version"] or ""),
                    ):
                        return False
                    if self._universe_evidence_digest(evidence) != digest:
                        return False
                    records = self._universe_evidence_status_records_in_tx(db, str(candidate["evidence_batch_id"]))
                except (TypeError, ValueError, KeyError, OverflowError):
                    return False
                try:
                    expected = json.loads(str(candidate["expected_statuses_json"] or "[]"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    expected = ["L", "D", "P"]
                expected_statuses = {str(status).strip().upper() for status in expected if str(status).strip()}
                if not expected_statuses or set(records) != expected_statuses or not self._universe_status_records_match_evidence(records, evidence):
                    return False
                pointer = db.execute(
                    "SELECT evidence_key,evidence_batch_id,evidence_digest FROM active_universe_evidence WHERE evidence_key=?",
                    (str(candidate["evidence_key"]),),
                ).fetchone()
                return bool(
                    pointer
                    and str(pointer["evidence_batch_id"]) == str(candidate["evidence_batch_id"])
                    and str(pointer["evidence_digest"] or "").strip().lower() == digest
                )

            # A process restart resumes an unfinished cycle.  A published
            # cycle is reusable only when both its manifest and active pointer
            # are still valid; otherwise it remains immutable and a replacement
            # cycle is created below.
            for row in rows:
                status = str(row["status"] or "")
                if status in {"staging", "failed"}:
                    db.execute(
                        "UPDATE universe_evidence_batches SET status='staging',error=NULL,expected_statuses_json=?,updated_at=? WHERE evidence_batch_id=?",
                        (json.dumps(expected_statuses), now, row["evidence_batch_id"]),
                    )
                    refreshed = db.execute("SELECT * FROM universe_evidence_batches WHERE evidence_batch_id=?", (row["evidence_batch_id"],)).fetchone()
                    return dict(refreshed)
                if published_cycle_is_valid(row):
                    return dict(row)

            source = next((row for row in rows if str(row["status"] or "") == "published"), None)
            identifier = str(batch_id or ("evidence-batch-" + uuid.uuid4().hex))
            # ``evidence_key`` was historically unique, so repair cycles use a
            # deterministic base identity plus an opaque suffix.  The identity
            # columns remain queryable while the durable link records which
            # published cycle was repaired.
            cycle_key = evidence_key if not rows else f"{evidence_key}:recovery:{uuid.uuid4().hex}"
            cycle_digest = self._universe_evidence_cycle_digest(cycle_key, identifier)
            db.execute(
                "INSERT INTO universe_evidence_batches(evidence_batch_id,evidence_key,cycle_digest,provider,effective_date,bj_calendar_policy,universe_version,status,expected_statuses_json,evidence_json,evidence_digest,recovery_of,created_at,updated_at,error) VALUES(?,?,?,?,?,?,?, 'staging',?,?,?,?,?,?,NULL)",
                (identifier, cycle_key, cycle_digest, provider_value, effective, policy_value, version_value, json.dumps(expected_statuses), "{}", "", str(source["evidence_batch_id"]) if source else None, now, now),
            )
            if source:
                # Preserve individually valid L/D/P responses in a new cycle;
                # the cycle digest and status integrity commitments are
                # recomputed so a bad source row is never blessed implicitly.
                records = self._universe_evidence_status_records_in_tx(db, str(source["evidence_batch_id"]))
                for status, record in records.items():
                    rows_text = json.dumps(record["rows"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                    integrity = self._universe_status_integrity_digest(
                        cycle_digest,
                        status,
                        str(record["content_hash"]),
                        str(record["request_digest"]),
                        str(record["payload_digest"]),
                        str(record["response_digest"]),
                    )
                    db.execute(
                        "INSERT INTO universe_evidence_statuses(evidence_batch_id,list_status,cycle_digest,rows_json,row_count,content_hash,request_digest,payload_digest,response_digest,integrity_digest,status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?, 'validated',?)",
                        (identifier, status, cycle_digest, rows_text, int(record["row_count"]), str(record["content_hash"]), str(record["request_digest"]), str(record["payload_digest"]), str(record["response_digest"]), integrity, now),
                    )
            row = db.execute("SELECT * FROM universe_evidence_batches WHERE evidence_batch_id=?", (identifier,)).fetchone()
            return dict(row)

    begin_universe_evidence_batch = get_or_create_universe_evidence_batch
    get_or_create_evidence_batch = get_or_create_universe_evidence_batch

    def universe_evidence_batch(self, batch_id: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM universe_evidence_batches WHERE evidence_batch_id=?", (str(batch_id),)).fetchone()
            return dict(row) if row else None

    get_evidence_batch = universe_evidence_batch

    @classmethod
    def _normalize_universe_status_rows(cls, status: str, rows) -> list[dict]:
        status_value = str(status or "").strip().upper()
        if status_value not in {"L", "D", "P"}:
            raise ValueError("invalid universe evidence list status")
        normalized: list[dict] = []
        seen: set[str] = set()
        for raw in rows or []:
            if not isinstance(raw, dict):
                raise ValueError("universe evidence status row is invalid")
            code_info = cls._canonical_raw_code(raw.get("ts_code") or raw.get("code"))
            if not code_info:
                raise ValueError("universe evidence status row has invalid ts_code")
            row_status = str(raw.get("list_status") or status_value).strip().upper()
            if row_status != status_value:
                raise ValueError("universe evidence status row has a mismatched list_status")
            list_date = cls._canonical_raw_date(raw.get("list_date") or "")
            delist_raw = str(raw.get("delist_date") or "").strip()
            delist_date = cls._canonical_raw_date(delist_raw) if delist_raw else ""
            if not list_date or (delist_raw and not delist_date):
                raise ValueError("universe evidence status row has invalid listing dates")
            if delist_date and delist_date < list_date:
                raise ValueError("universe evidence status row has invalid delisting range")
            code = code_info[0]
            if code in seen:
                raise ValueError("universe evidence status contains duplicate codes")
            seen.add(code)
            normalized.append({
                "code": code,
                "ts_code": code_info[1],
                "market": code_info[1].rsplit(".", 1)[-1],
                "list_status": status_value,
                "list_date": list_date,
                "delist_date": delist_date,
            })
        normalized.sort(key=lambda item: (item["code"], item["list_date"], item["delist_date"]))
        return normalized

    def stage_universe_evidence_status(
        self,
        batch_id: str,
        list_status: str,
        rows,
        *,
        request_digest: str = "",
        payload_digest: str = "",
        response_digest: str = "",
    ) -> dict:
        """Persist one validated L/D/P response before the full activation."""
        status = str(list_status or "").strip().upper()
        normalized = self._normalize_universe_status_rows(status, rows)
        rows_text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        content_hash = hashlib.sha256(rows_text.encode("utf-8")).hexdigest()
        response_value = str(response_digest or "").strip().lower() or self._universe_status_response_digest(status, normalized)
        request_value = str(request_digest or "")[:160]
        payload_value = str(payload_digest or "")[:160]
        with self._connect() as db:
            batch = db.execute("SELECT * FROM universe_evidence_batches WHERE evidence_batch_id=?", (str(batch_id),)).fetchone()
            if not batch:
                raise KeyError(f"unknown universe evidence batch {batch_id}")
            if str(batch["status"] or "") not in {"staging", "failed"}:
                raise RuntimeError("universe evidence batch is no longer staging")
            expected_cycle = self._universe_evidence_cycle_digest(str(batch["evidence_key"] or ""), str(batch_id))
            if str(batch["cycle_digest"] or "").strip().lower() != expected_cycle:
                raise RuntimeError("universe evidence batch cycle digest is invalid")
            now = self._provider_now_text()
            integrity_digest = self._universe_status_integrity_digest(
                expected_cycle, status, content_hash, request_value, payload_value, response_value,
            )
            db.execute(
                "INSERT INTO universe_evidence_statuses(evidence_batch_id,list_status,cycle_digest,rows_json,row_count,content_hash,request_digest,payload_digest,response_digest,integrity_digest,status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?, 'validated',?) "
                "ON CONFLICT(evidence_batch_id,list_status) DO UPDATE SET cycle_digest=excluded.cycle_digest,rows_json=excluded.rows_json,row_count=excluded.row_count,content_hash=excluded.content_hash,request_digest=excluded.request_digest,payload_digest=excluded.payload_digest,response_digest=excluded.response_digest,integrity_digest=excluded.integrity_digest,status='validated',updated_at=excluded.updated_at",
                (str(batch_id), status, expected_cycle, rows_text, len(normalized), content_hash, request_value, payload_value, response_value, integrity_digest, now),
            )
            db.execute("UPDATE universe_evidence_batches SET status='staging',error=NULL,updated_at=? WHERE evidence_batch_id=?", (now, str(batch_id)))
            row = db.execute("SELECT * FROM universe_evidence_statuses WHERE evidence_batch_id=? AND list_status=?", (str(batch_id), status)).fetchone()
            return dict(row)

    stage_evidence_status = stage_universe_evidence_status

    def _universe_evidence_status_records_in_tx(self, db, batch_id: str, *, validated_only: bool = True) -> dict[str, dict]:
        """Load and validate status rows using an already-open transaction."""
        batch = db.execute(
            "SELECT evidence_key,cycle_digest FROM universe_evidence_batches WHERE evidence_batch_id=?",
            (str(batch_id),),
        ).fetchone()
        if not batch:
            return {}
        expected_cycle = self._universe_evidence_cycle_digest(str(batch["evidence_key"] or ""), str(batch_id))
        if str(batch["cycle_digest"] or "").strip().lower() != expected_cycle:
            return {}
        sql = "SELECT * FROM universe_evidence_statuses WHERE evidence_batch_id=?"
        args: list[object] = [str(batch_id)]
        if validated_only:
            sql += " AND status='validated'"
        result: dict[str, dict] = {}
        for row in db.execute(sql, args).fetchall():
            status = str(row["list_status"] or "").strip().upper()
            if status not in {"L", "D", "P"}:
                continue
            if str(row["cycle_digest"] or "").strip().lower() != expected_cycle:
                continue
            try:
                parsed = json.loads(str(row["rows_json"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(parsed, list):
                continue
            try:
                normalized = self._normalize_universe_status_rows(status, parsed)
            except (TypeError, ValueError, KeyError):
                continue
            text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            content_hash = str(row["content_hash"] or "").strip().lower()
            if hashlib.sha256(text.encode("utf-8")).hexdigest() != content_hash:
                continue
            try:
                row_count = int(row["row_count"])
            except (TypeError, ValueError, OverflowError):
                continue
            if row_count != len(normalized):
                continue
            response_digest = str(row["response_digest"] or "").strip().lower()
            integrity_digest = str(row["integrity_digest"] or "").strip().lower()
            if not response_digest:
                continue
            expected_integrity = self._universe_status_integrity_digest(
                expected_cycle,
                status,
                content_hash,
                str(row["request_digest"] or ""),
                str(row["payload_digest"] or ""),
                response_digest,
            )
            if not integrity_digest or integrity_digest != expected_integrity:
                continue
            result[status] = {
                "rows": normalized,
                "row_count": row_count,
                "content_hash": content_hash,
                "request_digest": str(row["request_digest"] or ""),
                "payload_digest": str(row["payload_digest"] or ""),
                "response_digest": response_digest,
                "integrity_digest": integrity_digest,
                "cycle_digest": expected_cycle,
                "status": str(row["status"] or ""),
                "updated_at": str(row["updated_at"] or ""),
            }
        return result

    def universe_evidence_status_records(self, batch_id: str, *, validated_only: bool = True) -> dict[str, dict]:
        """Load reusable status rows together with their integrity metadata."""
        with self._connect() as db:
            return self._universe_evidence_status_records_in_tx(db, str(batch_id), validated_only=validated_only)

    load_universe_evidence_status_records = universe_evidence_status_records

    def universe_evidence_statuses(
        self,
        batch_id: str,
        *,
        validated_only: bool = True,
        with_metadata: bool = False,
        include_metadata: bool = False,
        metadata: bool = False,
    ) -> dict:
        """Return status rows, or full records when metadata is requested."""
        records = self.universe_evidence_status_records(batch_id, validated_only=validated_only)
        if with_metadata or include_metadata or metadata:
            return records
        return {status: record["rows"] for status, record in records.items()}

    load_universe_evidence_statuses = universe_evidence_statuses

    @classmethod
    def _universe_status_records_match_evidence(cls, records: dict[str, dict], evidence: dict) -> bool:
        """Check that durable status membership agrees with the manifest."""
        seen_codes: set[str] = set()
        all_rows: list[dict] = []
        for status in ("L", "D", "P"):
            record = records.get(status) or {}
            for row in record.get("rows", []) if isinstance(record, dict) else []:
                code = str(row.get("code") or "")
                if not code or code in seen_codes:
                    return False
                seen_codes.add(code)
                all_rows.append(row)
        try:
            evidence_version = int(evidence.get("evidence_version") or 0)
        except (TypeError, ValueError, OverflowError):
            evidence_version = 0
        if evidence_version < 2:
            return True
        effective = cls._canonical_raw_date(evidence.get("effective_date") or "") or ""
        policy = str(evidence.get("bj_calendar_policy") or "").strip().lower()
        eligible = []
        for row in all_rows:
            list_date = cls._canonical_raw_date(row.get("list_date") or "") or ""
            delist_date = cls._canonical_raw_date(row.get("delist_date") or "") if row.get("delist_date") else ""
            if not list_date or (effective and list_date > effective):
                continue
            if delist_date and effective and effective > delist_date:
                continue
            if policy == "exclude" and str(row.get("market") or "").upper() == "BJ":
                continue
            eligible.append({
                "code": str(row.get("code") or ""),
                "ts_code": str(row.get("ts_code") or "").upper(),
                "market": str(row.get("market") or "").upper(),
                "list_status": str(row.get("list_status") or "").upper(),
                "list_date": list_date,
                "delist_date": delist_date or "",
            })
        eligible.sort(key=lambda item: (item["code"], item["list_status"], item["list_date"], item["delist_date"]))
        manifest = evidence.get("memberships")
        if not isinstance(manifest, list):
            manifest = evidence.get("list_status_membership")
        if not isinstance(manifest, list):
            return False
        canonical_manifest = []
        for item in manifest:
            if not isinstance(item, dict):
                return False
            canonical_manifest.append({
                "code": str(item.get("code") or ""),
                "ts_code": str(item.get("ts_code") or "").upper(),
                "market": str(item.get("market") or "").upper(),
                "list_status": str(item.get("list_status") or "").upper(),
                "list_date": cls._canonical_raw_date(item.get("list_date") or "") or "",
                "delist_date": cls._canonical_raw_date(item.get("delist_date") or "") if item.get("delist_date") else "",
            })
        canonical_manifest.sort(key=lambda item: (item["code"], item["list_status"], item["list_date"], item["delist_date"]))
        if canonical_manifest != eligible:
            return False
        expected_status_counts: dict[str, int] = {}
        for item in eligible:
            status = item["list_status"]
            expected_status_counts[status] = expected_status_counts.get(status, 0) + 1
        if {str(key): int(value) for key, value in (evidence.get("status_counts") or {}).items()} != expected_status_counts:
            return False
        expected_market_counts: dict[str, int] = {}
        for item in eligible:
            market = item["market"]
            expected_market_counts[market] = expected_market_counts.get(market, 0) + 1
        if {str(key): int(value) for key, value in (evidence.get("markets") or {}).items()} != expected_market_counts:
            return False
        status_digests = evidence.get("status_digests")
        if isinstance(status_digests, dict):
            for status in ("L", "D", "P"):
                status_rows = [item for item in eligible if item["list_status"] == status]
                expected = hashlib.sha256(json.dumps(status_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
                if str(status_digests.get(status) or "").strip().lower() != expected:
                    return False
        return True

    def fail_universe_evidence_batch(self, batch_id: str, error: str = "") -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE universe_evidence_batches SET status='failed',error=?,updated_at=? WHERE evidence_batch_id=? AND status='staging'",
                (str(error or "universe evidence batch failed")[:500], self._provider_now_text(), str(batch_id)),
            )

    abort_universe_evidence_batch = fail_universe_evidence_batch

    def activate_universe_evidence(self, batch_id: str, evidence: dict) -> dict:
        """Atomically publish a complete evidence set and its active pointer."""
        with self._connect() as db:
            # Take the write lock before reading the batch and all three
            # statuses.  Otherwise a concurrent repair/restart could change a
            # status between validation and the active-pointer update.
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute("SELECT * FROM universe_evidence_batches WHERE evidence_batch_id=?", (str(batch_id),)).fetchone()
            if not batch:
                raise KeyError(f"unknown universe evidence batch {batch_id}")
            expected_cycle = self._universe_evidence_cycle_digest(str(batch["evidence_key"] or ""), str(batch_id))
            if str(batch["cycle_digest"] or "").strip().lower() != expected_cycle:
                raise RuntimeError("universe evidence batch cycle digest is invalid")
            if str(batch["status"] or "") not in {"staging", "published"}:
                raise RuntimeError("universe evidence batch is not activatable")

            normalized = self._normalize_universe_counts(evidence)
            evidence_version = str(normalized.get("universe_version") or "")
            digest = str(normalized.get("digest") or "").strip().lower()
            if not digest or digest != self._universe_evidence_digest(normalized):
                raise ValueError("universe evidence digest is invalid")
            validation_errors = self._validate_universe_evidence(
                normalized,
                requested_date=str(batch["effective_date"] or ""),
                actual_trade_date=str(batch["effective_date"] or ""),
                bj_calendar_policy=str(batch["bj_calendar_policy"] or ""),
                universe_version=str(batch["universe_version"] or ""),
            )
            if validation_errors:
                raise ValueError("; ".join(dict.fromkeys(validation_errors)))

            def validated_status_records() -> dict[str, dict]:
                records = self._universe_evidence_status_records_in_tx(db, str(batch_id))
                try:
                    expected = json.loads(str(batch["expected_statuses_json"] or "[]"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    expected = ["L", "D", "P"]
                expected_statuses = {str(status).strip().upper() for status in expected if str(status).strip()}
                if not expected_statuses or set(records) != expected_statuses:
                    raise RuntimeError("universe evidence batch is incomplete")
                if not self._universe_status_records_match_evidence(records, normalized):
                    raise ValueError("universe evidence statuses do not match manifest")
                return records

            validated_status_records()
            if str(batch["effective_date"]) != str(normalized.get("effective_date") or ""):
                raise ValueError("universe evidence effective date mismatch")
            if str(batch["bj_calendar_policy"]) != str(normalized.get("bj_calendar_policy") or ""):
                raise ValueError("universe evidence calendar policy mismatch")
            if str(batch["universe_version"] or "") and str(batch["universe_version"]) != evidence_version:
                raise ValueError("universe evidence version mismatch")
            # Re-read immediately before publishing.  The write transaction
            # already serializes external writers; this second validation also
            # rejects an in-transaction replacement hook before the pointer
            # switch, so a stale precheck can never publish a mixed cycle.
            validated_status_records()
            now = self._provider_now_text()
            text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            db.execute(
                "UPDATE universe_evidence_batches SET status='published',universe_version=?,evidence_json=?,evidence_digest=?,updated_at=?,error=NULL WHERE evidence_batch_id=?",
                (evidence_version, text, digest, now, str(batch_id)),
            )
            # A recovery cycle has a new evidence key but the active table's
            # natural identity is provider/date/policy/version.  Replace that
            # pointer only after every validation above has passed; the whole
            # operation is in this transaction, so readers see the old pointer
            # until the new one commits.
            db.execute(
                "DELETE FROM active_universe_evidence WHERE provider=? AND effective_date=? AND bj_calendar_policy=?",
                (batch["provider"], batch["effective_date"], batch["bj_calendar_policy"]),
            )
            db.execute(
                "INSERT INTO active_universe_evidence(evidence_key,evidence_batch_id,provider,effective_date,bj_calendar_policy,universe_version,evidence_digest,updated_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(evidence_key) DO UPDATE SET evidence_batch_id=excluded.evidence_batch_id,provider=excluded.provider,effective_date=excluded.effective_date,bj_calendar_policy=excluded.bj_calendar_policy,universe_version=excluded.universe_version,evidence_digest=excluded.evidence_digest,updated_at=excluded.updated_at",
                (batch["evidence_key"], str(batch_id), batch["provider"], batch["effective_date"], batch["bj_calendar_policy"], evidence_version, digest, now),
            )
            return {**dict(batch), "status": "published", "universe_version": evidence_version, "evidence_json": text, "evidence_digest": digest, "updated_at": now}

    publish_universe_evidence = activate_universe_evidence
    promote_universe_evidence = activate_universe_evidence

    def active_universe_evidence(
        self,
        effective_date: str,
        *,
        provider: str = "tushare",
        bj_calendar_policy: str = "sse_fallback",
        universe_version: str = "",
    ) -> dict | None:
        effective = self._canonical_raw_date(effective_date)
        if not effective:
            return None
        provider_value = str(provider or "tushare").strip().lower()
        policy_value = str(bj_calendar_policy or "sse_fallback").strip().lower()
        version_value = str(universe_version or "").strip()
        key = self._universe_evidence_key(provider_value, effective, policy_value, version_value)
        with self._connect() as db:
            if version_value:
                row = db.execute(
                    # Recovery cycles intentionally use a suffixed evidence
                    # key, so a configured version must be looked up by its
                    # natural identity rather than only by the original key.
                    "SELECT a.*,b.status,b.evidence_json,b.evidence_digest AS batch_evidence_digest,b.evidence_key AS batch_evidence_key,b.expected_statuses_json FROM active_universe_evidence a JOIN universe_evidence_batches b ON b.evidence_batch_id=a.evidence_batch_id WHERE a.provider=? AND a.effective_date=? AND a.bj_calendar_policy=? AND a.universe_version=? AND b.status='published' ORDER BY a.updated_at DESC LIMIT 1",
                    (provider_value, effective, policy_value, version_value),
                ).fetchone()
            else:
                row = db.execute(
                    "SELECT a.*,b.status,b.evidence_json,b.evidence_digest AS batch_evidence_digest,b.evidence_key AS batch_evidence_key,b.expected_statuses_json FROM active_universe_evidence a JOIN universe_evidence_batches b ON b.evidence_batch_id=a.evidence_batch_id WHERE a.provider=? AND a.effective_date=? AND a.bj_calendar_policy=? AND b.status='published' ORDER BY a.updated_at DESC LIMIT 1",
                    (provider_value, effective, policy_value),
                ).fetchone()
            if not row:
                return None
            try:
                expected_cycle = self._universe_evidence_cycle_digest(str(row["batch_evidence_key"] or ""), str(row["evidence_batch_id"] or ""))
                batch = db.execute("SELECT cycle_digest FROM universe_evidence_batches WHERE evidence_batch_id=?", (str(row["evidence_batch_id"]),)).fetchone()
                if not batch or str(batch["cycle_digest"] or "").strip().lower() != expected_cycle:
                    return None
                evidence = self._normalize_universe_counts(json.loads(str(row["evidence_json"] or "{}")))
                if self._validate_universe_evidence(
                    evidence,
                    requested_date=effective,
                    actual_trade_date=effective,
                    bj_calendar_policy=policy_value,
                    universe_version=version_value,
                ):
                    return None
                if (
                    str(evidence.get("digest") or "") != str(row["evidence_digest"] or "")
                    or str(row["batch_evidence_digest"] or "") != str(row["evidence_digest"] or "")
                    or self._universe_evidence_digest(evidence) != str(row["evidence_digest"] or "")
                ):
                    return None
                records = self.universe_evidence_status_records(str(row["evidence_batch_id"]))
                try:
                    expected = json.loads(str(row["expected_statuses_json"] or "[]"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    expected = ["L", "D", "P"]
                expected_statuses = {str(status).strip().upper() for status in expected if str(status).strip()}
                if not expected_statuses or set(records) != expected_statuses or not self._universe_status_records_match_evidence(records, evidence):
                    return None
            except (TypeError, ValueError, KeyError, OverflowError):
                return None
            return {**dict(row), "evidence": evidence, "status_records": records, "statuses": {key: value["rows"] for key, value in records.items()}}

    get_active_universe_evidence = active_universe_evidence

    def recent_universe_evidence(
        self,
        effective_date: str,
        *,
        provider: str = "tushare",
        bj_calendar_policy: str = "sse_fallback",
        universe_version: str = "",
        max_age_days: int = 30,
    ) -> dict | None:
        """Return the most recent still-valid universe evidence for reuse.

        The eligible-universe stock list changes slowly, so a daily snapshot
        does not need a fresh ``stock_basic`` fetch every trading day.  When
        the exact-date evidence is missing (e.g. ``stock_basic`` is rate
        limited), reuse the latest published evidence whose validity window
        covers the requested date and which is no older than ``max_age_days``.
        """
        effective = self._canonical_raw_date(effective_date)
        if not effective:
            return None
        provider_value = str(provider or "tushare").strip().lower()
        policy_value = str(bj_calendar_policy or "sse_fallback").strip().lower()
        try:
            cutoff = (datetime.strptime(effective, "%Y-%m-%d").date() - timedelta(days=max(0, int(max_age_days)))).isoformat()
        except (TypeError, ValueError, OverflowError):
            cutoff = ""
        with self._connect() as db:
            sql = (
                "SELECT a.*,b.status,b.evidence_json,b.evidence_digest AS batch_evidence_digest,b.evidence_key AS batch_evidence_key,b.expected_statuses_json "
                "FROM active_universe_evidence a JOIN universe_evidence_batches b ON b.evidence_batch_id=a.evidence_batch_id "
                "WHERE a.provider=? AND a.effective_date<=? AND a.bj_calendar_policy=? AND b.status='published'"
            )
            args: list[object] = [provider_value, effective, policy_value]
            if cutoff:
                sql += " AND a.effective_date>=?"
                args.append(cutoff)
            sql += " ORDER BY a.effective_date DESC LIMIT 1"
            row = db.execute(sql, args).fetchone()
            if not row:
                return None
            try:
                expected_cycle = self._universe_evidence_cycle_digest(str(row["batch_evidence_key"] or ""), str(row["evidence_batch_id"] or ""))
                batch = db.execute("SELECT cycle_digest FROM universe_evidence_batches WHERE evidence_batch_id=?", (str(row["evidence_batch_id"]),)).fetchone()
                if not batch or str(batch["cycle_digest"] or "").strip().lower() != expected_cycle:
                    return None
                evidence = self._normalize_universe_counts(json.loads(str(row["evidence_json"] or "{}")))
                # Reuse deliberately relaxes the version and exact-date checks
                # (a date-stamped universe_version differs per day), but still
                # enforces digest integrity, status-record completeness and the
                # valid_from/valid_to window against the requested date.
                if self._validate_universe_evidence(
                    evidence,
                    requested_date=effective,
                    actual_trade_date="",
                    bj_calendar_policy=policy_value,
                    universe_version="",
                ):
                    return None
                if (
                    str(evidence.get("digest") or "") != str(row["evidence_digest"] or "")
                    or str(row["batch_evidence_digest"] or "") != str(row["evidence_digest"] or "")
                    or self._universe_evidence_digest(evidence) != str(row["evidence_digest"] or "")
                ):
                    return None
                records = self.universe_evidence_status_records(str(row["evidence_batch_id"]))
                try:
                    expected = json.loads(str(row["expected_statuses_json"] or "[]"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    expected = ["L", "D", "P"]
                expected_statuses = {str(status).strip().upper() for status in expected if str(status).strip()}
                if not expected_statuses or set(records) != expected_statuses or not self._universe_status_records_match_evidence(records, evidence):
                    return None
            except (TypeError, ValueError, KeyError, OverflowError):
                return None
            return {**dict(row), "evidence": evidence, "status_records": records, "statuses": {key: value["rows"] for key, value in records.items()}}

    @staticmethod
    def _set_schema_version(db, version: int) -> None:
        db.execute(
            "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(int(version)),),
        )

    def _normalize_legacy_daily_bar_dates(self, db) -> None:
        """Normalize compact daily-bar dates within the caller's transaction."""
        legacy = db.execute(
            "SELECT code, trade_date, open, high, low, close, volume, amount, source, fetched_at, price_basis "
            "FROM daily_bars WHERE length(trade_date)=8"
        ).fetchall()
        for row in legacy:
            normalized = self._date_norm(str(row[1]))
            existing = db.execute(
                "SELECT price_basis FROM daily_bars WHERE code=? AND trade_date=?",
                (row[0], normalized),
            ).fetchone()
            basis = str(row[10] or "unknown")
            if existing and str(existing[0] or "unknown") not in {"", "unknown"}:
                basis = str(existing[0])
            if existing:
                if str(existing[0] or "unknown").strip().lower() in {"", "unknown"} and basis.strip().lower() not in {"", "unknown"}:
                    db.execute("UPDATE daily_bars SET price_basis=? WHERE code=? AND trade_date=?", (basis, row[0], normalized))
            else:
                db.execute(
                    "INSERT OR REPLACE INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at,price_basis) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (row[0], normalized, row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], basis),
                )
            db.execute("DELETE FROM daily_bars WHERE code=? AND trade_date=?", (row[0], row[1]))

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
        statements = (
            """CREATE TABLE IF NOT EXISTS signal_events_v11(
                origin TEXT NOT NULL, code TEXT NOT NULL, run_id TEXT NOT NULL DEFAULT 'legacy',
                last_sent_at TEXT NOT NULL, PRIMARY KEY(origin, code, run_id)
            )""",
            """INSERT OR IGNORE INTO signal_events_v11(origin,code,run_id,last_sent_at)
                SELECT origin,code,'legacy',last_sent_at FROM signal_events""",
            "DROP TABLE signal_events",
            "ALTER TABLE signal_events_v11 RENAME TO signal_events",
            """CREATE TABLE IF NOT EXISTS confirmation_events_v11(
                origin TEXT NOT NULL, code TEXT NOT NULL, run_id TEXT NOT NULL DEFAULT 'legacy',
                consecutive_count INTEGER NOT NULL, last_observed_at TEXT NOT NULL,
                PRIMARY KEY(origin, code, run_id)
            )""",
            """INSERT OR IGNORE INTO confirmation_events_v11(origin,code,run_id,consecutive_count,last_observed_at)
                SELECT origin,code,'legacy',consecutive_count,last_observed_at FROM confirmation_events""",
            "DROP TABLE confirmation_events",
            "ALTER TABLE confirmation_events_v11 RENAME TO confirmation_events",
            """CREATE TABLE IF NOT EXISTS price_states_v11(
                origin TEXT NOT NULL, code TEXT NOT NULL, run_id TEXT NOT NULL DEFAULT 'legacy',
                state TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(origin, code, run_id)
            )""",
            """INSERT OR IGNORE INTO price_states_v11(origin,code,run_id,state,updated_at)
                SELECT origin,code,'legacy',state,updated_at FROM price_states""",
            "DROP TABLE price_states",
            "ALTER TABLE price_states_v11 RENAME TO price_states",
        )
        for statement in statements:
            db.execute(statement)
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

    @staticmethod
    def _migrate_v13_provenance_and_symbols(db) -> None:
        """Add price provenance and build a durable code/name lookup index."""
        StockStore._ensure_column(db, "daily_bars", "price_basis", "TEXT NOT NULL DEFAULT 'unknown'")
        StockStore._ensure_column(db, "result_evaluations", "price_basis", "TEXT NOT NULL DEFAULT 'unknown'")
        StockStore._ensure_column(db, "result_evaluations", "plan_validated", "INTEGER NOT NULL DEFAULT 0")
        db.execute(
            """CREATE TABLE IF NOT EXISTS stock_symbols(
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                normalized_name TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )"""
        )
        db.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbols_normalized_name ON stock_symbols(normalized_name)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbols_code_prefix ON stock_symbols(code)")

        # A few pre-v13 stores contain both YYYYMMDD and YYYY-MM-DD rows.  Do
        # the copy before deleting the compact key so a known basis is never
        # lost, and prefer an already-normalized known basis when both exist.
        legacy = db.execute(
            "SELECT code,trade_date,open,high,low,close,volume,amount,source,fetched_at,price_basis "
            "FROM daily_bars WHERE length(trade_date)=8"
        ).fetchall()
        for row in legacy:
            normalized = StockStore._date_norm(str(row[1]))
            if normalized == str(row[1]):
                continue
            existing = db.execute(
                "SELECT price_basis FROM daily_bars WHERE code=? AND trade_date=?",
                (row[0], normalized),
            ).fetchone()
            basis = str(row[10] or "unknown")
            if existing and str(existing[0] or "unknown").strip().lower() not in {"", "unknown"}:
                basis = str(existing[0])
            if existing:
                if str(existing[0] or "unknown").strip().lower() in {"", "unknown"} and basis.strip().lower() not in {"", "unknown"}:
                    db.execute("UPDATE daily_bars SET price_basis=? WHERE code=? AND trade_date=?", (basis, row[0], normalized))
            else:
                db.execute(
                    "INSERT OR REPLACE INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at,price_basis) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (row[0], normalized, row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], basis),
                )
            db.execute("DELETE FROM daily_bars WHERE code=? AND trade_date=?", (row[0], row[1]))

        from .core import normalize_code, normalize_stock_name

        def display_name(value) -> str:
            text = str(value or "")
            text = "".join(char for char in text if ord(char) >= 32 and ord(char) != 127)
            return " ".join(text.split())[:64]

        rows: dict[str, tuple[str, str]] = {}
        sources = (
            ("watchlist", "SELECT code,name FROM watchlist WHERE name IS NOT NULL AND name<>''"),
            ("screen_candidate", "SELECT c.code,c.name FROM screen_candidates c JOIN screen_runs r ON r.run_id=c.run_id WHERE c.name IS NOT NULL AND c.name<>'' ORDER BY r.started_at"),
            ("daily_quote", "SELECT code,name FROM daily_quotes WHERE name IS NOT NULL AND name<>'' ORDER BY trade_date"),
        )
        for source, sql in sources:
            for raw_code, raw_name in db.execute(sql):
                code = normalize_code(raw_code)
                name = display_name(raw_name)
                if not re.fullmatch(r"\d{6}", code) or not name or normalize_stock_name(name) in {"", code}:
                    continue
                rows[code] = (name, source)
        now = datetime.utcnow().isoformat()
        db.executemany(
            "INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(code) DO UPDATE SET name=excluded.name,normalized_name=excluded.normalized_name,source=excluded.source,updated_at=excluded.updated_at",
            [(code, name, normalize_stock_name(name), source, now) for code, (name, source) in rows.items()],
        )

    @staticmethod
    def _migrate_v14_raw_dataset_generations(db) -> None:
        """Create append-only raw dataset and generation pointer tables."""
        statements = (
            """CREATE TABLE IF NOT EXISTS datasets(
                dataset_id TEXT PRIMARY KEY,
                dataset_key TEXT NOT NULL,
                provider TEXT NOT NULL CHECK(length(trim(provider)) > 0),
                frequency TEXT NOT NULL DEFAULT '1d',
                basis TEXT NOT NULL DEFAULT 'unadjusted' CHECK(basis='unadjusted'),
                universe TEXT NOT NULL DEFAULT 'A',
                created_at TEXT NOT NULL,
                UNIQUE(dataset_key, provider, frequency, basis, universe)
            )""",
            """CREATE TABLE IF NOT EXISTS batches(
                batch_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
                requested_date TEXT NOT NULL,
                actual_trade_date TEXT,
                start_date TEXT,
                end_date TEXT,
                status TEXT NOT NULL DEFAULT 'staging' CHECK(status IN ('staging','published','failed')),
                shadow INTEGER NOT NULL DEFAULT 0 CHECK(shadow IN (0,1)),
                publication_mode TEXT NOT NULL DEFAULT 'active' CHECK(publication_mode IN ('active','shadow')),
                quality TEXT NOT NULL DEFAULT 'unknown',
                source TEXT NOT NULL DEFAULT '',
                basis TEXT NOT NULL DEFAULT 'unadjusted' CHECK(basis='unadjusted'),
                generation INTEGER CHECK(generation IS NULL OR generation > 0),
                expected_days INTEGER NOT NULL DEFAULT 0 CHECK(expected_days >= 0),
                row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count >= 0),
                manifest_version INTEGER NOT NULL DEFAULT 1 CHECK(manifest_version > 0),
                manifest_hash TEXT NOT NULL DEFAULT '',
                expected_dates_json TEXT NOT NULL DEFAULT '[]',
                coverage_json TEXT NOT NULL DEFAULT '{}',
                universe_version TEXT NOT NULL DEFAULT '',
                universe_counts_json TEXT NOT NULL DEFAULT '{}',
                require_universe_evidence INTEGER NOT NULL DEFAULT 1 CHECK(require_universe_evidence IN (0,1)),
                bj_calendar_policy TEXT NOT NULL DEFAULT 'unknown',
                min_row_count INTEGER NOT NULL DEFAULT 4000 CHECK(min_row_count >= 1),
                min_overall_coverage REAL NOT NULL DEFAULT 0.97 CHECK(min_overall_coverage >= 0 AND min_overall_coverage <= 1),
                min_market_coverage REAL NOT NULL DEFAULT 0.95 CHECK(min_market_coverage >= 0 AND min_market_coverage <= 1),
                min_market_median_ratio REAL NOT NULL DEFAULT 0.95 CHECK(min_market_median_ratio >= 0 AND min_market_median_ratio <= 1),
                page_size INTEGER NOT NULL DEFAULT 6000 CHECK(page_size > 0 AND page_size <= 6000),
                created_at TEXT NOT NULL,
                published_at TEXT,
                error TEXT,
                CHECK(status <> 'published' OR (actual_trade_date IS NOT NULL AND manifest_hash <> ''))
            )""",
            """CREATE TABLE IF NOT EXISTS day_partitions(
                partition_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
                trade_date TEXT NOT NULL,
                partition_no INTEGER NOT NULL DEFAULT 0,
                content_hash TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                basis TEXT NOT NULL DEFAULT 'unadjusted' CHECK(basis='unadjusted'),
                row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count >= 1),
                market_counts_json TEXT NOT NULL DEFAULT '{}',
                digest_version INTEGER NOT NULL DEFAULT 1 CHECK(digest_version > 0),
                validation_status TEXT NOT NULL DEFAULT 'validated' CHECK(validation_status IN ('validated','rejected')),
                created_at TEXT NOT NULL,
                UNIQUE(dataset_id, trade_date, partition_no, content_hash)
            )""",
            """CREATE TABLE IF NOT EXISTS partition_bars(
                partition_id TEXT NOT NULL REFERENCES day_partitions(partition_id) ON DELETE CASCADE,
                trade_date TEXT NOT NULL,
                code TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                pre_close REAL NOT NULL,
                pct_change REAL NOT NULL,
                volume REAL NOT NULL,
                amount REAL NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                basis TEXT NOT NULL DEFAULT 'unadjusted' CHECK(basis='unadjusted'),
                PRIMARY KEY(partition_id, code),
                CHECK(trade_date <> '' AND code <> '' AND ts_code <> ''),
                CHECK(open > 0 AND high > 0 AND low > 0 AND close > 0 AND pre_close > 0),
                CHECK(high >= low AND high >= open AND high >= close AND low <= open AND low <= close),
                CHECK(volume >= 0 AND amount >= 0),
                UNIQUE(partition_id, ts_code)
            )""",
            """CREATE TABLE IF NOT EXISTS batch_days(
                batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
                trade_date TEXT NOT NULL,
                partition_id TEXT NOT NULL REFERENCES day_partitions(partition_id) ON DELETE RESTRICT,
                row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count >= 1),
                PRIMARY KEY(batch_id, trade_date, partition_id)
            )""",
            """CREATE TABLE IF NOT EXISTS raw_page_metadata(
                batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
                trade_date TEXT NOT NULL,
                partition_no INTEGER NOT NULL CHECK(partition_no >= 0),
                server_row_count INTEGER NOT NULL CHECK(server_row_count >= 0),
                server_terminal INTEGER NOT NULL DEFAULT 0 CHECK(server_terminal IN (0,1)),
                server_page_digest TEXT NOT NULL CHECK(length(trim(server_page_digest)) > 0),
                filter_policy TEXT NOT NULL DEFAULT 'include_all' CHECK(length(trim(filter_policy)) > 0),
                server_rows_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                PRIMARY KEY(batch_id,trade_date,partition_no)
            )""",
            """CREATE TABLE IF NOT EXISTS active_generations(
                dataset_id TEXT PRIMARY KEY REFERENCES datasets(dataset_id) ON DELETE CASCADE,
                active_batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE RESTRICT,
                previous_batch_id TEXT REFERENCES batches(batch_id) ON DELETE RESTRICT,
                generation INTEGER NOT NULL CHECK(generation > 0),
                updated_at TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS read_provenance(
                read_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
                batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE RESTRICT,
                generation INTEGER NOT NULL CHECK(generation > 0),
                reader TEXT NOT NULL DEFAULT '',
                basis TEXT NOT NULL DEFAULT 'unadjusted' CHECK(basis='unadjusted'),
                source TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                closed_at TEXT,
                pinned INTEGER NOT NULL DEFAULT 1 CHECK(pinned IN (0,1))
            )""",
            "CREATE INDEX IF NOT EXISTS idx_batches_dataset_status ON batches(dataset_id,status,generation)",
            "CREATE INDEX IF NOT EXISTS idx_day_partitions_lookup ON day_partitions(dataset_id,trade_date,partition_no)",
            "CREATE INDEX IF NOT EXISTS idx_batch_days_lookup ON batch_days(batch_id,trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_raw_page_metadata_lookup ON raw_page_metadata(batch_id,trade_date,partition_no)",
            "CREATE INDEX IF NOT EXISTS idx_partition_bars_code ON partition_bars(code,trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_read_provenance_batch ON read_provenance(batch_id,pinned,expires_at)",
            """CREATE TABLE IF NOT EXISTS provider_api_state(
                api_name TEXT PRIMARY KEY CHECK(length(trim(api_name)) > 0),
                bucket_limit INTEGER NOT NULL DEFAULT 1 CHECK(bucket_limit > 0),
                window_seconds INTEGER NOT NULL DEFAULT 60 CHECK(window_seconds > 0),
                window_started_at REAL NOT NULL DEFAULT 0 CHECK(window_started_at >= 0),
                request_count INTEGER NOT NULL DEFAULT 0 CHECK(request_count >= 0),
                blocked_until REAL NOT NULL DEFAULT 0 CHECK(blocked_until >= 0),
                retry_after REAL NOT NULL DEFAULT 0 CHECK(retry_after >= 0),
                rate_limit_failures INTEGER NOT NULL DEFAULT 0 CHECK(rate_limit_failures >= 0),
                failure_streak INTEGER NOT NULL DEFAULT 0 CHECK(failure_streak >= 0),
                circuit_open_until REAL NOT NULL DEFAULT 0 CHECK(circuit_open_until >= 0),
                last_error TEXT NOT NULL DEFAULT '',
                state_digest TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS provider_cache(
                cache_key TEXT PRIMARY KEY CHECK(length(trim(cache_key)) > 0),
                api_name TEXT NOT NULL REFERENCES provider_api_state(api_name) ON DELETE CASCADE,
                request_digest TEXT NOT NULL CHECK(length(trim(request_digest)) > 0),
                payload_digest TEXT NOT NULL CHECK(length(trim(payload_digest)) > 0),
                response_digest TEXT NOT NULL CHECK(length(trim(response_digest)) > 0),
                body_json TEXT NOT NULL,
                cache_version INTEGER NOT NULL DEFAULT 1 CHECK(cache_version > 0),
                created_at TEXT NOT NULL,
                expires_at REAL NOT NULL CHECK(expires_at >= 0),
                status TEXT NOT NULL DEFAULT 'valid' CHECK(status IN ('valid','stale','invalid')),
                UNIQUE(api_name, request_digest)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_provider_api_updated ON provider_api_state(updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_provider_cache_expiry ON provider_cache(api_name,expires_at,status)",
            "CREATE INDEX IF NOT EXISTS idx_provider_cache_digest ON provider_cache(api_name,request_digest,response_digest)",
            """CREATE TABLE IF NOT EXISTS universe_evidence_batches(
                evidence_batch_id TEXT PRIMARY KEY CHECK(length(trim(evidence_batch_id)) > 0),
                evidence_key TEXT NOT NULL UNIQUE CHECK(length(trim(evidence_key)) > 0),
                cycle_digest TEXT NOT NULL DEFAULT '',
                provider TEXT NOT NULL DEFAULT 'tushare' CHECK(length(trim(provider)) > 0),
                effective_date TEXT NOT NULL,
                bj_calendar_policy TEXT NOT NULL DEFAULT 'sse_fallback',
                universe_version TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'staging' CHECK(status IN ('staging','published','failed')),
                expected_statuses_json TEXT NOT NULL DEFAULT '["D","L","P"]',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                evidence_digest TEXT NOT NULL DEFAULT '',
                recovery_of TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS universe_evidence_statuses(
                evidence_batch_id TEXT NOT NULL REFERENCES universe_evidence_batches(evidence_batch_id) ON DELETE CASCADE,
                list_status TEXT NOT NULL CHECK(list_status IN ('L','D','P')),
                cycle_digest TEXT NOT NULL DEFAULT '',
                rows_json TEXT NOT NULL DEFAULT '[]',
                row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count >= 0),
                content_hash TEXT NOT NULL DEFAULT '',
                request_digest TEXT NOT NULL DEFAULT '',
                payload_digest TEXT NOT NULL DEFAULT '',
                response_digest TEXT NOT NULL DEFAULT '',
                integrity_digest TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'validated' CHECK(status IN ('staging','validated','rejected')),
                updated_at TEXT NOT NULL,
                PRIMARY KEY(evidence_batch_id,list_status)
            )""",
            """CREATE TABLE IF NOT EXISTS active_universe_evidence(
                evidence_key TEXT PRIMARY KEY REFERENCES universe_evidence_batches(evidence_key) ON DELETE RESTRICT,
                evidence_batch_id TEXT NOT NULL REFERENCES universe_evidence_batches(evidence_batch_id) ON DELETE RESTRICT,
                provider TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                bj_calendar_policy TEXT NOT NULL,
                universe_version TEXT NOT NULL DEFAULT '',
                evidence_digest TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(provider,effective_date,bj_calendar_policy,universe_version)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_universe_evidence_lookup ON universe_evidence_batches(provider,effective_date,bj_calendar_policy,status)",
            "CREATE INDEX IF NOT EXISTS idx_universe_evidence_statuses ON universe_evidence_statuses(evidence_batch_id,status)",
            "CREATE INDEX IF NOT EXISTS idx_active_universe_evidence_date ON active_universe_evidence(provider,effective_date)",
        )
        for statement in statements:
            db.execute(statement)
        aliases = (
            ("raw_datasets", "datasets"),
            ("raw_batches", "batches"),
            ("raw_day_partitions", "day_partitions"),
            ("raw_partition_bars", "partition_bars"),
            ("raw_batch_days", "batch_days"),
            ("raw_active_generations", "active_generations"),
            ("raw_read_provenance", "read_provenance"),
        )
        for alias, table in aliases:
            db.execute(f"CREATE VIEW IF NOT EXISTS {alias} AS SELECT * FROM {table}")
        evidence_aliases = (
            ("raw_universe_evidence_batches", "universe_evidence_batches"),
            ("raw_universe_evidence_statuses", "universe_evidence_statuses"),
            ("raw_active_universe_evidence", "active_universe_evidence"),
        )
        for alias, table in evidence_aliases:
            db.execute(f"CREATE VIEW IF NOT EXISTS {alias} AS SELECT * FROM {table}")

    @staticmethod
    def _canonical_raw_code(value) -> tuple[str, str] | None:
        """Return (numeric code, canonical Tushare code) or ``None``."""
        text = str(value or "").strip().upper()
        match = re.fullmatch(r"(\d{6})(?:\.([A-Z]{2}))?", text)
        if not match:
            return None
        code, suffix = match.groups()
        # 920xxx is the newer Beijing Stock Exchange range; the older 900xxx
        # Shanghai B-share range still belongs to SH.
        expected = "BJ" if code.startswith(("4", "8", "920")) else "SH" if code.startswith(("6", "68", "9")) else "SZ"
        if suffix and suffix != expected:
            return None
        suffix = suffix or expected
        return code, f"{code}.{suffix}"

    @staticmethod
    def _canonical_raw_date(value) -> str | None:
        text = str(value or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            digits = text.replace("-", "")
        elif re.fullmatch(r"\d{8}", text):
            digits = text
        else:
            return None
        try:
            parsed = datetime.strptime(digits, "%Y%m%d")
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed.strftime("%Y-%m-%d")

    @classmethod
    def _normalize_raw_bar(cls, row, expected_date: str | None = None, *, source: str = "tushare", basis: str = "unadjusted") -> dict:
        """Validate one raw daily row without changing the legacy tables."""
        if not isinstance(row, dict):
            row = {key: getattr(row, key) for key in ("code", "ts_code", "trade_date", "name", "open", "high", "low", "close", "pre_close", "pct_change", "pct_chg", "volume", "vol", "amount") if hasattr(row, key)}
        trade_date = cls._canonical_raw_date(row.get("trade_date"))
        expected = cls._canonical_raw_date(expected_date) if expected_date else trade_date
        if not trade_date or not expected or trade_date != expected:
            raise ValueError("raw bar has an invalid or unexpected trade_date")
        code_info = cls._canonical_raw_code(row.get("ts_code") or row.get("code") or row.get("symbol"))
        if not code_info:
            raise ValueError("raw bar has an invalid ts_code")
        code, ts_code = code_info

        def number(*keys, default=None, positive=False, nonnegative=False):
            value = default
            for key in keys:
                if key in row:
                    value = row.get(key)
                    break
            if isinstance(value, bool) or value is None or str(value).strip() == "":
                raise ValueError(f"raw bar missing {keys[0]}")
            try:
                value = float(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"raw bar has invalid {keys[0]}") from exc
            if not math.isfinite(value) or (positive and value <= 0) or (nonnegative and value < 0):
                raise ValueError(f"raw bar has invalid {keys[0]}")
            return value

        close = number("close", positive=True)
        open_value = number("open", positive=True)
        high = number("high", positive=True)
        low = number("low", positive=True)
        if high < max(open_value, close) or low > min(open_value, close) or high < low:
            raise ValueError("raw bar violates OHLC bounds")
        pre_close = number("pre_close", "prev_close", default=close, positive=True)
        pct_change = number("pct_chg", "pct_change", default=(close / pre_close - 1) * 100)
        volume = number("vol", "volume", default=0.0, nonnegative=True)
        amount = number("amount", default=0.0, nonnegative=True)
        expected_pct = (close / pre_close - 1) * 100
        if abs(pct_change - expected_pct) > 0.35:
            raise ValueError("raw bar pct_chg does not match close/pre_close")
        source_text = str(row.get("source") or source or "").strip()[:80]
        basis_text = str(row.get("basis") or row.get("price_basis") or basis or "").strip().lower()
        if basis_text != "unadjusted":
            raise ValueError("raw bar price basis is not unadjusted")
        name = str(row.get("name") or "").strip()[:128]
        return {
            "trade_date": trade_date,
            "code": code,
            "ts_code": ts_code,
            "name": name,
            "open": open_value,
            "high": high,
            "low": low,
            "close": close,
            "pre_close": pre_close,
            "pct_change": pct_change,
            "volume": volume,
            "amount": amount,
            "source": source_text,
            "basis": basis_text,
        }

    @staticmethod
    def _raw_market(value) -> str:
        code_info = StockStore._canonical_raw_code(value)
        if not code_info:
            return ""
        return code_info[1].rsplit(".", 1)[-1]

    @staticmethod
    def _raw_bar_payload(row) -> dict:
        """Return the fixed-field representation used by partition digests."""
        return {
            "trade_date": StockStore._canonical_raw_date(row.get("trade_date")) or str(row.get("trade_date") or ""),
            "code": str(row.get("code") or ""),
            "ts_code": str(row.get("ts_code") or ""),
            "name": str(row.get("name") or ""),
            "open": float(row.get("open")),
            "high": float(row.get("high")),
            "low": float(row.get("low")),
            "close": float(row.get("close")),
            "pre_close": float(row.get("pre_close")),
            "pct_change": float(row.get("pct_change")),
            "volume": float(row.get("volume")),
            "amount": float(row.get("amount")),
            "source": str(row.get("source") or ""),
            "basis": str(row.get("basis") or "").strip().lower(),
        }

    @classmethod
    def _raw_digest_for_rows(cls, rows) -> str:
        ordered = [cls._raw_bar_payload(row) for row in sorted(rows, key=lambda item: (str(item.get("code") or ""), str(item.get("ts_code") or "")))]
        payload = json.dumps(ordered, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def _raw_partition_db_digest(cls, db, partition_id: str) -> tuple[str, int, dict[str, int]]:
        rows = [
            dict(row) for row in db.execute(
                "SELECT trade_date,code,ts_code,name,open,high,low,close,pre_close,pct_change,volume,amount,source,basis "
                "FROM partition_bars WHERE partition_id=? ORDER BY code,ts_code",
                (str(partition_id),),
            )
        ]
        market_counts: dict[str, int] = {}
        for row in rows:
            market = cls._raw_market(row.get("ts_code") or row.get("code"))
            if not market:
                raise RuntimeError("raw partition contains an invalid market code")
            market_counts[market] = market_counts.get(market, 0) + 1
        return cls._raw_digest_for_rows(rows), len(rows), market_counts

    @staticmethod
    def _strict_market_counts_json(value, expected: dict[str, int]) -> bool:
        """Compare persisted market counts without coercing malformed JSON."""
        try:
            parsed = json.loads(str(value or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(parsed, dict) or any(not isinstance(key, str) for key in parsed):
            return False
        for key, raw in parsed.items():
            if key not in {"SH", "SZ", "BJ"} or isinstance(raw, bool):
                return False
            if not isinstance(raw, int) or raw <= 0:
                return False
        try:
            # JSON numeric spelling is part of the append-only partition
            # contract: 1.0 and true must not become a valid integer count.
            actual_text = json.dumps(parsed, sort_keys=True, separators=(",", ":"), allow_nan=False)
            expected_text = json.dumps(expected, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError, OverflowError):
            return False
        return actual_text == expected_text

    @staticmethod
    def _strict_db_int(value, *, minimum: int | None = None, maximum: int | None = None) -> int | None:
        """Read an integer column without truncating REAL/text tampering."""
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            parsed = value
        elif isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                return None
            # SQLite normally returns generated integer columns as ``int``;
            # accepting an integral REAL keeps old repaired stores readable.
            parsed = int(value)
        elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
            try:
                parsed = int(value.strip())
            except (TypeError, ValueError, OverflowError):
                return None
        else:
            return None
        if minimum is not None and parsed < minimum:
            return None
        if maximum is not None and parsed > maximum:
            return None
        return parsed

    @staticmethod
    def _median(values) -> float:
        ordered = sorted(float(value) for value in values)
        if not ordered:
            return 0.0
        middle = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) / 2.0

    @staticmethod
    def _strict_positive_int(value, message: str) -> int:
        if isinstance(value, bool):
            raise ValueError(message)
        if isinstance(value, int):
            parsed = value
            try:
                numeric = float(parsed)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(message) from exc
            if not math.isfinite(numeric) or numeric != parsed:
                raise ValueError(message)
        elif isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                raise ValueError(message)
            parsed = int(value)
        elif isinstance(value, str) and re.fullmatch(r"[+\-]?\d+", value.strip()):
            try:
                parsed = int(value.strip())
                numeric = float(value.strip())
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(message) from exc
            if not math.isfinite(numeric) or numeric != parsed:
                raise ValueError(message)
        else:
            raise ValueError(message)
        if parsed <= 0:
            raise ValueError(message)
        return parsed

    @staticmethod
    def _strict_bool(value, message: str) -> bool:
        if isinstance(value, bool):
            return value
        if value in (0, 1, "0", "1", "false", "true", "False", "True"):
            return str(value).lower() in {"1", "true"}
        raise ValueError(message)

    @classmethod
    def _universe_digest_payload(cls, evidence: dict) -> dict:
        """Return the canonical, digestable part of universe evidence."""
        payload = {
            "evidence_version": int(evidence.get("evidence_version") or 0),
            "method": str(evidence.get("method") or ""),
            "source": str(evidence.get("source") or ""),
            "universe_version": str(evidence.get("universe_version") or ""),
            "effective_date": str(evidence.get("effective_date") or ""),
            "valid_from": str(evidence.get("valid_from") or ""),
            "valid_to": str(evidence.get("valid_to") or ""),
            "total": evidence.get("total"),
            "markets": dict(sorted((evidence.get("markets") or {}).items())),
            "eligible_markets": sorted(str(value) for value in (evidence.get("eligible_markets") or [])),
            "bj_calendar_policy": str(evidence.get("bj_calendar_policy") or ""),
            "status_counts": dict(sorted((evidence.get("status_counts") or {}).items())),
            "suspension_method": str(evidence.get("suspension_method") or ""),
            "suspension_evidence": bool(evidence.get("suspension_evidence")),
        }
        try:
            version = int(evidence.get("evidence_version") or 0)
        except (TypeError, ValueError, OverflowError):
            version = 0
        if version >= 2:
            raw_memberships = evidence.get("memberships")
            if not isinstance(raw_memberships, list):
                raw_memberships = evidence.get("list_status_membership")
            memberships = []
            for item in raw_memberships if isinstance(raw_memberships, list) else []:
                if not isinstance(item, dict):
                    continue
                memberships.append({
                    "code": str(item.get("code") or ""),
                    "ts_code": str(item.get("ts_code") or ""),
                    "market": str(item.get("market") or "").upper(),
                    "list_status": str(item.get("list_status") or "").upper(),
                    "list_date": str(item.get("list_date") or ""),
                    "delist_date": str(item.get("delist_date") or ""),
                })
            memberships.sort(key=lambda item: (item["code"], item["list_status"], item["list_date"], item["delist_date"]))
            payload.update({
                "target_session": str(evidence.get("target_session") or ""),
                "calendar_policy": str(evidence.get("calendar_policy") or ""),
                "memberships": memberships,
                "status_digests": dict(sorted((evidence.get("status_digests") or {}).items())),
                "membership_digest": str(evidence.get("membership_digest") or ""),
                "market_digest": str(evidence.get("market_digest") or ""),
                "exact_market_digest": str(evidence.get("exact_market_digest") or ""),
            })
        return payload

    @classmethod
    def _universe_evidence_digest(cls, evidence: dict) -> str:
        text = json.dumps(cls._universe_digest_payload(evidence), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @classmethod
    def _normalize_universe_counts(cls, value) -> dict:
        """Normalize a versioned, independent full-market universe snapshot.

        Counts are intentionally not inferred from raw partitions.  The
        normalized object keeps enough provenance for storage to revalidate a
        published batch, while the canonical digest closes the evidence
        manifest against direct SQLite edits.
        """
        if value is None or value == "":
            return {}
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError("universe evidence is not valid JSON") from exc
        if isinstance(value, dict) and isinstance(value.get("universe_evidence"), dict):
            value = value["universe_evidence"]
        if isinstance(value, bool) or not isinstance(value, dict):
            raise ValueError("universe evidence must be an object")

        raw = dict(value)
        try:
            evidence_version = cls._strict_positive_int(
                raw.get("evidence_version", raw.get("schema_version", 1)),
                "universe evidence version is invalid",
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("universe evidence version is invalid") from exc

        def first(*keys):
            for key in keys:
                if key in raw and raw.get(key) not in (None, ""):
                    return raw.get(key)
            return None

        method = str(first("method", "evidence_method", "universe_method") or "").strip().lower()
        source = str(first("source", "provider", "evidence_source") or "").strip().lower()
        universe_version = str(first("universe_version", "version", "versioned_id", "universe_id") or "").strip()[:160]
        effective_date = cls._canonical_raw_date(first("effective_date", "trade_date", "as_of", "requested_date", "date") or "") or ""
        valid_from = cls._canonical_raw_date(first("valid_from", "effective_from", "start_date") or "") or ""
        valid_to = cls._canonical_raw_date(first("valid_to", "effective_to", "end_date") or "") or ""
        policy = str(first("bj_calendar_policy", "bj_policy", "calendar_policy") or "").strip().lower()
        suspension_method = str(first("suspension_method", "suspension_policy") or "").strip().lower()
        suspension_value = first("suspension_evidence", "has_suspension_evidence")
        suspension_evidence = False if suspension_value in (None, "") else cls._strict_bool(suspension_value, "suspension evidence is invalid")

        nested = raw.get("market_counts")
        if nested is None:
            nested = raw.get("eligible_market_counts")
        if nested is None and isinstance(raw.get("markets"), dict):
            nested = raw.get("markets")
        if nested is None and isinstance(raw.get("eligible_markets"), dict):
            nested = raw.get("eligible_markets")
        if nested is None:
            nested = {key: raw[key] for key in ("SH", "SZ", "BJ") if key in raw}
        if not isinstance(nested, dict):
            nested = {}
        normalized_markets: dict[str, int] = {}
        for key, raw_count in nested.items():
            market = str(key or "").strip().upper()
            if market not in {"SH", "SZ", "BJ"}:
                raise ValueError("universe evidence contains an unknown market")
            normalized_markets[market] = cls._strict_positive_int(raw_count, "universe market evidence is invalid")

        eligible_raw = raw.get("eligible_markets")
        if isinstance(eligible_raw, dict):
            eligible = list(eligible_raw)
        elif isinstance(eligible_raw, str):
            eligible = re.split(r"[,\s]+", eligible_raw.strip()) if eligible_raw.strip() else []
        elif isinstance(eligible_raw, (list, tuple, set)):
            eligible = list(eligible_raw)
        else:
            eligible = list(normalized_markets)
        eligible_markets = sorted({str(item or "").strip().upper() for item in eligible if str(item or "").strip()})

        total_value = first("total", "eligible_total", "overall", "count", "universe", "expected_total")
        total = None
        if total_value not in (None, ""):
            total = cls._strict_positive_int(total_value, "universe total evidence is invalid")
        elif normalized_markets:
            total = sum(normalized_markets.values())

        statuses = raw.get("status_counts", raw.get("list_status_counts", raw.get("statuses", {})))
        if statuses in (None, ""):
            statuses = {}
        if not isinstance(statuses, dict):
            raise ValueError("universe status evidence is invalid")
        status_counts: dict[str, int] = {}
        for key, raw_count in statuses.items():
            status = str(key or "").strip().upper()
            if status not in {"L", "D", "P"}:
                raise ValueError("universe status evidence is invalid")
            status_counts[status] = cls._strict_positive_int(raw_count, "universe status evidence is invalid")

        digest = str(first("digest", "universe_digest", "canonical_digest") or "").strip().lower()
        normalized = {
            "evidence_version": evidence_version,
            "method": method,
            "source": source,
            "universe_version": universe_version,
            "effective_date": effective_date,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "total": total,
            "markets": dict(sorted(normalized_markets.items())),
            "eligible_markets": eligible_markets,
            "bj_calendar_policy": policy,
            "status_counts": dict(sorted(status_counts.items())),
            "suspension_method": suspension_method,
            "suspension_evidence": suspension_evidence,
            "digest": digest,
        }
        if evidence_version >= 2:
            memberships = raw.get("memberships")
            if not isinstance(memberships, list):
                memberships = raw.get("list_status_membership")
            normalized_memberships = []
            for item in memberships if isinstance(memberships, list) else []:
                if not isinstance(item, dict):
                    continue
                normalized_memberships.append({
                    "code": str(item.get("code") or "").strip(),
                    "ts_code": str(item.get("ts_code") or "").strip().upper(),
                    "market": str(item.get("market") or "").strip().upper(),
                    "list_status": str(item.get("list_status") or "").strip().upper(),
                    "list_date": cls._canonical_raw_date(item.get("list_date") or "") or "",
                    "delist_date": cls._canonical_raw_date(item.get("delist_date") or "") or "",
                })
            normalized_memberships.sort(key=lambda item: (item["code"], item["list_status"], item["list_date"], item["delist_date"]))
            normalized.update({
                "target_session": cls._canonical_raw_date(raw.get("target_session") or raw.get("effective_date") or "") or "",
                "calendar_policy": str(raw.get("calendar_policy") or "").strip().lower(),
                "memberships": normalized_memberships,
                "list_status_membership": normalized_memberships,
                "status_digests": dict(sorted((raw.get("status_digests") or {}).items())),
                "membership_digest": str(raw.get("membership_digest") or "").strip().lower(),
                "market_digest": str(raw.get("market_digest") or "").strip().lower(),
                "exact_market_digest": str(raw.get("exact_market_digest") or "").strip().lower(),
            })
        return normalized

    @classmethod
    def _validate_universe_evidence(
        cls,
        evidence: dict,
        *,
        requested_date: str = "",
        actual_trade_date: str = "",
        bj_calendar_policy: str = "",
        universe_version: str = "",
        observed_markets: set[str] | None = None,
    ) -> list[str]:
        errors: list[str] = []
        if not isinstance(evidence, dict) or not evidence:
            return ["universe evidence is missing"]
        raw_evidence_version = evidence.get("evidence_version")
        if isinstance(raw_evidence_version, bool):
            evidence_version = 0
        elif isinstance(raw_evidence_version, int):
            evidence_version = raw_evidence_version
        elif isinstance(raw_evidence_version, float) and math.isfinite(raw_evidence_version) and raw_evidence_version.is_integer():
            evidence_version = int(raw_evidence_version)
        elif isinstance(raw_evidence_version, str) and re.fullmatch(r"[+\-]?\d+", raw_evidence_version.strip()):
            try:
                evidence_version = int(raw_evidence_version.strip())
            except (TypeError, ValueError, OverflowError):
                evidence_version = 0
        else:
            evidence_version = 0
        if evidence_version not in {1, 2}:
            errors.append("universe evidence version is unsupported")
        if not str(evidence.get("method") or "").strip() or not str(evidence.get("source") or "").strip():
            errors.append("universe evidence method/source is missing")
        evidence_version_id = str(evidence.get("universe_version") or "").strip()
        if not evidence_version_id:
            errors.append("universe evidence version identifier is missing")
        expected_version = str(universe_version or "").strip()
        if expected_version and evidence_version_id and evidence_version_id != expected_version:
            errors.append("universe evidence version identifier mismatch")
        digest = str(evidence.get("digest") or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            errors.append("universe evidence digest is missing or invalid")
        else:
            try:
                digest_matches = digest == cls._universe_evidence_digest(evidence)
            except (TypeError, ValueError, OverflowError):
                digest_matches = False
            if not digest_matches:
                errors.append("universe evidence digest mismatch")
        effective = cls._canonical_raw_date(evidence.get("effective_date") or "")
        requested = cls._canonical_raw_date(requested_date) if requested_date else ""
        actual = cls._canonical_raw_date(actual_trade_date) if actual_trade_date else ""
        if not effective:
            errors.append("universe evidence effective date is missing")
        if requested and effective and effective > requested:
            errors.append("universe evidence is from a future date")
        if actual and effective and effective != actual:
            errors.append("universe evidence date is stale or mismatched")
        valid_from = cls._canonical_raw_date(evidence.get("valid_from") or "")
        valid_to = cls._canonical_raw_date(evidence.get("valid_to") or "")
        if valid_from and valid_to and valid_from > valid_to:
            errors.append("universe evidence validity range is invalid")
        target = actual or requested
        if target and valid_from and target < valid_from:
            errors.append("universe evidence is not yet effective")
        if target and valid_to and target > valid_to:
            errors.append("universe evidence is stale")
        policy = str(evidence.get("bj_calendar_policy") or "").strip().lower()
        if policy not in {"require_bse", "sse_fallback", "exclude"}:
            errors.append("universe evidence BJ policy is missing or invalid")
        expected_policy = str(bj_calendar_policy or "").strip().lower()
        if expected_policy and policy and policy != expected_policy:
            errors.append("universe evidence BJ policy mismatch")
        markets = evidence.get("markets") if isinstance(evidence.get("markets"), dict) else {}
        eligible_raw = evidence.get("eligible_markets")
        if isinstance(eligible_raw, (list, tuple, set)):
            eligible = {str(value or "").strip().upper() for value in eligible_raw if str(value or "").strip()}
        else:
            eligible = set()
        market_keys = set(markets) if all(isinstance(key, str) for key in markets) else set()
        if any(key not in {"SH", "SZ", "BJ"} for key in market_keys):
            errors.append("universe evidence contains an unknown market")
        valid_market_counts = all(
            isinstance(value, int) and not isinstance(value, bool) and value > 0
            for value in markets.values()
        )
        if not eligible or eligible != market_keys:
            errors.append("universe evidence market set is invalid")
        required_markets = {"SH", "SZ"}
        if policy in {"require_bse", "sse_fallback"}:
            required_markets.add("BJ")
        if eligible != required_markets or market_keys != required_markets:
            errors.append("universe evidence market set is invalid for BJ policy")
        total = evidence.get("total")
        if (
            not isinstance(total, int)
            or isinstance(total, bool)
            or total <= 0
            or not markets
            or not valid_market_counts
            or sum(markets.values()) != total
        ):
            errors.append("universe evidence total does not match eligible markets")
        if not str(evidence.get("suspension_method") or "").strip():
            errors.append("universe suspension method is missing")
        if evidence_version >= 2:
            memberships = evidence.get("memberships")
            if not isinstance(memberships, list) or not memberships:
                errors.append("universe evidence membership is missing")
            else:
                canonical_memberships = []
                seen_codes: set[str] = set()
                status_from_membership: dict[str, int] = {}
                markets_from_membership: dict[str, int] = {}
                for item in memberships:
                    if not isinstance(item, dict):
                        errors.append("universe evidence membership row is invalid")
                        continue
                    code = str(item.get("code") or "").strip()
                    ts_code = str(item.get("ts_code") or "").strip().upper()
                    market = str(item.get("market") or "").strip().upper()
                    status = str(item.get("list_status") or "").strip().upper()
                    list_date = cls._canonical_raw_date(item.get("list_date") or "") or ""
                    delist_date = cls._canonical_raw_date(item.get("delist_date") or "") or ""
                    if not re.fullmatch(r"\d{6}", code) or not re.fullmatch(r"\d{6}\.[A-Z]{2}", ts_code) or market not in {"SH", "SZ", "BJ"} or status not in {"L", "D", "P"} or not list_date:
                        errors.append("universe evidence membership row is invalid")
                        continue
                    if code in seen_codes:
                        errors.append("universe evidence membership contains duplicate code")
                    seen_codes.add(code)
                    canonical_memberships.append({"code": code, "ts_code": ts_code, "market": market, "list_status": status, "list_date": list_date, "delist_date": delist_date})
                    status_from_membership[status] = status_from_membership.get(status, 0) + 1
                    markets_from_membership[market] = markets_from_membership.get(market, 0) + 1
                    if delist_date and delist_date < list_date:
                        errors.append("universe evidence membership date range is invalid")
                canonical_memberships.sort(key=lambda item: (item["code"], item["list_status"], item["list_date"], item["delist_date"]))
                try:
                    membership_text = json.dumps(canonical_memberships, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                    expected_membership_digest = hashlib.sha256(membership_text.encode("utf-8")).hexdigest()
                    market_text = json.dumps(dict(sorted(markets.items())), sort_keys=True, separators=(",", ":"))
                    expected_market_digest = hashlib.sha256(market_text.encode("utf-8")).hexdigest()
                except (TypeError, ValueError, OverflowError):
                    expected_membership_digest = expected_market_digest = ""
                if str(evidence.get("membership_digest") or "").lower() != expected_membership_digest:
                    errors.append("universe evidence membership digest mismatch")
                if str(evidence.get("market_digest") or "").lower() != expected_market_digest or str(evidence.get("exact_market_digest") or "").lower() != expected_market_digest:
                    errors.append("universe evidence market digest mismatch")
                if status_from_membership != {str(k): int(v) for k, v in (evidence.get("status_counts") or {}).items()}:
                    errors.append("universe evidence status membership mismatch")
                if markets_from_membership != {str(k): int(v) for k, v in markets.items()}:
                    errors.append("universe evidence market membership mismatch")
                if len(canonical_memberships) != int(total or 0):
                    errors.append("universe evidence membership total mismatch")
            if str(evidence.get("calendar_policy") or "").strip().lower() not in {"sse_fallback", ""}:
                errors.append("universe evidence calendar policy is invalid")
        if observed_markets is not None and not observed_markets.issubset(eligible):
            errors.append("raw rows contain a market outside the eligible universe")
        return errors

    @classmethod
    def _raw_coverage_metrics(
        cls,
        daily_counts: dict[str, dict[str, int]],
        *,
        min_row_count: int = DEFAULT_RAW_MIN_ROW_COUNT,
        min_overall_coverage: float = 0.97,
        min_market_coverage: float = 0.95,
        min_market_median_ratio: float = 0.95,
        universe_version: str = "",
        universe_counts=None,
        require_universe_evidence: bool = DEFAULT_RAW_REQUIRE_UNIVERSE_EVIDENCE,
        requested_date: str = "",
        actual_trade_date: str = "",
        bj_calendar_policy: str = "",
    ) -> dict:
        try:
            minimum_rows = max(1, int(min_row_count))
            overall_floor = float(min_overall_coverage)
            market_floor = float(min_market_coverage)
            median_floor = float(min_market_median_ratio)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("invalid raw coverage thresholds") from exc
        if not all(math.isfinite(value) and 0 <= value <= 1 for value in (overall_floor, market_floor, median_floor)):
            raise ValueError("invalid raw coverage thresholds")
        evidence_errors: list[str] = []
        try:
            evidence = cls._normalize_universe_counts(universe_counts)
        except (TypeError, ValueError, OverflowError) as exc:
            evidence = {}
            evidence_errors.append(str(exc) or "universe evidence is invalid")
        expected_total = evidence.get("total")
        expected_markets = dict(evidence.get("markets") or {})
        has_evidence = bool(evidence)
        dates = sorted(str(date) for date in daily_counts)
        totals = [max(0, int(daily_counts[date].get("total", 0) or 0)) for date in dates]
        total_median = cls._median(totals)
        errors: list[str] = []
        if not dates:
            errors.append("no daily partitions")
        # Raw data is fail-closed regardless of a caller's legacy boolean.
        # An operator may lower a numeric threshold for a fixture, but may
        # never disable independent universe evidence.
        if not has_evidence:
            errors.append("universe evidence is missing")
        errors.extend(evidence_errors)
        observed_markets = {
            market for date in dates for market in daily_counts[date] if market != "total" and int(daily_counts[date].get(market, 0) or 0) > 0
        }
        universe_validation_errors = cls._validate_universe_evidence(
            evidence,
            requested_date=requested_date,
            actual_trade_date=actual_trade_date,
            bj_calendar_policy=bj_calendar_policy,
            universe_version=universe_version,
            observed_markets=observed_markets,
        )
        errors.extend(universe_validation_errors)
        if any(total < minimum_rows for total in totals):
            errors.append("daily row count below configured minimum")
        overall_ratios = [total / total_median for total in totals] if total_median > 0 else []
        relative_overall = min(overall_ratios) if overall_ratios else 0.0
        expected_overall_ratios = [total / expected_total for total in totals] if expected_total else []
        absolute_overall = min(expected_overall_ratios) if expected_overall_ratios else None
        overall_coverage = min(relative_overall, absolute_overall) if absolute_overall is not None else relative_overall
        if absolute_overall is not None and absolute_overall + 1e-12 < overall_floor:
            errors.append("overall coverage below universe floor")
        if overall_coverage + 1e-12 < overall_floor:
            errors.append("overall coverage below floor")

        markets = sorted(observed_markets | set(expected_markets))
        market_medians: dict[str, float] = {}
        market_coverage: dict[str, float] = {}
        market_evidence: dict[str, bool] = {}
        market_median_ratios: dict[str, float] = {}
        for market in markets:
            counts = [max(0, int(daily_counts[date].get(market, 0) or 0)) for date in dates]
            market_has_rows = max(counts, default=0) > 0
            market_evidence[market] = market_has_rows
            median = cls._median(counts)
            market_medians[market] = median
            ratios = [count / median for count in counts] if median > 0 else []
            relative_coverage = min(ratios) if ratios else 0.0
            expected_market = expected_markets.get(market)
            absolute_ratios = [count / expected_market for count in counts] if expected_market else []
            absolute_coverage = min(absolute_ratios) if absolute_ratios else None
            median_ratio = median / expected_market if expected_market else None
            coverage = min(relative_coverage, absolute_coverage) if absolute_coverage is not None else relative_coverage
            market_coverage[market] = coverage
            if median_ratio is not None:
                market_median_ratios[market] = median_ratio
            if expected_market and not market_has_rows:
                errors.append(f"{market} universe evidence is missing")
            if absolute_coverage is not None and absolute_coverage + 1e-12 < market_floor:
                errors.append(f"{market} coverage below universe floor")
            if median_ratio is not None and median_ratio + 1e-12 < median_floor:
                errors.append(f"{market} median coverage below floor")
            if coverage + 1e-12 < market_floor or relative_coverage + 1e-12 < median_floor:
                errors.append(f"{market} coverage below floor")
        for market in sorted(set(expected_markets) - observed_markets):
            errors.append(f"{market} expected market is missing")

        return {
            "coverage_version": 1,
            "universe_version": str(evidence.get("universe_version") or universe_version or ""),
            "universe_evidence": bool(has_evidence and not evidence_errors and not universe_validation_errors),
            "universe_evidence_errors": list(dict.fromkeys(evidence_errors + universe_validation_errors)),
            "require_universe_evidence": True,
            "universe_digest": str(evidence.get("digest") or ""),
            "universe_method": str(evidence.get("method") or ""),
            "universe_source": str(evidence.get("source") or ""),
            "universe_effective_date": str(evidence.get("effective_date") or ""),
            "eligible_markets": sorted(str(value) for value in (evidence.get("eligible_markets") or [])),
            "bj_calendar_policy": str(evidence.get("bj_calendar_policy") or ""),
            "suspension_method": str(evidence.get("suspension_method") or ""),
            "expected_universe": {"total": expected_total, "markets": expected_markets},
            "dates": dates,
            "daily_counts": {date: {key: int(value) for key, value in sorted(daily_counts[date].items())} for date in dates},
            "window_median": total_median,
            "market_medians": market_medians,
            "market_median_ratios": market_median_ratios,
            "market_evidence": market_evidence,
            "overall_coverage": overall_coverage,
            "relative_overall_coverage": relative_overall,
            "universe_overall_coverage": absolute_overall,
            "market_coverage": market_coverage,
            "min_row_count": minimum_rows,
            "min_overall_coverage": overall_floor,
            "min_market_coverage": market_floor,
            "min_market_median_ratio": median_floor,
            "coverage_ok": not errors,
            "errors": errors,
        }

    @staticmethod
    def _raw_completed_date(db, value: str) -> bool:
        normalized = StockStore._canonical_raw_date(value)
        if not normalized:
            return False
        try:
            parsed = datetime.strptime(normalized, "%Y-%m-%d").date()
        except (TypeError, ValueError, OverflowError):
            return False
        china_now = datetime.now(timezone(timedelta(hours=8)))
        if parsed > china_now.date() or (parsed == china_now.date() and china_now.hour < 15):
            return False
        row = db.execute("SELECT status,is_open FROM trading_calendar WHERE trade_date=?", (normalized,)).fetchone()
        if row:
            status = str(row[0] or "").strip().lower()
            try:
                open_value = int(row[1])
            except (TypeError, ValueError, OverflowError):
                return False
            if open_value not in {0, 1}:
                return False
            return (status == "open" and open_value == 1) or (not status and open_value == 1)
        return parsed.weekday() < 5

    @staticmethod
    def _raw_dataset_id(dataset_key: str, provider: str, frequency: str, basis: str, universe: str) -> str:
        value = "|".join((dataset_key, provider, frequency, basis, universe))
        return "dataset-" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]

    def _get_or_create_raw_dataset(self, db, dataset_key: str = "tushare_daily", provider: str = "tushare", frequency: str = "1d", basis: str = "unadjusted", universe: str = "A") -> str:
        dataset_key = str(dataset_key or "tushare_daily").strip()[:80]
        provider = str(provider or "tushare").strip()[:80]
        frequency = str(frequency or "1d").strip()[:20]
        basis = str(basis or "unadjusted").strip().lower()[:32]
        universe = str(universe or "A").strip()[:32]
        if not dataset_key or not provider or basis != "unadjusted":
            raise ValueError("invalid raw dataset identity")
        row = db.execute(
            "SELECT dataset_id FROM datasets WHERE dataset_key=? AND provider=? AND frequency=? AND basis=? AND universe=?",
            (dataset_key, provider, frequency, basis, universe),
        ).fetchone()
        if row:
            return str(row[0])
        dataset_id = self._raw_dataset_id(dataset_key, provider, frequency, basis, universe)
        db.execute(
            "INSERT INTO datasets(dataset_id,dataset_key,provider,frequency,basis,universe,created_at) VALUES(?,?,?,?,?,?,?)",
            (dataset_id, dataset_key, provider, frequency, basis, universe, datetime.utcnow().isoformat()),
        )
        return dataset_id

    def create_raw_batch(
        self,
        requested_date: str,
        *,
        dataset_key: str = "tushare_daily",
        provider: str = "tushare",
        frequency: str = "1d",
        basis: str = "unadjusted",
        universe: str = "A",
        expected_days: int = 0,
        min_row_count: int = DEFAULT_RAW_MIN_ROW_COUNT,
        min_overall_coverage: float = 0.97,
        min_market_coverage: float = 0.95,
        min_market_median_ratio: float = 0.95,
        page_size: int = 6000,
        expected_trade_dates=None,
        universe_version: str = "",
        universe_counts=None,
        universe_evidence=None,
        require_universe_evidence: bool = DEFAULT_RAW_REQUIRE_UNIVERSE_EVIDENCE,
        bj_calendar_policy: str | None = None,
        batch_id: str | None = None,
    ) -> str:
        requested = self._canonical_raw_date(requested_date)
        if not requested:
            raise ValueError("raw batch requested_date must be YYYY-MM-DD")
        batch_id = str(batch_id or ("batch-" + uuid.uuid4().hex))
        now = datetime.utcnow().isoformat()
        try:
            min_row_count = max(1, int(min_row_count))
            min_overall_coverage = float(min_overall_coverage)
            min_market_coverage = float(min_market_coverage)
            min_market_median_ratio = float(min_market_median_ratio)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("invalid raw batch coverage thresholds") from exc
        if not all(math.isfinite(value) and 0 <= value <= 1 for value in (min_overall_coverage, min_market_coverage, min_market_median_ratio)):
            raise ValueError("invalid raw batch coverage thresholds")
        try:
            page_size = max(1, min(int(page_size), 6000))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("invalid raw batch page size") from exc
        expected_dates: list[str] = []
        if expected_trade_dates is not None:
            for value in expected_trade_dates:
                normalized_date = self._canonical_raw_date(value)
                if not normalized_date:
                    raise ValueError("expected_trade_dates contains an invalid date")
                expected_dates.append(normalized_date)
            expected_dates = sorted(set(expected_dates))
            if expected_days and len(expected_dates) != int(expected_days):
                raise ValueError("expected_trade_dates does not match expected_days")
            expected_days = len(expected_dates)
        raw_universe_counts = self._normalize_universe_counts(
            universe_counts if universe_counts is not None else universe_evidence
        )
        evidence_version = str(raw_universe_counts.get("universe_version") or "").strip()
        requested_universe_version = str(universe_version or "").strip()
        if requested_universe_version and evidence_version and requested_universe_version != evidence_version:
            raise ValueError("universe evidence version identifier mismatch")
        if not requested_universe_version and evidence_version:
            universe_version = evidence_version
        policy = str(bj_calendar_policy or raw_universe_counts.get("bj_calendar_policy") or "unknown").strip().lower()
        with self._connect() as db:
            dataset_id = self._get_or_create_raw_dataset(db, dataset_key, provider, frequency, basis, universe)
            db.execute(
                "INSERT INTO batches(batch_id,dataset_id,requested_date,status,quality,source,basis,expected_days,min_row_count,min_overall_coverage,min_market_coverage,min_market_median_ratio,page_size,universe_version,universe_counts_json,require_universe_evidence,bj_calendar_policy,expected_dates_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    batch_id, dataset_id, requested, "staging", "unknown", str(provider).strip()[:80],
                    str(basis).lower(), max(0, int(expected_days)), min_row_count,
                    min_overall_coverage, min_market_coverage, min_market_median_ratio, page_size,
                    str(universe_version or "").strip()[:160],
                    json.dumps(raw_universe_counts, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    1, policy, json.dumps(expected_dates, separators=(",", ":")), now,
                ),
            )
        return batch_id

    def find_raw_batch(
        self,
        requested_date: str,
        *,
        dataset_key: str = "tushare_daily",
        provider: str = "tushare",
        frequency: str = "1d",
        basis: str = "unadjusted",
        universe: str = "A",
        expected_days: int = 0,
        expected_trade_dates=None,
        page_size: int = 6000,
        min_row_count: int = DEFAULT_RAW_MIN_ROW_COUNT,
        min_overall_coverage: float = 0.97,
        min_market_coverage: float = 0.95,
        min_market_median_ratio: float = 0.95,
        bj_calendar_policy: str | None = None,
        universe_version: str = "",
        universe_counts=None,
        universe_evidence=None,
        statuses=("staging", "failed"),
    ) -> dict | None:
        """Find a restartable raw batch with the same immutable identity."""
        requested = self._canonical_raw_date(requested_date)
        if not requested:
            raise ValueError("raw batch requested_date must be YYYY-MM-DD")
        evidence = self._normalize_universe_counts(universe_counts if universe_counts is not None else universe_evidence)
        evidence_version = str(evidence.get("universe_version") or universe_version or "").strip()
        evidence_digest = str(evidence.get("digest") or "").strip().lower()
        try:
            wanted_days = max(0, int(expected_days))
            wanted_page_size = max(1, min(int(page_size), 6000))
            wanted_min_rows = max(1, int(min_row_count))
            floors = tuple(float(value) for value in (min_overall_coverage, min_market_coverage, min_market_median_ratio))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("invalid raw batch identity") from exc
        expected = None
        if expected_trade_dates is not None:
            expected = set()
            for value in expected_trade_dates:
                normalized = self._canonical_raw_date(value)
                if not normalized:
                    raise ValueError("expected_trade_dates contains an invalid date")
                expected.add(normalized)
            wanted_days = len(expected) if not wanted_days else wanted_days
        status_values = tuple(str(value).strip().lower() for value in (statuses or ("staging", "failed")))
        if not status_values:
            status_values = ("staging", "failed")
        placeholders = ",".join("?" for _ in status_values) or "?"
        with self._connect() as db:
            dataset = db.execute(
                "SELECT dataset_id FROM datasets WHERE dataset_key=? AND provider=? AND frequency=? AND basis=? AND universe=?",
                (str(dataset_key or "tushare").strip(), str(provider or "tushare").strip(), str(frequency or "1d").strip(), str(basis or "unadjusted").strip().lower(), str(universe or "A").strip()),
            ).fetchone()
            if not dataset:
                return None
            rows = db.execute(
                f"SELECT b.*,d.dataset_key,d.provider,d.frequency,d.basis AS dataset_basis,d.universe FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.dataset_id=? AND b.requested_date=? AND b.status IN ({placeholders}) ORDER BY CASE b.status WHEN 'staging' THEN 0 ELSE 1 END,b.created_at DESC",
                (dataset[0], requested, *status_values),
            ).fetchall()
            for row in rows:
                row_evidence = self._normalize_universe_counts(str(row["universe_counts_json"] or "{}"))
                if evidence_digest and str(row["universe_counts_json"] or "") and str(row_evidence.get("digest") or "").lower() != evidence_digest:
                    continue
                if evidence_version and str(row["universe_version"] or "") != evidence_version:
                    continue
                if wanted_days and self._strict_db_int(row["expected_days"], minimum=0) != wanted_days:
                    continue
                if wanted_page_size and self._strict_db_int(row["page_size"], minimum=1, maximum=6000) not in {wanted_page_size, None}:
                    continue
                if self._provider_float(row["min_row_count"], wanted_min_rows) != wanted_min_rows:
                    # ``min_row_count`` is integral but old repaired stores
                    # can expose it through a REAL affinity.
                    if self._strict_db_int(row["min_row_count"], minimum=1) != wanted_min_rows:
                        continue
                row_floors = tuple(float(row[key]) for key in ("min_overall_coverage", "min_market_coverage", "min_market_median_ratio"))
                if any(abs(left - right) > 1e-12 for left, right in zip(row_floors, floors)):
                    continue
                wanted_policy = str(bj_calendar_policy or evidence.get("bj_calendar_policy") or "").strip().lower()
                if wanted_policy and str(row["bj_calendar_policy"] or "").strip().lower() != wanted_policy and str(row["bj_calendar_policy"] or "").strip().lower() != "unknown":
                    continue
                if expected is not None:
                    try:
                        stored_expected = {self._canonical_raw_date(value) for value in json.loads(str(row["expected_dates_json"] or "[]"))}
                    except (TypeError, ValueError, json.JSONDecodeError):
                        stored_expected = set()
                    if stored_expected and stored_expected != expected:
                        continue
                return dict(row)
        return None

    find_staging_raw_batch = find_raw_batch
    resumable_raw_batch = find_raw_batch

    def resume_raw_batch(self, batch_id: str, *, expected_trade_dates=None, page_size: int | None = None) -> dict:
        """Reopen a failed batch after checking its stored request identity."""
        expected = None
        if expected_trade_dates is not None:
            expected = []
            for value in expected_trade_dates:
                normalized = self._canonical_raw_date(value)
                if not normalized:
                    raise ValueError("expected_trade_dates contains an invalid date")
                expected.append(normalized)
            expected = sorted(set(expected))
        with self._connect() as db:
            row = db.execute("SELECT * FROM batches WHERE batch_id=?", (str(batch_id),)).fetchone()
            if not row:
                raise KeyError(f"unknown raw batch {batch_id}")
            if str(row["status"] or "") not in {"staging", "failed"}:
                raise RuntimeError("raw batch is not restartable")
            if expected is not None:
                try:
                    stored = sorted(set(self._canonical_raw_date(value) for value in json.loads(str(row["expected_dates_json"] or "[]"))))
                except (TypeError, ValueError, json.JSONDecodeError):
                    stored = []
                if stored and stored != expected:
                    raise ValueError("raw batch expected dates do not match")
                db.execute("UPDATE batches SET expected_days=?,expected_dates_json=? WHERE batch_id=?", (len(expected), json.dumps(expected, separators=(",", ":")), str(batch_id)))
            if page_size is not None:
                parsed_size = max(1, min(int(page_size), 6000))
                db.execute("UPDATE batches SET page_size=? WHERE batch_id=?", (parsed_size, str(batch_id)))
            db.execute("UPDATE batches SET status='staging',error=NULL WHERE batch_id=?", (str(batch_id),))
            refreshed = db.execute("SELECT * FROM batches WHERE batch_id=?", (str(batch_id),)).fetchone()
            return dict(refreshed)

    reopen_raw_batch = resume_raw_batch

    def get_or_create_raw_batch(self, requested_date: str, **kwargs) -> dict:
        """Get a compatible staging batch or create one atomically."""
        batch_id = kwargs.pop("batch_id", None)
        found = self.find_raw_batch(requested_date, **kwargs)
        if found:
            if str(found.get("status") or "") == "failed":
                return self.resume_raw_batch(found["batch_id"], expected_trade_dates=kwargs.get("expected_trade_dates"), page_size=kwargs.get("page_size"))
            # Store the expected date manifest at the beginning of a new
            # process so the next process can distinguish a partial window.
            if kwargs.get("expected_trade_dates") is not None:
                return self.resume_raw_batch(found["batch_id"], expected_trade_dates=kwargs["expected_trade_dates"], page_size=kwargs.get("page_size"))
            return found
        self.create_raw_batch(requested_date, batch_id=batch_id, **kwargs)
        row = self.find_raw_batch(requested_date, **kwargs)
        if not row:
            raise RuntimeError("raw batch could not be created")
        return row

    begin_or_resume_raw_batch = get_or_create_raw_batch
    get_or_create_batch = get_or_create_raw_batch

    @staticmethod
    def _raw_page_filter_policy(value: str | None) -> str:
        policy = str(value or "include_all").strip().lower()
        aliases = {"all": "include_all", "include": "include_all", "include_bj": "include_bj", "exclude": "exclude_bj", "exclude_bj": "exclude_bj"}
        policy = aliases.get(policy, policy)
        if policy not in {"include_all", "include_bj", "exclude_bj"}:
            raise ValueError("raw page filter policy is invalid")
        return policy

    @classmethod
    def _raw_page_filtered_rows(cls, rows: list[dict], filter_policy: str) -> list[dict]:
        policy = cls._raw_page_filter_policy(filter_policy)
        if policy == "exclude_bj":
            return [row for row in rows if cls._raw_market(row.get("ts_code") or row.get("code")) != "BJ"]
        return list(rows)

    @classmethod
    def _raw_page_rows_equal(cls, left: list[dict], right: list[dict]) -> bool:
        def payload(rows):
            return [
                cls._raw_bar_payload(row)
                for row in sorted(rows, key=lambda item: (str(item.get("code") or ""), str(item.get("ts_code") or "")))
            ]

        return payload(left) == payload(right)

    @classmethod
    def _raw_page_metadata_values(
        cls,
        batch,
        trade_date: str,
        partition_no: int,
        server_rows,
        *,
        server_row_count: int | None = None,
        server_terminal: bool | None = None,
        server_page_digest: str = "",
        filter_policy: str = "include_all",
        source: str | None = None,
    ) -> dict:
        """Normalize one unfiltered server page and its pagination contract."""
        normalized_date = cls._canonical_raw_date(trade_date)
        if not normalized_date:
            raise ValueError("raw page trade_date must be canonical")
        try:
            page_no = cls._strict_db_int(partition_no, minimum=0)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("raw page partition_no must be a non-negative integer") from exc
        if page_no is None:
            raise ValueError("raw page partition_no must be a non-negative integer")
        policy = cls._raw_page_filter_policy(filter_policy)
        source_value = str(source or batch["provider"] or "").strip()[:80]
        raw_values = list(server_rows or [])
        normalized = [cls._normalize_raw_bar(row, normalized_date, source=source_value, basis="unadjusted") for row in raw_values]
        codes = [row["code"] for row in normalized]
        if len(codes) != len(set(codes)):
            raise ValueError("raw server page contains duplicate codes")
        computed_count = len(normalized)
        if server_row_count is None:
            raw_count = computed_count
        else:
            if isinstance(server_row_count, bool):
                raise ValueError("raw server row count is invalid")
            raw_count = cls._strict_db_int(server_row_count, minimum=0)
            if raw_count is None or raw_count != computed_count:
                raise ValueError("raw server row count does not match server page")
        try:
            page_size = cls._strict_db_int(batch["page_size"], minimum=1, maximum=6000) or 6000
        except (TypeError, ValueError, OverflowError):
            page_size = 6000
        expected_terminal = raw_count < page_size
        if server_terminal is None:
            terminal = expected_terminal
        elif isinstance(server_terminal, bool):
            terminal = server_terminal
        else:
            parsed_terminal = cls._strict_db_int(server_terminal, minimum=0, maximum=1)
            if parsed_terminal is None:
                raise ValueError("raw server page terminal flag is invalid")
            terminal = bool(parsed_terminal)
        if terminal != expected_terminal:
            raise ValueError("raw server page terminal flag does not match server row count")
        computed_digest = cls._raw_digest_for_rows(normalized)
        supplied_digest = str(server_page_digest or "").strip().lower()
        if supplied_digest and supplied_digest != computed_digest:
            raise ValueError("raw server page digest does not match server rows")
        if not supplied_digest:
            supplied_digest = computed_digest
        if not re.fullmatch(r"[0-9a-f]{64}", supplied_digest):
            raise ValueError("raw server page digest is invalid")
        return {
            "batch_id": str(batch["batch_id"]),
            "trade_date": normalized_date,
            "partition_no": page_no,
            "server_row_count": raw_count,
            "server_terminal": int(terminal),
            "server_page_digest": supplied_digest,
            "filter_policy": policy,
            "server_rows": normalized,
            "server_rows_json": json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        }

    @classmethod
    def _validate_raw_page_metadata_in_tx(cls, db, batch, metadata, filtered_rows: list[dict] | None = None) -> dict:
        """Validate persisted server-page metadata and filtered projection."""
        if not metadata:
            raise RuntimeError("raw page metadata is missing")
        try:
            parsed_rows = json.loads(str(metadata["server_rows_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("raw server page rows are invalid") from exc
        if not isinstance(parsed_rows, list):
            raise RuntimeError("raw server page rows are invalid")
        try:
            normalized = [cls._normalize_raw_bar(row, str(metadata["trade_date"]), source=str(batch["provider"]), basis="unadjusted") for row in parsed_rows]
            raw_count = cls._strict_db_int(metadata["server_row_count"], minimum=0)
            terminal = cls._strict_db_int(metadata["server_terminal"], minimum=0, maximum=1)
            partition_no = cls._strict_db_int(metadata["partition_no"], minimum=0)
        except (TypeError, ValueError, KeyError, OverflowError) as exc:
            raise RuntimeError("raw server page metadata is invalid") from exc
        if raw_count is None or terminal is None or partition_no is None or raw_count != len(normalized):
            raise RuntimeError("raw server page row count mismatch")
        page_codes = [str(row["code"]) for row in normalized]
        if len(page_codes) != len(set(page_codes)):
            raise RuntimeError("raw server page contains duplicate codes")
        batch_source = str(batch["provider"] or "").strip().casefold()
        if any(
            str(row.get("source") or "").strip().casefold() != batch_source
            or str(row.get("basis") or "").strip().lower() != "unadjusted"
            for row in normalized
        ):
            raise RuntimeError("raw server page source or basis mismatch")
        page_size = cls._strict_db_int(batch["page_size"], minimum=1, maximum=6000)
        if page_size is None or bool(terminal) != (raw_count < page_size):
            raise RuntimeError("raw server page terminal flag mismatch")
        digest = cls._raw_digest_for_rows(normalized)
        if digest != str(metadata["server_page_digest"] or "").strip().lower():
            raise RuntimeError("raw server page digest mismatch")
        policy = cls._raw_page_filter_policy(str(metadata["filter_policy"] or ""))
        expected_filtered = cls._raw_page_filtered_rows(normalized, policy)
        actual_filtered = list(filtered_rows or [])
        if not cls._raw_page_rows_equal(expected_filtered, actual_filtered):
            raise RuntimeError("raw server page filter projection mismatch")
        return {
            "trade_date": str(metadata["trade_date"]),
            "partition_no": partition_no,
            "server_row_count": raw_count,
            "server_terminal": bool(terminal),
            "server_page_digest": digest,
            "filter_policy": policy,
            "server_rows": normalized,
        }

    @classmethod
    def _stage_raw_page_metadata_in_tx(cls, db, batch, values: dict) -> None:
        existing = db.execute(
            "SELECT * FROM raw_page_metadata WHERE batch_id=? AND trade_date=? AND partition_no=?",
            (str(batch["batch_id"]), values["trade_date"], int(values["partition_no"])),
        ).fetchone()
        if existing:
            fields = ("server_row_count", "server_terminal", "server_page_digest", "filter_policy", "server_rows_json")
            if any(str(existing[field]) != str(values[field]) for field in fields):
                raise ValueError("raw server page metadata already staged with different content")
            return
        db.execute(
            "INSERT INTO raw_page_metadata(batch_id,trade_date,partition_no,server_row_count,server_terminal,server_page_digest,filter_policy,server_rows_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (str(batch["batch_id"]), values["trade_date"], int(values["partition_no"]), int(values["server_row_count"]), int(values["server_terminal"]), values["server_page_digest"], values["filter_policy"], values["server_rows_json"], datetime.utcnow().isoformat()),
        )

    def stage_raw_page_metadata(
        self,
        batch_id: str,
        trade_date: str,
        *,
        partition_no: int = 0,
        server_rows=None,
        server_row_count: int | None = None,
        server_terminal: bool | None = None,
        server_page_digest: str = "",
        filter_policy: str = "include_all",
        filtered_rows=None,
        source: str = "tushare",
    ) -> dict:
        """Persist an unfiltered page even when its filtered projection is empty."""
        with self._connect() as db:
            batch = db.execute(
                "SELECT b.*,d.provider,d.basis AS dataset_basis FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
                (str(batch_id),),
            ).fetchone()
            if not batch:
                raise KeyError(f"unknown raw batch {batch_id}")
            if str(batch["status"] or "") != "staging":
                raise RuntimeError("raw batch is no longer staging")
            values = self._raw_page_metadata_values(
                batch,
                trade_date,
                partition_no,
                server_rows,
                server_row_count=server_row_count,
                server_terminal=server_terminal,
                server_page_digest=server_page_digest,
                filter_policy=filter_policy,
                source=source,
            )
            self._stage_raw_page_metadata_in_tx(db, batch, values)
            metadata = db.execute(
                "SELECT * FROM raw_page_metadata WHERE batch_id=? AND trade_date=? AND partition_no=?",
                (str(batch_id), values["trade_date"], int(values["partition_no"])),
            ).fetchone()
            if filtered_rows is not None:
                self._validate_raw_page_metadata_in_tx(db, batch, metadata, list(filtered_rows or []))
            return dict(metadata)

    def reset_raw_batch_pages(self, batch_id: str, trade_date: str | None = None, *, from_page: int = 0) -> None:
        """Drop restartable page projections after a filtering-policy change."""
        try:
            first = max(0, int(from_page))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("raw page number is invalid") from exc
        normalized_date = self._canonical_raw_date(trade_date) if trade_date else None
        if trade_date and not normalized_date:
            raise ValueError("raw page trade_date must be canonical")
        with self._connect() as db:
            batch = db.execute("SELECT status FROM batches WHERE batch_id=?", (str(batch_id),)).fetchone()
            if not batch:
                raise KeyError(f"unknown raw batch {batch_id}")
            if str(batch["status"] or "") != "staging":
                raise RuntimeError("raw batch is no longer staging")
            where = "batch_id=? AND partition_no>=?"
            args: list[object] = [str(batch_id), first]
            if normalized_date:
                where += " AND trade_date=?"
                args.append(normalized_date)
            partition_ids = [str(row[0]) for row in db.execute(
                "SELECT partition_id FROM batch_days WHERE batch_id=? AND trade_date=? AND partition_id IN (SELECT partition_id FROM day_partitions WHERE partition_no>=?)" if normalized_date else "SELECT partition_id FROM batch_days WHERE batch_id=? AND partition_id IN (SELECT partition_id FROM day_partitions WHERE partition_no>=?)",
                ([str(batch_id), normalized_date, first] if normalized_date else [str(batch_id), first]),
            )]
            db.execute(f"DELETE FROM raw_page_metadata WHERE {where}", args)
            for partition_id in partition_ids:
                db.execute("DELETE FROM batch_days WHERE batch_id=? AND partition_id=?", (str(batch_id), partition_id))
                # A content-identical page can be reused by another staging
                # batch or by the active generation.  Only reclaim the
                # immutable partition after this batch is no longer a
                # reference; otherwise a policy reset in one batch would
                # destroy data still needed by another reader.
                referenced = db.execute(
                    "SELECT 1 FROM batch_days WHERE partition_id=? LIMIT 1",
                    (partition_id,),
                ).fetchone()
                if not referenced:
                    db.execute("DELETE FROM partition_bars WHERE partition_id=?", (partition_id,))
                    db.execute("DELETE FROM day_partitions WHERE partition_id=?", (partition_id,))
            total = db.execute("SELECT COALESCE(SUM(row_count),0) FROM batch_days WHERE batch_id=?", (str(batch_id),)).fetchone()
            db.execute("UPDATE batches SET row_count=? WHERE batch_id=?", (int(total[0] or 0), str(batch_id)))

    def raw_batch_partitions(self, batch_id: str, trade_date: str | None = None, *, include_rows: bool = True) -> list[dict]:
        """Read staged pages for restart reuse without exposing other batches."""
        normalized_date = self._canonical_raw_date(trade_date) if trade_date else None
        if trade_date and not normalized_date:
            raise ValueError("raw partition trade_date must be canonical")
        with self._connect() as db:
            batch = db.execute("SELECT b.*,d.provider,d.basis AS dataset_basis FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?", (str(batch_id),)).fetchone()
            if not batch:
                raise KeyError(f"unknown raw batch {batch_id}")
            if str(batch["status"] or "") not in {"staging", "failed"}:
                raise RuntimeError("raw batch is not staging")
            sql = (
                "SELECT bd.trade_date AS batch_trade_date,bd.row_count AS batch_row_count,dp.* "
                "FROM batch_days bd JOIN day_partitions dp ON dp.partition_id=bd.partition_id WHERE bd.batch_id=?"
            )
            args: list[object] = [str(batch_id)]
            if normalized_date:
                sql += " AND bd.trade_date=?"
                args.append(normalized_date)
            sql += " ORDER BY bd.trade_date,dp.partition_no,dp.partition_id"
            mappings = db.execute(sql, args).fetchall()
            metadata_sql = "SELECT * FROM raw_page_metadata WHERE batch_id=?"
            metadata_args: list[object] = [str(batch_id)]
            if normalized_date:
                metadata_sql += " AND trade_date=?"
                metadata_args.append(normalized_date)
            metadata_sql += " ORDER BY trade_date,partition_no"
            metadata_rows = db.execute(metadata_sql, metadata_args).fetchall()
            by_key = {(str(row["trade_date"]), int(row["partition_no"])): row for row in metadata_rows}
            mapping_by_key = {(str(row["trade_date"]), int(row["partition_no"])): row for row in mappings}
            result: list[dict] = []
            for key in sorted(set(mapping_by_key) | set(by_key)):
                mapping = mapping_by_key.get(key)
                metadata = by_key.get(key)
                partition_id = str(mapping["partition_id"]) if mapping else ""
                rows = [dict(row) for row in db.execute(
                    "SELECT trade_date,code,ts_code,name,open,high,low,close,pre_close,pct_change,volume,amount,source,basis FROM partition_bars WHERE partition_id=? ORDER BY code,ts_code",
                    (partition_id,),
                )] if partition_id else []
                # Staging reuse must not re-normalise/re-hash every stored bar on
                # every resume attempt (the same ~660k-bar cost that previously
                # blocked the read path).  Bars were already canonicalised and
                # digested when staged, so trust the append-only partition
                # metadata and only verify it is well-formed.
                if mapping:
                    count = self._strict_db_int(mapping["row_count"], minimum=0)
                    content_hash = str(mapping["content_hash"] or "")
                    if count is None or not re.fullmatch(r"[0-9a-f]{64}", content_hash):
                        raise RuntimeError("staged raw partition integrity check failed")
                    try:
                        market_counts = json.loads(str(mapping["market_counts_json"] or "{}"))
                    except (TypeError, ValueError, json.JSONDecodeError) as exc:
                        raise RuntimeError("staged raw partition market counts are invalid") from exc
                    if not isinstance(market_counts, dict) or any(
                        key not in {"SH", "SZ", "BJ"} or isinstance(value, bool) or not isinstance(value, int) or value <= 0
                        for key, value in market_counts.items()
                    ):
                        raise RuntimeError("staged raw partition market counts mismatch")
                else:
                    count = len(rows)
                    content_hash = self._raw_digest_for_rows(rows) if rows else ""
                    market_counts = {}
                    for row in rows:
                        market = self._raw_market(row.get("ts_code") or row.get("code"))
                        market_counts[market] = market_counts.get(market, 0) + 1
                if metadata is None:
                    # Rows written by an older v0.13 draft have no server-page
                    # record.  They remain readable as an unfiltered page, but
                    # new provider resumes will reject them when the active
                    # filtering policy differs.
                    metadata = self._raw_page_metadata_values(
                        batch,
                        str(key[0]),
                        int(key[1]),
                        rows,
                        source=str(batch["provider"]),
                    )
                    metadata = dict(metadata)
                server_row_count = self._strict_db_int(metadata["server_row_count"], minimum=0)
                server_terminal = self._strict_db_int(metadata["server_terminal"], minimum=0, maximum=1)
                if server_row_count is None or server_terminal is None:
                    raise RuntimeError("staged raw page metadata is invalid")
                page = {
                    "server_row_count": server_row_count,
                    "server_terminal": bool(server_terminal),
                    "server_page_digest": str(metadata["server_page_digest"] or "").strip().lower(),
                    "filter_policy": str(metadata["filter_policy"] or ""),
                }
                item = {
                    "trade_date": str(key[0]),
                    "partition_no": int(key[1]),
                    "partition_id": partition_id or None,
                    "row_count": count,
                    "content_hash": content_hash,
                    "market_counts": market_counts,
                    "server_row_count": int(page["server_row_count"]),
                    "server_terminal": bool(page["server_terminal"]),
                    "server_page_digest": str(page["server_page_digest"]),
                    "filter_policy": str(page["filter_policy"]),
                }
                if include_rows:
                    item["rows"] = rows
                result.append(item)
            return result

    staged_raw_partitions = raw_batch_partitions
    raw_staging_partitions = raw_batch_partitions
    list_raw_partitions = raw_batch_partitions

    def reuse_partition_for_batch(self, batch_id: str, trade_date: str, *, partition_no: int = 0) -> dict | None:
        """Reuse an already-staged partition from a prior generation.

        The true-incremental path: a new daily window reuses the overlapping
        sessions already on disk and only fetches the one new session.  Returns
        the same item shape as ``raw_batch_partitions`` entries, or ``None`` when
        no reusable partition exists for this dataset + trade date.
        """
        normalized_date = self._canonical_raw_date(trade_date)
        if not normalized_date:
            raise ValueError("raw partition trade_date must be canonical")
        partition_no = self._strict_db_int(partition_no, minimum=0)
        if partition_no is None:
            raise ValueError("raw partition_no must be a non-negative integer")
        with self._connect() as db:
            batch = db.execute(
                "SELECT b.*,d.dataset_key,d.provider,d.frequency,d.universe FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
                (str(batch_id),),
            ).fetchone()
            if not batch:
                raise KeyError(f"unknown raw batch {batch_id}")
            if str(batch["status"]) != "staging":
                raise RuntimeError("raw batch is no longer staging")
            existing = db.execute(
                "SELECT dp.partition_id,dp.content_hash,dp.row_count,dp.market_counts_json,dp.basis,dp.source "
                "FROM day_partitions dp "
                "WHERE dp.dataset_id=? AND dp.trade_date=? AND dp.partition_no=? AND dp.validation_status='validated' "
                "  AND NOT EXISTS (SELECT 1 FROM batch_days bd WHERE bd.batch_id=? AND bd.partition_id=dp.partition_id) "
                "ORDER BY dp.created_at DESC LIMIT 1",
                (batch["dataset_id"], normalized_date, partition_no, str(batch_id)),
            ).fetchone()
            if not existing:
                return None
            if str(existing["basis"] or "").lower() != "unadjusted" or str(existing["source"] or "").strip().casefold() != str(batch["provider"] or "").strip().casefold():
                return None
            partition_id = str(existing["partition_id"])
            source_meta = db.execute(
                "SELECT rpm.server_row_count,rpm.server_terminal,rpm.server_page_digest,rpm.filter_policy,rpm.server_rows_json "
                "FROM raw_page_metadata rpm JOIN batch_days bd ON bd.batch_id=rpm.batch_id AND bd.partition_id=? "
                "WHERE rpm.trade_date=? AND rpm.partition_no=? ORDER BY rpm.created_at DESC LIMIT 1",
                (partition_id, normalized_date, partition_no),
            ).fetchone()
            if not source_meta:
                return None
            rows = [dict(row) for row in db.execute(
                "SELECT trade_date,code,ts_code,name,open,high,low,close,pre_close,pct_change,volume,amount,source,basis FROM partition_bars WHERE partition_id=? ORDER BY code,ts_code",
                (partition_id,),
            )]
            if not rows:
                return None
            try:
                market_counts = json.loads(str(existing["market_counts_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                return None
            if not isinstance(market_counts, dict):
                return None
            db.execute(
                "INSERT OR IGNORE INTO batch_days(batch_id,trade_date,partition_id,row_count) VALUES(?,?,?,?)",
                (str(batch_id), normalized_date, partition_id, int(existing["row_count"] or len(rows))),
            )
            db.execute(
                "INSERT OR IGNORE INTO raw_page_metadata(batch_id,trade_date,partition_no,server_row_count,server_terminal,server_page_digest,filter_policy,server_rows_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    str(batch_id), normalized_date, partition_no,
                    int(source_meta["server_row_count"] or 0), int(source_meta["server_terminal"] or 0),
                    str(source_meta["server_page_digest"] or ""), str(source_meta["filter_policy"] or "include_all"),
                    str(source_meta["server_rows_json"] or "[]"), datetime.utcnow().isoformat(),
                ),
            )
            totals = db.execute("SELECT COALESCE(SUM(row_count),0) FROM batch_days WHERE batch_id=?", (str(batch_id),)).fetchone()
            db.execute("UPDATE batches SET row_count=? WHERE batch_id=?", (int(totals[0]), str(batch_id)))
            return {
                "trade_date": normalized_date,
                "partition_no": partition_no,
                "partition_id": partition_id,
                "row_count": int(existing["row_count"] or len(rows)),
                "content_hash": str(existing["content_hash"] or ""),
                "market_counts": market_counts,
                "server_row_count": int(source_meta["server_row_count"] or 0),
                "server_terminal": bool(int(source_meta["server_terminal"] or 0)),
                "server_page_digest": str(source_meta["server_page_digest"] or ""),
                "filter_policy": str(source_meta["filter_policy"] or "include_all"),
                "rows": rows,
            }

    def raw_batch_staged_rows(self, batch_id: str, trade_date: str | None = None) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        for partition in self.raw_batch_partitions(batch_id, trade_date, include_rows=True):
            for row in partition.get("rows", []):
                result.setdefault(str(partition["trade_date"]), []).append(row)
        for rows in result.values():
            rows.sort(key=lambda item: (str(item.get("code") or ""), str(item.get("ts_code") or "")))
        return result

    staged_raw_rows = raw_batch_staged_rows

    begin_raw_batch = create_raw_batch
    create_batch = create_raw_batch

    def fail_raw_batch(self, batch_id: str, error: str = "") -> None:
        """Mark an incomplete staging batch unusable without touching active data."""
        with self._connect() as db:
            db.execute(
                "UPDATE batches SET status='failed',error=? WHERE batch_id=? AND status='staging'",
                (str(error or "raw batch failed")[:500], str(batch_id)),
            )

    abort_raw_batch = fail_raw_batch
    fail_batch = fail_raw_batch

    def stage_raw_partition(
        self,
        batch_id: str,
        trade_date: str,
        rows,
        *,
        partition_no: int = 0,
        source: str = "tushare",
        basis: str = "unadjusted",
        server_rows=None,
        server_row_count: int | None = None,
        server_terminal: bool | None = None,
        server_page_digest: str = "",
        filter_policy: str = "include_all",
    ) -> str:
        """Validate, append, or reuse one filtered page partition."""
        normalized_date = self._canonical_raw_date(trade_date)
        if not normalized_date:
            raise ValueError("raw partition trade_date must be canonical")
        partition_no = self._strict_db_int(partition_no, minimum=0)
        if partition_no is None:
            raise ValueError("raw partition_no must be a non-negative integer")
        normalized = [self._normalize_raw_bar(row, normalized_date, source=source, basis=basis) for row in (rows or [])]
        if not normalized:
            raise ValueError("empty raw partitions are not staged")
        codes = [row["code"] for row in normalized]
        if len(codes) != len(set(codes)):
            raise ValueError("raw partition contains duplicate codes")
        content_hash = self._raw_digest_for_rows(normalized)
        market_counts: dict[str, int] = {}
        for row in normalized:
            market = self._raw_market(row["ts_code"])
            if not market:
                raise ValueError("raw partition row has an invalid market")
            market_counts[market] = market_counts.get(market, 0) + 1
        with self._connect() as db:
            batch = db.execute(
                "SELECT b.*,d.dataset_key,d.provider,d.frequency,d.universe FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
                (str(batch_id),),
            ).fetchone()
            if not batch:
                raise KeyError(f"unknown raw batch {batch_id}")
            if str(batch["status"]) != "staging":
                raise RuntimeError("raw batch is no longer staging")
            if str(batch["basis"]).lower() != "unadjusted":
                raise RuntimeError("raw batch basis is not unadjusted")
            source_value = str(source or batch["provider"]).strip()[:80]
            if str(batch["provider"]).strip().casefold() != source_value.casefold():
                raise ValueError("raw partition source does not match batch")
            if any(str(row.get("source") or "").strip().casefold() != source_value.casefold() for row in normalized):
                raise ValueError("raw partition rows have mixed source")
            page_values = self._raw_page_metadata_values(
                batch,
                normalized_date,
                partition_no,
                normalized if server_rows is None else server_rows,
                server_row_count=server_row_count,
                server_terminal=server_terminal,
                server_page_digest=server_page_digest,
                filter_policy=filter_policy,
                source=source_value,
            )
            if not self._raw_page_rows_equal(
                self._raw_page_filtered_rows(page_values["server_rows"], page_values["filter_policy"]),
                normalized,
            ):
                raise ValueError("raw partition does not match filtered server page")
            existing_mapping = db.execute(
                "SELECT bd.partition_id,dp.content_hash FROM batch_days bd JOIN day_partitions dp ON dp.partition_id=bd.partition_id "
                "WHERE bd.batch_id=? AND bd.trade_date=? AND dp.partition_no=?",
                (str(batch_id), normalized_date, partition_no),
            ).fetchone()
            existing = db.execute(
                "SELECT partition_id FROM day_partitions WHERE dataset_id=? AND trade_date=? AND partition_no=? AND content_hash=?",
                (batch["dataset_id"], normalized_date, partition_no, content_hash),
            ).fetchone()
            if existing_mapping and str(existing_mapping[1]) != content_hash:
                raise ValueError("raw partition_no already staged with different content")
            existing_codes = {
                str(row[0]) for row in db.execute(
                    "SELECT pb.code FROM batch_days bd JOIN day_partitions dp ON dp.partition_id=bd.partition_id "
                    "JOIN partition_bars pb ON pb.partition_id=dp.partition_id WHERE bd.batch_id=? AND bd.trade_date=?",
                    (str(batch_id), normalized_date),
                )
            }
            if existing_codes.intersection(codes) and not existing_mapping:
                raise ValueError("raw batch contains duplicate code partitions")
            if existing_mapping and not existing:
                partition_id = str(existing_mapping[0])
            elif existing:
                partition_id = str(existing[0])
            else:
                partition_id = "partition-" + uuid.uuid4().hex
                db.execute(
                    "INSERT INTO day_partitions(partition_id,dataset_id,trade_date,partition_no,content_hash,source,basis,row_count,market_counts_json,digest_version,validation_status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        partition_id, batch["dataset_id"], normalized_date, partition_no, content_hash,
                        source_value, "unadjusted", len(normalized),
                        json.dumps(market_counts, sort_keys=True, separators=(",", ":")), 1,
                        "validated", datetime.utcnow().isoformat(),
                    ),
                )
                db.executemany(
                    "INSERT INTO partition_bars(partition_id,trade_date,code,ts_code,name,open,high,low,close,pre_close,pct_change,volume,amount,source,basis) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(
                        partition_id, item["trade_date"], item["code"], item["ts_code"], item["name"], item["open"], item["high"], item["low"], item["close"], item["pre_close"], item["pct_change"], item["volume"], item["amount"], item["source"], item["basis"],
                    ) for item in normalized],
                )
            self._stage_raw_page_metadata_in_tx(db, batch, page_values)
            metadata = db.execute(
                "SELECT * FROM raw_page_metadata WHERE batch_id=? AND trade_date=? AND partition_no=?",
                (str(batch_id), normalized_date, partition_no),
            ).fetchone()
            self._validate_raw_page_metadata_in_tx(db, batch, metadata, normalized)
            db.execute(
                "INSERT OR IGNORE INTO batch_days(batch_id,trade_date,partition_id,row_count) VALUES(?,?,?,?)",
                (str(batch_id), normalized_date, partition_id, len(normalized)),
            )
            totals = db.execute(
                "SELECT COALESCE(SUM(row_count),0) FROM batch_days WHERE batch_id=?",
                (str(batch_id),),
            ).fetchone()
            db.execute("UPDATE batches SET row_count=? WHERE batch_id=?", (int(totals[0]), str(batch_id)))
        return partition_id

    stage_or_reuse_partition = stage_raw_partition
    stage_partition = stage_raw_partition

    def _validate_raw_partitions_in_tx(self, db, batch, fast: bool = False) -> dict:
        """Re-read every staged row and build the only trusted batch summary.

        ``fast=True`` skips the per-bar re-normalisation and page-metadata
        recomputation, trusting the publish-time validation of an immutable
        batch and verifying only the append-only partition digest/row-count/
        market-count contract.  This turns a multi-minute event-loop block
        (hundreds of thousands of float conversions) into a short digest pass.
        """
        mappings = db.execute(
            "SELECT bd.trade_date AS batch_trade_date,bd.partition_id AS batch_partition_id,bd.row_count AS batch_row_count,"
            "dp.partition_id,dp.dataset_id,dp.trade_date,dp.partition_no,dp.content_hash,dp.source,dp.basis,dp.row_count,"
            "dp.market_counts_json,dp.digest_version,dp.validation_status "
            "FROM batch_days bd LEFT JOIN day_partitions dp ON dp.partition_id=bd.partition_id "
            "WHERE bd.batch_id=? ORDER BY bd.trade_date,dp.partition_no,dp.partition_id",
            (str(batch["batch_id"]),),
        ).fetchall()
        if not mappings:
            raise RuntimeError("raw batch contains no partitions")
        page_metadata = {
            (str(row["trade_date"]), int(row["partition_no"])): row
            for row in db.execute(
                "SELECT * FROM raw_page_metadata WHERE batch_id=? ORDER BY trade_date,partition_no",
                (str(batch["batch_id"]),),
            ).fetchall()
        }
        trusted: list[dict] = []
        trusted_pages: list[dict] = []
        daily_counts: dict[str, dict[str, int]] = {}
        seen_codes: set[tuple[str, str]] = set()
        seen_partition_numbers: set[tuple[str, int]] = set()
        total_rows = 0
        for mapping in mappings:
            if not mapping["partition_id"] or str(mapping["batch_partition_id"]) != str(mapping["partition_id"]):
                raise RuntimeError("raw batch references a missing partition")
            partition_date = self._canonical_raw_date(mapping["trade_date"])
            batch_date = self._canonical_raw_date(mapping["batch_trade_date"])
            if not partition_date or partition_date != str(mapping["trade_date"]) or batch_date != partition_date:
                raise RuntimeError("raw batch contains a non-canonical or mismatched partition date")
            if str(mapping["dataset_id"]) != str(batch["dataset_id"]):
                raise RuntimeError("raw batch references another dataset")
            partition_no = self._strict_db_int(mapping["partition_no"], minimum=0)
            partition_row_count = self._strict_db_int(mapping["row_count"], minimum=1)
            batch_row_count = self._strict_db_int(mapping["batch_row_count"], minimum=1)
            digest_version = self._strict_db_int(mapping["digest_version"], minimum=1)
            if partition_no is None or partition_row_count is None or batch_row_count is None:
                raise RuntimeError("raw partition has an invalid integer contract")
            if digest_version != 1:
                raise RuntimeError("raw partition has an invalid partition number")
            partition_key = (partition_date, partition_no)
            if partition_key in seen_partition_numbers:
                raise RuntimeError("raw batch contains duplicate partition numbers")
            seen_partition_numbers.add(partition_key)
            if str(mapping["validation_status"] or "") != "validated" or str(mapping["basis"] or "").lower() != "unadjusted":
                raise RuntimeError("raw batch contains an unvalidated or adjusted partition")
            batch_source = str(batch["source"] or batch["provider"] or "").strip().casefold()
            partition_source = str(mapping["source"] or "").strip().casefold()
            if not batch_source or partition_source != batch_source:
                raise RuntimeError("raw batch contains a mixed source")
            if fast:
                # A published batch is immutable and was already re-hashed and
                # re-normalised bar-by-bar at publish time.  Re-hashing ~665k
                # bars on every process start (the read path runs before the
                # in-memory cache is warm) blocks the event loop for ~90s, so
                # trust the append-only partition metadata instead and only
                # verify that it is well-formed and self-consistent.
                content_hash = str(mapping["content_hash"] or "")
                if not re.fullmatch(r"[0-9a-f]{64}", content_hash):
                    raise RuntimeError("raw partition content hash is invalid")
                if partition_row_count != batch_row_count:
                    raise RuntimeError("raw partition row count mismatch")
                try:
                    market_counts = json.loads(str(mapping["market_counts_json"] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError("raw partition market counts are invalid") from exc
                if (
                    not isinstance(market_counts, dict)
                    or any(key not in {"SH", "SZ", "BJ"} for key in market_counts)
                    or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in market_counts.values())
                ):
                    raise RuntimeError("raw partition market counts mismatch")
                page_meta = page_metadata.get(partition_key)
                if page_meta is None:
                    raise RuntimeError("raw page metadata is missing")
                server_row_count = self._strict_db_int(page_meta["server_row_count"], minimum=0)
                server_terminal = self._strict_db_int(page_meta["server_terminal"], minimum=0, maximum=1)
                if server_row_count is None or server_terminal is None:
                    raise RuntimeError("raw server page metadata is invalid")
                server_page_digest = str(page_meta["server_page_digest"] or "").strip().lower()
                if not re.fullmatch(r"[0-9a-f]{64}", server_page_digest):
                    raise RuntimeError("raw server page digest is invalid")
                page = {
                    "trade_date": partition_date,
                    "partition_no": partition_no,
                    "server_row_count": server_row_count,
                    "server_terminal": bool(server_terminal),
                    "server_page_digest": server_page_digest,
                    "filter_policy": self._raw_page_filter_policy(str(page_meta["filter_policy"] or "")),
                }
                date_counts = daily_counts.setdefault(partition_date, {"total": 0})
                date_counts["total"] += partition_row_count
                for market, value in market_counts.items():
                    date_counts[market] = date_counts.get(market, 0) + value
                trusted.append({
                    "trade_date": partition_date,
                    "partition_no": partition_no,
                    "partition_id": str(mapping["partition_id"]),
                    "content_hash": content_hash,
                    "row_count": partition_row_count,
                    "market_counts": market_counts,
                })
                trusted_pages.append({
                    "trade_date": partition_date,
                    "partition_no": partition_no,
                    "server_row_count": int(page["server_row_count"]),
                    "server_terminal": bool(page["server_terminal"]),
                    "server_page_digest": str(page["server_page_digest"]),
                    "filter_policy": str(page["filter_policy"]),
                })
                total_rows += partition_row_count
                continue
            # Publication re-validation verifies the append-only digest (tamper
            # detection) without re-normalising every bar or re-parsing the
            # server-page metadata.  Both were already validated when the
            # partition was staged, so re-doing them here only re-serialises
            # ~665k bars and blocks the event loop for minutes.
            digest, count, market_counts = self._raw_partition_db_digest(db, str(mapping["partition_id"]))
            if digest != str(mapping["content_hash"] or "") or count != partition_row_count or count != batch_row_count:
                raise RuntimeError("raw partition digest or row count mismatch")
            if not self._strict_market_counts_json(mapping["market_counts_json"], market_counts):
                raise RuntimeError("raw partition market counts mismatch")
            metadata = page_metadata.get(partition_key)
            if metadata is None:
                raise RuntimeError("raw page metadata is missing")
            server_row_count = self._strict_db_int(metadata["server_row_count"], minimum=0)
            server_terminal = self._strict_db_int(metadata["server_terminal"], minimum=0, maximum=1)
            if server_row_count is None or server_terminal is None:
                raise RuntimeError("raw server page metadata is invalid")
            server_page_digest = str(metadata["server_page_digest"] or "").strip().lower()
            if not re.fullmatch(r"[0-9a-f]{64}", server_page_digest):
                raise RuntimeError("raw server page digest is invalid")
            page = {
                "trade_date": partition_date,
                "partition_no": partition_no,
                "server_row_count": server_row_count,
                "server_terminal": bool(server_terminal),
                "server_page_digest": server_page_digest,
                "filter_policy": self._raw_page_filter_policy(str(metadata["filter_policy"] or "")),
            }
            date_counts = daily_counts.setdefault(partition_date, {"total": 0})
            date_counts["total"] += count
            for market, value in market_counts.items():
                date_counts[market] = date_counts.get(market, 0) + value
            trusted.append({
                "trade_date": partition_date,
                "partition_no": partition_no,
                "partition_id": str(mapping["partition_id"]),
                "content_hash": digest,
                "row_count": count,
                "market_counts": market_counts,
            })
            trusted_pages.append({
                "trade_date": partition_date,
                "partition_no": partition_no,
                "server_row_count": int(page["server_row_count"]),
                "server_terminal": bool(page["server_terminal"]),
                "server_page_digest": str(page["server_page_digest"]),
                "filter_policy": str(page["filter_policy"]),
            })
            total_rows += count
        # A full server page may have no stored rows after policy filtering.
        # Keep that page in the trusted manifest so a later tamper or policy
        # change cannot make pagination silently terminate early.
        mapped_keys = set(seen_partition_numbers)
        for partition_key, metadata in page_metadata.items():
            if partition_key in mapped_keys:
                continue
            partition_date, partition_no = partition_key
            page = self._validate_raw_page_metadata_in_tx(db, batch, metadata, [])
            daily_counts.setdefault(partition_date, {"total": 0})
            trusted_pages.append({
                "trade_date": partition_date,
                "partition_no": int(partition_no),
                "server_row_count": int(page["server_row_count"]),
                "server_terminal": bool(page["server_terminal"]),
                "server_page_digest": str(page["server_page_digest"]),
                "filter_policy": str(page["filter_policy"]),
            })
        batch_row_count = self._strict_db_int(batch["row_count"], minimum=0)
        if batch_row_count is None or batch_row_count != total_rows:
            raise RuntimeError("raw batch row count mismatch")
        expected_days = self._strict_db_int(batch["expected_days"], minimum=0)
        min_row_count = self._strict_db_int(batch["min_row_count"], minimum=1)
        require_universe_evidence = self._strict_db_int(batch["require_universe_evidence"], minimum=0, maximum=1)
        if expected_days is None or min_row_count is None or require_universe_evidence is None:
            raise RuntimeError("raw batch has an invalid integer contract")
        dates = set(daily_counts)
        manifest_payload = {
            "manifest_version": 1,
            "batch_id": str(batch["batch_id"]),
            "dataset_id": str(batch["dataset_id"]),
            "requested_date": self._canonical_raw_date(batch["requested_date"]) or "",
            "source": str(batch["source"] or "").strip(),
            "basis": str(batch["basis"] or "").strip().lower(),
            "expected_days": expected_days,
            "min_row_count": min_row_count,
            "min_overall_coverage": float(batch["min_overall_coverage"]),
            "min_market_coverage": float(batch["min_market_coverage"]),
            "min_market_median_ratio": float(batch["min_market_median_ratio"]),
            "page_size": self._strict_db_int(batch["page_size"], minimum=1, maximum=6000) or 6000,
            "universe_version": str(batch["universe_version"] or ""),
            "universe_counts": self._normalize_universe_counts(str(batch["universe_counts_json"] or "{}")),
            "require_universe_evidence": bool(require_universe_evidence),
            "bj_calendar_policy": str(batch["bj_calendar_policy"] or "unknown").strip().lower(),
            "partitions": sorted(trusted, key=lambda item: (item["trade_date"], item["partition_no"], item["partition_id"])),
        }
        if page_metadata:
            manifest_payload["page_metadata"] = sorted(trusted_pages, key=lambda item: (item["trade_date"], item["partition_no"]))
        if batch["actual_trade_date"]:
            manifest_payload["actual_trade_date"] = self._canonical_raw_date(batch["actual_trade_date"]) or ""
        if batch["expected_dates_json"]:
            try:
                expected_dates = json.loads(str(batch["expected_dates_json"]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("raw batch expected dates are invalid") from exc
            if not isinstance(expected_dates, list):
                raise RuntimeError("raw batch expected dates are invalid")
            manifest_payload["expected_dates"] = sorted(str(value) for value in expected_dates)
        manifest_text = json.dumps(manifest_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        manifest_hash = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
        coverage = self._raw_coverage_metrics(
            daily_counts,
            min_row_count=int(batch["min_row_count"] or 1),
            min_overall_coverage=float(batch["min_overall_coverage"]),
            min_market_coverage=float(batch["min_market_coverage"]),
            min_market_median_ratio=float(batch["min_market_median_ratio"]),
            universe_version=str(batch["universe_version"] or ""),
            universe_counts=str(batch["universe_counts_json"] or "{}"),
            require_universe_evidence=True,
            requested_date=str(batch["requested_date"] or ""),
            actual_trade_date=str(batch["actual_trade_date"] or max(dates)),
            bj_calendar_policy=str(batch["bj_calendar_policy"] or ""),
        )
        return {
            "mappings": trusted,
            "dates": dates,
            "daily_counts": daily_counts,
            "row_count": total_rows,
            "manifest_payload": manifest_payload,
            "manifest_hash": manifest_hash,
            "coverage": coverage,
        }

    def _validate_raw_batch_dates_in_tx(self, db, batch, dates: set[str], actual: str, expected: set[str] | None = None) -> None:
        requested_text = str(batch["requested_date"] or "")
        requested = self._canonical_raw_date(requested_text)
        if not requested or requested != requested_text:
            raise ValueError("raw batch requested_date must be canonical")
        actual_text = str(actual or "")
        actual = self._canonical_raw_date(actual_text)
        if not actual or actual != actual_text:
            raise ValueError("raw batch actual_trade_date must be canonical")
        if not requested or not actual or actual > requested:
            raise ValueError("raw batch actual_trade_date is outside requested date")
        # A request may legitimately be made on a weekend or before today's
        # close; only the represented market date must be a completed session.
        # Future requests remain invalid and cannot be used to smuggle a
        # current/provisional bar into a published generation.
        requested_value = datetime.strptime(requested, "%Y-%m-%d").date()
        china_now = datetime.now(timezone(timedelta(hours=8)))
        if requested_value > china_now.date():
            raise ValueError("raw batch requested date is in the future")
        if not self._raw_completed_date(db, actual):
            raise ValueError("raw batch actual date is not a completed trading date")
        if not dates or actual not in dates or max(dates) > actual:
            raise ValueError("raw batch window extends beyond actual_trade_date")
        for value in dates:
            if self._canonical_raw_date(value) != value:
                raise ValueError("raw batch contains a non-canonical trading date")
            calendar_row = db.execute("SELECT status,is_open FROM trading_calendar WHERE trade_date=?", (value,)).fetchone()
            if calendar_row:
                if not self._raw_completed_date(db, value):
                    raise ValueError("raw batch contains an invalid or incomplete trading date")
                continue
            try:
                parsed = datetime.strptime(value, "%Y-%m-%d")
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("raw batch contains an invalid trading date") from exc
            china_now = datetime.now(timezone(timedelta(hours=8)))
            if parsed.date() > china_now.date() or (parsed.date() == china_now.date() and china_now.hour < 15):
                raise ValueError("raw batch contains a future or incomplete trading date")
        expected_days = int(batch["expected_days"] or 0)
        if expected_days and len(dates) != expected_days:
            raise RuntimeError("raw batch is missing or has extra expected trade dates")
        if expected is not None and dates != expected:
            raise RuntimeError("raw batch trade dates do not match expected window")
        start_date, end_date = min(dates), max(dates)
        if batch["start_date"] is not None and str(batch["start_date"]) != start_date:
            raise RuntimeError("raw batch start date mismatch")
        if batch["end_date"] is not None and str(batch["end_date"]) != end_date:
            raise RuntimeError("raw batch end date mismatch")

    def _validate_published_raw_batch_cached(self, db, batch) -> dict:
        """Validate a published raw batch once per process, then reuse.

        The first read re-hashes the stored bars so out-of-band SQLite changes
        remain detectable.  Callers keep this work off the event loop, and the
        result is cached by (batch_id, generation) for subsequent reads.
        """
        batch_id = str(batch["batch_id"] or "")
        generation = str(batch["generation"] or "")
        key = batch_id + ":" + generation
        cached = self._raw_batch_validation_cache.get(key)
        if cached is not None:
            return cached
        result = self._validate_published_raw_batch_in_tx(db, batch)
        self._raw_batch_validation_cache[key] = result
        return result

    def _validate_published_raw_batch_in_tx(self, db, batch, fast: bool = False) -> dict:
        if str(batch["status"] or "") != "published":
            raise RuntimeError("raw batch is not published")
        if str(batch["basis"] or "").lower() != "unadjusted" or str(batch["dataset_basis"] or "").lower() != "unadjusted":
            raise RuntimeError("raw batch basis is not unadjusted")
        if str(batch["source"] or "").strip().casefold() != str(batch["provider"] or "").strip().casefold():
            raise RuntimeError("raw batch source does not match provider")
        try:
            detail = self._validate_raw_partitions_in_tx(db, batch, fast=fast)
            actual_text = str(batch["actual_trade_date"] or "")
            actual = self._canonical_raw_date(actual_text)
            if not actual or actual != actual_text:
                raise RuntimeError("raw batch actual_trade_date must be canonical")
            self._validate_raw_batch_dates_in_tx(db, batch, detail["dates"], actual_text, expected=None)
            try:
                expected = json.loads(str(batch["expected_dates_json"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("raw batch expected dates are invalid") from exc
            if (
                not isinstance(expected, list)
                or any(self._canonical_raw_date(value) != str(value) for value in expected)
                or {self._canonical_raw_date(value) for value in expected} != detail["dates"]
            ):
                raise RuntimeError("raw batch expected dates mismatch")
            if int(batch["manifest_version"] or 0) != 1 or str(batch["manifest_hash"] or "") != detail["manifest_hash"]:
                raise RuntimeError("raw batch manifest mismatch")
            try:
                stored_coverage = json.loads(str(batch["coverage_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("raw batch coverage is invalid") from exc
            if not isinstance(stored_coverage, dict) or stored_coverage.get("coverage_ok") is not True:
                raise RuntimeError("raw batch coverage is not complete")
            if json.dumps(stored_coverage, sort_keys=True, separators=(",", ":"), allow_nan=False) != json.dumps(detail["coverage"], sort_keys=True, separators=(",", ":"), allow_nan=False):
                raise RuntimeError("raw batch coverage manifest mismatch")
            return detail
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError("raw batch integrity validation failed") from exc

    def raw_partition(self, partition_id: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM day_partitions WHERE partition_id=?", (str(partition_id),)).fetchone()
            return dict(row) if row else None

    def _cleanup_raw_batches_in_tx(self, db, dataset_id: str) -> list[str]:
        now = datetime.utcnow().isoformat()
        # Closed readers no longer protect their batch.  Remove those
        # provenance rows before deleting old batches so the RESTRICT foreign
        # key does not turn routine retention into a failed publication.
        db.execute("DELETE FROM read_provenance WHERE pinned=0 OR expires_at<=?", (now,))
        active = db.execute("SELECT active_batch_id,previous_batch_id FROM active_generations WHERE dataset_id=?", (dataset_id,)).fetchone()
        keep: set[str] = set()
        if active:
            keep.update(str(value) for value in active if value)
        for row in db.execute(
            "SELECT batch_id FROM batches WHERE dataset_id=? AND status='published' ORDER BY generation DESC, published_at DESC LIMIT 5",
            (dataset_id,),
        ):
            keep.add(str(row[0]))
        for row in db.execute(
            "SELECT batch_id FROM read_provenance WHERE dataset_id=? AND pinned=1 AND expires_at>?",
            (dataset_id, now),
        ):
            keep.add(str(row[0]))
        deleted: list[str] = []
        rows = db.execute(
            "SELECT batch_id FROM batches WHERE dataset_id=? AND status='published' ORDER BY COALESCE(generation,0) DESC,published_at DESC",
            (dataset_id,),
        ).fetchall()
        for row in rows:
            batch_id = str(row[0])
            if batch_id in keep:
                continue
            db.execute("DELETE FROM batch_days WHERE batch_id=?", (batch_id,))
            db.execute("DELETE FROM batches WHERE batch_id=?", (batch_id,))
            deleted.append(batch_id)
        db.execute(
            "DELETE FROM partition_bars WHERE partition_id IN (SELECT dp.partition_id FROM day_partitions dp WHERE dp.dataset_id=? AND NOT EXISTS (SELECT 1 FROM batch_days bd WHERE bd.partition_id=dp.partition_id))",
            (dataset_id,),
        )
        db.execute(
            "DELETE FROM day_partitions WHERE dataset_id=? AND NOT EXISTS (SELECT 1 FROM batch_days bd WHERE bd.partition_id=day_partitions.partition_id)",
            (dataset_id,),
        )
        return deleted

    def publish_raw_batch(
        self,
        batch_id: str,
        *,
        expected_trade_dates=None,
        actual_trade_date: str | None = None,
        quality: str = "good",
        source: str | None = None,
        coverage_metrics: dict | None = None,
        coverage: dict | None = None,
        shadow: bool = False,
        raw_publish_enabled: bool | None = None,
        lease_request_id: str | None = None,
        lease_owner: str | None = None,
        lease_fence: int | None = None,
    ) -> dict:
        """Validate a batch atomically, optionally without changing active data."""
        if raw_publish_enabled is not None:
            shadow = not bool(raw_publish_enabled)
        batch_id = str(batch_id)
        expected = None
        if expected_trade_dates is not None:
            expected = {self._canonical_raw_date(value) for value in expected_trade_dates}
            if None in expected:
                raise ValueError("expected_trade_dates contains an invalid date")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute(
                "SELECT b.*,d.dataset_key,d.provider,d.frequency,d.basis AS dataset_basis,d.universe FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
                (batch_id,),
            ).fetchone()
            if not batch:
                raise KeyError(f"unknown raw batch {batch_id}")
            lease_identity = (str(lease_request_id or "").strip(), str(lease_owner or "").strip())
            if lease_identity[0] or lease_identity[1] or lease_fence is not None:
                try:
                    fence_value = int(lease_fence)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise SnapshotLeaseLostError("snapshot lease fence is invalid") from exc
                if not lease_identity[0] or not lease_identity[1] or fence_value < 1:
                    raise SnapshotLeaseLostError("snapshot lease identity is incomplete")
                self._assert_snapshot_lease_in_tx(db, lease_identity[0], lease_identity[1], fence_value)
            if str(batch["status"]) == "published":
                self._validate_published_raw_batch_in_tx(db, batch)
                result = dict(batch)
                result.update({
                    "dataset_id": str(batch["dataset_id"]),
                    "dataset_key": str(batch["dataset_key"]),
                    "generation": int(batch["generation"] or 0),
                    "deleted_batches": [],
                })
                return result
            if str(batch["status"]) != "staging":
                raise RuntimeError("raw batch is not publishable")
            detail = self._validate_raw_partitions_in_tx(db, batch)
            dates = detail["dates"]
            actual = self._canonical_raw_date(actual_trade_date) if actual_trade_date else max(dates)
            self._validate_raw_batch_dates_in_tx(db, batch, dates, actual or "", expected=expected)
            supplied_coverage = coverage_metrics if coverage_metrics is not None else coverage
            if supplied_coverage is not None:
                try:
                    supplied_text = json.dumps(supplied_coverage, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ValueError("raw batch coverage metrics are invalid") from exc
                trusted_text = json.dumps(detail["coverage"], ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
                if supplied_text != trusted_text:
                    raise RuntimeError("raw batch coverage metrics mismatch")
            if not detail["coverage"].get("coverage_ok"):
                raise RuntimeError("raw batch coverage is below the configured floor")
            # Partition, manifest, and coverage validation can take long
            # enough for the lease to expire.  Recheck immediately before the
            # publication mutation so an expired owner cannot commit active
            # data merely because it claimed the lease earlier.
            if lease_identity[0]:
                self._assert_snapshot_lease_in_tx(db, lease_identity[0], lease_identity[1], fence_value)
            source_value = str(source or batch["source"] or batch["provider"]).strip()[:80]
            if source_value.casefold() != str(batch["source"] or batch["provider"]).strip().casefold():
                raise ValueError("raw batch source does not match staged source")
            previous = db.execute("SELECT active_batch_id,generation FROM active_generations WHERE dataset_id=?", (batch["dataset_id"],)).fetchone()
            latest_generation = db.execute(
                "SELECT MAX(generation) FROM batches WHERE dataset_id=? AND generation IS NOT NULL",
                (batch["dataset_id"],),
            ).fetchone()
            # Every published batch receives a monotonic publication
            # generation.  A shadow generation is metadata for that candidate
            # only; it must never replace the active pointer until an explicit
            # promotion revalidates it.
            generation = max(int(latest_generation[0] or 0) + 1, int(previous["generation"] or 0) + 1 if previous else 1, 1)
            start_date, end_date = min(dates), max(dates)
            quality_value = str(quality or "good").strip().lower()[:32]
            coverage_value = detail["coverage"]
            expected_dates_value = json.dumps(sorted(dates), separators=(",", ":"))
            manifest_payload = dict(detail["manifest_payload"])
            manifest_payload["actual_trade_date"] = actual
            manifest_payload["expected_dates"] = sorted(dates)
            manifest_text = json.dumps(manifest_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            manifest_hash = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
            db.execute(
                "UPDATE batches SET actual_trade_date=?,start_date=?,end_date=?,status='published',shadow=?,publication_mode=?,quality=?,source=?,basis='unadjusted',generation=?,row_count=?,manifest_version=1,manifest_hash=?,expected_dates_json=?,coverage_json=?,published_at=?,error=NULL WHERE batch_id=?",
                (
                    actual, start_date, end_date, 1 if shadow else 0, "shadow" if shadow else "active", quality_value, source_value, generation, detail["row_count"],
                    manifest_hash, expected_dates_value,
                    json.dumps(coverage_value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False),
                    datetime.utcnow().isoformat(), batch_id,
                ),
            )
            if shadow:
                deleted = []
            else:
                db.execute(
                    "INSERT INTO active_generations(dataset_id,active_batch_id,previous_batch_id,generation,updated_at) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(dataset_id) DO UPDATE SET previous_batch_id=active_generations.active_batch_id,active_batch_id=excluded.active_batch_id,generation=excluded.generation,updated_at=excluded.updated_at",
                    (batch["dataset_id"], batch_id, str(previous["active_batch_id"]) if previous else None, generation, datetime.utcnow().isoformat()),
                )
                deleted = self._cleanup_raw_batches_in_tx(db, str(batch["dataset_id"]))
            published = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            result = dict(published)
            result.update({"dataset_id": str(batch["dataset_id"]), "dataset_key": str(batch["dataset_key"]), "generation": generation, "deleted_batches": deleted})
            return result

    def promote_raw_batch(self, batch_id: str, *, source: str | None = None) -> dict:
        """Revalidate a shadow batch and make it the active generation."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute(
                "SELECT b.*,d.dataset_key,d.provider,d.frequency,d.basis AS dataset_basis,d.universe FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
                (str(batch_id),),
            ).fetchone()
            if not batch:
                raise KeyError(f"unknown raw batch {batch_id}")
            if str(batch["status"] or "") != "published" or int(batch["shadow"] or 0) != 1 or str(batch["publication_mode"] or "") != "shadow":
                raise RuntimeError("raw batch is not a shadow publication")
            # Promotion is a security-sensitive transition: re-validate the
            # full batch (per-bar digest) rather than the fast cached read
            # path, so tampered bars cannot be promoted into the active set.
            detail = self._validate_published_raw_batch_in_tx(db, batch)
            source_value = str(source or batch["source"] or batch["provider"]).strip()[:80]
            if source_value.casefold() != str(batch["source"] or batch["provider"]).strip().casefold():
                raise ValueError("raw batch source does not match staged source")
            previous = db.execute("SELECT active_batch_id,generation FROM active_generations WHERE dataset_id=?", (batch["dataset_id"],)).fetchone()
            latest = db.execute("SELECT MAX(generation) FROM batches WHERE dataset_id=?", (batch["dataset_id"],)).fetchone()
            # Promotion gets a fresh generation greater than both the current
            # active pointer and the shadow's publication generation.  Readers
            # can therefore distinguish the promoted snapshot atomically.
            generation = max(int(previous["generation"] or 0) + 1 if previous else 1, int(latest[0] or 0) + 1, 1)
            db.execute(
                "UPDATE batches SET shadow=0,publication_mode='active',generation=?,published_at=?,error=NULL WHERE batch_id=?",
                (generation, datetime.utcnow().isoformat(), str(batch_id)),
            )
            db.execute(
                "INSERT INTO active_generations(dataset_id,active_batch_id,previous_batch_id,generation,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(dataset_id) DO UPDATE SET previous_batch_id=active_generations.active_batch_id,active_batch_id=excluded.active_batch_id,generation=excluded.generation,updated_at=excluded.updated_at",
                (batch["dataset_id"], str(batch_id), str(previous["active_batch_id"]) if previous else None, generation, datetime.utcnow().isoformat()),
            )
            deleted = self._cleanup_raw_batches_in_tx(db, str(batch["dataset_id"]))
            published = db.execute("SELECT * FROM batches WHERE batch_id=?", (str(batch_id),)).fetchone()
            result = dict(published)
            result.update({"dataset_id": str(batch["dataset_id"]), "dataset_key": str(batch["dataset_key"]), "generation": generation, "deleted_batches": deleted, "revalidated": True})
            return result

    promote_batch = promote_raw_batch
    promote_shadow_batch = promote_raw_batch

    publish_complete_batch = publish_raw_batch
    publish_batch = publish_raw_batch

    @staticmethod
    def _raw_age_days(db, actual_date: str, as_of: str) -> int | None:
        actual = StockStore._canonical_raw_date(actual_date)
        cutoff = StockStore._canonical_raw_date(as_of) if as_of else datetime.now(timezone.utc).date().isoformat()
        if not actual or not cutoff or actual > cutoff:
            return None
        try:
            start = datetime.strptime(actual, "%Y-%m-%d").date()
            end = datetime.strptime(cutoff, "%Y-%m-%d").date()
        except (TypeError, ValueError, OverflowError):
            return None
        rows = db.execute(
            "SELECT trade_date,status,is_open FROM trading_calendar WHERE trade_date>? AND trade_date<=? ORDER BY trade_date",
            (actual, cutoff),
        ).fetchall()
        known: dict[str, str] = {}
        for row in rows:
            try:
                open_value = int(row["is_open"])
            except (TypeError, ValueError, OverflowError):
                open_value = -1
            status = str(row["status"] or "unknown").strip().lower()
            known[str(row["trade_date"])] = status if open_value in {0, 1} else "unknown"
        age = 0
        cursor = start
        while cursor < end:
            cursor += timedelta(days=1)
            status = known.get(cursor.isoformat())
            # An explicit unknown is still an unverified calendar day. Count
            # it as potentially open so stale raw data cannot pass by
            # under-counting its trading-day age.
            if status in {"open", "unknown"} or (status is None and cursor.weekday() < 5):
                age += 1
        return age

    def active_raw_batch(self, dataset_key: str = "tushare_daily", *, as_of: str = "", max_stale_trading_days: int = 2) -> dict | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT a.*,b.*,d.dataset_key,d.provider,d.frequency,d.basis AS dataset_basis,d.universe,"
                "a.dataset_id AS active_dataset_id,b.dataset_id AS batch_dataset_id,a.generation AS active_generation,b.generation AS batch_generation "
                "FROM active_generations a JOIN batches b ON b.batch_id=a.active_batch_id JOIN datasets d ON d.dataset_id=a.dataset_id "
                "WHERE d.dataset_key=? AND b.status='published' AND b.shadow=0 AND b.publication_mode='active'",
                (str(dataset_key or "tushare_daily"),),
            ).fetchone()
            if not row:
                return None
            try:
                self._validate_published_raw_batch_cached(db, row)
                if str(row["active_dataset_id"]) != str(row["batch_dataset_id"]) or int(row["active_generation"]) != int(row["batch_generation"]):
                    raise RuntimeError("active raw generation pointer mismatch")
            except (RuntimeError, ValueError, TypeError, KeyError, OverflowError):
                return None
            result = dict(row)
            age = self._raw_age_days(db, str(result.get("actual_trade_date") or ""), as_of)
            result["age_trading_days"] = age
            result["fresh"] = age is not None and age <= max(0, int(max_stale_trading_days)) and str(result.get("basis") or result.get("dataset_basis") or "").lower() == "unadjusted"
            return result

    def active_raw_generation(self, dataset_key: str = "tushare_daily", *, as_of: str = "", max_stale_trading_days: int = 2) -> dict | None:
        return self.active_raw_batch(dataset_key, as_of=as_of, max_stale_trading_days=max_stale_trading_days)

    def active_raw_universe_codes(
        self,
        dataset_key: str = "tushare_daily",
        *,
        as_of: str = "",
        max_stale_trading_days: int = 2,
        limit: int = 8000,
    ) -> tuple[list[str], dict]:
        """Return the active raw generation's code universe without reading bars.

        The raw generation establishes expected membership only.  It is not
        treated as intraday price evidence and its previous close is never
        mixed into the live cross-section.
        """
        active = self.active_raw_batch(dataset_key, as_of=as_of, max_stale_trading_days=max_stale_trading_days)
        if not active or not active.get("fresh"):
            return [], dict(active or {})
        requested = str(active.get("actual_trade_date") or "")
        batch_id = str(active.get("batch_id") or active.get("active_batch_id") or "")
        if not batch_id or not requested:
            return [], dict(active)
        with self._connect() as db:
            rows = db.execute(
                "SELECT DISTINCT pb.code FROM batch_days bd "
                "JOIN partition_bars pb ON pb.partition_id=bd.partition_id "
                "WHERE bd.batch_id=? AND bd.trade_date=? ORDER BY pb.code LIMIT ?",
                (batch_id, requested, max(1, min(int(limit), 10000))),
            ).fetchall()
        return [str(row[0]) for row in rows if str(row[0] or "").strip()], dict(active)

    def save_intraday_market_regime_state(self, state: dict, *, scope: str = "whole_market") -> dict:
        """Persist only compact immutable cross-section diagnostics/state."""
        value = dict(state or {})
        now = str(value.get("updated_at") or datetime.utcnow().isoformat())
        fields = (
            "regime", "pending_regime", "pending_count", "source", "source_timestamp",
            "sample_size", "expected_size", "coverage", "breadth", "advancing", "declining",
            "flat", "median_return", "quote_timestamp_min", "quote_timestamp_max", "quality", "reason",
        )
        row = {key: value.get(key) for key in fields}
        with self._connect() as db:
            db.execute(
                "INSERT INTO intraday_market_regime_state(scope,regime,pending_regime,pending_count,source,source_timestamp,sample_size,expected_size,coverage,breadth,advancing,declining,flat,median_return,quote_timestamp_min,quote_timestamp_max,quality,reason,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(scope) DO UPDATE SET regime=excluded.regime,pending_regime=excluded.pending_regime,pending_count=excluded.pending_count,source=excluded.source,source_timestamp=excluded.source_timestamp,sample_size=excluded.sample_size,expected_size=excluded.expected_size,coverage=excluded.coverage,breadth=excluded.breadth,advancing=excluded.advancing,declining=excluded.declining,flat=excluded.flat,median_return=excluded.median_return,quote_timestamp_min=excluded.quote_timestamp_min,quote_timestamp_max=excluded.quote_timestamp_max,quality=excluded.quality,reason=excluded.reason,updated_at=excluded.updated_at",
                (str(scope), str(row["regime"] or "unknown"), str(row["pending_regime"] or "unknown"), int(row["pending_count"] or 0), str(row["source"] or ""), str(row["source_timestamp"] or ""), int(row["sample_size"] or 0), int(row["expected_size"] or 0), float(row["coverage"] or 0), row["breadth"], int(row["advancing"] or 0), int(row["declining"] or 0), int(row["flat"] or 0), row["median_return"], str(row["quote_timestamp_min"] or ""), str(row["quote_timestamp_max"] or ""), str(row["quality"] or "unknown"), str(row["reason"] or "")[:500], now),
            )
            saved = db.execute("SELECT * FROM intraday_market_regime_state WHERE scope=?", (str(scope),)).fetchone()
        return dict(saved)

    def intraday_market_regime_state(self, scope: str = "whole_market") -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM intraday_market_regime_state WHERE scope=?", (str(scope),)).fetchone()
        return dict(row) if row else None

    def raw_batch_bars(self, batch_id: str, codes=None, *, before_or_equal: str = "", after: str = "") -> dict[str, list[dict]]:
        with self._connect() as db:
            row = db.execute(
                "SELECT b.*,d.dataset_key,d.provider,d.frequency,d.basis AS dataset_basis,d.universe FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=? AND b.status='published'",
                (str(batch_id),),
            ).fetchone()
            if not row:
                return {}
            try:
                self._validate_published_raw_batch_cached(db, row)
            except (RuntimeError, ValueError, TypeError, KeyError, OverflowError):
                return {}
            reader = RawBatchRead(db, "direct", str(row["batch_id"]), str(row["dataset_id"]), 0, str(row["basis"]), str(row["source"]))
            return reader.bars(codes, before_or_equal, after)

    def raw_history(self, codes=None, *, as_of: str = "", dataset_key: str = "tushare_daily", max_stale_trading_days: int = 2, after: str = "") -> tuple[dict[str, list[dict]], dict]:
        with self.pin_active_raw_batch(dataset_key, as_of=as_of, max_stale_trading_days=max_stale_trading_days, reader="history") as reader:
            if reader is None:
                return {}, {}
            return reader.bars(codes, before_or_equal=as_of, after=after), {
                "batch_id": reader.batch_id,
                "dataset_id": reader.dataset_id,
                "generation": reader.generation,
                "basis": reader.basis,
                "source": reader.source,
            }

    def read_active_raw_bars(self, codes=None, *, as_of: str = "", dataset_key: str = "tushare_daily", max_stale_trading_days: int = 2, after: str = "") -> dict[str, list[dict]]:
        return self.raw_history(codes, as_of=as_of, dataset_key=dataset_key, max_stale_trading_days=max_stale_trading_days, after=after)[0]

    load_raw_bars = read_active_raw_bars

    @contextmanager
    def pin_active_raw_batch(self, dataset_key: str = "tushare_daily", *, as_of: str = "", max_stale_trading_days: int = 2, reader: str = "", ttl_seconds: int = 900):
        """Pin one active generation and hold a SQLite read snapshot."""
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        read_id = "read-" + uuid.uuid4().hex
        read = None
        try:
            db.execute("BEGIN")
            row = db.execute(
                "SELECT a.*,b.*,d.dataset_key,d.provider,d.basis AS dataset_basis,d.frequency,d.universe,"
                "a.dataset_id AS active_dataset_id,b.dataset_id AS batch_dataset_id,a.generation AS active_generation,b.generation AS batch_generation "
                "FROM active_generations a JOIN batches b ON b.batch_id=a.active_batch_id JOIN datasets d ON d.dataset_id=a.dataset_id "
                "WHERE d.dataset_key=? AND b.status='published' AND b.shadow=0 AND b.publication_mode='active'",
                (str(dataset_key or "tushare_daily"),),
            ).fetchone()
            if row:
                try:
                    self._validate_published_raw_batch_cached(db, row)
                    if str(row["dataset_id"]) != str(row["batch_dataset_id"]) or int(row["active_generation"]) != int(row["batch_generation"]):
                        raise RuntimeError("active raw generation pointer mismatch")
                except (RuntimeError, ValueError, TypeError, KeyError, OverflowError):
                    row = None
            if row:
                age = self._raw_age_days(db, str(row["actual_trade_date"] or ""), as_of)
                valid = age is not None and age <= max(0, int(max_stale_trading_days)) and str(row["basis"] or "").lower() == "unadjusted"
                if valid:
                    now = datetime.utcnow()
                    expires = now + timedelta(seconds=max(60, int(ttl_seconds)))
                    with self._connect() as write_db:
                        write_db.execute(
                            "INSERT INTO read_provenance(read_id,dataset_id,batch_id,generation,reader,basis,source,started_at,expires_at,pinned) VALUES(?,?,?,?,?,?,?,?,?,1)",
                            (read_id, row["dataset_id"], row["active_batch_id"], int(row["generation"]), str(reader or ""), str(row["basis"]), str(row["source"]), now.isoformat(), expires.isoformat()),
                        )
                    read = RawBatchRead(db, read_id, str(row["active_batch_id"]), str(row["dataset_id"]), int(row["generation"]), str(row["basis"]), str(row["source"]))
            yield read
        finally:
            try:
                db.rollback()
            except sqlite3.Error:
                pass
            db.close()
            if read is not None:
                with self._connect() as write_db:
                    write_db.execute("UPDATE read_provenance SET pinned=0,closed_at=? WHERE read_id=?", (datetime.utcnow().isoformat(), read_id))

    pin_active_batch = pin_active_raw_batch

    # v0.13 raw generation methods are defined below this compatibility API.
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

    @staticmethod
    def _stock_display_name(value) -> str:
        text = str(value or "")
        text = "".join(char for char in text if ord(char) >= 32 and ord(char) != 127)
        return " ".join(text.split())[:64]

    @staticmethod
    def _coerce_cost(value) -> float | None:
        """Parse a strictly finite positive cost; malformed input is rejected."""
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, str) and not re.fullmatch(r"[+]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|[+]?\.\d+(?:[eE][+-]?\d+)?", value.strip()):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) and parsed > 0 else None

    def upsert_stock_symbol(self, code: str, name: str, source: str = "") -> bool:
        from .core import normalize_code, normalize_stock_name

        normalized_code = normalize_code(code)
        display = self._stock_display_name(name)
        normalized_name = normalize_stock_name(display)
        if not re.fullmatch(r"\d{6}", normalized_code) or not display or not normalized_name or normalized_name == normalized_code:
            return False
        with self._connect() as db:
            db.execute(
                "INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(code) DO UPDATE SET name=excluded.name,normalized_name=excluded.normalized_name,source=excluded.source,updated_at=excluded.updated_at",
                (normalized_code, display, normalized_name, str(source or "")[:80], datetime.utcnow().isoformat()),
            )
        return True

    def upsert_stock_symbols(self, rows, source: str = "") -> int:
        count = 0
        for item in rows or []:
            if isinstance(item, dict):
                code, name = item.get("code"), item.get("name")
                row_source = item.get("source") or source
            else:
                try:
                    code, name = item[0], item[1]
                    row_source = source
                except (IndexError, TypeError):
                    continue
            if self.upsert_stock_symbol(str(code or ""), str(name or ""), str(row_source or "")):
                count += 1
        return count

    def stock_symbol(self, code: str) -> dict | None:
        from .core import normalize_code

        normalized_code = normalize_code(code)
        if not re.fullmatch(r"\d{6}", normalized_code):
            return None
        with self._connect() as db:
            row = db.execute("SELECT * FROM stock_symbols WHERE code=?", (normalized_code,)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _escape_like(value: str) -> str:
        return str(value or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def search_stock_symbols(self, query: str, limit: int = 10) -> list[dict]:
        from .core import normalize_code, normalize_stock_name

        raw = str(query or "").strip()
        if not raw:
            return []
        code_query = normalize_code(raw)
        try:
            size = max(1, min(int(limit), 10))
        except (TypeError, ValueError, OverflowError):
            size = 10
        with self._connect() as db:
            if re.fullmatch(r"\d+", code_query):
                rows = db.execute(
                    "SELECT * FROM stock_symbols WHERE code LIKE ? ESCAPE '\\' ORDER BY code LIMIT ?",
                    (self._escape_like(code_query) + "%", size),
                ).fetchall()
            else:
                normalized = normalize_stock_name(raw)
                if not normalized:
                    return []
                rows = db.execute(
                    "SELECT * FROM stock_symbols WHERE normalized_name LIKE ? ESCAPE '\\' ORDER BY code LIMIT ?",
                    ("%" + self._escape_like(normalized) + "%", size),
                ).fetchall()
        return [dict(row) for row in rows]

    def resolve_stock_symbol(self, query: str, limit: int = 10) -> dict:
        """Resolve code/prefix, exact name, or one unique name substring."""
        from .core import normalize_code, normalize_stock_name

        raw = str(query or "").strip()
        miss = {"status": "miss", "query": raw, "matches": []}
        if not raw:
            return miss
        try:
            size = max(1, min(int(limit), 10))
        except (TypeError, ValueError, OverflowError):
            size = 10
        code_query = normalize_code(raw)
        with self._connect() as db:
            if re.fullmatch(r"\d{6}", code_query):
                row = db.execute("SELECT * FROM stock_symbols WHERE code=?", (code_query,)).fetchone()
                return {"status": "ok", "query": raw, "code": code_query, "name": str(row["name"]) if row else "", "matches": [dict(row)] if row else []}
            if re.fullmatch(r"\d{1,5}", code_query):
                rows = db.execute(
                    "SELECT * FROM stock_symbols WHERE code LIKE ? ESCAPE '\\' ORDER BY code LIMIT ?",
                    (self._escape_like(code_query) + "%", size + 1),
                ).fetchall()
            else:
                normalized = normalize_stock_name(raw)
                if not normalized:
                    return miss
                exact = db.execute("SELECT * FROM stock_symbols WHERE normalized_name=? ORDER BY code", (normalized,)).fetchall()
                if len(exact) == 1:
                    row = exact[0]
                    return {"status": "ok", "query": raw, "code": str(row["code"]), "name": str(row["name"]), "matches": [dict(row)]}
                rows = db.execute(
                    "SELECT * FROM stock_symbols WHERE normalized_name LIKE ? ESCAPE '\\' ORDER BY code LIMIT ?",
                    ("%" + self._escape_like(normalized) + "%", size + 1),
                ).fetchall()
        matches = [dict(row) for row in rows]
        if len(matches) == 1:
            row = matches[0]
            return {"status": "ok", "query": raw, "code": str(row["code"]), "name": str(row["name"]), "matches": matches}
        if matches:
            return {"status": "ambiguous", "query": raw, "matches": matches[:size]}
        return miss

    # Short aliases make the resolver convenient for command handlers and
    # preserve a small public API for integrations.
    resolve_stock = resolve_stock_symbol
    resolve_stock_query = resolve_stock_symbol

    def add_watch(self, scope: str, code: str, limit: int, cost_price: float | None = None, name: str | None = None) -> bool:
        from .core import normalize_code

        code = normalize_code(code)
        if not re.fullmatch(r"\d{6}", code):
            return False
        if cost_price is not None:
            parsed_cost = self._coerce_cost(cost_price)
            if parsed_cost is None:
                return False
            cost_price = parsed_cost
        clean_name = self._stock_display_name(name) or None
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
            if clean_name and clean_name != code:
                from .core import normalize_stock_name

                normalized_name = normalize_stock_name(clean_name)
                if normalized_name:
                    db.execute(
                        "INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?) "
                        "ON CONFLICT(code) DO UPDATE SET name=excluded.name,normalized_name=excluded.normalized_name,source=excluded.source,updated_at=excluded.updated_at",
                        (code, clean_name, normalized_name, "watchlist", datetime.utcnow().isoformat()),
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
                    parsed = float(cost) if cost is not None else 0.0
                    value = parsed if math.isfinite(parsed) and parsed > 0 else None
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

    def set_intraday_enabled(self, origin: str, enabled: bool) -> None:
        value = str(origin or "").strip()
        if not value:
            raise ValueError("intraday origin is required")
        with self._connect() as db:
            db.execute(
                "INSERT INTO intraday_origin_preferences(origin,enabled,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(origin) DO UPDATE SET enabled=excluded.enabled,updated_at=excluded.updated_at",
                (value, int(bool(enabled)), datetime.utcnow().isoformat()),
            )

    def is_intraday_enabled(self, origin: str) -> bool:
        value = str(origin or "").strip()
        if not value:
            return False
        with self._connect() as db:
            row = db.execute("SELECT enabled FROM intraday_origin_preferences WHERE origin=?", (value,)).fetchone()
            return bool(row[0]) if row else True

    def intraday_subscriptions(self) -> list[str]:
        with self._connect() as db:
            return [
                str(row[0])
                for row in db.execute(
                    "SELECT s.origin FROM subscriptions s LEFT JOIN intraday_origin_preferences p ON p.origin=s.origin "
                    "WHERE s.enabled=1 AND COALESCE(p.enabled,1)=1 ORDER BY s.origin"
                )
            ]

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

    def _daily_quote_rows(self, trade_date: str, quotes, now_text: str | None = None):
        from .core import normalize_code, normalize_stock_name

        normalized_trade_date = self._date_norm(trade_date)
        rows = []
        symbols = []
        symbol_timestamp = now_text or datetime.utcnow().isoformat()
        for quote in quotes or []:
            code = normalize_code(getattr(quote, "code", ""))
            if not re.fullmatch(r"\d{6}", code):
                continue
            rows.append(
                (normalized_trade_date, code, quote.name, quote.price, quote.prev_close, quote.amount,
                 quote.pct_change, quote.volume, quote.fetched_at.isoformat(), getattr(quote, "source", ""), quote.provider_ts.isoformat() if getattr(quote, "provider_ts", None) else None)
            )
            name = self._stock_display_name(getattr(quote, "name", ""))
            normalized_name = normalize_stock_name(name)
            if name and normalized_name and normalized_name != code:
                symbols.append((code, name, normalized_name, str(getattr(quote, "source", "") or "daily_quote")[:80], symbol_timestamp))
        return normalized_trade_date, rows, symbols

    def _save_daily_quotes_in_tx(self, db, trade_date: str, quotes, keep_days: int = 180, *, now_text: str | None = None) -> int:
        normalized_trade_date, rows, symbols = self._daily_quote_rows(trade_date, quotes, now_text=now_text)
        if not rows:
            return 0
        cutoff = (datetime.strptime(normalized_trade_date, "%Y-%m-%d") - timedelta(days=keep_days)).strftime("%Y-%m-%d")
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
        if symbols:
            db.executemany(
                "INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(code) DO UPDATE SET name=excluded.name,normalized_name=excluded.normalized_name,source=excluded.source,updated_at=excluded.updated_at",
                symbols,
            )
        db.execute("DELETE FROM daily_quotes WHERE trade_date < ?", (cutoff,))
        return len(rows)

    def save_daily_quotes(self, trade_date: str, quotes, keep_days: int = 180) -> int:
        with self._connect() as db:
            return self._save_daily_quotes_in_tx(db, trade_date, quotes, keep_days)

    @staticmethod
    def _quality_rank(value: str) -> int:
        return {"unknown": 0, "cached": 1, "degraded": 2, "partial": 3, "good": 4}.get(str(value or "").lower(), 0)

    def _save_snapshot_meta_in_tx(
        self,
        db,
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
        now_text: str | None = None,
    ) -> None:
        if not trade_date:
            return
        source = str(source or "unknown")
        quality = str(quality or "unknown").lower()
        now = now_text or datetime.utcnow().isoformat()
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
        with self._connect() as db:
            self._save_snapshot_meta_in_tx(
                db,
                trade_date,
                source,
                quality,
                complete,
                requested_date,
                note,
                snapshot_version=snapshot_version,
                state=state,
                attempts=attempts,
                last_error=last_error,
                next_retry_at=next_retry_at,
                terminal=terminal,
            )

    @staticmethod
    def _snapshot_diagnostic_failure_kind(diagnostics) -> str | None:
        """Return only an explicitly classified, durable retry reason."""
        if not isinstance(diagnostics, dict):
            return None
        for key in ("internal_error", "unexpected_error", "programming_error", "cancelled", "canceled", "invalid_date", "future_date"):
            value = diagnostics.get(key)
            if value is True or str(value or "").strip().lower() in {"1", "true", "yes", "on"}:
                return None
        value = str(diagnostics.get("failure_kind") or "").strip().lower()
        aliases = {
            "network_failed": "network",
            "rate_limited": "rate_limit",
            "breaker_open": "breaker",
            "calendar_unavailable": "calendar",
            "publish_failed": "publish",
            "coverage_failed": "coverage",
            "history": "history_invalid",
        }
        value = aliases.get(value, value)
        return value if value in {"network", "timeout", "breaker", "rate_limit", "not_published", "calendar", "publish", "coverage", "history_invalid"} else None

    @staticmethod
    def _snapshot_exception_failure_kind(exception) -> str | None:
        """Classify provider exceptions without importing the provider module."""
        if exception is None:
            return None
        if isinstance(exception, asyncio.CancelledError) or type(exception).__name__ in {"CancelledError", "CancelledError"}:
            raise exception
        names = {
            "TushareCircuitOpen": "breaker",
            "TushareRateLimitError": "rate_limit",
            "TushareNetworkError": "network",
            "TushareNotPublishedError": "not_published",
            "TusharePublishError": "publish",
            "TushareCoverageError": "coverage",
            "TushareHistoryError": "history_invalid",
            "TushareCalendarError": "calendar",
        }
        return names.get(type(exception).__name__)

    @staticmethod
    def _snapshot_result_is_typed(result) -> bool:
        if isinstance(result, dict):
            return all(key in result for key in ("quotes", "trade_date", "complete"))
        return all(hasattr(result, key) for key in ("quotes", "trade_date", "complete"))

    @staticmethod
    def _snapshot_result_value(result, key: str, default=None):
        if isinstance(result, dict):
            return result.get(key, default)
        return getattr(result, key, default)

    def finalize_snapshot_request_owned(
        self,
        request_id: str,
        requested_date: str,
        owner: str,
        fence: int,
        *,
        quotes=None,
        actual_trade_date: str | None = None,
        source: str = "tushare",
        quality: str = "unknown",
        complete: bool = False,
        note: str = "",
        keep_days: int = 180,
        snapshot_version: int | None = None,
        state: str | None = None,
        attempts: int | None = None,
        last_error: str | None = None,
        next_retry_at: str | None = None,
        terminal: bool = False,
        failure_kind: str = "",
        calendar_evidence: dict | None = None,
        provenance: dict | None = None,
        request_state: str | None = None,
        request_source: str | None = None,
        request_quality: str | None = None,
        request_last_error: str | None = None,
        request_next_retry_at: str | None = None,
        request_terminal: bool | None = None,
        request_failure_kind: str | None = None,
        persist_meta: bool = True,
        lease_ttl_seconds: float | None = None,
        now: float | None = None,
        result=None,
        exception=None,
        diagnostics: dict | None = None,
    ) -> dict:
        """CAS-finalize exactly one live ``fetching`` snapshot request.

        Durable classified state wins over cleanup.  Invalid or untyped results,
        unexpected exceptions, and cancellation intentionally leave the request
        in ``fetching``; the latter is re-raised to preserve task cancellation.
        """
        request_key = str(request_id or "").strip()
        requested_value = self._date_norm(requested_date)
        owner_value = str(owner or "").strip()
        try:
            fence_value = int(fence)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("snapshot lease fence is invalid") from exc
        if not request_key or not requested_value or not owner_value or fence_value < 1:
            raise ValueError("snapshot lease identity is required")
        if len(owner_value) > 160:
            raise ValueError("snapshot lease owner is too long")
        result_invalid = False
        if result is not None:
            if not self._snapshot_result_is_typed(result):
                result_invalid = True
            else:
                if quotes is None:
                    quotes = self._snapshot_result_value(result, "quotes", [])
                if actual_trade_date is None:
                    actual_trade_date = self._snapshot_result_value(result, "trade_date")
                if diagnostics is None:
                    diagnostics = self._snapshot_result_value(result, "diagnostics", {})
                if state is None and bool(self._snapshot_result_value(result, "complete", False)):
                    state = "complete"
        actual_value = self._date_norm(actual_trade_date) if actual_trade_date else None
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", requested_value):
            raise ValueError("snapshot requested date is invalid")
        if actual_value and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", actual_value):
            result_invalid = True
        if actual_value and actual_value > requested_value:
            result_invalid = True
        current = self._snapshot_lease_epoch(now)
        now_text = datetime.utcnow().isoformat()
        calendar_text = json.dumps(calendar_evidence or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        provenance_text = json.dumps(provenance or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        request_state_value = str(request_state if request_state is not None else state or "pending")[:32]
        request_source_value = str(request_source if request_source is not None else source or "")[:80]
        request_quality_value = str(request_quality if request_quality is not None else quality or "unknown")[:32]
        request_error_value = request_last_error if request_last_error is not None else last_error
        request_error_value = str(request_error_value)[:500] if request_error_value is not None else None
        request_retry_value = request_next_retry_at if request_next_retry_at is not None else next_retry_at
        request_retry_value = str(request_retry_value)[:80] if request_retry_value is not None else None
        request_terminal_value = int(bool(request_terminal if request_terminal is not None else terminal))
        request_failure_value = str(request_failure_kind if request_failure_kind is not None else failure_kind or "")[:64]
        request_attempts = max(0, int(attempts or 0))
        diagnostic_failure = self._snapshot_diagnostic_failure_kind(diagnostics)
        explicit_state = str(request_state if request_state is not None else state or "").strip().lower()

        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._assert_snapshot_lease_owner_in_tx(db, request_key, owner_value, fence_value, now=current)
            persisted_state = str(row["state"] or "").strip().lower()
            if persisted_state != "fetching":
                preserved = self._snapshot_lease_view(row, current, acquired=True, owner=owner_value, fence=fence_value)
                preserved["finalized"] = False
                preserved["preserved_state"] = persisted_state
                return preserved
            if isinstance(exception, asyncio.CancelledError) or (exception is not None and type(exception).__name__ == "CancelledError"):
                raise exception
            exception_failure = self._snapshot_exception_failure_kind(exception)
            if result_invalid:
                preserved = self._snapshot_lease_view(row, current, acquired=True, owner=owner_value, fence=fence_value)
                preserved["finalized"] = False
                preserved["preserved_state"] = "fetching"
                preserved["finalizer_reason"] = "invalid_result"
                return preserved
            if diagnostic_failure:
                request_state_value = "retry"
                request_failure_value = diagnostic_failure
                request_terminal_value = 0
                if not request_error_value:
                    request_error_value = "classified provider diagnostic"
            elif exception_failure:
                request_state_value = "retry"
                request_failure_value = exception_failure
                request_terminal_value = 0
                if not request_error_value:
                    request_error_value = f"typed provider exception: {exception_failure}"
            elif exception is not None:
                preserved = self._snapshot_lease_view(row, current, acquired=True, owner=owner_value, fence=fence_value)
                preserved["finalized"] = False
                preserved["preserved_state"] = "fetching"
                preserved["finalizer_reason"] = "internal_error"
                return preserved
            elif explicit_state not in {"retry", "partial", "complete", "terminal", "failed", "shadow"}:
                preserved = self._snapshot_lease_view(row, current, acquired=True, owner=owner_value, fence=fence_value)
                preserved["finalized"] = False
                preserved["preserved_state"] = "fetching"
                preserved["finalizer_reason"] = "unclassified"
                return preserved
            else:
                request_state_value = explicit_state
            if request_state_value in {"complete", "terminal"}:
                request_terminal_value = 1
            if request_state_value == "retry":
                request_terminal_value = 0
            saved = 0
            if persist_meta and actual_value:
                saved = self._save_daily_quotes_in_tx(
                    db,
                    actual_value,
                    quotes,
                    keep_days,
                    now_text=now_text,
                )
                self._save_snapshot_meta_in_tx(
                    db,
                    actual_value,
                    source,
                    quality,
                    request_state_value in {"complete", "terminal"} or complete,
                    requested_value,
                    note,
                    snapshot_version=snapshot_version,
                    state=request_state_value,
                    attempts=request_attempts,
                    last_error=request_error_value,
                    next_retry_at=request_retry_value,
                    terminal=bool(request_terminal_value),
                    now_text=now_text,
                )
            elif quotes and actual_value:
                saved = self._save_daily_quotes_in_tx(
                    db,
                    actual_value,
                    quotes,
                    keep_days,
                    now_text=now_text,
                )
            self._assert_snapshot_lease_in_tx(db, request_key, owner_value, fence_value, now=current)
            assignments = (
                "requested_date=?,actual_trade_date=?,state=?,attempts=?,source=?,quality=?,"
                "last_error=?,next_retry_at=?,terminal=?,failure_kind=?,calendar_evidence_json=?,"
                "provenance_json=?,updated_at=?"
            )
            values = [
                requested_value,
                actual_value,
                request_state_value,
                request_attempts,
                request_source_value,
                request_quality_value,
                request_error_value,
                request_retry_value,
                request_terminal_value,
                request_failure_value,
                calendar_text,
                provenance_text,
                now_text,
            ]
            if lease_ttl_seconds is not None:
                row = db.execute("SELECT lease_expires_at FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
                try:
                    expiry = float(row["lease_expires_at"] or 0) if row else 0.0
                except (TypeError, ValueError, OverflowError):
                    expiry = 0.0
                new_expiry = max(expiry, current + self._snapshot_lease_ttl(lease_ttl_seconds))
                assignments += ",lease_expires_at=?,lease_updated_at=?"
                values.extend((new_expiry, now_text))
            values.extend((request_key, owner_value, fence_value, current))
            cursor = db.execute(
                f"UPDATE snapshot_requests SET {assignments} WHERE request_id=? AND lease_owner=? AND lease_fence=? AND lease_expires_at>? AND terminal=0 AND state='fetching'",
                values,
            )
            if cursor.rowcount <= 0:
                current_row = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
                if current_row and str(current_row["lease_owner"] or "") == owner_value and int(current_row["lease_fence"] or 0) == fence_value and str(current_row["state"] or "").strip().lower() != "fetching":
                    preserved = self._snapshot_lease_view(current_row, current, acquired=True, owner=owner_value, fence=fence_value)
                    preserved["finalized"] = False
                    preserved["preserved_state"] = str(current_row["state"] or "").strip().lower()
                    return preserved
                raise SnapshotLeaseLostError("snapshot lease update lost ownership")
            refreshed = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
            result = self._snapshot_lease_view(refreshed, current, acquired=True, owner=owner_value, fence=fence_value)
            result["saved_quotes"] = saved
            result["finalized"] = True
            return result

    def save_tushare_snapshot_owned(self, *args, **kwargs) -> dict:
        """Compatibility wrapper for the fenced snapshot finalizer."""
        return self.finalize_snapshot_request_owned(*args, **kwargs)

    # Compatibility spellings for integrations that describe the operation as
    # a generic snapshot-result persistence call.
    persist_tushare_snapshot_owned = finalize_snapshot_request_owned
    save_snapshot_result_owned = finalize_snapshot_request_owned
    persist_snapshot_result_owned = finalize_snapshot_request_owned

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

    @staticmethod
    def _snapshot_lease_epoch(now: float | None = None) -> float:
        current = float(time.time() if now is None else now)
        if not math.isfinite(current) or current < 0:
            raise ValueError("snapshot lease clock value is invalid")
        return current

    @staticmethod
    def _snapshot_lease_ttl(value: float) -> float:
        ttl = float(value)
        if not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("snapshot lease TTL is invalid")
        return max(5.0, min(ttl, 86400.0))

    @classmethod
    def _snapshot_lease_view(cls, row, now: float, **extra) -> dict:
        value = dict(row) if row is not None else {}
        try:
            expiry = float(value.get("lease_expires_at") or 0)
        except (TypeError, ValueError, OverflowError):
            expiry = 0.0
        owner = str(value.get("lease_owner") or "")
        value.update({
            "lease_active": bool(owner and expiry > now),
            "lease_stale": bool(owner and expiry <= now),
            "lease_expires_at": expiry,
        })
        value.update(extra)
        return value

    @classmethod
    def _assert_snapshot_lease_in_tx(cls, db, request_id: str, owner: str, fence: int, *, now: float | None = None) -> None:
        """Fail closed unless one transaction still owns a live lease."""
        request_key = str(request_id or "").strip()
        owner_value = str(owner or "").strip()
        try:
            fence_value = int(fence)
        except (TypeError, ValueError, OverflowError) as exc:
            raise SnapshotLeaseLostError("snapshot lease fence is invalid") from exc
        if not request_key or not owner_value or fence_value < 1:
            raise SnapshotLeaseLostError("snapshot lease identity is incomplete")
        current = cls._snapshot_lease_epoch(now)
        row = db.execute(
            "SELECT lease_owner,lease_fence,lease_expires_at,state,terminal FROM snapshot_requests WHERE request_id=?",
            (request_key,),
        ).fetchone()
        try:
            expiry = float(row["lease_expires_at"] or 0) if row else 0.0
        except (TypeError, ValueError, OverflowError):
            expiry = 0.0
        state = str(row["state"] or "").strip().lower() if row else ""
        if (
            not row
            or str(row["lease_owner"] or "") != owner_value
            or int(row["lease_fence"] or 0) != fence_value
            or int(row["terminal"] or 0)
            or state in {"complete", "terminal"}
            or expiry <= current
        ):
            raise SnapshotLeaseLostError("snapshot lease owner or fence no longer matches")

    @classmethod
    def _assert_snapshot_lease_owner_in_tx(cls, db, request_id: str, owner: str, fence: int, *, now: float | None = None):
        """Check owner/fence without treating an already-finalized row as lost."""
        request_key = str(request_id or "").strip()
        owner_value = str(owner or "").strip()
        try:
            fence_value = int(fence)
        except (TypeError, ValueError, OverflowError) as exc:
            raise SnapshotLeaseLostError("snapshot lease fence is invalid") from exc
        if not request_key or not owner_value or fence_value < 1:
            raise SnapshotLeaseLostError("snapshot lease identity is incomplete")
        current = cls._snapshot_lease_epoch(now)
        row = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
        try:
            expiry = float(row["lease_expires_at"] or 0) if row else 0.0
        except (TypeError, ValueError, OverflowError):
            expiry = 0.0
        if (
            not row
            or str(row["lease_owner"] or "") != owner_value
            or int(row["lease_fence"] or 0) != fence_value
            or expiry <= current
        ):
            raise SnapshotLeaseLostError("snapshot lease owner or fence no longer matches")
        return row

    def snapshot_lease_state(self, request_id: str, *, now: float | None = None) -> dict | None:
        """Read request state and whether its durable lease is currently live."""
        current = self._snapshot_lease_epoch(now)
        with self._connect() as db:
            row = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (str(request_id or ""),)).fetchone()
            return self._snapshot_lease_view(row, current) if row else None

    def claim_snapshot_lease(
        self,
        request_id: str,
        requested_date: str,
        owner: str,
        *,
        ttl_seconds: float = 1800.0,
        now: float | None = None,
    ) -> dict:
        """Atomically claim or renew one schema-14 daily snapshot lease.

        A live lease owned by another process is never replaced.  An expired
        lease is replaced with a strictly larger fence, which makes later
        writes from the old owner fail their owner/fence comparison.
        """
        request_key = str(request_id or "").strip()
        date_value = self._date_norm(requested_date)
        owner_value = str(owner or "").strip()
        if not request_key or not date_value or not owner_value:
            raise ValueError("snapshot lease identity is required")
        if len(owner_value) > 160:
            raise ValueError("snapshot lease owner is too long")
        current = self._snapshot_lease_epoch(now)
        ttl = self._snapshot_lease_ttl(ttl_seconds)
        expiry = current + ttl
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO snapshot_requests(request_id,requested_date,state,attempts,source,quality,terminal,created_at,updated_at) "
                "VALUES(?,?, 'pending',0,'','unknown',0,?,?) ON CONFLICT(request_id) DO NOTHING",
                (request_key, date_value, now_text, now_text),
            )
            row = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
            if not row:
                raise RuntimeError("snapshot lease request could not be created")
            state = str(row["state"] or "").strip().lower()
            terminal = bool(int(row["terminal"] or 0)) or state in {"complete", "terminal"}
            if terminal:
                # Preserve the persisted owner/fence in the response.  A
                # terminal row is not claimable, and reporting the requester
                # here would make a waiter look like the lease owner.
                return self._snapshot_lease_view(row, current, acquired=False, reason="terminal")
            current_owner = str(row["lease_owner"] or "")
            try:
                current_expiry = float(row["lease_expires_at"] or 0)
            except (TypeError, ValueError, OverflowError):
                current_expiry = 0.0
            try:
                current_fence = max(0, int(row["lease_fence"] or 0))
            except (TypeError, ValueError, OverflowError):
                current_fence = 0
            if current_owner and current_expiry > current and current_owner != owner_value:
                return self._snapshot_lease_view(
                    row,
                    current,
                    acquired=False,
                    owner=current_owner,
                    fence=current_fence,
                    reason="active",
                )
            if current_owner == owner_value and current_expiry > current:
                renewed_expiry = max(current_expiry, expiry)
                db.execute(
                    "UPDATE snapshot_requests SET lease_expires_at=?,lease_updated_at=?,updated_at=? WHERE request_id=? AND lease_owner=? AND lease_fence=?",
                    (renewed_expiry, now_text, now_text, request_key, owner_value, current_fence),
                )
                refreshed = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
                return self._snapshot_lease_view(refreshed, current, acquired=True, renewed=True, owner=owner_value, fence=current_fence)
            next_fence = current_fence + 1
            cursor = db.execute(
                "UPDATE snapshot_requests SET state='fetching',terminal=0,next_retry_at=NULL,lease_owner=?,lease_fence=?,lease_expires_at=?,lease_updated_at=?,attempts=MAX(attempts,0)+1,updated_at=? "
                "WHERE request_id=? AND (lease_owner='' OR lease_expires_at<=? OR (lease_owner=? AND lease_fence=?))",
                (owner_value, next_fence, expiry, now_text, now_text, request_key, current, owner_value, current_fence),
            )
            if cursor.rowcount <= 0:
                refreshed = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
                return self._snapshot_lease_view(refreshed, current, acquired=False, owner=str(refreshed["lease_owner"] or ""), fence=int(refreshed["lease_fence"] or 0), reason="contended")
            refreshed = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
            return self._snapshot_lease_view(refreshed, current, acquired=True, renewed=False, owner=owner_value, fence=next_fence)

    def renew_snapshot_lease(
        self,
        request_id: str,
        owner: str,
        fence: int,
        *,
        ttl_seconds: float = 1800.0,
        now: float | None = None,
    ) -> dict | None:
        """Extend a lease only when owner and fence still match."""
        request_key = str(request_id or "").strip()
        owner_value = str(owner or "").strip()
        try:
            fence_value = int(fence)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("snapshot lease fence is invalid") from exc
        if not request_key or not owner_value or fence_value < 1:
            raise ValueError("snapshot lease identity is required")
        current = self._snapshot_lease_epoch(now)
        expiry = current + self._snapshot_lease_ttl(ttl_seconds)
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(
                "UPDATE snapshot_requests SET lease_expires_at=MAX(lease_expires_at,?),lease_updated_at=?,updated_at=? "
                "WHERE request_id=? AND lease_owner=? AND lease_fence=? AND lease_expires_at>? AND terminal=0",
                (expiry, now_text, now_text, request_key, owner_value, fence_value, current),
            )
            if cursor.rowcount <= 0:
                return None
            row = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
            return self._snapshot_lease_view(row, current, renewed=True, owner=owner_value, fence=fence_value)

    def release_snapshot_lease(self, request_id: str, owner: str, fence: int, *, now: float | None = None) -> bool:
        """Release only the currently owned lease; stale owners are ignored."""
        request_key = str(request_id or "").strip()
        owner_value = str(owner or "").strip()
        try:
            fence_value = int(fence)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("snapshot lease fence is invalid") from exc
        if not request_key or not owner_value or fence_value < 1:
            return False
        self._snapshot_lease_epoch(now)
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(
                "UPDATE snapshot_requests SET lease_owner='',lease_expires_at=0,lease_updated_at=?,updated_at=? WHERE request_id=? AND lease_owner=? AND lease_fence=?",
                (now_text, now_text, request_key, owner_value, fence_value),
            )
            return cursor.rowcount > 0

    def save_snapshot_request_owned(
        self,
        request_id: str,
        requested_date: str,
        owner: str,
        fence: int,
        *,
        actual_trade_date: str | None = None,
        state: str = "pending",
        attempts: int | None = None,
        source: str = "",
        quality: str = "unknown",
        last_error: str | None = None,
        next_retry_at: str | None = None,
        terminal: bool = False,
        failure_kind: str = "",
        calendar_evidence: dict | None = None,
        provenance: dict | None = None,
        lease_ttl_seconds: float | None = None,
        now: float | None = None,
    ) -> dict:
        """Persist request state only for the live owner/fence pair."""
        request_key = str(request_id or "").strip()
        date_value = self._date_norm(requested_date)
        owner_value = str(owner or "").strip()
        try:
            fence_value = int(fence)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("snapshot lease fence is invalid") from exc
        if not request_key or not date_value or not owner_value or fence_value < 1:
            raise ValueError("snapshot lease identity is required")
        current = self._snapshot_lease_epoch(now)
        now_text = datetime.utcnow().isoformat()
        calendar_text = json.dumps(calendar_evidence or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        provenance_text = json.dumps(provenance or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        fields = {
            "actual_trade_date": self._date_norm(actual_trade_date) if actual_trade_date else None,
            "state": str(state or "pending")[:32],
            "attempts": max(0, int(attempts or 0)),
            "source": str(source or "")[:80],
            "quality": str(quality or "unknown")[:32],
            "last_error": str(last_error)[:500] if last_error is not None else None,
            "next_retry_at": str(next_retry_at)[:80] if next_retry_at is not None else None,
            "terminal": int(bool(terminal)),
            "failure_kind": str(failure_kind or "")[:64],
            "calendar_evidence_json": calendar_text,
            "provenance_json": provenance_text,
            "updated_at": now_text,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
            if not row:
                raise SnapshotLeaseLostError("snapshot lease request is missing")
            try:
                expiry = float(row["lease_expires_at"] or 0)
            except (TypeError, ValueError, OverflowError):
                expiry = 0.0
            if str(row["lease_owner"] or "") != owner_value or int(row["lease_fence"] or 0) != fence_value or expiry <= current:
                raise SnapshotLeaseLostError("snapshot lease owner or fence no longer matches")
            # This method is only the pre-fetch marker.  Classified retry or
            # terminal outcomes must use the CAS finalizer below, otherwise a
            # late cleanup could overwrite a durable result.
            if str(state or "").strip().lower() != "fetching" or str(row["state"] or "").strip().lower() != "fetching":
                return self._snapshot_lease_view(row, current, acquired=True, owner=owner_value, fence=fence_value)
            assignments = ",".join(f"{name}=?" for name in fields)
            values = list(fields.values())
            if lease_ttl_seconds is not None:
                new_expiry = max(expiry, current + self._snapshot_lease_ttl(lease_ttl_seconds))
                assignments += ",lease_expires_at=?,lease_updated_at=?"
                values.extend((new_expiry, now_text))
            values.append(request_key)
            cursor = db.execute(
                f"UPDATE snapshot_requests SET requested_date=?,{assignments} WHERE request_id=? AND lease_owner=? AND lease_fence=? AND lease_expires_at>?",
                [date_value, *values, owner_value, fence_value, current],
            )
            if cursor.rowcount <= 0:
                raise SnapshotLeaseLostError("snapshot lease update lost ownership")
            refreshed = db.execute("SELECT * FROM snapshot_requests WHERE request_id=?", (request_key,)).fetchone()
            return self._snapshot_lease_view(refreshed, current, acquired=True, owner=owner_value, fence=fence_value)

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

    def reopen_snapshot_request(self, request_id: str, *, expected_updated_at: str) -> bool:
        """Reopen obsolete completion without replacing a live owner's lease."""
        with self._connect() as db:
            cursor = db.execute(
                "UPDATE snapshot_requests SET state='pending',terminal=0,next_retry_at=NULL,"
                "calendar_evidence_json='{}',updated_at=? "
                "WHERE request_id=? AND updated_at=? AND "
                "(terminal=1 OR state IN ('complete','terminal')) AND "
                "(lease_owner='' OR lease_expires_at<=?)",
                (datetime.utcnow().isoformat(), request_id, expected_updated_at,
                 self._snapshot_lease_epoch(None)),
            )
            return cursor.rowcount == 1

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

    def terminalize_prior_snapshot_requests(self, current_date: str, *, reason: str, limit: int = 20, now=None) -> list[dict]:
        """Close unfinished prior-date requests so reload cannot fetch stale sessions."""
        current = self._automatic_delivery_clock(now)
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            keys = [str(row[0]) for row in db.execute(
                "SELECT request_id FROM snapshot_requests WHERE requested_date<? AND terminal=0 AND state NOT IN ('complete','terminal') "
                "ORDER BY requested_date,updated_at LIMIT ?",
                (str(current_date), max(1, min(int(limit), 100))),
            )]
            if not keys:
                return []
            placeholders = ",".join("?" for _ in keys)
            db.execute(
                f"UPDATE snapshot_requests SET state='terminal',terminal=1,next_retry_at=NULL,last_error=?,lease_owner='',lease_expires_at=0,lease_updated_at=?,updated_at=? "
                f"WHERE request_id IN ({placeholders})",
                (str(reason or "prior-date snapshot request expired")[:500], now_text, now_text, *keys),
            )
            return [dict(row) for row in db.execute(
                f"SELECT * FROM snapshot_requests WHERE request_id IN ({placeholders}) ORDER BY requested_date,updated_at",
                keys,
            )]

    def save_daily_bars(self, code: str, bars, source: str = "", price_basis: str | None = None) -> int:
        from .core import normalize_code

        normalized_code = normalize_code(code)
        if not re.fullmatch(r"\d{6}", normalized_code):
            return 0
        rows = []
        for item in bars or []:
            try:
                trade_date = self._date_norm(str(item.get("trade_date") or ""))
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", trade_date):
                    continue
                values = tuple(float(item.get(key) or 0) for key in ("open", "high", "low", "close", "volume", "amount"))
                if not all(math.isfinite(value) for value in values):
                    continue
                basis = str(item.get("price_basis") or price_basis or "unknown").strip().lower() or "unknown"
                factor = item.get("corporate_action_factor")
                factor = float(factor) if factor is not None else None
                if factor is not None and (not math.isfinite(factor) or factor <= 0):
                    continue
                evidence = str(item.get("corporate_action_evidence") or "")[:500]
                rows.append((normalized_code, trade_date, *values, str(item.get("source") or source or ""), datetime.utcnow().isoformat(), basis, factor, evidence))
            except (AttributeError, TypeError, ValueError, OverflowError):
                continue
        if not rows:
            return 0
        with self._connect() as db:
            db.executemany("INSERT OR REPLACE INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at,price_basis,corporate_action_factor,corporate_action_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
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
            placeholders = ",".join("?" for _ in values[:900])
            rows = db.execute(
                f"SELECT code,name FROM stock_symbols WHERE code IN ({placeholders}) AND name<>'' ORDER BY updated_at DESC",
                values[:900],
            ).fetchall()
            for row in rows:
                code, name = str(row[0]), str(row[1]).strip()
                if code not in result and name and name != code:
                    result[code] = name
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

    @staticmethod
    def _recommendation_prediction(plan: dict, *, candidate_price: float | None) -> dict:
        """Build an auditable range, never a probability or point forecast."""
        try:
            reference = float(plan.get("reference_price") or candidate_price or 0)
            atr = float(plan.get("atr") or 0)
            resistance = float(plan.get("resistance") or 0)
            invalidation = float(plan.get("invalidation") or 0)
            target_low = float(plan.get("sell_low") or 0)
            target_high = float(plan.get("sell_high") or 0)
        except (TypeError, ValueError, OverflowError):
            return {"status": "insufficient_data", "reason": "price_plan_invalid"}
        if not all(math.isfinite(value) for value in (reference, atr, resistance, invalidation, target_low, target_high)) or reference <= 0 or atr <= 0:
            return {"status": "insufficient_data", "reason": "atr_or_reference_missing"}
        # A resistance is a ceiling, not an excuse to manufacture more upside.
        ceilings = [value for value in (resistance, target_low, reference + atr) if value > reference]
        if not ceilings:
            return {"status": "insufficient_data", "reason": "resistance_or_target_invalid"}
        upper = min(ceilings)
        lower = max(reference, min(upper, reference + atr * 0.5))
        risk = reference - invalidation if invalidation > 0 else 0
        reward = upper - reference
        if risk <= 0 or reward <= 0 or reward / risk < 1:
            return {"status": "insufficient_data", "reason": "risk_reward_inadequate"}
        return {
            "status": "available",
            "scenario_gain_pct_range": [round((lower / reference - 1) * 100, 2), round((upper / reference - 1) * 100, 2)],
            "basis": ["ATR", "resistance", "risk_reward"],
            "coverage": "plan_only",
            "confidence": "low",
            "risk_reward": round(reward / risk, 2),
        }

    def _save_recommendations_in_tx(self, db, run_id: str, actual_date: str, source: str, candidates, *, origin: str = "global", visibility: str = "public", caller_identity: str = "system:daily_screen") -> None:
        created_at = datetime.utcnow().isoformat()
        for candidate in candidates:
            plan_obj = getattr(candidate, "price_plan", None)
            quote = getattr(candidate, "quote", None)
            if quote is None:
                continue
            plan = {key: getattr(plan_obj, key) for key in plan_obj.__dataclass_fields__} if plan_obj is not None else {}
            provenance = plan.get("provenance") if isinstance(plan.get("provenance"), dict) else {}
            basis = str(provenance.get("basis") or "unknown").strip().lower()
            comparability = provenance.get("corporate_action_evidence") if isinstance(provenance.get("corporate_action_evidence"), dict) else {}
            comparable = comparability.get("comparable") is True and comparability.get("factor") == 1
            # M2 plan validity is independent from M3 corporate-action
            # comparability. Missing M3 evidence must fail outcome evaluation,
            # but cannot rewrite a valid close plan as invalid.
            plan_valid = bool(plan.get("validated")) and basis == "unadjusted"
            plan_reason = "" if plan_valid else ("price_plan_missing" if not plan else "price_plan_or_basis_unverified")
            comparability_status = "comparable" if comparable else "unknown"
            try:
                candidate_price = float(getattr(quote, "price", 0) or 0)
            except (TypeError, ValueError, OverflowError):
                candidate_price = None
            prediction = self._recommendation_prediction(plan, candidate_price=candidate_price) if plan_valid else {"status": "insufficient_data", "reason": plan_reason}
            overlay = getattr(candidate, "factor_overlay", None)
            regime = str(getattr(overlay, "market_regime", "unknown") or "unknown").strip().lower()
            # This is intentionally not a full stock-selection strategy hash:
            # it only groups price-plan configuration for comparable review.
            strategy_version = "price-plan-config-v1:" + hashlib.sha256(json.dumps({"price_plan_algorithm": 1, "tolerance_pct": provenance.get("tolerance_pct"), "basis": basis}, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:16]
            plan_version = "plan:" + hashlib.sha256(json.dumps(plan, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
            recommendation_id = "rec:" + hashlib.sha256(f"{run_id}|{getattr(quote, 'code', '')}".encode("utf-8")).hexdigest()[:32]
            timestamp = str(provenance.get("as_of") or provenance.get("last_date") or actual_date)
            freshness = "verified_close" if timestamp == actual_date else "unknown"
            db.execute(
                "INSERT OR IGNORE INTO recommendation_records(recommendation_id,run_id,recommended_date,code,name,candidate_price,confirmation_price,attention_low,attention_high,invalidation_price,confirmation_level,target_low,target_high,plan_version,market_regime,data_timestamp,freshness,source,caller_identity,price_basis,prediction_json,created_at,origin,visibility,strategy_version,plan_status,plan_reason,comparability_status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (recommendation_id, run_id, actual_date, str(getattr(quote, "code", "")), str(getattr(quote, "name", "")), candidate_price,
                 plan.get("reference_price"), plan.get("attention_low"), plan.get("attention_high"), plan.get("invalidation"), plan.get("confirmation"), plan.get("sell_low"), plan.get("sell_high"),
                 plan_version, regime, timestamp, freshness, str(source or ""), caller_identity, basis,
                 json.dumps(prediction, ensure_ascii=False, sort_keys=True, separators=(",", ":")), created_at, str(origin or "global"), str(visibility or "public"), strategy_version, "validated" if plan_valid else "unknown", plan_reason, comparability_status),
            )

    @staticmethod
    def _outcome_order(days: list[dict], level: float | None, invalidation: float | None, key: str) -> tuple[str, str]:
        if not level or level <= 0:
            return "not_applicable", ""
        for item in days:
            hit = float(item["high"]) >= level if key != "invalidation" else float(item["low"]) <= level
            if not hit:
                continue
            return "touched", str(item["trade_date"])
        return "not_touched", ""

    @staticmethod
    def _recommendation_bar_is_usable(bar: dict, basis: str) -> bool:
        try:
            values = [float(bar[key]) for key in ("open", "high", "low", "close", "volume", "amount")]
            if not all(math.isfinite(value) and value > 0 for value in values):
                return False
            open_, high, low, close, _volume, _amount = values
            if high < low or high < max(open_, close) or low > min(open_, close):
                return False
        except (KeyError, TypeError, ValueError, OverflowError):
            return False
        try:
            factor = float(bar.get("corporate_action_factor"))
        except (TypeError, ValueError, OverflowError):
            return False
        return str(bar.get("price_basis") or "").lower() == basis == "unadjusted" and factor == 1 and bool(str(bar.get("corporate_action_evidence") or "").strip())

    @staticmethod
    def _calendar_window(db, start: str, cutoff: str) -> tuple[list[str], str | None]:
        cursor = datetime.strptime(start, "%Y-%m-%d").date() + timedelta(days=1)
        end = datetime.strptime(cutoff, "%Y-%m-%d").date()
        open_days: list[str] = []
        while cursor <= end:
            value = cursor.isoformat()
            row = db.execute("SELECT is_open,status FROM trading_calendar WHERE trade_date=?", (value,)).fetchone()
            if not row or str(row["status"] or "").lower() not in {"open", "closed"}:
                return open_days, "calendar_window_unverified"
            if str(row["status"]).lower() == "open" and int(row["is_open"] or 0) == 1:
                open_days.append(value)
            cursor += timedelta(days=1)
        return open_days, None

    def evaluate_recommendation_outcomes(self, *, as_of: str | datetime | None = None, horizons=(1, 3, 5, 10)) -> dict:
        """Evaluate only bars known on or before ``as_of`` using saved calendar rows."""
        china_tz = timezone(timedelta(hours=8))
        if isinstance(as_of, datetime):
            current = as_of.replace(tzinfo=china_tz) if as_of.tzinfo is None else as_of.astimezone(china_tz)
            cutoff = current.date().isoformat()
        elif as_of is None:
            current = datetime.now(china_tz)
            cutoff = current.date().isoformat()
        else:
            current = None
            cutoff = self._date_norm(as_of)
        result = {"evaluated": 0, "complete": 0, "pending": 0, "unknown": 0, "unknown_order": 0}
        with self._connect() as db:
            rows = [dict(row) for row in db.execute("SELECT * FROM recommendation_records ORDER BY recommended_date,recommendation_id")]
            for record in rows:
                known_open, calendar_reason = self._calendar_window(db, record["recommended_date"], cutoff)
                for horizon in horizons:
                    horizon = int(horizon)
                    if str(record.get("plan_status")) != "validated":
                        self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, "unknown", reason=str(record.get("plan_reason") or "price_plan_unverified"), sample_complete=False)
                        result["unknown"] += 1
                        continue
                    if str(record.get("comparability_status") or "unknown") != "comparable":
                        self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, "unknown", reason="corporate_action_evidence_missing", sample_complete=False)
                        result["unknown"] += 1
                        continue
                    if len(known_open) < horizon:
                        self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, "pending", reason=calendar_reason or "observation_window_not_mature", sample_complete=False)
                        result["pending"] += 1
                        continue
                    dates = known_open[:horizon]
                    if current and dates[-1] == cutoff and current.time() < datetime.strptime("15:00", "%H:%M").time():
                        self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, "pending", reason="session_not_complete", sample_complete=False)
                        result["pending"] += 1
                        continue
                    complete = [db.execute("SELECT complete FROM daily_snapshot_meta WHERE trade_date=?", (day,)).fetchone() for day in dates]
                    if any(not item or int(item["complete"] or 0) != 1 for item in complete):
                        self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, "pending", reason="completed_session_evidence_missing", sample_complete=False)
                        result["pending"] += 1
                        continue
                    placeholders = ",".join("?" for _ in dates)
                    bars = [dict(row) for row in db.execute(f"SELECT * FROM daily_bars WHERE code=? AND trade_date IN ({placeholders}) ORDER BY trade_date", (record["code"], *dates))]
                    if len(bars) != horizon or any(not self._recommendation_bar_is_usable(bar, str(record.get("price_basis") or "unknown")) for bar in bars):
                        self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, "unknown", reason="missing_suspended_or_price_not_comparable", sample_complete=False)
                        result["unknown"] += 1
                        continue
                    base = float(record.get("confirmation_price") or record.get("candidate_price") or 0)
                    if not math.isfinite(base) or base <= 0:
                        self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, "unknown", reason="reference_price_invalid", sample_complete=False)
                        result["unknown"] += 1
                        continue
                    invalidation = float(record.get("invalidation_price") or 0)
                    confirmation, confirmation_date = self._outcome_order(bars, float(record.get("confirmation_level") or 0), invalidation, "confirmation")
                    target, target_date = self._outcome_order(bars, float(record.get("target_low") or 0), invalidation, "target")
                    invalidated, invalidated_date = self._outcome_order(bars, invalidation, None, "invalidation")
                    target_vs_invalidation = "not_touched"
                    if target == "touched" and invalidated == "touched":
                        target_vs_invalidation = "unknown_order" if target_date == invalidated_date else ("target_before_invalidation" if target_date < invalidated_date else "invalidation_before_target")
                    elif target == "touched":
                        target_vs_invalidation = "target_before_invalidation"
                    elif invalidated == "touched":
                        target_vs_invalidation = "invalidation_before_target"
                    order_unknown = target_vs_invalidation == "unknown_order"
                    status = "unknown_order" if order_unknown else "complete"
                    first = min((date for date in (confirmation_date, target_date, invalidated_date) if date), default="")
                    closes = [float(bar["close"]) for bar in bars]
                    peak = base; drawdown = 0.0
                    for close in closes:
                        peak = max(peak, close)
                        drawdown = min(drawdown, (close / peak - 1) * 100)
                    self._upsert_recommendation_outcome(db, record["recommendation_id"], horizon, cutoff, status,
                        close_price=float(bars[-1]["close"]), return_pct=(float(bars[-1]["close"]) / base - 1) * 100,
                        max_gain_pct=max((float(bar["high"]) / base - 1) * 100 for bar in bars), max_drawdown_pct=drawdown,
                        confirmation_order=confirmation, target_order=target, invalidation_order=invalidated, first_touch=first, reason="daily_bar_order_unprovable" if order_unknown else "", sample_complete=not order_unknown, session_complete=True, price_basis=str(record.get("price_basis") or "unknown"), event_order=target_vs_invalidation)
                    result[status] += 1
                    result["evaluated"] += 1
        return result

    @staticmethod
    def _upsert_recommendation_outcome(db, recommendation_id: str, horizon: int, evaluated_through: str, status: str, *, close_price=None, return_pct=None, max_gain_pct=None, max_drawdown_pct=None, confirmation_order="not_touched", target_order="not_touched", invalidation_order="not_touched", first_touch="", reason="", sample_complete=False, session_complete=False, price_basis="unknown", event_order="not_touched") -> None:
        db.execute(
            "INSERT INTO recommendation_outcomes(recommendation_id,horizon,evaluated_through,status,close_price,return_pct,max_gain_pct,max_drawdown_pct,confirmation_order,target_order,invalidation_order,first_touch,reason,sample_complete,updated_at,session_complete,price_basis,event_order) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(recommendation_id,horizon) DO UPDATE SET evaluated_through=excluded.evaluated_through,status=excluded.status,close_price=excluded.close_price,return_pct=excluded.return_pct,max_gain_pct=excluded.max_gain_pct,max_drawdown_pct=excluded.max_drawdown_pct,confirmation_order=excluded.confirmation_order,target_order=excluded.target_order,invalidation_order=excluded.invalidation_order,first_touch=excluded.first_touch,reason=excluded.reason,sample_complete=excluded.sample_complete,updated_at=excluded.updated_at,session_complete=excluded.session_complete,price_basis=excluded.price_basis,event_order=excluded.event_order",
            (recommendation_id, horizon, evaluated_through, status, close_price, return_pct, max_gain_pct, max_drawdown_pct, confirmation_order, target_order, invalidation_order, first_touch, reason, int(bool(sample_complete)), datetime.utcnow().isoformat(), int(bool(session_complete)), price_basis, event_order),
        )

    def recommendation_reviews(self, origin: str = "", limit: int = 20) -> list[dict]:
        with self._connect() as db:
            rows = db.execute("SELECT r.*,o.horizon,o.status,o.return_pct,o.max_gain_pct,o.max_drawdown_pct,o.confirmation_order,o.target_order,o.invalidation_order,o.event_order,o.first_touch,o.reason FROM recommendation_records r LEFT JOIN recommendation_outcomes o ON o.recommendation_id=r.recommendation_id AND o.horizon=5 WHERE r.visibility='public' OR r.origin=? ORDER BY r.recommended_date DESC,r.code LIMIT ?", (str(origin or ""), max(1, min(int(limit), 100)),)).fetchall()
            return [dict(row) for row in rows]

    def recommendation_performance(self, horizon: int = 5, origin: str = "") -> list[dict]:
        with self._connect() as db:
            rows = [dict(row) for row in db.execute("SELECT r.strategy_version,r.market_regime,o.* FROM recommendation_records r LEFT JOIN recommendation_outcomes o ON o.recommendation_id=r.recommendation_id AND o.horizon=? WHERE r.visibility='public' OR r.origin=? ORDER BY r.strategy_version,r.market_regime", (max(1, min(int(horizon), 10)), str(origin or "")))]
        groups: dict[tuple[str, str], list[dict]] = {}
        for row in rows:
            groups.setdefault((str(row["strategy_version"]), str(row["market_regime"])), []).append(row)
        result = []
        for (version, regime), items in groups.items():
            mature = [item for item in items if item.get("status") in {"complete", "unknown_order"}]
            # A same-day competing touch makes path order unknown, not the
            # close, MFE, or close-to-close drawdown unknown. Keep those
            # price-complete observations in their own denominator.
            price_evaluable = [item for item in mature if item.get("return_pct") is not None]
            order_evaluable = [item for item in items if item.get("status") == "complete"]
            returns = sorted(float(item["return_pct"]) for item in price_evaluable)
            gains = sorted(float(item["max_gain_pct"]) for item in price_evaluable if item.get("max_gain_pct") is not None)
            drawdowns = [float(item["max_drawdown_pct"]) for item in price_evaluable if item.get("max_drawdown_pct") is not None]
            median = (returns[(len(returns)-1)//2] + returns[len(returns)//2]) / 2 if returns else None
            median_gain = (gains[(len(gains)-1)//2] + gains[len(gains)//2]) / 2 if gains else None
            invalidation_count = sum(item.get("invalidation_order") == "touched" for item in order_evaluable)
            target_count = sum(item.get("target_order") == "touched" for item in order_evaluable)
            result.append({"strategy_version": version, "market_regime": regime, "sample_count": len(items), "mature_count": len(mature), "evaluable_count": len(price_evaluable), "price_evaluable_count": len(price_evaluable), "order_evaluable_count": len(order_evaluable), "pending_count": sum(item.get("status") == "pending" or item.get("status") is None for item in items), "unknown_count": sum(item.get("status") == "unknown" for item in items), "unknown_order_count": sum(item.get("status") == "unknown_order" for item in items), "positive_return_rate": (sum(value > 0 for value in returns) / len(returns)) if returns else None, "target_hit_rate": (target_count / len(order_evaluable)) if order_evaluable else None, "invalidation_count": invalidation_count, "invalidation_eligible_count": len(order_evaluable), "invalidation_rate": (invalidation_count / len(order_evaluable)) if order_evaluable else None, "median_return_pct": median, "median_max_gain_pct": median_gain, "return_distribution": {"min": returns[0], "max": returns[-1]} if returns else {}, "max_drawdown_pct": min(drawdowns) if drawdowns else None})
        return result

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
        publication_key: str = "",
        publication_payload: str = "",
        publication_invocation_id: str = "",
        publication_origins=None,
    ) -> dict:
        run_id = str(run_args[0])
        import json
        values = list(run_args)
        if len(values) < 12:
            raise ValueError("screen run arguments require the v7 twelve-field tuple")
        requested_date = self._canonical_raw_date(values[2])
        actual_date = self._canonical_raw_date(values[3]) if values[3] else None
        today = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
        if not requested_date or not actual_date:
            raise ValueError("screen run dates must be canonical")
        if requested_date > today or actual_date > today or actual_date > requested_date:
            raise ValueError("screen run dates violate requested/actual bounds")
        values[2], values[3] = requested_date, actual_date
        status, quality, error = str(values[9]), str(values[10]), values[11]
        coverage_value = max(0.0, min(1.0, float(coverage)))
        if isinstance(diagnostics, dict):
            diagnostics = json.dumps(diagnostics, ensure_ascii=False, default=str)
        diagnostics = str(diagnostics or "{}")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            publication_key = str(publication_key or "").strip()
            if publication_key:
                existing = db.execute(
                    "SELECT run_id FROM automatic_close_publications WHERE publication_key=?",
                    (publication_key,),
                ).fetchone()
                if existing:
                    return {
                        "run_id": str(existing["run_id"]),
                        "report_claimed": True,
                        "report_version": 0,
                        "idempotent": True,
                    }
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
                # The recommendation snapshot is created in the same
                # transaction as the immutable candidate; a later review can
                # therefore never silently use a replaced plan.
                recommendation_origin = str(scope or "global")
                visibility = "public" if recommendation_origin == "global" else "private"
                self._save_recommendations_in_tx(db, run_id, actual_date, str(values[4] or ""), candidates, origin=recommendation_origin, visibility=visibility, caller_identity=f"screen:{recommendation_origin}")
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
            if publication_key and report_claimed:
                payload = str(publication_payload or "")
                invocation = str(publication_invocation_id or "").strip()
                if not payload or not invocation:
                    raise ValueError("automatic close publication requires payload and invocation")
                origins = sorted({str(value).strip() for value in (publication_origins or []) if str(value).strip()})
                now_text = datetime.utcnow().isoformat()
                db.execute(
                    "INSERT INTO automatic_close_publications(publication_key,actual_trade_date,requested_date,run_id,invocation_id,payload,payload_hash,origins_json,outbox_prepared,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,0,?,?)",
                    (
                        publication_key,
                        actual_date,
                        requested_date,
                        run_id,
                        invocation,
                        payload,
                        hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                        json.dumps(origins, ensure_ascii=False, separators=(",", ":")),
                        now_text,
                        now_text,
                    ),
                )
        return {"run_id": run_id, "report_claimed": report_claimed, "report_version": allocated_version, "idempotent": False}

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

    def automatic_close_publication(self, publication_key: str) -> dict | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT * FROM automatic_close_publications WHERE publication_key=?",
                (str(publication_key or ""),),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        try:
            origins = json.loads(str(result.get("origins_json") or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            origins = []
        result["origins"] = [str(value) for value in origins if str(value).strip()] if isinstance(origins, list) else []
        return result

    @staticmethod
    def _automatic_delivery_clock(now=None) -> float:
        if now is None:
            return time.time()
        if isinstance(now, datetime):
            value = now
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.timestamp()
        value = float(now)
        if not math.isfinite(value) or value < 0:
            raise ValueError("automatic delivery clock is invalid")
        return value

    def prepare_automatic_close_deliveries(self, publication_key: str) -> list[dict]:
        key = str(publication_key or "").strip()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            publication = db.execute(
                "SELECT * FROM automatic_close_publications WHERE publication_key=?",
                (key,),
            ).fetchone()
            if not publication:
                raise KeyError(f"unknown automatic close publication {key}")
            try:
                origins = json.loads(str(publication["origins_json"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("automatic close publication destinations are invalid") from exc
            if not isinstance(origins, list):
                raise RuntimeError("automatic close publication destinations are invalid")
            now_text = datetime.utcnow().isoformat()
            for origin in sorted({str(value).strip() for value in origins if str(value).strip()}):
                delivery_id = hashlib.sha256(f"{key}\0{origin}".encode("utf-8")).hexdigest()
                db.execute(
                    "INSERT OR IGNORE INTO automatic_close_deliveries(delivery_id,publication_key,actual_trade_date,origin,run_id,payload,payload_hash,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',?,?)",
                    (
                        delivery_id,
                        key,
                        str(publication["actual_trade_date"]),
                        origin,
                        str(publication["run_id"]),
                        str(publication["payload"]),
                        str(publication["payload_hash"]),
                        now_text,
                        now_text,
                    ),
                )
            db.execute(
                "UPDATE automatic_close_publications SET outbox_prepared=1,updated_at=? WHERE publication_key=?",
                (now_text, key),
            )
            return [dict(row) for row in db.execute(
                "SELECT * FROM automatic_close_deliveries WHERE publication_key=? ORDER BY origin",
                (key,),
            )]

    def prepare_all_automatic_close_deliveries(self, *, limit: int = 20) -> int:
        with self._connect() as db:
            keys = [str(row[0]) for row in db.execute(
                "SELECT publication_key FROM automatic_close_publications WHERE outbox_prepared=0 ORDER BY created_at LIMIT ?",
                (max(1, min(int(limit), 100)),),
            )]
        for key in keys:
            self.prepare_automatic_close_deliveries(key)
        return len(keys)

    def recoverable_automatic_close_deliveries(
        self,
        *,
        now=None,
        limit: int = 100,
        max_attempts: int = 5,
        retry_window_seconds: float = 3600,
    ) -> list[dict]:
        current = self._automatic_delivery_clock(now)
        cutoff = datetime.fromtimestamp(current - max(60.0, float(retry_window_seconds)), timezone.utc).replace(tzinfo=None).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE automatic_close_deliveries SET state='unknown_delivery',lease_owner='',lease_expires_at=0,last_error=CASE WHEN last_error='' THEN 'delivery outcome unknown after sender interruption' ELSE last_error END,updated_at=? "
                "WHERE state='sending' AND lease_expires_at<=?",
                (datetime.utcnow().isoformat(), current),
            )
            db.execute(
                "UPDATE automatic_close_deliveries SET state='cancelled',next_retry_at=0,last_error=CASE WHEN last_error='' THEN 'confirmed send failures exhausted retry bounds' ELSE last_error || '; retry bounds exhausted' END,updated_at=? "
                "WHERE state='failed' AND (attempts>=? OR created_at<=?)",
                (datetime.utcnow().isoformat(), max(1, int(max_attempts)), cutoff),
            )
            rows = db.execute(
                "SELECT * FROM automatic_close_deliveries WHERE state='pending' OR (state='failed' AND next_retry_at<=?) ORDER BY created_at,origin LIMIT ?",
                (current, max(1, min(int(limit), 1000))),
            ).fetchall()
            return [dict(row) for row in rows]

    def claim_automatic_close_delivery(self, delivery_id: str, owner: str, *, ttl_seconds: float = 120, now=None) -> dict:
        current = self._automatic_delivery_clock(now)
        ttl = max(5.0, min(float(ttl_seconds), 3600.0))
        owner_value = str(owner or "").strip()[:160]
        if not owner_value:
            raise ValueError("automatic delivery owner is required")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM automatic_close_deliveries WHERE delivery_id=?", (str(delivery_id),)).fetchone()
            if not row:
                raise KeyError(f"unknown automatic close delivery {delivery_id}")
            state = str(row["state"])
            expiry = float(row["lease_expires_at"] or 0)
            if state == "sending" and expiry <= current:
                db.execute(
                    "UPDATE automatic_close_deliveries SET state='unknown_delivery',lease_owner='',lease_expires_at=0,last_error=CASE WHEN last_error='' THEN 'delivery outcome unknown after sender interruption' ELSE last_error END,updated_at=? WHERE delivery_id=?",
                    (datetime.utcnow().isoformat(), str(delivery_id)),
                )
                row = db.execute("SELECT * FROM automatic_close_deliveries WHERE delivery_id=?", (str(delivery_id),)).fetchone()
                return {**dict(row), "acquired": False, "reason": "unknown_delivery"}
            if state in {"sent", "unknown_delivery", "cancelled"}:
                return {**dict(row), "acquired": False, "reason": state}
            if state == "sending" and expiry > current:
                return {**dict(row), "acquired": False, "reason": "contended"}
            if state == "failed" and float(row["next_retry_at"] or 0) > current:
                return {**dict(row), "acquired": False, "reason": "backoff"}
            fence = max(0, int(row["lease_fence"] or 0)) + 1
            db.execute(
                "UPDATE automatic_close_deliveries SET state='sending',attempts=attempts+1,lease_owner=?,lease_fence=?,lease_expires_at=?,updated_at=? WHERE delivery_id=? AND state IN ('pending','failed')",
                (owner_value, fence, current + ttl, datetime.utcnow().isoformat(), str(delivery_id)),
            )
            refreshed = db.execute("SELECT * FROM automatic_close_deliveries WHERE delivery_id=?", (str(delivery_id),)).fetchone()
            return {**dict(refreshed), "acquired": True, "owner": owner_value, "fence": fence}

    def finish_automatic_close_delivery(
        self,
        delivery_id: str,
        owner: str,
        fence: int,
        *,
        sent: bool,
        error: str = "",
        retry_after_seconds: float = 60,
        max_attempts: int = 5,
        retry_window_seconds: float = 3600,
        now=None,
    ) -> dict:
        current = self._automatic_delivery_clock(now)
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current_row = db.execute(
                "SELECT attempts,created_at FROM automatic_close_deliveries WHERE delivery_id=?",
                (str(delivery_id),),
            ).fetchone()
            if not current_row:
                raise KeyError(f"unknown automatic close delivery {delivery_id}")
            exhausted = False
            if not sent:
                try:
                    created = datetime.fromisoformat(str(current_row["created_at"])).replace(tzinfo=timezone.utc).timestamp()
                except (TypeError, ValueError):
                    created = current
                exhausted = int(current_row["attempts"] or 0) >= max(1, int(max_attempts)) or current - created >= max(60.0, float(retry_window_seconds))
            state = "sent" if sent else ("cancelled" if exhausted else "failed")
            retry_at = 0.0 if sent or exhausted else current + max(1.0, min(float(retry_after_seconds), 3600.0))
            final_error = str(error or "")[:500]
            if exhausted:
                final_error = (final_error + "; retry bounds exhausted").strip("; ")[:500]
            changed = db.execute(
                "UPDATE automatic_close_deliveries SET state=?,lease_owner='',lease_expires_at=0,next_retry_at=?,last_error=?,updated_at=?,sent_at=? "
                "WHERE delivery_id=? AND state='sending' AND lease_owner=? AND lease_fence=? AND lease_expires_at>?",
                (
                    state,
                    retry_at,
                    final_error,
                    now_text,
                    now_text if sent else None,
                    str(delivery_id),
                    str(owner or ""),
                    int(fence),
                    current,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("automatic delivery acknowledgement lost ownership")
            row = db.execute("SELECT * FROM automatic_close_deliveries WHERE delivery_id=?", (str(delivery_id),)).fetchone()
            return dict(row)

    def mark_automatic_close_delivery_unknown(self, delivery_id: str, owner: str, fence: int, *, error: str, now=None) -> dict:
        current = self._automatic_delivery_clock(now)
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                "UPDATE automatic_close_deliveries SET state='unknown_delivery',lease_owner='',lease_expires_at=0,next_retry_at=0,last_error=?,updated_at=? "
                "WHERE delivery_id=? AND state='sending' AND lease_owner=? AND lease_fence=? AND lease_expires_at>?",
                (str(error or "delivery outcome unknown")[:500], now_text, str(delivery_id), str(owner or ""), int(fence), current),
            ).rowcount
            if changed != 1:
                raise RuntimeError("automatic unknown-delivery marker lost ownership")
            row = db.execute("SELECT * FROM automatic_close_deliveries WHERE delivery_id=?", (str(delivery_id),)).fetchone()
            return dict(row)

    def cancel_automatic_close_delivery(self, delivery_id: str, reason: str) -> bool:
        with self._connect() as db:
            changed = db.execute(
                "UPDATE automatic_close_deliveries SET state='cancelled',lease_owner='',lease_expires_at=0,last_error=?,updated_at=? WHERE delivery_id=? AND state IN ('pending','failed')",
                (str(reason or "cancelled")[:500], datetime.utcnow().isoformat(), str(delivery_id)),
            ).rowcount
            return changed == 1

    def automatic_close_delivery_summary(self, publication_key: str | None = None) -> dict[str, int]:
        with self._connect() as db:
            if publication_key:
                rows = db.execute(
                    "SELECT state,COUNT(*) FROM automatic_close_deliveries WHERE publication_key=? GROUP BY state",
                    (str(publication_key),),
                ).fetchall()
            else:
                rows = db.execute("SELECT state,COUNT(*) FROM automatic_close_deliveries GROUP BY state").fetchall()
        result = {state: 0 for state in ("pending", "sending", "sent", "failed", "unknown_delivery", "cancelled")}
        result.update({str(row[0]): int(row[1]) for row in rows})
        return result

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

    def latest_screen_candidates_for_intraday(self, limit: int = 30, scope: str = "global") -> list[dict]:
        """Read the active candidate run even when its plan is expired.

        Expired rows are returned only for a durable invalidation notice; the
        caller must still reject them as opportunity targets.
        """
        with self._connect() as db:
            active = db.execute("SELECT * FROM active_candidate_runs WHERE scope=?", (scope or "global",)).fetchone()
            if not active:
                return []
            rows = db.execute(
                "SELECT c.*,r.actual_trade_date,r.source,r.quality,r.status,r.coverage,a.valid_until FROM screen_candidates c "
                "JOIN screen_runs r ON r.run_id=c.run_id JOIN active_candidate_runs a ON a.run_id=c.run_id AND a.scope=? "
                "WHERE c.run_id=? ORDER BY c.score DESC,c.code ASC LIMIT ?",
                (scope or "global", str(active["run_id"]), max(1, min(int(limit), 100))),
            ).fetchall()
            return [dict(row) for row in rows]

    def current_intraday_candidate(self, code: str, scope: str = "global") -> dict | None:
        """Return the currently published candidate for one code, if any.

        Delivery uses this narrow lookup immediately before sending a queued
        opportunity notification.  It deliberately does not treat a historic
        candidate row as current merely because its outbox row is recoverable.
        """
        code_value = str(code or "").strip()
        if not code_value:
            return None
        with self._connect() as db:
            row = db.execute(
                "SELECT c.*,r.actual_trade_date,r.source,r.quality,r.status,r.coverage,a.valid_until FROM screen_candidates c "
                "JOIN screen_runs r ON r.run_id=c.run_id JOIN active_candidate_runs a ON a.run_id=c.run_id AND a.scope=? "
                "WHERE c.run_id=a.run_id AND c.code=? LIMIT 1",
                (scope or "global", code_value),
            ).fetchone()
            return dict(row) if row else None

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

    def job_run(self, job_key: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM job_runs WHERE job_key=?", (str(job_key or ""),)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _automatic_job_time(value: str | None, fallback: float) -> float:
        try:
            parsed = datetime.fromisoformat(str(value or ""))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except (TypeError, ValueError):
            return fallback

    def claim_automatic_close_job(
        self,
        job_key: str,
        trade_date: str,
        *,
        lease_seconds: int = 900,
        max_attempts: int = 6,
        retry_window_seconds: int = 14400,
        now=None,
    ) -> dict:
        current = self._automatic_delivery_clock(now)
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM job_runs WHERE job_key=?", (str(job_key),)).fetchone()
            if not row:
                db.execute(
                    "INSERT INTO job_runs(job_key,job_name,trade_date,started_at,status,automatic_attempts,automatic_first_started_at,automatic_next_retry_at) VALUES(?,?,?,?,?,?,?,0)",
                    (str(job_key), "automatic_close", str(trade_date), now_text, "running", 1, now_text),
                )
                value = db.execute("SELECT * FROM job_runs WHERE job_key=?", (str(job_key),)).fetchone()
                return {**dict(value), "acquired": True, "reason": "started"}
            value = dict(row)
            status = str(value.get("status") or "")
            if status in {"completed", "missed", "cancelled"}:
                return {**value, "acquired": False, "reason": status}
            started = self._automatic_job_time(value.get("started_at"), current)
            if status == "running" and current - started < max(60, int(lease_seconds)):
                return {**value, "acquired": False, "reason": "contended"}
            if status == "failed" and float(value.get("automatic_next_retry_at") or 0) > current:
                return {**value, "acquired": False, "reason": "backoff"}
            attempts = max(0, int(value.get("automatic_attempts") or 0))
            first_started = self._automatic_job_time(value.get("automatic_first_started_at") or value.get("started_at"), current)
            if attempts >= max(1, int(max_attempts)) or current - first_started >= max(300, int(retry_window_seconds)):
                reason = "automatic close retry bounds exhausted"
                db.execute(
                    "UPDATE job_runs SET finished_at=?,status='missed',error=?,automatic_terminal_reason=?,automatic_next_retry_at=0 WHERE job_key=?",
                    (now_text, reason, reason, str(job_key)),
                )
                terminal = db.execute("SELECT * FROM job_runs WHERE job_key=?", (str(job_key),)).fetchone()
                return {**dict(terminal), "acquired": False, "reason": "missed"}
            db.execute(
                "UPDATE job_runs SET started_at=?,finished_at=NULL,status='running',error=NULL,automatic_attempts=?,automatic_first_started_at=COALESCE(automatic_first_started_at,?),automatic_next_retry_at=0 WHERE job_key=?",
                (now_text, attempts + 1, now_text, str(job_key)),
            )
            claimed = db.execute("SELECT * FROM job_runs WHERE job_key=?", (str(job_key),)).fetchone()
            return {**dict(claimed), "acquired": True, "reason": "resumed"}

    def finish_automatic_close_job(
        self,
        job_key: str,
        *,
        status: str,
        error: str | None = None,
        retry_after_seconds: int = 300,
        max_attempts: int = 6,
        retry_window_seconds: int = 14400,
        now=None,
    ) -> dict:
        current = self._automatic_delivery_clock(now)
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM job_runs WHERE job_key=?", (str(job_key),)).fetchone()
            if not row:
                raise KeyError(f"unknown automatic close job {job_key}")
            value = dict(row)
            final_status = str(status or "failed")
            terminal_reason = ""
            next_retry = 0.0
            if final_status == "failed":
                attempts = max(0, int(value.get("automatic_attempts") or 0))
                first_started = self._automatic_job_time(value.get("automatic_first_started_at") or value.get("started_at"), current)
                if attempts >= max(1, int(max_attempts)) or current - first_started >= max(300, int(retry_window_seconds)):
                    final_status = "missed"
                    terminal_reason = "automatic close retry bounds exhausted"
                else:
                    next_retry = current + max(30, min(int(retry_after_seconds), 3600))
            db.execute(
                "UPDATE job_runs SET finished_at=?,status=?,error=?,automatic_next_retry_at=?,automatic_terminal_reason=? WHERE job_key=?",
                (now_text, final_status, error, next_retry, terminal_reason, str(job_key)),
            )
            finished = db.execute("SELECT * FROM job_runs WHERE job_key=?", (str(job_key),)).fetchone()
            return dict(finished)

    def terminalize_prior_automatic_close_jobs(self, current_date: str, *, reason: str, limit: int = 20, now=None) -> list[dict]:
        current = self._automatic_delivery_clock(now)
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            keys = [str(row[0]) for row in db.execute(
                "SELECT job_key FROM job_runs WHERE job_name='automatic_close' AND trade_date<? AND status IN ('running','failed') ORDER BY trade_date,started_at LIMIT ?",
                (str(current_date), max(1, min(int(limit), 100))),
            )]
            if not keys:
                return []
            placeholders = ",".join("?" for _ in keys)
            db.execute(
                f"UPDATE job_runs SET finished_at=?,status='missed',error=?,automatic_terminal_reason=?,automatic_next_retry_at=0 WHERE job_key IN ({placeholders})",
                (now_text, str(reason)[:500], str(reason)[:500], *keys),
            )
            return [dict(row) for row in db.execute(
                f"SELECT * FROM job_runs WHERE job_key IN ({placeholders}) ORDER BY trade_date,started_at",
                keys,
            )]

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
            if isinstance(is_open, str) and is_open.strip().lower() in {"open", "closed", "unknown"}:
                status = is_open.strip().lower()
            elif str(is_open).strip().lower() in {"1", "true"}:
                status = "open"
            elif str(is_open).strip().lower() in {"0", "false"}:
                status = "closed"
            else:
                status = "unknown"
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

    def save_result_evaluation(
        self,
        evaluation_id: str,
        run_id: str,
        code: str,
        as_of: str,
        horizon: int,
        status: str,
        close: float | None,
        return_pct: float | None,
        mfe_pct: float | None,
        mae_pct: float | None,
        first_touch: str | None,
        sample_complete: bool,
        price_basis: str = "unknown",
        plan_validated: bool = False,
        evaluation_dataset_id: str | None = None,
        evaluation_batch_id: str | None = None,
        evaluation_generation: int | None = None,
    ) -> None:
        generation = None
        if evaluation_generation is not None:
            try:
                parsed_generation = int(evaluation_generation)
                generation = parsed_generation if parsed_generation > 0 else None
            except (TypeError, ValueError, OverflowError):
                generation = None
        with self._connect() as db:
            db.execute(
                "INSERT INTO result_evaluations(evaluation_id,run_id,code,as_of,horizon,status,close,return_pct,mfe_pct,mae_pct,first_touch,sample_complete,created_at,price_basis,plan_validated,evaluation_dataset_id,evaluation_batch_id,evaluation_generation) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(evaluation_id) DO UPDATE SET status=excluded.status,close=excluded.close,return_pct=excluded.return_pct,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,first_touch=excluded.first_touch,sample_complete=excluded.sample_complete,price_basis=excluded.price_basis,plan_validated=excluded.plan_validated,evaluation_dataset_id=excluded.evaluation_dataset_id,evaluation_batch_id=excluded.evaluation_batch_id,evaluation_generation=excluded.evaluation_generation,created_at=excluded.created_at",
                (evaluation_id, run_id, code, as_of, int(horizon), status, close, return_pct, mfe_pct, mae_pct, first_touch, int(sample_complete), datetime.utcnow().isoformat(), str(price_basis or "unknown").strip().lower(), int(bool(plan_validated)), str(evaluation_dataset_id).strip() if evaluation_dataset_id else None, str(evaluation_batch_id).strip() if evaluation_batch_id else None, generation),
            )

    def evaluations(self, run_id: str | None = None, limit: int = 50) -> list[dict]:
        with self._connect() as db:
            if run_id:
                rows = db.execute("SELECT * FROM result_evaluations WHERE run_id=? AND price_basis='unadjusted' AND plan_validated=1 ORDER BY created_at DESC LIMIT ?", (run_id, max(1, min(int(limit), 200))))
            else:
                rows = db.execute("SELECT * FROM result_evaluations WHERE price_basis='unadjusted' AND plan_validated=1 ORDER BY created_at DESC LIMIT ?", (max(1, min(int(limit), 200)),))
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

    @staticmethod
    def _intraday_event_key(origin: str, code: str, signal: str, plan_version: str, sequence: int) -> str:
        digest = hashlib.sha256(f"{origin}\0{code}\0{signal}\0{plan_version}\0{sequence}".encode("utf-8")).hexdigest()
        return f"intraday:{digest}"

    @staticmethod
    def _intraday_event_fields(event: dict | None) -> dict | None:
        if event is None:
            return None
        values = {
            "name": str(event.get("name") or "")[:120],
            "run_id": str(event.get("run_id") or ""),
            "invocation_id": str(event.get("invocation_id") or "").strip(),
            "payload": str(event.get("payload") or ""),
            "quote_fetched_at": str(event.get("quote_fetched_at") or ""),
            "candidate_valid_until": str(event.get("candidate_valid_until") or ""),
            "market_regime": str(event.get("market_regime") or ""),
            "market_snapshot_at": str(event.get("market_snapshot_at") or ""),
            "risk_event": int(bool(event.get("risk_event"))),
        }
        if not values["invocation_id"] or not values["payload"]:
            raise ValueError("intraday event publication is incomplete")
        return values

    def _advance_intraday_signal_in_tx(
        self,
        db,
        keys: tuple[str, str, str, str],
        *,
        qualifies: bool,
        rearm_ready: bool,
        required: int,
        max_gap_seconds: float,
        cooldown_seconds: float,
        reason: str,
        current: float,
        now_text: str,
        event: dict | None = None,
    ) -> dict:
        row = db.execute(
            "SELECT * FROM intraday_signal_states WHERE origin=? AND code=? AND signal=? AND plan_version=?",
            keys,
        ).fetchone()
        armed = bool(row["armed"]) if row else True
        count = int(row["consecutive_count"] or 0) if row else 0
        previous_observed = float(row["last_observed_at"] or 0) if row else 0.0
        previous_triggered = float(row["last_triggered_at"] or 0) if row else 0.0
        trigger_count = int(row["trigger_count"] or 0) if row else 0
        outcome_reason = str(reason or "condition_false")[:300]
        triggered = False
        required_count = max(1, min(int(required), 20))
        max_gap = max(1.0, min(float(max_gap_seconds), 3600.0))
        cooldown = max(0.0, min(float(cooldown_seconds), 86400.0))

        if not qualifies:
            count = 0
            if rearm_ready:
                armed = True
                outcome_reason = "rearmed"
        elif not armed:
            count = 0
            outcome_reason = "awaiting_hysteresis_rearm"
        elif previous_triggered and current - previous_triggered < cooldown:
            count = 0
            outcome_reason = "cooldown"
        else:
            count = count + 1 if previous_observed and current - previous_observed <= max_gap else 1
            if count >= required_count:
                triggered = True
                armed = False
                count = 0
                trigger_count += 1
                previous_triggered = current
                outcome_reason = "triggered"
            else:
                outcome_reason = f"debounce:{count}/{required_count}"

        db.execute(
            "INSERT INTO intraday_signal_states(origin,code,signal,plan_version,armed,consecutive_count,last_condition,last_observed_at,last_triggered_at,trigger_count,last_reason,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(origin,code,signal,plan_version) DO UPDATE SET "
            "armed=excluded.armed,consecutive_count=excluded.consecutive_count,last_condition=excluded.last_condition,last_observed_at=excluded.last_observed_at,"
            "last_triggered_at=excluded.last_triggered_at,trigger_count=excluded.trigger_count,last_reason=excluded.last_reason,updated_at=excluded.updated_at",
            (*keys, int(armed), count, int(bool(qualifies)), current, previous_triggered, trigger_count, outcome_reason, now_text),
        )
        result = {
            "origin": keys[0], "code": keys[1], "signal": keys[2], "plan_version": keys[3],
            "triggered": triggered, "armed": armed, "consecutive_count": count,
            "event_sequence": trigger_count, "reason": outcome_reason,
        }
        if not triggered or event is None:
            return result
        event_key = self._intraday_event_key(*keys, trigger_count)
        db.execute(
            "INSERT OR IGNORE INTO intraday_event_outbox(event_key,origin,code,name,signal,plan_version,event_sequence,run_id,invocation_id,payload,payload_hash,quote_fetched_at,candidate_valid_until,market_regime,market_snapshot_at,risk_event,state,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)",
            (
                event_key, keys[0], keys[1], event["name"], keys[2], keys[3], trigger_count,
                event["run_id"], event["invocation_id"], event["payload"],
                hashlib.sha256(event["payload"].encode("utf-8")).hexdigest(),
                event["quote_fetched_at"], event["candidate_valid_until"], event["market_regime"], event["market_snapshot_at"], event["risk_event"],
                now_text, now_text,
            ),
        )
        outbox = db.execute("SELECT * FROM intraday_event_outbox WHERE event_key=?", (event_key,)).fetchone()
        if not outbox:
            raise RuntimeError("intraday event intent was not persisted")
        result["event_key"] = event_key
        result["outbox"] = dict(outbox)
        return result

    def observe_intraday_signal(
        self,
        origin: str,
        code: str,
        signal: str,
        plan_version: str,
        *,
        qualifies: bool,
        rearm_ready: bool,
        required: int = 2,
        max_gap_seconds: float = 90,
        cooldown_seconds: float = 1800,
        reason: str = "",
        now=None,
    ) -> dict:
        """Advance one durable debounce/hysteresis state machine."""
        current = self._automatic_delivery_clock(now)
        keys = tuple(str(value or "").strip() for value in (origin, code, signal, plan_version))
        if not all(keys):
            raise ValueError("intraday signal identity is incomplete")
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._advance_intraday_signal_in_tx(
                db, keys, qualifies=qualifies, rearm_ready=rearm_ready, required=required,
                max_gap_seconds=max_gap_seconds, cooldown_seconds=cooldown_seconds,
                reason=reason, current=current, now_text=now_text,
            )

    def observe_and_enqueue_intraday_event(
        self,
        origin: str,
        code: str,
        signal: str,
        plan_version: str,
        *,
        qualifies: bool,
        rearm_ready: bool,
        required: int = 2,
        max_gap_seconds: float = 90,
        cooldown_seconds: float = 1800,
        reason: str = "",
        name: str = "",
        run_id: str = "",
        invocation_id: str = "",
        payload: str = "",
        quote_fetched_at: str = "",
        candidate_valid_until: str = "",
        market_regime: str = "",
        market_snapshot_at: str = "",
        risk_event: bool = False,
        now=None,
    ) -> dict:
        """Atomically persist a triggered FSM transition and its outbox intent."""
        current = self._automatic_delivery_clock(now)
        keys = tuple(str(value or "").strip() for value in (origin, code, signal, plan_version))
        if not all(keys):
            raise ValueError("intraday signal identity is incomplete")
        event = self._intraday_event_fields({
            "name": name, "run_id": run_id, "invocation_id": invocation_id, "payload": payload,
            "quote_fetched_at": quote_fetched_at, "candidate_valid_until": candidate_valid_until,
            "market_regime": market_regime, "market_snapshot_at": market_snapshot_at,
            "risk_event": risk_event,
        })
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._advance_intraday_signal_in_tx(
                db, keys, qualifies=qualifies, rearm_ready=rearm_ready, required=required,
                max_gap_seconds=max_gap_seconds, cooldown_seconds=cooldown_seconds,
                reason=reason, current=current, now_text=now_text, event=event,
            )

    def invalidate_intraday_observations(
        self,
        origin: str,
        code: str,
        plan_version: str,
        reason: str,
        *,
        keep_signals=None,
        now=None,
    ) -> int:
        """Break debounce continuity when critical evidence is unavailable."""
        current = self._automatic_delivery_clock(now)
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        keep = sorted({str(value or "").strip() for value in (keep_signals or ()) if str(value or "").strip()})
        clauses = ["origin=?", "code=?", "plan_version=?"]
        values = [str(origin), str(code), str(plan_version)]
        if keep:
            clauses.append("signal NOT IN (" + ",".join("?" for _ in keep) + ")")
            values.extend(keep)
        with self._connect() as db:
            return db.execute(
                "UPDATE intraday_signal_states SET consecutive_count=0,last_condition=0,last_observed_at=?,last_reason=?,updated_at=? "
                "WHERE " + " AND ".join(clauses),
                (
                    current,
                    str(reason or "critical_evidence_missing")[:300],
                    now_text,
                    *values,
                ),
            ).rowcount

    def invalidate_intraday_target_states(self, targets, reason: str, *, now=None) -> int:
        """Clear confirmation continuity for a bounded batch of skipped targets."""
        current = self._automatic_delivery_clock(now)
        now_text = datetime.fromtimestamp(current, timezone.utc).replace(tzinfo=None).isoformat()
        rows = sorted({
            (str(origin or "").strip(), str(code or "").strip(), str(plan_version or "").strip())
            for origin, code, plan_version in (targets or ())
            if str(origin or "").strip() and str(code or "").strip() and str(plan_version or "").strip()
        })
        if not rows:
            return 0
        changed = 0
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for origin, code, plan_version in rows:
                changed += db.execute(
                    "UPDATE intraday_signal_states SET consecutive_count=0,last_condition=0,last_observed_at=?,last_reason=?,updated_at=? "
                    "WHERE origin=? AND code=? AND plan_version=?",
                    (current, str(reason or "critical_evidence_missing")[:300], now_text, origin, code, plan_version),
                ).rowcount
        return changed

    def enqueue_intraday_event(
        self,
        event_key: str,
        *,
        origin: str,
        code: str,
        name: str,
        signal: str,
        plan_version: str,
        event_sequence: int,
        run_id: str,
        invocation_id: str,
        payload: str,
        quote_fetched_at: str = "",
        candidate_valid_until: str = "",
        market_regime: str = "",
        market_snapshot_at: str = "",
        risk_event: bool = False,
    ) -> dict:
        values = {
            "event_key": str(event_key or "").strip(), "origin": str(origin or "").strip(),
            "code": str(code or "").strip(), "signal": str(signal or "").strip(),
            "plan_version": str(plan_version or "").strip(), "invocation_id": str(invocation_id or "").strip(),
            "payload": str(payload or ""),
        }
        if not all(values.values()) or int(event_sequence) < 1:
            raise ValueError("intraday event publication is incomplete")
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT OR IGNORE INTO intraday_event_outbox(event_key,origin,code,name,signal,plan_version,event_sequence,run_id,invocation_id,payload,payload_hash,quote_fetched_at,candidate_valid_until,market_regime,market_snapshot_at,risk_event,state,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)",
                (
                    values["event_key"], values["origin"], values["code"], str(name or "")[:120],
                    values["signal"], values["plan_version"], int(event_sequence), str(run_id or ""),
                    values["invocation_id"], values["payload"], hashlib.sha256(values["payload"].encode("utf-8")).hexdigest(),
                    str(quote_fetched_at or ""), str(candidate_valid_until or ""), str(market_regime or ""), str(market_snapshot_at or ""), int(bool(risk_event)),
                    now_text, now_text,
                ),
            )
            row = db.execute("SELECT * FROM intraday_event_outbox WHERE event_key=?", (values["event_key"],)).fetchone()
            return dict(row)

    def recoverable_intraday_deliveries(
        self,
        *,
        now=None,
        limit: int = 100,
        max_attempts: int = 5,
        retry_window_seconds: float = 3600,
    ) -> list[dict]:
        current = self._automatic_delivery_clock(now)
        cutoff = datetime.fromtimestamp(current - max(60.0, float(retry_window_seconds)), timezone.utc).replace(tzinfo=None).isoformat()
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE intraday_event_outbox SET state='unknown_delivery',lease_owner='',lease_expires_at=0,next_retry_at=0,"
                "last_error=CASE WHEN last_error='' THEN 'delivery outcome unknown after sender interruption' ELSE last_error END,updated_at=? "
                "WHERE state='sending' AND lease_expires_at<=?",
                (now_text, current),
            )
            db.execute(
                "UPDATE intraday_event_outbox SET state='cancelled',next_retry_at=0,last_error=CASE WHEN last_error='' THEN "
                "'confirmed send failures exhausted retry bounds' ELSE last_error || '; retry bounds exhausted' END,updated_at=? "
                "WHERE state='failed' AND (attempts>=? OR created_at<=?)",
                (now_text, max(1, int(max_attempts)), cutoff),
            )
            db.execute(
                "UPDATE intraday_event_outbox SET state='cancelled',next_retry_at=0,last_error='intraday event expired before delivery',updated_at=? "
                "WHERE state='pending' AND created_at<=?",
                (now_text, cutoff),
            )
            return [dict(row) for row in db.execute(
                "SELECT * FROM intraday_event_outbox WHERE state='pending' OR (state='failed' AND next_retry_at<=?) ORDER BY created_at,event_key LIMIT ?",
                (current, max(1, min(int(limit), 1000))),
            )]

    def claim_intraday_delivery(self, event_key: str, owner: str, *, ttl_seconds: float = 120, now=None) -> dict:
        current = self._automatic_delivery_clock(now)
        ttl = max(5.0, min(float(ttl_seconds), 3600.0))
        owner_value = str(owner or "").strip()[:160]
        if not owner_value:
            raise ValueError("intraday delivery owner is required")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM intraday_event_outbox WHERE event_key=?", (str(event_key),)).fetchone()
            if not row:
                raise KeyError(f"unknown intraday event {event_key}")
            state = str(row["state"])
            expiry = float(row["lease_expires_at"] or 0)
            if state == "sending" and expiry <= current:
                db.execute(
                    "UPDATE intraday_event_outbox SET state='unknown_delivery',lease_owner='',lease_expires_at=0,next_retry_at=0,"
                    "last_error=CASE WHEN last_error='' THEN 'delivery outcome unknown after sender interruption' ELSE last_error END,updated_at=? WHERE event_key=?",
                    (datetime.utcnow().isoformat(), str(event_key)),
                )
                row = db.execute("SELECT * FROM intraday_event_outbox WHERE event_key=?", (str(event_key),)).fetchone()
                return {**dict(row), "acquired": False, "reason": "unknown_delivery"}
            if state in {"sent", "unknown_delivery", "cancelled"}:
                return {**dict(row), "acquired": False, "reason": state}
            if state == "sending":
                return {**dict(row), "acquired": False, "reason": "contended"}
            if state == "failed" and float(row["next_retry_at"] or 0) > current:
                return {**dict(row), "acquired": False, "reason": "backoff"}
            fence = int(row["lease_fence"] or 0) + 1
            db.execute(
                "UPDATE intraday_event_outbox SET state='sending',attempts=attempts+1,lease_owner=?,lease_fence=?,lease_expires_at=?,updated_at=? "
                "WHERE event_key=? AND state IN ('pending','failed')",
                (owner_value, fence, current + ttl, datetime.utcnow().isoformat(), str(event_key)),
            )
            refreshed = db.execute("SELECT * FROM intraday_event_outbox WHERE event_key=?", (str(event_key),)).fetchone()
            return {**dict(refreshed), "acquired": True, "owner": owner_value, "fence": fence}

    def finish_intraday_delivery(
        self,
        event_key: str,
        owner: str,
        fence: int,
        *,
        sent: bool,
        error: str = "",
        retry_after_seconds: float = 60,
        max_attempts: int = 5,
        retry_window_seconds: float = 3600,
        now=None,
    ) -> dict:
        current = self._automatic_delivery_clock(now)
        now_text = datetime.utcnow().isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT attempts,created_at FROM intraday_event_outbox WHERE event_key=?", (str(event_key),)).fetchone()
            if not row:
                raise KeyError(f"unknown intraday event {event_key}")
            try:
                created = datetime.fromisoformat(str(row["created_at"])).replace(tzinfo=timezone.utc).timestamp()
            except (TypeError, ValueError):
                created = current
            exhausted = not sent and (int(row["attempts"] or 0) >= max(1, int(max_attempts)) or current - created >= max(60.0, float(retry_window_seconds)))
            state = "sent" if sent else ("cancelled" if exhausted else "failed")
            retry_at = 0.0 if sent or exhausted else current + max(1.0, min(float(retry_after_seconds), 3600.0))
            final_error = str(error or "")[:500]
            if exhausted:
                final_error = (final_error + "; retry bounds exhausted").strip("; ")[:500]
            changed = db.execute(
                "UPDATE intraday_event_outbox SET state=?,lease_owner='',lease_expires_at=0,next_retry_at=?,last_error=?,updated_at=?,sent_at=? "
                "WHERE event_key=? AND state='sending' AND lease_owner=? AND lease_fence=? AND lease_expires_at>?",
                (state, retry_at, final_error, now_text, now_text if sent else None, str(event_key), str(owner), int(fence), current),
            ).rowcount
            if changed != 1:
                raise RuntimeError("intraday delivery acknowledgement lost ownership")
            return dict(db.execute("SELECT * FROM intraday_event_outbox WHERE event_key=?", (str(event_key),)).fetchone())

    def mark_intraday_delivery_unknown(self, event_key: str, owner: str, fence: int, *, error: str, now=None) -> dict:
        current = self._automatic_delivery_clock(now)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                "UPDATE intraday_event_outbox SET state='unknown_delivery',lease_owner='',lease_expires_at=0,next_retry_at=0,last_error=?,updated_at=? "
                "WHERE event_key=? AND state='sending' AND lease_owner=? AND lease_fence=? AND lease_expires_at>?",
                (str(error or "delivery outcome unknown")[:500], datetime.utcnow().isoformat(), str(event_key), str(owner), int(fence), current),
            ).rowcount
            if changed != 1:
                raise RuntimeError("intraday unknown-delivery marker lost ownership")
            return dict(db.execute("SELECT * FROM intraday_event_outbox WHERE event_key=?", (str(event_key),)).fetchone())

    def cancel_intraday_delivery(self, event_key: str, reason: str, *, owner: str = "", fence: int = 0) -> bool:
        """Cancel a queued event, including a lease held by this sender.

        The owned-sending branch exists for pre-send validation: an event is
        claimed first to fence concurrent recovery workers, then cancelled
        before any transport call if its quote or plan is no longer valid.
        """
        owned = bool(str(owner or "").strip()) and int(fence or 0) > 0
        with self._connect() as db:
            if owned:
                changed = db.execute(
                    "UPDATE intraday_event_outbox SET state='cancelled',lease_owner='',lease_expires_at=0,next_retry_at=0,last_error=?,updated_at=? "
                    "WHERE event_key=? AND (state IN ('pending','failed') OR (state='sending' AND lease_owner=? AND lease_fence=?))",
                    (str(reason or "cancelled")[:500], datetime.utcnow().isoformat(), str(event_key), str(owner), int(fence)),
                ).rowcount
            else:
                changed = db.execute(
                    "UPDATE intraday_event_outbox SET state='cancelled',lease_owner='',lease_expires_at=0,next_retry_at=0,last_error=?,updated_at=? "
                    "WHERE event_key=? AND state IN ('pending','failed')",
                    (str(reason or "cancelled")[:500], datetime.utcnow().isoformat(), str(event_key)),
                ).rowcount
            return changed == 1

    def intraday_delivery_summary(self, origin: str | None = None) -> dict[str, int]:
        with self._connect() as db:
            if origin is None:
                rows = db.execute("SELECT state,COUNT(*) FROM intraday_event_outbox GROUP BY state").fetchall()
            else:
                rows = db.execute("SELECT state,COUNT(*) FROM intraday_event_outbox WHERE origin=? GROUP BY state", (str(origin),)).fetchall()
        result = {state: 0 for state in ("pending", "sending", "sent", "failed", "unknown_delivery", "cancelled")}
        result.update({str(row[0]): int(row[1]) for row in rows})
        return result

    def recent_intraday_states(self, origin: str, limit: int = 10) -> list[dict]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT code,signal,plan_version,armed,consecutive_count,last_condition,last_observed_at,last_triggered_at,trigger_count,last_reason,updated_at "
                "FROM intraday_signal_states WHERE origin=? ORDER BY updated_at DESC,code,signal LIMIT ?",
                (str(origin), max(1, min(int(limit), 50))),
            ).fetchall()
            return [dict(row) for row in rows]
