from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from astrbot_stock_watch.storage import StockStore
from webapp.data import Dashboard

from test_v0138_automatic_close import _imports
from test_v0140_recommendation_tracking import _record


TRADE_DATE = "2026-09-22"
NOW = datetime(2026, 9, 22, 8, 0, tzinfo=timezone.utc)


def _complete_day(store: StockStore, *, candidate_count: int = 0, run_id: str = "run-v1") -> None:
    store.save_snapshot_meta(TRADE_DATE, "fixture", "good", True, TRADE_DATE)
    store.save_screen_run(
        run_id,
        "automatic_close",
        TRADE_DATE,
        TRADE_DATE,
        "fixture",
        "2026-09-22T07:10:00Z",
        "2026-09-22T07:20:00Z",
        5000,
        candidate_count,
        "completed",
        "good",
        coverage=1.0,
    )


def test_schema_v18_deduplicates_anomaly_and_emits_recovery(tmp_path):
    store = StockStore(tmp_path / "acceptance.sqlite3")
    assert store.schema_version() == 24

    first = store.evaluate_daily_acceptance(TRADE_DATE, now=NOW)
    assert first["status"] == "critical" and first["event_kind"] == "anomaly"
    assert {row["code"] for row in first["findings"]} == {"daily_snapshot_missing", "candidate_freeze_missing"}

    repeated = store.evaluate_daily_acceptance(TRADE_DATE, now=NOW)
    assert repeated["changed"] is False and repeated["event_id"] == ""

    _complete_day(store)
    recovered = store.evaluate_daily_acceptance(TRADE_DATE, now=NOW)
    assert recovered["status"] == "healthy" and recovered["event_kind"] == "recovery"
    assert store.prepare_daily_acceptance_alerts(recovered["event_id"], ["origin-a", "origin-a"], "recovered") == 1
    assert store.prepare_daily_acceptance_alerts(recovered["event_id"], ["origin-a"], "recovered") == 0


def test_acceptance_event_and_alerts_commit_together_across_restart(tmp_path):
    Main, _, _, _ = _imports()
    database = tmp_path / "atomic.sqlite3"
    store = StockStore(database)
    result = store.evaluate_daily_acceptance(
        TRADE_DATE,
        now=NOW,
        alert_origins=("origin-a", "origin-b", "origin-a"),
        alert_message=Main._daily_acceptance_message,
    )

    restarted = StockStore(database)
    with restarted._connect() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT event_id,origin,state,payload FROM daily_acceptance_alerts ORDER BY origin"
        )]
    assert [row["origin"] for row in rows] == ["origin-a", "origin-b"]
    assert all(row["event_id"] == result["event_id"] and row["state"] == "pending" for row in rows)
    assert all("每日链路主动告警" in row["payload"] for row in rows)
    repeated = restarted.evaluate_daily_acceptance(TRADE_DATE, now=NOW)
    assert repeated["changed"] is False
    assert restarted.daily_acceptance_alert_summary()["pending"] == 2


def test_alert_format_failure_rolls_back_acceptance_event(tmp_path):
    store = StockStore(tmp_path / "rollback.sqlite3")

    def bad_message(_result):
        raise ValueError("formatter unavailable")

    try:
        store.evaluate_daily_acceptance(
            TRADE_DATE, now=NOW, alert_origins=("origin-a",), alert_message=bad_message,
        )
    except ValueError as exc:
        assert str(exc) == "formatter unavailable"
    else:
        raise AssertionError("format failure must roll back the entire acceptance transaction")
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM daily_acceptance_events").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM daily_acceptance_runs").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM daily_acceptance_alerts").fetchone()[0] == 0
    assert store.evaluate_daily_acceptance(TRADE_DATE, now=NOW)["event_kind"] == "anomaly"


def test_superseded_pending_alert_is_cancelled_before_send(tmp_path):
    Main, _, _, _ = _imports()
    store = StockStore(tmp_path / "superseded.sqlite3")
    first = store.evaluate_daily_acceptance(
        TRADE_DATE, now=NOW, alert_origins=("origin-a",), alert_message=Main._daily_acceptance_message,
    )
    pending = store.recoverable_daily_acceptance_alerts(now=NOW)[0]
    _complete_day(store)
    recovery = store.evaluate_daily_acceptance(
        TRADE_DATE, now=NOW, alert_origins=("origin-a",), alert_message=Main._daily_acceptance_message,
    )
    assert recovery["event_kind"] == "recovery" and recovery["event_id"] != first["event_id"]
    assert store.claim_daily_acceptance_alert(pending["alert_id"], "test-owner", now=NOW)["reason"] == "superseded"
    with store._connect() as db:
        stale = db.execute("SELECT state FROM daily_acceptance_alerts WHERE alert_id=?", (pending["alert_id"],)).fetchone()
    assert stale["state"] == "cancelled"
    assert store.daily_acceptance_alert_summary()["pending"] == 1


def test_daily_tick_enqueues_before_delivery_and_does_not_duplicate(tmp_path):
    Main, _, _, _ = _imports()
    store = StockStore(tmp_path / "tick.sqlite3")
    store.set_subscription("origin-a", True)
    main = Main.__new__(Main)
    main.store = store
    main.config = {"daily_scan_time": "15:10", "daily_acceptance_time": "15:40"}
    main._push_allowed = lambda _origin: True

    async def calendar_open(_date):
        return True

    async def observe_outbox():
        assert store.daily_acceptance_alert_summary()["pending"] == 1
        return store.daily_acceptance_alert_summary()

    main._calendar_open = calendar_open
    main._recover_daily_acceptance_alerts = observe_outbox

    async def scenario():
        early = await main._daily_acceptance_tick(datetime(2026, 9, 22, 7, 39, tzinfo=timezone.utc))
        first = await main._daily_acceptance_tick(datetime(2026, 9, 22, 7, 40, tzinfo=timezone.utc))
        second = await main._daily_acceptance_tick(datetime(2026, 9, 22, 7, 40, tzinfo=timezone.utc))
        return early, first, second

    early, first, second = asyncio.run(scenario())
    assert early == {"state": "before_acceptance"}
    assert first["event_kind"] == "anomaly" and second["changed"] is False
    assert store.daily_acceptance_alert_summary()["pending"] == 1


def test_schema_v17_database_migrates_additively_to_v18(tmp_path):
    database = tmp_path / "upgrade.sqlite3"
    store = StockStore(database)
    with store._connect() as db:
        for table in (
            "daily_acceptance_delivery_capability_evidence",
            "daily_acceptance_alerts",
            "daily_acceptance_events",
            "daily_acceptance_state",
            "daily_acceptance_runs",
        ):
            db.execute(f"DROP TABLE {table}")
        db.execute("UPDATE schema_meta SET value='17' WHERE key='schema_version'")

    upgraded = StockStore(database)
    assert upgraded.schema_version() == 24
    with upgraded._connect() as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"daily_acceptance_runs", "daily_acceptance_events", "daily_acceptance_alerts"}.issubset(tables)


def test_acceptance_reports_stale_ai_and_real_outcome_reason(tmp_path):
    store = StockStore(tmp_path / "blocked.sqlite3")
    _record(store, "r1", comparable="unknown")
    with store._connect() as db:
        db.execute("UPDATE recommendation_records SET run_id='run-v1',recommended_date='2026-09-16' WHERE recommendation_id='r1'")
    for day, opened in (("2026-09-17", True), ("2026-09-18", True),
                        ("2026-09-19", False), ("2026-09-20", False),
                        ("2026-09-21", True), ("2026-09-22", True)):
        store.save_calendar(day, opened, "fixture", ttl_seconds=999999999)
    store.evaluate_recommendation_outcomes(as_of=TRADE_DATE, horizons=(1,))
    _complete_day(store, candidate_count=1)
    batch = store.begin_recommendation_ai_review(
        "run-v1", model="deepseek-flash", prompt_version="shadow-risk-v1", input_sha256="a" * 64
    )
    with store._connect() as db:
        db.execute(
            "UPDATE recommendation_ai_review_batches SET requested_at='2026-09-22T06:00:00' WHERE review_batch_id=?",
            (batch["review_batch_id"],),
        )

    result = store.evaluate_daily_acceptance(
        TRADE_DATE,
        now=NOW,
        ai_expected=True,
        ai_pending_stale_minutes=30,
        outcome_overdue_days=2,
    )
    by_code = {row["code"]: row for row in result["findings"]}
    assert "ai_review_stale_pending" in by_code
    assert "corporate_action_evidence_missing" in by_code["recommendation_outcomes_blocked"]["message"]


def test_unknown_delivery_is_terminal_for_automatic_recovery(tmp_path):
    Main, _, _, _ = _imports()
    store = StockStore(tmp_path / "unknown.sqlite3")
    event = store.evaluate_daily_acceptance(TRADE_DATE, now=NOW)
    store.set_subscription("origin-a", True)
    store.prepare_daily_acceptance_alerts(event["event_id"], ["origin-a"], "alert")

    class Context:
        async def send_message(self, *_args, **_kwargs):
            raise RuntimeError("ambiguous transport")

    main = Main.__new__(Main)
    main.store = store
    main.context = Context()
    main.config = {}
    main._daily_acceptance_owner = "daily-acceptance:test"
    main._last_daily_acceptance_unknown_count = 0
    main._push_allowed = lambda _origin: True
    first = asyncio.run(main._recover_daily_acceptance_alerts())
    second = asyncio.run(main._recover_daily_acceptance_alerts())
    assert first["unknown_delivery"] == 1 and second["unknown_delivery"] == 1
    assert store.daily_acceptance_alert_summary()["sending"] == 0


def test_web_exposes_acceptance_status_and_outbox(tmp_path):
    database = tmp_path / "web.sqlite3"
    store = StockStore(database)
    event = store.evaluate_daily_acceptance(TRADE_DATE, now=NOW)
    store.prepare_daily_acceptance_alerts(event["event_id"], ["origin-a"], "alert")
    dashboard = Dashboard(database, now=lambda: NOW)
    assert dashboard.query("overview")["data"]["acceptance"]["status"] == "critical"
    health = dashboard.query("health")["data"]
    assert health["daily_acceptance"][0]["date"] == TRADE_DATE
    assert health["daily_acceptance_outbox"]["pending"] == 1
