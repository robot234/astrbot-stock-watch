from __future__ import annotations

import asyncio
import importlib
import sqlite3
import sys
import tempfile
import threading
import time as wall_time
import types
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


def _install_astrbot_stubs() -> None:
    logger = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
        debug=lambda *args, **kwargs: None,
    )
    api = types.ModuleType("astrbot.api")
    api.logger = logger
    event = types.ModuleType("astrbot.api.event")
    event.AstrMessageEvent = object
    event.MessageChain = lambda parts: parts
    event.filter = types.SimpleNamespace(command=lambda *args, **kwargs: lambda fn: fn)
    components = types.ModuleType("astrbot.api.message_components")
    components.Plain = lambda text: text
    star = types.ModuleType("astrbot.api.star")
    star.Context = object
    star.Star = object
    star.register = lambda *args, **kwargs: lambda cls: cls
    path_module = types.ModuleType("astrbot.core.utils.astrbot_path")
    path_module.get_astrbot_data_path = tempfile.mkdtemp
    modules = {
        "astrbot": types.ModuleType("astrbot"),
        "astrbot.api": api,
        "astrbot.api.event": event,
        "astrbot.api.message_components": components,
        "astrbot.api.star": star,
        "astrbot.core": types.ModuleType("astrbot.core"),
        "astrbot.core.utils": types.ModuleType("astrbot.core.utils"),
        "astrbot.core.utils.astrbot_path": path_module,
    }
    for name, module in modules.items():
        sys.modules.setdefault(name, module)


def _imports():
    _install_astrbot_stubs()
    main_module = importlib.import_module("astrbot_stock_watch.main")
    core_module = importlib.import_module("astrbot_stock_watch.core")
    storage_module = importlib.import_module("astrbot_stock_watch.storage")
    return main_module.Main, main_module.ScreenScoreResult, core_module, storage_module.StockStore


def _candidate(core, code: str):
    quote = core.Quote(
        code,
        code,
        10.0,
        9.8,
        1_000_000.0,
        2.0,
        100_000.0,
        history_days=120,
        suspended=False,
        limit_up=False,
        limit_down=False,
    )
    return core.Candidate(quote, 20, ["fixture"], base_score=20, risk_level="eligible")


def _run_args(run_id: str, job_name: str = "automatic_close", trade_date: str = "2026-09-09"):
    now = "2026-09-09T07:10:00Z"
    return (run_id, job_name, trade_date, trade_date, "tushare", now, now, 5000, 1, "completed", "good", None)


def _save_publication(store, core, run_id="auto-run", origins=("origin-a", "origin-b"), *, trade_date="2026-09-09", payload="verified report"):
    publication_key = f"automatic_close:{trade_date}"
    return store.save_screen_bundle_atomic(
        _run_args(run_id, trade_date=trade_date),
        [_candidate(core, "600000")],
        diagnostics={"indicator_coverage": 1.0},
        coverage=1.0,
        report_key=publication_key,
        publication_key=publication_key,
        publication_payload=payload,
        publication_invocation_id=publication_key,
        publication_origins=origins,
    )


def _delivery_main(Main, store, context, **config):
    main = Main.__new__(Main)
    main.store = store
    main.context = context
    main.config = config
    main._automatic_delivery_owner = "automatic-close:test"
    main._push_allowed = lambda _origin: True
    return main


def test_close_phase_never_runs_before_1500_and_handles_weekend_and_backoff():
    Main, _, core, _ = _imports()
    main = Main.__new__(Main)
    main.config = {"daily_scan_time": "14:00"}
    main._daily_retry_after = None
    assert main._automatic_close_phase(datetime(2026, 9, 9, 14, 59, tzinfo=core.CHINA_TZ)) == "before_close"
    assert main._automatic_close_phase(datetime(2026, 9, 9, 15, 0, tzinfo=core.CHINA_TZ)) == "calendar_check"
    assert main._automatic_close_phase(datetime(2026, 9, 12, 15, 30, tzinfo=core.CHINA_TZ)) == "weekend"
    main._daily_retry_after = datetime(2026, 9, 9, 15, 10, tzinfo=core.CHINA_TZ)
    assert main._automatic_close_phase(datetime(2026, 9, 9, 15, 5, tzinfo=core.CHINA_TZ)) == "backoff"


def test_close_tick_fails_closed_for_unknown_or_closed_calendar_and_uses_one_job_key():
    Main, _, core, _ = _imports()

    class Store:
        def __init__(self):
            self.calls = []

        def begin_job(self, *args):
            self.calls.append(args)
            return True

        def job_run(self, _key):
            return None

    async def scenario(calendar):
        main = Main.__new__(Main)
        main.config = {"daily_scan_time": "15:00"}
        main._daily_retry_after = None
        main.store = Store()

        async def calendar_open(_date):
            return calendar

        async def run_job(requested, key):
            return {"state": "ran", "requested": requested, "key": key}

        main._calendar_open = calendar_open
        main._run_automatic_close_job = run_job
        result = await main._automatic_close_tick(datetime(2026, 9, 9, 15, 1, tzinfo=core.CHINA_TZ))
        return main, result

    unknown_main, unknown = asyncio.run(scenario(None))
    closed_main, closed = asyncio.run(scenario(False))
    ready_main, ready = asyncio.run(scenario(True))
    assert unknown == {"state": "calendar_unknown"} and unknown_main.store.calls == []
    assert closed == {"state": "calendar_closed"} and closed_main.store.calls == []
    assert ready["key"] == "automatic_close:2026-09-09"
    assert ready_main.store.calls == [("automatic_close:2026-09-09", "automatic_close", "2026-09-09", 900)]


def test_upgrade_guard_skips_date_already_completed_by_legacy_scheduler():
    Main, _, core, _ = _imports()

    class Store:
        def __init__(self):
            self.begin_calls = 0

        def job_run(self, key):
            return {"job_key": key, "status": "completed"}

        def begin_job(self, *_args):
            self.begin_calls += 1
            return True

    main = Main.__new__(Main)
    main.config = {"daily_scan_time": "15:00"}
    main._daily_retry_after = None
    main.last_daily_scan = None
    main.store = Store()

    async def calendar_open(_date):
        return True

    main._calendar_open = calendar_open
    result = asyncio.run(main._automatic_close_tick(datetime(2026, 9, 9, 15, 1, tzinfo=core.CHINA_TZ)))
    assert result == {"state": "legacy_completed", "job_key": "daily_screen:2026-09-09"}
    assert main.store.begin_calls == 0


def test_publication_is_idempotent_and_destinations_are_durable(tmp_path):
    _, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "stock.sqlite3")
    first = _save_publication(store, core)
    second = store.save_screen_bundle_atomic(
        _run_args("auto-run-retry"),
        [_candidate(core, "000001")],
        diagnostics={"indicator_coverage": 1.0},
        coverage=1.0,
        report_key="automatic_close:2026-09-09",
        publication_key="automatic_close:2026-09-09",
        publication_payload="different retry payload",
        publication_invocation_id="automatic_close:2026-09-09",
        publication_origins=("origin-c",),
    )
    assert first["run_id"] == second["run_id"] == "auto-run"
    assert second["idempotent"] is True
    publication = store.automatic_close_publication("automatic_close:2026-09-09")
    assert publication["origins"] == ["origin-a", "origin-b"]
    deliveries = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")
    assert [(row["origin"], row["state"], row["payload"]) for row in deliveries] == [
        ("origin-a", "pending", "verified report"),
        ("origin-b", "pending", "verified report"),
    ]
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM screen_runs").fetchone()[0] == 1


def test_manual_and_duplicate_automatic_publication_race_keeps_one_auto_bundle(tmp_path):
    _, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "race.sqlite3")
    barrier = threading.Barrier(3)

    def automatic(run_id):
        barrier.wait()
        return _save_publication(store, core, run_id=run_id, origins=("origin-a",))

    def manual():
        barrier.wait()
        return store.save_screen_bundle_atomic(
            _run_args("manual-run", "daily_screen"),
            [_candidate(core, "000001")],
            diagnostics={"diagnostics_invocation_id": "manual", "indicator_coverage": 1.0},
            coverage=1.0,
            report_key="daily_screen:2026-09-09",
        )

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = [pool.submit(automatic, "auto-one"), pool.submit(automatic, "auto-two"), pool.submit(manual)]
        values = [item.result(timeout=10) for item in results]
    auto_ids = {values[0]["run_id"], values[1]["run_id"]}
    assert len(auto_ids) == 1
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM automatic_close_publications").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM screen_runs WHERE job_name='automatic_close'").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM screen_runs WHERE job_name='daily_screen'").fetchone()[0] == 1


def test_expired_sending_becomes_unknown_and_is_never_recoverable(tmp_path):
    _, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "unknown.sqlite3")
    _save_publication(store, core, origins=("origin-a",))
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    claimed = store.claim_automatic_close_delivery(delivery["delivery_id"], "worker-one", ttl_seconds=5, now=100)
    assert claimed["acquired"] is True and claimed["state"] == "sending"
    assert store.recoverable_automatic_close_deliveries(now=106) == []
    summary = store.automatic_close_delivery_summary("automatic_close:2026-09-09")
    assert summary["unknown_delivery"] == 1
    later = store.claim_automatic_close_delivery(delivery["delivery_id"], "worker-two", ttl_seconds=5, now=200)
    assert later["acquired"] is False and later["reason"] == "unknown_delivery"


def test_confirmed_send_failure_retries_and_sent_delivery_is_terminal(tmp_path):
    _, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "retry.sqlite3")
    _save_publication(store, core, origins=("origin-a",))
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    first = store.claim_automatic_close_delivery(delivery["delivery_id"], "worker", ttl_seconds=10, now=100)
    failed = store.finish_automatic_close_delivery(
        delivery["delivery_id"], "worker", first["fence"], sent=False, error="adapter rejected", retry_after_seconds=5, now=101
    )
    assert failed["state"] == "failed"
    assert store.recoverable_automatic_close_deliveries(now=105) == []
    assert len(store.recoverable_automatic_close_deliveries(now=106)) == 1
    second = store.claim_automatic_close_delivery(delivery["delivery_id"], "worker", ttl_seconds=10, now=106)
    sent = store.finish_automatic_close_delivery(delivery["delivery_id"], "worker", second["fence"], sent=True, now=107)
    assert sent["state"] == "sent" and sent["sent_at"]
    assert store.recoverable_automatic_close_deliveries(now=1000) == []


def test_confirmed_delivery_failures_stop_at_attempt_bound(tmp_path):
    _, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "retry-bound.sqlite3")
    _save_publication(store, core, origins=("origin-a",))
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    first = store.claim_automatic_close_delivery(delivery["delivery_id"], "worker", ttl_seconds=10, now=100)
    failed = store.finish_automatic_close_delivery(
        delivery["delivery_id"], "worker", first["fence"], sent=False, error="pre-send unavailable",
        retry_after_seconds=5, max_attempts=2, retry_window_seconds=600, now=101,
    )
    assert failed["state"] == "failed"
    second = store.claim_automatic_close_delivery(delivery["delivery_id"], "worker", ttl_seconds=10, now=106)
    terminal = store.finish_automatic_close_delivery(
        delivery["delivery_id"], "worker", second["fence"], sent=False, error="pre-send unavailable",
        retry_after_seconds=5, max_attempts=2, retry_window_seconds=600, now=107,
    )
    assert terminal["state"] == "cancelled" and "retry bounds exhausted" in terminal["last_error"]
    assert store.recoverable_automatic_close_deliveries(now=1000, max_attempts=2, retry_window_seconds=600) == []


def test_sent_but_ack_persistence_unknown_does_not_auto_resend(tmp_path):
    Main, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "ack.sqlite3")
    store.set_subscription("origin-a", True)
    _save_publication(store, core, origins=("origin-a",))
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    sent_payloads = []

    class Context:
        async def send_message(self, origin, payload):
            sent_payloads.append((origin, payload))

    main = _delivery_main(Main, store, Context(), automatic_delivery_lease_seconds=5)
    main._automatic_delivery_owner = "new-process"
    original_finish = store.finish_automatic_close_delivery

    def lose_ack(*args, **kwargs):
        raise sqlite3.OperationalError("simulated ACK persistence outage")

    store.finish_automatic_close_delivery = lose_ack
    assert asyncio.run(main._dispatch_automatic_delivery(delivery)) == "unknown_delivery"
    assert len(sent_payloads) == 1 and sent_payloads[0][0] == "origin-a"
    store.finish_automatic_close_delivery = original_finish
    with store._connect() as db:
        expiry = float(db.execute("SELECT lease_expires_at FROM automatic_close_deliveries").fetchone()[0])
    assert store.recoverable_automatic_close_deliveries(now=expiry + 1) == []
    assert store.automatic_close_delivery_summary()["unknown_delivery"] == 1
    assert len(sent_payloads) == 1


def test_accepted_then_timeout_is_terminal_unknown_without_resend(tmp_path):
    Main, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "accepted-timeout.sqlite3")
    store.set_subscription("origin-a", True)
    _save_publication(store, core, origins=("origin-a",))
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    calls = []

    class Context:
        async def send_message(self, origin, payload):
            calls.append((origin, payload))
            raise TimeoutError("adapter timed out after accepting request")

    main = _delivery_main(Main, store, Context())
    assert asyncio.run(main._dispatch_automatic_delivery(delivery)) == "unknown_delivery"
    assert len(calls) == 1
    assert store.automatic_close_delivery_summary()["unknown_delivery"] == 1
    assert store.recoverable_automatic_close_deliveries(now=10**10) == []


def test_partial_multi_chunk_send_is_terminal_unknown(tmp_path):
    Main, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "partial.sqlite3")
    store.set_subscription("origin-a", True)
    _save_publication(store, core, origins=("origin-a",), payload="a" * 500 + "\n" + "b" * 500)
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    calls = []

    class Context:
        async def send_message(self, origin, payload):
            calls.append((origin, payload))
            if len(calls) == 2:
                raise ConnectionError("second chunk outcome unknown")

    main = _delivery_main(Main, store, Context(), push_max_chars=500)
    assert asyncio.run(main._dispatch_automatic_delivery(delivery)) == "unknown_delivery"
    assert len(calls) == 2
    row = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    assert row["state"] == "unknown_delivery" and "chunk 2/2" in row["last_error"]


def test_typeerror_after_send_invocation_has_no_signature_fallback_or_retry(tmp_path):
    Main, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "typeerror.sqlite3")
    store.set_subscription("origin-a", True)
    _save_publication(store, core, origins=("origin-a",))
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    calls = []

    class Context:
        async def send_message(self, origin, payload):
            calls.append((origin, payload))
            raise TypeError("adapter raised after invocation")

    main = _delivery_main(Main, store, Context())
    assert asyncio.run(main._dispatch_automatic_delivery(delivery)) == "unknown_delivery"
    assert len(calls) == 1
    assert store.automatic_close_delivery_summary()["unknown_delivery"] == 1


def test_confirmed_pre_send_unavailable_is_bounded_retryable_failure(tmp_path):
    Main, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "pre-send.sqlite3")
    store.set_subscription("origin-a", True)
    _save_publication(store, core, origins=("origin-a",))
    delivery = store.prepare_automatic_close_deliveries("automatic_close:2026-09-09")[0]
    main = _delivery_main(Main, store, object(), automatic_delivery_retry_seconds=5)
    assert asyncio.run(main._dispatch_automatic_delivery(delivery)) == "failed"
    summary = store.automatic_close_delivery_summary()
    assert summary["failed"] == 1 and summary["unknown_delivery"] == 0


def test_reload_recovers_publication_created_before_outbox_rows(tmp_path):
    Main, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "publication-crash.sqlite3")
    store.set_subscription("origin-a", True)
    _save_publication(store, core, origins=("origin-a",))
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM automatic_close_deliveries").fetchone()[0] == 0
    sent_payloads = []

    class Context:
        async def send_message(self, origin, payload):
            sent_payloads.append((origin, payload))

    main = _delivery_main(Main, store, Context())
    main._automatic_delivery_owner = "reloaded-process"
    summary = asyncio.run(main._recover_automatic_deliveries())
    assert len(sent_payloads) == 1 and sent_payloads[0][0] == "origin-a"
    assert summary["sent"] == 1


def test_successful_automatic_job_persists_owned_diagnostics_and_sends_each_destination_once(tmp_path):
    Main, ScreenScoreResult, core, StockStore = _imports()
    store = StockStore(tmp_path / "success.sqlite3")
    quote = core.Quote(
        "600000", "sample", 10.0, 9.9, 1_000_000.0, 1.0, 100_000.0,
        source="tushare", history_days=120, suspended=False, limit_up=False, limit_down=False,
    )
    candidate = _candidate(core, "600000")
    store.save_daily_quotes("2026-09-09", [quote])
    store.set_subscription("origin-a", True)
    store.set_subscription("origin-b", True)
    store.begin_job("automatic_close:2026-09-09", "automatic_close", "2026-09-09")
    main = Main.__new__(Main)
    main.store = store
    main.config = {"candidate_limit": 30, "report_candidate_limit": 10, "screen_min_indicator_coverage": 0.8}
    main._daily_retry_after = None
    main.last_daily_scan = None
    main._last_screen_diagnostics = {}
    main._last_screen_report_claimed = True
    main._automatic_delivery_owner = "automatic-close:test"
    main._last_automatic_unknown_count = 0
    main._push_allowed = lambda _origin: True
    diagnostics = {
        "input": 1,
        "tradable": 1,
        "indicator_targets": 1,
        "indicator_raw_batch": 1,
        "indicator_network": 0,
        "indicator_memory_cache": 0,
        "indicator_persistent_cache": 0,
        "indicator_failed": 0,
        "indicator_coverage": 1.0,
        "screen_min_indicator_coverage": 0.8,
        "diagnostics_invocation_id": "automatic_close:2026-09-09",
        "diagnostics_requested_date": "2026-09-09",
        "diagnostics_actual_date": "2026-09-09",
        "market_stats_confirmed": True,
        "market_regime": "risk_on",
        "market_breadth": 1.0,
        "market_advancing": 1,
        "market_declining": 0,
        "market_flat": 0,
        "market_sample_size": 1,
        "market_median_return": 1.0,
        "raw_batch_id": "batch-fixture",
        "raw_generation": 1,
        "diagnostics_raw_batch_id": "batch-fixture",
        "diagnostics_raw_generation": 1,
        "factor_screen_count": 0,
    }

    async def candidates(_limit, *, invocation_id=""):
        assert invocation_id == "automatic_close:2026-09-09"
        return ScreenScoreResult.build([candidate], diagnostics)

    async def snapshot(_requested, _actual, _quotes):
        return {"source": "tushare", "quality": "good", "complete": True}

    async def report_diagnostics(values, **_kwargs):
        return values

    sent = []
    class Context:
        async def send_message(self, origin, payload):
            sent.append((origin, payload))
    main.context = Context()
    main._daily_candidates_result = candidates
    main._snapshot_context = snapshot
    main._report_diagnostics_for_send = report_diagnostics
    result = asyncio.run(main._run_automatic_close_job("2026-09-09", "automatic_close:2026-09-09"))
    assert result["state"] == "completed"
    assert [origin for origin, _payload in sent] == ["origin-a", "origin-b"]
    assert sent[0][1] == sent[1][1]
    publication = store.automatic_close_publication("automatic_close:2026-09-09")
    assert publication["run_id"] == result["run_id"]
    with store._connect() as db:
        run = db.execute("SELECT diagnostics FROM screen_runs WHERE run_id=?", (result["run_id"],)).fetchone()
        saved = __import__("json").loads(str(run[0]))
    assert saved["diagnostics_invocation_id"] == "automatic_close:2026-09-09"
    assert saved["raw_batch_id"] == saved["diagnostics_raw_batch_id"] == "batch-fixture"
    assert store.automatic_close_delivery_summary()["sent"] == 2


def test_delayed_or_wrong_date_snapshot_is_not_published(tmp_path):
    Main, ScreenScoreResult, core, StockStore = _imports()
    store = StockStore(tmp_path / "delay.sqlite3")
    main = Main.__new__(Main)
    main.store = store
    main.config = {"candidate_limit": 30}
    main._daily_retry_after = None
    main._last_screen_diagnostics = {}

    async def candidates(_limit, *, invocation_id=""):
        return ScreenScoreResult.build([], {"diagnostics_invocation_id": invocation_id})

    async def snapshot(_requested, actual, _quotes):
        return {"source": "tushare", "quality": "good", "complete": actual == "2026-09-09"}

    main._daily_candidates_result = candidates
    main._snapshot_context = snapshot
    store.save_daily_quotes("2026-09-08", [core.Quote("600000", "old", 10.0, 9.9, source="tushare")])
    store.begin_job("automatic_close:2026-09-09", "automatic_close", "2026-09-09")
    result = asyncio.run(main._run_automatic_close_job("2026-09-09", "automatic_close:2026-09-09"))
    assert result == {"state": "waiting_snapshot", "requested_date": "2026-09-09", "actual_date": "2026-09-08"}
    assert store.automatic_close_publication("automatic_close:2026-09-09") is None
    with store._connect() as db:
        job = db.execute("SELECT status,error FROM job_runs WHERE job_key='automatic_close:2026-09-09'").fetchone()
        assert tuple(job) == ("failed", "当日完整收盘数据尚未就绪，等待重试")


def test_coverage_or_unconfirmed_market_statistics_fail_closed():
    Main, _, _, _ = _imports()
    snapshot = {"complete": True, "quality": "good"}
    assert Main._automatic_report_failures(
        {"market_stats_confirmed": True, "indicator_coverage": 0.79, "screen_min_indicator_coverage": 0.8}, snapshot
    ) == ["indicator_coverage"]
    assert Main._automatic_report_failures(
        {"market_stats_confirmed": False, "indicator_coverage": 1.0, "screen_min_indicator_coverage": 0.8}, snapshot
    ) == ["market_stats_unconfirmed"]
    assert Main._automatic_report_failures(
        {"market_stats_confirmed": True, "indicator_coverage": 1.0, "screen_min_indicator_coverage": 0.8}, snapshot
    ) == []


def test_duplicate_job_is_blocked_and_stale_or_failed_job_can_resume(tmp_path):
    _, _, _, StockStore = _imports()
    store = StockStore(tmp_path / "jobs.sqlite3")
    key = "automatic_close:2026-09-09"
    assert store.begin_job(key, "automatic_close", "2026-09-09", 60) is True
    assert store.begin_job(key, "automatic_close", "2026-09-09", 60) is False
    store.finish_job(key, "failed", "retry")
    assert store.begin_job(key, "automatic_close", "2026-09-09", 60) is True
    with store._connect() as db:
        db.execute("UPDATE job_runs SET started_at=? WHERE job_key=?", ((datetime.utcnow() - timedelta(minutes=5)).isoformat(), key))
    assert store.begin_job(key, "automatic_close", "2026-09-09", 60) is True


def test_durable_job_backoff_retry_bounds_and_date_rollover_missed(tmp_path):
    _, _, _, StockStore = _imports()
    store = StockStore(tmp_path / "durable-jobs.sqlite3")
    key = "automatic_close:2026-09-09"
    first = store.claim_automatic_close_job(key, "2026-09-09", now=100, max_attempts=2, retry_window_seconds=600)
    assert first["acquired"] is True and first["automatic_attempts"] == 1
    failed = store.finish_automatic_close_job(
        key, status="failed", error="not published", retry_after_seconds=60,
        max_attempts=2, retry_window_seconds=600, now=101,
    )
    assert failed["status"] == "failed" and failed["automatic_next_retry_at"] == 161
    assert store.claim_automatic_close_job(key, "2026-09-09", now=150, max_attempts=2, retry_window_seconds=600)["reason"] == "backoff"
    second = store.claim_automatic_close_job(key, "2026-09-09", now=161, max_attempts=2, retry_window_seconds=600)
    assert second["acquired"] is True and second["automatic_attempts"] == 2
    exhausted = store.finish_automatic_close_job(
        key, status="failed", error="still unavailable", retry_after_seconds=60,
        max_attempts=2, retry_window_seconds=600, now=162,
    )
    assert exhausted["status"] == "missed" and exhausted["automatic_terminal_reason"]

    prior = "automatic_close:2026-09-08"
    store.claim_automatic_close_job(prior, "2026-09-08", now=200)
    missed = store.terminalize_prior_automatic_close_jobs(
        "2026-09-09", reason="crossed date boundary", now=201,
    )
    assert [row["job_key"] for row in missed] == [prior]
    assert store.job_run(prior)["status"] == "missed"


def test_prior_unfinished_snapshot_requests_become_terminal_without_touching_complete_rows(tmp_path):
    _, _, _, StockStore = _imports()
    store = StockStore(tmp_path / "stale-snapshots.sqlite3")
    store.save_snapshot_request("daily_snapshot:2026-09-07", "2026-09-07", state="fetching", attempts=3)
    store.save_snapshot_request(
        "daily_snapshot:2026-09-08", "2026-09-08", actual_trade_date="2026-09-08",
        state="complete", attempts=1, quality="good", terminal=True,
    )
    rows = store.terminalize_prior_snapshot_requests(
        "2026-09-09", reason="crossed date boundary", now=datetime(2026, 9, 9, tzinfo=timezone.utc),
    )
    assert [row["request_id"] for row in rows] == ["daily_snapshot:2026-09-07"]
    assert store.snapshot_request("daily_snapshot:2026-09-07")["state"] == "terminal"
    assert store.snapshot_request("daily_snapshot:2026-09-07")["terminal"] == 1
    assert store.snapshot_request("daily_snapshot:2026-09-08")["state"] == "complete"


def test_tick_uses_persisted_backoff_after_reload_and_does_not_repeat_expensive_failure(tmp_path):
    Main, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "tick-backoff.sqlite3")
    current = datetime(2026, 9, 9, 15, 20, tzinfo=core.CHINA_TZ)
    current_epoch = current.timestamp()
    key = "automatic_close:2026-09-09"
    store.claim_automatic_close_job(key, "2026-09-09", now=current_epoch)
    store.finish_automatic_close_job(
        key,
        status="failed",
        error="expensive report gate",
        retry_after_seconds=300,
        now=current_epoch + 1,
    )

    main = Main.__new__(Main)
    main.store = store
    main.config = {"daily_scan_time": "15:00", "automatic_close_retry_seconds": 300}
    main._daily_retry_after = None
    main.last_daily_scan = None
    main._calendar_open = lambda _date: asyncio.sleep(0, result=True)
    calls = []

    async def run_job(requested, job_key):
        calls.append((requested, job_key))
        return {"state": "ran"}

    main._run_automatic_close_job = run_job
    first = asyncio.run(main._automatic_close_tick(current + timedelta(seconds=20)))
    assert first == {"state": "backoff", "job_key": key}
    assert calls == []
    second = asyncio.run(main._automatic_close_tick(current + timedelta(seconds=302)))
    assert second == {"state": "ran"}
    assert calls == [("2026-09-09", key)]


def test_outbox_preparation_recovery_is_bounded_and_does_not_rescan_completed_publications(tmp_path):
    _, _, core, StockStore = _imports()
    store = StockStore(tmp_path / "bounded-outbox.sqlite3")
    _save_publication(store, core, run_id="run-08", trade_date="2026-09-08", origins=("origin-a",))
    _save_publication(store, core, run_id="run-09", trade_date="2026-09-09", origins=("origin-b",))
    assert store.prepare_all_automatic_close_deliveries(limit=1) == 1
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM automatic_close_publications WHERE outbox_prepared=1").fetchone()[0] == 1
    assert store.prepare_all_automatic_close_deliveries(limit=1) == 1
    assert store.prepare_all_automatic_close_deliveries(limit=1) == 0
