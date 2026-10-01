from __future__ import annotations

import asyncio
import contextlib
import importlib
import sqlite3
import sys
import tempfile
import threading
import time
import types
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from astrbot_stock_watch.storage import SnapshotLeaseLostError, StockStore


def _install_astrbot_stubs() -> None:
    """Provide the small AstrBot surface needed to import Main in isolation."""
    fake_logger = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
        debug=lambda *args, **kwargs: None,
    )
    api = types.ModuleType("astrbot.api")
    api.logger = fake_logger
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


def _main_class():
    _install_astrbot_stubs()
    return importlib.import_module("astrbot_stock_watch.main").Main


def _main_harness(store):
    main_class = _main_class()
    main = main_class.__new__(main_class)
    main.config = {
        "tushare_raw_dataset_key": "tushare_daily",
        "tushare_raw_max_stale_trading_days": 2,
        "tushare_raw_chunk_size": 50,
        "tushare_snapshot_lease_ttl_seconds": 5,
        "tushare_snapshot_lease_wait_seconds": 0,
    }
    main.store = store
    main.raw_dataset_key = "tushare_daily"
    main.raw_session_count = 1
    main.raw_max_stale_trading_days = 2
    main.raw_chunk_size = 50
    main.raw_lookback_days = 60
    main._last_screen_diagnostics = {}
    main._raw_screen_provenance = {}
    main._daily_date_alias = {}
    main._daily_retry_after = None
    main._daily_snapshot_lock = asyncio.Lock()
    return main


def test_cache_only_raw_read_keeps_the_event_loop_moving():
    """A slow raw-cache read must not pause unrelated AstrBot coroutines."""

    class SlowStore:
        def snapshot_request(self, request_id):
            return {}

    main = _main_harness(SlowStore())

    def slow_snapshot(*args, **kwargs):
        time.sleep(0.18)
        return {}, None, {}, "unavailable"

    main._fresh_raw_snapshot = slow_snapshot

    async def verify():
        ticks = 0
        stop = asyncio.Event()

        async def heartbeat():
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.01)

        task = asyncio.create_task(heartbeat())
        await asyncio.sleep(0.02)
        baseline = ticks
        result_task = asyncio.create_task(main._cache_only_tushare_snapshot("2026-08-28"))
        await asyncio.sleep(0.07)
        assert ticks - baseline >= 4
        assert await result_task == ([], False, "2026-08-28")
        stop.set()
        await task

    asyncio.run(verify())


def test_screen_bundle_write_keeps_the_event_loop_moving():
    """A slow transactional report write must not block other bot coroutines."""

    class SlowStore:
        def save_screen_bundle_atomic(self, *args, **kwargs):
            time.sleep(0.18)
            return {"report_claimed": True}

    main = _main_harness(SlowStore())

    async def verify():
        ticks = 0
        stop = asyncio.Event()

        async def heartbeat():
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.01)

        task = asyncio.create_task(heartbeat())
        await asyncio.sleep(0.02)
        baseline = ticks
        record_task = asyncio.create_task(
            main._record_screen(
                "2020-08-28",
                "2020-08-28",
                "tushare",
                [],
                [],
            )
        )
        await asyncio.sleep(0.07)
        assert ticks - baseline >= 4
        assert await record_task
        stop.set()
        await task

    asyncio.run(verify())


def test_symbol_upsert_keeps_the_event_loop_moving():
    """The real Sina parser must batch slow symbol writes off the bot loop."""
    from astrbot_stock_watch.providers import SinaQuoteProvider

    started = threading.Event()
    calls = []
    fields = [""] * 32
    fields[0] = "浦发银行"
    fields[2] = "9.80"
    fields[3] = "10.00"
    fields[8] = "12345"
    fields[9] = "67890"
    fields[30] = "2026-09-08"
    fields[31] = "10:30:00"
    payload = f'var hq_str_sh600000="{",".join(fields)}";'

    class SlowSymbolStore:
        def upsert_stock_symbol(self, code, name, source):
            calls.append((code, name, source))
            started.set()
            time.sleep(0.18)

    class SinaResponse:
        text = payload

        def raise_for_status(self):
            return None

    class RiskResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"data": {"diff": [{
                "f12": "600000", "f43": 1000, "f51": 1000, "f52": 900,
                "suspended": "0",
            }]}}

    class SinaClient:
        async def get(self, url, **kwargs):
            if url.endswith("list=sh600000"):
                return SinaResponse()
            assert url == "https://push2.eastmoney.com/api/qt/ulist.np/get"
            assert kwargs["params"]["secids"] == "1.600000"
            return RiskResponse()

    class SinaSlot:
        async def __aenter__(self):
            return SinaClient()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class SinaHttp:
        def slot(self):
            return SinaSlot()

    provider = SinaQuoteProvider.__new__(SinaQuoteProvider)
    provider.symbol_store = SlowSymbolStore()
    provider.http = SinaHttp()

    async def verify():
        ticks = 0
        stop = asyncio.Event()

        async def heartbeat():
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.01)

        heartbeat_task = asyncio.create_task(heartbeat())
        fetch_task = asyncio.create_task(provider.fetch_quotes(["600000"]))
        while not started.is_set():
            if fetch_task.done():
                await fetch_task
            await asyncio.sleep(0.005)
        baseline = ticks
        await asyncio.sleep(0.07)
        assert ticks - baseline >= 4
        quotes = await fetch_task
        assert len(quotes) == 1
        assert quotes[0].code == "600000"
        assert quotes[0].name == "浦发银行"
        assert quotes[0].price == 10.0
        stop.set()
        await heartbeat_task

    asyncio.run(verify())
    assert calls == [("600000", "浦发银行", "sina")]


def test_intraday_market_snapshot_fetches_batches_with_bounded_concurrency_without_symbol_rewrites():
    from astrbot_stock_watch.providers import SinaQuoteProvider

    provider = SinaQuoteProvider.__new__(SinaQuoteProvider)
    provider.http = types.SimpleNamespace(max_concurrency=2)
    active = 0
    maximum_active = 0
    calls = []

    async def fetch_quotes(codes, *, remember_symbols=True):
        nonlocal active, maximum_active
        calls.append((tuple(codes), remember_symbols))
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.02)
        active -= 1
        return list(codes)

    provider.fetch_quotes = fetch_quotes
    codes = [f"{600000 + index:06d}" for index in range(1200)]
    result = asyncio.run(provider.fetch_intraday_market_snapshot(codes))

    assert result.expected_codes == tuple(codes)
    assert result.batch_count == 3
    assert result.failed_batches == 0
    assert result.quotes == codes
    assert maximum_active == 2
    assert [len(batch) for batch, _remember in calls] == [500, 500, 200]
    assert all(remember is False for _batch, remember in calls)


class _AsyncSlot:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _EvaluationHttp:
    def slot(self):
        return _AsyncSlot()


@pytest.mark.parametrize("slow_operation", ["stage", "publish"])
def test_evaluation_raw_writes_keep_the_event_loop_moving(slow_operation):
    """Slow partition staging and publication must run outside the bot loop."""
    from astrbot_stock_watch.providers import TushareBulkDailyProvider

    started = threading.Event()
    calls = []

    class SlowRawStore:
        def stage_raw_partition(self, *args, **kwargs):
            calls.append("stage")
            if slow_operation == "stage":
                started.set()
                time.sleep(0.18)
            return "partition-1"

        def publish_raw_batch(self, *args, **kwargs):
            calls.append("publish")
            if slow_operation == "publish":
                started.set()
                time.sleep(0.18)
            return {
                "status": "published",
                "dataset_id": "dataset-1",
                "generation": 1,
                "shadow": False,
            }

        def fail_raw_batch(self, *args, **kwargs):
            calls.append("fail")

    provider = TushareBulkDailyProvider.__new__(TushareBulkDailyProvider)
    provider.storage = SlowRawStore()
    provider.http = _EvaluationHttp()
    provider.dataset_key = "tushare_daily_evaluation"
    provider.page_size = 6000
    provider.bj_calendar_policy = "require_bse"
    provider.raw_publish_enabled = True
    provider._begin_raw_batch = lambda *args, **kwargs: ("batch-1", {"page_size": 6000})
    provider._staged_pages = lambda *args, **kwargs: ([], 0, False)
    provider._page_filter_policy = lambda *args, **kwargs: "none"
    provider._record_success = lambda *args, **kwargs: None
    provider.fetch_completed_trade_dates_range = lambda *args, **kwargs: _async_value(["2026-09-02"])
    provider.fetch_universe_evidence = lambda *args, **kwargs: _async_value({
        "universe_version": "fixture-v1",
        "digest": "fixture-digest",
        "effective_date": "2026-09-02",
        "eligible_markets": ["SH"],
        "method": "fixture",
        "source": "fixture",
        "suspension_method": "fixture",
        "bj_calendar_policy": "require_bse",
    })
    provider._coverage_metrics = lambda *args, **kwargs: {
        "coverage_ok": True,
        "errors": [],
        "eligible_markets": ["SH"],
    }

    async def pages(*args, **kwargs):
        yield 0, [{
            "code": "600000",
            "ts_code": "600000.SH",
            "trade_date": "2026-09-02",
            "close": 10.0,
        }]

    provider._iter_pages = pages

    async def verify():
        ticks = 0
        stop = asyncio.Event()

        async def heartbeat():
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.01)

        heartbeat_task = asyncio.create_task(heartbeat())
        fetch_task = asyncio.create_task(
            provider.fetch_evaluation_daily_result("2026-09-01", horizon=1)
        )
        while not started.is_set():
            await asyncio.sleep(0.005)
        baseline = ticks
        await asyncio.sleep(0.07)
        assert ticks - baseline >= 4
        result = await fetch_task
        assert result.complete is True
        assert result.batch_id == "batch-1"
        stop.set()
        await heartbeat_task

    asyncio.run(verify())
    assert calls == ["stage", "publish"]


async def _async_value(value):
    return value


class _LeaseLostStore(StockStore):
    """A real temporary store whose renewal reports ownership loss."""

    def __init__(self, path):
        super().__init__(path)
        self.renew_calls = 0
        self.claim_calls = 0
        self.release_calls = 0

    def claim_snapshot_lease(
        self,
        request_id,
        requested_date,
        owner,
        *,
        ttl_seconds=1800.0,
        now=None,
    ):
        self.claim_calls += 1
        return {
            "acquired": True,
            "owner": owner,
            "fence": 1,
            "lease_active": True,
            "terminal": False,
        }

    def snapshot_lease_state(self, request_id, *, now=None):
        return {
            "lease_active": True,
            "terminal": False,
            "owner": "test-owner",
            "fence": 1,
        }

    def renew_snapshot_lease(
        self,
        request_id,
        owner,
        fence,
        *,
        ttl_seconds=1800.0,
        now=None,
    ):
        self.renew_calls += 1
        return False

    def release_snapshot_lease(self, request_id, owner, fence, *, now=None):
        self.release_calls += 1
        return False

    def save_snapshot_request_owned(
        self,
        request_id,
        requested_date,
        owner,
        fence,
        *,
        state="",
        attempts=0,
        source="",
        quality="unknown",
        **kwargs,
    ):
        raise AssertionError("lost lease must not persist a request")

    def save_tushare_snapshot_owned(
        self,
        request_id,
        requested_date,
        owner,
        fence,
        *,
        quotes,
        actual_trade_date,
        source,
        quality,
        complete,
        state,
        attempts,
        terminal,
        **kwargs,
    ):
        raise AssertionError("lost lease must not persist a snapshot")

class _NoProvider:
    last_diagnostics = {}

    def __init__(self):
        self.provider_calls = 0
        self.eastmoney_calls = 0
        setattr(self, "tushare_token", "fixture-token")

    async def fetch_completed_trade_dates(self, *args, **kwargs):
        self.provider_calls += 1
        raise AssertionError("lost lease must not resolve a provider calendar")

    async def fetch_bulk_daily_result(self, *args, **kwargs):
        self.provider_calls += 1
        raise AssertionError("lost lease must not fetch Tushare data")

    async def fetch_eastmoney_fallback_result(self, *args, **kwargs):
        self.eastmoney_calls += 1
        raise AssertionError("lost lease must not fetch Eastmoney data")


def test_main_lease_loss_fails_closed_before_provider_or_persistence(tmp_path):
    store = _LeaseLostStore(tmp_path / "lease-loss.sqlite3")
    main = _main_harness(store)
    provider = _NoProvider()
    main.quotes = provider
    context = {
        "request_id": "daily_snapshot:2026-08-28",
        "owner": "test-owner",
        "fence": 1,
        "ttl_seconds": 3.0,
        "renew_method": store.renew_snapshot_lease,
    }

    async def verify_renewer_and_guard():
        lost = asyncio.Event()
        renew_task = asyncio.create_task(main._snapshot_lease_renewer(context, lost))
        try:
            await asyncio.wait_for(lost.wait(), timeout=1.5)
        finally:
            renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renew_task
        assert lost.is_set()
        with pytest.raises(SnapshotLeaseLostError) as raised:
            await main._snapshot_lease_guard(context, lost)
        assert str(raised.value) == "snapshot lease was lost"

    asyncio.run(verify_renewer_and_guard())
    before = (
        store.snapshot_request("daily_snapshot:2026-08-28"),
        store.daily_quotes("2026-08-28"),
        store.snapshot_meta("2026-08-28"),
    )
    result = asyncio.run(main._daily_snapshot("2026-08-28"))
    after = (
        store.snapshot_request("daily_snapshot:2026-08-28"),
        store.daily_quotes("2026-08-28"),
        store.snapshot_meta("2026-08-28"),
    )
    assert result == ([], False, "2026-08-28")
    assert main._last_screen_diagnostics["failure_kind"] == "lease_lost"
    assert provider.provider_calls == 0
    assert provider.eastmoney_calls == 0
    assert before == after
    assert store.claim_calls == 1
    assert store.release_calls == 1


class _Response:
    status_code = 200
    request = None

    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body

    def raise_for_status(self):
        return None


class _Transport:
    def __init__(self, body):
        self.body = body
        self.calls = []

    async def post(self, url, json):
        self.calls.append({"url": url, "json": dict(json)})
        return _Response(self.body)


def test_persisted_gateway_state_survives_cache_hit_and_resets_on_success(tmp_path):
    from astrbot_stock_watch.providers import TushareRequestGateway
    from astrbot_stock_watch.storage import StockStore

    token = "fixture-token"
    store = StockStore(tmp_path / "gateway.sqlite3")
    gateway = TushareRequestGateway(
        token,
        storage=store,
        clock=lambda: 100.0,
        enforce_rate_limits=True,
    )
    cached_payload = {
        "api_name": "daily",
        "token": token,
        "params": {"trade_date": "20260828"},
    }
    cached_body = {
        "code": 0,
        "data": {"fields": ["ts_code"], "items": [["600000.SH"]]},
    }
    store.provider_api_state("daily", bucket_limit=30, window_seconds=60)
    store.save_provider_cache(
        "daily",
        gateway.request_digest(cached_payload),
        cached_body,
        ttl_seconds=60,
        now=100.0,
    )
    with store._connect() as db:
        db.execute(
            "UPDATE provider_api_state SET rate_limit_failures=2,failure_streak=3,blocked_until=0,retry_after=0,circuit_open_until=0,last_error='preseeded' WHERE api_name='daily'"
        )
    before_hit = store.provider_api_state("daily", bucket_limit=30, window_seconds=60)
    transport = _Transport(cached_body)

    async def cache_hit():
        return await gateway.request_json(transport, cached_payload, cache_ttl=60)

    assert asyncio.run(cache_hit()) == cached_body
    assert transport.calls == []
    assert store.provider_api_state("daily", bucket_limit=30, window_seconds=60) == before_hit

    success_payload = {
        "api_name": "daily",
        "token": token,
        "params": {"trade_date": "20260829"},
    }
    success_body = {
        "code": 0,
        "msg": "success-marker",
        "data": {"fields": ["ts_code"], "items": [["600001.SH"]]},
    }
    transport.body = success_body

    async def cache_miss_success():
        return await gateway.request_json(transport, success_payload, cache_ttl=0)

    assert asyncio.run(cache_miss_success()) == success_body
    assert len(transport.calls) == 1
    assert transport.calls[0]["json"]["token"] == token
    after_success = store.provider_api_state("daily", bucket_limit=30, window_seconds=60)
    assert after_success["failure_streak"] == 0
    assert after_success["rate_limit_failures"] == 0
    assert after_success["blocked_until"] == 0
    assert after_success["retry_after"] == 0
    assert after_success["circuit_open_until"] == 0
    assert after_success["last_error"] == ""

    with sqlite3.connect(store.path) as db:
        cache_rows = [row[0] for row in db.execute("SELECT body_json FROM provider_cache WHERE api_name='daily'")]
        state_row = db.execute(
            "SELECT last_error,state_digest FROM provider_api_state WHERE api_name='daily'"
        ).fetchone()
    assert len(cache_rows) == 1
    assert all("success-marker" not in body for body in cache_rows)
    assert all(token not in body and '"msg"' not in body for body in cache_rows)
    assert state_row == ("", state_row[1])
    assert token.encode("utf-8") not in store.path.read_bytes()
    assert b"success-marker" not in store.path.read_bytes()
