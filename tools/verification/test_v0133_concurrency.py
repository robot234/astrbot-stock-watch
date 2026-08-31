from __future__ import annotations

import asyncio
import contextlib
import importlib
import sqlite3
import sys
import tempfile
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
