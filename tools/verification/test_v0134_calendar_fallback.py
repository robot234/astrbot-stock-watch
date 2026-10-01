from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
import types
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path

import httpx
import pytest


ROOT = Path(__file__).resolve().parents[2]
# Placeholder credentials for fixtures; constants keep release secret scanning precise.
CONFIGURED_TOKEN = "configured"
FIXTURE_TOKEN = "fixture-token"
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))


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
    for name, module in {
        "astrbot": types.ModuleType("astrbot"),
        "astrbot.api": api,
        "astrbot.api.event": event,
        "astrbot.api.message_components": components,
        "astrbot.api.star": star,
        "astrbot.core": types.ModuleType("astrbot.core"),
        "astrbot.core.utils": types.ModuleType("astrbot.core.utils"),
        "astrbot.core.utils.astrbot_path": path_module,
    }.items():
        sys.modules.setdefault(name, module)


_install_astrbot_stubs()

from astrbot_stock_watch.core import Quote
from astrbot_stock_watch.main import Main
from astrbot_stock_watch.providers import (
    BulkDailyResult,
    SinaQuoteProvider,
    TUSHARE_DAILY_CALENDAR_RULE,
    TUSHARE_DAILY_CALENDAR_SOURCE,
    TUSHARE_DAILY_CALENDAR_SYMBOLS,
    TushareBulkDailyProvider,
    TushareNetworkError,
    TushareCalendarEndpointError,
    TushareCalendarError,
    TusharePermissionError,
    TushareRequestGateway,
    TushareRateLimitError,
)
from astrbot_stock_watch.storage import SnapshotLeaseLostError, StockStore


class _Response:
    status_code = 200

    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self.body


class _EastmoneyRuntime:
    def __init__(self, rows):
        self.rows = list(rows)
        self.requests = []

    @asynccontextmanager
    async def slot(self):
        yield self

    async def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return _Response({"data": {"klines": list(self.rows)}})


class _SequencedEastmoneyRuntime:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = []

    @asynccontextmanager
    async def slot(self):
        yield self

    async def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _DailyCalendarGateway:
    def __init__(self, outcomes):
        self.outcomes = dict(outcomes)
        self.requests = []

    async def request_json(self, client, payload, **kwargs):
        self.requests.append((dict(payload), dict(kwargs)))
        outcome = self.outcomes[payload["params"]["ts_code"]]
        if isinstance(outcome, list):
            outcome = list(outcome)
            return {"code": 0, "data": {"fields": ["ts_code", "trade_date"], "items": [[payload["params"]["ts_code"], value] for value in outcome]}}
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _daily_calendar_provider(outcomes, session_count=3, gateway=None):
    gateway = gateway or _DailyCalendarGateway(outcomes)
    provider = TushareBulkDailyProvider(
        "configured",
        http_runtime=_SlotOnlyRuntime(),
        gateway=gateway,
        session_count=session_count,
    )
    return provider, gateway


def _daily_calendar_body(symbol, dates, *, fields=None, items=None):
    fields = fields or ["ts_code", "trade_date"]
    if items is None:
        items = [[symbol, value] for value in dates]
    return {"code": 0, "data": {"fields": fields, "items": items}}


def _calendar_dates(count: int, latest: date = date(2026, 8, 28)) -> list[str]:
    first = latest - timedelta(days=count - 1)
    return [(first + timedelta(days=index)).isoformat() for index in range(count)]


def _kline_rows(dates: list[str]) -> list[str]:
    return [f"{value},1,1,1,1,1,1,1,1,1,1" for value in dates]


def test_eastmoney_calendar_returns_exact_newest_first_and_bounded_request():
    source_dates = _calendar_dates(120)
    runtime = _EastmoneyRuntime(_kline_rows(source_dates))
    provider = SinaQuoteProvider(http_runtime=runtime, session_count=120)

    result = asyncio.run(provider.fetch_eastmoney_completed_trade_dates("2026-08-28"))

    assert result == list(reversed(source_dates))
    assert len(result) == 120
    assert result[0] == "2026-08-28"
    url, request = runtime.requests[0]
    assert url.endswith("/api/qt/stock/kline/get")
    assert request["params"]["secid"] == "1.000001"
    assert request["params"]["klt"] == "101"
    assert request["params"]["fqt"] == "0"
    assert int(request["params"]["lmt"]) <= 1000
    assert request["params"]["end"] == "20260828"


def test_eastmoney_calendar_retries_one_transport_disconnect_then_succeeds():
    source_dates = _calendar_dates(120)
    runtime = _SequencedEastmoneyRuntime([
        httpx.RemoteProtocolError("peer disconnected"),
        _Response({"data": {"klines": _kline_rows(source_dates)}}),
    ])
    provider = SinaQuoteProvider(http_runtime=runtime, session_count=120)

    result = asyncio.run(provider.fetch_eastmoney_completed_trade_dates("2026-08-28"))

    assert result == list(reversed(source_dates))
    assert len(runtime.requests) == 2


def test_eastmoney_calendar_exhausts_network_retry_as_typed_failure():
    runtime = _SequencedEastmoneyRuntime([
        httpx.RemoteProtocolError("peer disconnected"),
        httpx.RemoteProtocolError("peer disconnected again"),
    ])
    provider = SinaQuoteProvider(http_runtime=runtime, session_count=120)

    with pytest.raises(TushareCalendarEndpointError) as caught:
        asyncio.run(provider.fetch_eastmoney_completed_trade_dates("2026-08-28"))

    assert len(runtime.requests) == 2
    assert caught.value.category == "network"
    assert caught.value.attempts == 2


def test_eastmoney_calendar_validation_failure_is_not_retried():
    rows = _kline_rows(_calendar_dates(120))
    rows[0] = "not-a-date,1,1,1,1,1,1,1,1,1,1"
    runtime = _EastmoneyRuntime(rows)
    provider = SinaQuoteProvider(http_runtime=runtime, session_count=120)

    with pytest.raises(ValueError):
        asyncio.run(provider.fetch_eastmoney_completed_trade_dates("2026-08-28"))

    assert len(runtime.requests) == 1


def test_eastmoney_calendar_cancellation_is_not_retried():
    runtime = _SequencedEastmoneyRuntime([asyncio.CancelledError()])
    provider = SinaQuoteProvider(http_runtime=runtime, session_count=120)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(provider.fetch_eastmoney_completed_trade_dates("2026-08-28"))

    assert len(runtime.requests) == 1


@pytest.mark.parametrize(
    ("dates", "error"),
    [
        (_calendar_dates(119), TushareCalendarError),
        (list(reversed(_calendar_dates(120)))[:-1] + [list(reversed(_calendar_dates(120)))[-2]], ValueError),
        (_calendar_dates(120)[:60] + list(reversed(_calendar_dates(120)[60:])), ValueError),
        (["not-a-date"] + _calendar_dates(119), ValueError),
        (_calendar_dates(119) + ["2026-08-29"], ValueError),
        (_calendar_dates(120, date(2025, 1, 1)), TushareCalendarError),
    ],
    ids=["fewer", "duplicate", "unordered", "malformed", "future", "stale"],
)
def test_eastmoney_calendar_rejects_invalid_windows(dates, error):
    provider = SinaQuoteProvider(
        http_runtime=_EastmoneyRuntime(_kline_rows(dates)),
        session_count=120,
    )

    with pytest.raises(error):
        asyncio.run(provider.fetch_eastmoney_completed_trade_dates("2026-08-28"))


@pytest.mark.parametrize(
    "row",
    [
        f"{_calendar_dates(120)[0]},1",
        f"{_calendar_dates(120)[0]},1,1,1,1,1,1,1,1,1,nan",
        f"{_calendar_dates(120)[0]},1,1,1,1,1,1,1,1,1,1,1",
    ],
    ids=["truncated", "nonfinite", "oversized"],
)
def test_eastmoney_calendar_rejects_incomplete_or_nonfinite_kline_rows(row):
    rows = _kline_rows(_calendar_dates(120))
    rows[0] = row
    provider = SinaQuoteProvider(
        http_runtime=_EastmoneyRuntime(rows),
        session_count=120,
    )

    with pytest.raises(ValueError):
        asyncio.run(provider.fetch_eastmoney_completed_trade_dates("2026-08-28"))


def _main_harness(store: StockStore, session_count: int = 120):
    main = Main.__new__(Main)
    main.config = {
        "tushare_raw_dataset_key": "tushare_daily",
        "tushare_raw_max_stale_trading_days": 2,
        "tushare_raw_chunk_size": 50,
        "tushare_snapshot_lease_ttl_seconds": 5,
        "tushare_snapshot_lease_wait_seconds": 0,
        "calendar_ttl_seconds": 86400,
    }
    main.store = store
    main.raw_dataset_key = "tushare_daily"
    main.raw_session_count = session_count
    main.raw_max_stale_trading_days = 2
    main.raw_chunk_size = 50
    main._last_screen_diagnostics = {}
    main._raw_screen_provenance = {}
    main._daily_date_alias = {}
    main._daily_retry_after = None
    main._daily_snapshot_lock = asyncio.Lock()
    main._snapshot_fallback_owner = None
    main._authorized_fallback_owner = None
    return main


class _CalendarFallbackProvider:
    last_diagnostics = {}

    def __init__(self, dates, eastmoney_error=None):
        setattr(self, "tushare_token", CONFIGURED_TOKEN)
        self.dates = list(dates)
        self.eastmoney_error = eastmoney_error
        self.calendar_calls = 0
        self.eastmoney_calendar_calls = 0
        self.bulk_calls = []

    async def fetch_completed_trade_dates(self, *_args, **_kwargs):
        self.calendar_calls += 1
        raise TushareRateLimitError("trade_cal rate limited")

    async def fetch_eastmoney_completed_trade_dates(self, *_args, **_kwargs):
        self.eastmoney_calendar_calls += 1
        if self.eastmoney_error is not None:
            raise self.eastmoney_error
        return list(self.dates)

    async def fetch_bulk_daily_result(self, *_args, **kwargs):
        self.bulk_calls.append(kwargs)
        return BulkDailyResult(
            [Quote("600000", "Test", 12.0, 11.9, 100000, 0.8, 1000, source="tushare")],
            {},
            "2026-08-28",
            "uncommitted-batch",
            complete=True,
        )

    async def fetch_eastmoney_fallback_result(self, *_args, **_kwargs):
        raise AssertionError("calendar fallback must not become an Eastmoney price fallback")


def test_calendar_failure_uses_em_evidence_then_tushare_bulk_and_persists_request(tmp_path):
    dates = list(reversed(_calendar_dates(120)))
    store = StockStore(tmp_path / "calendar-evidence.sqlite3")
    main = _main_harness(store)
    provider = _CalendarFallbackProvider(dates)
    main.quotes = provider

    result = asyncio.run(main._daily_snapshot("2026-08-28"))

    assert result == ([], False, "2026-08-28")
    assert provider.calendar_calls == 1
    assert provider.eastmoney_calendar_calls == 1
    assert len(provider.bulk_calls) == 1
    evidence = provider.bulk_calls[0]["calendar_evidence"]
    assert evidence["calendar_source"] == "eastmoney_sse_index"
    assert evidence["source"] == "eastmoney_sse_index"
    assert evidence["calendar_session_count"] == 120
    assert evidence["fallback_reason"] == "rate_limit"
    assert evidence["cutoff"] == "2026-08-28"

    with sqlite3.connect(store.path) as db:
        row = db.execute(
            "SELECT COUNT(*), MIN(source), MIN(expires_at > fetched_at) FROM trading_calendar WHERE source=?",
            ("eastmoney_sse_index",),
        ).fetchone()
    assert row == (120, "eastmoney_sse_index", 1)
    request = store.snapshot_request("daily_snapshot:2026-08-28")
    persisted = json.loads(request["calendar_evidence_json"])
    assert persisted["source"] == "eastmoney_sse_index"
    assert persisted["session_count"] == 120
    assert len(persisted["dates"]) == 120


class _SlotOnlyRuntime:
    @asynccontextmanager
    async def slot(self):
        yield self


def _universe_evidence(trade_date: str) -> dict:
    evidence = {
        "evidence_version": 1,
        "method": "stock_basic",
        "source": "verification",
        "universe_version": "verification-universe",
        "effective_date": trade_date,
        "total": 3,
        "markets": {"BJ": 1, "SH": 1, "SZ": 1},
        "eligible_markets": ["BJ", "SH", "SZ"],
        "bj_calendar_policy": "require_bse",
        "status_counts": {"L": 3},
        "suspension_method": "not_available_ratios_only",
        "suspension_evidence": False,
    }
    evidence["digest"] = StockStore._universe_evidence_digest(evidence)
    return evidence


def test_bulk_reuses_em_calendar_evidence_and_calls_tushare_daily_per_date():
    dates = ["2026-08-28", "2026-08-27", "2026-08-26"]
    evidence = {
        "calendar_resolved": True,
        "calendar_target_date": dates[0],
        "calendar_dates": dates,
        "calendar_session_count": len(dates),
        "calendar_source": "eastmoney_sse_index",
        "calendar_policy": "eastmoney_sse_index",
        "calendar_cutoff_date": dates[0],
    }
    provider = TushareBulkDailyProvider(
        "configured",
        http_runtime=_SlotOnlyRuntime(),
        session_count=3,
        min_snapshot_size=1,
        min_overall_coverage=0,
        min_market_coverage=0,
        min_market_median_ratio=0,
    )
    calendar_calls = []
    daily_calls = []

    async def must_not_call_calendar(*_args, **_kwargs):
        calendar_calls.append(True)
        raise AssertionError("resolved EM calendar evidence must skip trade_cal")

    async def universe(trade_date, **_kwargs):
        value = _universe_evidence(trade_date)
        provider.universe_version = value["universe_version"]
        provider.universe_counts = value
        return value

    async def daily(trade_date, *, offset=0, **_kwargs):
        daily_calls.append((trade_date, offset))
        if offset:
            return []
        return [
            {
                "code": code,
                "ts_code": ts_code,
                "trade_date": trade_date,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "pre_close": 10.0,
                "pct_change": 5.0,
                "volume": 100.0,
                "amount": 1000.0,
                "name": code,
                "source": "tushare",
                "price_basis": "unadjusted",
            }
            for code, ts_code in (("600000", "600000.SH"), ("000001", "000001.SZ"), ("920001", "920001.BJ"))
        ]

    provider.fetch_completed_trade_dates = must_not_call_calendar
    provider.fetch_universe_evidence = universe
    provider.fetch_daily_page = daily
    result = asyncio.run(provider.fetch_bulk_daily_result("2026-08-28", calendar_evidence=evidence))

    assert result.complete is True
    assert result.source == "tushare"
    assert result.quality == "good"
    assert calendar_calls == []
    assert [value[0] for value in daily_calls] == dates
    assert all(item.source == "tushare" for item in result.quotes)


def test_invalid_calendar_without_cache_stays_fail_closed_and_retries(tmp_path):
    """A rejected calendar is data-invalid, not transient: no EM price preview."""
    store = StockStore(tmp_path / "invalid-calendar-no-cache.sqlite3")
    main = _main_harness(store, session_count=1)

    class Provider:
        last_diagnostics = {}
        tushare_token = CONFIGURED_TOKEN

        async def fetch_completed_trade_dates(self, *_args, **_kwargs):
            raise TushareCalendarError("completed-session calendar returned no dates")

        async def fetch_bulk_daily_result(self, *_args, **_kwargs):
            raise AssertionError("invalid calendar must not start raw collection")

        async def fetch_eastmoney_fallback_result(self, *_args, **_kwargs):
            raise AssertionError("invalid calendar must not enter Eastmoney price preview")

    main.quotes = Provider()
    quotes, fetched, actual = asyncio.run(main._daily_snapshot("2026-08-28"))

    assert quotes == [] and fetched is False and actual == "2026-08-28"
    assert main._last_screen_diagnostics["calendar_unavailable"] == 1
    assert main._last_screen_diagnostics["failure_kind"] == "calendar"
    assert main._last_screen_diagnostics["fallback_allowed"] is False

    request = store.snapshot_request("daily_snapshot:2026-08-28")
    assert request["state"] == "retry"
    assert request["state"] != "fetching"
    assert request["failure_kind"] == "calendar"
    assert request["next_retry_at"]


def test_daily_permission_failure_is_fail_closed_without_em_price_call(tmp_path):
    store = StockStore(tmp_path / "daily-permission.sqlite3")
    main = _main_harness(store, session_count=1)

    class Provider:
        last_diagnostics = {}

        async def fetch_completed_trade_dates(self, *_args, **_kwargs):
            return ["2026-08-28"]

        async def fetch_bulk_daily_result(self, *_args, **_kwargs):
            raise TusharePermissionError("daily permission denied")

        async def fetch_eastmoney_fallback_result(self, *_args, **_kwargs):
            raise AssertionError("daily permission failure must not call EM prices")

    provider = Provider()
    setattr(provider, "tushare_token", CONFIGURED_TOKEN)
    main.quotes = provider
    with pytest.raises(TusharePermissionError):
        asyncio.run(main._daily_snapshot("2026-08-28"))
    assert main._last_screen_diagnostics["permission_denied"] == 1
    assert main._last_screen_diagnostics["fallback_allowed"] is False


def test_calendar_endpoint_exhaustion_persists_retry_without_raw_rows(tmp_path):
    store = StockStore(tmp_path / "calendar-endpoint.sqlite3")
    main = _main_harness(store, session_count=1)
    provider = _CalendarFallbackProvider(
        [],
        TushareCalendarEndpointError(
            "Eastmoney SSE index calendar endpoint unavailable after 2 attempts",
            category="network",
            attempts=2,
            required_sessions=1,
        ),
    )
    main.quotes = provider

    result = asyncio.run(main._daily_snapshot("2026-08-28"))

    assert result == ([], False, "2026-08-28")
    request = store.snapshot_request("daily_snapshot:2026-08-28")
    assert request["state"] == "retry"
    assert request["state"] != "fetching"
    assert request["failure_kind"] == "calendar_endpoint_unavailable"
    assert request["next_retry_at"]
    assert main._last_screen_diagnostics["calendar_endpoint_unavailable"] == 1
    assert main._last_screen_diagnostics["calendar_failure_kind"] == "network"
    assert main._last_screen_diagnostics["fallback_allowed"] is False

    with sqlite3.connect(store.path) as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                "('raw_batches','batches','raw_partition_bars','partition_bars')"
            )
        }
        counts = {
            table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }
    assert all(value == 0 for value in counts.values())
    assert provider.bulk_calls == []


def test_daily_symbol_consensus_is_calendar_only_and_uses_shared_daily_cache_key():
    dates = ["2026-08-28", "2026-08-27", "2026-08-26", "2026-08-25"]
    provider, gateway = _daily_calendar_provider({symbol: dates for symbol in TUSHARE_DAILY_CALENDAR_SYMBOLS})

    evidence = asyncio.run(provider.fetch_daily_symbol_calendar_evidence("2026-08-28"))

    assert evidence["source"] == TUSHARE_DAILY_CALENDAR_SOURCE
    assert evidence["policy"] == TUSHARE_DAILY_CALENDAR_RULE
    assert evidence["symbols"] == list(TUSHARE_DAILY_CALENDAR_SYMBOLS)
    assert evidence["calendar_dates"] == dates[:3]
    assert evidence["consensus_count"] == 3
    assert len(evidence["per_symbol"]) == 3
    assert all(item["session_count"] == 3 for item in evidence["per_symbol"])
    assert len(gateway.requests) == 3
    assert all(payload["api_name"] == "daily" for payload, _kwargs in gateway.requests)
    assert all(payload["fields"] == "ts_code,trade_date" for payload, _kwargs in gateway.requests)
    assert all(kwargs["cache_ttl"] == 300 for _payload, kwargs in gateway.requests)
    assert all(kwargs["cache_key"].startswith(TUSHARE_DAILY_CALENDAR_SOURCE + ":refresh_v2:") for _payload, kwargs in gateway.requests)


def test_daily_symbol_consensus_accepts_one_symbol_gap_with_two_of_three():
    outcomes = {
        TUSHARE_DAILY_CALENDAR_SYMBOLS[0]: ["2026-08-28", "2026-08-27"],
        TUSHARE_DAILY_CALENDAR_SYMBOLS[1]: ["2026-08-28", "2026-08-27"],
        TUSHARE_DAILY_CALENDAR_SYMBOLS[2]: ["2026-08-28", "2026-08-26"],
    }
    provider, _gateway = _daily_calendar_provider(outcomes, session_count=2)

    evidence = asyncio.run(provider.fetch_daily_symbol_calendar_evidence("2026-08-28"))

    assert evidence["calendar_dates"] == ["2026-08-28", "2026-08-27"]
    assert evidence["consensus_count"] == 2


def test_main_persists_complete_daily_symbol_consensus_evidence(tmp_path):
    source_provider, _gateway = _daily_calendar_provider({symbol: ["2026-08-28", "2026-08-27", "2026-08-26"] for symbol in TUSHARE_DAILY_CALENDAR_SYMBOLS})
    daily_evidence = asyncio.run(source_provider.fetch_daily_symbol_calendar_evidence("2026-08-28"))

    class Provider:
        async def fetch_completed_trade_dates(self, *_args, **_kwargs):
            raise TushareRateLimitError("trade_cal rate limited")

        async def fetch_daily_symbol_calendar_evidence(self, *_args, **_kwargs):
            return daily_evidence

    store = StockStore(tmp_path / "daily-calendar-evidence.sqlite3")
    main = _main_harness(store, session_count=3)
    main.quotes = Provider()

    target, evidence = asyncio.run(main._resolve_tushare_completed_session("2026-08-28"))

    assert target == "2026-08-28"
    assert evidence["source"] == TUSHARE_DAILY_CALENDAR_SOURCE
    assert evidence["symbols"] == list(TUSHARE_DAILY_CALENDAR_SYMBOLS)
    assert len(evidence["per_symbol"]) == 3
    assert all(len(item["digest"]) == 64 for item in evidence["per_symbol"])
    assert evidence["fallback_reason"] == "rate_limit"
    with sqlite3.connect(store.path) as db:
        row = db.execute(
            "SELECT COUNT(*), MIN(source), MAX(is_open) FROM trading_calendar WHERE source=?",
            (TUSHARE_DAILY_CALENDAR_SOURCE,),
        ).fetchone()
    assert row == (3, TUSHARE_DAILY_CALENDAR_SOURCE, 1)


@pytest.mark.parametrize(
    ("outcomes", "error"),
    [
        (
            {
                TUSHARE_DAILY_CALENDAR_SYMBOLS[0]: _daily_calendar_body(TUSHARE_DAILY_CALENDAR_SYMBOLS[0], [], fields=["ts_code"]),
                TUSHARE_DAILY_CALENDAR_SYMBOLS[1]: ["2026-08-28", "2026-08-27"],
                TUSHARE_DAILY_CALENDAR_SYMBOLS[2]: ["2026-08-28", "2026-08-27"],
            },
            ValueError,
        ),
        (
            {
                TUSHARE_DAILY_CALENDAR_SYMBOLS[0]: ["2026-08-28", "2026-08-27", "2026-08-27"],
                TUSHARE_DAILY_CALENDAR_SYMBOLS[1]: ["2026-08-28", "2026-08-27"],
                TUSHARE_DAILY_CALENDAR_SYMBOLS[2]: ["2026-08-28", "2026-08-27"],
            },
            ValueError,
        ),
        (
            {
                TUSHARE_DAILY_CALENDAR_SYMBOLS[0]: ["2026-08-28"],
                TUSHARE_DAILY_CALENDAR_SYMBOLS[1]: ["2026-08-28"],
                TUSHARE_DAILY_CALENDAR_SYMBOLS[2]: ["2026-08-28"],
            },
            TushareCalendarError,
        ),
        (
            {
                TUSHARE_DAILY_CALENDAR_SYMBOLS[0]: ["2026-08-28", "2026-08-27"],
                TUSHARE_DAILY_CALENDAR_SYMBOLS[1]: ["2026-08-26", "2026-08-25"],
                TUSHARE_DAILY_CALENDAR_SYMBOLS[2]: ["2026-08-24", "2026-08-23"],
            },
            TushareCalendarError,
        ),
    ],
    ids=["malformed", "duplicate", "short", "disagreement"],
)
def test_daily_symbol_consensus_fails_closed_for_invalid_windows(outcomes, error):
    provider, _gateway = _daily_calendar_provider(outcomes, session_count=2)

    with pytest.raises(error):
        asyncio.run(provider.fetch_daily_symbol_calendar_evidence("2026-08-28"))


def test_daily_symbol_calendar_provider_failure_uses_em_only_after_daily_failure():
    class Provider(_CalendarFallbackProvider):
        async def fetch_daily_symbol_calendar_evidence(self, *_args, **_kwargs):
            raise TusharePermissionError("daily calendar permission denied")

    store = StockStore(Path(tempfile.mkdtemp()) / "daily-calendar-em.sqlite3")
    main = _main_harness(store, session_count=2)
    provider = Provider(list(reversed(_calendar_dates(120))))
    main.quotes = provider

    target, evidence = asyncio.run(main._resolve_tushare_completed_session("2026-08-28"))

    assert target == "2026-08-28"
    assert provider.calendar_calls == 1
    assert provider.eastmoney_calendar_calls == 1
    assert evidence["source"] == "eastmoney_sse_index"
    assert evidence["fallback_reason"] == "permission"
    assert evidence["calendar_trade_cal_failure_reason"] == "rate_limit"
    assert evidence["calendar_daily_symbol_failure_reason"] == "permission"


def test_daily_symbol_calendar_validation_failure_does_not_use_em():
    class Provider(_CalendarFallbackProvider):
        async def fetch_daily_symbol_calendar_evidence(self, *_args, **_kwargs):
            raise TushareCalendarError("daily calendar disagreement")

        async def fetch_eastmoney_completed_trade_dates(self, *_args, **_kwargs):
            raise AssertionError("invalid daily calendar evidence must not use EM")

    main = _main_harness(StockStore(Path(tempfile.mkdtemp()) / "daily-calendar-invalid.sqlite3"), session_count=2)
    main.quotes = Provider([])

    with pytest.raises(TushareCalendarError):
        asyncio.run(main._resolve_tushare_completed_session("2026-08-28"))


def test_gateway_shares_daily_bucket_but_keeps_calendar_cache_key_distinct(tmp_path):
    store = StockStore(tmp_path / "shared-daily-gateway.sqlite3")

    class Transport:
        def __init__(self):
            self.calls = []

        async def post(self, url, json):
            self.calls.append((url, json))
            if json["params"].get("ts_code"):
                return _Response({"code": 0, "data": {"fields": ["ts_code", "trade_date"], "items": [[json["params"]["ts_code"], "20260828"]]}})
            return _Response({"code": 0, "data": {"fields": ["ts_code"], "items": [["600000.SH"]]}})

    gateway = TushareRequestGateway(
        "fixture-token",
        storage=store,
        clock=lambda: 100.0,
        enforce_rate_limits=True,
    )
    transport = Transport()
    calendar_payload = {
        "api_name": "daily",
        "token": "fixture-token",
        "params": {"ts_code": "600519.SH", "start_date": "20260801", "end_date": "20260828"},
        "fields": "ts_code,trade_date",
    }
    price_payload = {
        "api_name": "daily",
        "token": "fixture-token",
        "params": {"trade_date": "20260828", "limit": 1, "offset": 0},
        "fields": "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
    }

    async def run():
        await gateway.request_json(transport, calendar_payload, api_name="daily", cache_ttl=60, cache_key="calendar:600519.SH")
        await gateway.request_json(transport, price_payload, api_name="daily")
        await gateway.request_json(transport, calendar_payload, api_name="daily", cache_ttl=60, cache_key="calendar:600519.SH")

    asyncio.run(run())

    assert len(transport.calls) == 2
    state = store.provider_api_state("daily", bucket_limit=50, window_seconds=60)
    assert state["bucket_limit"] == 50
    assert state["window_seconds"] == 60
    with sqlite3.connect(store.path) as db:
        cache_rows = db.execute("SELECT api_name,cache_key FROM provider_cache").fetchall()
    assert cache_rows == [("daily", "calendar:600519.SH")]


def test_bulk_reuses_daily_symbol_calendar_evidence_without_calendar_call():
    dates = ["2026-08-28", "2026-08-27", "2026-08-26"]
    per_symbol = [
        {"ts_code": symbol, "row_count": 3, "session_count": 3, "min_date": dates[-1], "max_date": dates[0], "digest": "a" * 64}
        for symbol in TUSHARE_DAILY_CALENDAR_SYMBOLS
    ]
    evidence = {
        "calendar_resolved": True,
        "calendar_target_date": dates[0],
        "calendar_dates": dates,
        "calendar_session_count": 3,
        "calendar_source": TUSHARE_DAILY_CALENDAR_SOURCE,
        "calendar_policy": TUSHARE_DAILY_CALENDAR_RULE,
        "calendar_cutoff_date": dates[0],
        "source": TUSHARE_DAILY_CALENDAR_SOURCE,
        "policy": TUSHARE_DAILY_CALENDAR_RULE,
        "cutoff": dates[0],
        "session_count": 3,
        "symbols": list(TUSHARE_DAILY_CALENDAR_SYMBOLS),
        "per_symbol": per_symbol,
        "consensus_rule": TUSHARE_DAILY_CALENDAR_RULE,
        "consensus_count": 3,
        "window_start": "2026-08-01",
        "window_end": dates[0],
        "dates_digest": "b" * 64,
    }
    provider = TushareBulkDailyProvider(
        "configured",
        http_runtime=_SlotOnlyRuntime(),
        session_count=3,
        min_snapshot_size=1,
        min_overall_coverage=0,
        min_market_coverage=0,
        min_market_median_ratio=0,
    )
    calendar_calls = []

    async def must_not_call_calendar(*_args, **_kwargs):
        calendar_calls.append(True)
        raise AssertionError("daily-symbol evidence must skip a second calendar request")

    async def universe(trade_date, **_kwargs):
        value = _universe_evidence(trade_date)
        provider.universe_version = value["universe_version"]
        provider.universe_counts = value
        return value

    async def daily(trade_date, *, offset=0, **_kwargs):
        if offset:
            return []
        return [
            {
                "code": code,
                "ts_code": ts_code,
                "trade_date": trade_date,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "pre_close": 10.0,
                "pct_change": 5.0,
                "volume": 100.0,
                "amount": 1000.0,
                "name": "Test",
                "source": "tushare",
                "price_basis": "unadjusted",
            }
            for code, ts_code in (("600000", "600000.SH"), ("000001", "000001.SZ"), ("920001", "920001.BJ"))
        ]

    provider.fetch_completed_trade_dates = must_not_call_calendar
    provider.fetch_universe_evidence = universe
    provider.fetch_daily_page = daily
    result = asyncio.run(provider.fetch_bulk_daily_result("2026-08-28", calendar_evidence=evidence))

    assert result.complete is True
    assert result.diagnostics["calendar_reused"] is True
    assert calendar_calls == []
    assert all(item.source == "tushare" for item in result.quotes)


def test_calendar_endpoint_message_does_not_claim_daily_was_unpublished():
    main = Main.__new__(Main)
    main.config = {"report_candidate_limit": 10}
    main._last_screen_diagnostics = {
        "calendar_endpoint_unavailable": 1,
        "failure_kind": "calendar_endpoint_unavailable",
    }

    lines = main._market_report_lines(
        "2026-08-28",
        "2026-08-28",
        [],
        [],
        {"source": "tushare", "quality": "unknown", "complete": False},
    )
    text = "\n".join(lines)

    assert "交易日历证据不可用" in text
    assert "尚未开始 Tushare daily" in text
    assert "Tushare raw 批次缺失" not in text


def _persist_fetching_consensus(store, evidence, *, requested_date="2026-08-28", owner="consensus-owner", now=1000.0):
    request_id = f"daily_snapshot:{requested_date}"
    claim = store.claim_snapshot_lease(request_id, requested_date, owner, ttl_seconds=60, now=now)
    assert claim["acquired"] is True
    store.save_snapshot_request_owned(
        request_id,
        requested_date,
        owner,
        claim["fence"],
        state="fetching",
        attempts=1,
        source="tushare",
        quality="unknown",
        calendar_evidence=evidence,
        provenance={},
        now=now + 0.1,
    )
    return claim


def test_persisted_daily_symbol_consensus_is_reused_before_any_calendar_call(tmp_path):
    source_provider, _gateway = _daily_calendar_provider(
        {symbol: ["2026-08-28", "2026-08-27", "2026-08-26"] for symbol in TUSHARE_DAILY_CALENDAR_SYMBOLS}
    )
    evidence = asyncio.run(source_provider.fetch_daily_symbol_calendar_evidence("2026-08-28"))
    store = StockStore(tmp_path / "persisted-consensus.sqlite3")
    _persist_fetching_consensus(store, evidence)
    main = _main_harness(store, session_count=3)

    class Provider:
        async def fetch_completed_trade_dates(self, *_args, **_kwargs):
            raise AssertionError("valid persisted consensus must skip trade_cal")

        async def fetch_daily_symbol_calendar_evidence(self, *_args, **_kwargs):
            raise AssertionError("valid persisted consensus must skip daily-symbol calls")

    main.quotes = Provider()
    target, reused = asyncio.run(main._resolve_tushare_completed_session("2026-08-28"))

    assert target == "2026-08-28"
    assert reused == evidence


def test_persisted_consensus_rejects_recomputed_top_level_dates_without_two_of_three_support(tmp_path):
    source_provider, _gateway = _daily_calendar_provider(
        {symbol: ["2026-08-28", "2026-08-27", "2026-08-26"] for symbol in TUSHARE_DAILY_CALENDAR_SYMBOLS}
    )
    evidence = asyncio.run(source_provider.fetch_daily_symbol_calendar_evidence("2026-08-28"))
    tampered = json.loads(json.dumps(evidence))
    symbol_dates = [
        ["2026-08-28", "2026-08-27", "2026-08-26"],
        ["2026-08-28", "2026-08-27", "2026-08-25"],
        ["2026-08-28", "2026-08-26", "2026-08-24"],
    ]
    for detail, dates in zip(tampered["per_symbol"], symbol_dates):
        detail["dates"] = dates
        detail["min_date"] = dates[-1]
        detail["max_date"] = dates[0]
        detail["digest"] = source_provider._daily_calendar_digest(dates)
    tampered["calendar_dates"] = ["2026-08-28", "2026-08-27", "2026-08-25"]
    tampered["dates"] = list(tampered["calendar_dates"])
    tampered["calendar_target_date"] = tampered["target"] = tampered["dates"][0]
    tampered["dates_digest"] = source_provider._daily_calendar_digest(tampered["dates"])

    store = StockStore(tmp_path / "persisted-consensus-majority-tamper.sqlite3")
    _persist_fetching_consensus(store, tampered)
    main = _main_harness(store, session_count=3)

    class Provider:
        calendar_calls = 0

        async def fetch_completed_trade_dates(self, *_args, **_kwargs):
            self.calendar_calls += 1
            return ["2026-08-28", "2026-08-27", "2026-08-26"]

    provider = Provider()
    main.quotes = provider
    target, resolved = asyncio.run(main._resolve_tushare_completed_session("2026-08-28"))

    assert provider.calendar_calls == 1
    assert target == "2026-08-28"
    assert resolved["source"] == "tushare"


@pytest.mark.parametrize("field", ["requested_date", "cutoff", "session_count", "dates_digest", "provider_policy_fingerprint", "policy_version"])
def test_mismatched_persisted_consensus_falls_back_to_current_online_resolver(tmp_path, field):
    source_provider, _gateway = _daily_calendar_provider(
        {symbol: ["2026-08-28", "2026-08-27", "2026-08-26"] for symbol in TUSHARE_DAILY_CALENDAR_SYMBOLS}
    )
    evidence = asyncio.run(source_provider.fetch_daily_symbol_calendar_evidence("2026-08-28"))
    broken = dict(evidence)
    if field == "requested_date":
        broken[field] = "2026-08-27"
    elif field == "cutoff":
        broken[field] = "2026-08-27"
    elif field == "session_count":
        broken[field] = 2
    elif field == "dates_digest":
        broken[field] = "0" * 64
    elif field == "provider_policy_fingerprint":
        broken[field] = "1" * 64
    else:
        broken[field] = "legacy"
    store = StockStore(tmp_path / f"mismatch-{field}.sqlite3")
    _persist_fetching_consensus(store, broken)
    main = _main_harness(store, session_count=3)

    class Provider:
        calendar_calls = 0

        async def fetch_completed_trade_dates(self, *_args, **_kwargs):
            self.calendar_calls += 1
            return ["2026-08-28", "2026-08-27", "2026-08-26"]

    provider = Provider()
    main.quotes = provider
    target, resolved = asyncio.run(main._resolve_tushare_completed_session("2026-08-28"))

    assert provider.calendar_calls == 1
    assert target == "2026-08-28"
    assert resolved["source"] == "tushare"


def _claim_for_finalizer(store, *, owner="finalizer-owner", now=1000.0, ttl=60):
    claim = store.claim_snapshot_lease("daily_snapshot:2026-08-28", "2026-08-28", owner, ttl_seconds=ttl, now=now)
    assert claim["acquired"] is True
    return claim


@pytest.mark.parametrize("case", ["untyped", "future", "internal_error"])
def test_snapshot_finalizer_keeps_invalid_or_unexpected_outcomes_fetching(tmp_path, case):
    store = StockStore(tmp_path / f"finalizer-{case}.sqlite3")
    claim = _claim_for_finalizer(store)
    kwargs = {"now": 1001.0}
    if case == "untyped":
        kwargs["result"] = object()
    elif case == "future":
        kwargs["result"] = BulkDailyResult([], {}, "2026-08-29", "future-batch", complete=True)
    else:
        kwargs["exception"] = RuntimeError("unexpected provider failure")
    result = store.finalize_snapshot_request_owned(
        "daily_snapshot:2026-08-28",
        "2026-08-28",
        "finalizer-owner",
        claim["fence"],
        **kwargs,
    )

    assert result["finalized"] is False
    assert store.snapshot_request("daily_snapshot:2026-08-28")["state"] == "fetching"
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0


def test_snapshot_finalizer_re_raises_cancellation_without_leaving_fetching(tmp_path):
    store = StockStore(tmp_path / "finalizer-cancel.sqlite3")
    claim = _claim_for_finalizer(store)
    with pytest.raises(asyncio.CancelledError):
        store.finalize_snapshot_request_owned(
            "daily_snapshot:2026-08-28",
            "2026-08-28",
            "finalizer-owner",
            claim["fence"],
            exception=asyncio.CancelledError(),
            now=1001.0,
        )
    assert store.snapshot_request("daily_snapshot:2026-08-28")["state"] == "fetching"


def test_snapshot_finalizer_classified_state_wins_over_late_cleanup_and_stale_owner(tmp_path):
    store = StockStore(tmp_path / "finalizer-cas.sqlite3")
    first = _claim_for_finalizer(store, owner="old-owner", now=1000.0, ttl=5)
    second = _claim_for_finalizer(store, owner="new-owner", now=1006.0, ttl=60)
    assert second["fence"] > first["fence"]
    with pytest.raises(SnapshotLeaseLostError):
        store.finalize_snapshot_request_owned(
            "daily_snapshot:2026-08-28",
            "2026-08-28",
            "old-owner",
            first["fence"],
            state="complete",
            request_state="complete",
            quotes=[],
            actual_trade_date="2026-08-28",
            complete=True,
            now=1007.0,
        )

    store.finalize_snapshot_request_owned(
        "daily_snapshot:2026-08-28",
        "2026-08-28",
        "new-owner",
        second["fence"],
        diagnostics={"failure_kind": "rate_limit"},
        now=1007.0,
    )
    store.save_snapshot_request_owned(
        "daily_snapshot:2026-08-28",
        "2026-08-28",
        "new-owner",
        second["fence"],
        state="complete",
        attempts=2,
        source="tushare",
        quality="good",
        now=1008.0,
    )
    preserved = store.finalize_snapshot_request_owned(
        "daily_snapshot:2026-08-28",
        "2026-08-28",
        "new-owner",
        second["fence"],
        state="complete",
        request_state="complete",
        quotes=[],
        actual_trade_date="2026-08-28",
        complete=True,
        now=1008.0,
    )
    assert preserved["finalized"] is False
    assert store.snapshot_request("daily_snapshot:2026-08-28")["state"] == "retry"


def test_published_raw_cache_finalizes_stuck_snapshot_and_renders_complete_header(tmp_path):
    store = StockStore(tmp_path / "published-raw-finalize.sqlite3")
    requested_date = "2026-09-08"
    request_id = f"daily_snapshot:{requested_date}"
    store.save_snapshot_request(
        request_id,
        requested_date,
        state="fetching",
        attempts=64,
        source="tushare",
        quality="unknown",
    )
    main = _main_harness(store, session_count=1)
    main.quotes = types.SimpleNamespace(tushare_token=FIXTURE_TOKEN)
    quote = Quote("600000", "Test", 12.0, 11.9, 100000, 0.8, 1000, source="tushare")
    evidence = {
        "calendar_cutoff_date": requested_date,
        "calendar_target_date": requested_date,
        "source": "tushare",
        "policy": "sse_fallback",
    }

    async def resolve_calendar(_requested_date):
        return requested_date, evidence

    async def fresh_raw_snapshot(*_args, **_kwargs):
        return [quote], requested_date, {
            "active_batch_id": "raw-generation-6",
            "batch_id": "raw-generation-6",
            "generation": 6,
            "source": "tushare",
            "basis": "unadjusted",
        }, "ok"

    main._resolve_tushare_completed_session = resolve_calendar
    main._fresh_raw_snapshot_async = fresh_raw_snapshot

    assert asyncio.run(main._daily_snapshot(requested_date)) == ([quote], False, requested_date)
    meta = store.snapshot_meta(requested_date)
    request = store.snapshot_request(request_id)
    assert meta["complete"] == 1
    assert meta["quality"] == "good"
    assert request["state"] == "complete"
    assert request["terminal"] == 1
    assert request["attempts"] >= 65

    snapshot = asyncio.run(main._snapshot_context(requested_date, requested_date, [quote]))
    report = main._market_report_lines(requested_date, requested_date, [quote], [], snapshot)
    assert report[1] == "行情：tushare · 完整｜共 1 只"

    # A repeated recovery sees the terminal request and leaves the finalized
    # generation and its metadata unchanged.
    before = (request["updated_at"], meta["fetched_at"])
    assert asyncio.run(main._daily_snapshot(requested_date)) == ([quote], False, requested_date)
    assert store.snapshot_request(request_id)["state"] == "complete"
    assert store.snapshot_meta(requested_date)["complete"] == 1
    assert (store.snapshot_request(request_id)["updated_at"], store.snapshot_meta(requested_date)["fetched_at"]) == before


@pytest.mark.parametrize(
    ("actual_date", "provenance"),
    [
        ("2026-09-08", {"source": "tushare", "basis": "unadjusted"}),
        ("2026-09-05", {"active_batch_id": "wrong-date", "source": "tushare", "basis": "unadjusted"}),
    ],
    ids=["incomplete", "wrong-date"],
)
def test_raw_cache_without_complete_matching_generation_stays_fail_closed(tmp_path, actual_date, provenance):
    store = StockStore(tmp_path / f"raw-cache-{actual_date}.sqlite3")
    requested_date = "2026-09-08"
    request_id = f"daily_snapshot:{requested_date}"
    store.save_snapshot_request(request_id, requested_date, state="fetching", attempts=64, source="tushare")
    main = _main_harness(store, session_count=1)
    main.quotes = types.SimpleNamespace(tushare_token=FIXTURE_TOKEN)
    quote = Quote("600000", "Test", 12.0, 11.9, 100000, 0.8, 1000, source="tushare")

    async def resolve_calendar(_requested_date):
        return requested_date, {
            "calendar_cutoff_date": requested_date,
            "calendar_target_date": requested_date,
            "source": "tushare",
            "policy": "sse_fallback",
        }

    async def fresh_raw_snapshot(*_args, **_kwargs):
        return [quote], actual_date, dict(provenance), "ok"

    async def incomplete_bulk(*_args, **_kwargs):
        return BulkDailyResult([], {}, actual_date, "", complete=False)

    main._resolve_tushare_completed_session = resolve_calendar
    main._fresh_raw_snapshot_async = fresh_raw_snapshot
    main.quotes.fetch_bulk_daily_result = incomplete_bulk

    quotes, complete, returned_date = asyncio.run(main._daily_snapshot(requested_date))
    assert complete is False
    assert returned_date == requested_date
    if actual_date == requested_date:
        assert quotes == [quote]
    else:
        assert quotes == []
    assert store.snapshot_meta(requested_date) is None
    request = store.snapshot_request(request_id)
    assert request["terminal"] == 0
    assert request["state"] != "complete"
    snapshot = asyncio.run(main._snapshot_context(requested_date, requested_date, quotes))
    assert snapshot["complete"] is False
    assert "完整" not in main._market_report_lines(requested_date, requested_date, quotes, [], snapshot)[1]


def test_market_sync_invocation_emits_one_response_with_correlation_diagnostics(tmp_path):
    store = StockStore(tmp_path / "market-sync-yield.sqlite3")
    main = _main_harness(store, session_count=1)
    main.quotes = types.SimpleNamespace(tushare_token=FIXTURE_TOKEN)

    async def empty_snapshot(_trade_date):
        main._last_screen_diagnostics = {"history_unavailable": True}
        return [], False, "2026-08-28"

    main._daily_snapshot = empty_snapshot

    class Event:
        def plain_result(self, text):
            return text

    async def collect():
        return [item async for item in main.market_sync(Event())]

    outputs = asyncio.run(collect())
    assert len(outputs) == 1
    assert main._last_screen_diagnostics["market_sync_yield_count"] == 1
    assert main._last_screen_diagnostics["market_sync_invocation_id"]
    assert main._last_screen_diagnostics["market_sync_branch"] == "empty_snapshot"
