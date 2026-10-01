from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))
if str(REPOSITORY_ROOT / "tests") not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT / "tests"))

from astrbot_stock_watch.core import CHINA_TZ, Quote
from astrbot_stock_watch.providers import (
    SinaQuoteProvider,
    TushareCircuitOpen,
    TusharePermissionError,
    TushareRateLimitError,
    TushareRealtimeQuoteError,
)
from test_v0133_concurrency import _main_class


NOW = datetime(2026, 9, 16, 10, 0, tzinfo=CHINA_TZ)


def _body(*rows):
    return {
        "code": 0,
        "data": {
            "fields": ["ts_code", "name", "pre_close", "close", "vol", "amount", "trade_time"],
            "items": list(rows),
        },
    }


def _row(code="600000.SH", close=10.2):
    return [code, "Fixture", 10.0, close, 123.0, 4567.0, "2026-09-16 10:00:00"]


class _Gateway:
    rate_limit_block_seconds = 65.0

    def __init__(self, response):
        self.response = response
        self.calls = []

    async def request_api(self, api_name, payload, **kwargs):
        self.calls.append((api_name, dict(payload), dict(kwargs)))
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


def _provider(mode="disabled", response=None):
    gateway = _Gateway(response or _body(_row()))
    provider = SinaQuoteProvider(
        tushare_token="fixture-token",
        gateway=gateway,
        realtime_backup_mode=mode,
        realtime_backup_min_interval_seconds=5,
    )
    provider._realtime_backup_now = lambda: NOW

    async def no_risk_enrichment(_quotes):
        return None

    provider._enrich_risk_fields = no_risk_enrichment
    return provider, gateway


def _sina_quote(code="600000"):
    return Quote(code, "Sina", 10.1, 10.0, 3000.0, 1.0, 100.0, source="sina", provider_ts=NOW, fetched_at=NOW,
                 suspended=False, limit_up=False, limit_down=False)


def test_default_disabled_makes_zero_rt_k_calls():
    provider, gateway = _provider()

    async def primary(_codes, *, remember_symbols=True):
        return [_sina_quote()]

    provider.fetch_quotes = primary
    quotes = asyncio.run(provider.fetch_target_quotes(["600000"]))
    assert quotes[0].source == "sina"
    assert gateway.calls == []
    assert provider.last_realtime_backup_diagnostics["last_status"] == "disabled"


def test_rt_k_parse_keeps_provider_and_collection_timestamps_separate():
    fetched_at = NOW + timedelta(seconds=3)
    quotes = SinaQuoteProvider._parse_tushare_rt_k_response(_body(_row()), ["600000"], fetched_at=fetched_at)
    quote = quotes[0]
    assert quote.code == "600000" and quote.source == "tushare_rt_k"
    assert quote.provider_ts == NOW
    assert quote.fetched_at == fetched_at
    assert quote.suspended is quote.limit_up is quote.limit_down is None


@pytest.mark.parametrize(
    "body",
    [
        _body(_row(), _row()),
        _body(_row("000001.SZ")),
        _body(["600000.SH", "Fixture", 10.0, "bad", 123.0, 4567.0, "2026-09-16 10:00:00"]),
    ],
)
def test_rt_k_malformed_duplicate_or_unrequested_rows_fail_closed(body):
    with pytest.raises(TushareRealtimeQuoteError):
        SinaQuoteProvider._parse_tushare_rt_k_response(body, ["600000"], fetched_at=NOW)


def test_shadow_compares_but_never_replaces_sina():
    provider, gateway = _provider("shadow", _body(_row(close=10.3)))

    async def primary(_codes, *, remember_symbols=True):
        return [_sina_quote()]

    provider.fetch_quotes = primary
    quotes = asyncio.run(provider.fetch_target_quotes(["600000"]))
    assert [quote.source for quote in quotes] == ["sina"]
    assert len(gateway.calls) == 1
    assert provider.last_realtime_backup_diagnostics["last_status"] == "shadow_compared"
    assert provider.last_realtime_backup_diagnostics["price_mismatches"] == 1


def test_fallback_uses_complete_rt_k_only_after_primary_failure_or_empty_result():
    provider, gateway = _provider("fallback", _body(_row()))

    async def failed_primary(_codes, *, remember_symbols=True):
        raise RuntimeError("sina unavailable")

    provider.fetch_quotes = failed_primary
    recovered = asyncio.run(provider.fetch_target_quotes(["600000"]))
    assert [quote.source for quote in recovered] == ["tushare_rt_k"]
    assert len(gateway.calls) == 1

    provider, gateway = _provider("fallback", _body(_row()))

    async def empty_primary(_codes, *, remember_symbols=True):
        return []

    provider.fetch_quotes = empty_primary
    recovered = asyncio.run(provider.fetch_target_quotes(["600000"]))
    assert [quote.source for quote in recovered] == ["tushare_rt_k"]
    assert len(gateway.calls) == 1


def test_fallback_never_merges_partial_sina_batch():
    provider, gateway = _provider("fallback", _body(_row(), _row("000001.SZ", 11.0)))

    async def partial_primary(_codes, *, remember_symbols=True):
        return [_sina_quote("600000")]

    provider.fetch_quotes = partial_primary
    quotes = asyncio.run(provider.fetch_target_quotes(["600000", "000001"]))
    assert [quote.code for quote in quotes] == ["600000"]
    assert [quote.source for quote in quotes] == ["sina"]
    assert gateway.calls == []


def test_fallback_unknown_risk_remains_blocking():
    provider, _gateway = _provider("fallback", _body(_row()))

    async def empty_primary(_codes, *, remember_symbols=True):
        return []

    provider.fetch_quotes = empty_primary
    quote = asyncio.run(provider.fetch_target_quotes(["600000"]))[0]
    Main = _main_class()
    item = Main.__new__(Main)
    item._int = lambda key, default, _minimum, _maximum: {"quote_clock_skew_seconds": 120}.get(key, default)
    item._float = lambda _key, default, _minimum, _maximum: default
    specs, reasons = Main._intraday_signal_specs(item, quote, {}, live_regime="strong", now=NOW)
    assert specs == [] and reasons == ["risk_state_unknown"]


@pytest.mark.parametrize("error", [TusharePermissionError("denied"), TushareRateLimitError("limited"), TushareCircuitOpen("open")])
def test_permission_rate_and_circuit_errors_are_bounded_and_fail_closed(error):
    provider, gateway = _provider("fallback", error)

    async def failed_primary(_codes, *, remember_symbols=True):
        raise RuntimeError("sina unavailable")

    provider.fetch_quotes = failed_primary
    with pytest.raises(RuntimeError, match="sina unavailable"):
        asyncio.run(provider.fetch_target_quotes(["600000"]))
    with pytest.raises(RuntimeError, match="sina unavailable"):
        asyncio.run(provider.fetch_target_quotes(["600000"]))
    assert len(gateway.calls) == 1
    assert provider.last_realtime_backup_diagnostics["last_status"] in {"permission_denied", "rate_or_circuit_blocked"}
