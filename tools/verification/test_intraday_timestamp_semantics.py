from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2].parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))

from astrbot_stock_watch.core import CHINA_TZ, Quote
from astrbot_stock_watch.providers import SinaQuoteProvider
from test_v013_local import _main_class


def _freshness_harness():
    Main = _main_class()
    item = Main.__new__(Main)
    item._source_health = {"sina": {"rejection_reasons": {}, "rejected_quotes": 0}}
    item._int = lambda name, default, minimum, maximum: default
    return item


def _quote(provider_ts: datetime, fetched_at: datetime) -> Quote:
    return Quote("600000", "Test", 10.0, 9.9, 100.0, 1.0, 1000.0, source="sina", provider_ts=provider_ts, fetched_at=fetched_at)


def test_sina_parser_separates_provider_and_collection_time():
    fields = [""] * 32
    fields[0] = "浦发银行"
    fields[2] = "9.80"
    fields[3] = "10.00"
    fields[8] = "12345"
    fields[9] = "67890"
    fields[30] = "2026-09-14"
    fields[31] = "10:30:00"
    payload = f'var hq_str_sh600000="{",".join(fields)}";'

    class Response:
        text = payload

        def raise_for_status(self):
            return None

    class Client:
        async def get(self, *_args, **_kwargs):
            return Response()

    class Slot:
        async def __aenter__(self):
            return Client()

        async def __aexit__(self, *_args):
            return False

    class Http:
        def slot(self):
            return Slot()

    provider = SinaQuoteProvider.__new__(SinaQuoteProvider)
    provider.http = Http()
    provider.symbol_store = None

    async def remember(_symbols):
        return None

    provider._remember_symbols = remember
    quote = asyncio.run(provider.fetch_quotes(["600000"]))[0]
    assert quote.provider_ts == datetime(2026, 9, 14, 10, 30, tzinfo=CHINA_TZ)
    assert quote.fetched_at != quote.provider_ts
    assert provider.last_diagnostics["accepted_count"] == 1


def test_freshness_rejects_old_provider_timestamp_even_when_collected_now():
    now = datetime(2026, 9, 14, 11, 0, tzinfo=CHINA_TZ)
    item = _freshness_harness()
    Main = _main_class()
    accepted = Main._fresh_quotes(item, [_quote(now - timedelta(minutes=3), now)], now=now)
    assert accepted == []
    assert item._source_health["sina"]["last_rejections"] == {"stale_provider_ts": 1}


def test_freshness_rejects_future_and_mixed_provider_times_with_reasons():
    now = datetime(2026, 9, 14, 11, 0, tzinfo=CHINA_TZ)
    item = _freshness_harness()
    Main = _main_class()
    future = _quote(now + timedelta(minutes=10), now)
    mixed_new = _quote(now - timedelta(seconds=5), now)
    mixed_old = _quote(now - timedelta(minutes=2), now)
    assert Main._fresh_quotes(item, [future], now=now) == []
    assert item._source_health["sina"]["last_rejections"] == {"future_provider_ts": 1}
    assert Main._fresh_quotes(item, [mixed_new, mixed_old], now=now) == []
    assert item._source_health["sina"]["last_rejections"] == {"mixed_provider_ts": 2}
