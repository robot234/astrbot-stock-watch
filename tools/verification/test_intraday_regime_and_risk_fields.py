from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2].parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))

from astrbot_stock_watch.core import CHINA_TZ, Quote
from astrbot_stock_watch.providers import SinaQuoteProvider
from test_v0133_concurrency import _main_class


def _quote(when: datetime) -> Quote:
    return Quote("600000", "Test", 10.0, 9.9, 100.0, 1.0, 1000.0, source="sina", provider_ts=when, fetched_at=when)


def test_authoritative_risk_fields_require_explicit_limit_prices():
    quote = _quote(datetime.now(CHINA_TZ))
    SinaQuoteProvider._apply_authoritative_risk_fields([quote], [{"f12": "600000", "f43": 1000, "f51": 1000, "f52": 900}])
    assert (quote.suspended, quote.limit_up, quote.limit_down) == (False, True, False)

    unknown = _quote(datetime.now(CHINA_TZ))
    SinaQuoteProvider._apply_authoritative_risk_fields([unknown], [{"f12": "600000", "f43": 1000}])
    assert (unknown.suspended, unknown.limit_up, unknown.limit_down) == (None, None, None)


def test_regime_refresh_accepts_bounded_future_source_clock_and_clears_active_flag():
    Main = _main_class()
    item = Main.__new__(Main)
    now = datetime.now(CHINA_TZ)
    saved = []

    class Store:
        def active_raw_universe_codes(self, *_args, **_kwargs):
            return ["600000"], {"fresh": True}

        def save_intraday_market_regime_state(self, state):
            saved.append(dict(state))
            return dict(state)

    class Provider:
        async def fetch_intraday_market_snapshot(self, _codes):
            return SimpleNamespace(quotes=[_quote(now + timedelta(seconds=30))], source="sina", failed_batches=0)

    item.store = Store()
    item.quotes = Provider()
    item._intraday_market_refresh_active = False
    item._raw_dataset = lambda: "tushare_daily"
    item._raw_stale_days = lambda: 2
    item._store_call = lambda method, *args, **kwargs: asyncio.to_thread(method, *args, **kwargs)
    item._int = lambda name, default, minimum, maximum: {"market_min_snapshot_size": 1, "intraday_market_max_timestamp_skew_seconds": 90, "intraday_market_max_age_seconds": 150, "quote_clock_skew_seconds": 120}.get(name, default)
    item._float = lambda name, default, minimum, maximum: 0.5 if name == "intraday_market_min_coverage" else default

    state = asyncio.run(Main._refresh_intraday_market_regime_state(item, now))
    assert state["quality"] == "good"
    assert saved and saved[-1]["reason"] == "current_day_verified"
    assert item._intraday_market_refresh_active is False
