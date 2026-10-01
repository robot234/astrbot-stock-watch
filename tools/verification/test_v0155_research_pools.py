from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import importlib
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from test_v0139_intraday_m2 import _imports, _main

Main, core, StockStore = _imports()
selector = importlib.import_module("astrbot_stock_watch.research_selector")


def _history(code: str, name: str, amount: float, *, last_volume: float = 200) -> list[dict]:
    rows = []
    for index in range(22):
        date = datetime(2026, 8, 22) + timedelta(days=index)
        close = 12 + index * 0.08
        rows.append({"trade_date": date.date().isoformat(), "code": code, "name": name,
                     "open": close - 0.05, "high": close + 0.1, "low": close - 0.1,
                     "close": close, "pre_close": close - 0.08, "amount": amount,
                     "volume": last_volume if index == 21 else 100,
                     "pct_change": 0.5, "price_basis": "unadjusted", "source": "tushare"})
    return rows


def _snapshot():
    histories = {
        code: _history(code, name, amount, last_volume=volume)
        for code, name, amount, volume in (
            ("600000", "Alpha", 90000000, 200),
            ("300001", "Beta", 80000000, 200),
            ("688001", "Gamma", 70000000, 200),
            ("600053", "*ST example", 100000000, 200),
            ("601059", "Halted", 200000000, 0),
            ("920001", "Outside", 300000000, 200),
        )
    }
    return {"trade_date": "2026-09-12", "batch_id": "batch-1", "source": "tushare",
            "basis": "unadjusted", "published_at": "2026-09-12T16:00:00+08:00",
            "day_rows": [rows[-1] for rows in histories.values()], "histories": histories}


def test_research_selectors_split_boards_and_preserve_unknown():
    snapshot = _snapshot()
    primary, radar, stats = selector.select_pools(snapshot["day_rows"], snapshot["histories"],
                                                   primary_limit=1, radar_limit=2)
    assert [row["code"] for row in primary + radar] == ["600000", "300001", "688001"]
    assert all(row["risk_level"] == "unknown" for row in primary + radar)
    assert stats["liquid"] == 3 and stats["ranked"] == 3
    broken = dict(snapshot["histories"])
    broken["600000"] = [dict(row, price_basis="qfq") for row in broken["600000"]]
    primary, radar, _ = selector.select_pools(snapshot["day_rows"], broken, primary_limit=1, radar_limit=2)
    assert "600000" not in [row["code"] for row in primary + radar]


def test_freeze_is_immutable_and_does_not_publish_formal_candidates(tmp_path):
    store = StockStore(tmp_path / "research.sqlite3")
    snapshot = _snapshot()
    primary, radar, stats = selector.select_pools(snapshot["day_rows"], snapshot["histories"],
                                                   primary_limit=1, radar_limit=2)
    first = store.save_research_pools(snapshot, primary, radar, stats)
    again = store.save_research_pools(snapshot, [], [], {"changed": True})
    assert first == again
    assert len(first["picks"]["primary"]) == 1 and len(first["picks"]["radar"]) == 2
    assert store.current_research_radar("300001", first["run_id"])
    assert store.current_research_radar("600000", first["run_id"]) is None
    assert store.active_candidate_runs() == []
    assert store.recent_screen_runs() == []
    assert store.schema_version() == 23


def test_plugin_freezes_current_day_only_and_keeps_pools_disjoint(tmp_path):
    store = StockStore(tmp_path / "plugin-research.sqlite3")
    snapshot = _snapshot()
    for index in range(8):
        code = f"600{100 + index}"
        rows = _history(code, "Extra", 60000000 - index * 100000)
        snapshot["day_rows"].append(rows[-1])
        snapshot["histories"][code] = rows
    store.active_raw_batch = lambda **_kwargs: {
        "fresh": True, "quality": "good", "actual_trade_date": snapshot["trade_date"],
        "batch_id": snapshot["batch_id"],
    }
    store.research_input = lambda **_kwargs: snapshot
    main = _main(Main, store)

    async def checks():
        assert await main._freeze_research_pools("2026-09-13") == {}
        frozen = await main._freeze_research_pools("2026-09-12")
        assert frozen["batch_id"] == "batch-1"
        assert await main._freeze_research_pools("2026-09-12") == frozen
        assert await main._current_research_pools("2026-09-13") == frozen
        assert await main._current_research_pools("2026-09-12") == {}
        targets = main._build_intraday_targets({"session": []}, {"session": {}}, {"session"},
                                                {}, [], frozen)["session"]
        primary_codes = {code for code, row in targets.items() if row.get("research_pool") == "primary"}
        radar_codes = {code for code, row in targets.items() if row.get("research_pool") == "radar"}
        assert len(primary_codes) == 7 and len(radar_codes) == 4
        assert not primary_codes.intersection(radar_codes)
        watched = next(iter(radar_codes))
        assert "research_pool" not in main._build_intraday_targets(
            {"session": [watched]}, {"session": {}}, {"session"}, {}, [], frozen
        )["session"][watched]

    asyncio.run(checks())


def test_radar_observation_needs_two_fresh_touches_and_expires(tmp_path):
    store = StockStore(tmp_path / "radar.sqlite3")
    snapshot = _snapshot()
    primary, radar, stats = selector.select_pools(snapshot["day_rows"], snapshot["histories"],
                                                   primary_limit=1, radar_limit=2)
    frozen = store.save_research_pools(snapshot, primary, radar, stats)
    item = frozen["picks"]["radar"][0]
    now = datetime(2026, 9, 13, 10, 0, tzinfo=core.CHINA_TZ)
    quote = core.Quote(item["code"], item["name"], float(item["close"]) * 1.03,
                       amount=9000000, provider_ts=now, fetched_at=now)
    target = {"research": item, "research_run_id": frozen["run_id"],
              "research_trade_date": frozen["trade_date"], "plan_version": frozen["run_id"] + ":radar"}
    main = _main(Main, store)
    sent = []

    async def deliver(outbox):
        sent.append(outbox)
        return "sent"

    main._dispatch_intraday_delivery = deliver
    async def checks():
        assert (await main._process_research_radar("session", quote, target, now=now, invocation_id="i1"))[0] == 0
        later = now + timedelta(seconds=30)
        quote.provider_ts = quote.fetched_at = later
        assert (await main._process_research_radar("session", quote, target, now=later, invocation_id="i2"))[0] == 1
        assert (await main._process_research_radar("session", quote, target, now=later, invocation_id="i3"))[0] == 0
        assert len(sent) == 1 and "不是买入建议" in sent[0]["payload"]
        assert sent[0]["risk_event"] == 0
        async def current(_today):
            return frozen
        main._current_research_pools = current
        delivery = dict(sent[0], created_at=datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                        quote_fetched_at=datetime.now(timezone.utc).isoformat())
        assert await main._validate_intraday_delivery_before_send(delivery) is None
        async def stale(_today):
            return {}
        main._current_research_pools = stale
        assert "expired" in await main._validate_intraday_delivery_before_send(delivery)

    asyncio.run(checks())
    quote.provider_ts = None
    assert not selector.radar_crossed(quote, float(item["close"]), threshold_pct=2, now=now)
    quote.provider_ts = quote.fetched_at = now
    quote.suspended = True
    assert not selector.radar_crossed(quote, float(item["close"]), threshold_pct=2, now=now)
