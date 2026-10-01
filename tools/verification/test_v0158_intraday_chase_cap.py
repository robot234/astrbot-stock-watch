"""Synthetic guard against treating a jumped price as an executable entry."""
import asyncio
from datetime import datetime, timedelta

from test_v0139_intraday_m2 import _imports, _main, _quote, _stored


def test_breakout_observation_has_bounded_price_and_known_stored_risk(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    main = _main(Main, StockStore(tmp_path / "cap.sqlite3"))
    stored = _stored(core)
    target = {"stored": stored, "plan_version": "known", "provenance": {"close_candidate"}}

    def breakout(price, row=stored, completed_bar_close=11.03, completed_bar_start=now - timedelta(minutes=1)):
        specs, reasons = main._intraday_signal_specs(
            _quote(core, price, now=now), {**target, "stored": row},
            live_regime="neutral", completed_bar_close=completed_bar_close,
            completed_bar_start=completed_bar_start, now=now)
        return next((s for s in specs if s["signal"] == "confirmed_breakout"), None), reasons

    assert breakout(11.03)[0]["qualifies"] is True
    assert breakout(11.03, completed_bar_close=None, completed_bar_start=None)[0]["awaiting_completed_bar"] is True
    assert breakout(11.03, completed_bar_close=10.9)[0]["qualifies"] is False
    assert breakout(11.03, completed_bar_start=now - timedelta(minutes=4))[0]["qualifies"] is False
    assert breakout(11.5)[0]["qualifies"] is False
    assert breakout(11.5)[0]["evidence"][2] == "chase_ceiling=11.22"
    assert breakout(11.03, {**stored, "risk_level": "unknown"})[1] == ["stored_risk_unknown"]
    assert breakout(11.03, {**stored, "risk_level": "watch_only"})[0]["qualifies"] is False


def test_30_second_cycles_count_distinct_completed_bars_only(tmp_path):
    Main, core, StockStore = _imports()
    store = StockStore(tmp_path / "real-cycle-sequence.sqlite3")
    main = _main(Main, store)
    aggregator = core.MinuteBarAggregator()
    main._dispatch_intraday_delivery = lambda _row: asyncio.sleep(0, result="sent")
    start = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    target = {"stored": _stored(core), "plan_version": "bar-v1", "run_id": "close-run",
              "provenance": {"close_candidate"}}

    async def tick(when, *, price=11.03, risk=None, replay=None):
        quote = _quote(core, price, now=when)
        bar = replay if replay is not None else aggregator.update(quote)
        row = target if risk is None else {**target, "stored": {**target["stored"], "risk_level": risk}}
        triggered, reasons = await main._process_intraday_events(
            "origin-a", quote, row, minute_signal="", invocation_id=f"tick-{when.isoformat()}",
            completed_bar_close=bar.close if bar else None,
            completed_bar_start=bar.start if bar else None,
            live_regime="neutral", now=when)
        state = next((r for r in store.recent_intraday_states("origin-a", 30)
                      if r["signal"] == "confirmed_breakout"), None)
        return triggered, (state or {}).get("consecutive_count", 0), reasons, bar

    async def checks():
        assert (await tick(start))[1] == 0
        assert (await tick(start + timedelta(seconds=30)))[1] == 0
        first = await tick(start + timedelta(minutes=1))
        assert first[1] == 1 and first[0] == 0 and first[3] is not None
        duplicate = await tick(start + timedelta(minutes=1, seconds=5), replay=first[3])
        assert duplicate[1] == 1 and duplicate[0] == 0
        middle = await tick(start + timedelta(minutes=1, seconds=30))
        assert middle[1] == 1 and middle[0] == 0
        second = await tick(start + timedelta(minutes=2))
        assert second[0] == 1 and second[1] == 0
        with store._connect() as db:
            assert db.execute("SELECT COUNT(*) FROM intraday_event_outbox WHERE origin=? AND signal=?",
                              ("origin-a", "confirmed_breakout")).fetchone()[0] == 1

    asyncio.run(checks())


def test_breakout_continuity_breaks_on_price_risk_and_long_gap(tmp_path):
    Main, core, StockStore = _imports()
    store = StockStore(tmp_path / "breaks.sqlite3")
    main = _main(Main, store)
    main._dispatch_intraday_delivery = lambda _row: asyncio.sleep(0, result="sent")
    start = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    target = {"stored": _stored(core), "plan_version": "breaks-v1", "run_id": "close-run",
              "provenance": {"close_candidate"}}

    async def observe(when, *, price=11.03, bar_start=None, risk="eligible"):
        quote = _quote(core, price, now=when)
        row = {**target, "stored": {**target["stored"], "risk_level": risk}}
        await main._process_intraday_events(
            "origin-a", quote, row, minute_signal="", invocation_id=when.isoformat(),
            completed_bar_close=11.03 if bar_start else None, completed_bar_start=bar_start,
            live_regime="neutral", now=when)
        return next(r for r in store.recent_intraday_states("origin-a", 30)
                    if r["signal"] == "confirmed_breakout")["consecutive_count"]

    async def checks():
        assert await observe(start + timedelta(minutes=1), bar_start=start) == 1
        assert await observe(start + timedelta(minutes=1, seconds=30), price=11.5) == 0
        assert await observe(start + timedelta(minutes=2), bar_start=start + timedelta(minutes=1)) == 1
        assert await observe(start + timedelta(minutes=2, seconds=30), risk="unknown") == 0
        assert await observe(start + timedelta(minutes=3), bar_start=start + timedelta(minutes=2)) == 1
        assert await observe(start + timedelta(hours=3), bar_start=start + timedelta(hours=3, minutes=-1)) == 1

    asyncio.run(checks())
