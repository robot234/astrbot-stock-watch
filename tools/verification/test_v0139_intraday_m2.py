from __future__ import annotations

import asyncio
import importlib
import json
import sys
import tempfile
import types
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
    return main_module.Main, core_module, storage_module.StockStore


def _main(Main, store, context=None, **config):
    main = Main.__new__(Main)
    main.store = store
    main.context = context or types.SimpleNamespace(send_message=None)
    main.config = {
        "quote_interval_seconds": 30,
        "intraday_min_amount": 5_000_000,
        "intraday_confirmation_periods": 2,
        "intraday_confirmation_max_gap_seconds": 90,
        "intraday_cooldown_seconds": 300,
        **config,
    }
    main._intraday_delivery_owner = "intraday:test"
    main._last_intraday_unknown_count = 0
    main._push_allowed = lambda origin: bool(origin)
    main._intraday_health = {
        "last_cycle_at": None, "last_success_at": None, "last_error_at": None,
        "cycles": 0, "successful_cycles": 0, "failed_cycles": 0,
        "stale_quotes": 0, "accepted_quotes": 0, "completed_bars": 0,
        "consecutive_failures": 0, "selected_targets": 0, "candidate_targets": 0,
        "expired_candidates": 0, "focus_targets": 0, "dropped_targets": 0, "triggered_events": 0,
        "last_state": "not_started",
        "last_invocation_id": None, "last_nontrigger_reasons": {},
    }
    main._source_health = {"sina": {"batches": 0, "successes": 0, "failures": 0, "last_success_at": None, "last_error_at": None}}
    main.minute_bars = types.SimpleNamespace(
        symbol_count=lambda: 0,
        bar_count=lambda: 0,
        reset=lambda: None,
        restore=lambda *_args, **_kwargs: None,
        update=lambda _quote: None,
    )
    main._intraday_date = None
    main._minute_restore_pending = False
    main._annotation_task = None
    main._last_annotation_at = None
    return main


def _stored(core, *, run_id="close-run", regime="neutral", risk="eligible"):
    plan = core.PricePlan(
        state="ready", reference_price=10.0, atr=0.5, support=9.5, resistance=11.0,
        attention_low=9.8, attention_high=10.2, confirmation=11.0,
        sell_low=12.0, sell_high=12.5, invalidation=9.0, quality="good",
        evidence=["fixture"], provenance={
            "context": "daily_close", "basis": "unadjusted", "source": "fixture",
            "actual_date": "2026-09-08", "last_date": "2026-09-08",
            "last_close": 10.0, "tolerance_pct": 1.0, "deviation_pct": 0.0,
            "anchor_price": 10.0, "reference_price": 10.0,
        }, validated=True,
    )
    return {
        "code": "600000", "run_id": run_id, "risk_level": risk,
        "price_plan": json.dumps({name: getattr(plan, name) for name in plan.__dataclass_fields__}),
        "factor_payload": json.dumps({"market_regime": regime}),
        "actual_trade_date": "2026-09-08", "valid_until": "2026-10-01T23:59:59+08:00",
    }


def _quote(core, price=10.0, *, now=None, amount=20_000_000, volume_ratio=1.0, pct=1.0, suspended=False, limit_up=False, limit_down=False, st=False):
    when = now or datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    return core.Quote(
        "600000", "浦发银行", price, 9.9, amount, pct, 1_000_000,
        volume_ratio=volume_ratio, history_days=120, atr14=0.5,
        suspended=suspended, limit_up=limit_up, limit_down=limit_down, st=st, fetched_at=when,
    )


def test_targets_merge_watch_candidate_and_focus_with_traceable_plan_version(tmp_path):
    Main, core, StockStore = _imports()
    main = _main(Main, StockStore(tmp_path / "targets.sqlite3"), intraday_focus_codes="600000,000001")
    stored = _stored(core)
    targets = main._build_intraday_targets(
        {"origin-a": ["600000"]}, {"origin-a": {"600000": 8.5}}, {"origin-a"},
        {"600000": stored}, ["600000", "000001"],
    )
    merged = targets["origin-a"]["600000"]
    assert merged["provenance"] == {"watchlist", "close_candidate", "configured_focus"}
    assert merged["cost_price"] == 8.5
    assert merged["run_id"] == "close-run" and merged["valid_until"].startswith("2026-10-01")
    assert merged["plan_version"].startswith("close-run:")
    assert targets["origin-a"]["000001"]["plan_version"] == "watch-v1"


def test_intraday_cycle_filters_expired_candidates_and_observes_session_state(tmp_path):
    Main, core, StockStore = _imports()
    store = StockStore(tmp_path / "cycle.sqlite3")
    store.set_subscription("origin-a", True)
    store.add_watch("origin-a", "600000", 10)
    main = _main(Main, store, intraday_focus_codes="000001", minute_enabled=False)
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    expired = _stored(core)
    expired["code"] = "300001"
    store.latest_screen_candidates_for_intraday = lambda _limit: [expired]

    async def calendar_open(_date):
        return True

    async def no_recovery():
        return {}

    async def invalid_candidate(*_args, **_kwargs):
        return False

    async def no_events(*_args, **_kwargs):
        return 0, ["fixture:no_trigger"]

    async def fetch_quotes(codes):
        rows = []
        for code in codes:
            quote = _quote(core, now=now)
            quote.code = code
            quote.name = code
            rows.append(quote)
        return rows

    main._calendar_open = calendar_open
    main._recover_intraday_deliveries = no_recovery
    main._candidate_is_valid_async = invalid_candidate
    main._process_intraday_events = no_events
    main.quotes = types.SimpleNamespace(fetch_quotes=fetch_quotes)
    main._quote_freshness_clock = lambda cycle_started_at: cycle_started_at
    main._refresh_intraday_market_regime_state = lambda current: asyncio.sleep(0, result={
        "regime": "neutral", "quality": "good", "reason": "fixture_current_market", "source": "sina",
        "source_timestamp": current.astimezone(timezone.utc).isoformat(),
        "quote_timestamp_min": current.astimezone(timezone.utc).isoformat(),
        "quote_timestamp_max": current.astimezone(timezone.utc).isoformat(),
        "sample_size": 1000, "expected_size": 1000, "coverage": 1.0,
    })
    result = asyncio.run(main._intraday_cycle(now))
    assert result["state"] == "completed" and result["targets"] == 2
    assert main._intraday_health["candidate_targets"] == 0
    assert main._intraday_health["expired_candidates"] == 1
    assert main._intraday_health["focus_targets"] == 1

    weekend = datetime(2026, 9, 12, 10, 0, tzinfo=core.CHINA_TZ)
    assert asyncio.run(main._intraday_cycle(weekend)) == {"state": "outside_session", "date": "2026-09-12"}
    assert main._intraday_health["last_state"] == "outside_session"
    assert main._intraday_health["last_nontrigger_reasons"] == {"outside_session": 1}


def test_durable_debounce_cooldown_hysteresis_rearm_reload_and_plan_versions(tmp_path):
    _, _, StockStore = _imports()
    path = tmp_path / "fsm.sqlite3"
    store = StockStore(path)
    args = ("origin-a", "600000", "attention_entry", "plan-v1")
    assert store.observe_intraday_signal(*args, qualifies=True, rearm_ready=False, required=2, cooldown_seconds=300, now=100)["reason"] == "debounce:1/2"
    fired = store.observe_intraday_signal(*args, qualifies=True, rearm_ready=False, required=2, cooldown_seconds=300, now=130)
    assert fired["triggered"] is True and fired["event_sequence"] == 1
    assert store.observe_intraday_signal(*args, qualifies=True, rearm_ready=False, required=2, cooldown_seconds=300, now=160)["reason"] == "awaiting_hysteresis_rearm"
    assert store.observe_intraday_signal(*args, qualifies=False, rearm_ready=True, required=2, cooldown_seconds=300, now=180)["reason"] == "rearmed"
    assert store.observe_intraday_signal(*args, qualifies=True, rearm_ready=False, required=2, cooldown_seconds=300, now=200)["reason"] == "cooldown"
    reloaded = StockStore(path)
    assert reloaded.observe_intraday_signal(*args, qualifies=True, rearm_ready=False, required=2, cooldown_seconds=300, now=450)["reason"] == "debounce:1/2"
    second = reloaded.observe_intraday_signal(*args, qualifies=True, rearm_ready=False, required=2, cooldown_seconds=300, now=480)
    assert second["triggered"] is True and second["event_sequence"] == 2
    new_plan = ("origin-a", "600000", "attention_entry", "plan-v2")
    assert reloaded.observe_intraday_signal(*new_plan, qualifies=True, rearm_ready=False, required=1, now=481)["triggered"] is True

    oscillating = ("origin-a", "600001", "confirmed_breakout", "plan-v1")
    assert reloaded.observe_intraday_signal(*oscillating, qualifies=True, rearm_ready=False, required=2, now=500)["reason"] == "debounce:1/2"
    assert reloaded.observe_intraday_signal(*oscillating, qualifies=False, rearm_ready=False, required=2, now=520)["triggered"] is False
    assert reloaded.observe_intraday_signal(*oscillating, qualifies=True, rearm_ready=False, required=2, now=540)["reason"] == "debounce:1/2"
    assert reloaded.observe_intraday_signal(*oscillating, qualifies=True, rearm_ready=False, required=2, now=560)["triggered"] is True


def test_signal_specs_cover_thresholds_regimes_volume_and_hard_risk(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    main = _main(Main, StockStore(tmp_path / "specs.sqlite3"))
    neutral = {"stored": _stored(core, regime="neutral"), "plan_version": "v1", "provenance": {"close_candidate"}}
    weak = {"stored": _stored(core, regime="risk_off"), "plan_version": "v1", "provenance": {"close_candidate"}}
    specs, reasons = main._intraday_signal_specs(_quote(core, 10.0, now=now, volume_ratio=2.5), neutral, minute_signal="rapid", live_regime="neutral", now=now)
    by_signal = {item["signal"]: item for item in specs}
    assert by_signal["attention_entry"]["qualifies"] is True
    assert by_signal["abnormal_volume"]["qualifies"] is True
    assert by_signal["rapid_move"]["qualifies"] is True
    assert reasons == []
    weak_specs, _ = main._intraday_signal_specs(_quote(core, 11.03, now=now), weak, live_regime="weak", now=now)
    weak_by_signal = {item["signal"]: item for item in weak_specs}
    assert weak_by_signal["confirmed_breakout"]["qualifies"] is False
    assert weak_by_signal["confirmed_breakout"]["required"] == by_signal["confirmed_breakout"]["required"] + 1
    strong = {"stored": _stored(core, regime="risk_on"), "plan_version": "v1", "provenance": {"close_candidate"}}
    hard, hard_reasons = main._intraday_signal_specs(_quote(core, 10.0, now=now, suspended=True), strong, live_regime="risk_on", now=now)
    assert hard[0]["signal"] == "risk_invalidated" and hard[0]["qualifies"] is True
    assert hard_reasons == ["suspended"]
    illiquid, _ = main._intraday_signal_specs(_quote(core, 10.0, now=now, amount=100), neutral, live_regime="neutral", now=now)
    assert illiquid[0]["qualifies"] is True and "illiquid" in illiquid[0]["evidence"]
    for field in ("limit_up", "limit_down"):
        limited, limited_reasons = main._intraday_signal_specs(
            _quote(core, 10.0, now=now, **{field: True}), neutral, live_regime="neutral", now=now,
        )
        assert limited[0]["signal"] == "risk_invalidated"
        assert limited[0]["qualifies"] is True and limited_reasons == ["price_limit"]

    unknown = {"stored": _stored(core, regime="unknown"), "plan_version": "v1", "provenance": {"close_candidate"}}
    unknown_specs, unknown_reasons = main._intraday_signal_specs(_quote(core, 11.5, now=now), unknown, now=now)
    unknown_by_signal = {item["signal"]: item for item in unknown_specs}
    assert "live_market_regime_unavailable" in unknown_reasons
    assert unknown_by_signal["confirmed_breakout"]["qualifies"] is False
    assert unknown_by_signal["invalidation_breach"]["qualifies"] is False


def test_stale_or_unknown_quote_and_invalid_plan_fail_closed(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    main = _main(Main, StockStore(tmp_path / "closed.sqlite3"))
    target = {"stored": _stored(core), "plan_version": "v1", "provenance": {"close_candidate"}}
    assert main._intraday_signal_specs(_quote(core, now=now - timedelta(minutes=3)), target, now=now)[1] == ["stale_quote"]
    assert main._intraday_signal_specs(_quote(core, now=now, suspended=None), target, now=now)[1] == ["risk_state_unknown"]
    invalid = dict(_stored(core)); invalid["price_plan"] = "{}"
    specs, reasons = main._intraday_signal_specs(_quote(core, now=now), {"stored": invalid, "plan_version": "bad", "provenance": {"close_candidate"}}, now=now)
    assert "invalid_or_unverified_plan" in reasons
    assert not any(item["signal"] in {"attention_entry", "confirmed_breakout", "invalidation_breach"} for item in specs)


def test_missing_critical_evidence_breaks_debounce_continuity(tmp_path):
    Main, core, StockStore = _imports()
    store = StockStore(tmp_path / "evidence-gap.sqlite3")
    main = _main(Main, store)
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    target = {"stored": _stored(core), "plan_version": "plan-v1", "run_id": "close-run", "provenance": {"close_candidate"}}
    main._dispatch_intraday_delivery = lambda _row: asyncio.sleep(0, result="sent")
    first = asyncio.run(main._process_intraday_events(
        "origin-a", _quote(core, now=now), target, minute_signal="", invocation_id="intraday-1", live_regime="neutral", now=now,
    ))
    missing = asyncio.run(main._process_intraday_events(
        "origin-a", _quote(core, now=now + timedelta(seconds=30), suspended=None), target,
        minute_signal="", invocation_id="intraday-2", live_regime="neutral", now=now + timedelta(seconds=30),
    ))
    resumed = asyncio.run(main._process_intraday_events(
        "origin-a", _quote(core, now=now + timedelta(seconds=60)), target,
        minute_signal="", invocation_id="intraday-3", live_regime="neutral", now=now + timedelta(seconds=60),
    ))
    assert first[0] == missing[0] == resumed[0] == 0
    assert missing[1] == ["risk_state_unknown"]
    state = next(row for row in store.recent_intraday_states("origin-a", 20) if row["signal"] == "attention_entry")
    assert state["consecutive_count"] == 1 and state["last_reason"] == "debounce:1/2"


def test_payload_contains_required_evidence_and_levels(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    main = _main(Main, StockStore(tmp_path / "payload.sqlite3"))
    target = {"stored": _stored(core), "plan_version": "plan-1", "run_id": "close-run", "provenance": {"watchlist", "close_candidate"}}
    spec = next(item for item in main._intraday_signal_specs(_quote(core, now=now), target, now=now)[0] if item["signal"] == "attention_entry")
    text = main._intraday_event_payload(_quote(core, now=now), target, spec, invocation_id="intraday-run", now=now)
    for value in ("2026-09-09 10:00:00", "600000", "浦发银行", "现价：10.00", "信号：attention_entry", "风险区间 9.80-10.20", "建议买入价 11.00", "失效 9.00", "行情新鲜度：0秒", "plan-1", "intraday-run"):
        assert value in text


class _TimeoutContext:
    def __init__(self, fail_after=0, error=TimeoutError("accepted then timeout")):
        self.calls = 0
        self.fail_after = fail_after
        self.error = error

    async def send_message(self, _origin, _message):
        self.calls += 1
        if self.calls > self.fail_after:
            raise self.error


def _queued(store, *, payload="intraday payload", key="intraday:event:1", sequence=1, run_id="", quote_fetched_at=None, candidate_valid_until=""):
    snapshot_at = datetime.now(timezone.utc).isoformat()
    store.save_intraday_market_regime_state({
        "regime": "neutral", "source": "sina", "source_timestamp": snapshot_at,
        "sample_size": 5550, "expected_size": 5550, "coverage": 1.0,
        "breadth": 0.5, "advancing": 2775, "declining": 2200, "flat": 575,
        "median_return": 0.0, "quote_timestamp_min": snapshot_at,
        "quote_timestamp_max": snapshot_at, "quality": "good", "reason": "fixture",
    })
    return store.enqueue_intraday_event(
        key, origin="origin-a", code="600000", name="浦发银行", signal="attention_entry",
        plan_version="plan-v1", event_sequence=sequence, run_id=run_id,
        invocation_id="intraday-run", payload=payload,
        quote_fetched_at=quote_fetched_at or datetime.now(timezone.utc).isoformat(),
        candidate_valid_until=candidate_valid_until,
        market_regime="neutral", market_snapshot_at=snapshot_at,
    )


def test_ambiguous_send_and_partial_chunks_become_unknown_without_retry(tmp_path):
    Main, _, StockStore = _imports()
    store = StockStore(tmp_path / "unknown.sqlite3")
    store.set_subscription("origin-a", True)
    first = _queued(store)
    context = _TimeoutContext()
    main = _main(Main, store, context)
    assert asyncio.run(main._dispatch_intraday_delivery(first)) == "unknown_delivery"
    assert context.calls == 1
    assert store.intraday_delivery_summary("origin-a")["unknown_delivery"] == 1
    assert store.recoverable_intraday_deliveries(now=10**10) == []

    long_payload = "x" * 550 + "\n" + "y" * 550
    second = _queued(store, payload=long_payload, key="intraday:event:2", sequence=2)
    partial = _TimeoutContext(fail_after=1)
    main = _main(Main, store, partial, push_max_chars=500)
    assert asyncio.run(main._dispatch_intraday_delivery(second)) == "unknown_delivery"
    assert partial.calls == 2


def test_typeerror_has_no_fallback_and_ack_loss_is_unknown(tmp_path):
    Main, _, StockStore = _imports()
    store = StockStore(tmp_path / "ack.sqlite3")
    store.set_subscription("origin-a", True)
    row = _queued(store)
    context = _TimeoutContext(error=TypeError("transport type failure"))
    main = _main(Main, store, context)
    assert asyncio.run(main._dispatch_intraday_delivery(row)) == "unknown_delivery"
    assert context.calls == 1

    row = _queued(store, key="intraday:event:ack-loss", sequence=2)
    sent = _TimeoutContext(fail_after=99)
    main = _main(Main, store, sent)
    original = store.finish_intraday_delivery
    store.finish_intraday_delivery = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ack lost"))
    assert asyncio.run(main._dispatch_intraday_delivery(row)) == "unknown_delivery"
    store.finish_intraday_delivery = original
    assert store.intraday_delivery_summary("origin-a")["unknown_delivery"] == 2


def test_intraday_event_key_and_outbox_are_idempotent_after_sent_ack(tmp_path):
    Main, _, StockStore = _imports()
    store = StockStore(tmp_path / "idempotent.sqlite3")
    store.set_subscription("origin-a", True)
    row = _queued(store)
    context = _TimeoutContext(fail_after=99)
    main = _main(Main, store, context)
    assert asyncio.run(main._dispatch_intraday_delivery(row)) == "sent"
    duplicate = _queued(store)
    assert duplicate["event_key"] == row["event_key"] and duplicate["state"] == "sent"
    assert asyncio.run(main._dispatch_intraday_delivery(duplicate)) == "sent"
    assert context.calls == 1


def test_expired_sending_is_unknown_and_confirmed_pre_send_failure_is_bounded(tmp_path):
    Main, _, StockStore = _imports()
    store = StockStore(tmp_path / "recovery.sqlite3")
    store.set_subscription("origin-a", True)
    row = _queued(store)
    claim = store.claim_intraday_delivery(row["event_key"], "worker", ttl_seconds=5, now=100)
    assert claim["acquired"] is True
    assert store.recoverable_intraday_deliveries(now=106) == []
    assert store.intraday_delivery_summary()["unknown_delivery"] == 1

    failed = _queued(store, key="intraday:event:pre-send", sequence=2)
    main = _main(Main, store, types.SimpleNamespace(send_message=None), intraday_delivery_max_attempts=1)
    assert asyncio.run(main._dispatch_intraday_delivery(failed)) == "cancelled"
    assert store.intraday_delivery_summary()["cancelled"] == 1


class _Event:
    def __init__(self, origin):
        self.unified_msg_origin = origin

    def plain_result(self, text):
        return text


async def _collect(generator):
    return [item async for item in generator]


def test_pause_resume_status_are_idempotent_and_origin_scoped(tmp_path):
    Main, _, StockStore = _imports()
    store = StockStore(tmp_path / "commands.sqlite3")
    store.set_subscription("origin-a", True)
    store.set_subscription("origin-b", True)
    main = _main(Main, store)
    main._push_allowed = lambda origin: origin == "origin-a"
    assert "已暂停" in asyncio.run(_collect(main.pause_intraday(_Event("origin-a"))))[0]
    assert "已经暂停" in asyncio.run(_collect(main.pause_intraday(_Event("origin-a"))))[0]
    assert store.is_subscribed("origin-a") is True
    assert store.is_intraday_enabled("origin-a") is False
    assert store.is_subscribed("origin-b") is True
    assert "已恢复" in asyncio.run(_collect(main.resume_intraday(_Event("origin-a"))))[0]
    assert "已经运行" in asyncio.run(_collect(main.resume_intraday(_Event("origin-a"))))[0]
    assert store.is_subscribed("origin-a") is True
    assert store.is_intraday_enabled("origin-a") is True
    assert "不在推送白名单" in asyncio.run(_collect(main.resume_intraday(_Event("origin-b"))))[0]
    status = asyncio.run(_collect(main.intraday_status(_Event("origin-a"))))[0]
    assert "盯盘状态：运行" in status and "origin-b" not in status

    store.set_subscription("origin-a", False)
    assert "尚未开启提醒" in asyncio.run(_collect(main.resume_intraday(_Event("origin-a"))))[0]
    assert store.is_subscribed("origin-a") is False


def test_paused_intraday_destination_cancels_pending_event_without_send(tmp_path):
    Main, _, StockStore = _imports()
    store = StockStore(tmp_path / "paused-outbox.sqlite3")
    store.set_subscription("origin-a", True)
    store.set_intraday_enabled("origin-a", False)
    row = _queued(store)
    context = _TimeoutContext(fail_after=99)
    main = _main(Main, store, context)
    assert asyncio.run(main._dispatch_intraday_delivery(row)) == "cancelled"
    assert context.calls == 0
    assert store.intraday_delivery_summary("origin-a")["cancelled"] == 1


def test_intraday_processing_does_not_overwrite_full_market_diagnostics(tmp_path):
    Main, core, StockStore = _imports()
    store = StockStore(tmp_path / "isolation.sqlite3")
    store.set_subscription("origin-a", True)
    main = _main(Main, store)
    main._last_screen_diagnostics = {"diagnostics_invocation_id": "full-market-run", "input": 5550}
    main._dispatch_intraday_delivery = lambda _row: asyncio.sleep(0, result="sent")
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    target = {"stored": _stored(core), "plan_version": "plan-v1", "run_id": "close-run", "provenance": {"close_candidate"}}
    first = asyncio.run(main._process_intraday_events("origin-a", _quote(core, now=now), target, minute_signal="", invocation_id="intraday-run", live_regime="neutral", now=now))
    second = asyncio.run(main._process_intraday_events("origin-a", _quote(core, now=now + timedelta(seconds=30)), target, minute_signal="", invocation_id="intraday-run", live_regime="neutral", now=now + timedelta(seconds=30)))
    assert first[0] == 0 and second[0] >= 1
    assert main._last_screen_diagnostics == {"diagnostics_invocation_id": "full-market-run", "input": 5550}


def test_intraday_delivery_can_overlap_immutable_full_market_render(tmp_path):
    Main, core, StockStore = _imports()
    store = StockStore(tmp_path / "concurrent-isolation.sqlite3")
    store.set_subscription("origin-a", True)
    main = _main(Main, store)
    full_diagnostics = {
        "diagnostics_invocation_id": "full-market-run", "input": 5550,
        "market_stats_confirmed": True, "market_regime": "neutral", "market_breadth": 0.58,
    }
    main._last_screen_diagnostics = dict(full_diagnostics)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def paused_delivery(_row):
        entered.set()
        await release.wait()
        return "sent"

    main._dispatch_intraday_delivery = paused_delivery
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    target = {"stored": _stored(core), "plan_version": "plan-v1", "run_id": "close-run", "provenance": {"close_candidate"}}

    async def scenario():
        await main._process_intraday_events(
            "origin-a", _quote(core, now=now), target, minute_signal="",
            invocation_id="intraday-run", live_regime="neutral", now=now,
        )
        task = asyncio.create_task(main._process_intraday_events(
            "origin-a", _quote(core, now=now + timedelta(seconds=30)), target, minute_signal="",
            invocation_id="intraday-run", live_regime="neutral", now=now + timedelta(seconds=30),
        ))
        await entered.wait()
        captured = dict(full_diagnostics)
        release.set()
        result = await task
        return captured, result

    captured, result = asyncio.run(scenario())
    assert result[0] >= 1
    assert captured == full_diagnostics
    assert main._last_screen_diagnostics == full_diagnostics


def _outbox_rows(store, *, code=None, signal=None):
    clauses, values = [], []
    if code is not None:
        clauses.append("code=?")
        values.append(code)
    if signal is not None:
        clauses.append("signal=?")
        values.append(signal)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with store._connect() as db:
        return [dict(row) for row in db.execute("SELECT * FROM intraday_event_outbox" + where + " ORDER BY event_key", values)]


def test_atomic_trigger_rolls_back_when_outbox_intent_cannot_persist_then_recovers_once(tmp_path):
    _, _, StockStore = _imports()
    path = tmp_path / "atomic-intent.sqlite3"
    store = StockStore(path)
    with store._connect() as db:
        db.execute(
            "CREATE TRIGGER abort_intraday_intent BEFORE INSERT ON intraday_event_outbox "
            "BEGIN SELECT RAISE(ABORT, 'fixture outbox failure'); END"
        )
    kwargs = dict(
        qualifies=True, rearm_ready=False, required=1, reason="fixture", name="浦发银行",
        run_id="", invocation_id="atomic-run", payload="atomic payload", now=100,
    )
    try:
        store.observe_and_enqueue_intraday_event("origin-a", "600000", "attention_entry", "watch-v1", **kwargs)
        assert False, "fixture trigger should abort the complete transaction"
    except Exception as exc:
        assert "fixture outbox failure" in str(exc)
    assert store.recent_intraday_states("origin-a", 20) == []
    assert _outbox_rows(store) == []
    with store._connect() as db:
        db.execute("DROP TRIGGER abort_intraday_intent")
    restarted = StockStore(path)
    fired = restarted.observe_and_enqueue_intraday_event(
        "origin-a", "600000", "attention_entry", "watch-v1", **{**kwargs, "now": 130},
    )
    assert fired["triggered"] is True and fired["event_sequence"] == 1
    rows = _outbox_rows(restarted)
    assert len(rows) == 1 and rows[0]["state"] == "pending" and rows[0]["event_sequence"] == 1


def _cycle_main(Main, core, StockStore, tmp_path, *, candidates, quote_batches):
    store = StockStore(tmp_path / "cycle-regression.sqlite3")
    store.set_subscription("origin-a", True)
    main = _main(Main, store, minute_enabled=False)
    main.store.latest_screen_candidates_for_intraday = lambda _limit: list(candidates)
    main._quote_freshness_clock = lambda cycle_started_at: cycle_started_at

    async def current_market(current):
        stamp = current.astimezone(timezone.utc).isoformat()
        return {
            "regime": "neutral", "quality": "good", "reason": "fixture_current_market", "source": "sina",
            "source_timestamp": stamp, "quote_timestamp_min": stamp, "quote_timestamp_max": stamp,
            "sample_size": 1000, "expected_size": 1000, "coverage": 1.0,
        }

    main._refresh_intraday_market_regime_state = current_market

    async def calendar_open(_date):
        return True

    async def candidate_valid(*_args, **_kwargs):
        return True

    async def no_recovery():
        return {}

    async def sent(_row):
        return "sent"

    batches = iter(quote_batches)

    async def fetch_quotes(_codes):
        return next(batches)

    main._calendar_open = calendar_open
    main._candidate_is_valid_async = candidate_valid
    main._recover_intraday_deliveries = no_recovery
    main._dispatch_intraday_delivery = sent
    main.quotes = types.SimpleNamespace(fetch_quotes=fetch_quotes)
    return main, store


def test_actual_cycle_stale_and_missing_quotes_break_confirmation_and_emit_one_data_invalidation(tmp_path):
    Main, core, StockStore = _imports()
    base = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    candidate = _stored(core)
    candidate["name"] = "浦发银行"
    good1 = _quote(core, now=base)
    stale = _quote(core, now=base - timedelta(minutes=3))
    good2 = _quote(core, now=base + timedelta(seconds=60))
    good3 = _quote(core, now=base + timedelta(seconds=90))
    main, store = _cycle_main(Main, core, StockStore, tmp_path, candidates=[candidate], quote_batches=[[good1], [stale], [good2], [good3]])
    first = asyncio.run(main._intraday_cycle(base))
    stale_result = asyncio.run(main._intraday_cycle(base + timedelta(seconds=30)))
    resumed = asyncio.run(main._intraday_cycle(base + timedelta(seconds=60)))
    confirmed = asyncio.run(main._intraday_cycle(base + timedelta(seconds=90)))
    assert first["triggered"] == 0 and stale_result["state"] == "failed_closed"
    assert resumed["triggered"] == 0 and confirmed["triggered"] >= 1
    attention = next(row for row in store.recent_intraday_states("origin-a", 20) if row["signal"] == "attention_entry")
    assert attention["trigger_count"] == 1
    assert len(_outbox_rows(store, code="__market_data__", signal="data_invalidated")) == 1


def test_actual_cycle_partial_missing_symbol_resets_only_the_missing_target(tmp_path):
    Main, core, StockStore = _imports()
    base = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    first = _stored(core)
    second = _stored(core)
    second["code"] = "000001"
    second["name"] = "平安银行"

    def quote(code, when):
        row = _quote(core, now=when)
        row.code = code
        row.name = code
        return row

    main, store = _cycle_main(
        Main, core, StockStore, tmp_path, candidates=[first, second], quote_batches=[
            [quote("600000", base), quote("000001", base)],
            [quote("600000", base + timedelta(seconds=30))],
            [quote("600000", base + timedelta(seconds=60)), quote("000001", base + timedelta(seconds=60))],
        ],
    )
    asyncio.run(main._intraday_cycle(base))
    asyncio.run(main._intraday_cycle(base + timedelta(seconds=30)))
    asyncio.run(main._intraday_cycle(base + timedelta(seconds=60)))
    missing_state = next(
        row for row in store.recent_intraday_states("origin-a", 40)
        if row["code"] == "000001" and row["signal"] == "attention_entry"
    )
    assert missing_state["consecutive_count"] == 1 and missing_state["trigger_count"] == 0
    assert len(_outbox_rows(store, code="000001", signal="data_invalidated")) == 1


def test_actual_cycle_hard_risk_clears_omitted_opportunity_state_before_recovery(tmp_path):
    Main, core, StockStore = _imports()
    base = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    candidate = _stored(core)
    candidate["name"] = "浦发银行"
    main, store = _cycle_main(
        Main, core, StockStore, tmp_path, candidates=[candidate], quote_batches=[
            [_quote(core, now=base)],
            [_quote(core, now=base + timedelta(seconds=30), suspended=True)],
            [_quote(core, now=base + timedelta(seconds=60))],
            [_quote(core, now=base + timedelta(seconds=90))],
        ],
    )
    assert asyncio.run(main._intraday_cycle(base))["triggered"] == 0
    assert asyncio.run(main._intraday_cycle(base + timedelta(seconds=30)))["triggered"] >= 1
    assert asyncio.run(main._intraday_cycle(base + timedelta(seconds=60)))["triggered"] == 0
    final = asyncio.run(main._intraday_cycle(base + timedelta(seconds=90)))
    assert final["triggered"] >= 1
    attention = next(row for row in store.recent_intraday_states("origin-a", 20) if row["signal"] == "attention_entry")
    assert attention["trigger_count"] == 1


def test_expired_candidate_and_unavailable_live_market_emit_durable_risk_not_opportunity(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    expired = _stored(core)
    expired["name"] = "过期候选"
    main, store = _cycle_main(Main, core, StockStore, tmp_path, candidates=[expired], quote_batches=[])

    async def invalid_candidate(*_args, **_kwargs):
        return False

    main._candidate_is_valid_async = invalid_candidate
    main._refresh_intraday_market_regime_state = lambda _current: asyncio.sleep(0, result={
        "regime": "unknown", "quality": "unknown", "reason": "live_market_evidence_unavailable", "source": "",
    })
    result = asyncio.run(main._intraday_cycle(now))
    assert result["state"] == "no_targets" and result["triggered"] == 2
    rows = _outbox_rows(store)
    assert {row["signal"] for row in rows} == {"plan_expired", "market_regime_invalidated"}
    assert all(row["risk_event"] == 1 for row in rows)
    assert all("当前行情：未验证" in row["payload"] for row in rows)


def test_queued_opportunity_is_cancelled_for_stale_quote_or_replaced_plan_without_transport(tmp_path):
    Main, _, StockStore = _imports()
    store = StockStore(tmp_path / "queued-validation.sqlite3")
    store.set_subscription("origin-a", True)
    context = _TimeoutContext(fail_after=99)
    main = _main(Main, store, context)
    stale = _queued(store, key="intraday:stale", quote_fetched_at="2026-09-09T00:00:00+00:00")
    assert asyncio.run(main._dispatch_intraday_delivery(stale)) == "cancelled"
    assert context.calls == 0

    replaced = _queued(store, key="intraday:replaced", sequence=2, run_id="close-run")
    store.current_intraday_candidate = lambda _code: None
    assert asyncio.run(main._dispatch_intraday_delivery(replaced)) == "cancelled"
    assert context.calls == 0
    rows = {row["event_key"]: row for row in _outbox_rows(store)}
    assert "stale before delivery" in rows["intraday:stale"]["last_error"]
    assert "no longer active" in rows["intraday:replaced"]["last_error"]


def test_queued_candidate_expiry_and_old_risk_notice_are_cancelled_without_transport(tmp_path):
    Main, core, StockStore = _imports()
    store = StockStore(tmp_path / "queued-expiry.sqlite3")
    store.set_subscription("origin-a", True)
    context = _TimeoutContext(fail_after=99)
    main = _main(Main, store, context)
    candidate = _stored(core)
    candidate["valid_until"] = "2026-10-01T23:59:59+08:00"
    plan_version = main._intraday_plan_version(candidate)
    snapshot_at = datetime.now(timezone.utc).isoformat()
    store.save_intraday_market_regime_state({
        "regime": "neutral", "source": "sina", "source_timestamp": snapshot_at,
        "sample_size": 5550, "expected_size": 5550, "coverage": 1.0,
        "breadth": 0.5, "advancing": 2775, "declining": 2200, "flat": 575,
        "median_return": 0.0, "quote_timestamp_min": snapshot_at,
        "quote_timestamp_max": snapshot_at, "quality": "good", "reason": "fixture",
    })
    expired = store.enqueue_intraday_event(
        "intraday:expired-candidate", origin="origin-a", code="600000", name="浦发银行",
        signal="attention_entry", plan_version=plan_version, event_sequence=1, run_id="close-run",
        invocation_id="fixture", payload="candidate payload", quote_fetched_at=datetime.now(timezone.utc).isoformat(),
        candidate_valid_until=candidate["valid_until"],
        market_regime="neutral", market_snapshot_at=snapshot_at,
    )
    store.current_intraday_candidate = lambda _code: candidate

    async def invalid_candidate(*_args, **_kwargs):
        return False

    main._candidate_is_valid_async = invalid_candidate
    assert asyncio.run(main._dispatch_intraday_delivery(expired)) == "cancelled"

    risk = store.enqueue_intraday_event(
        "intraday:old-risk", origin="origin-a", code="600000", name="浦发银行",
        signal="data_invalidated", plan_version="watch-v1", event_sequence=1, run_id="",
        invocation_id="fixture", payload="risk warning", risk_event=True,
    )
    with store._connect() as db:
        db.execute("UPDATE intraday_event_outbox SET created_at='2000-01-01T00:00:00' WHERE event_key=?", (risk["event_key"],))
    assert asyncio.run(main._dispatch_intraday_delivery(risk)) == "cancelled"
    assert context.calls == 0
    rows = {row["event_key"]: row for row in _outbox_rows(store)}
    assert "candidate plan expired" in rows["intraday:expired-candidate"]["last_error"]
    assert "risk invalidation expired" in rows["intraday:old-risk"]["last_error"]


def _market_rows(core, now, changes):
    rows = []
    for index, change in enumerate(changes, start=1):
        code = f"{600000 + index:06d}"
        price = 10.0 * (1 + change / 100)
        quote = core.Quote(
            code, code, price, 10.0, 20_000_000, change, 1_000_000,
            source="sina", provider_ts=now, fetched_at=now,
            suspended=False, limit_up=False, limit_down=False,
        )
        rows.append(quote)
    return rows


def _market_main(Main, core, StockStore, tmp_path, *, snapshots, db_name="market-regime.sqlite3", **config):
    store = StockStore(tmp_path / db_name)
    settings = {
        "minute_enabled": False, "market_min_snapshot_size": 1000,
        "intraday_market_min_coverage": 0.75, "intraday_market_regime_confirmations": 2,
        "intraday_market_refresh_seconds": 30, "intraday_market_max_age_seconds": 150,
        "intraday_market_max_timestamp_skew_seconds": 90,
    }
    settings.update(config)
    main = _main(Main, store, **settings)
    codes = [row.code for row in snapshots[0]] if snapshots else ["600001", "600002", "600003", "600004"]
    store.active_raw_universe_codes = lambda *_args, **_kwargs: (codes, {"batch_id": "raw-fixture", "generation": 9, "fresh": True})
    batches = iter(snapshots)

    async def fetch_market(_codes):
        return types.SimpleNamespace(quotes=next(batches), source="sina", failed_batches=0, batch_count=1)

    main.quotes = types.SimpleNamespace(fetch_intraday_market_snapshot=fetch_market)
    main._intraday_market_evaluation_clock = lambda started_at: started_at
    return main, store


def test_whole_market_context_requires_current_complete_coherent_source_timestamps_and_persists_hysteresis(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    strong = _market_rows(core, now, [1.2] * 700 + [-0.1] * 300)
    strong_again = _market_rows(core, now + timedelta(seconds=31), [1.1] * 700 + [-0.1] * 300)
    weak = _market_rows(core, now + timedelta(seconds=62), [-0.4] * 700 + [0.1] * 300)
    weak_again = _market_rows(core, now + timedelta(seconds=93), [-0.6] * 700 + [0.1] * 300)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[strong, strong_again, weak, weak_again])
    first = asyncio.run(main._resolve_intraday_market_context(now))
    second = asyncio.run(main._resolve_intraday_market_context(now + timedelta(seconds=31)))
    third = asyncio.run(main._resolve_intraday_market_context(now + timedelta(seconds=62)))
    fourth = asyncio.run(main._resolve_intraday_market_context(now + timedelta(seconds=93)))
    assert first["quality"] == "good" and first["regime"] == "unknown" and first["pending_regime"] == "strong"
    assert second["regime"] == "strong" and second["sample_size"] == 1000 and second["coverage"] == 1.0
    assert third["regime"] == "strong" and third["pending_regime"] == "weak"
    assert fourth["regime"] == "weak"
    reloaded = StockStore(store.path).intraday_market_regime_state()
    assert reloaded["regime"] == "weak" and reloaded["source"] == "sina"


def test_whole_market_context_rejects_coverage_source_and_mixed_time_gaps(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    rows = _market_rows(core, now, [0.5] * 600 + [-0.3] * 400)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[rows[:700]])
    incomplete = asyncio.run(main._resolve_intraday_market_context(now))
    assert incomplete["regime"] == "unknown" and incomplete["reason"] == "intraday_market_coverage_inadequate"

    mixed = _market_rows(core, now, [0.5] * 600 + [-0.3] * 400)
    mixed[-1].provider_ts = mixed[-1].fetched_at = now - timedelta(minutes=3)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[mixed])
    rejected = asyncio.run(main._resolve_intraday_market_context(now))
    assert rejected["regime"] == "unknown" and rejected["reason"] in {"intraday_market_timestamp_gap", "intraday_market_stale"}

    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[rows], db_name="market-regime-post-fetch.sqlite3")
    async def bad_source(_codes):
        return types.SimpleNamespace(quotes=rows, source="eastmoney", failed_batches=0, batch_count=1)
    main.quotes.fetch_intraday_market_snapshot = bad_source
    rejected_source = asyncio.run(main._resolve_intraday_market_context(now))
    assert rejected_source["reason"] == "intraday_market_source_unverified"


def test_actual_cycle_uses_whole_market_state_and_queued_opportunity_is_cancelled_after_environment_turns_unavailable(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    market = _market_rows(core, now, [1.2] * 700 + [-0.1] * 300)
    market_again = _market_rows(core, now + timedelta(seconds=31), [1.2] * 700 + [-0.1] * 300)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[market, market_again], intraday_confirmation_periods=1)
    store.set_subscription("origin-a", True)
    candidate = _stored(core)
    store.latest_screen_candidates_for_intraday = lambda _limit: [candidate]
    main._calendar_open = lambda _date: asyncio.sleep(0, result=True)
    main._candidate_is_valid_async = lambda *_args, **_kwargs: asyncio.sleep(0, result=True)
    main._recover_intraday_deliveries = lambda: asyncio.sleep(0, result={})
    target_quote = _quote(core, now=now)
    main.quotes.fetch_quotes = lambda _codes: asyncio.sleep(0, result=[target_quote])
    main._quote_freshness_clock = lambda cycle_started_at: cycle_started_at
    main._dispatch_intraday_delivery = lambda _row: asyncio.sleep(0, result="sent")
    first = asyncio.run(main._intraday_cycle(now))
    target_quote.fetched_at = target_quote.provider_ts = now + timedelta(seconds=31)
    second = asyncio.run(main._intraday_cycle(now + timedelta(seconds=31)))
    assert first["triggered"] >= 1 and second["triggered"] >= 1
    opportunity = next(row for row in _outbox_rows(store) if row["risk_event"] == 0)
    assert opportunity["market_regime"] == "strong" and opportunity["market_snapshot_at"]

    state = store.intraday_market_regime_state()
    state.update({"regime": "unknown", "quality": "unknown", "reason": "provider_error", "source_timestamp": ""})
    store.save_intraday_market_regime_state(state)
    sender = _TimeoutContext(fail_after=99)
    main.context = sender
    main._dispatch_intraday_delivery = Main._dispatch_intraday_delivery.__get__(main, Main)
    assert asyncio.run(main._dispatch_intraday_delivery(opportunity)) == "cancelled"
    assert sender.calls == 0


def test_actual_cycle_provider_error_invalidates_environment_without_opportunity(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    seed = _market_rows(core, now, [0.2] * 600 + [-0.1] * 400)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[seed])
    store.set_subscription("origin-a", True)
    candidate = _stored(core)
    store.latest_screen_candidates_for_intraday = lambda _limit: [candidate]
    main._calendar_open = lambda _date: asyncio.sleep(0, result=True)
    main._candidate_is_valid_async = lambda *_args, **_kwargs: asyncio.sleep(0, result=True)
    main._recover_intraday_deliveries = lambda: asyncio.sleep(0, result={})

    async def provider_error(_codes):
        raise RuntimeError("fixture provider failure")

    main.quotes.fetch_intraday_market_snapshot = provider_error
    main.quotes.fetch_quotes = lambda _codes: asyncio.sleep(0, result=[_quote(core, now=now)])
    main._quote_freshness_clock = lambda cycle_started_at: cycle_started_at
    main._dispatch_intraday_delivery = lambda _row: asyncio.sleep(0, result="sent")
    result = asyncio.run(main._intraday_cycle(now))
    rows = _outbox_rows(store)
    assert result["state"] == "failed_closed" and result["reason"] == "intraday_market_provider_error"
    assert {row["signal"] for row in rows} == {"market_regime_invalidated"}
    assert all(row["risk_event"] == 1 for row in rows)
    assert not any(row["risk_event"] == 0 for row in rows)


def test_live_market_regime_escalates_weak_to_risk_off_and_recovers_only_after_confirmation(tmp_path):
    Main, core, StockStore = _imports()
    now = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    severe = _market_rows(core, now, [-2.0] * 800 + [0.1] * 200)
    severe_again = _market_rows(core, now + timedelta(seconds=31), [-2.0] * 800 + [0.1] * 200)
    recovering = _market_rows(core, now + timedelta(seconds=62), [-0.6] * 650 + [0.3] * 350)
    recovering_again = _market_rows(core, now + timedelta(seconds=93), [-0.6] * 650 + [0.3] * 350)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[severe, severe_again, recovering, recovering_again])
    store.save_intraday_market_regime_state({
        "regime": "weak", "pending_regime": "unknown", "pending_count": 0,
        "source": "sina", "source_timestamp": (now - timedelta(seconds=31)).astimezone(timezone.utc).isoformat(),
        "sample_size": 1000, "expected_size": 1000, "coverage": 1.0, "breadth": 0.35,
        "advancing": 350, "declining": 650, "flat": 0, "median_return": -0.4,
        "quote_timestamp_min": (now - timedelta(seconds=31)).astimezone(timezone.utc).isoformat(),
        "quote_timestamp_max": (now - timedelta(seconds=31)).astimezone(timezone.utc).isoformat(),
        "quality": "good", "reason": "fixture",
    })
    first = asyncio.run(main._resolve_intraday_market_context(now))
    second = asyncio.run(main._resolve_intraday_market_context(now + timedelta(seconds=31)))
    third = asyncio.run(main._resolve_intraday_market_context(now + timedelta(seconds=62)))
    fourth = asyncio.run(main._resolve_intraday_market_context(now + timedelta(seconds=93)))
    assert first["regime"] == "weak" and first["pending_regime"] == "risk_off"
    assert second["regime"] == "risk_off"
    assert third["regime"] == "risk_off" and third["pending_regime"] == "weak"
    assert fourth["regime"] == "weak"


def test_market_evaluation_clock_accepts_post_fetch_rows_but_rejects_future_and_latency_stale(tmp_path):
    Main, core, StockStore = _imports()
    started = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    completed = started + timedelta(seconds=45)
    rows = _market_rows(core, completed, [0.5] * 600 + [-0.2] * 400)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[rows])
    main._intraday_market_evaluation_clock = lambda _started: completed
    accepted = asyncio.run(main._resolve_intraday_market_context(started))
    assert accepted["quality"] == "good" and accepted["quote_timestamp_max"].startswith("2026-09-09T02:00:45")

    future_accepted = _market_rows(core, completed + timedelta(seconds=120), [0.5] * 600 + [-0.2] * 400)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[future_accepted], db_name="market-regime-future-accepted.sqlite3")
    main._intraday_market_evaluation_clock = lambda _started: completed
    accepted_future = asyncio.run(main._resolve_intraday_market_context(started))
    assert accepted_future["quality"] == "good"

    future_rejected = _market_rows(core, completed + timedelta(seconds=121), [0.5] * 600 + [-0.2] * 400)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[future_rejected], db_name="market-regime-future-rejected.sqlite3")
    main._intraday_market_evaluation_clock = lambda _started: completed
    rejected_future = asyncio.run(main._resolve_intraday_market_context(started))
    assert rejected_future["reason"] == "intraday_market_future_timestamp"

    stale = _market_rows(core, started, [0.5] * 600 + [-0.2] * 400)
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[stale], db_name="market-regime-stale.sqlite3")
    main._intraday_market_evaluation_clock = lambda _started: started + timedelta(seconds=151)
    rejected_stale = asyncio.run(main._resolve_intraday_market_context(started))
    assert rejected_stale["reason"] == "intraday_market_stale"


def test_market_freshness_and_queued_validation_use_oldest_quote_boundary(tmp_path):
    Main, core, StockStore = _imports()
    start = datetime(2026, 9, 9, 10, 0, tzinfo=core.CHINA_TZ)
    oldest = start - timedelta(seconds=89)
    latest = start
    rows = _market_rows(core, latest, [0.5] * 600 + [-0.2] * 400)
    rows[0].provider_ts = rows[0].fetched_at = oldest
    main, store = _market_main(Main, core, StockStore, tmp_path, snapshots=[rows], intraday_market_max_age_seconds=150)
    accepted = asyncio.run(main._resolve_intraday_market_context(start))
    assert accepted["quality"] == "good" and main._intraday_market_state_is_fresh(accepted, start + timedelta(seconds=30))
    delivery = {
        "event_key": "fixture", "origin": "origin-a", "signal": "attention_entry", "risk_event": 0,
        "created_at": (start + timedelta(seconds=30)).astimezone(timezone.utc).isoformat(),
        "market_regime": accepted["regime"] or "neutral", "market_snapshot_at": accepted["source_timestamp"],
        "quote_fetched_at": latest.astimezone(timezone.utc).isoformat(), "run_id": "",
    }
    # The first observation awaits regime confirmation, so use a confirmed
    # neutral state for the isolated delivery-boundary assertion.
    accepted["regime"] = "neutral"
    accepted["pending_regime"] = "unknown"
    accepted["pending_count"] = 0
    store.save_intraday_market_regime_state(accepted)
    delivery["market_regime"] = "neutral"
    assert asyncio.run(main._validate_intraday_delivery_before_send(delivery, start + timedelta(seconds=30))) is None
    assert main._intraday_market_state_is_fresh(store.intraday_market_regime_state(), start + timedelta(seconds=62)) is False


def test_recommendation_commands_are_read_only_and_origin_scoped(tmp_path):
    Main, _core, StockStore = _imports()
    store = StockStore(tmp_path / "recommendation_commands.sqlite3")
    with store._connect() as db:
        for ident, origin, visibility in (("a", "origin-a", "private"), ("b", "origin-b", "private")):
            db.execute("INSERT INTO recommendation_records(recommendation_id,run_id,recommended_date,code,name,plan_version,market_regime,data_timestamp,freshness,source,caller_identity,price_basis,prediction_json,created_at,origin,visibility,strategy_version,plan_status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (ident, ident, "2026-09-09", "600000", ident, "plan", "neutral", "2026-09-09", "verified_close", "fixture", "fixture", "unadjusted", "{}", "2026-09-09T16:00:00", origin, visibility, "strategy", "unknown"))
    main = _main(Main, store)
    event_a = types.SimpleNamespace(unified_msg_origin="origin-a", plain_result=lambda text: text)
    with store._connect() as db:
        before = db.execute("SELECT COUNT(*) FROM recommendation_outcomes").fetchone()[0]
    rendered = asyncio.run(_collect(main.recommendation_review(event_a)))
    performance_origin_a = asyncio.run(_collect(main.strategy_performance(event_a)))
    with store._connect() as db:
        after = db.execute("SELECT COUNT(*) FROM recommendation_outcomes").fetchone()[0]
    assert len(rendered) == len(performance_origin_a) == 1
    assert "origin-a" not in rendered[0] and "b" not in rendered[0] and "b" not in performance_origin_a[0]
    assert before == after == 0

    store.recommendation_performance = lambda *_args: [{
        "strategy_version": "price-plan-config-v1:fixture", "market_regime": "neutral",
        "sample_count": 1, "mature_count": 1, "price_evaluable_count": 1,
        "order_evaluable_count": 0, "pending_count": 0, "unknown_count": 0,
        "unknown_order_count": 1, "positive_return_rate": None, "target_hit_rate": None,
        "invalidation_count": 0, "invalidation_eligible_count": 0,
        "invalidation_rate": None, "median_return_pct": None,
        "median_max_gain_pct": None, "return_distribution": {}, "max_drawdown_pct": None,
    }]
    performance = asyncio.run(_collect(main.strategy_performance(event_a)))
    assert len(performance) == 1 and "数据不足" in performance[0] and "失效0/0" in performance[0]


def test_daily_close_producer_requires_explicit_corporate_action_evidence(tmp_path):
    Main, core, _StockStore = _imports()
    main = _main(Main, _StockStore(tmp_path / "producer.sqlite3"))
    main._raw_screen_provenance = {}
    main._tushare_mode = lambda: False
    quote = core.Quote("600099", "生产器夹具", 10, atr14=1, support20=10, resistance20=11, history_days=20, indicator_last_date="2026-09-09", indicator_last_close=10, indicator_price_basis="unadjusted", indicator_source="tushare")
    plan = main._daily_close_plan(quote, "2026-09-09")
    assert plan.validated is True and "corporate_action_evidence" not in plan.provenance
    quote.corporate_action_factor = 7.25
    quote.corporate_action_evidence = "tushare:adj_factor:2026-09-09:600099.SH"
    assert "corporate_action_evidence" not in main._daily_close_plan(quote, "2026-09-09").provenance
    quote.corporate_action_observed_at = "2026-09-09T06:00:00+00:00"
    quote.corporate_action_response_sha256 = "a" * 64
    sourced = main._daily_close_plan(quote, "2026-09-09")
    assert sourced.provenance["corporate_action_evidence"]["factor"] == 7.25
    quote.corporate_action_factor = None
    quote.corporate_action_evidence = ""
    main._raw_screen_provenance = {"corporate_action_evidence": {"comparable": True, "factor": 1, "source": "upstream-fixture"}}
    injected = main._daily_close_plan(quote, "2026-09-09")
    assert "corporate_action_evidence" not in injected.provenance


def test_recommendation_commands_overlap_full_market_diagnostic_without_mutation(tmp_path):
    Main, _core, StockStore = _imports()
    store = StockStore(tmp_path / "recommendation_diagnostic_overlap.sqlite3")
    with store._connect() as db:
        db.execute("INSERT INTO recommendation_records(recommendation_id,run_id,recommended_date,code,name,plan_version,market_regime,data_timestamp,freshness,source,caller_identity,price_basis,prediction_json,created_at,origin,visibility,strategy_version,plan_status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("a", "a", "2026-09-09", "600000", "a", "plan", "neutral", "2026-09-09", "verified_close", "fixture", "fixture", "unadjusted", "{}", "2026-09-09T16:00:00", "origin-a", "private", "strategy", "unknown"))
    main = _main(Main, store)
    full_diagnostics = {"diagnostics_invocation_id": "full-market-run", "input": 5550, "market_stats_confirmed": True}
    main._last_screen_diagnostics = dict(full_diagnostics)
    event = types.SimpleNamespace(unified_msg_origin="origin-a", plain_result=lambda text: text)

    async def scenario():
        review = asyncio.create_task(_collect(main.recommendation_review(event)))
        performance = asyncio.create_task(_collect(main.strategy_performance(event)))
        diagnostics = asyncio.create_task(asyncio.sleep(0, result=dict(main._last_screen_diagnostics)))
        return await review, await performance, await diagnostics

    review, performance, diagnostics = asyncio.run(scenario())
    assert review and performance and diagnostics == full_diagnostics
    assert main._last_screen_diagnostics == full_diagnostics
