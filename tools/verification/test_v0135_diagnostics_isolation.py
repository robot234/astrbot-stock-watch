from __future__ import annotations

import asyncio
import importlib
import sys
import tempfile
import types
from pathlib import Path

import pytest


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
    return main_module.Main, main_module.ScreenScoreResult, core_module


def _candidate(core, code: str = "600000"):
    quote = core.Quote(
        code,
        "隔离样本",
        10.0,
        9.9,
        1_000_000.0,
        1.0,
        100_000.0,
        history_days=60,
        suspended=False,
        limit_up=False,
        limit_down=False,
    )
    return core.Candidate(
        quote,
        12,
        ["趋势确认+2"],
        base_score=12,
        risk_level="eligible",
        factor_overlay=core.FactorOverlay(
            fundamental_coverage=1.0,
            risk_factor_coverage=1.0,
        ),
    )


def _full_diagnostics(requested_date: str) -> dict[str, object]:
    return {
        "input": 1000,
        "tradable": 900,
        "indicator_targets": 300,
        "indicator_raw_batch": 300,
        "indicator_network": 0,
        "indicator_memory_cache": 0,
        "indicator_persistent_cache": 0,
        "indicator_failed": 0,
        "indicator_coverage": 1.0,
        "market_regime": "risk_on",
        "market_breadth": 0.6,
        "market_advancing": 600,
        "market_declining": 300,
        "market_flat": 100,
        "market_sample_size": 1000,
        "market_median_return": 0.8,
        "raw_batch_id": "full-batch",
        "raw_generation": 6,
        "diagnostics_invocation_id": "full-run",
        "diagnostics_requested_date": requested_date,
        "diagnostics_actual_date": requested_date,
        "diagnostics_raw_batch_id": "full-batch",
        "diagnostics_raw_generation": 6,
    }


def test_intraday_shared_overwrite_cannot_change_full_market_record_or_report():
    Main, ScreenScoreResult, core = _imports()
    main = Main.__new__(Main)
    main.config = {
        "market_min_snapshot_size": 1000,
        "report_candidate_limit": 10,
    }
    main.raw_dataset_key = "tushare_daily"
    main.raw_max_stale_trading_days = 2
    main._last_screen_diagnostics = {}
    main._raw_screen_provenance = {}
    main._last_screen_report_claimed = True
    main.quotes = types.SimpleNamespace(tushare_token="configured")
    main.store = types.SimpleNamespace(
        active_raw_batch=lambda *_args, **_kwargs: {
            "batch_id": "full-batch",
            "generation": 6,
        }
    )
    full_quotes = [core.Quote(f"{index:06d}", "全市场", 10.0, pct_change=1.0) for index in range(1000)]
    full_candidate = _candidate(core)
    small_candidate = _candidate(core, "000001")
    recorded = {}
    replies = []
    persisted = asyncio.Event()
    resume = asyncio.Event()

    async def daily_snapshot(requested_date):
        main._last_screen_diagnostics = {"snapshot_owner": "full-run"}
        return full_quotes, False, requested_date

    async def score_result(quotes, _limit, as_of="", **kwargs):
        if kwargs.get("invocation_id") == "full-run":
            return ScreenScoreResult.build([full_candidate], _full_diagnostics(as_of))
        return ScreenScoreResult.build(
            [small_candidate],
            {
                "input": len(quotes),
                "tradable": len(quotes),
                "indicator_targets": 1,
                "indicator_raw_batch": 0,
                "indicator_network": 1,
                "indicator_memory_cache": 0,
                "indicator_persistent_cache": 0,
                "indicator_failed": 0,
                "diagnostics_invocation_id": kwargs.get("invocation_id"),
                "diagnostics_requested_date": kwargs.get("requested_date"),
                "diagnostics_actual_date": as_of,
            },
        )

    async def snapshot_context(_requested, _actual, _quotes):
        return {"source": "tushare", "quality": "good", "complete": True}

    async def record_screen(*_args, diagnostics=None, **_kwargs):
        recorded.update(dict(diagnostics or {}))
        persisted.set()
        await resume.wait()
        return "screen-full"

    async def reply_origin(_origin, text):
        replies.append(text)

    main._daily_snapshot = daily_snapshot
    main._score_quotes_result = score_result
    main._snapshot_context = snapshot_context
    main._record_screen = record_screen
    main._reply_origin = reply_origin
    main._preview_fallback_needed = lambda *_args, **_kwargs: asyncio.sleep(0, result=False)

    async def verify():
        full_task = asyncio.create_task(main._run_market_sync("local-chat", "full-run", 10))
        await asyncio.wait_for(persisted.wait(), timeout=1)
        small = await main._score_quotes(
            [small_candidate.quote],
            1,
            "2026-09-09",
            include_factors=False,
            requested_date="2026-09-09",
            invocation_id="intraday-run",
        )
        assert small == [small_candidate]
        assert main._last_screen_diagnostics["diagnostics_invocation_id"] == "intraday-run"
        assert main._last_screen_diagnostics["input"] == 1
        resume.set()
        await asyncio.wait_for(full_task, timeout=1)

    asyncio.run(verify())

    assert recorded["diagnostics_invocation_id"] == "full-run"
    assert recorded["diagnostics_requested_date"] == "2026-09-09"
    assert recorded["diagnostics_actual_date"] == "2026-09-09"
    assert recorded["diagnostics_raw_batch_id"] == "full-batch"
    assert recorded["diagnostics_raw_generation"] == 6
    assert recorded["market_stats_confirmed"] is True
    assert len(replies) == 1
    assert "行情：tushare · 完整｜共 1000 只" in replies[0]
    assert "涨/跌/平 600/300/100" in replies[0]
    assert "上涨占比 60.0%" in replies[0]
    assert "中位涨跌 +0.80%" in replies[0]
    assert "指标：raw批次 300" in replies[0]
    assert "intraday-run" not in replies[0]


def test_invalid_market_statistics_only_degrade_the_market_line():
    Main, _, core = _imports()
    main = Main.__new__(Main)
    main.config = {"market_min_snapshot_size": 1000, "report_candidate_limit": 10}
    diagnostics = _full_diagnostics("2026-09-09")
    diagnostics["market_flat"] = 99
    checked = main._validate_report_diagnostics(
        diagnostics,
        requested_date="2026-09-09",
        actual_date="2026-09-09",
        quote_count=1000,
        active_raw={"batch_id": "full-batch", "generation": 6},
    )
    candidate = _candidate(core)
    lines = main._market_report_lines(
        "2026-09-09",
        "2026-09-09",
        [candidate.quote] * 1000,
        [candidate],
        {"source": "tushare", "quality": "good", "complete": True},
        checked,
    )
    text = "\n".join(lines)
    assert checked["market_stats_confirmed"] is False
    assert "市场：统计未确认｜候选 1 只" in text
    assert "隔离样本（600000）" in text
    assert "行情：tushare · 完整｜共 1000 只" in text


def test_compact_candidate_reports_coverage_and_specific_missing_reasons():
    _, _, core = _imports()
    candidate = _candidate(core)
    candidate.quote.history_days = 12
    candidate.quote.limit_down = None
    candidate.factor_overlay = core.FactorOverlay(
        fundamental_coverage=0.6,
        fundamental_missing=("cash_quality", "valuation"),
        risk_factor_coverage=0.5,
        risk_factor_missing=("audit_flag",),
    )
    text = core.format_compact_candidate(candidate, 1)
    assert "技术12日(历史<20日)" in text
    assert "基本面60%(缺现金质量/估值)" in text
    assert "风险60%(缺跌停/审计)" in text
    assert len(text) < 500


def test_empty_raw_rows_keep_the_declared_two_value_return_contract():
    Main, _, _ = _imports()
    assert Main._quotes_from_raw_bars({}, "2026-09-09") == ([], None)


def test_screen_score_result_diagnostics_are_immutable():
    _, ScreenScoreResult, _ = _imports()
    result = ScreenScoreResult.build([], {"diagnostics_invocation_id": "one"})
    with pytest.raises(TypeError):
        result.diagnostics["diagnostics_invocation_id"] = "two"
