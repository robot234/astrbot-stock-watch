"""Configuration defaults, deprecated keys and research parameters from the 2026-10-07 audit."""
from __future__ import annotations

import asyncio
import importlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from test_v0139_intraday_m2 import _imports, _main
from test_v0155_research_pools import _history, _snapshot

Main, _core, StockStore = _imports()
main_module = importlib.import_module("astrbot_stock_watch.main")
SCHEMA = json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))
SOURCE = (ROOT / "main.py").read_text(encoding="utf-8")
ADVANCED = {
    "automatic_close_max_attempts": 6, "automatic_close_retry_window_seconds": 14400, "automatic_close_retry_seconds": 300,
    "automatic_delivery_lease_seconds": 120, "automatic_delivery_max_attempts": 5,
    "automatic_delivery_retry_window_seconds": 3600, "automatic_delivery_retry_seconds": 60,
    "intraday_risk_delivery_max_age_seconds": 900,
}


def _literal(value):
    return value == "True" if value in ("True", "False") else float(value.replace("_", ""))


def test_every_literal_fallback_matches_the_panel_default():
    calls = re.findall(r'self\._(int|float|bool)\(\s*"([a-z0-9_]+)",\s*(-?\d[\d_]*(?:\.\d+)?|True|False)\b', SOURCE)
    assert len(calls) > 100
    mismatches = []
    for _kind, key, default in calls:
        assert key in SCHEMA, key
        expected, actual = SCHEMA[key]["default"], _literal(default)
        if (isinstance(expected, bool) or isinstance(actual, bool)) and expected is not actual:
            mismatches.append(key)
        elif not isinstance(expected, bool) and not isinstance(actual, bool) and float(expected) != float(actual):
            mismatches.append(key)
    assert mismatches == []
    read = set(re.findall(r'self\.config\.get\(\s*"([a-z0-9_]+)"', SOURCE))
    assert read - set(SCHEMA) == {"tushare_raw_universe_evidence"}


def test_advanced_keys_are_listed_with_code_defaults():
    for key, default in ADVANCED.items():
        assert SCHEMA[key]["default"] == default
        assert SCHEMA[key]["description"].startswith("【高级】")
    assert SCHEMA["tushare_raw_lookback_days"]["default"] == SCHEMA["tushare_raw_session_count"]["default"] == 120


def test_schema_defaults_drive_interval_and_concurrency_fallbacks(tmp_path):
    main = _main(Main, StockStore(tmp_path / "config.sqlite3"))
    main.config = {}
    assert main._quote_interval_seconds() == SCHEMA["quote_interval_seconds"]["default"] == 5
    assert main._max_concurrency() == SCHEMA["max_concurrency"]["default"] == 5
    main.config = {"quote_interval_seconds": 1, "max_concurrency": 100}
    assert main._quote_interval_seconds() == 5
    assert main._max_concurrency() == 64 and main._max_concurrency(20) == 20
    assert SOURCE.count('self._int("quote_interval_seconds"') == 1
    assert SOURCE.count('self._int("max_concurrency"') == 1


def test_deprecated_confirmation_keys_are_reported_and_never_read(tmp_path):
    main = _main(Main, StockStore(tmp_path / "deprecated.sqlite3"))
    main.config = {"confirmation_enabled": False, "confirmation_periods": 2, "confirmation_max_gap_seconds": 90}
    assert main._warn_deprecated_settings() == []
    main.config = {"confirmation_enabled": True, "confirmation_periods": 5, "intraday_confirmation_periods": 2}
    assert main._warn_deprecated_settings() == ["confirmation_enabled", "confirmation_periods"]
    for key in main_module._DEPRECATED_SETTINGS:
        assert SCHEMA[key]["description"].startswith("【已废弃·不生效】")
        assert SOURCE.count(f'"{key}"') == 1


def test_research_freeze_records_independent_parameters(tmp_path):
    store = StockStore(tmp_path / "research-params.sqlite3")
    snapshot = _snapshot()
    for index in range(8):
        code = f"600{100 + index}"
        rows = _history(code, "Extra", 60000000 - index * 100000)
        snapshot["day_rows"].append(rows[-1])
        snapshot["histories"][code] = rows
    store.active_raw_batch = lambda **_kwargs: {"fresh": True, "quality": "good",
                                                "actual_trade_date": snapshot["trade_date"], "batch_id": snapshot["batch_id"]}
    requested = {}
    store.research_input = lambda **kwargs: requested.update(kwargs) or snapshot
    main = _main(Main, store)
    frozen = asyncio.run(main._freeze_research_pools("2026-09-12"))
    assert requested["deep_limit"] == 300
    diagnostics = json.loads(frozen["diagnostics"])
    assert diagnostics["selection_policy"] == "technical-research-v1"
    assert diagnostics["parameters"] == {"deep_limit": 300, "primary_limit": 7, "radar_limit": 20, "price_min": 2.0,
                                         "price_max": 80.0, "independent_of": ["price_min", "price_max", "deep_screen_limit"]}
    assert len(frozen["picks"]["primary"]) == 7
    text = Main._research_pool_text(frozen, "primary")
    assert "研究参数（独立固定，不读取 price_min/price_max/deep_screen_limit）" in text
    assert "成交额前300只深筛，重点7、警戒20，价格2-80元" in text
    legacy = {**frozen, "diagnostics": json.dumps({"selection_policy": "technical-research-v1"})}
    assert "冻结记录未包含参数" in Main._research_pool_text(legacy, "radar")
