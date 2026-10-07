"""Cross-item setting checks (C07): what the code silently replaces, clamps or can never act on."""
from __future__ import annotations

import ast
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import types

from test_v0138_automatic_close import _imports
from webapp.data import Dashboard

Main, _ScreenScoreResult, _core, StockStore = _imports()
main_module = importlib.import_module("astrbot_stock_watch.main")
checks = importlib.import_module("astrbot_stock_watch.config_checks")
providers = importlib.import_module("astrbot_stock_watch.providers")
ROOT = Path(__file__).resolve().parents[2]
DEFAULTS = dict(main_module._SCHEMA_DEFAULTS)
HOLIDAY = datetime(2026, 10, 7, 2, 30, tzinfo=timezone.utc)


def _issues(**changes):
    return checks.setting_issues({**DEFAULTS, **changes}, DEFAULTS)


def _codes(**changes):
    return {(issue["code"], tuple(issue["keys"])) for issue in _issues(**changes)}


def test_defaults_and_missing_settings_raise_nothing():
    assert checks.setting_issues(DEFAULTS, DEFAULTS) == []
    assert checks.setting_issues({}, DEFAULTS) == []
    assert checks.setting_issues(None, DEFAULTS) == []


def test_an_empty_price_range_is_an_error_and_equal_bounds_are_not():
    issue, = _issues(price_min=90, price_max=80)
    assert issue["code"] == "price_range_empty" and issue["level"] == "error"
    assert issue["message"] == "price_min 90 高于 price_max 80" and "正式候选会一直为空" in issue["effect"]
    assert _issues(price_min=10, price_max=10) == []


def test_a_percentage_typed_into_a_ratio_reports_the_clamped_value():
    issue, = _issues(screen_min_indicator_coverage=80)
    assert issue["code"] == "out_of_range" and "0.95 表示 95%" in issue["message"] and issue["effect"] == "实际按 1 使用"
    issue, = _issues(intraday_market_min_coverage=0.3)
    assert issue["effect"] == "实际按 0.5 使用" and "0.95 表示" not in issue["message"]


def test_risk_off_bounds_follow_the_effective_weak_thresholds():
    issue, = _issues(intraday_market_risk_off_breadth=0.45)
    assert issue["keys"] == ["intraday_market_risk_off_breadth"] and issue["effect"] == "实际按 0.4 使用"
    issue, = _issues(intraday_market_risk_off_median_pct=0)
    assert issue["effect"] == "实际按 -0.2 使用"


def test_strong_and_weak_median_thresholds_keep_their_order():
    assert _codes(intraday_market_strong_median_pct=-0.3) == {
        ("market_threshold_order", ("intraday_market_strong_median_pct", "intraday_market_weak_median_pct"))}


def test_minute_trigger_combinations_that_can_never_fire():
    assert _issues(minute_bar_history=10, minute_trigger_min_bars=30) == []
    assert _issues(minute_trigger_enabled="on") == []
    assert _codes(minute_trigger_enabled=True, minute_enabled=False) == {
        ("minute_trigger_without_bars", ("minute_trigger_enabled", "minute_enabled"))}
    issue, = _issues(minute_trigger_enabled=True, minute_bar_history=10, minute_trigger_min_bars=30)
    assert issue["code"] == "minute_history_short" and issue["level"] == "error"
    assert "至少要 30 根" in issue["message"] and "只保留 10 根" in issue["message"]


def test_clock_settings_that_fall_back_or_get_postponed():
    issue, = _issues(daily_scan_time="25:00")
    assert issue["code"] == "time_format" and "（25:00）" in issue["message"] and issue["effect"] == "收盘扫描实际按 15:10 执行"
    issue, = _issues(daily_acceptance_time="15:00")
    assert issue["code"] == "acceptance_before_scan" and issue["effect"] == "每日验收实际按 15:10 执行"
    assert _issues(daily_scan_time="", daily_acceptance_time=None) == []
    assert _issues(daily_scan_time="9:05", daily_acceptance_time="09:30") == []


def test_enum_values_match_the_readers_and_free_text_is_never_echoed():
    for key, allowed in checks.ENUM_SETTINGS.items():
        for value in allowed:
            assert _issues(**{key: f" {value.upper()} "}) == []
    assert _issues(tushare_bj_calendar_policy="fallback") == []
    issue, = _issues(factor_source="akshare")
    assert issue["code"] == "enum_value" and "（akshare）" in issue["message"] and issue["effect"] == "不会补充行业 / 基本面因子"
    issue, = _issues(realtime_backup_mode="https://private.example/x")
    assert "private.example" not in json.dumps(issue, ensure_ascii=False)


def test_enum_tables_agree_with_the_provider_readers():
    for value in checks.ENUM_SETTINGS["tushare_bj_calendar_policy"] + checks.ENUM_ALIASES["tushare_bj_calendar_policy"]:
        assert providers.TushareBulkDailyProvider("fixture", bj_calendar_policy=value).bj_calendar_policy in (
            checks.ENUM_SETTINGS["tushare_bj_calendar_policy"])
    assert providers.TushareBulkDailyProvider("fixture", bj_calendar_policy="sse").bj_calendar_policy == "require_bse"
    for value in checks.ENUM_SETTINGS["realtime_backup_mode"]:
        assert providers.SinaQuoteProvider(realtime_backup_mode=value).realtime_backup_mode == value
    assert providers.SinaQuoteProvider(realtime_backup_mode="always").realtime_backup_mode == "disabled"
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    # "off" fetches nothing because no factor branch accepts it; the dropdown keeps that choice reachable.
    assert 'factor_source in {"auto", "eastmoney", "custom", "tushare"}' in source and 'factor_mode == "score"' in source


def test_schema_dropdowns_list_exactly_the_accepted_values():
    schema = json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))
    assert {key: tuple(item["options"]) for key, item in schema.items() if "options" in item} == dict(checks.ENUM_SETTINGS)
    assert all(schema[key]["default"] in allowed for key, allowed in checks.ENUM_SETTINGS.items())
    assert not any("enum" in item for item in schema.values())


def test_universe_statuses_report_ignored_letters_and_the_forced_listed_status():
    assert _codes(tushare_universe_statuses="D;P X") == {("universe_status_unknown", ("tushare_universe_statuses",)),
                                                         ("universe_status_listed_missing", ("tushare_universe_statuses",))}
    assert _issues(tushare_universe_statuses="l d") == []


def test_non_numbers_and_split_snapshot_minimums():
    issue, = _issues(price_max="eighty")
    assert issue["code"] == "not_a_number" and issue["effect"] == "实际按默认值 80 使用"
    assert _issues(price_max="80") == []
    issue, = _issues(daily_snapshot_min_size=500)
    assert issue["code"] == "snapshot_minimum_split" and "按 500 判定" in issue["effect"]
    assert _issues(daily_snapshot_min_size=1000) == []


def test_bounds_table_matches_every_clamp_in_main():
    calls = [node for node in ast.walk(ast.parse((ROOT / "main.py").read_text(encoding="utf-8")))
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("_int", "_float")
             and node.args and isinstance(node.args[0], ast.Constant)]
    seen = {}
    for node in calls:
        bounds = tuple(ast.literal_eval(arg) if isinstance(arg, (ast.Constant, ast.UnaryOp)) else None for arg in node.args[2:4])
        seen.setdefault(node.args[0].value, set()).add((*bounds, node.func.attr == "_int"))
    for key, (minimum, maximum) in checks.BOUNDS.items():
        assert seen[key] == {(minimum, maximum, key in checks.INTEGER_SETTINGS)}, key
        assert (maximum is None) == (key in checks.DEPENDENT_MAXIMUM)
    assert seen["daily_snapshot_min_size"] == {(1, 10000, True), (1000, 10000, True)}


def test_load_time_issues_are_logged_and_written_to_the_public_snapshot(monkeypatch):
    warnings = []
    monkeypatch.setattr(main_module.logger, "warning", lambda *args, **kwargs: warnings.append(args))
    stub = types.SimpleNamespace(config={**DEFAULTS, "price_min": 90}, deprecated_settings=[])
    stub.setting_issues = Main._warn_setting_issues(stub)
    assert [issue["code"] for issue in stub.setting_issues] == ["price_range_empty"]
    assert any("price_min 90 高于 price_max 80" in args for args in warnings)
    assert Main.public_settings_snapshot(stub)["setting_issues"] == stub.setting_issues
    monkeypatch.setattr(checks, "setting_issues", lambda *_: 1 / 0)
    assert Main._warn_setting_issues(stub) == []


def test_settings_page_passes_issues_through_as_trimmed_text(tmp_path):
    database = tmp_path / "plugin.sqlite3"
    StockStore(database)
    artifact = tmp_path / "plugin_data" / "intraday_quotes.json"
    artifact.parent.mkdir()
    issue = {"code": "price_range_empty", "level": "error", "keys": ["price_min", "price_max"],
             "message": "price_min 90 高于 price_max 80\x07", "effect": "正式候选会一直为空"}
    (artifact.parent / "public_settings.json").write_text(json.dumps({
        "schema_version": 1, "values": {}, "configured": {},
        "setting_issues": [issue, {"level": "fatal", "message": "dropped"}, "junk"]}), encoding="utf-8")
    snapshot = Dashboard(database, artifact_path=artifact, now=lambda: HOLIDAY).query("settings")["data"]["snapshot"]
    assert snapshot["setting_issues"] == [{**issue, "message": "price_min 90 高于 price_max 80"}]
    missing = Dashboard(database, artifact_path=tmp_path / "none" / "intraday_quotes.json", now=lambda: HOLIDAY)
    assert missing.query("settings")["data"]["snapshot"]["setting_issues"] == []
