"""Cross-item checks of the plugin settings for the load-time log and the Web settings page.

Each check states what the running code does with a value or a combination.  None of them
changes a value, so a mistyped setting is reported but can never loosen a gate.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import time
import math
import re
from types import MappingProxyType

# Values accepted where each setting is read; the panel offers them as a dropdown.
ENUM_SETTINGS = MappingProxyType({
    "factor_mode": ("report_only", "score"),
    "factor_source": ("auto", "eastmoney", "tushare", "custom", "off"),
    "tushare_bj_calendar_policy": ("require_bse", "sse_fallback", "exclude"),
    "realtime_backup_mode": ("disabled", "shadow", "fallback"),
})
# Older spellings the Tushare provider still maps onto require_bse / sse_fallback.
ENUM_ALIASES = MappingProxyType({"tushare_bj_calendar_policy": ("strict", "required", "fallback")})
_ENUM_EFFECTS = {
    "factor_mode": "按 report_only 处理：因子只展示，不参与排序",
    "factor_source": "不会补充行业 / 基本面因子",
    "tushare_bj_calendar_policy": "按 require_bse 处理",
    "realtime_backup_mode": "按 disabled 处理：不请求 rt_k 备源",
}

# (minimum, maximum) clamped where main.py reads each value.  A maximum of None is the effective
# value of DEPENDENT_MAXIMUM[key].  tools/verification keeps this table equal to every main.py call.
BOUNDS = MappingProxyType({
    "price_min": (0.01, 100000),
    "price_max": (0.01, 100000),
    "tushare_raw_min_overall_coverage": (0.0, 1.0),
    "tushare_raw_min_market_coverage": (0.0, 1.0),
    "tushare_raw_min_market_median_ratio": (0.0, 1.0),
    "screen_min_indicator_coverage": (0.0, 1.0),
    "intraday_market_min_coverage": (0.50, 1.0),
    "intraday_market_strong_breadth": (0.50, 0.95),
    "intraday_market_weak_breadth": (0.05, 0.50),
    "intraday_market_risk_off_breadth": (0.01, None),
    "intraday_market_strong_median_pct": (-10, 10),
    "intraday_market_weak_median_pct": (-10, 10),
    "intraday_market_risk_off_median_pct": (-10, None),
    "minute_bar_history": (10, 2000),
    "minute_trigger_lookback": (1, 60),
    "minute_trigger_min_bars": (1, 120),
    "minute_trigger_consecutive_up": (1, 20),
})
DEPENDENT_MAXIMUM = MappingProxyType({
    "intraday_market_risk_off_breadth": "intraday_market_weak_breadth",
    "intraday_market_risk_off_median_pct": "intraday_market_weak_median_pct",
})
INTEGER_SETTINGS = frozenset({"minute_bar_history", "minute_trigger_lookback", "minute_trigger_min_bars",
                              "minute_trigger_consecutive_up", "daily_snapshot_min_size"})
_RATIO_SETTINGS = frozenset(key for key in BOUNDS if key.endswith(("_coverage", "_ratio", "_breadth")))
_SHOWN_TEXT = re.compile(r"[A-Za-z0-9_.:,\-]{1,40}")


def _shown(value) -> str:
    """Echo a string only when it could not carry a credential, endpoint or path."""
    text = str(value if value is not None else "").strip()
    return f"（{text}）" if _SHOWN_TEXT.fullmatch(text) else ""


def _read(config: Mapping, defaults: Mapping, key: str):
    """The number main.py's _int/_float would parse before clamping, and whether it parsed."""
    default = defaults.get(key)
    try:
        raw = config.get(key, default)
        value = int(raw) if key in INTEGER_SETTINGS else float(raw)
    except (TypeError, ValueError, OverflowError):
        return default, False
    if isinstance(value, float) and not math.isfinite(value):
        return default, False
    return value, True


def _effective(config: Mapping, defaults: Mapping, key: str):
    value, parsed = _read(config, defaults, key)
    if not parsed:
        return value
    minimum, maximum = BOUNDS[key]
    if maximum is None:
        maximum = _effective(config, defaults, DEPENDENT_MAXIMUM[key])
    return max(minimum, min(value, maximum))


def _flag(config: Mapping, defaults: Mapping, key: str) -> bool:
    value = config.get(key, defaults.get(key))
    return value.lower() in {"1", "true", "yes", "on"} if isinstance(value, str) else bool(value)


def _clock(raw, fallback: str) -> tuple[time, bool]:
    """Mirror Main._automatic_close_time: HH:MM, anything else falls back silently."""
    text = str(raw or fallback).strip()
    try:
        hour, minute = (int(value) for value in text.split(":", 1))
        return time(hour, minute), True
    except (TypeError, ValueError, OverflowError):
        return time.fromisoformat(fallback), False


def setting_issues(config: Mapping, defaults: Mapping) -> list[dict]:
    """Settings or combinations the code silently replaces, clamps or cannot act on."""
    config = config if isinstance(config, Mapping) else {}
    issues: list[dict] = []

    def add(code: str, level: str, keys, message: str, effect: str) -> None:
        issues.append({"code": code, "level": level, "keys": list(keys), "message": message, "effect": effect})

    def number(key: str):
        return _effective(config, defaults, key)

    for key, allowed in ENUM_SETTINGS.items():
        raw = config.get(key, defaults.get(key))
        if str(raw if raw is not None else "").strip().lower() not in (*allowed, *ENUM_ALIASES.get(key, ())):
            add("enum_value", "warning", [key], f"{key}{_shown(raw)} 不是可选值（{' / '.join(allowed)}）", _ENUM_EFFECTS[key])

    statuses = str(config.get("tushare_universe_statuses", defaults.get("tushare_universe_statuses")) or "L,D,P").strip().upper()
    parts = [part.strip() for part in statuses.replace(";", ",").replace(" ", ",").split(",") if part.strip()]
    unknown = [part for part in parts if part not in ("L", "D", "P")]
    if unknown:
        add("universe_status_unknown", "warning", ["tushare_universe_statuses"],
            f"tushare_universe_statuses 里有不认识的状态{_shown(','.join(unknown))}", "只拉取 L / D / P 中列出的状态，其余忽略")
    if "L" not in parts:
        add("universe_status_listed_missing", "warning", ["tushare_universe_statuses"],
            "tushare_universe_statuses 没有包含 L（上市）", "L 总会自动加入")

    scan_raw = config.get("daily_scan_time", defaults.get("daily_scan_time"))
    scan, scan_ok = _clock(scan_raw, "15:10")
    if not scan_ok:
        add("time_format", "warning", ["daily_scan_time"], f"daily_scan_time{_shown(scan_raw)} 不是有效的 HH:MM", "收盘扫描实际按 15:10 执行")
    acceptance_raw = config.get("daily_acceptance_time", defaults.get("daily_acceptance_time"))
    acceptance, acceptance_ok = _clock(acceptance_raw, "15:40")
    if not acceptance_ok:
        add("time_format", "warning", ["daily_acceptance_time"], f"daily_acceptance_time{_shown(acceptance_raw)} 不是有效的 HH:MM",
            "每日验收实际按 15:40 执行")
    if acceptance < scan:
        add("acceptance_before_scan", "warning", ["daily_acceptance_time", "daily_scan_time"],
            f"每日验收 {acceptance:%H:%M} 早于收盘扫描 {scan:%H:%M}", f"每日验收实际按 {scan:%H:%M} 执行")

    for key, (minimum, maximum) in BOUNDS.items():
        value, parsed = _read(config, defaults, key)
        if not parsed:
            add("not_a_number", "warning", [key], f"{key} 不是有效数字", f"实际按默认值 {defaults.get(key)} 使用")
            continue
        if maximum is None:
            maximum = number(DEPENDENT_MAXIMUM[key])
        if not minimum <= value <= maximum:
            hint = "（这是 0—1 的比例，0.95 表示 95%）" if key in _RATIO_SETTINGS and value > 1 else ""
            add("out_of_range", "warning", [key], f"{key} = {value:g} 超出允许范围 {minimum:g}—{maximum:g}{hint}",
                f"实际按 {max(minimum, min(value, maximum)):g} 使用")

    price_min, price_max = number("price_min"), number("price_max")
    if price_min > price_max:
        add("price_range_empty", "error", ["price_min", "price_max"], f"price_min {price_min:g} 高于 price_max {price_max:g}",
            "没有股票能通过正式筛选的价格条件，正式候选会一直为空")

    strong, weak = number("intraday_market_strong_median_pct"), number("intraday_market_weak_median_pct")
    if strong <= weak:
        add("market_threshold_order", "warning", ["intraday_market_strong_median_pct", "intraday_market_weak_median_pct"],
            f"强市涨跌幅中位数门槛 {strong:g}% 不高于弱市门槛 {weak:g}%", "强弱市的中位数条件没有拉开，判定基本只靠上涨占比")

    if _flag(config, defaults, "minute_trigger_enabled"):
        if not _flag(config, defaults, "minute_enabled"):
            add("minute_trigger_without_bars", "error", ["minute_trigger_enabled", "minute_enabled"],
                "分钟触发已开启，但分钟线记录（minute_enabled）关闭", "没有分钟线，分钟触发永远不会触发")
        needed = max(number("minute_trigger_min_bars"), number("minute_trigger_lookback") + 1,
                     number("minute_trigger_consecutive_up") + 1)
        history = number("minute_bar_history")
        if history < needed:
            add("minute_history_short", "error",
                ["minute_bar_history", "minute_trigger_min_bars", "minute_trigger_lookback", "minute_trigger_consecutive_up"],
                f"分钟触发至少要 {needed} 根已完成分钟线，但每只股票只保留 {history} 根", "分钟触发永远不会触发")

    size, parsed = _read(config, defaults, "daily_snapshot_min_size")
    if parsed and max(1, min(size, 10000)) < 1000:
        used = max(1, min(size, 10000))
        add("snapshot_minimum_split", "warning", ["daily_snapshot_min_size"], f"daily_snapshot_min_size = {used} 低于 1000",
            f"raw 批次和东财临时筛选按 {used} 判定，日快照完整性按 1000 判定，口径不一致")
    return issues
