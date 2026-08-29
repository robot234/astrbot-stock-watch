from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Iterable
from zoneinfo import ZoneInfo
import json
import math
import re
import unicodedata

CHINA_TZ = ZoneInfo("Asia/Shanghai")

_RISK_LABELS = {
    "eligible": "可跟踪",
    "watch_only": "需复核",
    "blocked": "已拦截",
    "unknown": "数据不足",
}


def risk_label(risk_level: str) -> str:
    """Return a user-facing label for an internal candidate risk enum."""
    return _RISK_LABELS.get(str(risk_level), "风险待确认")


@dataclass(slots=True)
class Quote:
    code: str
    name: str
    price: float
    prev_close: float = 0.0
    amount: float = 0.0
    pct_change: float = 0.0
    volume: float = 0.0
    rsi6: float | None = None
    ma5: float | None = None
    ma10: float | None = None
    ma20: float | None = None
    volume_ratio: float | None = None
    atr14: float | None = None
    support20: float | None = None
    resistance20: float | None = None
    volatility20: float | None = None
    momentum5: float | None = None
    momentum20: float | None = None
    industry_score: float | None = None
    fundamental_score: float | None = None
    history_days: int = 0
    source: str = ""
    provider_ts: datetime | None = None
    # None means the provider did not establish the state. It is not the
    # same as a confirmed False and remains visible to risk review.
    suspended: bool | None = None
    limit_up: bool | None = None
    limit_down: bool | None = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(CHINA_TZ))
    # Indicator provenance is deliberately appended after the legacy fields so
    # positional construction from pre-v0.12 callers remains compatible.
    indicator_last_date: str = ""
    indicator_last_close: float | None = None
    indicator_price_basis: str = "unknown"
    indicator_source: str = ""

    @property
    def last_indicator_date(self) -> str:
        """Compatibility spelling used by older integrations."""
        return self.indicator_last_date

    @last_indicator_date.setter
    def last_indicator_date(self, value: str) -> None:
        self.indicator_last_date = str(value or "")

    @property
    def last_indicator_close(self) -> float | None:
        return self.indicator_last_close

    @last_indicator_close.setter
    def last_indicator_close(self, value: float | None) -> None:
        self.indicator_last_close = value

    @property
    def price_basis(self) -> str:
        return self.indicator_price_basis

    @price_basis.setter
    def price_basis(self, value: str) -> None:
        self.indicator_price_basis = str(value or "unknown")


@dataclass(slots=True)
class Candidate:
    quote: Quote
    score: int
    reasons: list[str]
    score_max: int = 50
    base_score: int = 0
    composite_score: int | None = None
    risk_level: str = "unknown"
    risk_flags: list[str] = field(default_factory=list)
    price_plan: "PricePlan | None" = None
    factor_overlay: "FactorOverlay | None" = None


@dataclass(slots=True)
class FactorOverlay:
    industry_name: str = ""
    industry_score: float | None = None
    fundamental_score: float | None = None
    market_regime: str = "unknown"
    market_adjustment: int = 0
    source: str = ""
    as_of: str = ""
    quality: str = "unknown"
    # Current provider classification is a display-only annotation.  Keep it
    # after the legacy fields so existing positional construction remains
    # compatible with v0.11.0 payloads.
    current_industry_name: str = ""

    @property
    def adjustment(self) -> int:
        values = (self.industry_score, self.fundamental_score)
        return int(round(sum(value for value in values if value is not None))) + self.market_adjustment



@dataclass(slots=True)
class PricePlan:
    """Reference levels for manual research; never an order or execution instruction."""

    state: str
    reference_price: float
    atr: float | None
    support: float | None
    resistance: float | None
    attention_low: float | None
    attention_high: float | None
    confirmation: float | None
    sell_low: float | None
    sell_high: float | None
    invalidation: float | None
    quality: str
    evidence: list[str] = field(default_factory=list)
    # Appended defaults keep all pre-v0.12 positional constructors valid.
    provenance: dict[str, object] = field(default_factory=dict)
    validated: bool = False


@dataclass(slots=True)
class RiskReview:
    verdict: str
    severity: str
    flags: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MarketContext:
    regime: str
    breadth: float
    advancing: int
    declining: int
    total_amount: float
    evidence: list[str] = field(default_factory=list)


def assess_market_context(quotes: Iterable[Quote]) -> MarketContext:
    """Derive a transparent market regime from the same daily snapshot."""
    rows = []
    for q in quotes:
        try:
            if math.isfinite(float(q.pct_change)) and math.isfinite(float(q.price)) and q.price > 0:
                rows.append(q)
        except (TypeError, ValueError):
            continue
    advancing = sum(1 for q in rows if q.pct_change > 0.2)
    declining = sum(1 for q in rows if q.pct_change < -0.2)
    breadth = advancing / len(rows) if rows else 0.0
    ratio = (advancing - declining) / len(rows) if rows else 0.0
    if not rows:
        regime = "unknown"
    elif ratio >= 0.15:
        regime = "risk_on"
    elif ratio <= -0.15:
        regime = "risk_off"
    else:
        regime = "neutral"
    evidence = [f"上涨{advancing}只、下跌{declining}只、样本{len(rows)}只", f"上涨占比{breadth:.1%}"]
    return MarketContext(regime, breadth, advancing, declining, sum(max(0.0, q.amount) for q in rows), evidence)


@dataclass(slots=True)
class NewsItem:
    title: str
    link: str = ""
    summary: str = ""
    published_at: str = ""
    source: str = ""


@dataclass(slots=True)
class MinuteBar:
    code: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float


class MinuteBarAggregator:
    def __init__(self, max_bars_per_code: int = 120):
        self.max_bars_per_code = max(10, max_bars_per_code)
        self.current: dict[str, MinuteBar] = {}
        self.history: dict[str, list[MinuteBar]] = {}
        self._baseline: dict[str, tuple[object, float, float]] = {}

    def update(self, quote: Quote) -> MinuteBar | None:
        if not isinstance(quote.fetched_at, datetime):
            return None
        timestamp = quote.fetched_at.astimezone(CHINA_TZ) if quote.fetched_at.tzinfo else quote.fetched_at.replace(tzinfo=CHINA_TZ)
        start = timestamp.replace(second=0, microsecond=0)
        previous = self.current.get(quote.code)
        if previous and start < previous.start:
            return None
        day = start.date()
        try:
            volume = max(0.0, float(quote.volume))
            amount = max(0.0, float(quote.amount))
        except (TypeError, ValueError):
            return None
        baseline = self._baseline.get(quote.code)
        day_changed = baseline is not None and baseline[0] != day
        if day_changed:
            previous = None
            self.current.pop(quote.code, None)
        if baseline is None or day_changed:
            delta_volume = delta_amount = 0.0
        else:
            delta_volume = volume - baseline[1] if volume >= baseline[1] else volume
            delta_amount = amount - baseline[2] if amount >= baseline[2] else amount
        self._baseline[quote.code] = (day, volume, amount)
        if previous and previous.start == start:
            previous.high = max(previous.high, quote.price)
            previous.low = min(previous.low, quote.price)
            previous.close = quote.price
            previous.volume += delta_volume
            previous.amount += delta_amount
            return None
        completed = previous
        self.current[quote.code] = MinuteBar(quote.code, start, quote.price, quote.price, quote.price, quote.price, delta_volume, delta_amount)
        if completed:
            bars = self.history.setdefault(quote.code, [])
            bars.append(completed)
            del bars[:-self.max_bars_per_code]
        return completed

    def bars(self, code: str) -> list[MinuteBar]:
        return list(self.history.get(code, []))

    def restore(self, bars: Iterable[MinuteBar | dict], trade_date: str | None = None) -> int:
        """Restore completed bars only; leave the current bar and baseline empty."""
        expected = str(trade_date or "")[:10]
        restored = 0
        for item in bars or []:
            try:
                if isinstance(item, MinuteBar):
                    code, start = str(item.code), item.start
                    values = (item.open, item.high, item.low, item.close, item.volume, item.amount)
                else:
                    code = str(item.get("code") or "")
                    raw_start = item.get("start") or item.get("start_at")
                    start = raw_start if isinstance(raw_start, datetime) else datetime.fromisoformat(str(raw_start).replace("Z", "+00:00"))
                    values = tuple(item.get(key) for key in ("open", "high", "low", "close", "volume", "amount"))
                if not code or not isinstance(start, datetime):
                    continue
                start = start if start.tzinfo else start.replace(tzinfo=CHINA_TZ)
                if expected and start.astimezone(CHINA_TZ).date().isoformat() != expected:
                    continue
                numbers = tuple(float(value or 0) for value in values)
                if not all(math.isfinite(value) for value in numbers):
                    continue
                target = self.history.setdefault(code, [])
                if any(existing.start == start for existing in target):
                    continue
                target.append(MinuteBar(code, start, *numbers))
                target.sort(key=lambda bar: bar.start)
                if len(target) > self.max_bars_per_code:
                    del target[:-self.max_bars_per_code]
                restored += 1
            except (AttributeError, TypeError, ValueError, OverflowError):
                continue
        return restored

    def reset(self) -> None:
        self.current.clear()
        self.history.clear()
        self._baseline.clear()

    def symbol_count(self) -> int:
        return len(set(self.current) | set(self.history))

    def bar_count(self) -> int:
        return sum(len(values) for values in self.history.values())


def normalize_code(code: str) -> str:
    value = str(code or "").strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if value.startswith(prefix):
            value = value[2:]
    return value


def normalize_stock_name(name: str) -> str:
    """Return a bounded, control/whitespace-free key for stock-name lookup."""
    value = unicodedata.normalize("NFKC", str(name or ""))
    value = "".join(char for char in value if not unicodedata.category(char).startswith("C") and not char.isspace())
    return value.casefold()[:64]


def parse_codes(raw: str | Iterable[str]) -> list[str]:
    values = raw.replace("，", ",").replace("\n", ",").replace(" ", ",").split(",") if isinstance(raw, str) else list(raw)
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        code = normalize_code(item)
        if code.isdigit() and len(code) == 6 and code not in seen:
            result.append(code)
            seen.add(code)
    return result


def calc_rsi(closes: Iterable[float], period: int = 6) -> float | None:
    try:
        data = [float(value) for value in closes]
    except (TypeError, ValueError):
        return None
    if not data or not all(math.isfinite(value) for value in data):
        return None
    if period <= 0 or len(data) <= period:
        return None
    changes = [data[index] - data[index - 1] for index in range(1, len(data))][-period:]
    gains = sum(max(change, 0.0) for change in changes) / period
    losses = sum(max(-change, 0.0) for change in changes) / period
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    return round(100 - 100 / (1 + gains / losses), 2)


def simple_moving_average(values: Iterable[float], period: int) -> float | None:
    data = [float(value) for value in values]
    if period <= 0 or len(data) < period:
        return None
    return sum(data[-period:]) / period


def calc_atr(highs: Iterable[float], lows: Iterable[float], closes: Iterable[float], period: int = 14) -> float | None:
    try:
        high_data, low_data, close_data = list(map(float, highs)), list(map(float, lows)), list(map(float, closes))
    except (TypeError, ValueError):
        return None
    if not high_data or not all(math.isfinite(value) for value in high_data + low_data + close_data):
        return None
    if period <= 0 or len(high_data) < period + 1 or len(low_data) != len(high_data) or len(close_data) != len(high_data):
        return None
    true_ranges = []
    for index in range(1, len(high_data)):
        true_ranges.append(max(high_data[index] - low_data[index], abs(high_data[index] - close_data[index - 1]), abs(low_data[index] - close_data[index - 1])))
    if len(true_ranges) < period:
        return None
    return round(sum(true_ranges[-period:]) / period, 4)


def calculate_daily_indicators(bars: Iterable[dict]) -> dict[str, object]:
    """Calculate indicators from point-in-time, unadjusted daily bars only."""
    normalized: list[tuple[str, float, float, float, float, float, str, str]] = []
    provenance_ok = True
    saw_rows = False
    sources: set[str] = set()
    for row in bars or []:
        saw_rows = True
        if not isinstance(row, dict):
            provenance_ok = False
            continue
        raw_basis = row.get("price_basis")
        basis = str(raw_basis or "unknown").strip().casefold() or "unknown"
        raw_source = row.get("source")
        source = raw_source.strip() if isinstance(raw_source, str) else ""
        source_key = source.casefold()
        if basis != "unadjusted" or not _source_is_trusted(source):
            provenance_ok = False
        sources.add(source_key)
        if len(sources) > 1:
            provenance_ok = False
        try:
            raw_trade_date = str(row.get("trade_date") or "").strip().replace("-", "")
            if not re.fullmatch(r"\d{8}", raw_trade_date):
                continue
            trade_date = datetime.strptime(raw_trade_date, "%Y%m%d").date().isoformat()
            open_price = float(row.get("open"))
            high = float(row.get("high"))
            low = float(row.get("low"))
            close = float(row.get("close"))
            volume = float(row.get("volume") or 0)
        except (AttributeError, TypeError, ValueError, OverflowError):
            continue
        if not trade_date or not all(math.isfinite(value) for value in (open_price, high, low, close, volume)):
            continue
        if min(open_price, high, low, close) <= 0 or high < low or high < max(open_price, close) or low > min(open_price, close):
            continue
        normalized.append((trade_date, open_price, high, low, close, max(0.0, volume), basis, source))
    if not saw_rows or not provenance_ok:
        # Do not calculate from a seemingly valid subset when the batch's
        # price basis or source cannot be established consistently.
        normalized.clear()
    normalized.sort(key=lambda row: row[0])
    closes = [row[4] for row in normalized]
    highs = [row[2] for row in normalized]
    lows = [row[3] for row in normalized]
    volumes = [row[5] for row in normalized]
    returns = [(closes[index] - closes[index - 1]) / closes[index - 1] for index in range(1, len(closes)) if closes[index - 1] > 0]
    recent_returns = returns[-20:]
    volatility = None
    if len(recent_returns) >= 5:
        average = sum(recent_returns) / len(recent_returns)
        volatility = (sum((value - average) ** 2 for value in recent_returns) / len(recent_returns)) ** 0.5
    return {
        "rsi6": calc_rsi(closes, 6),
        "ma5": simple_moving_average(closes, 5),
        "ma10": simple_moving_average(closes, 10),
        "ma20": simple_moving_average(closes, 20),
        "volume_ratio": (volumes[-1] / (sum(volumes[-6:-1]) / 5)) if len(volumes) >= 6 and sum(volumes[-6:-1]) > 0 else None,
        "atr14": calc_atr(highs, lows, closes, 14),
        "support20": min(lows[-20:]) if len(lows) >= 20 else None,
        "resistance20": max(highs[-20:]) if len(highs) >= 20 else None,
        "volatility20": volatility,
        "history_days": len(closes),
        "momentum5": ((closes[-1] / closes[-6] - 1) * 100) if len(closes) >= 6 and closes[-6] > 0 else None,
        "momentum20": ((closes[-1] / closes[-21] - 1) * 100) if len(closes) >= 21 and closes[-21] > 0 else None,
        "indicator_last_date": normalized[-1][0] if normalized else "",
        "indicator_last_close": normalized[-1][4] if normalized else None,
        "indicator_price_basis": normalized[-1][6] if normalized else "unknown",
        "indicator_source": normalized[-1][7] if normalized else "",
    }


def apply_daily_indicators(quote: Quote, bars: Iterable[dict]) -> bool:
    """Assign a daily-indicator set and report whether its history is usable for screening."""
    values = calculate_daily_indicators(bars)
    quote.rsi6, quote.ma5, quote.ma10, quote.ma20, quote.volume_ratio = (
        values["rsi6"], values["ma5"], values["ma10"], values["ma20"], values["volume_ratio"],
    )
    quote.atr14, quote.support20, quote.resistance20, quote.volatility20 = (
        values["atr14"], values["support20"], values["resistance20"], values["volatility20"],
    )
    quote.history_days = int(values["history_days"] or 0)
    quote.momentum5, quote.momentum20 = values["momentum5"], values["momentum20"]
    quote.indicator_last_date = str(values.get("indicator_last_date") or "")
    quote.indicator_last_close = values.get("indicator_last_close")
    quote.indicator_price_basis = str(values.get("indicator_price_basis") or "unknown")
    quote.indicator_source = str(values.get("indicator_source") or "")
    return (
        quote.history_days >= 20
        and _finite_positive(quote.atr14) is not None
        and bool(_normalize_plan_date(quote.indicator_last_date))
        and _finite_positive(quote.indicator_last_close) is not None
        and str(quote.indicator_price_basis or "").strip().casefold() == "unadjusted"
        and _source_is_trusted(quote.indicator_source)
    )


def _finite_positive(value) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _strict_positive_number(value) -> float | None:
    """Accept persisted numeric values without coercing arbitrary strings."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return _finite_positive(value)


_MAX_PRICE_PLAN_TOLERANCE_PCT = 20.0
_UNTRUSTED_SOURCE_VALUES = {"", "unknown", "none", "null", "n/a", "na", "-", "--"}


def _source_is_trusted(value) -> bool:
    if not isinstance(value, str):
        return False
    return value.strip().casefold() not in _UNTRUSTED_SOURCE_VALUES


def _strict_nonnegative_number(value) -> float | None:
    """Accept persisted non-negative numbers without coercing arbitrary strings."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _normalize_plan_date(value) -> str:
    text = str(value or "").strip().replace("-", "")
    if not re.fullmatch(r"\d{8}", text):
        return ""
    try:
        parsed = datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return ""
    return parsed.date().isoformat()


def _plan_provenance(
    quote: Quote,
    actual_date: str = "",
    context: str = "",
    reason: str = "",
    *,
    tolerance_pct=None,
    reference_price=None,
    deviation_pct=None,
) -> dict[str, object]:
    """Build the stable provenance shape persisted with every plan."""
    anchor = _finite_positive(reference_price)
    last_close = _finite_positive(getattr(quote, "indicator_last_close", None))
    if deviation_pct is None and anchor is not None and last_close is not None:
        deviation_pct = abs(last_close - anchor) / anchor * 100
    return {
        "context": str(context or "unscoped"),
        "actual_date": _normalize_plan_date(actual_date),
        "last_date": _normalize_plan_date(getattr(quote, "indicator_last_date", "")),
        "last_close": last_close,
        "basis": str(getattr(quote, "indicator_price_basis", "unknown") or "unknown").strip().lower(),
        "source": (
            getattr(quote, "indicator_source", "").strip()
            if isinstance(getattr(quote, "indicator_source", ""), str)
            else ""
        ),
        "tolerance_pct": _strict_nonnegative_number(tolerance_pct),
        "deviation_pct": _strict_nonnegative_number(deviation_pct),
        "anchor_price": anchor,
        "reference_price": anchor,
        "anchor": anchor,
        "reason": str(reason or ""),
    }


def _hidden_price_plan(
    quote: Quote,
    actual_date: str = "",
    context: str = "",
    reason: str = "",
    *,
    provenance: dict[str, object] | None = None,
    tolerance_pct=None,
    reference_price=None,
) -> PricePlan:
    provenance = (
        dict(provenance)
        if isinstance(provenance, dict)
        else _plan_provenance(
            quote,
            actual_date,
            context,
            reason,
            tolerance_pct=tolerance_pct,
            reference_price=reference_price,
        )
    )
    provenance["reason"] = str(reason or "")
    evidence = [reason] if reason else []
    return PricePlan("unknown", 0.0, None, None, None, None, None, None, None, None, None, "invalid", evidence, provenance, False)


def price_plan_levels_valid(plan: PricePlan | None) -> bool:
    """Return whether every persisted level has a finite, safe ordering."""
    if not isinstance(plan, PricePlan):
        return False
    names = ("reference_price", "atr", "support", "resistance", "attention_low", "attention_high", "confirmation", "sell_low", "sell_high", "invalidation")
    values: dict[str, float] = {}
    for name in names:
        value = _strict_positive_number(getattr(plan, name, None))
        if value is None:
            return False
        values[name] = value
    # Keep the semantic order explicit.  Reference price is the close anchor,
    # while support/invalidation and confirmation/sell zones must not overlap.
    return (
        values["invalidation"] < values["attention_low"] <= values["attention_high"]
        < values["confirmation"] <= values["sell_low"] <= values["sell_high"]
        and values["invalidation"] < values["support"] <= values["resistance"] <= values["confirmation"]
    )


def price_plan_is_validated(plan: PricePlan | None) -> bool:
    """Require the explicit v0.12 validation marker and fixed provenance."""
    if not isinstance(plan, PricePlan) or plan.validated is not True or plan.quality != "good":
        return False
    provenance = plan.provenance if isinstance(plan.provenance, dict) else {}
    actual_date = _normalize_plan_date(provenance.get("actual_date"))
    last_date = _normalize_plan_date(provenance.get("last_date"))
    reference = _strict_positive_number(plan.reference_price)
    last_close = _strict_positive_number(provenance.get("last_close"))
    tolerance = _strict_nonnegative_number(provenance.get("tolerance_pct"))
    if (
        provenance.get("context") != "daily_close"
        or str(provenance.get("basis") or "").strip().casefold() != "unadjusted"
        or not _source_is_trusted(provenance.get("source"))
        or not actual_date
        or last_date != actual_date
        or reference is None
        or last_close is None
        or tolerance is None
        or tolerance > _MAX_PRICE_PLAN_TOLERANCE_PCT
        or not price_plan_levels_valid(plan)
    ):
        return False

    # New plans must carry an explicit close tolerance and anchor.  The
    # optional legacy alias is checked when present, but old plans lacking the
    # new provenance contract remain hidden and unverified.
    required_anchor_fields = ("anchor_price", "reference_price")
    if any(field not in provenance for field in required_anchor_fields):
        return False
    for field in ("anchor_price", "reference_price", "anchor"):
        if field not in provenance:
            continue
        anchor = _strict_positive_number(provenance.get(field))
        if anchor is None or not math.isclose(anchor, reference, rel_tol=1e-9, abs_tol=1e-9):
            return False

    deviation = abs(last_close - reference) / reference * 100
    if not math.isfinite(deviation) or deviation > tolerance + 1e-9:
        return False
    if "deviation_pct" in provenance and provenance.get("deviation_pct") is not None:
        recorded = _strict_nonnegative_number(provenance.get("deviation_pct"))
        if recorded is None or not math.isclose(recorded, deviation, rel_tol=1e-7, abs_tol=1e-7):
            return False
    return True


def validate_price_plan(
    plan: PricePlan | None,
    quote: Quote,
    actual_date: str = "",
    tolerance_pct: float = 1.0,
    *,
    context: str = "daily_close",
) -> PricePlan:
    """Validate a plan only against a matching unadjusted daily close."""
    actual = _normalize_plan_date(actual_date)
    reference = _strict_positive_number(getattr(plan, "reference_price", None)) if plan else None
    last_close = _strict_positive_number(getattr(quote, "indicator_last_close", None))
    tolerance = _strict_nonnegative_number(tolerance_pct)
    provenance = _plan_provenance(
        quote,
        actual,
        context,
        tolerance_pct=tolerance,
        reference_price=reference,
    )
    failures: list[str] = []
    if context != "daily_close":
        failures.append("仅收盘日线计划可验证")
    if not actual:
        failures.append("缺少有效实际交易日")
    if provenance["basis"] != "unadjusted":
        failures.append("日线价格不是未复权口径")
    if not _source_is_trusted(provenance.get("source")):
        failures.append("日线来源不可信或缺失")
    last_date = _normalize_plan_date(provenance.get("last_date"))
    if not last_date or last_date != actual:
        failures.append("指标最后日期与实际交易日不一致")
    if reference is None or last_close is None:
        failures.append("收盘价或计划参考价不是有限正数")
    if tolerance is None or tolerance > _MAX_PRICE_PLAN_TOLERANCE_PCT:
        failures.append("容差不是0到20之间的有限非负数")
    deviation = None
    if reference is not None and last_close is not None:
        deviation = abs(last_close - reference) / reference * 100
        provenance["deviation_pct"] = deviation
        if tolerance is not None and tolerance <= _MAX_PRICE_PLAN_TOLERANCE_PCT and deviation > tolerance + 1e-9:
            failures.append(f"收盘价与参考价偏差{deviation:.4f}%超过{tolerance:.4f}%")
    if not price_plan_levels_valid(plan):
        failures.append("价位顺序或数值无效")
    if failures:
        provenance["reason"] = "；".join(failures)
        return _hidden_price_plan(quote, actual, context, provenance["reason"], provenance=provenance)
    provenance["reason"] = ""
    return PricePlan(
        plan.state,
        reference,
        _finite_positive(plan.atr),
        _finite_positive(plan.support),
        _finite_positive(plan.resistance),
        _finite_positive(plan.attention_low),
        _finite_positive(plan.attention_high),
        _finite_positive(plan.confirmation),
        _finite_positive(plan.sell_low),
        _finite_positive(plan.sell_high),
        _finite_positive(plan.invalidation),
        "good",
        list(plan.evidence or []),
        provenance,
        True,
    )


def build_price_plan(
    quote: Quote,
    tick: float = 0.01,
    *,
    context: str = "",
    actual_date: str = "",
    tolerance_pct: float = 1.0,
) -> PricePlan:
    """Derive reference zones, validating only when context is daily_close."""
    try:
        tick = float(tick)
    except (TypeError, ValueError, OverflowError):
        tick = 0.01
    if not math.isfinite(tick) or tick <= 0:
        tick = 0.01

    def rounded(value: float | None) -> float | None:
        if value is None or value <= 0:
            return None
        return round(round(value / tick) * tick, 2)

    price = _finite_positive(getattr(quote, "price", None))
    if price is None:
        return _hidden_price_plan(quote, actual_date, context, "价格数据不是有限正数", tolerance_pct=tolerance_pct)
    atr = _finite_positive(getattr(quote, "atr14", None))
    support = _finite_positive(getattr(quote, "support20", None))
    resistance = _finite_positive(getattr(quote, "resistance20", None))
    evidence: list[str] = []
    if atr is None or int(getattr(quote, "history_days", 0) or 0) < 20:
        plan = PricePlan("unknown", price, atr, support, resistance, None, None, None, None, None, None, "insufficient", ["历史数据不足20根，暂不计算参考价位"], _plan_provenance(quote, actual_date, context, tolerance_pct=tolerance_pct, reference_price=price), False)
        return validate_price_plan(plan, quote, actual_date, tolerance_pct, context=context) if context == "daily_close" else plan
    if support is None:
        support = max(0.01, price - 1.5 * atr)
        evidence.append("支撑位使用 ATR 回退估计")
    else:
        evidence.append("支撑位取近20日低点")
    if resistance is None:
        resistance = price + 2 * atr
        evidence.append("压力位使用 ATR 回退估计")
    else:
        evidence.append("压力位取近20日高点")
    attention_low = max(0.01, support - 0.25 * atr)
    attention_high = max(attention_low, support + 0.25 * atr)
    confirmation = resistance + max(0.2 * atr, 2 * tick)
    sell_low = confirmation + atr
    sell_high = sell_low + atr
    invalidation = max(0.01, support - atr)
    state = "invalidated" if price <= invalidation else "near_sell" if price >= sell_low else "confirmed" if price >= confirmation else "in_attention" if attention_low <= price <= attention_high else "between"
    plan = PricePlan(state, price, rounded(atr), rounded(support), rounded(resistance), rounded(attention_low), rounded(attention_high), rounded(confirmation), rounded(sell_low), rounded(sell_high), rounded(invalidation), "good", evidence, _plan_provenance(quote, actual_date, context, tolerance_pct=tolerance_pct, reference_price=price), False)
    return validate_price_plan(plan, quote, actual_date, tolerance_pct, context=context) if context == "daily_close" else plan


def build_price_plan_for_context(quote: Quote, actual_date: str = "", *, context: str = "", tolerance_pct: float = 1.0, tick: float = 0.01) -> PricePlan:
    """Named wrapper for callers that need to make context explicit."""
    return build_price_plan(quote, tick, context=context, actual_date=actual_date, tolerance_pct=tolerance_pct)


def price_plan_distance(plan: PricePlan | None, current_price) -> tuple[float, float] | None:
    """Return (absolute, percentage) distance from a validated plan anchor."""
    if not price_plan_is_validated(plan):
        return None
    reference = _finite_positive(plan.reference_price)
    current = _finite_positive(current_price)
    if reference is None or current is None:
        return None
    absolute = current - reference
    return absolute, absolute / reference * 100


def format_plan_distance(plan: PricePlan | None, current_price) -> str:
    """Format one consistent candidate-vs-current price comparison line."""
    distance = price_plan_distance(plan, current_price)
    if distance is None:
        return ""
    absolute, percent = distance
    return f"候选参考价{plan.reference_price:.2f}，实时当前价{float(current_price):.2f}，距离{absolute:+.2f}（{percent:+.2f}%）"


def _stored_plan_is_validated(plan: dict) -> bool:
    """Validate the persisted marker without trusting arbitrary JSON values."""
    if not isinstance(plan, dict) or plan.get("validated") is not True or plan.get("quality") != "good":
        return False
    provenance = plan.get("provenance")
    if not isinstance(provenance, dict) or provenance.get("context") != "daily_close" or provenance.get("basis") != "unadjusted":
        return False
    try:
        parsed = PricePlan(
            str(plan.get("state") or "unknown"),
            plan.get("reference_price"), plan.get("atr"), plan.get("support"), plan.get("resistance"),
            plan.get("attention_low"), plan.get("attention_high"), plan.get("confirmation"),
            plan.get("sell_low"), plan.get("sell_high"), plan.get("invalidation"), "good",
            plan.get("evidence") if isinstance(plan.get("evidence"), list) else [], provenance, True,
        )
        return price_plan_is_validated(parsed)
    except (TypeError, ValueError, OverflowError):
        return False


def format_stored_plan_distance(plan: dict, current_price) -> str:
    """Stored-row counterpart of :func:`format_plan_distance`."""
    if not _stored_plan_is_validated(plan):
        return ""
    reference = _finite_positive(plan.get("reference_price"))
    current = _finite_positive(current_price)
    if reference is None or current is None:
        return ""
    absolute = current - reference
    return f"候选参考价{reference:.2f}，实时当前价{current:.2f}，距离{absolute:+.2f}（{absolute / reference * 100:+.2f}%）"


def review_risk(quote: Quote, candidate: Candidate | None = None) -> RiskReview:
    flags: list[str] = []
    evidence: list[str] = []
    unknown_state = False
    if quote.suspended is True:
        flags.append("停牌")
    elif quote.suspended is None:
        flags.append("停牌状态未知")
        unknown_state = True
    if quote.limit_up is True or quote.limit_down is True:
        flags.append("涨跌停")
    elif quote.limit_up is None or quote.limit_down is None:
        flags.append("涨跌停状态未知")
        unknown_state = True
    if not math.isfinite(float(quote.price)) or quote.price <= 0:
        flags.append("价格无效")
    plan = candidate.price_plan if candidate else None
    if price_plan_is_validated(plan) and plan.invalidation is not None and _finite_positive(quote.price) is not None and quote.price <= plan.invalidation:
        flags.append("跌破失效位")
    if quote.history_days and quote.history_days < 20:
        flags.append("历史数据不足")
    if candidate and any("空头" in reason or "放量下跌" in reason for reason in candidate.reasons):
        flags.append("技术趋势偏弱")
    if quote.volatility20 is not None and quote.volatility20 >= 0.08:
        flags.append("波动率偏高")
    if flags and any(item in flags for item in ("停牌", "涨跌停", "价格无效", "跌破失效位")):
        return RiskReview("blocked", "high", flags, ["触发硬性交易状态过滤"])
    if unknown_state or not quote.history_days or quote.atr14 is None:
        return RiskReview("unknown", "unknown", flags + (["技术数据不完整"] if not quote.history_days or quote.atr14 is None else []), ["缺少足够行情状态或历史数据，不能判定风险"])
    if flags:
        return RiskReview("watch_only", "medium", flags, evidence or ["存在需要人工复核的技术风险"])
    return RiskReview("eligible", "low", [], ["基础行情、状态和历史指标检查通过"])


def is_tradable(quote: Quote, price_min: float = 2, price_max: float = 80) -> bool:
    return price_min <= quote.price <= price_max and quote.price > 0 and quote.suspended is not True and quote.limit_up is not True and quote.limit_down is not True


def score_quote(quote: Quote) -> Candidate:
    """Multi-factor research score; it ranks setups, never predicts returns."""
    score = 0
    reasons: list[str] = []
    if quote.rsi6 is not None and math.isfinite(float(quote.rsi6)):
        if 40 <= quote.rsi6 <= 65:
            score += 5
            reasons.append("RSI6处于可跟踪区间+5")
        elif quote.rsi6 < 30 and (quote.momentum5 or 0) > 0:
            score += 6
            reasons.append("RSI6超卖后回升+6")
        elif quote.rsi6 >= 75:
            score -= 5
            reasons.append("RSI6过热-5")
    if quote.ma5 is not None and quote.ma10 is not None and quote.ma20 is not None:
        if quote.ma5 > quote.ma10 > quote.ma20:
            score += 12
            reasons.append("均线多头趋势+12")
        elif quote.ma5 < quote.ma10 < quote.ma20:
            score -= 10
            reasons.append("均线空头-10")
    if quote.momentum5 is not None and math.isfinite(float(quote.momentum5)):
        if quote.momentum5 > 3:
            score += 8
            reasons.append("5日动量转强+8")
        elif quote.momentum5 < -5:
            score -= 4
            reasons.append("5日动量偏弱-4")
    if quote.momentum20 is not None and math.isfinite(float(quote.momentum20)):
        if quote.momentum20 > 0:
            score += 5
            reasons.append("20日趋势为正+5")
        elif quote.momentum20 < -10:
            score -= 5
            reasons.append("20日趋势偏弱-5")
    if quote.volume_ratio is not None:
        if quote.pct_change > 0 and quote.volume_ratio > 1:
            score += 5
            reasons.append("价涨量增+5")
        elif quote.pct_change < 0 and quote.volume_ratio > 1:
            score -= 5
            reasons.append("放量下跌-5")
    if quote.volatility20 is not None and math.isfinite(float(quote.volatility20)) and quote.volatility20 >= 0.08:
        score -= 5
        reasons.append("波动率偏高-5")
    candidate = Candidate(quote, score, reasons, base_score=score)
    candidate.price_plan = build_price_plan(quote)
    review = review_risk(quote, candidate)
    candidate.risk_level = review.verdict
    candidate.risk_flags = review.flags
    return candidate


def in_trading_session(now: datetime | None = None) -> bool:
    current = (now or datetime.now(CHINA_TZ)).astimezone(CHINA_TZ)
    if current.weekday() >= 5:
        return False
    value = current.time()
    return time(9, 30) <= value < time(11, 30) or time(13, 0) <= value < time(15, 0)


def _clean_format_text(value, limit: int) -> str:
    return re.sub(r"[\x00-\x1f\x7f]", "", str(value or "")).strip()[:limit]


def _finite_format_number(value) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _format_pct_change(value) -> str:
    number = _finite_format_number(value)
    return f"{number:+.2f}%" if number is not None else "涨跌未记录"


def _decode_stored_object(value) -> dict:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _format_stored_score(value, fallback: str = "未知") -> str:
    number = _finite_format_number(value)
    if number is None:
        return fallback
    formatted = str(int(number)) if number.is_integer() else f"{number:g}"
    return formatted if len(formatted) <= 16 else fallback


def _format_stored_price(value) -> str | None:
    number = _finite_format_number(value)
    if number is None or number <= 0:
        return None
    formatted = f"{number:.2f}"
    return formatted if len(formatted) <= 20 else None


_PCT_UNSET = object()


def format_stored_candidate(row: dict, index: int, pct_change=_PCT_UNSET) -> str:
    """Render one persisted candidate as the same fixed three-line summary.

    Persisted rows are external state: malformed JSON and non-finite price
    levels deliberately degrade to a readable fallback instead of aborting a
    whole candidate-pool report.
    """
    row = row if isinstance(row, dict) else {}
    code = _clean_format_text(row.get("code"), 20) or "未知代码"
    name = _clean_format_text(row.get("name"), 24)
    if not name or name == code:
        name = "名称未获取"
    score = _format_stored_score(row.get("score"))
    score_max = _format_stored_score(row.get("score_max"))
    score_label = "综合" if score_max == "70" else "技术"
    risk = risk_label(_clean_format_text(row.get("risk_level"), 32) or "unknown")

    raw_reasons = row.get("reasons")
    reasons_data = []
    if isinstance(raw_reasons, list):
        reasons_data = raw_reasons
    elif isinstance(raw_reasons, str):
        try:
            decoded = json.loads(raw_reasons)
            reasons_data = decoded if isinstance(decoded, list) else []
        except (TypeError, ValueError, json.JSONDecodeError):
            reasons_data = []
    reasons = [
        re.sub(r"[+-]\d+$", "", _clean_format_text(item, 80))
        for item in reasons_data
        if _clean_format_text(item, 80)
    ][:3]
    reason_text = "、".join(reasons) or "技术指标有限"

    plan = _decode_stored_object(row.get("price_plan"))
    levels = "历史数据不足" if str(plan.get("quality") or "").strip().lower() == "insufficient" else "无已验证收盘计划"
    if _stored_plan_is_validated(plan):
        invalid_price = any(
            key in plan and plan.get(key) is not None and _finite_format_number(plan.get(key)) is None
            for key in ("reference_price", "atr", "support", "resistance", "attention_low", "attention_high", "confirmation", "sell_low", "sell_high", "invalidation")
        )
        attention_low = _format_stored_price(plan.get("attention_low"))
        attention_high = _format_stored_price(plan.get("attention_high"))
        invalidation = _format_stored_price(plan.get("invalidation"))
        confirmation = _format_stored_price(plan.get("confirmation"))
        if (
            not invalid_price
            and attention_low is not None
            and attention_high is not None
            and invalidation is not None
            and float(attention_low) <= float(attention_high)
            and (plan.get("confirmation") is None or confirmation is not None)
        ):
            reference = _format_stored_price(plan.get("reference_price"))
            levels = f"参考 {reference or '未知'}｜关注 {attention_low}-{attention_high}｜失效 {invalidation}"
            if confirmation is not None:
                levels = f"{levels}｜确认 {confirmation}"

    raw_pct = row.get("pct_change") if pct_change is _PCT_UNSET else pct_change
    return (
        f"{_format_stored_score(index, fallback=str(index))}. {name}（{code}）｜{score_label} {score}/{score_max}｜{_format_pct_change(raw_pct)}｜{risk}\n"
        f"   看点：{reason_text}\n"
        f"   价位：{levels}"
    )


def format_stored_compact_candidate(row: dict, index: int, pct_change=_PCT_UNSET) -> str:
    """Compatibility alias for callers that name the three-line view compact."""
    return format_stored_candidate(row, index, pct_change)


def format_candidate(candidate: Candidate) -> str:
    quote = candidate.quote
    name = re.sub(r"[\x00-\x1f\x7f]", "", str(quote.name or "")).strip()[:40]
    if not name or name == quote.code:
        name = "名称未知"
    why = "、".join(candidate.reasons) or "暂无技术加分"
    metrics = []
    if quote.rsi6 is not None:
        metrics.append(f"RSI6={quote.rsi6:.1f}")
    if quote.ma5 is not None and quote.ma10 is not None and quote.ma20 is not None:
        metrics.append(f"MA5/10/20={quote.ma5:.2f}/{quote.ma10:.2f}/{quote.ma20:.2f}")
    if quote.volume_ratio is not None:
        metrics.append(f"量比={quote.volume_ratio:.2f}")
    if quote.momentum5 is not None and quote.momentum20 is not None:
        metrics.append(f"5/20日动量={quote.momentum5:+.1f}%/{quote.momentum20:+.1f}%")
    evidence = "、".join(metrics) or "当前可用技术指标不足"
    volume_evidence = f"量比{quote.volume_ratio:.2f}" if quote.volume_ratio is not None and _finite_format_number(quote.volume_ratio) is not None else "量能证据不足"
    factor_line = ""
    overlay = candidate.factor_overlay
    if overlay:
        pieces = [f"行业{overlay.industry_score if overlay.industry_score is not None else '未知'}", f"基本面{overlay.fundamental_score if overlay.fundamental_score is not None else '未知'}", f"大盘{overlay.market_regime}{overlay.market_adjustment:+d}"]
        factor_line = f"因子参考：{'，'.join(pieces)}；来源{overlay.source or '未知'}；质量{overlay.quality}\n"
    risk_items: list[str] = list(candidate.risk_flags)
    if any("空头" in reason or "放量下跌" in reason for reason in candidate.reasons):
        risk_items.append("均线偏弱或出现放量下跌")
    if quote.rsi6 is not None and quote.rsi6 >= 70:
        risk_items.append("RSI6处于高位，短线波动风险较高")
    if not metrics:
        risk_items.append("技术数据不足，评分参考价值有限")
    risk_text = "；".join(risk_items) if risk_items else "暂未触发机械风险项，但指标可能滞后或不完整"
    plan = candidate.price_plan
    if price_plan_is_validated(plan):
        levels = f"关注区{plan.attention_low:.2f}-{plan.attention_high:.2f}，确认位{plan.confirmation:.2f}，参考卖出区{plan.sell_low:.2f}-{plan.sell_high:.2f}，失效位{plan.invalidation:.2f}"
        distance = format_plan_distance(plan, quote.price)
        if distance:
            levels = f"{distance}；{levels}"
    elif plan is not None and plan.quality == "insufficient":
        levels = "历史数据不足，暂不计算参考价位"
    else:
        levels = "无已验证收盘计划，暂不展示价位"
    conclusion = "进入观察池，先复核日线趋势、基本面、公告和流动性；价位仅供人工研究"
    composite_line = f"综合评分：{candidate.composite_score}/70\n" if candidate.composite_score is not None else ""
    pct_text = _format_pct_change(quote.pct_change)
    pct_line = "涨跌未记录" if pct_text == "涨跌未记录" else f"涨跌{pct_text}"
    return (
        f"候选：{name}（{quote.code}）\n"
        f"行情：现价{quote.price:.2f}，{pct_line}，成交额{quote.amount:.0f}\n"
        f"技术评分：{candidate.base_score}/{candidate.score_max}（规则加分：{why}）\n"
        f"{composite_line}"
        f"技术证据：{evidence}\n"
        f"量能证据：{volume_evidence}\n"
        f"{factor_line}"
        f"参考价位：{levels}\n"
        f"风险审核：{risk_label(candidate.risk_level)}；{risk_text}\n"
        f"研究结论：{conclusion}。"
    )


def format_compact_candidate(candidate: Candidate, index: int) -> str:
    """Three-line market-screen summary; detailed reports keep using format_candidate."""
    quote = candidate.quote
    name = re.sub(r"[\x00-\x1f\x7f]", "", str(quote.name or "")).strip()[:24]
    if not name or name == quote.code:
        name = "名称未获取"
    risk = risk_label(candidate.risk_level)
    reasons = [re.sub(r"[+-]\d+$", "", str(item)) for item in candidate.reasons[:3]]
    reason_text = "、".join(reasons) or "技术指标有限"
    plan = candidate.price_plan
    score_label = "综合" if candidate.composite_score is not None else "技术"
    if price_plan_is_validated(plan):
        distance = format_plan_distance(plan, quote.price)
        levels = f"参考 {plan.reference_price:.2f}｜关注 {plan.attention_low:.2f}-{plan.attention_high:.2f}｜失效 {plan.invalidation:.2f}"
        if plan.confirmation is not None:
            levels = f"{levels}｜确认 {plan.confirmation:.2f}"
        if distance:
            levels = f"{levels}｜{distance}"
    elif plan is not None and plan.quality == "insufficient":
        levels = "历史数据不足"
    else:
        levels = "无已验证收盘计划"
    return (
        f"{index}. {name}（{quote.code}）｜{score_label} {candidate.score}/{candidate.score_max}｜{_format_pct_change(quote.pct_change)}｜{risk}\n"
        f"   看点：{reason_text}\n"
        f"   价位：{levels}"
    )
